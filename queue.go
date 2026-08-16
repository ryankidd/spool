package spool

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Errors returned by Queue.
var (
	// ErrEmpty is returned by Lease when no job is ready for delivery.
	ErrEmpty = errors.New("spool: no job ready")

	// ErrLeaseExpired is returned by Ack when the job's lease is no longer
	// the one being acked — the deadline passed, or the job was already
	// acked by this lease.
	ErrLeaseExpired = errors.New("spool: lease expired")

	// ErrClosed is returned by every Queue method once the queue is closed.
	ErrClosed = errors.New("spool: queue is closed")
)

// Clock reports the current time. Queue only uses it to compute and compare
// lease deadlines, so tests can drive expiry without sleeping.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Job is a leased unit of work. It is a snapshot handed to one consumer: the
// lease id identifies this particular delivery, and Ack only succeeds while
// that delivery is still the current one.
type Job struct {
	// ID identifies the job for its whole lifetime, across redeliveries.
	ID uint64
	// Payload is the bytes passed to Enqueue.
	Payload []byte
	// LeaseID identifies this delivery. It changes on every redelivery.
	LeaseID uint64
	// Deadline is when this lease expires, per the queue's clock.
	Deadline time.Time
	// Deliveries counts how many times the job has been handed out,
	// including this delivery. It is 1 on a first delivery.
	Deliveries int
}

// Stats is a point-in-time count of the jobs a queue is holding.
type Stats struct {
	// Ready is the number of jobs waiting to be leased.
	Ready int
	// Leased is the number of jobs currently held by a consumer.
	Leased int
}

// jobState is the queue's in-memory view of one unacked job.
type jobState struct {
	payload    []byte
	deliveries int
	lease      uint64 // 0 when the job is ready rather than leased
	deadline   time.Time
}

// firstLeaseID is where lease ids start. Zero is reserved as the "not
// leased" marker in jobState, so handing it out as a real lease id would let
// an expired lease ack a job that had already been released.
const firstLeaseID = 1

// Queue is a durable job queue built on a write-ahead log. Every state
// transition — enqueue, lease, ack — is appended to the log before it takes
// effect, so the queue's state is exactly the fold of its log and survives a
// restart by replaying it.
//
// The log is stored as a directory of rotating segments rather than one file,
// so acked work does not accumulate forever: once a run of segments holds only
// jobs that have been acked, it is dropped. See "Rotation and compaction" in
// the package README.
//
// A Queue is safe for concurrent use by multiple producers and consumers.
type Queue struct {
	clock           Clock
	syncPolicy      SyncPolicy
	maxSegmentBytes int64

	mu        sync.Mutex
	slog      *segmentedLog
	closed    bool
	jobs      map[uint64]*jobState
	ready     []uint64            // ids waiting for delivery, in order
	leased    map[uint64]struct{} // ids currently held by a consumer
	nextJob   uint64
	nextLease uint64

	// openAtSeal records, for each sealed segment, how many jobs were still
	// unacked at the moment it was sealed — the count of records in that
	// segment and everything before it that a fresh replay would still need.
	// A sealed segment whose value is zero closed over no live job and, with
	// every earlier segment, can be dropped. See reclaimSegmentsLocked.
	openAtSeal map[uint64]int
}

// Option configures a Queue at open time.
type Option func(*Queue)

// WithClock sets the clock used for lease deadlines. The default is
// time.Now, whose monotonic reading makes deadlines immune to wall-clock
// jumps while the process is running.
func WithClock(c Clock) Option {
	return func(q *Queue) { q.clock = c }
}

// WithSync sets the policy deciding when the queue's log fsyncs. The default is
// SyncEveryAppend, so every Enqueue, Lease and Ack is on stable storage before
// it returns. A looser policy (see SyncEvery) trades a bounded tail of
// un-synced writes for throughput; the durability that costs is spelled out on
// SyncEvery and in the package README.
func WithSync(p SyncPolicy) Option {
	return func(q *Queue) { q.syncPolicy = p }
}

// WithMaxSegmentBytes sets the size a log segment may reach before the queue
// rolls to a new one. Rotation is what makes compaction possible: a whole
// sealed segment is the unit that gets reclaimed once the jobs it recorded are
// all acked. A non-positive value keeps the default. Smaller segments reclaim
// space sooner but rotate more often; the default suits jobs of a few hundred
// bytes.
func WithMaxSegmentBytes(n int64) Option {
	return func(q *Queue) {
		if n > 0 {
			q.maxSegmentBytes = n
		}
	}
}

// OpenQueue opens the queue whose log lives in the directory at path,
// replaying it to rebuild the queue's state, then reopens the newest segment
// for appending. The directory and its first segment are created if they don't
// exist yet, so a fresh path gives an empty queue.
func OpenQueue(path string, opts ...Option) (*Queue, error) {
	q := &Queue{
		clock:           systemClock{},
		syncPolicy:      SyncEveryAppend(),
		maxSegmentBytes: defaultMaxSegmentBytes,
		jobs:            make(map[uint64]*jobState),
		leased:          make(map[uint64]struct{}),
		nextLease:       firstLeaseID,
		openAtSeal:      make(map[uint64]int),
	}
	for _, opt := range opts {
		opt(q)
	}

	sl, err := newSegmentedLog(path, q.clock, q.syncPolicy, q.maxSegmentBytes, q.applyRecord, q.recordSeal)
	if err != nil {
		return nil, err
	}
	q.slog = sl

	q.buildReady()

	// A queue that was drained before it closed can have whole segments that
	// are already reclaimable; do that pass now rather than waiting for the
	// next rotation.
	q.reclaimSegmentsLocked()
	return q, nil
}

// applyRecord folds one log record into the queue's in-memory state during
// replay. It is the inverse of the appends Enqueue, Lease and Ack make.
//
// Leases deliberately do not survive replay. A lease is a promise to one
// consumer in one process; if the queue is being reopened, that consumer is
// gone and waiting out its deadline would only delay the work. So a job whose
// last record is a lease comes back ready for immediate redelivery (see
// buildReady), and the log never needs to store a deadline — only the lease
// id, which keeps the delivery count and stale-ack detection accurate across
// restarts.
func (q *Queue) applyRecord(data []byte) error {
	r, err := decodeRecord(data)
	if err != nil {
		return err
	}

	switch r.kind {
	case kindEnqueue:
		if _, ok := q.jobs[r.job]; ok {
			return fmt.Errorf("spool: duplicate enqueue record for job %d", r.job)
		}
		q.jobs[r.job] = &jobState{payload: r.payload}
	case kindLease:
		js, ok := q.jobs[r.job]
		if !ok {
			return fmt.Errorf("spool: lease record for unknown job %d", r.job)
		}
		js.lease = r.lease
		js.deliveries++
	case kindAck:
		js, ok := q.jobs[r.job]
		if !ok {
			return fmt.Errorf("spool: ack record for unknown job %d", r.job)
		}
		if js.lease != r.lease {
			return fmt.Errorf("spool: ack record for job %d carries lease %d, want %d", r.job, r.lease, js.lease)
		}
		delete(q.jobs, r.job)
	}

	if r.job >= q.nextJob {
		q.nextJob = r.job + 1
	}
	if r.lease >= q.nextLease {
		q.nextLease = r.lease + 1
	}
	return nil
}

// recordSeal captures how many jobs are still unacked at a sealed segment's
// boundary during replay. len(jobs) at that point is exactly the number of
// jobs enqueued in that segment or earlier and not yet acked in that segment
// or earlier — the segment's contribution to a fresh replay. Zero means the
// segment, with all before it, can be dropped.
func (q *Queue) recordSeal(seq uint64) {
	q.openAtSeal[seq] = len(q.jobs)
}

// buildReady turns the replayed job set into the ordered ready queue. Every
// surviving job comes back ready, since leases do not outlive a reopen.
func (q *Queue) buildReady() {
	q.ready = make([]uint64, 0, len(q.jobs))
	for id, js := range q.jobs {
		js.lease = 0
		js.deadline = time.Time{}
		q.ready = append(q.ready, id)
	}
	// Job ids are handed out in enqueue order, so sorting restores it.
	sort.Slice(q.ready, func(i, j int) bool { return q.ready[i] < q.ready[j] })
}

// Enqueue durably appends a job to the queue and returns its id. The payload
// is copied, so the caller may reuse the slice.
func (q *Queue) Enqueue(payload []byte) (uint64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return 0, ErrClosed
	}

	id := q.nextJob
	rec := record{kind: kindEnqueue, job: id, payload: payload}
	rotated, sealed, err := q.slog.append(rec.encode())
	if err != nil {
		return 0, err
	}

	q.nextJob++
	q.jobs[id] = &jobState{payload: append([]byte(nil), payload...)}
	q.ready = append(q.ready, id)
	q.afterAppendLocked(rotated, sealed)
	return id, nil
}

// afterAppendLocked runs the segment bookkeeping that follows an append which
// rolled to a new segment. It must be called after the record's effect has
// been applied to jobs, so len(jobs) is the segment's live-job count at seal —
// see recordSeal. It then reclaims any segments the seal made obsolete.
func (q *Queue) afterAppendLocked(rotated bool, sealed uint64) {
	if !rotated {
		return
	}
	q.openAtSeal[sealed] = len(q.jobs)
	q.reclaimSegmentsLocked()
}

// reclaimSegmentsLocked drops the longest run of oldest sealed segments the
// log no longer needs. A sealed segment is droppable once no job it recorded
// is still live: concretely, once openAtSeal for that segment is zero, meaning
// every job enqueued in it or earlier was also acked in it or earlier. The
// manifest lets us jump the floor straight to the highest such segment, past
// any earlier boundary that still had work in flight, so a busy stretch
// between two drained points does not block reclaiming the drained prefix.
//
// It is deliberately conservative: a single never-acked job pins its segment
// and everything after it, and nothing is dropped while any job that touches a
// segment remains unacked or leaseable.
func (q *Queue) reclaimSegmentsLocked() {
	target, found := uint64(0), false
	for seq, open := range q.openAtSeal {
		if open != 0 || seq >= q.slog.activeSeq {
			continue
		}
		if !found || seq > target {
			target, found = seq, true
		}
	}
	if !found {
		return
	}
	if err := q.slog.dropThrough(target); err != nil {
		// Reclamation is durability housekeeping, not a correctness step: the
		// dropped segments only hold already-acked work. If it fails, leave
		// them in place and retry after the next rotation rather than failing
		// the caller's operation.
		return
	}
	for seq := range q.openAtSeal {
		if seq <= target {
			delete(q.openAtSeal, seq)
		}
	}
}

// reclaimLocked returns every job whose lease has expired to the ready
// queue, oldest job first. Expiry writes nothing to the log: it isn't a
// decision the queue makes, it's the absence of an ack by a deadline, and
// replaying the log recomputes it. That keeps a queue full of slow consumers
// from growing the log while no work is completing.
//
// This costs a scan of the leased set, not of every job, so it stays
// proportional to the number of consumers in flight.
func (q *Queue) reclaimLocked(now time.Time) {
	var expired []uint64
	for id := range q.leased {
		if js := q.jobs[id]; now.After(js.deadline) {
			expired = append(expired, id)
		}
	}
	if len(expired) == 0 {
		return
	}

	// Map iteration order is random; sort so redelivery order is
	// deterministic when several leases expire at once.
	sort.Slice(expired, func(i, j int) bool { return expired[i] < expired[j] })
	for _, id := range expired {
		q.jobs[id].lease = 0
		delete(q.leased, id)
		q.ready = append(q.ready, id)
	}
}

// Lease hands the oldest ready job to the caller with a deadline d in the
// future. The job stays off the ready queue until it is acked or its lease
// expires. If no job is ready, Lease returns ErrEmpty.
//
// A job whose lease expires without an ack goes back on the ready queue, at
// the tail, and is handed to whichever consumer leases next. Delivery is
// therefore at least once: a consumer that does the work and dies before
// acking, or that overruns its deadline, will see the job delivered again.
func (q *Queue) Lease(d time.Duration) (*Job, error) {
	if d <= 0 {
		return nil, errors.New("spool: lease duration must be positive")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, ErrClosed
	}

	now := q.clock.Now()
	q.reclaimLocked(now)
	if len(q.ready) == 0 {
		return nil, ErrEmpty
	}

	id := q.ready[0]
	js := q.jobs[id]
	lease := q.nextLease

	rec := record{kind: kindLease, job: id, lease: lease}
	rotated, sealed, err := q.slog.append(rec.encode())
	if err != nil {
		return nil, err
	}

	q.ready = q.ready[1:]
	q.leased[id] = struct{}{}
	q.nextLease++
	js.lease = lease
	js.deadline = now.Add(d)
	js.deliveries++
	q.afterAppendLocked(rotated, sealed)

	return &Job{
		ID:         id,
		Payload:    append([]byte(nil), js.payload...),
		LeaseID:    lease,
		Deadline:   js.deadline,
		Deliveries: js.deliveries,
	}, nil
}

// Ack durably marks a leased job as done and removes it from the queue. It
// returns ErrLeaseExpired if j is not the current delivery of that job —
// because the deadline passed, or because the job was already acked.
//
// The deadline is enforced strictly: an ack that arrives after it fails even
// if no other consumer has picked the job up yet. A lease that could be acked
// late would be no deadline at all, and the consumer holding it has no way to
// know whether its work was still wanted.
func (q *Queue) Ack(j *Job) error {
	if j == nil {
		return errors.New("spool: ack of a nil job")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrClosed
	}
	q.reclaimLocked(q.clock.Now())

	js, ok := q.jobs[j.ID]
	if !ok || js.lease != j.LeaseID {
		return fmt.Errorf("%w: job %d lease %d", ErrLeaseExpired, j.ID, j.LeaseID)
	}

	rec := record{kind: kindAck, job: j.ID, lease: j.LeaseID}
	rotated, sealed, err := q.slog.append(rec.encode())
	if err != nil {
		return err
	}

	delete(q.jobs, j.ID)
	delete(q.leased, j.ID)
	q.afterAppendLocked(rotated, sealed)
	return nil
}

// Stats reports how many jobs are ready and how many are leased. Leases that
// have already expired are counted as ready, not as leased.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.reclaimLocked(q.clock.Now())
	return Stats{Ready: len(q.ready), Leased: len(q.leased)}
}

// Close closes the underlying log. Further calls on the queue return
// ErrClosed. Jobs that were leased and not acked are simply unacked in the
// log, so reopening the queue makes them ready again.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrClosed
	}
	q.closed = true
	return q.slog.close()
}
