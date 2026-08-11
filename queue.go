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

// Queue is a durable job queue built on a write-ahead log. Every state
// transition — enqueue, lease, ack — is appended to the log before it takes
// effect, so the queue's state is exactly the fold of its log and survives a
// restart by replaying it.
//
// A Queue is safe for concurrent use by multiple producers and consumers.
type Queue struct {
	clock Clock

	mu        sync.Mutex
	log       *Log
	closed    bool
	jobs      map[uint64]*jobState
	ready     []uint64            // ids waiting for delivery, in order
	leased    map[uint64]struct{} // ids currently held by a consumer
	nextJob   uint64
	nextLease uint64
}

// Option configures a Queue at open time.
type Option func(*Queue)

// WithClock sets the clock used for lease deadlines. The default is
// time.Now, whose monotonic reading makes deadlines immune to wall-clock
// jumps while the process is running.
func WithClock(c Clock) Option {
	return func(q *Queue) { q.clock = c }
}

// OpenQueue opens the queue whose log lives at path, replaying it to rebuild
// the queue's state, then reopens the log for appending. A path that doesn't
// exist yet gives an empty queue.
func OpenQueue(path string, opts ...Option) (*Queue, error) {
	q := &Queue{
		clock:  systemClock{},
		jobs:   make(map[uint64]*jobState),
		leased: make(map[uint64]struct{}),
	}
	for _, opt := range opts {
		opt(q)
	}

	if err := q.replay(path); err != nil {
		return nil, err
	}

	l, err := Open(path)
	if err != nil {
		return nil, err
	}
	q.log = l
	return q, nil
}

// replay rebuilds the queue's state from the log at path.
//
// Leases deliberately do not survive replay. A lease is a promise to one
// consumer in one process; if the queue is being reopened, that consumer is
// gone and waiting out its deadline would only delay the work. So a job whose
// last record is a lease comes back ready for immediate redelivery, and the
// log never needs to store a deadline — only the lease id, which keeps the
// delivery count and stale-ack detection accurate across restarts.
func (q *Queue) replay(path string) error {
	err := Replay(path, func(data []byte) error {
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
	})
	if err != nil {
		return err
	}

	q.ready = make([]uint64, 0, len(q.jobs))
	for id, js := range q.jobs {
		js.lease = 0
		js.deadline = time.Time{}
		q.ready = append(q.ready, id)
	}
	// Job ids are handed out in enqueue order, so sorting restores it.
	sort.Slice(q.ready, func(i, j int) bool { return q.ready[i] < q.ready[j] })
	return nil
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
	if err := q.log.Append(rec.encode()); err != nil {
		return 0, err
	}

	q.nextJob++
	q.jobs[id] = &jobState{payload: append([]byte(nil), payload...)}
	q.ready = append(q.ready, id)
	return id, nil
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
	if err := q.log.Append(rec.encode()); err != nil {
		return nil, err
	}

	q.ready = q.ready[1:]
	q.leased[id] = struct{}{}
	q.nextLease++
	js.lease = lease
	js.deadline = now.Add(d)
	js.deliveries++

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
	if err := q.log.Append(rec.encode()); err != nil {
		return err
	}

	delete(q.jobs, j.ID)
	delete(q.leased, j.ID)
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
	return q.log.Close()
}
