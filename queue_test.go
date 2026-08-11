package spool

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a manually advanced Clock, so lease expiry can be tested
// without sleeping.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func openQueue(t *testing.T, path string, opts ...Option) *Queue {
	t.Helper()
	q, err := OpenQueue(path, opts...)
	if err != nil {
		t.Fatalf("OpenQueue: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func TestEnqueueLeaseAck(t *testing.T) {
	q := openQueue(t, filepath.Join(t.TempDir(), "queue.log"))

	id, err := q.Enqueue([]byte("resize avatar"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job, err := q.Lease(time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if job.ID != id {
		t.Fatalf("leased job %d, want %d", job.ID, id)
	}
	if string(job.Payload) != "resize avatar" {
		t.Fatalf("leased payload %q, want %q", job.Payload, "resize avatar")
	}
	if job.Deliveries != 1 {
		t.Fatalf("first delivery reported Deliveries=%d, want 1", job.Deliveries)
	}
	if got := q.Stats(); got != (Stats{Leased: 1}) {
		t.Fatalf("after Lease, stats %+v, want one leased job", got)
	}

	if err := q.Ack(job); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if got := q.Stats(); got != (Stats{}) {
		t.Fatalf("after Ack, stats %+v, want an empty queue", got)
	}
	if _, err := q.Lease(time.Minute); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Lease on drained queue: %v, want ErrEmpty", err)
	}
}

func TestLeaseIsFIFO(t *testing.T) {
	q := openQueue(t, filepath.Join(t.TempDir(), "queue.log"))

	want := []string{"first", "second", "third"}
	for _, payload := range want {
		if _, err := q.Enqueue([]byte(payload)); err != nil {
			t.Fatalf("Enqueue(%q): %v", payload, err)
		}
	}

	for _, payload := range want {
		job, err := q.Lease(time.Minute)
		if err != nil {
			t.Fatalf("Lease: %v", err)
		}
		if string(job.Payload) != payload {
			t.Fatalf("leased %q, want %q", job.Payload, payload)
		}
	}
}

func TestLeaseEmptyQueue(t *testing.T) {
	q := openQueue(t, filepath.Join(t.TempDir(), "queue.log"))

	if _, err := q.Lease(time.Minute); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Lease on empty queue: %v, want ErrEmpty", err)
	}
}

func TestLeasedJobIsNotDeliveredTwice(t *testing.T) {
	clock := newFakeClock()
	q := openQueue(t, filepath.Join(t.TempDir(), "queue.log"), WithClock(clock))

	if _, err := q.Enqueue([]byte("only job")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.Lease(time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}

	// Still inside the lease, so the job is not up for grabs.
	clock.Advance(59 * time.Second)
	if _, err := q.Lease(time.Minute); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Lease while a lease is live: %v, want ErrEmpty", err)
	}
}

func TestRedeliveryAfterLeaseExpiry(t *testing.T) {
	clock := newFakeClock()
	q := openQueue(t, filepath.Join(t.TempDir(), "queue.log"), WithClock(clock))

	id, err := q.Enqueue([]byte("flaky job"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	first, err := q.Lease(time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}

	clock.Advance(2 * time.Minute)
	if got := q.Stats(); got != (Stats{Ready: 1}) {
		t.Fatalf("after expiry, stats %+v, want one ready job", got)
	}

	second, err := q.Lease(time.Minute)
	if err != nil {
		t.Fatalf("Lease after expiry: %v", err)
	}
	if second.ID != id {
		t.Fatalf("redelivered job %d, want %d", second.ID, id)
	}
	if string(second.Payload) != "flaky job" {
		t.Fatalf("redelivered payload %q, want %q", second.Payload, "flaky job")
	}
	if second.LeaseID == first.LeaseID {
		t.Fatalf("redelivery reused lease id %d", second.LeaseID)
	}
	if second.Deliveries != 2 {
		t.Fatalf("redelivery reported Deliveries=%d, want 2", second.Deliveries)
	}

	// The first consumer's lease is dead; only the current one can ack.
	if err := q.Ack(first); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Ack with an expired lease: %v, want ErrLeaseExpired", err)
	}
	if err := q.Ack(second); err != nil {
		t.Fatalf("Ack with the current lease: %v", err)
	}
	if got := q.Stats(); got != (Stats{}) {
		t.Fatalf("after Ack, stats %+v, want an empty queue", got)
	}
}

func TestAckAfterDeadlineIsRejected(t *testing.T) {
	clock := newFakeClock()
	q := openQueue(t, filepath.Join(t.TempDir(), "queue.log"), WithClock(clock))

	if _, err := q.Enqueue([]byte("slow job")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Lease(time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}

	// Nobody else has leased the job, but the deadline still stands.
	clock.Advance(time.Minute + time.Nanosecond)
	if err := q.Ack(job); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Ack past the deadline: %v, want ErrLeaseExpired", err)
	}
	if got := q.Stats(); got != (Stats{Ready: 1}) {
		t.Fatalf("after a rejected Ack, stats %+v, want the job ready again", got)
	}
}

func TestDoubleAckIsRejected(t *testing.T) {
	q := openQueue(t, filepath.Join(t.TempDir(), "queue.log"))

	if _, err := q.Enqueue([]byte("job")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Lease(time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if err := q.Ack(job); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := q.Ack(job); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("second Ack: %v, want ErrLeaseExpired", err)
	}
}

func TestStateSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.log")
	q := openQueue(t, path)

	for _, payload := range []string{"one", "two", "three"} {
		if _, err := q.Enqueue([]byte(payload)); err != nil {
			t.Fatalf("Enqueue(%q): %v", payload, err)
		}
	}

	// Finish "one", leave "two" leased and unacked, never touch "three".
	first, err := q.Lease(time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if err := q.Ack(first); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	leaked, err := q.Lease(time.Hour)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openQueue(t, path)
	if got := reopened.Stats(); got != (Stats{Ready: 2}) {
		t.Fatalf("after reopen, stats %+v, want two ready jobs", got)
	}

	// The unacked job comes back first, in enqueue order, and remembers
	// that it has been delivered before.
	job, err := reopened.Lease(time.Minute)
	if err != nil {
		t.Fatalf("Lease after reopen: %v", err)
	}
	if job.ID != leaked.ID {
		t.Fatalf("first job after reopen is %d, want the unacked %d", job.ID, leaked.ID)
	}
	if string(job.Payload) != "two" {
		t.Fatalf("payload after reopen %q, want %q", job.Payload, "two")
	}
	if job.Deliveries != 2 {
		t.Fatalf("redelivery after reopen reported Deliveries=%d, want 2", job.Deliveries)
	}
	if job.LeaseID == leaked.LeaseID {
		t.Fatalf("reopened queue reused lease id %d", job.LeaseID)
	}

	next, err := reopened.Lease(time.Minute)
	if err != nil {
		t.Fatalf("Lease after reopen: %v", err)
	}
	if string(next.Payload) != "three" {
		t.Fatalf("second job after reopen %q, want %q", next.Payload, "three")
	}

	// New jobs get fresh ids rather than colliding with replayed ones.
	id, err := reopened.Enqueue([]byte("four"))
	if err != nil {
		t.Fatalf("Enqueue after reopen: %v", err)
	}
	if id <= next.ID {
		t.Fatalf("Enqueue after reopen reused id %d, want greater than %d", id, next.ID)
	}
}

func TestReopenOfMissingLogIsEmpty(t *testing.T) {
	q := openQueue(t, filepath.Join(t.TempDir(), "does-not-exist.log"))

	if got := q.Stats(); got != (Stats{}) {
		t.Fatalf("stats of a fresh queue %+v, want empty", got)
	}
}

func TestClosedQueueRejectsOperations(t *testing.T) {
	q, err := OpenQueue(filepath.Join(t.TempDir(), "queue.log"))
	if err != nil {
		t.Fatalf("OpenQueue: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := q.Enqueue([]byte("job")); !errors.Is(err, ErrClosed) {
		t.Errorf("Enqueue after Close: %v, want ErrClosed", err)
	}
	if _, err := q.Lease(time.Minute); !errors.Is(err, ErrClosed) {
		t.Errorf("Lease after Close: %v, want ErrClosed", err)
	}
	if err := q.Ack(&Job{ID: 1}); !errors.Is(err, ErrClosed) {
		t.Errorf("Ack after Close: %v, want ErrClosed", err)
	}
}

func TestLeaseRejectsNonPositiveDuration(t *testing.T) {
	q := openQueue(t, filepath.Join(t.TempDir(), "queue.log"))

	if _, err := q.Enqueue([]byte("job")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.Lease(0); err == nil {
		t.Fatal("Lease(0) returned nil error, want an error")
	}
}

func TestConcurrentConsumers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.log")
	q := openQueue(t, path)

	const (
		producers = 4
		perQueue  = 50
		consumers = 8
		total     = producers * perQueue
	)

	var produce sync.WaitGroup
	for p := 0; p < producers; p++ {
		produce.Add(1)
		go func(p int) {
			defer produce.Done()
			for i := 0; i < perQueue; i++ {
				if _, err := q.Enqueue([]byte(fmt.Sprintf("job %d-%d", p, i))); err != nil {
					t.Errorf("Enqueue: %v", err)
					return
				}
			}
		}(p)
	}

	var (
		acked   int64
		mu      sync.Mutex
		seen    = make(map[uint64]int)
		consume sync.WaitGroup
	)
	// The lease is long enough that nothing expires while the test runs, so
	// with correct locking every job is delivered exactly once.
	deadline := time.Now().Add(30 * time.Second)
	for c := 0; c < consumers; c++ {
		consume.Add(1)
		go func() {
			defer consume.Done()
			for atomic.LoadInt64(&acked) < total {
				if time.Now().After(deadline) {
					t.Error("consumers did not drain the queue in time")
					return
				}
				job, err := q.Lease(time.Minute)
				if errors.Is(err, ErrEmpty) {
					runtime.Gosched()
					continue
				}
				if err != nil {
					t.Errorf("Lease: %v", err)
					return
				}
				if err := q.Ack(job); err != nil {
					t.Errorf("Ack(%d): %v", job.ID, err)
					return
				}
				mu.Lock()
				seen[job.ID]++
				mu.Unlock()
				atomic.AddInt64(&acked, 1)
			}
		}()
	}

	produce.Wait()
	consume.Wait()

	if len(seen) != total {
		t.Fatalf("acked %d distinct jobs, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %d was delivered and acked %d times, want 1", id, n)
		}
	}
	if got := q.Stats(); got != (Stats{}) {
		t.Fatalf("after draining, stats %+v, want an empty queue", got)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openQueue(t, path)
	if got := reopened.Stats(); got != (Stats{}) {
		t.Fatalf("after reopening a drained queue, stats %+v, want empty", got)
	}
}
