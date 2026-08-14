package spool

import (
	"path/filepath"
	"testing"
	"time"
)

// countSyncs wraps a log's sync function with a counter, returning a pointer to
// the running total. It keeps the real fsync so behaviour is unchanged.
func countSyncs(l *Log) *int {
	var n int
	orig := l.syncFn
	l.syncFn = func() error {
		n++
		return orig()
	}
	return &n
}

func TestSyncEveryAppendSyncsEachAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	l, err := Open(path) // default policy is SyncEveryAppend
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	syncs := countSyncs(l)

	for i, rec := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		if err := l.Append(rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if want := i + 1; *syncs != want {
			t.Fatalf("after %d appends, synced %d times, want %d", want, *syncs, want)
		}
	}

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if *syncs != 4 {
		t.Fatalf("Close did not flush: synced %d times, want 4", *syncs)
	}
}

func TestSyncEveryDefersWithinInterval(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	clock := newFakeClock()

	l, err := Open(path, WithSyncPolicy(SyncEvery(time.Second)), withClock(clock))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	syncs := countSyncs(l)

	// The first append seeds the interval window; it does not sync.
	if err := l.Append([]byte("a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if *syncs != 0 {
		t.Fatalf("first append synced %d times, want 0", *syncs)
	}

	// Still inside the interval, so a second append does not sync either.
	if err := l.Append([]byte("b")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if *syncs != 0 {
		t.Fatalf("append inside the interval synced %d times, want 0", *syncs)
	}

	// Cross the interval and the next append flushes both pending records.
	clock.Advance(time.Second)
	if err := l.Append([]byte("c")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if *syncs != 1 {
		t.Fatalf("append after the interval synced %d times, want 1", *syncs)
	}

	// The window restarts from that sync: appends totalling less than the
	// interval do not sync again.
	clock.Advance(400 * time.Millisecond)
	if err := l.Append([]byte("d")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	clock.Advance(400 * time.Millisecond)
	if err := l.Append([]byte("e")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if *syncs != 1 {
		t.Fatalf("appends within a fresh window synced %d times, want 1", *syncs)
	}

	// Once the interval elapses since the last sync, the next append syncs.
	clock.Advance(200 * time.Millisecond)
	if err := l.Append([]byte("f")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if *syncs != 2 {
		t.Fatalf("append at the interval boundary synced %d times, want 2", *syncs)
	}

	// Close flushes whatever the interval policy left in the page cache, so a
	// clean shutdown is durable no matter the policy.
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if *syncs != 3 {
		t.Fatalf("Close did not flush the tail: synced %d times, want 3", *syncs)
	}
}

func TestQueueSyncsEveryAppendByDefault(t *testing.T) {
	q, err := OpenQueue(filepath.Join(t.TempDir(), "queue.log"))
	if err != nil {
		t.Fatalf("OpenQueue: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	syncs := countSyncs(q.log)
	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue([]byte("job")); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if *syncs != 3 {
		t.Fatalf("default queue synced %d times over 3 enqueues, want 3", *syncs)
	}
}

func TestQueueHonoursSyncEvery(t *testing.T) {
	clock := newFakeClock()
	q, err := OpenQueue(
		filepath.Join(t.TempDir(), "queue.log"),
		WithClock(clock),
		WithSync(SyncEvery(time.Second)),
	)
	if err != nil {
		t.Fatalf("OpenQueue: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	syncs := countSyncs(q.log)

	// The queue passes its clock to the log, so enqueues within the interval
	// batch under one deferred sync.
	if _, err := q.Enqueue([]byte("first")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.Enqueue([]byte("second")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if *syncs != 0 {
		t.Fatalf("enqueues inside the interval synced %d times, want 0", *syncs)
	}

	clock.Advance(time.Second)
	if _, err := q.Enqueue([]byte("third")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if *syncs != 1 {
		t.Fatalf("enqueue after the interval synced %d times, want 1", *syncs)
	}
}
