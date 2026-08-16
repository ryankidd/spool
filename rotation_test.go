package spool

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// activeSegmentPath returns the path of the highest-numbered segment file.
func activeSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	seqs := segmentSeqs(t, dir)
	if len(seqs) == 0 {
		t.Fatalf("no segments in %s", dir)
	}
	return filepath.Join(dir, segmentName(seqs[len(seqs)-1]))
}

// drain leases and acks every ready job, returning the payloads in delivery
// order.
func drain(t *testing.T, q *Queue) []string {
	t.Helper()
	var out []string
	for {
		job, err := q.Lease(time.Minute)
		if err == ErrEmpty {
			return out
		}
		if err != nil {
			t.Fatalf("Lease: %v", err)
		}
		out = append(out, string(job.Payload))
		if err := q.Ack(job); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

// perJobSegment sizes a segment so that one job's enqueue, lease and ack fill
// it exactly, with the ack the record that trips rotation. Because the queue
// is empty the instant the ack lands, that segment seals with zero live jobs
// and is immediately reclaimable.
const perJobSegment = 67

func TestSegmentsRotateAtThreshold(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openQueue(t, dir, WithMaxSegmentBytes(80))

	for i := 0; i < 12; i++ {
		if _, err := q.Enqueue([]byte(fmt.Sprintf("job-%02d", i))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	if got := len(segmentSeqs(t, dir)); got < 2 {
		t.Fatalf("after 12 enqueues into 80-byte segments, %d segment(s), want at least 2", got)
	}
}

func TestReplayAcrossSegmentsMatchesSingleSegment(t *testing.T) {
	// Run the identical op sequence against a heavily rotated log and a
	// single-segment log, then reopen both and confirm they reconstruct the
	// same queue.
	run := func(t *testing.T, maxBytes int64) (Stats, []string) {
		dir := filepath.Join(t.TempDir(), "queue")
		q := openQueue(t, dir, WithMaxSegmentBytes(maxBytes))

		// Enqueue 20, ack the even ids, leave the odd ids leased and unacked.
		for i := 0; i < 20; i++ {
			if _, err := q.Enqueue([]byte(fmt.Sprintf("payload-%02d", i))); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
		}
		for i := 0; i < 20; i++ {
			job, err := q.Lease(time.Minute)
			if err != nil {
				t.Fatalf("Lease: %v", err)
			}
			if i%2 == 0 {
				if err := q.Ack(job); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			}
		}
		if err := q.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		reopened := openQueue(t, dir, WithMaxSegmentBytes(maxBytes))
		return reopened.Stats(), drain(t, reopened)
	}

	rotatedStats, rotatedOrder := run(t, 96)
	singleStats, singleOrder := run(t, defaultMaxSegmentBytes)

	if rotatedStats != singleStats {
		t.Fatalf("stats after reopen: rotated %+v, single %+v", rotatedStats, singleStats)
	}
	if !reflect.DeepEqual(rotatedOrder, singleOrder) {
		t.Fatalf("drain order after reopen differs:\n rotated %q\n single  %q", rotatedOrder, singleOrder)
	}
	// Sanity: the unacked odd payloads came back, in enqueue order.
	want := []string{}
	for i := 1; i < 20; i += 2 {
		want = append(want, fmt.Sprintf("payload-%02d", i))
	}
	if !reflect.DeepEqual(singleOrder, want) {
		t.Fatalf("reopened queue drained %q, want the unacked odd jobs %q", singleOrder, want)
	}
}

func TestFullyAckedPrefixIsReclaimed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openQueue(t, dir, WithMaxSegmentBytes(perJobSegment))

	for i := 0; i < 8; i++ {
		id, err := q.Enqueue(nil)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		job, err := q.Lease(time.Minute)
		if err != nil {
			t.Fatalf("Lease: %v", err)
		}
		if job.ID != id {
			t.Fatalf("leased %d, want %d", job.ID, id)
		}
		if err := q.Ack(job); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}

	// Every sealed segment held one fully-acked job, so all but the active
	// segment should have been reclaimed.
	if got := len(segmentSeqs(t, dir)); got > 1 {
		t.Fatalf("after acking every job, %d segments remain, want the active one only", got)
	}
	if got := q.Stats(); got != (Stats{}) {
		t.Fatalf("stats %+v, want empty", got)
	}

	// The floor moved, so a reopen replays only the live tail and still sees
	// an empty queue — nothing acked was resurrected.
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openQueue(t, dir, WithMaxSegmentBytes(perJobSegment))
	if got := reopened.Stats(); got != (Stats{}) {
		t.Fatalf("after reopen, stats %+v, want empty", got)
	}
}

func TestUnackedJobBlocksReclamation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openQueue(t, dir, WithMaxSegmentBytes(perJobSegment))

	// A single job that is enqueued and leased but never acked. Leasing it
	// takes it off the ready queue with a long deadline, so it stays live and
	// invisible while it pins its segment.
	if _, err := q.Enqueue([]byte("pinned")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pinnedJob, err := q.Lease(time.Hour)
	if err != nil {
		t.Fatalf("Lease pinned: %v", err)
	}
	if string(pinnedJob.Payload) != "pinned" {
		t.Fatalf("leased %q first, want the pinned job", pinnedJob.Payload)
	}

	// Now churn a batch of jobs that are each fully acked. None of their
	// segments can be dropped while the pinned job sits in the oldest one.
	for i := 0; i < 8; i++ {
		if _, err := q.Enqueue(nil); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		job, err := q.Lease(time.Minute)
		if err != nil {
			t.Fatalf("Lease: %v", err)
		}
		if err := q.Ack(job); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}

	// The oldest segment (holding the pinned enqueue) must still be present,
	// so the floor never advanced past it.
	seqs := segmentSeqs(t, dir)
	if len(seqs) == 0 || seqs[0] != 1 {
		t.Fatalf("oldest segment was reclaimed while a job in it is unacked: segments %v", seqs)
	}

	// And the pinned job survives a reopen as ready work.
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openQueue(t, dir, WithMaxSegmentBytes(perJobSegment))
	if got := reopened.Stats(); got != (Stats{Ready: 1}) {
		t.Fatalf("after reopen, stats %+v, want the one pinned job ready", got)
	}
	job, err := reopened.Lease(time.Minute)
	if err != nil {
		t.Fatalf("Lease after reopen: %v", err)
	}
	if string(job.Payload) != "pinned" {
		t.Fatalf("survivor payload %q, want %q", job.Payload, "pinned")
	}
}

func TestReclamationLeavesUnackedTailIntact(t *testing.T) {
	// Older segments fully acked and reclaimed, newer segments holding live
	// work: a reopen must drop nothing live and resurrect nothing acked.
	dir := filepath.Join(t.TempDir(), "queue")
	q := openQueue(t, dir, WithMaxSegmentBytes(perJobSegment))

	// Fully process three jobs, reclaiming their segments.
	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue([]byte(fmt.Sprintf("done-%d", i))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		job, err := q.Lease(time.Minute)
		if err != nil {
			t.Fatalf("Lease: %v", err)
		}
		if err := q.Ack(job); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	// Now enqueue two jobs and leave them unacked in the live tail.
	for _, p := range []string{"live-a", "live-b"} {
		if _, err := q.Enqueue([]byte(p)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openQueue(t, dir, WithMaxSegmentBytes(perJobSegment))
	if got := drain(t, reopened); !reflect.DeepEqual(got, []string{"live-a", "live-b"}) {
		t.Fatalf("after reclaim + reopen drained %q, want the two live jobs only", got)
	}
}

func TestTornWriteAtActiveSegmentTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openQueue(t, dir, WithMaxSegmentBytes(200))

	for _, p := range []string{"a", "b", "c"} {
		if _, err := q.Enqueue([]byte(p)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-append: garbage bytes at the physical tail of the
	// active segment, as a partial record would leave.
	active := activeSegmentPath(t, dir)
	f, err := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 9, 1, 2}); err != nil {
		t.Fatalf("Write torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen truncates the torn tail and recovers everything before it.
	reopened := openQueue(t, dir, WithMaxSegmentBytes(200))
	if got := drain(t, reopened); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("after torn-tail recovery drained %q, want a,b,c", got)
	}
}

func TestSealedSegmentCorruptionIsAnError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openQueue(t, dir, WithMaxSegmentBytes(perJobSegment))

	// Two jobs that stay unacked, so both their segments are pinned and none
	// are reclaimed: the first segment is sealed but still needed.
	for _, p := range []string{"first", "second"} {
		if _, err := q.Enqueue([]byte(p)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if _, err := q.Lease(time.Minute); err != nil {
			t.Fatalf("Lease: %v", err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seqs := segmentSeqs(t, dir)
	if len(seqs) < 2 {
		t.Fatalf("want at least two segments, got %v", seqs)
	}
	// Corrupt a payload byte in the oldest (sealed) segment. It is not the
	// physical tail of the log, so this is real corruption, not a torn write.
	sealed := filepath.Join(dir, segmentName(seqs[0]))
	fh, err := os.OpenFile(sealed, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	// The first record's payload starts at offset 8; flip a byte inside it.
	var b [1]byte
	if _, err := fh.ReadAt(b[:], 8); err != nil {
		fh.Close()
		t.Fatalf("ReadAt: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := fh.WriteAt(b[:], 8); err != nil {
		fh.Close()
		t.Fatalf("WriteAt: %v", err)
	}
	fh.Close()

	if _, err := OpenQueue(dir, WithMaxSegmentBytes(perJobSegment)); err == nil {
		t.Fatal("OpenQueue succeeded on a corrupt sealed segment, want an error")
	}
}

func TestCrashDuringCompactionRecovers(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openQueue(t, dir, WithMaxSegmentBytes(perJobSegment))

	// Reclaim a few segments normally so the floor advances.
	for i := 0; i < 4; i++ {
		if _, err := q.Enqueue(nil); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		job, err := q.Lease(time.Minute)
		if err != nil {
			t.Fatalf("Lease: %v", err)
		}
		if err := q.Ack(job); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	// Leave one live job in the tail so the reopen has something to verify.
	if _, err := q.Enqueue([]byte("survivor")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	floor := q.slog.floor
	if floor == 0 {
		t.Fatalf("expected the floor to advance after acking a batch")
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Recreate the state a crash between the manifest write and the unlinks
	// leaves behind: an orphan segment below the floor. It must be ignored by
	// replay and swept away on open, with no effect on state.
	orphan := filepath.Join(dir, segmentName(floor-1))
	if err := os.WriteFile(orphan, []byte("garbage that would fail a strict replay"), 0o644); err != nil {
		t.Fatalf("WriteFile orphan: %v", err)
	}

	reopened := openQueue(t, dir, WithMaxSegmentBytes(perJobSegment))
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan segment below the floor was not swept: stat err %v", err)
	}
	if got := drain(t, reopened); !reflect.DeepEqual(got, []string{"survivor"}) {
		t.Fatalf("after recovery drained %q, want the survivor only", got)
	}
}
