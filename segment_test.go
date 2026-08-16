package spool

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

// segmentSeqs returns the sequence numbers of the segment files present in a
// log directory, ascending.
func segmentSeqs(t *testing.T, dir string) []uint64 {
	t.Helper()
	seqs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	return seqs
}

func TestSegmentedLogRotatesAndReplays(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "log")

	var records [][]byte
	for i := 0; i < 9; i++ {
		records = append(records, []byte(fmt.Sprintf("record-%02d", i)))
	}

	// A 40-byte cap against ~17-byte records rolls a new segment every few
	// appends, so nine records span several segments.
	sl, err := newSegmentedLog(dir, systemClock{}, SyncEveryAppend(), 40, func([]byte) error { return nil }, nil)
	if err != nil {
		t.Fatalf("newSegmentedLog: %v", err)
	}
	for _, r := range records {
		if _, _, err := sl.append(r); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := sl.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	seqs := segmentSeqs(t, dir)
	if len(seqs) < 2 {
		t.Fatalf("expected several segments, got %d", len(seqs))
	}

	// Replaying the whole directory returns every record, in append order,
	// across all the segments, and reports each sealed segment as it goes —
	// every segment but the active (last) one.
	var got [][]byte
	var replaySeals []uint64
	sl2, err := newSegmentedLog(dir, systemClock{}, SyncEveryAppend(), 40, func(d []byte) error {
		got = append(got, append([]byte(nil), d...))
		return nil
	}, func(seq uint64) { replaySeals = append(replaySeals, seq) })
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := sl2.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !reflect.DeepEqual(got, records) {
		t.Fatalf("replay across segments returned %q, want %q", got, records)
	}
	if !reflect.DeepEqual(replaySeals, seqs[:len(seqs)-1]) {
		t.Fatalf("replay reported sealed segments %v, want all but the active %v", replaySeals, seqs[:len(seqs)-1])
	}
}

func TestDropThroughAdvancesFloorAndSweeps(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "log")

	sl, err := newSegmentedLog(dir, systemClock{}, SyncEveryAppend(), 40, func([]byte) error { return nil }, nil)
	if err != nil {
		t.Fatalf("newSegmentedLog: %v", err)
	}
	for i := 0; i < 9; i++ {
		if _, _, err := sl.append([]byte(fmt.Sprintf("record-%02d", i))); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	seqs := segmentSeqs(t, dir)
	if len(seqs) < 3 {
		t.Fatalf("want at least three segments to drop through, got %v", seqs)
	}
	target := seqs[0] // drop the oldest sealed segment
	if err := sl.dropThrough(target); err != nil {
		t.Fatalf("dropThrough: %v", err)
	}
	if err := sl.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, segmentName(target))); !os.IsNotExist(err) {
		t.Fatalf("dropped segment %d still present: %v", target, err)
	}
	floor, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if floor != target+1 {
		t.Fatalf("manifest floor = %d, want %d", floor, target+1)
	}

	// dropThrough refuses to drop the active segment.
	if err := sl.dropThrough(sl.activeSeq); err == nil {
		t.Fatal("dropThrough of the active segment returned nil, want an error")
	}
}

func TestDropThroughOrphansAreSweptOnOpen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "log")
	sl, err := newSegmentedLog(dir, systemClock{}, SyncEveryAppend(), 40, func([]byte) error { return nil }, nil)
	if err != nil {
		t.Fatalf("newSegmentedLog: %v", err)
	}
	for i := 0; i < 6; i++ {
		if _, _, err := sl.append([]byte(fmt.Sprintf("record-%02d", i))); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := sl.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate a compaction that committed its manifest but crashed before
	// unlinking: a segment file sits below a floor pointing past it.
	if err := writeManifest(dir, 2); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	// Segment 1 is now an orphan below the floor of 2.
	if _, err := os.Stat(filepath.Join(dir, segmentName(1))); err != nil {
		t.Fatalf("expected segment 1 to exist: %v", err)
	}

	sl2, err := newSegmentedLog(dir, systemClock{}, SyncEveryAppend(), 40, func([]byte) error { return nil }, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := sl2.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, segmentName(1))); !os.IsNotExist(err) {
		t.Fatalf("orphan segment below the floor was not swept: %v", err)
	}
}

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if floor, err := readManifest(dir); err != nil || floor != 0 {
		t.Fatalf("readManifest of empty dir = (%d, %v), want (0, nil)", floor, err)
	}
	for _, want := range []uint64{1, 42, 1 << 40} {
		if err := writeManifest(dir, want); err != nil {
			t.Fatalf("writeManifest(%d): %v", want, err)
		}
		got, err := readManifest(dir)
		if err != nil {
			t.Fatalf("readManifest: %v", err)
		}
		if got != want {
			t.Fatalf("manifest round trip = %d, want %d", got, want)
		}
	}
}

func TestSegmentNamesSortBySequence(t *testing.T) {
	// Lexical order of the file names must match numeric sequence order, or a
	// replay would stitch segments together out of order.
	names := []string{segmentName(2), segmentName(10), segmentName(1)}
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	want := []string{segmentName(1), segmentName(2), segmentName(10)}
	if !reflect.DeepEqual(sorted, want) {
		t.Fatalf("sorted segment names %v, want %v", sorted, want)
	}
}
