package spool

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestAppendReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	records := [][]byte{
		[]byte("first record"),
		[]byte("second record"),
		[]byte(""),
		[]byte("fourth record, after an empty one"),
	}
	for _, r := range records {
		if err := l.Append(r); err != nil {
			t.Fatalf("Append(%q): %v", r, err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var got [][]byte
	err = Replay(path, func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		got = append(got, cp)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if !reflect.DeepEqual(got, records) {
		t.Fatalf("Replay returned %q, want %q", got, records)
	}
}

func TestReplayTruncatedHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	records := [][]byte{[]byte("first"), []byte("second")}
	for _, r := range records {
		if err := l.Append(r); err != nil {
			t.Fatalf("Append(%q): %v", r, err)
		}
	}
	beforeThird := fileSize(t, path)
	if err := l.Append([]byte("third")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-Append: only the first 3 of the third record's
	// 8-byte header made it to disk before the process died.
	if err := os.Truncate(path, beforeThird+3); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	var got [][]byte
	err = Replay(path, func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		got = append(got, cp)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !reflect.DeepEqual(got, records) {
		t.Fatalf("Replay returned %q, want %q", got, records)
	}
}

func TestReplayTruncatedPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	records := [][]byte{[]byte("first"), []byte("second")}
	for _, r := range records {
		if err := l.Append(r); err != nil {
			t.Fatalf("Append(%q): %v", r, err)
		}
	}
	// A third record whose payload write never completes.
	if err := l.Append([]byte("third record, long enough to truncate mid-payload")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	full := fileSize(t, path)
	if err := os.Truncate(path, full-10); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	var got [][]byte
	err = Replay(path, func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		got = append(got, cp)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !reflect.DeepEqual(got, records) {
		t.Fatalf("Replay returned %q, want %q", got, records)
	}
}

func TestReplayCorruptTrailingRecordDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	records := [][]byte{[]byte("first"), []byte("second")}
	for _, r := range records {
		if err := l.Append(r); err != nil {
			t.Fatalf("Append(%q): %v", r, err)
		}
	}
	if err := l.Append([]byte("third")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Flip a bit in the last record's payload without touching its length
	// or checksum, simulating a write that landed corrupted on disk.
	flipLastByte(t, path)

	var got [][]byte
	err = Replay(path, func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		got = append(got, cp)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !reflect.DeepEqual(got, records) {
		t.Fatalf("Replay returned %q, want %q", got, records)
	}
}

func TestReplayCorruptMidLogRecordFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := l.Append([]byte("first")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	offset := fileSize(t, path)
	if err := l.Append([]byte("second")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Append([]byte("third")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt "second"'s payload (which is followed by "third", still
	// intact) by flipping a bit partway through the file rather than at
	// the very end.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	// offset points at the start of the second record's header; the
	// payload starts 8 bytes in.
	if _, err := f.WriteAt([]byte{0xFF}, offset+8); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err = Replay(path, func([]byte) error { return nil })
	if err == nil {
		t.Fatal("Replay of mid-log corruption returned nil error, want an error")
	}
}

func flipLastByte(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	size := fileSize(t, path)
	var b [1]byte
	if _, err := f.ReadAt(b[:], size-1); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], size-1); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	return info.Size()
}

func TestReplayMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.log")

	var calls int
	if err := Replay(path, func([]byte) error { calls++; return nil }); err != nil {
		t.Fatalf("Replay on missing file: %v", err)
	}
	if calls != 0 {
		t.Fatalf("Replay on missing file called fn %d times, want 0", calls)
	}
}
