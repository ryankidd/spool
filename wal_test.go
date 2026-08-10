package spool

import (
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
