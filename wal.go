// Package spool implements an append-only write-ahead log.
package spool

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Log is an append-only write-ahead log backed by a file. Each record is
// stored as a 4-byte big-endian length, a 4-byte CRC32 checksum of the
// payload, then the payload itself.
type Log struct {
	f *os.File
}

// Open opens (creating if necessary) the log file at path for appending.
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("spool: open %s: %w", path, err)
	}
	return &Log{f: f}, nil
}

// Close closes the underlying log file.
func (l *Log) Close() error {
	return l.f.Close()
}

// Append writes data to the log as a new record.
func (l *Log) Append(data []byte) error {
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(data)))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(data))

	if _, err := l.f.Write(header); err != nil {
		return fmt.Errorf("spool: write record header: %w", err)
	}
	if _, err := l.f.Write(data); err != nil {
		return fmt.Errorf("spool: write record payload: %w", err)
	}
	return nil
}

// Replay reads every record from the log file at path, in the order they
// were appended, calling fn with each record's payload. A missing file is
// treated as an empty log, not an error.
//
// A torn write — the process was killed partway through Append — leaves a
// truncated header or payload at the physical end of the file. Replay
// discards that trailing partial record and returns everything before it
// with no error, rather than failing the whole replay over an in-progress
// write that never completed.
func Replay(path string, fn func(data []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("spool: open %s: %w", path, err)
	}
	defer f.Close()

	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(f, header); err != nil {
			if err == io.EOF {
				// Clean boundary: the last record ended exactly at EOF.
				return nil
			}
			if err == io.ErrUnexpectedEOF {
				// A partial header can only happen if the file physically
				// ends here — a torn write from a killed process. Discard
				// it and keep everything read so far.
				return nil
			}
			return fmt.Errorf("spool: read record header: %w", err)
		}

		length := binary.BigEndian.Uint32(header[0:4])
		wantCRC := binary.BigEndian.Uint32(header[4:8])

		data := make([]byte, length)
		if _, err := io.ReadFull(f, data); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Same reasoning as the header case: a truncated payload
				// only occurs at the physical end of the file.
				return nil
			}
			return fmt.Errorf("spool: read record payload: %w", err)
		}
		if gotCRC := crc32.ChecksumIEEE(data); gotCRC != wantCRC {
			// A checksum mismatch on the record at the physical end of the
			// file is indistinguishable from a torn write (the header
			// landed on disk but the payload didn't finish, or landed
			// corrupted) and is handled the same way: discard it, return
			// everything before it. But a torn write can only ever leave
			// garbage at the tail, since Append only ever extends the
			// file — it cannot explain a bad checksum with more valid log
			// after it. If more bytes follow, this is real corruption in
			// the middle of the log, not a crash artifact, and silently
			// dropping it would risk losing already-acknowledged records
			// that happen to sit downstream. That case is a hard error.
			var probe [1]byte
			n, _ := f.Read(probe[:])
			if n == 0 {
				return nil
			}
			return fmt.Errorf("spool: checksum mismatch mid-log (not at tail): want %x got %x", wantCRC, gotCRC)
		}
		if err := fn(data); err != nil {
			return err
		}
	}
}
