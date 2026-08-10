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
				return nil
			}
			return fmt.Errorf("spool: read record header: %w", err)
		}

		length := binary.BigEndian.Uint32(header[0:4])
		wantCRC := binary.BigEndian.Uint32(header[4:8])

		data := make([]byte, length)
		if _, err := io.ReadFull(f, data); err != nil {
			return fmt.Errorf("spool: read record payload: %w", err)
		}
		if gotCRC := crc32.ChecksumIEEE(data); gotCRC != wantCRC {
			return fmt.Errorf("spool: checksum mismatch: want %x got %x", wantCRC, gotCRC)
		}
		if err := fn(data); err != nil {
			return err
		}
	}
}
