// Package spool implements a durable job queue on top of an append-only
// write-ahead log. Log holds the raw log primitive; Queue layers enqueue,
// lease and ack semantics over it.
package spool

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

// SyncPolicy decides when a Log forces its appended records to stable storage
// with fsync. A write that has only reached the OS page cache survives the
// writing process being killed, but not a power loss or kernel panic — fsync
// is what closes that gap, at the cost of a disk round-trip per sync.
//
// The zero value is not a valid policy; build one with SyncEveryAppend or
// SyncEvery.
type SyncPolicy struct {
	// everyAppend syncs after each Append. When false the policy is
	// interval-based.
	everyAppend bool
	// interval is the minimum time between syncs for the interval policy.
	interval time.Duration
}

// SyncEveryAppend returns the strongest policy: every Append is followed by an
// fsync before it returns, so a record that Append reported as written has
// reached stable storage and survives a power loss. This is the default and
// the slowest, since each append pays a disk round-trip.
func SyncEveryAppend() SyncPolicy {
	return SyncPolicy{everyAppend: true}
}

// SyncEvery returns a looser policy that syncs at most once per interval,
// checked against the log's clock as appends happen. It trades durability for
// throughput: between syncs, and after the last append during an idle period,
// records sit in the page cache and a power loss discards them — a bounded
// tail of un-synced writes, not the whole log. A non-positive interval syncs
// on every append, matching SyncEveryAppend.
//
// The check is lazy: a sync only fires on an Append that finds the interval
// has elapsed since the last one, so there is no background goroutine and no
// timer. A single append followed by a long silence is therefore not synced
// until the next append or Close — see Close, which always flushes.
func SyncEvery(interval time.Duration) SyncPolicy {
	if interval < 0 {
		interval = 0
	}
	return SyncPolicy{interval: interval}
}

// LogOption configures a Log at Open time.
type LogOption func(*Log)

// WithSyncPolicy sets the policy deciding when the log fsyncs. The default,
// used when this option is absent, is SyncEveryAppend.
func WithSyncPolicy(p SyncPolicy) LogOption {
	return func(l *Log) { l.policy = p }
}

// withClock sets the clock the interval sync policy consults. It is
// unexported because only the interval policy uses it and the queue passes its
// own clock down; direct callers get the system clock and rarely need another.
func withClock(c Clock) LogOption {
	return func(l *Log) { l.clock = c }
}

// Log is an append-only write-ahead log backed by a file. Each record is
// stored as a 4-byte big-endian length, a 4-byte CRC32 checksum of the
// payload, then the payload itself.
type Log struct {
	f      *os.File
	clock  Clock
	policy SyncPolicy

	// syncFn performs the actual fsync. It defaults to f.Sync and is a field
	// so tests can observe how often the policy triggers a sync.
	syncFn func() error
	// lastSync is when the interval policy last synced; started reports
	// whether it has been set, so the first append can seed the window
	// without treating a zero time as long ago.
	lastSync time.Time
	started  bool
}

// Open opens (creating if necessary) the log file at path for appending.
//
// If the log ends in a torn write, Open truncates the file back to the end
// of the last intact record before accepting any append. Without that, the
// next Append would write past the partial record and cement it in the
// middle of the log, where Replay can no longer tell a crash artifact from
// real corruption — so the damage would only become visible on the reopen
// after next. Mid-log corruption is reported here rather than truncated
// away, since discarding it would throw out valid records behind it.
//
// By default every Append is fsynced before it returns; pass WithSyncPolicy to
// trade that for a looser interval policy. See SyncPolicy.
func Open(path string, opts ...LogOption) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("spool: open %s: %w", path, err)
	}

	valid, err := scanRecords(f, nil)
	if err != nil {
		f.Close()
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("spool: stat %s: %w", path, err)
	}
	if info.Size() > valid {
		if err := f.Truncate(valid); err != nil {
			f.Close()
			return nil, fmt.Errorf("spool: truncate %s to %d: %w", path, valid, err)
		}
	}

	l := &Log{f: f, clock: systemClock{}, policy: SyncEveryAppend()}
	l.syncFn = l.f.Sync
	for _, opt := range opts {
		opt(l)
	}
	return l, nil
}

// Close flushes any writes the sync policy has not yet forced to disk and then
// closes the underlying file. A clean Close is therefore durable regardless of
// policy: the loose interval policy only leaves a tail un-synced across a
// crash, never across an orderly shutdown.
func (l *Log) Close() error {
	syncErr := l.syncFn()
	closeErr := l.f.Close()
	if syncErr != nil {
		return fmt.Errorf("spool: sync log on close: %w", syncErr)
	}
	return closeErr
}

// Append writes data to the log as a new record, then fsyncs according to the
// log's sync policy.
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
	return l.maybeSync()
}

// maybeSync fsyncs the log if the sync policy calls for it after this append.
// Under the every-append policy it always syncs; under the interval policy the
// first append seeds the window and later appends sync only once the interval
// has elapsed since the last sync. It is not safe for concurrent callers, but
// neither is Append: the queue serializes both under its own lock.
func (l *Log) maybeSync() error {
	now := l.clock.Now()
	if !l.policy.everyAppend {
		if !l.started {
			l.started = true
			l.lastSync = now
			return nil
		}
		if now.Sub(l.lastSync) < l.policy.interval {
			return nil
		}
	}
	if err := l.syncFn(); err != nil {
		return fmt.Errorf("spool: sync log: %w", err)
	}
	l.lastSync = now
	l.started = true
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

	_, err = scanRecords(f, fn)
	return err
}

// scanRecords walks the log from the current offset of f, calling fn (which
// may be nil) with each intact record's payload, and returns the offset just
// past the last intact record. A trailing partial or corrupt record stops the
// scan without an error; corruption with valid log after it is an error.
func scanRecords(f *os.File, fn func(data []byte) error) (int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("spool: seek to start: %w", err)
	}

	var valid int64
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(f, header); err != nil {
			if err == io.EOF {
				// Clean boundary: the last record ended exactly at EOF.
				return valid, nil
			}
			if err == io.ErrUnexpectedEOF {
				// A partial header can only happen if the file physically
				// ends here — a torn write from a killed process. Discard
				// it and keep everything read so far.
				return valid, nil
			}
			return valid, fmt.Errorf("spool: read record header: %w", err)
		}

		length := binary.BigEndian.Uint32(header[0:4])
		wantCRC := binary.BigEndian.Uint32(header[4:8])

		data := make([]byte, length)
		if _, err := io.ReadFull(f, data); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Same reasoning as the header case: a truncated payload
				// only occurs at the physical end of the file.
				return valid, nil
			}
			return valid, fmt.Errorf("spool: read record payload: %w", err)
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
				return valid, nil
			}
			return valid, fmt.Errorf("spool: checksum mismatch mid-log (not at tail): want %x got %x", wantCRC, gotCRC)
		}
		if fn != nil {
			if err := fn(data); err != nil {
				return valid, err
			}
		}
		valid += int64(len(header)) + int64(length)
	}
}
