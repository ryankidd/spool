package spool

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// defaultMaxSegmentBytes is the size a segment may reach before the next append
// rolls to a fresh one. It is large enough that a queue of small jobs writes
// many records per segment, so rotation is rare relative to appends.
const defaultMaxSegmentBytes = 4 << 20 // 4 MiB

const (
	segmentSuffix = ".seg"
	// segmentDigits zero-pads the sequence number so a lexical sort of the
	// file names matches their numeric order.
	segmentDigits = 20
	manifestName  = "manifest"
	manifestTmp   = "manifest.tmp"
)

// segmentedLog is an append-only log split across a directory of numbered
// segment files. Records are never split across a segment boundary, so each
// segment is a self-framed run of the same length-prefixed, CRC32-checked
// records a single Log holds, and replaying the segments in sequence order is
// exactly replaying one long log.
//
// Only the highest-numbered segment is open for appending; the rest are
// sealed and immutable. A sealed segment can be dropped once the log no longer
// needs it to reconstruct state — see dropThrough and the queue's reclamation
// policy. The manifest records the lowest live segment so a crash midway
// through dropping a prefix can be recovered without ambiguity.
type segmentedLog struct {
	dir     string
	clock   Clock
	policy  SyncPolicy
	maxSize int64

	active      *Log
	activeSeq   uint64
	activeBytes int64

	// floor is the lowest segment sequence that is still live: segments below
	// it have been dropped and are not read on replay. It comes from the
	// manifest and only ever rises.
	floor uint64
}

// segmentName returns the file name for a segment sequence number.
func segmentName(seq uint64) string {
	return fmt.Sprintf("%0*d%s", segmentDigits, seq, segmentSuffix)
}

// segmentPath returns the full path of a segment within the log directory.
func (s *segmentedLog) segmentPath(seq uint64) string {
	return filepath.Join(s.dir, segmentName(seq))
}

// listSegments returns the sequence numbers of the segment files in dir, in
// ascending order.
func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("spool: read log dir %s: %w", dir, err)
	}
	var seqs []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, segmentSuffix) {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)
		if err != nil {
			continue
		}
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

// newSegmentedLog opens (creating if necessary) the segmented log rooted at
// dir. It replays every live segment in order, passing each record's payload
// to onRecord, and calls onSeal after each fully sealed segment with that
// segment's sequence number — the caller uses that boundary to record how much
// live state the segment closed over. The newest segment is left open for
// appending, with any torn trailing record truncated first, exactly as a
// single Log recovers.
func newSegmentedLog(dir string, clock Clock, policy SyncPolicy, maxSize int64, onRecord func([]byte) error, onSeal func(uint64)) (*segmentedLog, error) {
	if maxSize <= 0 {
		maxSize = defaultMaxSegmentBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("spool: create log dir %s: %w", dir, err)
	}

	floor, err := readManifest(dir)
	if err != nil {
		return nil, err
	}

	s := &segmentedLog{dir: dir, clock: clock, policy: policy, maxSize: maxSize, floor: floor}

	seqs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}

	// Segments below the floor are the remains of a compaction that was
	// interrupted before it finished deleting them. They are never read, so
	// clear them out now and fsync the directory once if anything changed.
	var removedOrphan bool
	live := seqs[:0:0]
	for _, seq := range seqs {
		if seq < floor {
			if err := os.Remove(s.segmentPath(seq)); err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("spool: remove stale segment %d: %w", seq, err)
			}
			removedOrphan = true
			continue
		}
		live = append(live, seq)
	}
	if removedOrphan {
		if err := fsyncDir(dir); err != nil {
			return nil, err
		}
	}

	if err := s.replay(live, onRecord, onSeal); err != nil {
		return nil, err
	}

	if err := s.openActive(live); err != nil {
		return nil, err
	}
	return s, nil
}

// replay reads the live segments in order, applying every record to onRecord
// and reporting each sealed segment boundary to onSeal. Only the last segment
// may end in a torn write; a short or corrupt tail on any earlier segment is
// real corruption, since a sealed segment was completely written before the
// next one was created, and is reported as an error rather than silently
// dropped.
func (s *segmentedLog) replay(live []uint64, onRecord func([]byte) error, onSeal func(uint64)) error {
	for i, seq := range live {
		path := s.segmentPath(seq)
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("spool: open segment %d: %w", seq, err)
		}
		valid, err := scanRecords(f, onRecord)
		if err != nil {
			f.Close()
			return err
		}
		info, statErr := f.Stat()
		f.Close()
		if statErr != nil {
			return fmt.Errorf("spool: stat segment %d: %w", seq, statErr)
		}

		if i < len(live)-1 {
			// A sealed segment must end exactly at its last intact record.
			// Trailing bytes mean a record was left torn inside the log, not
			// at its physical tail, which would drop already-durable records.
			if info.Size() != valid {
				return fmt.Errorf("spool: sealed segment %d has %d trailing bytes past the last intact record", seq, info.Size()-valid)
			}
			if onSeal != nil {
				onSeal(seq)
			}
		}
	}
	return nil
}

// openActive opens the newest live segment for appending, or creates the first
// segment when the log is empty. Opening truncates a torn trailing record, so
// the active segment is clean before the first append.
func (s *segmentedLog) openActive(live []uint64) error {
	if len(live) == 0 {
		seq := uint64(1)
		if s.floor > seq {
			seq = s.floor
		}
		l, err := Open(s.segmentPath(seq), WithSyncPolicy(s.policy), withClock(s.clock))
		if err != nil {
			return err
		}
		if err := fsyncDir(s.dir); err != nil {
			l.Close()
			return err
		}
		s.active, s.activeSeq, s.activeBytes = l, seq, 0
		return nil
	}

	seq := live[len(live)-1]
	l, err := Open(s.segmentPath(seq), WithSyncPolicy(s.policy), withClock(s.clock))
	if err != nil {
		return err
	}
	info, err := os.Stat(s.segmentPath(seq))
	if err != nil {
		l.Close()
		return fmt.Errorf("spool: stat active segment %d: %w", seq, err)
	}
	s.active, s.activeSeq, s.activeBytes = l, seq, info.Size()
	return nil
}

// recordSize is the number of bytes a record of the given payload length
// occupies on disk: the 8-byte length+CRC header plus the payload.
func recordSize(payloadLen int) int64 {
	return 8 + int64(payloadLen)
}

// append writes data to the active segment and rolls to a new segment once the
// active one reaches the size threshold. The record always lands whole in the
// segment that was active at entry; rotation only prepares the next segment for
// the following append. When a rotation happens it returns rotated=true and the
// sequence number of the segment that was just sealed.
func (s *segmentedLog) append(data []byte) (rotated bool, sealedSeq uint64, err error) {
	if err := s.active.Append(data); err != nil {
		return false, 0, err
	}
	s.activeBytes += recordSize(len(data))
	if s.activeBytes < s.maxSize {
		return false, 0, nil
	}

	sealed := s.activeSeq
	newSeq := s.activeSeq + 1
	next, err := Open(s.segmentPath(newSeq), WithSyncPolicy(s.policy), withClock(s.clock))
	if err != nil {
		return false, 0, err
	}
	// Make the new segment's directory entry durable before it holds any
	// record: under the every-append policy a later append fsyncs the record's
	// bytes, but that is worthless if a crash can lose the file naming them.
	if err := fsyncDir(s.dir); err != nil {
		next.Close()
		return false, 0, err
	}
	// Sealing flushes the old segment so it is fully durable once retired.
	if err := s.active.Close(); err != nil {
		next.Close()
		return false, 0, err
	}
	s.active, s.activeSeq, s.activeBytes = next, newSeq, 0
	return true, sealed, nil
}

// dropThrough drops every live segment up to and including seq, which must name
// a sealed segment the caller has determined the log no longer needs. It first
// records the new floor in the manifest with a durable atomic rename, so the
// drop is committed before any file is unlinked: a crash after the manifest is
// written but before the files are gone leaves recoverable orphans that the
// next open clears, never a hole in the live range.
func (s *segmentedLog) dropThrough(seq uint64) error {
	if seq >= s.activeSeq {
		return fmt.Errorf("spool: refusing to drop active segment %d", seq)
	}
	newFloor := seq + 1
	if newFloor <= s.floor {
		return nil
	}
	if err := writeManifest(s.dir, newFloor); err != nil {
		return err
	}
	// The manifest is the commit point: from here the segments below newFloor
	// are logically gone whether or not the unlinks complete.
	start := s.floor
	if start == 0 {
		start = 1
	}
	s.floor = newFloor
	for x := start; x <= seq; x++ {
		if err := os.Remove(s.segmentPath(x)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("spool: remove dropped segment %d: %w", x, err)
		}
	}
	return fsyncDir(s.dir)
}

// close flushes and closes the active segment. Sealed segments are already
// closed and durable.
func (s *segmentedLog) close() error {
	return s.active.Close()
}

// readManifest returns the log's floor — the lowest live segment sequence. A
// missing manifest means nothing has been dropped yet and every segment is
// live, reported as floor 0.
func readManifest(dir string) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("spool: read manifest: %w", err)
	}
	if len(b) < 8 {
		return 0, fmt.Errorf("spool: manifest is %d bytes, too short", len(b))
	}
	return binary.BigEndian.Uint64(b[:8]), nil
}

// writeManifest durably records floor. It writes a temp file, fsyncs it,
// renames it over the manifest (atomic on POSIX), then fsyncs the directory so
// the rename itself is durable. A crash at any point leaves either the old or
// the new manifest intact, never a partial one.
func writeManifest(dir string, floor uint64) error {
	tmp := filepath.Join(dir, manifestTmp)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], floor)

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("spool: open manifest temp: %w", err)
	}
	if _, err := f.Write(buf[:]); err != nil {
		f.Close()
		return fmt.Errorf("spool: write manifest temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("spool: sync manifest temp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("spool: close manifest temp: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, manifestName)); err != nil {
		return fmt.Errorf("spool: rename manifest: %w", err)
	}
	return fsyncDir(dir)
}

// fsyncDir flushes a directory's own metadata, making a create, rename or
// unlink within it durable.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("spool: open dir %s to sync: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("spool: sync dir %s: %w", dir, err)
	}
	return nil
}
