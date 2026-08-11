package spool

import (
	"encoding/binary"
	"fmt"
)

// recordKind identifies which queue state transition a log record describes.
type recordKind uint8

const (
	kindEnqueue recordKind = 1
	kindLease   recordKind = 2
	kindAck     recordKind = 3
)

// record is a single queue state transition as it is stored in the log. The
// queue's entire state is the fold of these records in log order, so every
// transition that changes what a consumer would observe has to be written
// before it takes effect in memory.
//
// The encoded form is:
//
//	kind    1 byte
//	job     8 bytes, big-endian job id
//	then, for kindEnqueue:
//	payload N bytes, running to the end of the record
//	or, for kindLease and kindAck:
//	lease   8 bytes, big-endian lease id
//
// The payload needs no length prefix because the log already frames records
// and hands back exactly the bytes that were appended.
type record struct {
	kind    recordKind
	job     uint64
	lease   uint64 // kindLease, kindAck
	payload []byte // kindEnqueue
}

const recordHeaderLen = 1 + 8

func (r record) encode() []byte {
	n := recordHeaderLen
	switch r.kind {
	case kindEnqueue:
		n += len(r.payload)
	default:
		n += 8
	}

	b := make([]byte, 0, n)
	b = append(b, byte(r.kind))
	b = binary.BigEndian.AppendUint64(b, r.job)
	if r.kind == kindEnqueue {
		return append(b, r.payload...)
	}
	return binary.BigEndian.AppendUint64(b, r.lease)
}

func decodeRecord(b []byte) (record, error) {
	if len(b) < recordHeaderLen {
		return record{}, fmt.Errorf("spool: record is %d bytes, too short to decode", len(b))
	}

	r := record{
		kind: recordKind(b[0]),
		job:  binary.BigEndian.Uint64(b[1:recordHeaderLen]),
	}
	rest := b[recordHeaderLen:]

	switch r.kind {
	case kindEnqueue:
		r.payload = append([]byte(nil), rest...)
	case kindLease, kindAck:
		if len(rest) != 8 {
			return record{}, fmt.Errorf("spool: record kind %d has %d trailing bytes, want 8", r.kind, len(rest))
		}
		r.lease = binary.BigEndian.Uint64(rest)
	default:
		return record{}, fmt.Errorf("spool: unknown record kind %d", r.kind)
	}
	return r, nil
}
