# spool

A minimal append-only write-ahead log in Go, meant as the storage layer for
a crash-safe job queue: writes go through the log first, so a `kill -9`
mid-write doesn't silently lose work.

This is an early slice of the project. Right now `spool` provides the raw
log primitive — append records, replay them back in order — and nothing
above it yet. There is no queue API (enqueue/lease/ack), no crash recovery
for a torn last write, and no fsync policy control. See "Status" below for
exactly what that means in practice.

## Install

```sh
go get github.com/ryankidd/spool
```

## Usage

```go
package main

import (
	"fmt"
	"log"

	"github.com/ryankidd/spool"
)

func main() {
	l, err := spool.Open("jobs.wal")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	if err := l.Append([]byte("job-1 payload")); err != nil {
		log.Fatal(err)
	}

	err = spool.Replay("jobs.wal", func(data []byte) error {
		fmt.Printf("record: %s\n", data)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

## On-disk format

Each record is written as:

| field    | size    | meaning                          |
|----------|---------|-----------------------------------|
| length   | 4 bytes | big-endian length of the payload |
| checksum | 4 bytes | CRC32 (IEEE) of the payload       |
| payload  | N bytes | the record itself                |

Records are appended back to back with no separators; `Replay` walks the
file from the start, reading one record at a time.

## Status

What this gives you today:

- Every successfully replayed record is verified against its checksum —
  silent bit-rot in a fully-written record is caught, not returned as data.
- `Replay` on a path that doesn't exist yet returns an empty log, not an
  error, so callers can treat "no log file" and "empty log" the same way.

What this does **not** give you yet:

- No recovery from a torn write. If the process is killed mid-`Append` (log
  file ends partway through a header or payload), `Replay` currently
  returns an error on the truncated trailing record instead of discarding
  it and returning everything before it — that recovery behavior is the
  next thing to build here, not something already in place.
- No fsync — `Append` writes go through the OS page cache like any other
  file write. A power loss (as opposed to a killed process) can still lose
  the last writes that hadn't reached disk. An fsync policy (every write,
  or on an interval) is planned but not implemented.
- No queue semantics. This package is just the log; enqueue/lease/ack and
  redelivery of unacked jobs live on top of it and aren't built yet.

## Test

```sh
go test ./... -race
```
