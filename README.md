# spool

A crash-safe job queue in Go, built on an append-only write-ahead log. Every
state change — enqueue, lease, ack — is written to the log before it takes
effect, so the queue's state is exactly the fold of its log: reopen the
directory and you get the same queue back, minus anything a `kill -9`
interrupted mid-write.

There is no server and no background goroutine. `spool` is a library over a
small directory of append-only log segments, safe for concurrent use within a
single process. Rotating the log into segments is what lets it reclaim the
space acked jobs leave behind, so the directory tracks queue depth rather than
total throughput — see "Rotation and compaction". See "Status" below for
exactly which guarantees are and aren't in place.

## Guarantees

The precise contract, so it can be relied on without reading the rest of the
document. Everything here is qualified in detail below.

**What `spool` promises:**

- **Nothing acknowledged is lost.** Under the default `SyncEveryAppend()`
  policy, once `Enqueue`, `Lease` or `Ack` returns success its record is
  `fsync`ed to disk and survives a power loss or kernel panic, not just a
  killed process.
- **State is the fold of the log.** Reopening the directory replays every
  record and reconstructs the identical queue — ready jobs, in enqueue order,
  with delivery counts intact.
- **At-least-once delivery.** A leased job is invisible to other consumers
  until it is acked or its lease expires; a lease that expires without an ack
  is redelivered.
- **Torn writes are recovered.** A `kill -9` mid-append leaves at most one
  partial trailing record, which `Open` discards before the next write; every
  record before it survives. This is survivable across repeated crash/reopen
  cycles, not just once.
- **Real corruption is reported, not hidden.** A checksum mismatch or short
  read anywhere but the torn tail is returned as a hard error with the file
  left intact, rather than silently dropping data.
- **Growth is bounded by outstanding work.** Sealed segments are reclaimed
  once every job they recorded is acked, so the directory tracks queue depth
  rather than total throughput.

**What it explicitly does not promise:**

- **Not exactly-once.** A job whose lease expires is delivered again whether
  or not the work was done; jobs must be idempotent.
- **`SyncEvery(d)` risks a bounded tail.** The looser policy trades durability
  for throughput: writes made since the last sync are only in the page cache
  and a power loss discards them. It never risks more than that un-synced tail.
- **A never-drained queue keeps growing.** Compaction drops whole obsolete
  segments but never rewrites a live one, so a single never-acked job pins its
  segment and every segment after it.
- **One process per directory.** There is no cross-process locking; two queues
  over the same path interleave their appends and diverge.
- **No nack, dead-letter, retry limit, delay, or priority.** A job that fails
  forever is redelivered forever; `Job.Deliveries` is the hook for a consumer
  to give up on it.

## Install

```sh
go get github.com/ryankidd/spool
```

## Usage

```go
package main

import (
	"errors"
	"log"
	"time"

	"github.com/ryankidd/spool"
)

func main() {
	q, err := spool.OpenQueue("jobs.queue")
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	if _, err := q.Enqueue([]byte("resize avatar")); err != nil {
		log.Fatal(err)
	}

	job, err := q.Lease(30 * time.Second)
	if errors.Is(err, spool.ErrEmpty) {
		return // nothing to do right now
	}
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("job %d, delivery %d: %s", job.ID, job.Deliveries, job.Payload)

	// Ack within the lease, or the job is handed to someone else.
	if err := q.Ack(job); err != nil {
		log.Fatal(err)
	}
}
```

## Queue API

| call | does |
|------|------|
| `OpenQueue(path, opts...)` | Replays the log directory at `path` to rebuild state, then opens the newest segment for appending. A missing directory is an empty queue. |
| `Enqueue(payload) (id, error)` | Durably adds a job and returns its id. The payload is copied. |
| `Lease(d) (*Job, error)` | Hands the oldest ready job to the caller with a deadline `d` from now. Returns `ErrEmpty` if nothing is ready. |
| `Ack(job) error` | Durably completes a job. Returns `ErrLeaseExpired` if that lease is no longer the current delivery. |
| `Stats() Stats` | Counts of ready and leased jobs. Expired leases count as ready. |
| `Close() error` | Closes the log. Later calls return `ErrClosed`. |
| `WithClock(c)` | Option replacing the clock used for lease deadlines — mainly so tests can expire a lease without sleeping. |
| `WithSync(p)` | Option choosing when the log fsyncs: `SyncEveryAppend()` (default) or `SyncEvery(d)` for a looser, higher-throughput policy. See "Durability and fsync". |
| `WithMaxSegmentBytes(n)` | Option setting the size a segment may reach before the log rolls to a new one. A whole sealed segment is the unit of compaction. See "Rotation and compaction". |

A `Job` carries its `ID` (stable across redeliveries), `Payload`, `LeaseID`
(new on every delivery), `Deadline`, and `Deliveries`, the number of times it
has been handed out including this one. `Deliveries > 1` means the job is a
retry.

### Delivery semantics

Delivery is **at least once**, never exactly once:

- A leased job is invisible to other consumers until it is acked or its lease
  expires. Two consumers can't hold the same job at the same time.
- If the lease expires without an ack, the job goes back on the tail of the
  ready queue and is delivered again. A consumer that finished the work but
  died before acking will see that work done twice, so jobs should be
  idempotent.
- An ack after the deadline is rejected with `ErrLeaseExpired` even if no
  other consumer has picked the job up yet. A lease that could be acked late
  wouldn't be a deadline.
- Ordering is FIFO by enqueue for first deliveries. A redelivered job goes to
  the back of the ready queue, so retries can overtake nothing but do fall
  behind work enqueued while they were leased.

### Lease deadlines and the clock

Deadlines are compared using `time.Now`, whose monotonic reading means an NTP
step or a wall-clock adjustment can't expire a live lease early or hold an
expired one open. `WithClock` swaps that out for tests.

Expiry is not written to the log. It isn't a decision the queue makes — it's
the absence of an ack by a deadline — and it's recomputed on replay, so a
queue full of slow consumers can't grow the log while no work completes.
Leases deliberately don't survive a reopen either: a lease is a promise to one
consumer in one process, and once that process is gone, waiting out its
deadline would only delay the work. So replay brings every unacked job back as
ready, in enqueue order, with its delivery count intact.

Expired leases are reclaimed lazily, when `Lease`, `Ack`, or `Stats` is
called. There is no timer goroutine, so a queue nobody is polling won't
redeliver anything on its own.

### Durability and fsync

A write reaches the OS page cache immediately and, from there, survives the
writing process being killed — a `kill -9` mid-`Append` loses at most the one
torn record, which recovery already handles. It does **not** survive a power
loss or kernel panic until it has been `fsync`ed to the disk. `WithSync`
chooses when that `fsync` happens, and the choice is a straight durability /
throughput trade:

- **`SyncEveryAppend()` (default).** Every `Enqueue`, `Lease` and `Ack`
  `fsync`s the log before it returns. When one of those calls reports success,
  its record is on stable storage and survives a power loss — nothing
  acknowledged is ever lost. The cost is a disk round-trip per operation.

- **`SyncEvery(d)`.** The log `fsync`s at most once per interval `d`, so a run
  of appends inside the same interval is flushed by a single sync. Everything
  up to the last sync is durable; the writes made **since** the last sync are
  only in the page cache, and a power loss or kernel panic discards them. For
  the queue that means a bounded tail is at risk: an `Enqueue` that returned an
  id can vanish, and an `Ack` can be lost so its job is redelivered on the next
  open. It never risks more than that un-synced tail — records before the last
  sync, and the log's structure, are unaffected.

  Two honest caveats on `SyncEvery(d)`:

  - The sync is **lazy** — it fires on the first append that finds `d` has
    elapsed since the last sync, and there is no background flusher. So the
    window is bounded by "writes since the last sync", not strictly by `d` of
    wall-clock time: a single append followed by an idle period is not synced
    until the next append. `Close` covers the graceful case (below); only a
    crash during that idle window exposes the tail.
  - `d <= 0` degenerates to syncing on every append, same as
    `SyncEveryAppend()`.

`Close` always `fsync`s before it closes the file, under either policy, so an
orderly shutdown is durable regardless of the sync policy — only a crash inside
an un-synced window loses anything. Torn-write recovery is unchanged either
way: whatever did reach the disk is what `Open` recovers, truncating a torn
trailing record before the next append.

The same option is available on the raw log as `Open(path,
spool.WithSyncPolicy(p))`.

### Rotation and compaction

The log is a directory of numbered, append-only **segments**. Appends go to the
newest segment; once it passes `WithMaxSegmentBytes` (4 MiB by default) the next
append rolls to a fresh one and the old segment is **sealed** — closed, fsynced,
never written again. A record is never split across a boundary, so replaying the
segments in order is exactly replaying one long log.

Sealing is what makes reclamation possible. A whole sealed segment is dropped
once it is **fully obsolete**: every job with a record in it has been acked
*and* that ack was itself recorded no later than the segment. Precisely, the
queue drops the oldest run of segments up to the newest sealed segment `i` for
which the count of jobs enqueued in segments `0..i` but not yet acked in
segments `0..i` is zero. At that boundary the segments before it net to nothing
— every job they opened was also closed within them — so a fresh replay of the
segments after it reconstructs the identical queue. Anything still unacked or
leaseable keeps its segment, and every segment after it, alive.

This is deliberately conservative, and what it promises is narrow:

- **Growth is bounded by un-acked work, not by throughput.** As jobs are acked
  and the queue drains, old segments are reclaimed, so the directory tracks how
  much work is outstanding rather than how much has ever passed through. A
  batch workload that drains between batches reclaims almost everything.
- **A backlog is not reclaimed.** Reclamation can only advance to a point in the
  log where nothing enqueued before it is still outstanding. A queue that is
  never drained — always some job in flight across every rotation — keeps
  growing, because there is no such point. In the limit, a **single job that is
  never acked** (a poison job redelivered forever, or one nobody leases) pins
  its segment and everything after it indefinitely. `Job.Deliveries` is there so
  a consumer can decide to drop such a job and let the log move on.
- **No online rewrite.** Reclamation only ever drops whole sealed segments; it
  never rewrites a live segment to squeeze out the acked records mixed into it.
  So the floor advances in segment-sized steps, and a mostly-acked segment that
  still holds one live job is kept in full.

Reclamation is crash-safe. A small `manifest` file records the lowest live
segment; it is updated with a durable atomic rename *before* any segment file is
unlinked, so a crash mid-compaction leaves at worst some already-superseded
segment files below the floor, which the next open ignores and sweeps away.
Rotation is crash-safe too: a crash mid-rotation loses at most the in-flight
append that triggered it, exactly as any torn write does.

## On-disk format

Each queue segment is a single append-only log file, and that primitive is
exported too — `Open`, `Append` and `Replay` give you the raw record stream of
one file if you want a different set of semantics over it. The queue composes a
directory of these files, one per segment, plus a `manifest` recording the
lowest live segment (see "Rotation and compaction"). Within a file, each record
is written as:

| field    | size    | meaning                          |
|----------|---------|-----------------------------------|
| length   | 4 bytes | big-endian length of the payload |
| checksum | 4 bytes | CRC32 (IEEE) of the payload       |
| payload  | N bytes | the record itself                |

Records are appended back to back with no separators; `Replay` walks the
file from the start, reading one record at a time.

The queue stores one state transition per record. Each payload starts with a
1-byte kind and an 8-byte big-endian job id, then:

| kind    | rest of the record                       |
|---------|------------------------------------------|
| enqueue | the job payload, to the end of the record |
| lease   | 8 bytes, big-endian lease id             |
| ack     | 8 bytes, big-endian lease id             |

No length prefix is needed on the job payload: the log already frames records
and hands back exactly the bytes that were appended.

## Status

What this gives you today:

- Queue state is fully reconstructible from the log. Enqueue, lease and ack
  each append a record before the change is visible in memory, so replaying
  the file rebuilds the queue exactly, including delivery counts.
- Safe for concurrent producers and consumers in one process. The tests cover
  multiple consumers draining a queue under `-race`; no job is delivered
  twice while its lease is live.
- Every successfully replayed record is verified against its checksum —
  silent bit-rot in a fully-written record is caught, not returned as data.
- `Replay` on a path that doesn't exist yet returns an empty log, not an
  error, so callers can treat "no log file" and "empty log" the same way.
- A configurable `fsync` policy. By default every append is `fsync`ed before
  it returns, so anything the queue acknowledged survives a power loss, not
  just a killed process; `WithSync(SyncEvery(d))` relaxes that to at most one
  sync per interval, trading a bounded un-synced tail for throughput. Both are
  spelled out under "Durability and fsync", and `Close` flushes under either.
- Recovery from a torn write. If the process is killed mid-`Append`, the
  active segment ends with a partial header, a partial payload, or a payload
  that landed corrupted — recovery discards that trailing record and keeps
  everything before it, with no error. This only applies to the *last* record
  physically in the newest segment: a checksum mismatch or short read anywhere
  earlier — mid-segment, or at the tail of a sealed segment — can't be a torn
  write (`Append` only ever extends the active file, and a sealed segment was
  finished before the next one existed), so it's treated as real corruption and
  reported as a hard error instead of silently dropping data.
- Segment rotation and compaction. The log rotates into fixed-size segments and
  drops whole segments once every job they recorded has been acked, so the log
  grows with outstanding work rather than with total throughput. It is
  conservative — a still-unacked job pins its segment — and never rewrites a
  live segment. What it does and does not promise is spelled out under
  "Rotation and compaction".
- Repeated recovery. `Open` truncates a torn trailing record away before it
  accepts any append, so the next write starts at the end of the last intact
  record rather than after the damaged bytes. Crashing mid-`Append`,
  reopening, and crashing again is survivable arbitrarily many times, not
  once. Mid-log corruption is reported by `Open` too, and the file is left
  untouched in that case — truncating there would discard valid records
  sitting behind the damage.
- Crash-tested, not just unit-tested. A fuzzer kills a real worker process
  with `SIGKILL` at arbitrary points mid-write, across many seeds, and checks
  that no job the worker reported acked is ever lost or redelivered after
  recovery. A short pass runs on every build; see "Test".

What this does **not** give you yet:

- Not exactly-once. See "Delivery semantics" above: a job whose lease expires
  is delivered again, whether or not the work was actually done.
- No online segment rewrite. Compaction drops whole obsolete segments but never
  rewrites a live one, so a never-draining queue — or a single job that is never
  acked — keeps its segments and the log keeps growing. Bounded growth holds
  only to the extent work is actually acked; see "Rotation and compaction".
- One process per log directory. There is no locking, and two `Queue` values
  over the same path — in one process or two — will interleave their appends and
  diverge from each other.
- No nack, no dead-letter queue, no maximum attempt count, no scheduled or
  delayed jobs, no priorities. A job that fails forever is redelivered
  forever; `Job.Deliveries` is there so a consumer can decide to drop it.

## Test

```sh
go test ./... -race
```

Alongside the unit tests, a crash fuzzer builds a small worker that drives a
real queue through a randomized enqueue/lease/ack workload — recording each
job id only after its ack has returned durable — then kills that worker with
`SIGKILL` at an arbitrary point mid-write, reopens the queue, and asserts that
nothing the worker reported acked comes back as live work. A short, bounded
pass runs on every `go test`; set `SPOOL_CRASH_SEEDS` to sweep many more kill
points locally:

```sh
SPOOL_CRASH_SEEDS=500 go test -run TestCrashRecovery -count=1
```
