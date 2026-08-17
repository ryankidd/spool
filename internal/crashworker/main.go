// Command crashworker drives a spool queue through a randomized
// enqueue/lease/ack workload and records every durably-acked job id to a side
// file. It runs the workload forever, so a parent process can kill it at an
// arbitrary point mid-write and afterwards reopen the queue to check that no
// job the worker reported acked was lost or redelivered.
//
// The ordering that makes the side file trustworthy: an id is written only
// after Ack has returned, by which point the ack record is already durable
// under the default sync policy. The reported set is therefore always a subset
// of the truly-acked set, never the reverse, so a report the parent reads back
// can be held against the recovered queue without false positives.
package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/ryankidd/spool"
)

func main() {
	dir := flag.String("dir", "", "queue directory")
	acks := flag.String("acks", "", "file to append durably-acked job ids to")
	seed := flag.Int64("seed", 1, "workload seed")
	seg := flag.Int64("seg", 256, "max segment bytes, to force frequent rotation")
	flag.Parse()

	if *dir == "" || *acks == "" {
		fmt.Fprintln(os.Stderr, "crashworker: -dir and -acks are required")
		os.Exit(2)
	}

	if err := run(*dir, *acks, *seed, *seg); err != nil {
		fmt.Fprintf(os.Stderr, "crashworker: %v\n", err)
		os.Exit(1)
	}
}

func run(dir, acks string, seed, seg int64) error {
	q, err := spool.OpenQueue(dir, spool.WithMaxSegmentBytes(seg))
	if err != nil {
		return fmt.Errorf("open queue: %w", err)
	}
	defer q.Close()

	af, err := os.OpenFile(acks, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open acks file: %w", err)
	}
	defer af.Close()

	// Tell the parent the queue is open and the workload is about to start, so
	// it can time its kill against real writes rather than process startup.
	fmt.Println("ready")

	rng := rand.New(rand.NewSource(seed))
	const leaseFor = time.Minute
	for {
		// Enqueue a little under half the time; the rest of the time consume,
		// so the queue neither starves nor grows without bound.
		if rng.Intn(100) < 45 {
			if _, err := q.Enqueue(randomPayload(rng)); err != nil {
				return fmt.Errorf("enqueue: %w", err)
			}
			continue
		}

		job, err := q.Lease(leaseFor)
		if errors.Is(err, spool.ErrEmpty) {
			if _, err := q.Enqueue(randomPayload(rng)); err != nil {
				return fmt.Errorf("enqueue: %w", err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("lease: %w", err)
		}

		// Drop one lease in five without acking, leaving in-flight work the
		// recovery has to bring back. These ids are never reported acked.
		if rng.Intn(5) == 0 {
			continue
		}

		if err := q.Ack(job); err != nil {
			// A lease can expire under the fault harness if the OS scheduled us
			// out for longer than the deadline; that job simply isn't acked.
			if errors.Is(err, spool.ErrLeaseExpired) {
				continue
			}
			return fmt.Errorf("ack: %w", err)
		}

		// Ack has returned, so the ack record is durable. Only now is it safe
		// to publish the id as ground truth the parent will hold us to.
		if err := reportAck(af, job.ID); err != nil {
			return fmt.Errorf("report ack: %w", err)
		}
	}
}

// randomPayload returns a payload of random length, so records vary in size and
// rotation boundaries fall in different places across seeds.
func randomPayload(rng *rand.Rand) []byte {
	b := make([]byte, rng.Intn(49))
	rng.Read(b)
	return b
}

// reportAck appends id as a decimal line and fsyncs the side file, so the id is
// on stable storage before the worker can be killed.
func reportAck(af *os.File, id uint64) error {
	line := append(strconv.AppendUint(nil, id, 10), '\n')
	if _, err := af.Write(line); err != nil {
		return err
	}
	return af.Sync()
}
