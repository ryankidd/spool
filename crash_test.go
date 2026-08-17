package spool

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The crash fuzzer runs a real child process against a queue, kills it with
// SIGKILL at an arbitrary point mid-workload, then reopens the queue and
// asserts the central durability invariant: every job the child reported
// durably acked before the kill stays acked — never lost, never redelivered.
//
// It uses a real subprocess and a real wall-clock delay on purpose: a torn
// write only happens when a process dies partway through one, which no
// injectable clock can simulate. The randomness is seeded so a failing seed
// reproduces.

// crashSeedSegmentBytes keeps segments tiny so a short run rotates, seals,
// compacts and rewrites the manifest many times, and a kill can land inside
// any of those steps.
const crashSeedSegmentBytes = 256

// buildCrashWorker compiles the crash worker once for the test and returns the
// path to the binary.
func buildCrashWorker(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "crashworker")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, "./internal/crashworker")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build crash worker: %v\n%s", err, out)
	}
	return bin
}

// crashSeedCount is the number of seeds the fuzzer runs. It is small by default
// so the CI pass stays bounded, and overridable for a longer local run:
//
//	SPOOL_CRASH_SEEDS=500 go test -run TestCrashRecoveryLosesNoAckedJob -count=1
func crashSeedCount(t *testing.T) int {
	t.Helper()
	v := os.Getenv("SPOOL_CRASH_SEEDS")
	if v == "" {
		return 8
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("SPOOL_CRASH_SEEDS=%q: want a positive integer", v)
	}
	return n
}

func TestCrashRecoveryLosesNoAckedJob(t *testing.T) {
	bin := buildCrashWorker(t)
	for i := 0; i < crashSeedCount(t); i++ {
		seed := int64(i) + 1
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			runCrashSeed(t, bin, seed)
		})
	}
}

// runCrashSeed launches the worker for one seed, kills it mid-workload, and
// hands the recovered queue and the reported acks to the invariant check.
func runCrashSeed(t *testing.T, bin string, seed int64) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "queue")
	acksPath := filepath.Join(t.TempDir(), "acked")

	cmd := exec.Command(bin,
		"-dir", dir,
		"-acks", acksPath,
		"-seed", strconv.FormatInt(seed, 10),
		"-seg", strconv.Itoa(crashSeedSegmentBytes),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}

	// Wait for the worker to open the queue and start writing, so the kill
	// lands during real work rather than during startup.
	br := bufio.NewReader(stdout)
	line, err := br.ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "ready" {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("worker never reported ready: line %q err %v stderr %q", line, err, stderr.String())
	}

	// Let it run for a seed-derived, bounded interval, then SIGKILL it at an
	// arbitrary point mid-workload.
	kr := rand.New(rand.NewSource(seed ^ 0x5DEECE66D))
	time.Sleep(time.Duration(10+kr.Intn(40)) * time.Millisecond)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill worker: %v", err)
	}
	_, _ = io.Copy(io.Discard, br)
	_ = cmd.Wait() // exit status is the kill signal; that is expected

	acked := readAckedIDs(t, acksPath)
	assertNoAckedJobLost(t, dir, acked, seed)
}

// assertNoAckedJobLost reopens the crashed queue and checks that nothing the
// worker reported acked reappears, that recovery replays without error, and
// that the recovered state is itself stable across a further reopen.
func assertNoAckedJobLost(t *testing.T, dir string, acked map[uint64]bool, seed int64) {
	t.Helper()

	// Reopening at all exercises replay of a log a kill left in an arbitrary
	// state: a torn tail record, a half-created segment, a manifest mid-rename.
	// A replay error here means torn or corrupt state escaped recovery.
	q, err := OpenQueue(dir, WithMaxSegmentBytes(crashSeedSegmentBytes))
	if err != nil {
		t.Fatalf("seed %d: reopen after crash: %v", seed, err)
	}

	// Drain the recovered queue. Anything that comes back was, by definition,
	// not acked before the crash, so no reported-acked id may appear. A
	// resurrected ack is exactly the durability failure this fuzzer exists to
	// catch. Each drained job is acked immediately, so the ready set only
	// shrinks and the drain terminates.
	for {
		job, err := q.Lease(time.Minute)
		if errors.Is(err, ErrEmpty) {
			break
		}
		if err != nil {
			q.Close()
			t.Fatalf("seed %d: lease during drain: %v", seed, err)
		}
		if acked[job.ID] {
			q.Close()
			t.Fatalf("seed %d: job %d was durably acked before the crash but replayed as live work", seed, job.ID)
		}
		if err := q.Ack(job); err != nil {
			q.Close()
			t.Fatalf("seed %d: ack during drain: %v", seed, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("seed %d: close after drain: %v", seed, err)
	}

	// Draining and acking the survivors must itself be durable: a second reopen
	// sees an empty, stable queue with nothing resurrected.
	q2, err := OpenQueue(dir, WithMaxSegmentBytes(crashSeedSegmentBytes))
	if err != nil {
		t.Fatalf("seed %d: second reopen: %v", seed, err)
	}
	defer q2.Close()
	if got := q2.Stats(); got != (Stats{}) {
		t.Fatalf("seed %d: after draining, reopened stats %+v, want empty", seed, got)
	}
}

// readAckedIDs reads the ids the worker reported acked. Only whole, newline
// terminated lines are trusted: the final element after the last newline is
// either empty (a clean end) or a line a kill interrupted, and neither is a
// committed record.
func readAckedIDs(t *testing.T, path string) map[uint64]bool {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// The worker was killed before it acked anything.
			return map[uint64]bool{}
		}
		t.Fatalf("read acked ids: %v", err)
	}

	acked := make(map[uint64]bool)
	lines := strings.Split(string(data), "\n")
	for _, line := range lines[:len(lines)-1] {
		id, err := strconv.ParseUint(line, 10, 64)
		if err != nil {
			t.Fatalf("acked ids file has a malformed line %q: %v", line, err)
		}
		acked[id] = true
	}
	return acked
}
