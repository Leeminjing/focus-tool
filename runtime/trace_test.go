package runtime

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestTraceCapturesDurabilityAdmissionAndActualLockIntervals(t *testing.T) {
	var mu sync.Mutex
	var events []TraceEvent
	config := durableConfig(filepath.Join(t.TempDir(), "runtime.jsonl"))
	config.SegmentBytes = 1
	config.BeforeExecute = func(context.Context, Task) error { return nil }
	config.Trace = func(event TraceEvent) { mu.Lock(); defer mu.Unlock(); events = append(events, event) }
	e := testEngine(t, config)
	submit(t, e, task("traced", ownerA))
	event := completion(t, e, ownerA, "traced")
	if err := e.Ack(ownerA, event.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	seen := map[string]int{}
	for _, span := range events {
		seen[span.Phase]++
		if span.DurationNS < 0 || span.EndNS < span.StartNS {
			t.Fatal(span)
		}
		if span.Phase == "engine_lock" && span.WaitNS+span.HoldNS != span.DurationNS {
			t.Fatal("lock components do not sum", span)
		}
		if span.Phase == "compaction" {
			inside := false
			for _, lock := range events {
				if lock.Phase == "engine_lock" && lock.AcquiredNS <= span.StartNS && lock.EndNS >= span.EndNS {
					inside = true
				}
			}
			if !inside {
				t.Fatal("compaction has no containing lock interval", span)
			}
		}
	}
	for _, phase := range []string{"engine_lock", "dispatch_assigned", "admission", "wal_sync", "wal_encode", "wal_append", "wal_fsync", "compaction", "completion_handoff"} {
		if seen[phase] == 0 {
			t.Fatal("missing diagnostic", phase)
		}
	}
	var accepted, assigned, durable int64
	for _, span := range events {
		if span.TaskID == "traced" {
			if span.Phase == "wal_sync" && span.State == "queued" {
				accepted = span.EndNS
			}
			if span.Phase == "dispatch_assigned" {
				assigned = span.StartNS
			}
			if span.Phase == "wal_sync" && span.State == "completed" {
				durable = span.EndNS
			}
		}
	}
	if accepted == 0 || assigned < accepted || durable < assigned {
		t.Fatal(accepted, assigned, durable)
	}
}

func TestTracedPythonOutputCapPreservesCancellationPrecedence(t *testing.T) {
	python, err := exec.LookPath("python")
	if err != nil {
		t.Skip("Python not installed; Python integration environment runs this regression")
	}
	for _, traced := range []bool{false, true} {
		t.Run(fmt.Sprint(traced), func(t *testing.T) {
			directory := t.TempDir()
			source := "import atexit, time\nfrom pathlib import Path\ndef finish():\n def hold():\n  Path('flushed').write_text('ready')\n  time.sleep(60)\n atexit.register(hold)\n return 'x' * 256\n"
			if err := os.WriteFile(filepath.Join(directory, "tools.py"), []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			job := Task{ID: "limit", Owner: ownerA, Kind: "python", Directory: directory, Python: &PythonCall{Interpreter: python, Module: "tools", Function: "finish"}}
			if traced {
				ctx = withExecutionTrace(ctx, func(TraceEvent) {}, job)
			}
			out := make(chan Result, 1)
			go func() { out <- (PythonExecutor{Process: ProcessExecutor{OutputLimit: 128}}).Execute(ctx, job) }()
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Stat(filepath.Join(directory, "flushed")); err == nil {
					break
				}
				if time.Now().After(deadline) {
					cancel()
					receive(t, out)
					t.Fatal("worker did not flush before exit hook")
				}
				time.Sleep(5 * time.Millisecond)
			}
			cancel()
			result := receive(t, out)
			if result.State != "cancelled" || result.PID == 0 || len(result.Output) > 128 {
				t.Fatal(result)
			}
		})
	}
}
