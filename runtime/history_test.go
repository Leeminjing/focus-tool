package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func durableConfig(path string) Config {
	return Config{Concurrency: 2, Capacity: 8, PerRunCapacity: 4, MaxActiveRuns: 2, SegmentBytes: 2048, Journal: path,
		Executors: map[string]Executor{"test": ExecutorFunc(func(context.Context, Task) Result {
			return Result{State: "completed", Output: `{"value":9007199254740993}`}
		})}}
}

func TestHistoryEvictedButIdentityAndDependenciesSurviveCompaction(t *testing.T) {
	config := durableConfig(filepath.Join(t.TempDir(), "runtime.jsonl"))
	var calls atomic.Int32
	config.Executors["test"] = ExecutorFunc(func(context.Context, Task) Result {
		calls.Add(1)
		return Result{State: "completed", Output: `{"value":9007199254740993}`}
	})
	bound := make(chan Task, 1)
	config.Executors["python"] = ExecutorFunc(func(_ context.Context, task Task) Result { bound <- task; return Result{State: "completed"} })
	e, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	if err = e.OpenRun(ownerA, 1); err != nil {
		t.Fatal(err)
	}
	parent := task("parent", ownerA)
	submit(t, e, parent)
	first := completion(t, e, ownerA, "parent")
	if err = e.Ack(ownerA, first.Sequence); err != nil {
		t.Fatal(err)
	}
	if stats := e.Stats(); stats.Tasks != 0 || stats.Events != 0 {
		t.Fatalf("history retained: %+v", stats)
	}
	if err = e.Compact(); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	if status, err := e.Submit(parent); err != nil || status.State != "completed" || calls.Load() != 1 {
		t.Fatalf("identity replayed: %+v %v %d", status, err, calls.Load())
	}
	conflict := parent
	conflict.TimeoutMS = 1
	if _, err := e.Submit(conflict); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	child := task("child", ownerA)
	child.Kind = "python"
	child.Python = &PythonCall{Interpreter: "python", Module: "tools", Function: "work"}
	child.Dependencies = []string{"parent"}
	child.Bindings = map[string]Binding{"value": {TaskID: "parent", Path: []string{"value"}}}
	submit(t, e, child)
	if got := receive(t, bound); string(got.Python.Args["value"]) != "9007199254740993" {
		t.Fatal(got)
	}
	completion(t, e, ownerA, "child")
}

func TestRunChurnRemovesResidentHistoryAndPreservesClosedCustody(t *testing.T) {
	config := durableConfig(filepath.Join(t.TempDir(), "runtime.jsonl"))
	e, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	for n := 0; n < 40; n++ {
		owner := Owner{"context", fmt.Sprint(n)}
		if err = e.OpenRun(owner, 1); err != nil {
			t.Fatal(err)
		}
		submit(t, e, task("job", owner))
		event := completion(t, e, owner, "job")
		if n%2 == 0 {
			if err = e.Ack(owner, event.Sequence); err != nil {
				t.Fatal(err)
			}
		}
		if err = e.CloseRun(owner); err != nil {
			t.Fatal(err)
		}
		stats := e.Stats()
		if stats.Tasks != 0 || stats.Events != 0 || stats.Runs != 0 || stats.QueueOwners != 0 {
			t.Fatalf("resident history after %d: %+v", n, stats)
		}
		if stats.JournalBytes >= config.SegmentBytes {
			t.Fatalf("recent history exceeds segment policy: %+v", stats)
		}
		if receipt := e.Archive(owner); receipt.Custodian != "runtime_archive" || receipt.Unacknowledged != n%2 {
			t.Fatal(receipt)
		}
	}
	if e.Stats().Compactions < 2 {
		t.Fatal("rotation did not execute")
	}
	if status, err := e.Submit(task("job", Owner{"context", "0"})); err != nil || status.State != "completed" {
		t.Fatal(status, err)
	}
	if err = e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	if stats := e.Stats(); stats.Tasks != 0 || stats.Events != 0 || stats.Runs != 0 {
		t.Fatal(stats)
	}
	page, err := e.ArchivePage(Owner{"context", "1"}, PageOptions{})
	if err != nil || len(page.Items) != 1 {
		t.Fatal(page, err)
	}
}

func TestArchiveBytePagesResumeAcrossRestartAndRejectForeignCursor(t *testing.T) {
	config := durableConfig(filepath.Join(t.TempDir(), "runtime.jsonl"))
	config.Executors["test"] = ExecutorFunc(func(context.Context, Task) Result {
		return Result{State: "completed", Output: strings.Repeat("x", 512<<10)}
	})
	e, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	if err = e.OpenRun(ownerA, 1); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 17; n++ {
		id := fmt.Sprint(n)
		submit(t, e, task(id, ownerA))
		event := completion(t, e, ownerA, id)
		if err = e.Ack(ownerA, event.Sequence); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.CloseRun(ownerA); err != nil {
		t.Fatal(err)
	}
	options := PageOptions{Limit: 3, MaxBytes: 1200 << 10}
	page, err := e.ArchivePage(ownerA, options)
	if err != nil || len(page.Items) != 2 || page.NextCursor == "" {
		t.Fatalf("bad first page: %d %v", len(page.Items), err)
	}
	if _, err = e.ArchivePage(ownerA, PageOptions{MaxBytes: 512}); !errors.Is(err, ErrPageTooSmall) {
		t.Fatal(err)
	}
	cursor := page.NextCursor
	if err = e.Compact(); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	if _, err = e.ArchivePage(ownerB, PageOptions{Cursor: cursor}); err == nil {
		t.Fatal("foreign cursor accepted")
	}
	seen := map[uint64]bool{}
	for {
		data, _ := json.Marshal(page)
		if len(data) > options.MaxBytes || len(page.Items) > options.Limit {
			t.Fatal("page over budget")
		}
		for _, event := range page.Items {
			if seen[event.Sequence] {
				t.Fatal("duplicate")
			}
			seen[event.Sequence] = true
		}
		if page.NextCursor == "" {
			break
		}
		options.Cursor = page.NextCursor
		page, err = e.ArchivePage(ownerA, options)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 17 {
		t.Fatal(len(seen))
	}
}

func TestRecoveryAtSealedBoundaryAndPartiallyAppliedSnapshot(t *testing.T) {
	for _, stage := range []string{"before-new-segment", "after-new-segment", "partial-index"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "runtime.jsonl")
			config := durableConfig(path)
			config.SegmentBytes = 1 << 20
			e, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			if err = e.OpenRun(ownerA, 1); err != nil {
				t.Fatal(err)
			}
			submit(t, e, task("once", ownerA))
			event := completion(t, e, ownerA, "once")
			if err = e.Ack(ownerA, event.Sequence); err != nil {
				t.Fatal(err)
			}
			if err = e.CloseRun(ownerA); err != nil {
				t.Fatal(err)
			}
			if err = e.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if stage == "partial-index" {
				state := RunState{Owner: ownerA, Revision: 1, Closed: true, Ack: event.Sequence, Facts: 1, Consumed: 1}
				if err = e.history.apply(record{Version: 2, Run: &state}); err != nil {
					t.Fatal(err)
				}
				if err = os.Remove(e.history.keyPath("tasks", taskKey(ownerA, "once"))); err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
			} else {
				if err = os.Rename(path, path+".sealed"); err != nil {
					t.Fatal(err)
				}
				if stage == "after-new-segment" {
					if err = os.WriteFile(path, []byte(`{"torn":`), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			e, err = New(config)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close(context.Background())
			if status, err := e.Submit(task("once", ownerA)); err != nil || status.State != "completed" {
				t.Fatal(status, err)
			}
			if receipt := e.Archive(ownerA); receipt.Unacknowledged != 0 {
				t.Fatal(receipt)
			}
			page, err := e.ArchivePage(ownerA, PageOptions{})
			if err != nil || len(page.Items) != 1 || page.Items[0].Sequence != event.Sequence {
				t.Fatal(page, err)
			}
		})
	}
}

func TestUnknownFencesSurviveAckCloseCompactionAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.jsonl")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashJournalHelper$", "--", "--crash-helper", path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	ready := make(chan bool, 1)
	go func() { scanner := bufio.NewScanner(stdout); ready <- scanner.Scan() && scanner.Text() == "ready" }()
	if !receive(t, ready) {
		t.Fatal("worker did not start")
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	config := durableConfig(path)
	e, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	unknown := completion(t, e, ownerA, "uncertain")
	if unknown.Result.State != "unknown" {
		t.Fatal(unknown)
	}
	if err = e.Ack(ownerA, unknown.Sequence); err != nil {
		t.Fatal(err)
	}
	if err = e.CloseRun(ownerA); err != nil {
		t.Fatal(err)
	}
	if err = e.Compact(); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := make(chan Task, 4)
	config.Executors["test"] = ExecutorFunc(func(_ context.Context, task Task) Result { started <- task; return Result{State: "completed"} })
	e, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	if stats := e.Stats(); stats.Unknown != 1 || stats.Tasks != 1 || stats.Events != 0 || stats.Runs != 0 {
		t.Fatal(stats)
	}
	if err = e.OpenRun(ownerB, 1); err != nil {
		t.Fatal(err)
	}
	conflict := task("conflict", ownerB)
	conflict.Resources = []Resource{{"workspace", "read"}}
	submit(t, e, conflict)
	submit(t, e, task("free", ownerB))
	if got := receive(t, started); got.ID != "free" {
		t.Fatalf("unknown fence cleared: %+v", got)
	}
	completion(t, e, ownerB, "free")
	if status, err := e.Status(ownerB, "conflict"); err != nil || status.State != "queued" {
		t.Fatal(status, err)
	}
}

func TestLegacyJournalCountersMigrateIdempotently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.jsonl")
	state := RunState{Owner: ownerA, Revision: 1}
	job := task("old", ownerA)
	result := Result{State: "completed", Output: `true`}
	status := Status{Task: job, State: "completed", Result: &result}
	event := Completion{Sequence: 1, Task: job, Result: result}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	_ = encoder.Encode(record{Run: &state})
	_ = encoder.Encode(record{Task: &status, Event: &event})
	state.Ack = 1
	state.Closed = true
	_ = encoder.Encode(record{Run: &state})
	file.Close()
	config := durableConfig(path)
	for n := 0; n < 2; n++ {
		e, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		if receipt := e.Archive(ownerA); receipt.Unacknowledged != 0 {
			t.Fatal(receipt)
		}
		if status, err := e.Submit(job); err != nil || status.State != "completed" {
			t.Fatal(status, err)
		}
		if n == 1 {
			if err = e.Compact(); err != nil {
				t.Fatal(err)
			}
		}
		if err = e.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecoveryRejectsReducedCombinedRunQuota(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.jsonl")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	state := RunState{Owner: ownerA, Revision: 1}
	_ = encoder.Encode(record{Version: 2, Run: &state})
	for n := 1; n <= 4; n++ {
		job := task(fmt.Sprint(n), ownerA)
		status := Status{Task: job, State: "queued"}
		item := record{Version: 2, Task: &status}
		if n <= 2 {
			result := Result{State: "completed"}
			status.State = "completed"
			status.Result = &result
			state.Facts = uint64(n)
			item.Run = &state
			item.Event = &Completion{Sequence: uint64(n), Task: job, Result: result}
		}
		_ = encoder.Encode(item)
	}
	file.Close()
	config := durableConfig(path)
	config.PerRunCapacity = 3
	e, err := New(config)
	if err == nil {
		e.Close(context.Background())
		t.Fatal("recovered 2 pending + 2 facts with quota 3")
	}
	if !strings.Contains(err.Error(), "combined per-Run") {
		t.Fatal(err)
	}
}

func TestPartialCompactionQueriesFailClosedUntilReplay(t *testing.T) {
	config := durableConfig(filepath.Join(t.TempDir(), "runtime.jsonl"))
	config.SegmentBytes = 1 << 20
	e, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.OpenRun(ownerA, 1); err != nil {
		t.Fatal(err)
	}
	submit(t, e, task("one", ownerA))
	event := completion(t, e, ownerA, "one")
	if err = e.CloseRun(ownerA); err != nil {
		t.Fatal(err)
	}
	// A compaction stopped after indexing a prefix. WAL/cache remain intact.
	if err = e.history.apply(record{Version: 2, Event: &event}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.storageFailure(errors.New("injected index write failure"))
	e.mu.Unlock()
	if _, err = e.ArchivePage(ownerA, PageOptions{}); !errors.Is(err, ErrStorageUnknown) {
		t.Fatal(err)
	}
	if err = e.Close(context.Background()); !errors.Is(err, ErrStorageUnknown) {
		t.Fatal(err)
	}
	e, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	page, err := e.ArchivePage(ownerA, PageOptions{})
	if err != nil || len(page.Items) != 1 {
		t.Fatal(page, err)
	}
}

func TestStreamingReplayHasNoWholeJournal128MiBLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.jsonl")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	state := RunState{Owner: ownerA, Revision: 1}
	data, _ := json.Marshal(record{Version: 2, Run: &state})
	// Whitespace pads otherwise-small legal frames. This checks streaming
	// recovery past the former whole-file cap without a huge retained payload.
	frame := append([]byte(strings.Repeat(" ", 1<<20)), data...)
	frame = append(frame, '\n')
	for n := 0; n < 129; n++ {
		if _, err = file.Write(frame); err != nil {
			file.Close()
			t.Fatal(err)
		}
	}
	file.Close()
	e, err := New(durableConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	if e.Stats().JournalBytes <= 128<<20 {
		t.Fatal("fixture did not exceed old cap")
	}
	if err = e.Compact(); err != nil {
		t.Fatal(err)
	}
	if e.Stats().JournalBytes != 0 {
		t.Fatal("WAL did not compact")
	}
	if err = e.OpenRun(ownerA, 1); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveCursorExcludesFactsArrivingAfterSnapshot(t *testing.T) {
	config := durableConfig(filepath.Join(t.TempDir(), "runtime.jsonl"))
	config.SegmentBytes = 1 << 20
	started, release := make(chan struct{}, 1), make(chan struct{})
	config.Executors["test"] = ExecutorFunc(func(_ context.Context, task Task) Result {
		if task.ID == "late" {
			started <- struct{}{}
			<-release
			return Result{State: "cancelled"}
		}
		return Result{State: "completed"}
	})
	e, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	if err = e.OpenRun(ownerA, 1); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		submit(t, e, task(id, ownerA))
		event := completion(t, e, ownerA, id)
		if err = e.Ack(ownerA, event.Sequence); err != nil {
			t.Fatal(err)
		}
	}
	submit(t, e, task("late", ownerA))
	receive(t, started)
	if err = e.CloseRun(ownerA); err != nil {
		t.Fatal(err)
	}
	page, err := e.ArchivePage(ownerA, PageOptions{Limit: 1})
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatal(page, err)
	}
	close(release)
	completion(t, e, ownerA, "late")
	if err = e.Compact(); err != nil {
		t.Fatal(err)
	}
	continued, err := e.ArchivePage(ownerA, PageOptions{Cursor: page.NextCursor, Limit: 10})
	if err != nil || len(continued.Items) != 1 || continued.Items[0].Task.ID != "second" || continued.Through != page.Through || continued.NextCursor != "" {
		t.Fatal(continued, err)
	}
	fresh, err := e.ArchivePage(ownerA, PageOptions{})
	if err != nil || len(fresh.Items) != 3 {
		t.Fatal(fresh, err)
	}
}
