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
	"sync/atomic"
	"testing"
	"time"
)

var ownerA = Owner{"A", "run-A"}
var ownerB = Owner{"B", "run-B"}

func testEngine(t *testing.T, config Config) *Engine {
	t.Helper()
	engine, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := engine.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	for _, owner := range []Owner{ownerA, ownerB} {
		if err := engine.OpenRun(owner, 1); err != nil {
			t.Fatal(err)
		}
	}
	return engine
}

func submit(t *testing.T, e *Engine, task Task) Status {
	t.Helper()
	status, err := e.Submit(task)
	if err != nil {
		t.Fatal(err)
	}
	return status
}
func task(id string, owner Owner) Task { return Task{ID: id, Owner: owner, Revision: 1, Kind: "test"} }
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}
func completion(t *testing.T, e *Engine, owner Owner, id string) Completion {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		for _, event := range e.Events(owner, 0) {
			if event.Task.ID == id {
				return event
			}
		}
		select {
		case <-e.Changed():
		case <-deadline:
			t.Fatal("completion timed out")
			return Completion{}
		}
	}
}

func TestFairQueueBackpressureAndOwnerRouting(t *testing.T) {
	started := make(chan Task, 8)
	release := make(chan struct{})
	executor := ExecutorFunc(func(ctx context.Context, task Task) Result {
		started <- task
		select {
		case <-release:
			return Result{State: "completed", Output: `1`}
		case <-ctx.Done():
			return Result{State: "cancelled"}
		}
	})
	e := testEngine(t, Config{Concurrency: 1, Capacity: 4, Executors: map[string]Executor{"test": executor}})
	first := task("a0", ownerA)
	submit(t, e, first)
	receive(t, started)
	submit(t, e, task("a1", ownerA))
	submit(t, e, task("a2", ownerA))
	submit(t, e, task("b0", ownerB))
	if _, err := e.Submit(task("overflow", ownerB)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("backpressure: %v", err)
	}
	if _, err := e.Submit(first); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	first.TimeoutMS = 1
	if _, err := e.Submit(first); !errors.Is(err, ErrConflict) {
		t.Fatalf("identity conflict: %v", err)
	}
	release <- struct{}{}
	if next := receive(t, started); next.Owner != ownerB {
		t.Fatalf("busy owner monopolized slot: %+v", next)
	}
	release <- struct{}{}
	receive(t, started)
	release <- struct{}{}
	receive(t, started)
	release <- struct{}{}
	completion(t, e, ownerA, "a2")
	completion(t, e, ownerB, "b0")
	for _, event := range e.Events(ownerA, 0) {
		if event.Task.Owner != ownerA {
			t.Fatal("cross-owner delivery")
		}
	}
	if len(e.Events(ownerB, 0)) != 1 {
		t.Fatal("wrong B mailbox")
	}
}

func TestDependencyBindingAndFailurePropagation(t *testing.T) {
	started := make(chan Task, 8)
	executor := ExecutorFunc(func(ctx context.Context, task Task) Result {
		started <- task
		if task.ID == "parent" {
			return Result{State: "completed", Output: `{"nested":[{"value":42}]}`}
		}
		if task.ID == "bad" {
			return Result{State: "failed", Error: "broken"}
		}
		return Result{State: "completed", Output: `true`}
	})
	e := testEngine(t, Config{Concurrency: 2, Capacity: 10, Executors: map[string]Executor{"test": executor, "python": executor}})
	submit(t, e, task("parent", ownerA))
	child := task("child", ownerA)
	child.Kind = "python"
	child.Python = &PythonCall{Interpreter: "python", Module: "tools", Function: "work"}
	child.Dependencies = []string{"parent"}
	child.Bindings = map[string]Binding{"answer": {TaskID: "parent", Path: []string{"nested", "0", "value"}}}
	submit(t, e, child)
	completion(t, e, ownerA, "child")
	var bound Task
	for n := 0; n < 2; n++ {
		got := receive(t, started)
		if got.ID == "child" {
			bound = got
		}
	}
	if string(bound.Python.Args["answer"]) != "42" {
		t.Fatalf("dependency not bound: %+v", bound)
	}
	submit(t, e, task("bad", ownerA))
	dependent := task("blocked", ownerA)
	dependent.Dependencies = []string{"bad"}
	submit(t, e, dependent)
	if event := completion(t, e, ownerA, "blocked"); event.Result.State != "skipped" {
		t.Fatal(event)
	}
	crossOwner := task("cross", ownerB)
	crossOwner.Dependencies = []string{"parent"}
	if _, err := e.Submit(crossOwner); err == nil {
		t.Fatal("cross-owner dependency accepted")
	}
}

func TestCancellationRetainsResourceUntilExecutorStops(t *testing.T) {
	started := make(chan Task, 4)
	cancelling := make(chan struct{})
	cleanup := make(chan struct{})
	executor := ExecutorFunc(func(ctx context.Context, task Task) Result {
		started <- task
		if task.ID == "old" {
			<-ctx.Done()
			close(cancelling)
			<-cleanup
			return Result{State: "cancelled"}
		}
		return Result{State: "completed"}
	})
	e := testEngine(t, Config{Concurrency: 2, Capacity: 8, Executors: map[string]Executor{"test": executor}})
	old := task("old", ownerA)
	old.Resources = []Resource{{"workspace", "write"}}
	submit(t, e, old)
	receive(t, started)
	if err := e.Cancel(ownerA, "old"); err != nil {
		t.Fatal(err)
	}
	receive(t, cancelling)
	if status, _ := e.Status(ownerA, "old"); status.State != "cancel_requested" {
		t.Fatal(status)
	}
	next := task("next", ownerB)
	next.Resources = old.Resources
	submit(t, e, next)
	submit(t, e, task("independent", ownerB))
	if got := receive(t, started); got.ID != "independent" {
		t.Fatalf("released conflicting resource before cleanup: %+v", got)
	}
	close(cleanup)
	if got := receive(t, started); got.ID != "next" {
		t.Fatal(got)
	}
	completion(t, e, ownerB, "next")
}

func TestRevisionAndExecutionTimeAdmission(t *testing.T) {
	var permitted atomic.Bool
	gateEntered := make(chan struct{}, 1)
	gateRelease := make(chan struct{})
	e := testEngine(t, Config{Concurrency: 1, Capacity: 4, BeforeExecute: func(ctx context.Context, task Task) error {
		gateEntered <- struct{}{}
		select {
		case <-gateRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
		if !permitted.Load() {
			return errors.New("lease expired")
		}
		return nil
	}, Executors: map[string]Executor{"test": ExecutorFunc(func(context.Context, Task) Result { t.Error("denied executor ran"); return Result{State: "completed"} })}})
	submit(t, e, task("denied", ownerA))
	receive(t, gateEntered)
	close(gateRelease)
	if event := completion(t, e, ownerA, "denied"); event.Result.State != "failed" {
		t.Fatal(event)
	}
	if err := e.OpenRun(ownerA, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(task("stale", ownerA)); err == nil {
		t.Fatal("stale plan submitted")
	}
	if err := e.CloseRun(ownerB); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(task("closed", ownerB)); err == nil {
		t.Fatal("closed run submitted")
	}
}

func TestJournalReplayAckPartialTailAndExclusiveWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	config := Config{Concurrency: 1, Capacity: 2, Journal: path, Executors: map[string]Executor{"test": ExecutorFunc(func(context.Context, Task) Result { return Result{State: "completed", Output: `"persisted"`} })}}
	e, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(config); err == nil {
		t.Fatal("journal allowed two writers")
	}
	if err := e.OpenRun(ownerA, 1); err != nil {
		t.Fatal(err)
	}
	submit(t, e, task("one", ownerA))
	one := completion(t, e, ownerA, "one")
	submit(t, e, task("two", ownerA))
	two := completion(t, e, ownerA, "two")
	if err := e.Ack(ownerA, two.Sequence); err == nil {
		t.Fatal("ack skipped unconsumed event")
	}
	if err := e.Ack(ownerB, one.Sequence); err == nil {
		t.Fatal("cross-owner ack accepted")
	}
	if err := e.Ack(ownerA, one.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(`{"torn":`)
	file.Close()
	reopened, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	events := reopened.Events(ownerA, 0)
	if len(events) != 1 || events[0].Task.ID != "two" {
		t.Fatalf("bad replay: %+v", events)
	}
	if status, err := reopened.Submit(task("one", ownerA)); err != nil || status.State != "completed" {
		t.Fatalf("replayed execution: %+v %v", status, err)
	}
}

func TestCrashJournalHelper(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "--crash-helper" {
		return
	}
	e, err := New(Config{Concurrency: 1, Capacity: 3, Journal: os.Args[len(os.Args)-1], Executors: map[string]Executor{"test": ExecutorFunc(func(ctx context.Context, task Task) Result {
		fmt.Println("ready")
		<-ctx.Done()
		return Result{State: "cancelled"}
	})}})
	if err != nil {
		os.Exit(3)
	}
	_ = e.OpenRun(ownerA, 1)
	job := task("uncertain", ownerA)
	job.Resources = []Resource{{"workspace", "write"}}
	_, _ = e.Submit(job)
	for {
		time.Sleep(time.Hour)
	}
}

func TestCrashMarksExecutionUnknownAndFencesResources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashJournalHelper$", "--", "--crash-helper", path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	ready := make(chan bool, 1)
	go func() { scanner := bufio.NewScanner(stdout); ready <- scanner.Scan() && scanner.Text() == "ready" }()
	if !receive(t, ready) {
		t.Fatal("crash helper did not run")
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	started := make(chan Task, 4)
	e := testEngine(t, Config{Concurrency: 1, Capacity: 4, Journal: path, Executors: map[string]Executor{"test": ExecutorFunc(func(ctx context.Context, task Task) Result { started <- task; return Result{State: "completed"} })}})
	if event := completion(t, e, ownerA, "uncertain"); event.Result.State != "unknown" {
		t.Fatal(event)
	}
	conflict := task("conflict", ownerB)
	conflict.Resources = []Resource{{"workspace", "read"}}
	submit(t, e, conflict)
	submit(t, e, task("independent", ownerB))
	if got := receive(t, started); got.ID != "independent" {
		t.Fatalf("unknown execution replayed or resource fence lost: %+v", got)
	}
	status, _ := e.Status(ownerB, "conflict")
	if status.State != "queued" {
		t.Fatal(status)
	}
}

func TestBindFailureDoesNotExecute(t *testing.T) {
	var calls atomic.Int32
	e := testEngine(t, Config{Concurrency: 1, Capacity: 4, Executors: map[string]Executor{"python": ExecutorFunc(func(context.Context, Task) Result { calls.Add(1); return Result{State: "completed", Output: `{}`} })}})
	parent := task("parent", ownerA)
	parent.Kind = "python"
	parent.Python = &PythonCall{Interpreter: "python", Module: "tools", Function: "work"}
	submit(t, e, parent)
	child := clone(parent)
	child.ID = "child"
	child.Dependencies = []string{"parent"}
	child.Bindings = map[string]Binding{"x": {TaskID: "parent", Path: []string{"missing"}}}
	submit(t, e, child)
	if event := completion(t, e, ownerA, "child"); event.Result.State != "failed" || calls.Load() != 1 {
		data, _ := json.Marshal(event)
		t.Fatal(string(data), calls.Load())
	}
}
