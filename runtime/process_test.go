package runtime

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestProcessHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-2] != "--process-helper" {
		return
	}
	switch os.Args[len(os.Args)-1] {
	case "output":
		fmt.Print("first\nsecond\n")
	case "flood":
		fmt.Print(strings.Repeat("x", 10000))
	case "fail":
		fmt.Fprint(os.Stderr, "actual failure detail")
		os.Exit(7)
	case "hold":
		for {
			time.Sleep(time.Hour)
		}
	}
	os.Exit(0)
}

func processTask(mode string) Task {
	return Task{Command: []string{os.Args[0], "-test.run=^TestProcessHelper$", "--", "--process-helper", mode}}
}

func TestProcessExecutorPreservesOutputAndReportsFailures(t *testing.T) {
	executor := ProcessExecutor{OutputLimit: 128}
	if result := executor.Execute(context.Background(), processTask("output")); result.State != "completed" || result.Output != "first\nsecond\n" {
		t.Fatal(result)
	}
	if result := executor.Execute(context.Background(), processTask("fail")); result.State != "failed" || !strings.Contains(result.Error, "actual failure detail") {
		t.Fatal(result)
	}
	if result := executor.Execute(context.Background(), processTask("flood")); result.State != "failed" || len(result.Output) != 128 {
		t.Fatal(result)
	}
}

func TestProcessExecutorCancelsAndReapsChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	result := ProcessExecutor{}.Execute(ctx, processTask("hold"))
	if result.State != "cancelled" || result.PID == 0 {
		t.Fatal(result)
	}
}

func TestUnacknowledgedResultsApplyBackpressure(t *testing.T) {
	e := testEngine(t, Config{Concurrency: 1, Capacity: 1, Executors: map[string]Executor{"test": ExecutorFunc(func(context.Context, Task) Result { return Result{State: "completed"} })}})
	submit(t, e, task("first", ownerA))
	event := completion(t, e, ownerA, "first")
	if _, err := e.Submit(task("next-A", ownerA)); err != ErrRunFull {
		t.Fatalf("slow Run bypassed its quota: %v", err)
	}
	submit(t, e, task("next-B", ownerB))
	other := completion(t, e, ownerB, "next-B")
	if err := e.Ack(ownerB, other.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := e.Ack(ownerA, event.Sequence); err != nil {
		t.Fatal(err)
	}
	submit(t, e, task("next-A", ownerA))
	completion(t, e, ownerA, "next-A")
}

func TestContextHasOneActiveRunAndClosedRuntimeRejectsAck(t *testing.T) {
	e := testEngine(t, Config{Concurrency: 1, Capacity: 2, Executors: map[string]Executor{"test": ExecutorFunc(func(context.Context, Task) Result { return Result{State: "completed"} })}})
	if err := e.OpenRun(Owner{"A", "another-run"}, 1); err == nil {
		t.Fatal("two active runs for same Context")
	}
	submit(t, e, task("one", ownerA))
	event := completion(t, e, ownerA, "one")
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Ack(ownerA, event.Sequence); err != ErrClosed {
		t.Fatalf("ack after journal closed: %v", err)
	}
}

func TestExecutorPanicBecomesFailureAndReleasesCapacity(t *testing.T) {
	e := testEngine(t, Config{Concurrency: 1, Capacity: 2, Executors: map[string]Executor{"test": ExecutorFunc(func(context.Context, Task) Result { panic("test panic") })}})
	submit(t, e, task("panic", ownerA))
	event := completion(t, e, ownerA, "panic")
	if event.Result.State != "failed" || !strings.Contains(event.Result.Error, "test panic") {
		t.Fatal(event)
	}
	if err := e.Ack(ownerA, event.Sequence); err != nil {
		t.Fatal(err)
	}
	submit(t, e, task("again", ownerB))
	completion(t, e, ownerB, "again")
}

func TestBindingRejectsJSONPrefixWithTrailingLogs(t *testing.T) {
	e := testEngine(t, Config{Concurrency: 1, Capacity: 4, Executors: map[string]Executor{
		"test": ExecutorFunc(func(context.Context, Task) Result { return Result{State: "completed", Output: "{}\nnot-json"} }),
		"python": ExecutorFunc(func(context.Context, Task) Result {
			t.Error("malformed dependency executed")
			return Result{State: "completed"}
		}),
	}})
	submit(t, e, task("parent", ownerA))
	child := task("child", ownerA)
	child.Kind = "python"
	child.Python = &PythonCall{Interpreter: "python", Module: "tools", Function: "work"}
	child.Dependencies = []string{"parent"}
	child.Bindings = map[string]Binding{"value": {TaskID: "parent"}}
	submit(t, e, child)
	if event := completion(t, e, ownerA, "child"); event.Result.State != "failed" {
		t.Fatal(event)
	}
}

func TestClosedRunRetainsFactsWithoutBlockingAnotherContext(t *testing.T) {
	e := testEngine(t, Config{Concurrency: 1, Capacity: 1, Executors: map[string]Executor{"test": ExecutorFunc(func(context.Context, Task) Result { return Result{State: "completed"} })}})
	submit(t, e, task("abandoned", ownerA))
	event := completion(t, e, ownerA, "abandoned")
	if err := e.CloseRun(ownerA); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(task("other-context", ownerB)); err != nil {
		t.Fatalf("closed Run blocked B: %v", err)
	}
	if len(e.Events(ownerA, 0)) != 1 {
		t.Fatal("archiving deleted the execution fact")
	}
	if err := e.Ack(ownerA, event.Sequence); err == nil {
		t.Fatal("closed Run retained consumption authority")
	}
	if len(e.ArchivedEvents(ownerA, 0)) != 1 || e.Archive(ownerA).Custodian != "runtime_archive" {
		t.Fatal("archive custody missing")
	}
}
