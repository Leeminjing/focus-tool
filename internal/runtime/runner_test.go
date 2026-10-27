package runtime

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// A real leaf process exercises cancellation and process reaping on the host OS.
func TestWorkerProcess(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-2] != "--helper" {
		return
	}
	switch os.Args[len(os.Args)-1] {
	case "hold":
		fmt.Println("ready")
		for {
			time.Sleep(time.Hour)
		}
	case "finish":
		fmt.Println(`{"ok":true}`)
	default:
		os.Exit(3)
	}
	os.Exit(0)
}

func helperTask(id, kind string) Task {
	return Task{ID: id, ContextID: "A", Revision: 1, Command: []string{os.Args[0], "-test.run=^TestWorkerProcess$", "--", "--helper", kind}}
}

func await(t *testing.T, job *Job) Result {
	t.Helper()
	select {
	case result := <-job.Done:
		return result
	case <-time.After(10 * time.Second):
		job.Cancel()
		t.Fatal("job did not settle")
		return Result{}
	}
}

func ready(t *testing.T, trace *Trace, id string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		for _, event := range trace.Events() {
			if event.TaskID == id && event.Kind == "first_output" {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatal("worker never became ready")
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestCancellationReapsRunningChildAndFreesSlot(t *testing.T) {
	trace := NewTrace()
	runner := NewRunner(1, trace)
	job := runner.Start(context.Background(), helperTask("running", "hold"))
	t.Cleanup(job.Cancel)
	ready(t, trace, "running")
	job.Cancel()
	job.Cancel()
	result := await(t, job)
	if result.State != "cancelled" || result.PID == 0 {
		t.Fatalf("expected stopped child, got %+v", result)
	}
	if next := await(t, runner.Start(context.Background(), helperTask("next", "finish"))); next.State != "completed" {
		t.Fatalf("slot not released: %+v", next)
	}
	requests := 0
	for _, event := range trace.Events() {
		if event.TaskID == "running" && event.Kind == "cancel_requested" {
			requests++
		}
	}
	if requests != 1 {
		t.Fatalf("cancel requests=%d", requests)
	}
}

func TestQueuedCancellationNeverStartsProcess(t *testing.T) {
	trace := NewTrace()
	runner := NewRunner(1, trace)
	first := runner.Start(context.Background(), helperTask("first", "hold"))
	t.Cleanup(first.Cancel)
	ready(t, trace, "first")
	queued := runner.Start(context.Background(), helperTask("queued", "finish"))
	queued.Cancel()
	if result := await(t, queued); result.State != "cancelled" || result.PID != 0 {
		t.Fatalf("queued task ran: %+v", result)
	}
	first.Cancel()
	await(t, first)
}

func TestConcurrencyBound(t *testing.T) {
	trace := NewTrace()
	runner := NewRunner(2, trace)
	var jobs []*Job
	for n := 0; n < 12; n++ {
		jobs = append(jobs, runner.Start(context.Background(), helperTask(fmt.Sprint(n), "finish")))
	}
	for _, job := range jobs {
		if result := await(t, job); result.State != "completed" {
			t.Fatalf("worker failed: %+v", result)
		}
	}
	active := 0
	for _, event := range trace.Events() {
		switch event.Kind {
		case "started":
			active++
		case "completed":
			active--
		}
		if active < 0 || active > 2 {
			t.Fatalf("invalid active process count %d", active)
		}
	}
	if active != 0 {
		t.Fatalf("leaked active processes: %d", active)
	}
}

func TestInboxScopesRevisionsAndDeduplicates(t *testing.T) {
	inbox := NewInbox()
	inbox.Revision["A"] = 2
	inbox.Revision["B"] = 1
	result := Result{Task: Task{ID: "same", ContextID: "A", Revision: 1}, State: "completed"}
	if got := inbox.Consume(result); got != "stale" {
		t.Fatal(got)
	}
	result.Task.ContextID = "B"
	if got := inbox.Consume(result); got != "accepted" {
		t.Fatal(got)
	}
	if got := inbox.Consume(result); got != "duplicate" {
		t.Fatal(got)
	}
	result.Task.ContextID = "A"
	result.Task.Revision = 2
	result.State = "cancelled"
	if got := inbox.Consume(result); got != "non_success" {
		t.Fatal(got)
	}
	result.State = "completed"
	if got := inbox.Consume(result); got != "accepted" {
		t.Fatal(got)
	}
	result.Task.ContextID = "unknown"
	result.Task.Revision = 0
	if got := inbox.Consume(result); got != "stale" {
		t.Fatal(got)
	}
}
