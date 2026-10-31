package runtime

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func hostFrame(t *testing.T, requests <-chan HostRequest) HostRequest {
	t.Helper()
	select {
	case r := <-requests:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("missing host frame")
		return HostRequest{}
	}
}

func TestHostIdentityCancellationAndCleanupDeadline(t *testing.T) {
	frames := make(chan HostRequest, 8)
	h, _ := NewHostExecutor(func(r HostRequest) error { frames <- r; return nil }, 20*time.Millisecond)
	task := Task{ID: "job", Owner: Owner{"ctx", "run"}, Kind: "host", Host: &HostCall{Tool: "trusted", Args: map[string]json.RawMessage{}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Result, 1)
	go func() { done <- h.Execute(ctx, task) }()
	r := hostFrame(t, frames)
	if err := h.Complete(r.ExecutionID, Owner{"other", "run"}, task.ID, Result{}); err == nil {
		t.Fatal("wrong owner accepted")
	}
	cancel()
	if got := hostFrame(t, frames); got.Type != "host_cancel" || got.ExecutionID != r.ExecutionID {
		t.Fatal(got)
	}
	select {
	case <-done:
		t.Fatal("cancellation request pretended cleanup")
	default:
	}
	if err := h.Complete(r.ExecutionID, task.Owner, task.ID, Result{State: "cancelled", Host: &HostReceipt{ProcessCleanup: "confirmed"}}); err != nil {
		t.Fatal(err)
	}
	if got := <-done; got.State != "cancelled" {
		t.Fatal(got)
	}
	if err := h.Complete(r.ExecutionID, task.Owner, task.ID, Result{}); err == nil {
		t.Fatal("expired identity accepted")
	}
	ctx, cancel = context.WithCancel(context.Background())
	go func() { done <- h.Execute(ctx, task) }()
	r = hostFrame(t, frames)
	cancel()
	hostFrame(t, frames)
	if got := <-done; got.State != "unknown" {
		t.Fatal(got)
	}
}

func TestHostUnknownSurvivesAckArchiveCompactionAndRestart(t *testing.T) {
	frames := make(chan HostRequest, 8)
	h, _ := NewHostExecutor(func(r HostRequest) error { frames <- r; return nil }, time.Second)
	cfg := Config{Journal: filepath.Join(t.TempDir(), "wal"), Concurrency: 2, Capacity: 8, Executors: map[string]Executor{"host": h}}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	a, b := Owner{"A", "r"}, Owner{"B", "r"}
	e.OpenRun(a, 1)
	e.OpenRun(b, 1)
	task := Task{ID: "one", Owner: a, Revision: 1, Kind: "host", Host: &HostCall{Tool: "trusted", Args: map[string]json.RawMessage{}}, Resources: []Resource{{"global-host", "write"}}}
	if _, err = e.Submit(task); err != nil {
		t.Fatal(err)
	}
	r := hostFrame(t, frames)
	if err = h.Complete(r.ExecutionID, a, task.ID, Result{State: "completed", Host: &HostReceipt{ProcessCleanup: "unknown"}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for e.Stats().Unknown != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.Stats().Unknown != 1 {
		t.Fatal("unknown result lost fence")
	}
	if status, err := e.Submit(task); err != nil || status.State != "unknown" {
		t.Fatal(status, err)
	}
	events := e.Pending(0)
	if len(events) != 1 {
		t.Fatal(events)
	}
	if err = e.Ack(a, events[0].Sequence); err != nil {
		t.Fatal(err)
	}
	if err = e.CloseRun(a); err != nil {
		t.Fatal(err)
	}
	if err = e.Compact(); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	task.Owner = b
	task.ID = "two"
	if _, err = e.Submit(task); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	select {
	case got := <-frames:
		t.Fatal("unknown resource was released", got)
	default:
	}
	if e.Stats().Unknown != 1 {
		t.Fatal("restart dropped unknown fence")
	}
}
