package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestRecoveredQueueRequiresTrustedInventoryAndCurrentAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.jsonl")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	state := RunState{Owner: ownerA, Revision: 1}
	job := task("restored", ownerA)
	status := Status{Task: job, State: "queued"}
	enc := json.NewEncoder(file)
	for _, item := range []record{{Version: 2, Run: &state}, {Version: 2, Task: &status}} {
		if err = enc.Encode(item); err != nil {
			t.Fatal(err)
		}
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	config := durableConfig(path)
	var allowed atomic.Bool
	var calls atomic.Int32
	config.BeforeExecute = func(context.Context, Task) error {
		if !allowed.Load() {
			return fmt.Errorf("current authority revoked")
		}
		return nil
	}
	config.Executors["test"] = ExecutorFunc(func(context.Context, Task) Result { calls.Add(1); return Result{State: "completed"} })
	e, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	if !e.Stats().DispatchPaused || e.Stats().Queued != 1 {
		t.Fatal(e.Stats())
	}
	if err = e.ResumeDispatch(nil); err == nil {
		t.Fatal("unregistered queue resumed")
	}
	if err = e.ResumeDispatch([]TaskIdentity{{Owner: ownerB, TaskID: job.ID}}); err == nil {
		t.Fatal("another Context's registration resumed queue")
	}
	if calls.Load() != 0 {
		t.Fatal("recovery raced ahead of trusted registration")
	}
	if err = e.ResumeDispatch([]TaskIdentity{{Owner: ownerA, TaskID: job.ID}}); err != nil {
		t.Fatal(err)
	}
	if result := completion(t, e, ownerA, job.ID); result.Result.State != "failed" || calls.Load() != 0 {
		t.Fatal(result, calls.Load())
	}
	if err = e.PauseDispatch(); err != nil {
		t.Fatal(err)
	}
	allowed.Store(true)
	submit(t, e, task("new", ownerA))
	if !e.Stats().DispatchPaused || calls.Load() != 0 {
		t.Fatal("pause did not hold queue")
	}
	if err = e.ResumeDispatch([]TaskIdentity{{Owner: ownerA, TaskID: "new"}}); err != nil {
		t.Fatal(err)
	}
	if result := completion(t, e, ownerA, "new"); result.Result.State != "completed" || calls.Load() != 1 {
		t.Fatal(result, calls.Load())
	}
}
