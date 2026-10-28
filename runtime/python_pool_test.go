package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const poolTools = `import os, sys, time, threading, asyncio
from pathlib import Path
counter = 0
def state(change=False):
 global counter
 counter += 1
 result = {"counter": counter, "cwd": os.getcwd(), "dirty": os.environ.get("FOCUS_POOL_DIRTY")}
 print("diagnostics from this call")
 if change:
  os.environ["FOCUS_POOL_DIRTY"] = "yes"
  sys.path.append("leaked-search-path")
  os.chdir("sub")
 return result
def hold(name="one"):
 Path("started-" + name).write_text("yes")
 while not Path("gate").exists(): time.sleep(.005)
 return name
def die():
 Path("once").write_text(Path("once").read_text() + "x" if Path("once").exists() else "x")
 os._exit(7)
def large(): return "x" * 20000
def simple(): return 42
def fail(): raise ValueError("deliberate failure")
def exit_hook():
 import atexit
 atexit.register(lambda: None)
 return True
def wrong_frame():
 os.write(1, b'{"id":"wrong","state":"completed","output":"true","reusable":true}\n')
 return True
def thread():
 def later():
  while not Path("let-go").exists(): time.sleep(.005)
  Path("pollution").write_text("bad")
 threading.Thread(target=later, daemon=True).start()
 return True
async def pending():
 asyncio.create_task(asyncio.sleep(60))
 return True
def manual_loop():
 loop = asyncio.new_event_loop()
 asyncio.set_event_loop(loop)
 async def later():
  await asyncio.sleep(0)
  Path("pollution").write_text("bad")
 loop.create_task(later())
 return True
`

type poolWorkspace struct{ directory, interpreter string }

func poolFixture(t *testing.T, limit, maxTasks, outputLimit int) (PythonExecutor, poolWorkspace) {
	t.Helper()
	python, err := exec.LookPath("python")
	if err != nil {
		t.Skip("Python not installed")
	}
	directory := t.TempDir()
	if err = os.Mkdir(filepath.Join(directory, "sub"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "tools.py"), []byte(poolTools), 0600); err != nil {
		t.Fatal(err)
	}
	pool, err := NewPythonPool(limit, maxTasks, outputLimit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close() })
	return PythonExecutor{Pool: pool, Process: ProcessExecutor{OutputLimit: outputLimit}}, poolWorkspace{directory, python}
}

func poolTask(fixture poolWorkspace, id, function string, args map[string]json.RawMessage) Task {
	return Task{ID: id, Owner: ownerA, Revision: 1, Kind: "python", Directory: fixture.directory, Python: &PythonCall{Interpreter: fixture.interpreter, Module: "tools", Function: function, Args: args, Reuse: true}}
}

func waitPoolFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("worker marker timed out", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPoolReuseFreshModuleCwdEnvironmentAndOutput(t *testing.T) {
	t.Setenv("FOCUS_POOL_DIRTY", "clean")
	executor, fixture := poolFixture(t, 2, 256, 4096)
	var pid int
	for n := 0; n < 40; n++ {
		job := poolTask(fixture, fmt.Sprint(n), "state", map[string]json.RawMessage{"change": json.RawMessage(fmt.Sprint(n == 0))})
		result := executor.Execute(context.Background(), job)
		if result.State != "completed" {
			t.Fatal(result)
		}
		if pid == 0 {
			pid = result.PID
		}
		if result.PID != pid {
			t.Fatal("sequential calls did not reuse the worker")
		}
		var output struct {
			Counter int
			Cwd     string
			Dirty   any
		}
		if err := json.Unmarshal([]byte(result.Output), &output); err != nil {
			t.Fatal(err, result)
		}
		actualDirectory, actualErr := os.Stat(output.Cwd)
		expectedDirectory, expectedErr := os.Stat(job.Directory)
		// Windows temp paths may use 8.3 aliases; the worker uses a resolved
		// workspace. Assert directory identity, not spelling or case.
		if actualErr != nil || expectedErr != nil || !os.SameFile(actualDirectory, expectedDirectory) {
			t.Fatalf("working directory changed: got %q (%v), want %q (%v)", output.Cwd, actualErr, job.Directory, expectedErr)
		}
		if output.Counter != 1 || output.Dirty != "clean" {
			t.Fatal("state leaked", output)
		}
	}
	stats := executor.Pool.Stats()
	if stats.Started != 1 || stats.Completed != 40 || stats.Reused != 39 || stats.PeakLive > 2 {
		t.Fatal(stats)
	}
	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}
	if stats = executor.Pool.Stats(); stats.Live != 0 || stats.Reserved != 0 {
		t.Fatal("shutdown leaked worker/pipe readers", stats)
	}
}

func TestPoolBoundAndAdmissionAfterCapacityWait(t *testing.T) {
	executor, fixture := poolFixture(t, 1, 256, 4096)
	var released atomic.Bool
	waiting := make(chan TraceEvent, 1)
	e := testEngine(t, Config{Concurrency: 2, Capacity: 8, BeforeExecute: func(_ context.Context, task Task) error {
		if task.Owner == ownerB {
			if !released.Load() {
				t.Error("admission happened before bounded capacity became available")
			}
			return fmt.Errorf("permission revoked while waiting")
		}
		return nil
	}, Trace: func(event TraceEvent) {
		if event.Phase == "pool_wait_begin" && event.TaskID == "denied" {
			select {
			case waiting <- event:
			default:
			}
		}
	}, Executors: map[string]Executor{"python": executor}})
	first := poolTask(fixture, "hold", "hold", nil)
	submit(t, e, first)
	waitPoolFile(t, filepath.Join(first.Directory, "started-one"))
	next := poolTask(fixture, "denied", "state", nil)
	next.Owner = ownerB
	submit(t, e, next)
	receive(t, waiting)
	released.Store(true)
	if err := os.WriteFile(filepath.Join(first.Directory, "gate"), []byte("yes"), 0600); err != nil {
		t.Fatal(err)
	}
	completion(t, e, ownerA, "hold")
	denied := completion(t, e, ownerB, "denied")
	if denied.Result.State != "failed" || denied.Result.PID != 0 {
		t.Fatal(denied)
	}
	stats := executor.Pool.Stats()
	if stats.PeakLive != 1 || stats.Started != 1 {
		t.Fatal(stats)
	}
}

func TestPoolTaskContextDoesNotOwnWorkerOrNextLease(t *testing.T) {
	executor, fixture := poolFixture(t, 1, 256, 4096)
	old, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := executor.Execute(old, poolTask(fixture, "first", "state", nil))
	if first.State != "completed" {
		t.Fatal(first)
	}
	job := poolTask(fixture, "second", "hold", nil)
	out := make(chan Result, 1)
	go func() { out <- executor.Execute(context.Background(), job) }()
	waitPoolFile(t, filepath.Join(job.Directory, "started-one"))
	cancel()
	if err := os.WriteFile(filepath.Join(job.Directory, "gate"), []byte("yes"), 0600); err != nil {
		t.Fatal(err)
	}
	second := receive(t, out)
	if second.State != "completed" || second.PID != first.PID {
		t.Fatal("late cancel killed the next lease", first, second)
	}
}

func TestPoolFailuresRetireWithoutReplayOrBackgroundPollution(t *testing.T) {
	for _, function := range []string{"die", "large", "fail", "thread", "pending", "manual_loop", "exit_hook", "wrong_frame"} {
		t.Run(function, func(t *testing.T) {
			executor, fixture := poolFixture(t, 1, 256, 128)
			job := poolTask(fixture, "bad", function, nil)
			result := executor.Execute(context.Background(), job)
			if result.State != "failed" || len(result.Output) > 128 || len(result.Error) > 128 {
				t.Fatal(result)
			}
			if executor.Pool.Stats().Live != 0 {
				t.Fatal("poisoned worker returned before exit")
			}
			if function == "die" {
				data, err := os.ReadFile(filepath.Join(job.Directory, "once"))
				if err != nil || string(data) != "x" {
					t.Fatal("crashed task was replayed", string(data), err)
				}
			}
			os.WriteFile(filepath.Join(job.Directory, "let-go"), []byte("yes"), 0600)
			next := executor.Execute(context.Background(), poolTask(fixture, "good", "simple", nil))
			if next.State != "completed" || next.PID == result.PID {
				t.Fatal("worker not replaced", result, next)
			}
			if _, err := os.Stat(filepath.Join(job.Directory, "pollution")); !os.IsNotExist(err) {
				t.Fatal("background job survived worker retirement", err)
			}
		})
	}
}

func TestPoolConcurrentBoundAndWorkspaceReplacement(t *testing.T) {
	executor, fixture := poolFixture(t, 2, 256, 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan Result, 24)
	for n := 0; n < 24; n++ {
		go func(n int) {
			job := poolTask(fixture, fmt.Sprint(n), "hold", map[string]json.RawMessage{"name": json.RawMessage(fmt.Sprintf("%q", fmt.Sprint(n)))})
			results <- executor.Execute(ctx, job)
		}(n)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		files, err := os.ReadDir(fixture.directory)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, file := range files {
			if strings.HasPrefix(file.Name(), "started-") {
				count++
			}
		}
		if count >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("workers did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := os.WriteFile(filepath.Join(fixture.directory, "gate"), []byte("yes"), 0600); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 24; n++ {
		if result := receive(t, results); result.State != "completed" {
			t.Fatal(result)
		}
	}
	if stats := executor.Pool.Stats(); stats.PeakLive != 2 || stats.Started != 2 {
		t.Fatal(stats)
	}
	// A new workspace cannot inherit the old cwd/module; reuse an owned slot
	// only after the old process is stopped. Test at a one-worker hard bound.
	other := t.TempDir()
	if err := os.WriteFile(filepath.Join(other, "tools.py"), []byte("def simple():\n return 'new-workspace'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	single, firstWorkspace := poolFixture(t, 1, 256, 4096)
	first := single.Execute(ctx, poolTask(firstWorkspace, "first", "simple", nil))
	if first.State != "completed" {
		t.Fatal(first)
	}
	job := poolTask(firstWorkspace, "other", "simple", nil)
	job.Directory = other
	second := single.Execute(ctx, job)
	if second.State != "completed" || second.PID == first.PID || !strings.Contains(second.Output, "new-workspace") || single.Pool.Stats().PeakLive != 1 {
		t.Fatal(first, second, single.Pool.Stats())
	}
}

func TestPoolCancellationAndLifetimeRecycling(t *testing.T) {
	executor, fixture := poolFixture(t, 1, 2, 4096)
	job := poolTask(fixture, "cancel", "hold", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Result, 1)
	go func() { out <- executor.Execute(ctx, job) }()
	waitPoolFile(t, filepath.Join(job.Directory, "started-one"))
	cancel()
	if result := receive(t, out); result.State != "cancelled" || executor.Pool.Stats().Live != 0 {
		t.Fatal(result, executor.Pool.Stats())
	}
	var pids []int
	for n := 0; n < 4; n++ {
		result := executor.Execute(context.Background(), poolTask(fixture, fmt.Sprint(n), "state", nil))
		if result.State != "completed" {
			t.Fatal(result)
		}
		pids = append(pids, result.PID)
	}
	if pids[0] != pids[1] || pids[2] != pids[3] || pids[1] == pids[2] || executor.Pool.Stats().PeakLive > 1 {
		t.Fatal(pids, executor.Pool.Stats())
	}
}
