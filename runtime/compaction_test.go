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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func sealForTest(t *testing.T, e *Engine) *sealedSegment {
	t.Helper()
	unlock := e.lock("compact", ownerA, "")
	defer unlock()
	if err := e.startCompaction(record{}); err != nil {
		t.Fatal(err)
	}
	return e.journal.sealed
}

func waitSegment(t *testing.T, segment *sealedSegment) {
	t.Helper()
	select {
	case <-segment.done:
	case <-time.After(5 * time.Second):
		t.Fatal("builder did not retire")
	}
}

func TestBackgroundImmutableSealedTailFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.jsonl")
	partial := []byte(`{"version":2,"run":`)
	if err := os.WriteFile(path+".sealed", partial, 0600); err != nil {
		t.Fatal(err)
	}
	if e, err := New(durableConfig(path)); err == nil {
		_ = e.Close(context.Background())
		t.Fatal("truncated immutable segment was accepted")
	}
	data, err := os.ReadFile(path + ".sealed")
	if err != nil || string(data) != string(partial) {
		t.Fatal("immutable evidence was truncated", string(data), err)
	}
}

func TestBackgroundOverlayPublicationAndCursor(t *testing.T) {
	config := durableConfig(filepath.Join(t.TempDir(), "runtime.jsonl"))
	config.SegmentBytes = 1 << 20
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	config.compactionHook = func(phase string) error {
		if phase == "index_record" {
			once.Do(func() { close(blocked); <-release })
		}
		return nil
	}
	e := testEngine(t, config)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var events []Completion
	for n := 0; n < 2; n++ {
		job := task(fmt.Sprint(n), ownerA)
		submit(t, e, job)
		events = append(events, completion(t, e, ownerA, job.ID))
	}
	if err := e.Ack(ownerA, events[0].Sequence); err != nil {
		t.Fatal(err)
	}
	if err := e.CloseRun(ownerA); err != nil {
		t.Fatal(err)
	}
	segment := sealForTest(t, e)
	receive(t, blocked)
	// Partially built disk files must not duplicate facts or resurrect old Runs.
	page, err := e.ArchivePage(ownerA, PageOptions{Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Items[0].Sequence != events[0].Sequence || page.NextCursor == "" {
		t.Fatal(page, err)
	}
	if status, err := e.Submit(task("0", ownerA)); err != nil || status.State != "completed" {
		t.Fatal(status, err)
	}
	if err := e.OpenRun(ownerB, 2); err != nil {
		t.Fatal(err)
	}
	if err := e.CloseRun(ownerB); err != nil {
		t.Fatal(err)
	}
	if receipt := e.Archive(ownerB); receipt.Custodian != "runtime_archive" {
		t.Fatal(receipt)
	}
	if err := e.OpenRun(ownerB, 1); err == nil {
		t.Fatal("sealed old Run state overrode active close")
	}
	close(release)
	waitSegment(t, segment)
	next, err := e.ArchivePage(ownerA, PageOptions{Cursor: page.NextCursor, Limit: 1})
	if err != nil || len(next.Items) != 1 || next.Items[0].Sequence != events[1].Sequence || next.NextCursor != "" {
		t.Fatal(next, err)
	}
	if receipt := e.Archive(ownerB); receipt.Custodian != "runtime_archive" {
		t.Fatal("published stale Run", receipt)
	}
	if err := e.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	next, err = e.ArchivePage(ownerA, PageOptions{Cursor: page.NextCursor, Limit: 1})
	if err != nil || len(next.Items) != 1 || next.Items[0].Sequence != events[1].Sequence {
		t.Fatal(next, err)
	}
}

func TestBackgroundBacklogIsBoundedAndReadersContinue(t *testing.T) {
	config := durableConfig(filepath.Join(t.TempDir(), "runtime.jsonl"))
	config.SegmentBytes = 2048
	config.MaxActiveRuns = 3
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	config.compactionHook = func(phase string) error {
		if phase == "index_record" {
			once.Do(func() { close(blocked); <-release })
		}
		return nil
	}
	e := testEngine(t, config)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	submit(t, e, task("visible", ownerA))
	visible := completion(t, e, ownerA, "visible")
	// Force only the seal, not the explicit Compact indexing barrier.
	e.mu.Lock()
	hasSegment := e.journal.sealed != nil
	e.mu.Unlock()
	if !hasSegment {
		sealForTest(t, e)
	}
	receive(t, blocked)
	var written atomic.Int32
	finished := make(chan error, 1)
	go func() {
		for n := 0; n < 32; n++ {
			owner := Owner{"churn", fmt.Sprint(n)}
			if err := e.OpenRun(owner, 1); err != nil {
				finished <- err
				return
			}
			if err := e.CloseRun(owner); err != nil {
				finished <- err
				return
			}
			written.Add(1)
		}
		finished <- nil
	}()
	deadline := time.Now().Add(5 * time.Second)
	for e.Stats().CompactionWaits == 0 {
		if time.Now().After(deadline) {
			t.Fatal("writer did not apply backpressure")
		}
		time.Sleep(time.Millisecond)
	}
	before := e.Stats()
	t.Logf("saturated snapshot: %+v", before)
	count := written.Load()
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelClose()
	closeResult := make(chan error, 1)
	go func() { closeResult <- e.Close(closeCtx) }()
	if err := receive(t, closeResult); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close blocked behind saturated mutation gate", err)
	}
	if before.PendingSegments != 1 || before.CompactionWorkers != 1 || before.PendingBytes > config.SegmentBytes+1024 || before.JournalBytes > config.SegmentBytes+1024 {
		t.Fatal("unbounded segments", before)
	}
	for n := 0; n < 20; n++ {
		status, err := e.Status(ownerA, "visible")
		if err != nil || status.State != "completed" {
			t.Fatal(status, err)
		}
		pending := e.Pending(0)
		if len(pending) != 1 || pending[0].Sequence != visible.Sequence {
			t.Fatal("delivery blocked or lost", pending)
		}
		stats := e.Stats()
		if stats.CachedTasks != before.CachedTasks || stats.CachedEvents != before.CachedEvents || stats.CachedRuns != before.CachedRuns || stats.PendingBytes != before.PendingBytes || stats.JournalBytes != before.JournalBytes || written.Load() != count {
			t.Fatal("backlog grew while saturated", stats)
		}
	}
	close(release)
	if err := receive(t, finished); err != nil {
		t.Fatal(err)
	}
	if err := e.Compact(); err != nil {
		t.Fatal(err)
	}
	if stats := e.Stats(); stats.PendingSegments != 0 || stats.CompactionWorkers != 0 {
		t.Fatal(stats)
	}
	t.Logf("drained snapshot: %+v", e.Stats())
}

func TestBackgroundCloseDeadlineRetainsOwnership(t *testing.T) {
	config := durableConfig(filepath.Join(t.TempDir(), "runtime.jsonl"))
	config.SegmentBytes = 1 << 20
	blocked, release := make(chan struct{}), make(chan struct{})
	config.compactionHook = func(phase string) error {
		if phase == "built" {
			close(blocked)
			<-release
		}
		return nil
	}
	e, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		_ = e.Close(context.Background())
	}()
	if err = e.OpenRun(ownerA, 1); err != nil {
		t.Fatal(err)
	}
	sealForTest(t, e)
	receive(t, blocked)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- e.Close(ctx) }()
	if err = receive(t, closed); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close must time out without abandoning the builder", err)
	}
	if second, err := New(config); err == nil {
		_ = second.Close(context.Background())
		t.Fatal("timed-out close released journal ownership")
	}
	close(release)
	if err = e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	config.compactionHook = nil
	second, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Close(context.Background())
}

func TestBackgroundCloseDeadlineDuringCancellationPersistence(t *testing.T) {
	config := durableConfig(filepath.Join(t.TempDir(), "runtime.jsonl"))
	config.SegmentBytes = 1 << 20
	blocked, release, started := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	var once sync.Once
	config.compactionHook = func(phase string) error {
		if phase == "built" {
			once.Do(func() { close(blocked); <-release })
		}
		return nil
	}
	config.Executors["test"] = ExecutorFunc(func(ctx context.Context, _ Task) Result {
		started <- struct{}{}
		<-ctx.Done()
		return Result{State: "cancelled"}
	})
	e := testEngine(t, config)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	submit(t, e, task("running", ownerA))
	receive(t, started)
	sealForTest(t, e)
	receive(t, blocked)
	e.mu.Lock()
	e.config.SegmentBytes = 1
	e.mu.Unlock()
	if err := e.OpenRun(ownerB, 2); err != nil {
		t.Fatal(err)
	} // Fill active, leave the gate free.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- e.Close(ctx) }()
	if err := receive(t, closed); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cancellation persistence ignored close deadline", err)
	}
	if stats := e.Stats(); stats.Running != 1 {
		t.Fatal("timeout released an unstopped execution", stats)
	}
	close(release)
	if err := e.Close(context.Background()); err != nil {
		t.Fatal("close did not retry pending cancellation", err)
	}
}

func TestBackgroundFailureRetainsRecoverableWAL(t *testing.T) {
	for _, phase := range []string{"index_record", "built", "published", "visible", "retired"} {
		t.Run(phase, func(t *testing.T) {
			config := durableConfig(filepath.Join(t.TempDir(), "runtime.jsonl"))
			config.SegmentBytes = 1 << 20
			var armed atomic.Bool
			config.compactionHook = func(at string) error {
				if armed.Load() && at == phase {
					return errors.New("injected " + phase)
				}
				return nil
			}
			e, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			if err = e.OpenRun(ownerA, 1); err != nil {
				t.Fatal(err)
			}
			submit(t, e, task("once", ownerA))
			event := completion(t, e, ownerA, "once")
			if err = e.CloseRun(ownerA); err != nil {
				t.Fatal(err)
			}
			armed.Store(true)
			segment := sealForTest(t, e)
			waitSegment(t, segment)
			if _, err = e.ArchivePage(ownerA, PageOptions{}); !errors.Is(err, ErrStorageUnknown) {
				t.Fatal(err)
			}
			if err = e.Close(context.Background()); !errors.Is(err, ErrStorageUnknown) {
				t.Fatal(err)
			}
			config.compactionHook = nil
			e, err = New(config)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close(context.Background())
			page, err := e.ArchivePage(ownerA, PageOptions{})
			if err != nil || len(page.Items) != 1 || page.Items[0].Sequence != event.Sequence {
				t.Fatal(page, err)
			}
			if status, err := e.Submit(task("once", ownerA)); err != nil || status.State != "completed" {
				t.Fatal(status, err)
			}
		})
	}
}

// Child runs real fsync/index/rename boundaries; the parent kills it without
// graceful shutdown and reopens the same journal with no execution replay.
func TestBackgroundCrashHelper(t *testing.T) {
	if os.Getenv("FOCUS_COMPACTION_CRASH_HELPER") != "1" {
		return
	}
	path, phase := os.Getenv("FOCUS_COMPACTION_PATH"), os.Getenv("FOCUS_COMPACTION_PHASE")
	config := durableConfig(path)
	config.SegmentBytes = 1 << 20
	started := make(chan bool, 1)
	config.Executors["test"] = ExecutorFunc(func(_ context.Context, job Task) Result {
		if job.ID == "uncertain" {
			started <- true
			select {}
		}
		return Result{State: "completed", Output: `{"value":9007199254740993}`}
	})
	var armed atomic.Bool
	var cursor string
	boundary, activeReady := make(chan struct{}, 1), make(chan struct{})
	concurrent := phase != "sealed" && phase != "active"
	config.compactionHook = func(at string) error {
		if armed.Load() && at == phase {
			if concurrent {
				boundary <- struct{}{}
				<-activeReady
			}
			_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"phase": at, "cursor": cursor})
			select {}
		}
		return nil
	}
	e, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, who := range []Owner{ownerA, ownerB} {
		if err = e.OpenRun(who, 1); err != nil {
			t.Fatal(err)
		}
	}
	for n := 0; n < 2; n++ {
		id := fmt.Sprint(n)
		submit(t, e, task(id, ownerA))
		completion(t, e, ownerA, id)
	}
	if err = e.CloseRun(ownerA); err != nil {
		t.Fatal(err)
	}
	page, err := e.ArchivePage(ownerA, PageOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	cursor = page.NextCursor
	submit(t, e, task("parent", ownerB))
	parent := completion(t, e, ownerB, "parent")
	if err = e.Ack(ownerB, parent.Sequence); err != nil {
		t.Fatal(err)
	}
	uncertain := task("uncertain", ownerB)
	uncertain.Resources = []Resource{{Name: "uncertain-resource", Mode: "write"}}
	submit(t, e, uncertain)
	receive(t, started)
	ownerC := Owner{"C", "active-tail"}
	if err = e.OpenRun(ownerC, 1); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	sealForTest(t, e)
	if concurrent {
		receive(t, boundary)
		// The builder has older C revision 1 in its immutable overlay. Persist
		// a newer result, ack, revision and close in the active WAL before kill.
		submit(t, e, task("newer", ownerC))
		event := completion(t, e, ownerC, "newer")
		if err = e.Ack(ownerC, event.Sequence); err != nil {
			t.Fatal(err)
		}
		if err = e.OpenRun(ownerC, 2); err != nil {
			t.Fatal(err)
		}
		if err = e.CloseRun(ownerC); err != nil {
			t.Fatal(err)
		}
		close(activeReady)
	}
	select {}
}

func TestBackgroundCrashRecoveryAtSwitchBoundaries(t *testing.T) {
	for _, phase := range []string{"sealed", "active", "index_record", "built", "published", "visible", "retired"} {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "runtime.jsonl")
			cmd := exec.Command(os.Args[0], "-test.run=^TestBackgroundCrashHelper$")
			cmd.Env = append(os.Environ(), "FOCUS_COMPACTION_CRASH_HELPER=1", "FOCUS_COMPACTION_PATH="+path, "FOCUS_COMPACTION_PHASE="+phase)
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			ready := make(chan string, 1)
			go func() {
				scanner := bufio.NewScanner(out)
				if scanner.Scan() {
					ready <- scanner.Text()
				} else {
					ready <- ""
				}
			}()
			line := receive(t, ready)
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			var marker struct{ Phase, Cursor string }
			if err = json.Unmarshal([]byte(line), &marker); err != nil || marker.Phase != phase || marker.Cursor == "" {
				t.Fatal(line, err)
			}
			config := durableConfig(path)
			config.SegmentBytes = 1 << 20
			var calls atomic.Int32
			config.Executors["test"] = ExecutorFunc(func(context.Context, Task) Result { calls.Add(1); return Result{State: "completed"} })
			bound := make(chan Task, 1)
			config.Executors["python"] = ExecutorFunc(func(_ context.Context, job Task) Result { bound <- job; return Result{State: "completed"} })
			e, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close(context.Background())
			if phase != "sealed" && phase != "active" {
				ownerC := Owner{"C", "active-tail"}
				if receipt := e.Archive(ownerC); receipt.Custodian != "runtime_archive" || receipt.Unacknowledged != 0 {
					t.Fatal("sealed state overrode newer active close/ack", receipt)
				}
				if status, err := e.Submit(task("newer", ownerC)); err != nil || status.State != "completed" || calls.Load() != 0 {
					t.Fatal("active result lost/replayed", status, err)
				}
				if err := e.OpenRun(ownerC, 1); err == nil {
					t.Fatal("closed revised Run resurrected")
				}
			}
			if status, err := e.Submit(task("0", ownerA)); err != nil || status.State != "completed" || calls.Load() != 0 {
				t.Fatal(status, err, calls.Load())
			}
			page, err := e.ArchivePage(ownerA, PageOptions{Cursor: marker.Cursor, Limit: 1})
			if err != nil || len(page.Items) != 1 || page.Items[0].Task.ID != "1" {
				t.Fatal(page, err)
			}
			unknown := completion(t, e, ownerB, "uncertain")
			if unknown.Result.State != "unknown" {
				t.Fatal(unknown)
			}
			child := task("bound", ownerB)
			child.Kind = "python"
			child.Python = &PythonCall{Interpreter: "python", Module: "tools", Function: "work"}
			child.Dependencies = []string{"parent"}
			child.Bindings = map[string]Binding{"value": {TaskID: "parent", Path: []string{"value"}}}
			submit(t, e, child)
			if got := receive(t, bound); string(got.Python.Args["value"]) != "9007199254740993" {
				t.Fatal(got)
			}
			completion(t, e, ownerB, "bound")
			conflict := task("conflict", ownerB)
			conflict.Resources = []Resource{{Name: "uncertain-resource", Mode: "read"}}
			submit(t, e, conflict)
			if err = e.Compact(); err != nil {
				t.Fatal(err)
			}
			if status, err := e.Status(ownerB, "conflict"); err != nil || status.State != "queued" || calls.Load() != 0 {
				t.Fatal("unknown fence released", status, err, calls.Load())
			}
		})
	}
}
