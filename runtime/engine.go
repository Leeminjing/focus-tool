package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	goruntime "runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type entry struct {
	Status
	cancel context.CancelFunc
}
type finished struct {
	key      string
	result   Result
	owner    Owner
	id       string
	returned time.Time
}

type Engine struct {
	mu              sync.Mutex
	mutation        chan struct{} // Cancellable ordering gate while compaction waits release mu.
	lockSpan        *engineLockSpan
	ready           bool
	config          Config
	tasks           map[string]*entry
	runs            map[Owner]RunState
	queues          map[Owner][]string
	owners          []Owner
	next            int
	active          int
	events          []Completion
	sequence        uint64
	journal         *journal
	history         *history
	fatal           error
	closing         bool
	paused          bool
	wake            chan struct{}
	changed         chan struct{}
	finished        chan finished
	done            chan struct{}
	traceOperation  string // Set only under mu; used to attribute persistence spans.
	executorsClosed bool
}

func taskKey(owner Owner, id string) string {
	data, _ := json.Marshal([]string{owner.ContextID, owner.RunID, id})
	return string(data)
}
func clone[T any](value T) T {
	data, _ := json.Marshal(value)
	var result T
	_ = json.Unmarshal(data, &result)
	return result
}

func New(config Config) (*Engine, error) {
	if config.Concurrency < 1 || config.Capacity < config.Concurrency {
		return nil, fmt.Errorf("capacity must be >= positive concurrency")
	}
	if config.PerRunCapacity == 0 {
		config.PerRunCapacity = config.Capacity / config.Concurrency
	}
	if config.PerRunCapacity < 1 {
		return nil, fmt.Errorf("per-run capacity must be positive")
	}
	if config.MaxActiveRuns == 0 {
		config.MaxActiveRuns = max(128, config.Capacity)
	}
	if config.SegmentBytes == 0 {
		config.SegmentBytes = 8 << 20
	}
	if config.CompactionMode == "" {
		config.CompactionMode = "background"
	}
	if config.CompactionMode != "background" && config.CompactionMode != "sync" {
		return nil, fmt.Errorf("compaction mode must be background or sync")
	}
	if config.MaxActiveRuns < 1 || config.SegmentBytes < 1 {
		return nil, fmt.Errorf("run and segment limits must be positive")
	}
	executors := make(map[string]Executor, len(config.Executors))
	for kind, executor := range config.Executors {
		executors[kind] = executor
	}
	config.Executors = executors
	file, history, err := openJournal(config.Journal)
	if err != nil {
		return nil, err
	}
	e := &Engine{config: config, mutation: make(chan struct{}, 1), tasks: map[string]*entry{}, runs: map[Owner]RunState{}, queues: map[Owner][]string{}, journal: file, history: history, wake: make(chan struct{}, 1), changed: make(chan struct{}, 1), finished: make(chan finished, config.Concurrency), done: make(chan struct{})}
	if history != nil {
		if err := e.restore(); err != nil {
			file.close()
			return nil, err
		}
	}
	for key, item := range e.tasks {
		if item.State == "running" || item.State == "cancel_requested" {
			if err := e.complete(key, Result{State: "unknown", Error: "runtime restarted during execution; automatic replay prohibited"}); err != nil {
				if file != nil {
					file.close()
				}
				return nil, err
			}
		}
		if item.State == "queued" {
			run, ok, err := e.runState(item.Task.Owner)
			if err != nil {
				file.close()
				return nil, err
			}
			if !ok || run.Closed || run.Revision != item.Task.Revision {
				if err := e.complete(key, Result{State: "cancelled", Error: "run closed or revised before recovery"}); err != nil {
					if file != nil {
						file.close()
					}
					return nil, err
				}
			}
		}
	}
	e.ready = true
	e.paused = config.StartPaused
	for _, item := range e.tasks {
		if item.State == "queued" {
			e.paused = true
			break
		}
	}
	go e.loop()
	e.signal()
	return e, nil
}

func (e *Engine) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}
func (e *Engine) Changed() <-chan struct{} { return e.changed }
func (e *Engine) notify() {
	select {
	case e.changed <- struct{}{}:
	default:
	}
}
func (e *Engine) enqueue(owner Owner, key string) {
	if _, ok := e.queues[owner]; !ok {
		e.owners = append(e.owners, owner)
	}
	e.queues[owner] = append(e.queues[owner], key)
}

func (e *Engine) runState(owner Owner) (RunState, bool, error) {
	if state, ok := e.runs[owner]; ok {
		return state, true, nil
	}
	if e.journal != nil {
		if state, ok := e.journal.runs[owner]; ok {
			return state, true, nil
		}
		if segment := e.journal.sealed; segment != nil {
			if state, ok := segment.runs[owner]; ok {
				return state, true, nil
			}
		}
	}
	if e.history != nil {
		return e.history.run(owner)
	}
	return RunState{}, false, nil
}

func (e *Engine) lookup(owner Owner, id string) (Status, bool, error) {
	if item := e.tasks[taskKey(owner, id)]; item != nil {
		return item.Status, true, nil
	}
	if e.journal != nil {
		if status, ok := e.journal.tasks[taskKey(owner, id)]; ok {
			return status, true, nil
		}
		if segment := e.journal.sealed; segment != nil {
			if status, ok := segment.tasks[taskKey(owner, id)]; ok {
				return status, true, nil
			}
		}
	}
	if e.history != nil {
		return e.history.status(owner, id)
	}
	return Status{}, false, nil
}

// Durable history is evicted immediately; no LRU of identities or payloads.
func (e *Engine) prune() {
	if e.history == nil {
		return
	}
	for key, item := range e.tasks {
		if terminal(item.State) && item.State != "unknown" {
			delete(e.tasks, key)
		}
	}
	for owner, state := range e.runs {
		if state.Closed {
			delete(e.runs, owner)
		}
	}
	live := e.events[:0]
	for _, event := range e.events {
		state, ok := e.runs[event.Task.Owner]
		if ok && !state.Closed && event.Sequence > state.Ack {
			live = append(live, event)
		}
	}
	clear(e.events[len(live):])
	e.events = live
	// Preserve the next surviving owner when removing empty queues. Taking the
	// old numeric index modulo the new length can let a busy Run cut in line.
	nextOwner := Owner{}
	for offset := 0; offset < len(e.owners); offset++ {
		owner := e.owners[(e.next+offset)%len(e.owners)]
		for _, key := range e.queues[owner] {
			if item := e.tasks[key]; item != nil && item.State == "queued" {
				nextOwner = owner
				break
			}
		}
		if nextOwner.ContextID != "" {
			break
		}
	}
	owners := e.owners[:0]
	for _, owner := range e.owners {
		queue := e.queues[owner][:0]
		for _, key := range e.queues[owner] {
			if item := e.tasks[key]; item != nil && item.State == "queued" {
				queue = append(queue, key)
			}
		}
		clear(e.queues[owner][len(queue):])
		if len(queue) == 0 {
			delete(e.queues, owner)
		} else {
			e.queues[owner] = queue
			owners = append(owners, owner)
		}
	}
	clear(e.owners[len(owners):])
	e.owners = owners
	e.next = 0
	for n, owner := range owners {
		if owner == nextOwner {
			e.next = n
			break
		}
	}
}

func (e *Engine) OpenRun(owner Owner, revision int) error {
	defer e.lock("open_run", owner, "")()
	if e.closing {
		return ErrClosed
	}
	if e.fatal != nil {
		return e.fatal
	}
	if owner.ContextID == "" || owner.RunID == "" || revision < 1 {
		return fmt.Errorf("owner and positive revision are required")
	}
	old, ok, err := e.runState(owner)
	if err != nil {
		return err
	}
	for other, state := range e.runs {
		if other.ContextID == owner.ContextID && other != owner && !state.Closed {
			return fmt.Errorf("context already has an active run")
		}
	}
	if ok && (old.Closed || revision < old.Revision) {
		return fmt.Errorf("run closed or revision moved backwards")
	}
	if ok && revision == old.Revision {
		return nil
	}
	if !ok && len(e.runs) >= e.config.MaxActiveRuns {
		return fmt.Errorf("active Run limit exhausted")
	}
	state := old
	state.Owner, state.Revision = owner, revision
	if err := e.save(record{Run: &state}); err != nil {
		return err
	}
	e.runs[owner] = state
	for key, item := range e.tasks {
		if item.Task.Owner == owner && item.Task.Revision != revision && !terminal(item.State) {
			if err := e.cancel(key); err != nil {
				return err
			}
		}
	}
	e.prune()
	e.signal()
	return nil
}

func (e *Engine) Submit(task Task) (Status, error) {
	if _, err := json.Marshal(task); err != nil {
		return Status{}, fmt.Errorf("invalid task JSON: %w", err)
	}
	task = clone(task)
	defer e.lock("submit", task.Owner, task.ID)()
	if e.closing {
		return Status{}, ErrClosed
	}
	if e.fatal != nil {
		return Status{}, e.fatal
	}
	key := taskKey(task.Owner, task.ID)
	old, found, err := e.lookup(task.Owner, task.ID)
	if err != nil {
		return Status{}, err
	}
	if found {
		if !reflect.DeepEqual(old.Task, task) {
			return Status{}, ErrConflict
		}
		return clone(old), nil
	}
	run, ok := e.runs[task.Owner]
	if !ok || run.Closed || task.Revision != run.Revision {
		return Status{}, fmt.Errorf("unknown run or stale revision")
	}
	if task.ID == "" || e.config.Executors[task.Kind] == nil {
		return Status{}, fmt.Errorf("task ID and registered executor are required")
	}
	if task.TimeoutMS < 0 {
		return Status{}, fmt.Errorf("negative timeout")
	}
	count := 0
	runCount := 0
	for _, item := range e.tasks {
		if !terminal(item.State) || item.State == "unknown" {
			count++
			if item.Task.Owner == task.Owner && item.State != "unknown" {
				runCount++
			}
		}
	}
	for _, event := range e.events {
		if event.Task.Owner == task.Owner && event.Sequence > run.Ack {
			runCount++
		}
	}
	if count >= e.config.Capacity {
		return Status{}, ErrQueueFull
	}
	if runCount >= e.config.PerRunCapacity {
		return Status{}, ErrRunFull
	}
	deps := map[string]bool{}
	for _, id := range task.Dependencies {
		dep, ok, err := e.lookup(task.Owner, id)
		if err != nil {
			return Status{}, err
		}
		if !ok || id == task.ID || dep.Task.Revision != task.Revision || deps[id] {
			return Status{}, fmt.Errorf("dependencies must be unique, previously submitted tasks in the same run and revision")
		}
		deps[id] = true
	}
	for _, binding := range task.Bindings {
		if task.Kind != "python" || !deps[binding.TaskID] {
			return Status{}, fmt.Errorf("bindings require a Python task and declared dependency")
		}
	}
	resources := map[string]bool{}
	for _, claim := range task.Resources {
		if claim.Name == "" || (claim.Mode != "read" && claim.Mode != "write") || resources[claim.Name] {
			return Status{}, fmt.Errorf("invalid or repeated resource claim")
		}
		resources[claim.Name] = true
	}
	if task.Kind == "process" && len(task.Command) == 0 {
		return Status{}, fmt.Errorf("empty process command")
	}
	if task.Kind == "python" && (task.Python == nil || task.Python.Interpreter == "" || task.Python.Module == "" || task.Python.Function == "") {
		return Status{}, fmt.Errorf("incomplete Python call")
	}
	status := Status{Task: task, State: "queued", Timing: Timing{QueuedNS: time.Now().UnixNano()}}
	if err := e.save(record{Task: &status}); err != nil {
		return Status{}, err
	}
	e.tasks[key] = &entry{Status: status}
	e.enqueue(task.Owner, key)
	e.signal()
	return clone(status), nil
}

func (e *Engine) Cancel(owner Owner, id string) error {
	defer e.lock("cancel", owner, id)()
	if e.closing {
		return ErrClosed
	}
	if e.fatal != nil {
		return e.fatal
	}
	key := taskKey(owner, id)
	if e.tasks[key] == nil {
		_, ok, err := e.lookup(owner, id)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("unknown task")
		}
		return nil
	}
	err := e.cancel(key)
	e.signal()
	return err
}

func (e *Engine) cancel(key string) error {
	item := e.tasks[key]
	if terminal(item.State) || item.State == "cancel_requested" {
		return nil
	}
	if item.State == "queued" {
		return e.complete(key, Result{State: "cancelled", Error: "cancelled before execution"})
	}
	status := item.Status
	status.State = "cancel_requested"
	if err := e.save(record{Task: &status}); err != nil {
		return err
	}
	item.Status = status
	if item.cancel != nil {
		item.cancel()
	}
	return nil
}

func (e *Engine) Status(owner Owner, id string) (Status, error) {
	defer e.lock("status", owner, id)()
	item, ok, err := e.lookup(owner, id)
	if err != nil {
		return Status{}, err
	}
	if !ok {
		return Status{}, fmt.Errorf("unknown task")
	}
	return clone(item), nil
}

func (e *Engine) Events(owner Owner, after uint64) []Completion {
	defer e.lock("events", owner, "")()
	var result []Completion
	if e.history != nil {
		state, ok, err := e.runState(owner)
		if err != nil {
			e.storageFailure(err)
			return nil
		}
		if !ok {
			return nil
		}
		if after < state.Ack {
			after = state.Ack
		}
		page, err := e.resultPageLocked(owner, PageOptions{After: after, Limit: 1000, MaxBytes: 4 << 20}, state.Closed)
		if err != nil {
			return nil
		}
		return page.Items
	}
	for _, event := range e.events {
		if event.Task.Owner == owner && event.Sequence > after && event.Sequence > e.runs[owner].Ack {
			result = append(result, clone(event))
		}
	}
	return result
}

func (e *Engine) Pending(after uint64) []Completion {
	defer e.lock("pending", Owner{}, "")()
	var result []Completion
	for _, event := range e.events {
		if event.Sequence > after && event.Sequence > e.runs[event.Task.Owner].Ack && !e.runs[event.Task.Owner].Closed {
			result = append(result, clone(event))
		}
	}
	return result
}

// Closed Run results transfer to the journal custodian, retaining facts and Ack.
// Inspect them explicitly through Events; they no longer enter the live pump.
func (e *Engine) Archive(owner Owner) ArchiveReceipt {
	defer e.lock("archive", owner, "")()
	receipt := ArchiveReceipt{Owner: owner, Custodian: "run"}
	state, _, err := e.runState(owner)
	if err != nil {
		e.storageFailure(err)
		return receipt
	}
	if state.Closed {
		receipt.Custodian = "runtime_archive"
	}
	for _, item := range e.tasks {
		if item.Task.Owner == owner && !terminal(item.State) {
			receipt.Pending++
		}
	}
	receipt.Unacknowledged = int(state.Facts - state.Consumed)
	return receipt
}

func (e *Engine) ArchivedEvents(owner Owner, after uint64) []Completion {
	// Legacy Go convenience method returns at most one page. The wire API and
	// Python client expose the continuation cursor explicitly.
	page, err := e.ArchivePage(owner, PageOptions{After: after})
	if err != nil {
		return nil
	}
	return page.Items
}

func (e *Engine) Stats() Stats {
	defer e.lock("stats", Owner{}, "")()
	stats := Stats{Running: e.active, Tasks: len(e.tasks), Events: len(e.events), Runs: len(e.runs), QueueOwners: len(e.owners), DispatchPaused: e.paused}
	if observer, ok := e.config.Executors["python"].(interface{ PoolStats() *PoolStats }); ok {
		stats.PythonPool = observer.PoolStats()
	}
	for _, item := range e.tasks {
		if item.State == "queued" {
			stats.Queued++
		}
		if item.State == "unknown" {
			stats.Unknown++
		}
	}
	for _, event := range e.events {
		if event.Sequence > e.runs[event.Task.Owner].Ack && !e.runs[event.Task.Owner].Closed {
			stats.Unacknowledged++
		}
	}
	var mem goruntime.MemStats
	goruntime.ReadMemStats(&mem)
	stats.HeapBytes = mem.HeapAlloc
	if e.journal != nil {
		stats.JournalBytes = e.journal.bytes
		stats.Compactions = e.journal.compactions
		stats.CachedTasks, stats.CachedEvents, stats.CachedRuns = len(e.journal.tasks), len(e.journal.events), len(e.journal.runs)
		stats.PublishedSequence = e.journal.publishedThrough
		stats.CompactionWaits = e.journal.waits
		if segment := e.journal.sealed; segment != nil {
			stats.PendingSegments, stats.PendingBytes = 1, segment.bytes
			stats.CachedTasks += len(segment.tasks)
			stats.CachedRuns += len(segment.runs)
			stats.CachedEvents += len(segment.events)
			if !segment.finished {
				stats.CompactionWorkers = 1
			}
		}
	}
	return stats
}

func (e *Engine) Ack(owner Owner, sequence uint64) error {
	defer e.lock("ack", owner, "")()
	if e.closing {
		return ErrClosed
	}
	if e.fatal != nil {
		return e.fatal
	}
	state, ok := e.runs[owner]
	if !ok {
		return fmt.Errorf("unknown run")
	}
	if state.Closed {
		return fmt.Errorf("result custody transferred to runtime_archive")
	}
	if sequence <= state.Ack {
		return nil
	}
	for _, event := range e.events {
		if event.Task.Owner == owner && event.Sequence > state.Ack {
			if event.Sequence != sequence {
				return fmt.Errorf("ack must consume the next event belonging to this run")
			}
			state.Ack = sequence
			state.Consumed++
			if err := e.save(record{Run: &state}); err != nil {
				return err
			}
			e.runs[owner] = state
			e.prune()
			return nil
		}
	}
	return fmt.Errorf("unknown event")
}

func (e *Engine) CloseRun(owner Owner) error {
	defer e.lock("close_run", owner, "")()
	if e.closing {
		return ErrClosed
	}
	if e.fatal != nil {
		return e.fatal
	}
	state, ok, err := e.runState(owner)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("unknown run")
	}
	state.Closed = true
	if err := e.save(record{Run: &state}); err != nil {
		return err
	}
	e.runs[owner] = state
	for key, item := range e.tasks {
		if item.Task.Owner == owner && !terminal(item.State) {
			if err := e.cancel(key); err != nil {
				return err
			}
		}
	}
	e.prune()
	e.signal()
	return nil
}

func (e *Engine) complete(key string, result Result) error {
	item := e.tasks[key]
	status := item.Status
	status.State = result.State
	status.Result = &result
	status.Timing.FinishedNS = time.Now().UnixNano()
	event := Completion{Sequence: e.sequence + 1, Task: item.Task, Result: result, Timing: status.Timing}
	state, ok, err := e.runState(item.Task.Owner)
	if err != nil {
		return e.storageFailure(err)
	}
	if !ok {
		return e.storageFailure(fmt.Errorf("task Run missing"))
	}
	state.Facts++
	if err := e.save(record{Task: &status, Event: &event, Run: &state}); err != nil {
		return err
	}
	item.Status = status
	e.sequence = event.Sequence
	e.events = append(e.events, event)
	e.runs[state.Owner] = state
	e.prune()
	e.notify()
	return nil
}

func (e *Engine) available(task Task) bool {
	for _, item := range e.tasks {
		if item.State != "running" && item.State != "cancel_requested" && item.State != "unknown" {
			continue
		}
		for _, held := range item.Task.Resources {
			for _, wanted := range task.Resources {
				if held.Name == wanted.Name && (held.Mode == "write" || wanted.Mode == "write") {
					return false
				}
			}
		}
	}
	return true
}

func (e *Engine) bind(task Task) (Task, error) {
	task = clone(task)
	for arg, binding := range task.Bindings {
		dep, ok, err := e.lookup(task.Owner, binding.TaskID)
		if err != nil {
			return task, err
		}
		if !ok || dep.Result == nil {
			return task, fmt.Errorf("dependency result missing")
		}
		result := dep.Result
		var value any
		if !json.Valid([]byte(result.Output)) {
			return task, fmt.Errorf("dependency output is not a complete JSON value")
		}
		decoder := json.NewDecoder(strings.NewReader(result.Output))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return task, fmt.Errorf("dependency output is not JSON: %w", err)
		}
		for _, part := range binding.Path {
			switch container := value.(type) {
			case map[string]any:
				var ok bool
				value, ok = container[part]
				if !ok {
					return task, fmt.Errorf("binding field %q missing", part)
				}
			case []any:
				n, err := strconv.Atoi(part)
				if err != nil || n < 0 || n >= len(container) {
					return task, fmt.Errorf("invalid binding index")
				}
				value = container[n]
			default:
				return task, fmt.Errorf("binding traverses a scalar")
			}
		}
		if task.Python.Args == nil {
			task.Python.Args = map[string]json.RawMessage{}
		}
		task.Python.Args[arg], _ = json.Marshal(value)
	}
	return task, nil
}

// Round robin across run owners; within a run, skip tasks blocked on dependencies
// or resource claims so independent work does not suffer head-of-line blocking.
func (e *Engine) dispatch() bool {
	for attempt := 0; attempt < len(e.owners); attempt++ {
		index := (e.next + attempt) % len(e.owners)
		owner := e.owners[index]
		pending := e.queues[owner][:0]
		for _, key := range e.queues[owner] {
			if e.tasks[key] != nil && e.tasks[key].State == "queued" {
				pending = append(pending, key)
			}
		}
		e.queues[owner] = pending
		for _, key := range e.queues[owner] {
			item := e.tasks[key]
			if item.State != "queued" {
				continue
			}
			ready, failed := true, false
			for _, id := range item.Task.Dependencies {
				dep, ok, err := e.lookup(owner, id)
				if err != nil {
					e.storageFailure(err)
					return false
				}
				if !ok {
					e.storageFailure(fmt.Errorf("dependency missing"))
					return false
				}
				if terminal(dep.State) && dep.State != "completed" {
					failed = true
				}
				if dep.State != "completed" {
					ready = false
				}
			}
			if failed {
				if e.complete(key, Result{State: "skipped", Error: "dependency did not complete successfully"}) != nil {
					return false
				}
				return true
			}
			if !ready || !e.available(item.Task) {
				continue
			}
			task, err := e.bind(item.Task)
			if err != nil {
				if e.complete(key, Result{State: "failed", Error: err.Error()}) != nil {
					return false
				}
				return true
			}
			status := item.Status
			status.State = "running"
			status.Timing.StartedNS = time.Now().UnixNano()
			if e.config.Trace != nil {
				e.config.Trace(TraceEvent{Phase: "dispatch_assigned", Owner: owner, TaskID: item.Task.ID, StartNS: status.Timing.StartedNS, EndNS: status.Timing.StartedNS})
			}
			if e.save(record{Task: &status}) != nil {
				return false
			}
			item.Status = status
			ctx, cancel := context.WithCancel(context.Background())
			if task.TimeoutMS > 0 {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), time.Duration(task.TimeoutMS)*time.Millisecond)
			}
			item.cancel = cancel
			e.active++
			e.next = index + 1
			e.prune()
			go e.execute(ctx, key, task)
			return true
		}
	}
	return false
}

func (e *Engine) execute(ctx context.Context, key string, task Task) {
	result := Result{State: "failed"}
	ctx = withExecutionTrace(ctx, e.config.Trace, task)
	defer func() {
		if recovered := recover(); recovered != nil {
			result = Result{State: "failed", Error: fmt.Sprint("executor panic: ", recovered)}
		}
		returned := time.Now()
		e.finished <- finished{key: key, result: result, owner: task.Owner, id: task.ID, returned: returned}
	}()
	executor := e.config.Executors[task.Kind]
	if preparer, ok := executor.(PreparingExecutor); ok {
		start := time.Now()
		prepared, err := preparer.Prepare(ctx, task)
		e.traceSpan("executor_prepare", task, "", start)
		if err != nil {
			result.Error = err.Error()
			if ctx.Err() != nil {
				result.State = "cancelled"
			}
			return
		}
		defer prepared.Release()
		executor = prepared
	}
	admissionStart := time.Now()
	if e.config.BeforeExecute != nil {
		if err := e.config.BeforeExecute(ctx, task); err != nil {
			e.traceSpan("admission", task, "denied", admissionStart)
			result.Error = "execution admission: " + err.Error()
			if ctx.Err() != nil {
				result.State = "cancelled"
			}
			return
		}
	}
	e.traceSpan("admission", task, "allowed", admissionStart)
	if ctx.Err() != nil {
		result = Result{State: "cancelled", Error: ctx.Err().Error()}
		return
	}
	result = executor.Execute(ctx, task)
	if !terminal(result.State) || result.State == "unknown" || result.State == "skipped" {
		result = Result{State: "failed", Error: "executor returned invalid terminal state"}
	}
}

func (e *Engine) loop() {
	defer close(e.done)
	for {
		unlock := e.lock("dispatch", Owner{}, "")
		for !e.closing && !e.paused && e.fatal == nil && e.active < e.config.Concurrency && e.dispatch() {
		}
		stop := (e.closing || e.fatal != nil) && e.active == 0
		unlock()
		if stop {
			return
		}
		select {
		case <-e.wake:
		case outcome := <-e.finished:
			unlock := e.lock("completion", outcome.owner, outcome.id)
			e.traceSpan("completion_handoff", Task{Owner: outcome.owner, ID: outcome.id}, "", outcome.returned)
			item := e.tasks[outcome.key]
			item.cancel()
			item.cancel = nil
			e.active--
			_ = e.complete(outcome.key, outcome.result)
			unlock()
		}
	}
}

func (e *Engine) Close(ctx context.Context) error {
	unlock, err := e.lockContext(ctx, "close", Owner{}, "")
	if err != nil {
		return err
	}
	e.closing = true
	// Retry unfinished cancellations after a previous close timed out before
	// its WAL append. A cancel request alone never releases execution resources.
	for key, item := range e.tasks {
		if !terminal(item.State) {
			if err := e.cancel(key); err != nil {
				if e.fatal != nil {
					break
				}
				unlock()
				e.signal()
				return err
			}
		}
	}
	unlock()
	e.signal()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
	}
	unlock, err = e.lockContext(ctx, "shutdown", Owner{}, "")
	if err != nil {
		return err
	}
	defer unlock()
	if e.journal != nil && e.journal.sealed != nil {
		// A completed execution is not permission to abandon an index builder
		// that still owns the journal and its stable lock.
		if err := e.joinCompaction(ctx, e.journal.sealed, "shutdown"); err != nil {
			return err // Keep journal/lock ownership until a later Close can join.
		}
	}
	if !e.executorsClosed {
		e.executorsClosed = true
		for _, executor := range e.config.Executors {
			if closer, ok := executor.(io.Closer); ok {
				if err := closer.Close(); err != nil && e.fatal == nil {
					e.fatal = err
				}
			}
		}
	}
	if e.journal != nil {
		err := e.journal.close()
		e.journal = nil
		if e.fatal == nil {
			e.fatal = err
		}
	}
	return e.fatal
}
