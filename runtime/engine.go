package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	key    string
	result Result
}

type Engine struct {
	mu       sync.Mutex
	config   Config
	tasks    map[string]*entry
	runs     map[Owner]RunState
	queues   map[Owner][]string
	owners   []Owner
	next     int
	active   int
	events   []Completion
	sequence uint64
	journal  *os.File
	fatal    error
	closing  bool
	wake     chan struct{}
	changed  chan struct{}
	finished chan finished
	done     chan struct{}
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
	executors := make(map[string]Executor, len(config.Executors))
	for kind, executor := range config.Executors {
		executors[kind] = executor
	}
	config.Executors = executors
	file, records, err := openJournal(config.Journal)
	if err != nil {
		return nil, err
	}
	e := &Engine{config: config, tasks: map[string]*entry{}, runs: map[Owner]RunState{}, queues: map[Owner][]string{}, journal: file, wake: make(chan struct{}, 1), changed: make(chan struct{}, 1), finished: make(chan finished, config.Concurrency), done: make(chan struct{})}
	for _, item := range records {
		if item.Run != nil {
			e.runs[item.Run.Owner] = *item.Run
		}
		if item.Task != nil {
			status := *item.Task
			key := taskKey(status.Task.Owner, status.Task.ID)
			if _, ok := e.tasks[key]; !ok {
				e.enqueue(status.Task.Owner, key)
			}
			e.tasks[key] = &entry{Status: status}
		}
		if item.Event != nil {
			e.events = append(e.events, *item.Event)
			if item.Event.Sequence > e.sequence {
				e.sequence = item.Event.Sequence
			}
		}
	}
	for key, item := range e.tasks {
		if item.State == "running" || item.State == "cancel_requested" {
			if err := e.complete(key, Result{State: "unknown", Error: "runtime restarted during execution; automatic replay prohibited"}); err != nil {
				if file != nil {
					file.Close()
				}
				return nil, err
			}
		}
		if item.State == "queued" {
			run, ok := e.runs[item.Task.Owner]
			if !ok || run.Closed || run.Revision != item.Task.Revision {
				if err := e.complete(key, Result{State: "cancelled", Error: "run closed or revised before recovery"}); err != nil {
					if file != nil {
						file.Close()
					}
					return nil, err
				}
			}
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

func (e *Engine) OpenRun(owner Owner, revision int) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closing {
		return ErrClosed
	}
	if e.fatal != nil {
		return e.fatal
	}
	if owner.ContextID == "" || owner.RunID == "" || revision < 1 {
		return fmt.Errorf("owner and positive revision are required")
	}
	old, ok := e.runs[owner]
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
	state := RunState{Owner: owner, Revision: revision, Ack: old.Ack}
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
	e.signal()
	return nil
}

func (e *Engine) Submit(task Task) (Status, error) {
	if _, err := json.Marshal(task); err != nil {
		return Status{}, fmt.Errorf("invalid task JSON: %w", err)
	}
	task = clone(task)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closing {
		return Status{}, ErrClosed
	}
	if e.fatal != nil {
		return Status{}, e.fatal
	}
	key := taskKey(task.Owner, task.ID)
	if old, ok := e.tasks[key]; ok {
		if !reflect.DeepEqual(old.Task, task) {
			return Status{}, ErrConflict
		}
		return clone(old.Status), nil
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
		if !terminal(item.State) {
			count++
			if item.Task.Owner == task.Owner {
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
		dep, ok := e.tasks[taskKey(task.Owner, id)]
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
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closing {
		return ErrClosed
	}
	if e.fatal != nil {
		return e.fatal
	}
	key := taskKey(owner, id)
	if e.tasks[key] == nil {
		return fmt.Errorf("unknown task")
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
	e.mu.Lock()
	defer e.mu.Unlock()
	item := e.tasks[taskKey(owner, id)]
	if item == nil {
		return Status{}, fmt.Errorf("unknown task")
	}
	return clone(item.Status), nil
}

func (e *Engine) Events(owner Owner, after uint64) []Completion {
	e.mu.Lock()
	defer e.mu.Unlock()
	var result []Completion
	for _, event := range e.events {
		if event.Task.Owner == owner && event.Sequence > after && event.Sequence > e.runs[owner].Ack {
			result = append(result, clone(event))
		}
	}
	return result
}

func (e *Engine) Pending(after uint64) []Completion {
	e.mu.Lock()
	defer e.mu.Unlock()
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
	e.mu.Lock()
	defer e.mu.Unlock()
	receipt := ArchiveReceipt{Owner: owner, Custodian: "run"}
	if e.runs[owner].Closed {
		receipt.Custodian = "runtime_archive"
	}
	for _, item := range e.tasks {
		if item.Task.Owner == owner && !terminal(item.State) {
			receipt.Pending++
		}
	}
	for _, event := range e.events {
		if event.Task.Owner == owner && event.Sequence > e.runs[owner].Ack {
			receipt.Unacknowledged++
		}
	}
	return receipt
}

func (e *Engine) ArchivedEvents(owner Owner, after uint64) []Completion {
	e.mu.Lock()
	defer e.mu.Unlock()
	var result []Completion
	if !e.runs[owner].Closed {
		return result
	}
	for _, event := range e.events {
		if event.Task.Owner == owner && event.Sequence > after {
			result = append(result, clone(event))
		}
	}
	return result
}

func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	stats := Stats{Running: e.active, Tasks: len(e.tasks), Events: len(e.events)}
	for _, item := range e.tasks {
		if item.State == "queued" {
			stats.Queued++
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
		if info, err := e.journal.Stat(); err == nil {
			stats.JournalBytes = info.Size()
		}
	}
	return stats
}

func (e *Engine) Ack(owner Owner, sequence uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
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
			if err := e.save(record{Run: &state}); err != nil {
				return err
			}
			e.runs[owner] = state
			return nil
		}
	}
	return fmt.Errorf("unknown event")
}

func (e *Engine) CloseRun(owner Owner) error {
	e.mu.Lock()
	defer e.mu.Unlock()
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
	if err := e.save(record{Task: &status, Event: &event}); err != nil {
		return err
	}
	item.Status = status
	e.sequence = event.Sequence
	e.events = append(e.events, event)
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
		result := e.tasks[taskKey(task.Owner, binding.TaskID)].Result
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
			if e.tasks[key].State == "queued" {
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
				dep := e.tasks[taskKey(owner, id)]
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
			go e.execute(ctx, key, task)
			return true
		}
	}
	return false
}

func (e *Engine) execute(ctx context.Context, key string, task Task) {
	result := Result{State: "failed"}
	defer func() {
		if recovered := recover(); recovered != nil {
			result = Result{State: "failed", Error: fmt.Sprint("executor panic: ", recovered)}
		}
		e.finished <- finished{key, result}
	}()
	if e.config.BeforeExecute != nil {
		if err := e.config.BeforeExecute(ctx, task); err != nil {
			result.Error = "execution admission: " + err.Error()
			if ctx.Err() != nil {
				result.State = "cancelled"
			}
			return
		}
	}
	if ctx.Err() != nil {
		result = Result{State: "cancelled", Error: ctx.Err().Error()}
		return
	}
	result = e.config.Executors[task.Kind].Execute(ctx, task)
	if !terminal(result.State) || result.State == "unknown" || result.State == "skipped" {
		result = Result{State: "failed", Error: "executor returned invalid terminal state"}
	}
}

func (e *Engine) loop() {
	defer close(e.done)
	for {
		e.mu.Lock()
		for !e.closing && e.fatal == nil && e.active < e.config.Concurrency && e.dispatch() {
		}
		stop := (e.closing || e.fatal != nil) && e.active == 0
		e.mu.Unlock()
		if stop {
			return
		}
		select {
		case <-e.wake:
		case outcome := <-e.finished:
			e.mu.Lock()
			item := e.tasks[outcome.key]
			item.cancel()
			item.cancel = nil
			e.active--
			_ = e.complete(outcome.key, outcome.result)
			e.mu.Unlock()
		}
	}
}

func (e *Engine) Close(ctx context.Context) error {
	e.mu.Lock()
	if !e.closing {
		e.closing = true
		for key, item := range e.tasks {
			if !terminal(item.State) {
				_ = e.cancel(key)
			}
		}
	}
	e.mu.Unlock()
	e.signal()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.journal != nil {
		err := e.journal.Close()
		e.journal = nil
		if e.fatal == nil {
			e.fatal = err
		}
	}
	return e.fatal
}
