// Package runtime contains the M1 local process runner. It is a research kernel,
// not yet a durable multi-context scheduler.
package runtime

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"sync"
	"time"
)

type Task struct {
	ID        string   `json:"id"`
	ContextID string   `json:"context_id"`
	Revision  int      `json:"revision"`
	Command   []string `json:"command,omitempty"`
}

type Event struct {
	Kind      string  `json:"kind"`
	TaskID    string  `json:"task_id"`
	ContextID string  `json:"context_id"`
	Revision  int     `json:"revision"`
	AtMS      float64 `json:"at_ms"`
	PID       int     `json:"pid,omitempty"`
	Detail    string  `json:"detail,omitempty"`
}

// Trace serializes concurrent producers and uses elapsed monotonic time.
type Trace struct {
	start  time.Time
	mu     sync.Mutex
	events []Event
}

func NewTrace() *Trace { return &Trace{start: time.Now()} }
func (t *Trace) Add(kind string, task Task, pid int, detail string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, Event{kind, task.ID, task.ContextID, task.Revision, float64(time.Since(t.start).Nanoseconds()) / 1e6, pid, detail})
}
func (t *Trace) Events() []Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]Event(nil), t.events...)
}

type Result struct {
	Task   Task   `json:"task"`
	State  string `json:"state"`
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
	PID    int    `json:"pid,omitempty"`
}

type Job struct {
	Task   Task
	Done   <-chan Result
	cancel context.CancelFunc
	once   sync.Once
	trace  *Trace
}

func (j *Job) Cancel() {
	j.once.Do(func() {
		j.trace.Add("cancel_requested", j.Task, 0, "")
		j.cancel()
	})
}

type Runner struct {
	slots chan struct{}
	Trace *Trace
}

func NewRunner(limit int, trace *Trace) *Runner {
	if limit < 1 {
		panic("concurrency must be positive")
	}
	return &Runner{slots: make(chan struct{}, limit), Trace: trace}
}

// Start accepts a task immediately; Done receives exactly one terminal result.
// Context cancellation kills only the direct child. M1 workers spawn no children.
func (r *Runner) Start(parent context.Context, task Task) *Job {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan Result, 1)
	job := &Job{Task: task, Done: done, cancel: cancel, trace: r.Trace}
	r.Trace.Add("queued", task, 0, "")
	go func() {
		defer cancel()
		defer close(done)
		result := Result{Task: task, State: "failed"}
		select {
		case r.slots <- struct{}{}:
			defer func() { <-r.slots }()
		case <-ctx.Done():
			result.State, result.Error = "cancelled", ctx.Err().Error()
			r.finish(done, result)
			return
		}
		if ctx.Err() != nil {
			result.State, result.Error = "cancelled", ctx.Err().Error()
			r.finish(done, result)
			return
		}
		if len(task.Command) == 0 {
			result.Error = "empty command"
			r.finish(done, result)
			return
		}
		cmd := exec.CommandContext(ctx, task.Command[0], task.Command[1:]...)
		stdout, err := cmd.StdoutPipe()
		if err == nil {
			err = cmd.Start()
		}
		if err != nil {
			result.Error = err.Error()
			if ctx.Err() != nil {
				result.State = "cancelled"
			}
			r.finish(done, result)
			return
		}
		result.PID = cmd.Process.Pid
		r.Trace.Add("started", task, result.PID, "")
		scanner := bufio.NewScanner(stdout)
		first := true
		for scanner.Scan() {
			if first {
				r.Trace.Add("first_output", task, result.PID, "")
				first = false
			}
			result.Output = scanner.Text()
		}
		readErr := scanner.Err()
		if readErr != nil {
			_ = cmd.Process.Kill()
		}
		err = cmd.Wait() // Cancellation is acknowledged only after the child is reaped.
		if err == nil && readErr == nil {
			result.State = "completed"
		} else {
			result.Error = fmt.Sprintf("wait=%v read=%v", err, readErr)
			if ctx.Err() != nil {
				result.State = "cancelled"
			}
		}
		r.finish(done, result)
	}()
	return job
}

func (r *Runner) finish(done chan<- Result, result Result) {
	r.Trace.Add(result.State, result.Task, result.PID, result.Error)
	done <- result
}

// Inbox is owned by one controller goroutine. Revisions are scoped to Context.
// Only completed, current, previously unseen task results may affect a decision.
type Inbox struct {
	Revision map[string]int
	seen     map[string]bool
}

func NewInbox() *Inbox { return &Inbox{Revision: map[string]int{}, seen: map[string]bool{}} }
func (i *Inbox) Consume(result Result) string {
	if revision, exists := i.Revision[result.Task.ContextID]; !exists || revision != result.Task.Revision {
		return "stale"
	}
	if result.State != "completed" {
		return "non_success"
	}
	key := fmt.Sprintf("%q/%d/%q", result.Task.ContextID, result.Task.Revision, result.Task.ID)
	if i.seen[key] {
		return "duplicate"
	}
	i.seen[key] = true
	return "accepted"
}
