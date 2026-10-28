package runtime

import (
	"context"
	"time"
)

// TraceEvent is optional diagnostic data, never a durable execution fact.
// Unix timestamps correlate processes on one host; durations use each process's
// monotonic clock. Cross-process differences require an unadjusted host clock.
type TraceEvent struct {
	Clock      string `json:"clock,omitempty"`
	Phase      string `json:"phase"`
	Operation  string `json:"operation,omitempty"`
	Owner      Owner  `json:"owner"`
	TaskID     string `json:"task_id,omitempty"`
	Sequence   uint64 `json:"sequence,omitempty"`
	RequestID  uint64 `json:"request_id,omitempty"`
	StartNS    int64  `json:"start_ns"`
	EndNS      int64  `json:"end_ns"`
	DurationNS int64  `json:"duration_ns"`
	WaitNS     int64  `json:"wait_ns,omitempty"`
	AcquiredNS int64  `json:"acquired_ns,omitempty"`
	HoldNS     int64  `json:"hold_ns,omitempty"`
	Bytes      int    `json:"bytes,omitempty"`
	Records    int    `json:"records,omitempty"`
	State      string `json:"state,omitempty"`
}

// Implementations must be concurrency-safe, must not call Engine methods, and
// should buffer diagnostic output. Callbacks can occur under the Engine lock.
type TraceFunc func(TraceEvent)

func (e *Engine) traceSpan(phase string, task Task, state string, start time.Time) {
	if e.config.Trace != nil {
		end := time.Now()
		e.config.Trace(TraceEvent{Phase: phase, Owner: task.Owner, TaskID: task.ID, State: state,
			StartNS: start.UnixNano(), EndNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds()})
	}
}

func (e *Engine) lock(operation string, owner Owner, taskID string) func() {
	if e.config.Trace == nil {
		e.mu.Lock()
		return e.mu.Unlock
	}
	start := time.Now()
	e.mu.Lock()
	acquired := time.Now()
	e.traceOperation = operation
	return func() {
		end := time.Now()
		e.traceOperation = ""
		e.mu.Unlock()
		e.config.Trace(TraceEvent{Phase: "engine_lock", Operation: operation, Owner: owner, TaskID: taskID,
			StartNS: start.UnixNano(), AcquiredNS: acquired.UnixNano(), EndNS: end.UnixNano(),
			DurationNS: end.Sub(start).Nanoseconds(), WaitNS: acquired.Sub(start).Nanoseconds(), HoldNS: end.Sub(acquired).Nanoseconds()})
	}
}

type traceContextKey struct{}
type executionTrace struct {
	emit TraceFunc
	task Task
}

func withExecutionTrace(ctx context.Context, emit TraceFunc, task Task) context.Context {
	if emit == nil {
		return ctx
	}
	return context.WithValue(ctx, traceContextKey{}, executionTrace{emit, task})
}

func executionTracer(ctx context.Context) executionTrace {
	trace, _ := ctx.Value(traceContextKey{}).(executionTrace)
	return trace
}

func (t executionTrace) span(phase string, start time.Time) {
	if t.emit == nil {
		return
	}
	end := time.Now()
	t.emit(TraceEvent{Phase: phase, Owner: t.task.Owner, TaskID: t.task.ID, StartNS: start.UnixNano(), EndNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds()})
}
