// Package runtime implements the shared, local tool runtime in the architecture.
package runtime

import (
	"context"
	"encoding/json"
	"errors"
)

var (
	ErrClosed         = errors.New("runtime or run is closed")
	ErrQueueFull      = errors.New("runtime capacity exhausted")
	ErrRunFull        = errors.New("run capacity exhausted")
	ErrStorageUnknown = errors.New("journal write failed; acceptance may be unknown")
	ErrConflict       = errors.New("task identity already has different parameters")
)

type Owner struct {
	ContextID string `json:"context_id"`
	RunID     string `json:"run_id"`
}

type Resource struct {
	Name string `json:"name"`
	Mode string `json:"mode"` // read or write; names are explicit shared resource keys.
}

type Binding struct {
	TaskID string   `json:"task_id"`
	Path   []string `json:"path,omitempty"`
}

type PythonCall struct {
	Interpreter string                     `json:"interpreter"`
	Module      string                     `json:"module"`
	Function    string                     `json:"function"`
	Args        map[string]json.RawMessage `json:"args"`
}

type Task struct {
	ID           string             `json:"id"`
	Owner        Owner              `json:"owner"`
	Revision     int                `json:"revision"`
	Kind         string             `json:"kind"`
	Command      []string           `json:"command,omitempty"`
	Python       *PythonCall        `json:"python,omitempty"`
	Directory    string             `json:"directory,omitempty"`
	Dependencies []string           `json:"dependencies,omitempty"`
	Bindings     map[string]Binding `json:"bindings,omitempty"`
	Resources    []Resource         `json:"resources,omitempty"`
	TimeoutMS    int                `json:"timeout_ms,omitempty"`
}

type Result struct {
	State  string `json:"state"`
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
	PID    int    `json:"pid,omitempty"`
}

type Completion struct {
	Sequence uint64 `json:"sequence"`
	Task     Task   `json:"task"`
	Result   Result `json:"result"`
	Timing   Timing `json:"timing"`
}

type Timing struct {
	QueuedNS   int64 `json:"queued_ns"`
	StartedNS  int64 `json:"started_ns,omitempty"`
	FinishedNS int64 `json:"finished_ns,omitempty"`
}

type ArchiveReceipt struct {
	Owner          Owner  `json:"owner"`
	Custodian      string `json:"custodian"`
	Pending        int    `json:"pending_executions"`
	Unacknowledged int    `json:"unacknowledged_facts"`
}

type Stats struct {
	Running        int    `json:"running"`
	Queued         int    `json:"queued"`
	Unacknowledged int    `json:"unacknowledged"`
	Tasks          int    `json:"retained_tasks"`
	Events         int    `json:"retained_events"`
	HeapBytes      uint64 `json:"heap_bytes"`
	JournalBytes   int64  `json:"journal_bytes"`
}

type Executor interface {
	Execute(context.Context, Task) Result
}
type ExecutorFunc func(context.Context, Task) Result

func (f ExecutorFunc) Execute(ctx context.Context, task Task) Result { return f(ctx, task) }

type Config struct {
	Concurrency    int
	Capacity       int    // Global nonterminal task bound, independent of result backlogs.
	PerRunCapacity int    // Nonterminal tasks + unacked facts per active Run; 0 uses Capacity/Concurrency.
	Journal        string // Empty means memory only. One runtime must own a journal file.
	Executors      map[string]Executor
	BeforeExecute  func(context.Context, Task) error
}

type RunState struct {
	Owner    Owner  `json:"owner"`
	Revision int    `json:"revision"`
	Ack      uint64 `json:"ack"`
	Closed   bool   `json:"closed"`
}

type Status struct {
	Task   Task    `json:"task"`
	State  string  `json:"state"`
	Result *Result `json:"result,omitempty"`
	Timing Timing  `json:"timing"`
}

func terminal(state string) bool {
	switch state {
	case "completed", "failed", "cancelled", "skipped", "unknown":
		return true
	}
	return false
}
