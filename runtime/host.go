package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type HostRequest struct {
	Type        string `json:"type"`
	ExecutionID string `json:"execution_id"`
	Task        Task   `json:"task"`
}

type hostWait struct {
	task     Task
	reply    chan Result
	answered bool
}

// HostExecutor owns bounded in-flight calls, not tools or framework runtimes.
// Missing cleanup evidence always becomes a durable unknown fence in Engine.
type HostExecutor struct {
	mu             sync.Mutex
	waiting        map[string]hostWait
	send           func(HostRequest) error
	closed         chan struct{}
	once           sync.Once
	cleanupTimeout time.Duration
}

func NewHostExecutor(send func(HostRequest) error, cleanupTimeout time.Duration) (*HostExecutor, error) {
	if send == nil || cleanupTimeout <= 0 {
		return nil, fmt.Errorf("host sender and positive cleanup deadline required")
	}
	return &HostExecutor{waiting: map[string]hostWait{}, send: send, closed: make(chan struct{}), cleanupTimeout: cleanupTimeout}, nil
}

func hostUnknown(reason string) Result {
	return Result{State: "unknown", Error: reason, Host: &HostReceipt{ProcessCleanup: "unknown"}}
}

func (h *HostExecutor) Execute(ctx context.Context, task Task) Result {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return hostUnknown(err.Error())
	}
	id := hex.EncodeToString(nonce[:])
	wait := hostWait{task: task, reply: make(chan Result, 1)}
	h.mu.Lock()
	h.waiting[id] = wait
	h.mu.Unlock()
	defer func() { h.mu.Lock(); delete(h.waiting, id); h.mu.Unlock() }()
	select {
	case <-h.closed:
		return hostUnknown("host disconnected before dispatch")
	default:
	}
	if err := h.send(HostRequest{Type: "host_execute", ExecutionID: id, Task: task}); err != nil {
		return hostUnknown("host dispatch delivery uncertain: " + err.Error())
	}
	select {
	case result := <-wait.reply:
		return result
	case <-h.closed:
		return hostUnknown("host disconnected; execution/cleanup uncertain")
	case <-ctx.Done():
	}
	if err := h.send(HostRequest{Type: "host_cancel", ExecutionID: id, Task: task}); err != nil {
		return hostUnknown("host cancellation delivery uncertain: " + err.Error())
	}
	timer := time.NewTimer(h.cleanupTimeout)
	defer timer.Stop()
	select {
	case result := <-wait.reply:
		return result // A request is not proof of cancellation.
	case <-h.closed:
		return hostUnknown("host disconnected during cleanup")
	case <-timer.C:
		return hostUnknown("host cleanup receipt deadline exceeded; no replay or resource release")
	}
}

func (h *HostExecutor) Complete(id string, owner Owner, taskID string, result Result) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	wait, ok := h.waiting[id]
	if !ok || wait.task.Owner != owner || wait.task.ID != taskID {
		return fmt.Errorf("expired or mismatched host execution identity")
	}
	if wait.answered {
		return fmt.Errorf("host execution already answered")
	}
	result = validateHostResult(result)
	wait.answered = true
	h.waiting[id] = wait
	wait.reply <- clone(result)
	return nil
}

func validateHostResult(result Result) Result {
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > 1<<20 {
		result = hostUnknown("host receipt invalid or exceeds 1 MiB")
	} else if result.State != "completed" && result.State != "failed" && result.State != "cancelled" && result.State != "unknown" {
		result = hostUnknown("host returned invalid terminal state")
	} else if result.Host == nil || (result.Host.ProcessCleanup != "confirmed" && result.Host.ProcessCleanup != "not_started") {
		result.State = "unknown"
		if result.Error == "" {
			result.Error = "host process cleanup not confirmed"
		}
	}
	return result
}

func (h *HostExecutor) Disconnect() { h.once.Do(func() { close(h.closed) }) }
