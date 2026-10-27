package runtime

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
)

//go:embed python_worker.py
var pythonWorker string

// cappedBuffer drains all output while retaining only a bounded prefix, so a
// noisy tool cannot deadlock on its pipe or exhaust daemon memory.
type cappedBuffer struct {
	mu       sync.Mutex
	data     []byte
	limit    int
	overflow bool
}

func (b *cappedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	remaining := b.limit - len(b.data)
	if len(data) > remaining {
		b.overflow = true
	} else {
		remaining = len(data)
	}
	b.data = append(b.data, data[:remaining]...)
	return len(data), nil
}

type ProcessExecutor struct{ OutputLimit int }

func (p ProcessExecutor) Execute(ctx context.Context, task Task) Result { return p.run(ctx, task, "") }

func (p ProcessExecutor) run(ctx context.Context, task Task, input string) Result {
	result := Result{State: "failed"}
	if len(task.Command) == 0 {
		result.Error = "empty command"
		return result
	}
	limit := p.OutputLimit
	if limit <= 0 {
		limit = 1 << 20
	}
	stdout, stderr := &cappedBuffer{limit: limit}, &cappedBuffer{limit: limit}
	cmd := exec.CommandContext(ctx, task.Command[0], task.Command[1:]...)
	cmd.Dir = task.Directory
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Stdin = strings.NewReader(input)
	if err := cmd.Start(); err != nil {
		result.Error = err.Error()
		if ctx.Err() != nil {
			result.State = "cancelled"
		}
		return result
	}
	result.PID = cmd.Process.Pid
	err := cmd.Wait() // Resources remain owned until the direct child is reaped.
	result.Output = string(stdout.data)
	if err == nil && !stdout.overflow && !stderr.overflow {
		result.State = "completed"
		return result
	}
	if err != nil {
		result.Error = fmt.Sprintf("%v: %s", err, stderr.data)
	} else {
		result.Error = "tool output exceeded configured limit"
	}
	if ctx.Err() != nil {
		result.State = "cancelled"
	}
	return result
}

type PythonExecutor struct{ Process ProcessExecutor }

func (p PythonExecutor) Execute(ctx context.Context, task Task) Result {
	if task.Python == nil {
		return Result{State: "failed", Error: "missing Python call"}
	}
	input, err := json.Marshal(task.Python)
	if err != nil {
		return Result{State: "failed", Error: err.Error()}
	}
	task.Command = []string{task.Python.Interpreter, "-u", "-c", pythonWorker}
	result := p.Process.run(ctx, task, string(input))
	if result.State == "completed" && !json.Valid([]byte(result.Output)) {
		result.State = "failed"
		result.Error = "Python worker did not return a JSON value"
	}
	return result
}
