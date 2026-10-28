package runtime

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
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

func (p ProcessExecutor) Execute(ctx context.Context, task Task) Result {
	return p.run(ctx, task, "", false)
}

func (p ProcessExecutor) run(ctx context.Context, task Task, input string, envelope bool) Result {
	result := Result{State: "failed"}
	if len(task.Command) == 0 {
		result.Error = "empty command"
		return result
	}
	limit := p.OutputLimit
	if limit <= 0 {
		limit = 1 << 20
	}
	stdoutLimit := limit
	if envelope {
		stdoutLimit = 2*limit + 4096
	} // JSON-string wrapper only; logical output cap is checked after unwrapping.
	stdout, stderr := &cappedBuffer{limit: stdoutLimit}, &cappedBuffer{limit: limit}
	cmd := exec.CommandContext(ctx, task.Command[0], task.Command[1:]...)
	cmd.Dir = task.Directory
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Stdin = strings.NewReader(input)
	trace := executionTracer(ctx)
	launch := time.Now()
	startErr := cmd.Start()
	trace.span("process_start", launch)
	if err := startErr; err != nil {
		result.Error = err.Error()
		if ctx.Err() != nil {
			result.State = "cancelled"
		}
		return result
	}
	result.PID = cmd.Process.Pid
	wait := time.Now()
	err := cmd.Wait() // Resources remain owned until the direct child is reaped.
	trace.span("process_wait", wait)
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
	trace := executionTracer(ctx)
	if trace.emit != nil {
		input, err = json.Marshal(struct {
			*PythonCall
			Trace bool `json:"_trace"`
		}{task.Python, true})
		if err != nil {
			return Result{State: "failed", Error: err.Error()}
		}
	}
	task.Command = []string{task.Python.Interpreter, "-u", "-c", pythonWorker}
	result := p.Process.run(ctx, task, string(input), trace.emit != nil)
	if trace.emit != nil {
		var frame struct {
			Output  string       `json:"output"`
			Spans   []TraceEvent `json:"spans"`
			Version int          `json:"trace_version"`
		}
		if err := json.Unmarshal([]byte(result.Output), &frame); err != nil || frame.Version != 1 {
			limit := p.Process.OutputLimit
			if limit <= 0 {
				limit = 1 << 20
			}
			if len(result.Output) > limit {
				result.Output = result.Output[:limit]
			}
			if result.State == "completed" {
				result.State = "failed"
				result.Error = "Python worker returned invalid diagnostic envelope"
			}
			return result
		}
		result.Output = frame.Output
		limit := p.Process.OutputLimit
		if limit <= 0 {
			limit = 1 << 20
		}
		if len(result.Output) > limit {
			result.Output = result.Output[:limit]
			if result.State == "completed" {
				result.State = "failed"
				result.Error = "tool output exceeded configured limit"
			}
		}
		for _, span := range frame.Spans {
			span.Owner = trace.task.Owner
			span.TaskID = trace.task.ID
			trace.emit(span)
		}
	}
	if result.State == "completed" && !json.Valid([]byte(result.Output)) {
		result.State = "failed"
		result.Error = "Python worker did not return a JSON value"
	}
	return result
}
