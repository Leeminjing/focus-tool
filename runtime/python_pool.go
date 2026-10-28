package runtime

import (
	"bufio"
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed python_pool_worker.py
var pythonPoolWorker string

type PoolStats struct {
	Limit     int    `json:"limit"`
	Reserved  int    `json:"reserved"`
	Live      int    `json:"live"`
	PeakLive  int    `json:"peak_live"`
	Started   uint64 `json:"started"`
	Retired   uint64 `json:"retired"`
	Completed uint64 `json:"completed"`
	Reused    uint64 `json:"reused_calls"`
}

type poolKey struct{ interpreter, directory string }

// PythonPool owns worker lifetimes, independently of any one task context.
// Its lease reservation does not start an interpreter or send caller code.
type PythonPool struct {
	mu                           sync.Mutex
	workers                      map[*poolWorker]bool // true while exclusively reserved
	changed                      chan struct{}
	closed                       bool
	limit, maxTasks, outputLimit int
	stats                        PoolStats
}

func NewPythonPool(limit, maxTasks, outputLimit int) (*PythonPool, error) {
	if limit < 1 || maxTasks < 1 {
		return nil, fmt.Errorf("positive Python worker and lifetime limits required")
	}
	if outputLimit <= 0 {
		outputLimit = 1 << 20
	}
	return &PythonPool{workers: map[*poolWorker]bool{}, changed: make(chan struct{}), limit: limit, maxTasks: maxTasks, outputLimit: outputLimit, stats: PoolStats{Limit: limit}}, nil
}

func (p *PythonPool) signal() { close(p.changed); p.changed = make(chan struct{}) } // under mu

func (p *PythonPool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	stats := p.stats
	stats.Reserved = len(p.workers)
	return stats
}

func pythonPoolKey(task Task) (poolKey, error) {
	interpreter, err := exec.LookPath(task.Python.Interpreter)
	if err != nil {
		return poolKey{}, err
	}
	interpreter, err = filepath.Abs(interpreter)
	if err != nil {
		return poolKey{}, err
	}
	directory, err := filepath.Abs(task.Directory)
	if err != nil {
		return poolKey{}, err
	}
	if resolved, err := filepath.EvalSymlinks(directory); err == nil {
		directory = resolved
	}
	return poolKey{interpreter, directory}, nil
}

func (p *PythonPool) prepare(ctx context.Context, task Task) (PreparedExecution, error) {
	key, err := pythonPoolKey(task)
	if err != nil {
		return nil, err
	}
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrClosed
		}
		var candidate *poolWorker
		for worker, busy := range p.workers {
			if !busy {
				candidate = worker
				if worker.key == key {
					break
				}
			}
		}
		if candidate == nil && len(p.workers) < p.limit {
			candidate = newPoolWorker(key, p.outputLimit)
		}
		if candidate != nil {
			p.workers[candidate] = true
			p.mu.Unlock()
			return &pooledCall{pool: p, worker: candidate, key: key}, nil
		}
		changed := p.changed
		p.mu.Unlock()
		trace := executionTracer(ctx)
		wait := time.Now()
		trace.span("pool_wait_begin", wait)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
		trace.span("pool_wait", wait)
	}
}

func (p *PythonPool) retire(worker *poolWorker) {
	worker.stop()
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.workers[worker]; ok {
		delete(p.workers, worker)
		if worker.counted {
			p.stats.Live--
			p.stats.Retired++
		}
		p.signal()
	}
}

func (p *PythonPool) replace(worker *poolWorker, key poolKey) (*poolWorker, error) {
	worker.stop() // Old process is reaped before its bounded slot can be reused.
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.workers[worker]; !ok || p.closed {
		return nil, ErrClosed
	}
	delete(p.workers, worker)
	if worker.counted {
		p.stats.Live--
		p.stats.Retired++
	}
	next := newPoolWorker(key, p.outputLimit)
	p.workers[next] = true
	return next, nil
}

func (p *PythonPool) Close() error {
	p.mu.Lock()
	p.closed = true
	p.signal()
	workers := make([]*poolWorker, 0, len(p.workers))
	for worker := range p.workers {
		workers = append(workers, worker)
	}
	p.mu.Unlock()
	for _, worker := range workers {
		p.retire(worker)
	}
	return nil
}

type workerFrame struct {
	Type      string       `json:"type"`
	ID        string       `json:"id"`
	State     string       `json:"state"`
	Output    string       `json:"output"`
	Error     string       `json:"error"`
	Reusable  bool         `json:"reusable"`
	Bootstrap TraceEvent   `json:"bootstrap"`
	Spans     []TraceEvent `json:"spans"`
	err       error
}

type poolWorker struct {
	key                       poolKey
	limit                     int
	guard                     sync.Mutex // serializes process creation with pool shutdown
	stopping                  bool
	cmd                       *exec.Cmd
	input                     *os.File
	output                    *os.File
	stderr                    *os.File
	frames                    chan workerFrame
	exited, stopRead, stopped chan struct{}
	readers                   sync.WaitGroup
	writers                   sync.WaitGroup
	stopOnce                  sync.Once
	pid                       atomic.Int64
	rawStderr                 cappedBuffer
	poison                    atomic.Bool
	counted                   bool // pool.mu
	jobs                      int  // exclusive lease; modified under pool.mu during Release
}

func newPoolWorker(key poolKey, limit int) *poolWorker {
	return &poolWorker{key: key, limit: limit, frames: make(chan workerFrame, 1), exited: make(chan struct{}), stopRead: make(chan struct{}), stopped: make(chan struct{}), rawStderr: cappedBuffer{limit: limit}}
}

func (w *poolWorker) start(p *PythonPool, trace executionTrace) error {
	w.guard.Lock()
	defer w.guard.Unlock()
	if w.stopping {
		return ErrClosed
	}
	if w.pid.Load() != 0 {
		return nil
	}
	inRead, inWrite, err := os.Pipe()
	if err != nil {
		return err
	}
	outRead, outWrite, err := os.Pipe()
	if err != nil {
		inRead.Close()
		inWrite.Close()
		return err
	}
	errRead, errWrite, err := os.Pipe()
	if err != nil {
		inRead.Close()
		inWrite.Close()
		outRead.Close()
		outWrite.Close()
		return err
	}
	children := []*os.File{inRead, outWrite, errWrite}
	defer func() {
		for _, file := range children {
			file.Close()
		}
	}()
	w.input, w.output, w.stderr = inWrite, outRead, errRead
	w.cmd = exec.Command(w.key.interpreter, "-u", "-c", pythonPoolWorker) // Never CommandContext(jobCtx).
	w.cmd.Dir = w.key.directory
	w.cmd.Stdin = inRead
	w.cmd.Stdout = outWrite
	w.cmd.Stderr = errWrite
	start := time.Now()
	err = w.cmd.Start()
	trace.span("process_start", start)
	if err != nil {
		return err
	}
	w.pid.Store(int64(w.cmd.Process.Pid))
	p.mu.Lock()
	w.counted = true
	p.stats.Started++
	p.stats.Live++
	if p.stats.Live > p.stats.PeakLive {
		p.stats.PeakLive = p.stats.Live
	}
	p.mu.Unlock()
	w.readers.Add(2)
	go func() {
		defer w.readers.Done()
		scanner := bufio.NewScanner(w.output)
		scanner.Buffer(make([]byte, 4096), 8*w.limit+16384)
		for scanner.Scan() {
			var frame workerFrame
			if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
				frame.err = fmt.Errorf("invalid worker frame: %w", err)
			}
			select {
			case w.frames <- frame:
			case <-w.stopRead:
				return
			}
			if frame.err != nil {
				return
			}
		}
		failure := scanner.Err()
		if failure == nil {
			failure = io.EOF
		}
		select {
		case w.frames <- workerFrame{err: failure}:
		case <-w.stopRead:
		}
	}()
	go func() {
		defer w.readers.Done()
		data := make([]byte, 4096)
		for {
			n, err := w.stderr.Read(data)
			if n > 0 {
				w.rawStderr.Write(data[:n])
				w.poison.Store(true)
			}
			if err != nil {
				return
			}
		}
	}()
	go func() { _ = w.cmd.Wait(); close(w.exited) }()
	return nil
}

func (w *poolWorker) stop() {
	w.stopOnce.Do(func() {
		w.guard.Lock()
		w.stopping = true
		close(w.stopRead)
		if w.pid.Load() != 0 {
			_ = w.cmd.Process.Kill()
		} else {
			close(w.exited)
		}
		w.guard.Unlock()
		<-w.exited
		for _, file := range []*os.File{w.input, w.output, w.stderr} {
			if file != nil {
				file.Close()
			}
		}
		w.readers.Wait()
		w.writers.Wait()
		close(w.stopped)
	})
	<-w.stopped
}

type pooledCall struct {
	pool          *PythonPool
	worker        *poolWorker
	key           poolKey
	used, healthy bool
	release       sync.Once
}

func (call *pooledCall) Release() {
	call.release.Do(func() {
		p, w := call.pool, call.worker
		if w == nil {
			return
		}
		p.mu.Lock()
		_, exists := p.workers[w]
		keep := exists && !p.closed && (!call.used || call.healthy) && w.pid.Load() != 0 && !w.poison.Load()
		if call.used && call.healthy {
			w.jobs++
			p.stats.Completed++
			if w.jobs > 1 {
				p.stats.Reused++
			}
			keep = keep && w.jobs < p.maxTasks
		}
		if keep {
			p.workers[w] = false
			p.signal()
		}
		p.mu.Unlock()
		if !keep {
			p.retire(w)
		}
	})
}

func (call *pooledCall) Execute(ctx context.Context, task Task) (result Result) {
	call.used = true
	trace := executionTracer(ctx)
	start := time.Now()
	reused := false
	defer func() {
		if trace.emit != nil {
			end := time.Now()
			trace.emit(TraceEvent{Phase: "python_call", Owner: task.Owner, TaskID: task.ID, StartNS: start.UnixNano(), EndNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds(), PID: result.PID, Reused: reused})
		}
	}()
	fail := func(err error) Result {
		call.pool.retire(call.worker)
		state := "failed"
		if ctx.Err() != nil {
			state = "cancelled"
		}
		message := err.Error()
		call.worker.rawStderr.mu.Lock()
		if len(call.worker.rawStderr.data) > 0 {
			message += ": " + string(call.worker.rawStderr.data)
		}
		call.worker.rawStderr.mu.Unlock()
		if len(message) > call.pool.outputLimit {
			message = message[:call.pool.outputLimit]
		}
		return Result{State: state, Error: message, PID: int(call.worker.pid.Load())}
	}
	if ctx.Err() != nil {
		return fail(ctx.Err())
	}
	if call.worker.key != call.key {
		next, err := call.pool.replace(call.worker, call.key)
		if err != nil {
			return fail(err)
		}
		call.worker = next
	}
	w := call.worker
	reused = w.pid.Load() != 0
	if !reused {
		if err := w.start(call.pool, trace); err != nil {
			return fail(err)
		}
		select {
		case <-ctx.Done():
			return fail(ctx.Err())
		case frame := <-w.frames:
			if frame.err != nil {
				return fail(fmt.Errorf("worker startup: %w", frame.err))
			}
			if frame.Type != "ready" {
				return fail(fmt.Errorf("missing worker ready handshake"))
			}
			if trace.emit != nil {
				span := frame.Bootstrap
				span.Owner = task.Owner
				span.TaskID = task.ID
				trace.emit(span)
			}
			trace.span("worker_ready", start)
		}
	}
	if w.poison.Load() {
		return fail(fmt.Errorf("worker wrote outside its framed diagnostics"))
	}
	select {
	case frame := <-w.frames:
		return fail(fmt.Errorf("unsolicited worker frame before dispatch: %v", frame.err))
	default:
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fail(err)
	}
	id := hex.EncodeToString(nonce[:])
	data, err := json.Marshal(struct {
		ID       string                     `json:"id"`
		Module   string                     `json:"module"`
		Function string                     `json:"function"`
		Args     map[string]json.RawMessage `json:"args"`
		Limit    int                        `json:"limit"`
		Trace    bool                       `json:"trace"`
	}{id, task.Python.Module, task.Python.Function, task.Python.Args, call.pool.outputLimit, trace.emit != nil})
	if err != nil {
		return fail(err)
	}
	w.guard.Lock()
	if w.stopping {
		w.guard.Unlock()
		return fail(ErrClosed)
	}
	w.writers.Add(1)
	writeDone := make(chan error, 1)
	go func() { defer w.writers.Done(); _, err := w.input.Write(append(data, '\n')); writeDone <- err }()
	w.guard.Unlock()
	select {
	case <-ctx.Done():
		return fail(ctx.Err())
	case err := <-writeDone:
		if err != nil {
			return fail(fmt.Errorf("worker dispatch failed; not replayed: %w", err))
		}
	}
	select {
	case <-ctx.Done():
		return fail(ctx.Err())
	case frame := <-w.frames:
		if frame.err != nil {
			return fail(fmt.Errorf("worker exited or protocol failed; effects may have occurred; not replayed: %w", frame.err))
		}
		if frame.ID != id {
			return fail(fmt.Errorf("worker response identity mismatch; not replayed"))
		}
		if ctx.Err() != nil {
			return fail(ctx.Err())
		}
		result = Result{State: frame.State, Output: frame.Output, Error: frame.Error, PID: int(w.pid.Load())}
		limit := call.pool.outputLimit
		if len(result.Output) > limit {
			result.Output = result.Output[:limit]
			result.State = "failed"
			result.Error = "tool output exceeded configured limit"
		}
		if len(result.Error) > limit {
			result.Error = result.Error[:limit]
		}
		if frame.State != "completed" && frame.State != "failed" {
			return fail(fmt.Errorf("invalid worker result state"))
		}
		if result.State == "completed" && !json.Valid([]byte(result.Output)) {
			result.State = "failed"
			result.Error = "worker result is not JSON"
		}
		for _, span := range frame.Spans {
			if trace.emit != nil {
				span.Owner = task.Owner
				span.TaskID = task.ID
				trace.emit(span)
			}
		}
		call.healthy = result.State == "completed" && frame.Reusable && !w.poison.Load()
		if !call.healthy {
			call.pool.retire(w)
		}
		return result
	}
}
