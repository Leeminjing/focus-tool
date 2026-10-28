// runtime speaks a local JSONL protocol on stdio. Stdout is reserved for frames.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"time"

	tool "focus-tool-runtime/runtime"
)

type writer struct {
	mu      sync.Mutex
	encoder *json.Encoder
}

func (w *writer) send(value any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.encoder.Encode(value)
}

type request struct {
	ID       uint64     `json:"id"`
	Method   string     `json:"method"`
	Owner    tool.Owner `json:"owner"`
	Revision int        `json:"revision"`
	Task     tool.Task  `json:"task"`
	TaskID   string     `json:"task_id"`
	After    uint64     `json:"after"`
	Sequence uint64     `json:"sequence"`
	Token    uint64     `json:"token"`
	Allowed  bool       `json:"allowed"`
	Reason   string     `json:"reason"`
	Collect  bool       `json:"collect"`
	Cursor   string     `json:"cursor"`
	Limit    int        `json:"limit"`
	MaxBytes int        `json:"max_bytes"`
}

type approvals struct {
	mu      sync.Mutex
	next    atomic.Uint64
	waiting map[uint64]chan string
	out     *writer
}

func (a *approvals) check(ctx context.Context, task tool.Task) error {
	// A stopped or disconnected client cannot retain an execution slot forever.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	token := a.next.Add(1)
	reply := make(chan string, 1)
	a.mu.Lock()
	a.waiting[token] = reply
	a.mu.Unlock()
	defer func() { a.mu.Lock(); delete(a.waiting, token); a.mu.Unlock() }()
	if err := a.out.send(map[string]any{"type": "authorize", "token": token, "task": task}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case reason := <-reply:
		if reason != "" {
			return fmt.Errorf("%s", reason)
		}
		return nil
	}
}

func (a *approvals) answer(req request) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	reply := a.waiting[req.Token]
	if reply == nil {
		return fmt.Errorf("authorization request expired")
	}
	reason := req.Reason
	if !req.Allowed && reason == "" {
		reason = "execution denied"
	}
	if req.Allowed {
		reason = ""
	}
	select {
	case reply <- reason:
		return nil
	default:
		return fmt.Errorf("authorization already answered")
	}
}

func serve() (serveError error) {
	journal := flag.String("journal", ".focus-tool/runtime.jsonl", "durable local JSONL journal; empty disables persistence")
	concurrency := flag.Int("concurrency", 4, "maximum executing tasks")
	capacity := flag.Int("capacity", 128, "maximum outstanding tasks")
	perRunCapacity := flag.Int("per-run-capacity", 0, "per-run outstanding tasks and unacknowledged facts; 0 uses capacity/concurrency")
	maxRuns := flag.Int("max-active-runs", 0, "resident open Run bound; 0 uses max(128, capacity)")
	segmentBytes := flag.Int64("segment-bytes", 0, "WAL segment size before online compaction; 0 uses 8 MiB")
	requireApproval := flag.Bool("require-approval", true, "ask the client to recheck admission before each execution")
	tracePath := flag.String("trace", "", "optional fresh diagnostic JSONL path; buffered, not execution facts")
	flag.Parse()
	trace, closeTrace, err := openTrace(*tracePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := closeTrace(); serveError == nil {
			serveError = err
		}
	}()
	out := &writer{encoder: json.NewEncoder(os.Stdout)}
	gates := &approvals{waiting: map[uint64]chan string{}, out: out}
	config := tool.Config{Concurrency: *concurrency, Capacity: *capacity, Journal: *journal, Executors: map[string]tool.Executor{"process": tool.ProcessExecutor{}, "python": tool.PythonExecutor{}}}
	config.PerRunCapacity = *perRunCapacity
	config.MaxActiveRuns, config.SegmentBytes = *maxRuns, *segmentBytes
	config.Trace = trace
	if *requireApproval {
		config.BeforeExecute = gates.check
	}
	engine, err := tool.New(config)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = engine.Close(ctx)
	}()
	pumpCtx, stopPump := context.WithCancel(context.Background())
	defer stopPump()
	go func() {
		var cursor uint64
		for {
			for _, event := range engine.Pending(cursor) {
				started := time.Now()
				if err := out.send(map[string]any{"type": "event", "event": event}); err != nil {
					return
				}
				if trace != nil {
					end := time.Now()
					trace(tool.TraceEvent{Phase: "server_event_send", Owner: event.Task.Owner, TaskID: event.Task.ID, Sequence: event.Sequence, StartNS: started.UnixNano(), EndNS: end.UnixNano(), DurationNS: end.Sub(started).Nanoseconds()})
				}
				cursor = event.Sequence
			}
			select {
			case <-pumpCtx.Done():
				return
			case <-engine.Changed():
			}
		}
	}()
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 4<<20)
	for scanner.Scan() {
		started := time.Now()
		var req request
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			return fmt.Errorf("invalid request: %w", err)
		}
		var value any
		var err error
		switch req.Method {
		case "open_run":
			err = engine.OpenRun(req.Owner, req.Revision)
		case "submit":
			value, err = engine.Submit(req.Task)
		case "cancel":
			err = engine.Cancel(req.Owner, req.TaskID)
		case "status":
			value, err = engine.Status(req.Owner, req.TaskID)
		case "events":
			var page tool.ArchivePage
			page, err = engine.EventPage(req.Owner, tool.PageOptions{After: req.After})
			if err == nil && page.NextCursor != "" {
				err = fmt.Errorf("result exceeds one page; use event_page")
			}
			if err == nil && len(page.Items) > 0 {
				value = page.Items
			}
		case "archive_events":
			value, err = engine.ArchivePage(req.Owner, tool.PageOptions{Cursor: req.Cursor, After: req.After, Limit: req.Limit, MaxBytes: req.MaxBytes})
		case "event_page":
			value, err = engine.EventPage(req.Owner, tool.PageOptions{Cursor: req.Cursor, After: req.After, Limit: req.Limit, MaxBytes: req.MaxBytes})
		case "compact":
			err = engine.Compact()
		case "stats":
			if req.Collect {
				goruntime.GC()
			}
			value = engine.Stats()
		case "ack":
			err = engine.Ack(req.Owner, req.Sequence)
		case "close_run":
			err = engine.CloseRun(req.Owner)
			if err == nil {
				value = engine.Archive(req.Owner)
			}
		case "authorize":
			err = gates.answer(req)
		case "close":
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = engine.Close(ctx)
			cancel()
		default:
			err = fmt.Errorf("unknown method %q", req.Method)
		}
		response := map[string]any{"type": "response", "id": req.ID, "result": value}
		if err != nil {
			response["error"] = err.Error()
			response["error_code"] = "rejected"
			if req.Method == "submit" && errors.Is(err, tool.ErrStorageUnknown) {
				response["error_code"] = "acceptance_unknown"
			}
		}
		if writeErr := out.send(response); writeErr != nil {
			return writeErr
		}
		serverSpan(trace, "server_request", req, started)
		if req.Method == "close" {
			return err
		}
	}
	return scanner.Err()
}

func main() {
	if err := serve(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
