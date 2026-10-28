package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	tool "focus-tool-runtime/runtime"
)

// Diagnostic writes are buffered, not fsynced. They are not part of the WAL or
// its success acknowledgement. A fresh path avoids mixing independent clocks.
func openTrace(path string) (tool.TraceFunc, func() error, error) {
	if path == "" {
		return nil, func() error { return nil }, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, nil, err
	}
	buffer := bufio.NewWriterSize(file, 64<<10)
	encoder := json.NewEncoder(buffer)
	var mu sync.Mutex
	var failure error
	closed := false
	emit := func(event tool.TraceEvent) {
		mu.Lock()
		defer mu.Unlock()
		if closed {
			return
		}
		if failure == nil {
			failure = encoder.Encode(event)
		}
	}
	close := func() error {
		mu.Lock()
		defer mu.Unlock()
		closed = true
		if err := buffer.Flush(); failure == nil {
			failure = err
		}
		if err := file.Close(); failure == nil {
			failure = err
		}
		return failure
	}
	return emit, close, nil
}

func serverSpan(emit tool.TraceFunc, phase string, req request, start time.Time) {
	if emit == nil {
		return
	}
	end := time.Now()
	owner, id := req.Owner, req.TaskID
	if req.Method == "submit" {
		owner, id = req.Task.Owner, req.Task.ID
	}
	emit(tool.TraceEvent{Phase: phase, Operation: req.Method, Owner: owner, TaskID: id, RequestID: req.ID, Sequence: req.Sequence,
		StartNS: start.UnixNano(), EndNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds()})
}
