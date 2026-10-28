package runtime

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type record struct {
	Version int         `json:"version,omitempty"`
	Run     *RunState   `json:"run,omitempty"`
	Task    *Status     `json:"task,omitempty"`
	Event   *Completion `json:"event,omitempty"`
}

type journal struct {
	file        *os.File
	lock        *os.File // Stable lock identity across segment rotation.
	path        string
	bytes       int64
	compactions uint64
	// Only the current WAL segment is cached. Intermediate transitions are
	// coalesced when indexing; every original transition is still fsynced first.
	tasks  map[string]Status
	runs   map[Owner]RunState
	events []Completion
}

// Replay one frame at a time; only a torn FINAL frame is truncated. Complete
// corruption fails closed. The legacy single-file journal is also this format.
func replayFile(file *os.File, apply func(record) error) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReader(file)
	var offset int64
	for {
		var data []byte
		for {
			fragment, err := reader.ReadSlice('\n')
			if len(data)+len(fragment) > maxRecordBytes {
				return fmt.Errorf("journal frame exceeds limit")
			}
			data = append(data, fragment...)
			if err == bufio.ErrBufferFull {
				continue
			}
			if err == io.EOF {
				if err := file.Truncate(offset); err != nil {
					return err
				}
				_, err = file.Seek(0, io.SeekEnd)
				return err
			}
			if err != nil {
				return err
			}
			break
		}
		var item record
		if err := json.Unmarshal(data, &item); err != nil {
			return fmt.Errorf("corrupt journal: %w", err)
		}
		if item.Run == nil && item.Task == nil {
			return fmt.Errorf("journal frame has no state")
		}
		if err := apply(item); err != nil {
			return err
		}
		offset += int64(len(data))
	}
}

func openJournal(path string) (*journal, *history, error) {
	if path == "" {
		return nil, nil, nil
	}
	if err := durableMkdir(filepath.Dir(path)); err != nil {
		return nil, nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (*journal, *history, error) { lock.Close(); return nil, nil, err }
	if err := lockJournal(lock); err != nil {
		return fail(err)
	}
	h := &history{root: path + ".store"}
	if err := durableMkdir(h.root); err != nil {
		return fail(err)
	}
	// One sealed segment can remain after a crash. Replay before the active
	// segment; canonical files are idempotent derived state of these records.
	sealed := path + ".sealed"
	legacy := false
	apply := func(item record) error {
		if item.Version < 2 {
			legacy = true
		}
		return h.apply(item)
	}
	if file, err := os.OpenFile(sealed, os.O_RDWR, 0600); err == nil {
		err = replayFile(file, apply)
		file.Close()
		if err != nil {
			return fail(err)
		}
	} else if !os.IsNotExist(err) {
		return fail(err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fail(err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		file.Close()
		return fail(err)
	}
	if err := replayFile(file, apply); err != nil {
		file.Close()
		return fail(err)
	}
	if legacy {
		if err := h.migrateCounters(); err != nil {
			file.Close()
			return fail(err)
		}
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return fail(err)
	}
	if err := removeDurable(sealed); err != nil {
		file.Close()
		return fail(err)
	}
	return &journal{file: file, lock: lock, path: path, bytes: info.Size(), tasks: map[string]Status{}, runs: map[Owner]RunState{}}, h, nil
}

// History is durable before rotation. Seal, create/fsync the new segment, then
// discard the redundant WAL. Startup repairs either rename boundary.
func (j *journal) compact(h *history) error {
	// The WAL remains replayable until ALL canonical records/indexes are
	// durable. Repeated compaction after any interrupted write is idempotent.
	for _, status := range j.tasks {
		if err := h.apply(record{Version: 2, Task: &status}); err != nil {
			return err
		}
	}
	for _, event := range j.events {
		if err := writeJSON(h.eventPath(event.Task.Owner, event.Sequence), event); err != nil {
			return err
		}
	}
	if len(j.events) > 0 {
		if err := writeJSON(filepath.Join(h.root, "sequence.json"), j.events[len(j.events)-1].Sequence); err != nil {
			return err
		}
	}
	for _, state := range j.runs {
		if err := h.apply(record{Version: 2, Run: &state}); err != nil {
			return err
		}
	}
	if err := j.file.Close(); err != nil {
		return err
	}
	j.file = nil
	if err := replaceDurable(j.path, j.path+".sealed"); err != nil {
		return err
	}
	file, err := os.OpenFile(j.path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	j.file = file
	if err := file.Sync(); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(j.path)); err != nil {
		return err
	}
	if err := removeDurable(j.path + ".sealed"); err != nil {
		return err
	}
	j.bytes = 0
	j.tasks = map[string]Status{}
	j.runs = map[Owner]RunState{}
	j.events = nil
	j.compactions++
	return nil
}

func (j *journal) close() error {
	var err error
	if j.file != nil {
		err = j.file.Close()
		j.file = nil
	}
	if lockErr := j.lock.Close(); err == nil {
		err = lockErr
	}
	return err
}

func (e *Engine) storageFailure(err error) error {
	e.fatal = fmt.Errorf("%w; execution stopped: %v", ErrStorageUnknown, err)
	for _, entry := range e.tasks {
		if entry.cancel != nil {
			entry.cancel()
		}
	}
	return e.fatal
}

func (e *Engine) save(item record) error {
	if e.fatal != nil {
		return e.fatal
	}
	if e.journal == nil {
		return nil
	}
	item.Version = 2
	started := time.Now()
	data, err := json.Marshal(item)
	encoded := time.Now()
	if err == nil && len(data)+1 > maxRecordBytes {
		err = fmt.Errorf("journal frame exceeds limit")
	}
	if err == nil {
		_, err = e.journal.file.Write(append(data, '\n'))
	}
	appended := time.Now()
	if err == nil {
		err = e.journal.file.Sync()
	}
	if e.config.Trace != nil {
		end := time.Now()
		event := TraceEvent{Phase: "wal_sync", Operation: e.traceOperation, StartNS: started.UnixNano(), EndNS: end.UnixNano(), DurationNS: end.Sub(started).Nanoseconds(), Bytes: len(data) + 1, State: "synced"}
		if err != nil {
			event.State = "failed"
		}
		if item.Run != nil {
			event.Owner = item.Run.Owner
			if e.traceOperation == "ack" {
				event.Sequence = item.Run.Ack
			}
		}
		if item.Task != nil {
			event.Owner = item.Task.Task.Owner
			event.TaskID = item.Task.Task.ID
			event.State = item.Task.State
			if err != nil {
				event.State = "failed"
			}
		}
		if item.Event != nil {
			event.Sequence = item.Event.Sequence
		}
		e.config.Trace(event)
		for _, part := range []struct {
			name       string
			start, end time.Time
		}{{"wal_encode", started, encoded}, {"wal_append", encoded, appended}, {"wal_fsync", appended, end}} {
			span := event
			span.Phase = part.name
			span.StartNS = part.start.UnixNano()
			span.EndNS = part.end.UnixNano()
			span.DurationNS = part.end.Sub(part.start).Nanoseconds()
			e.config.Trace(span)
		}
	}
	if err == nil {
		if item.Task != nil {
			e.journal.tasks[taskKey(item.Task.Task.Owner, item.Task.Task.ID)] = *item.Task
		}
		if item.Run != nil {
			e.journal.runs[item.Run.Owner] = *item.Run
		}
		if item.Event != nil {
			e.journal.events = append(e.journal.events, *item.Event)
		}
		e.journal.bytes += int64(len(data) + 1)
		if e.journal.bytes >= e.config.SegmentBytes {
			err = e.compact(item)
		}
	}
	if err != nil {
		return e.storageFailure(err)
	}
	return nil
}

func (e *Engine) compact(cause record) error {
	start := time.Now()
	count := len(e.journal.tasks) + len(e.journal.events) + len(e.journal.runs)
	err := e.journal.compact(e.history)
	if e.config.Trace != nil {
		end := time.Now()
		event := TraceEvent{Phase: "compaction", Operation: e.traceOperation, StartNS: start.UnixNano(), EndNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds(), Records: count, State: "completed"}
		if cause.Run != nil {
			event.Owner = cause.Run.Owner
		}
		if cause.Task != nil {
			event.Owner = cause.Task.Task.Owner
			event.TaskID = cause.Task.Task.ID
		}
		if err != nil {
			event.State = "failed"
		}
		e.config.Trace(event)
	}
	return err
}

// Compact never deletes canonical facts, identities, dependencies or fences.
func (e *Engine) Compact() error {
	defer e.lock("compact", Owner{}, "")()
	if e.closing {
		return ErrClosed
	}
	if e.fatal != nil {
		return e.fatal
	}
	if e.journal == nil {
		return fmt.Errorf("compaction requires a durable journal")
	}
	if err := e.compact(record{}); err != nil {
		return e.storageFailure(err)
	}
	return nil
}
