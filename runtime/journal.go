package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type record struct {
	Run   *RunState   `json:"run,omitempty"`
	Task  *Status     `json:"task,omitempty"`
	Event *Completion `json:"event,omitempty"`
}

// A record and its completion are one fsynced JSONL frame. An incomplete final
// frame is discarded on recovery; corruption in a complete frame fails closed.
func openJournal(path string) (*os.File, []record, error) {
	if path == "" {
		return nil, nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (*os.File, []record, error) { file.Close(); return nil, nil, err }
	if err := lockJournal(file); err != nil {
		return fail(err)
	}
	data, err := io.ReadAll(io.LimitReader(file, 128<<20))
	if err != nil {
		return fail(err)
	}
	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	if info.Size() > 128<<20 {
		return fail(fmt.Errorf("journal exceeds 128 MiB research limit"))
	}
	end := bytes.LastIndexByte(data, '\n') + 1
	var records []record
	for _, line := range bytes.Split(data[:end], []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var item record
		if err := json.Unmarshal(line, &item); err != nil {
			return fail(fmt.Errorf("corrupt journal: %w", err))
		}
		if item.Run == nil && item.Task == nil {
			return fail(fmt.Errorf("journal frame has no state"))
		}
		records = append(records, item)
	}
	if err := file.Truncate(int64(end)); err != nil {
		return fail(err)
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fail(err)
	}
	return file, records, nil
}

func (e *Engine) save(item record) error {
	if e.fatal != nil {
		return e.fatal
	}
	if e.journal == nil {
		return nil
	}
	data, err := json.Marshal(item)
	if err == nil {
		var info os.FileInfo
		info, err = e.journal.Stat()
		if err == nil && info.Size()+int64(len(data))+1 > 128<<20 {
			err = fmt.Errorf("journal exceeds 128 MiB research limit; archive it before continuing")
		}
	}
	if err == nil {
		_, err = e.journal.Write(append(data, '\n'))
	}
	if err == nil {
		err = e.journal.Sync()
	}
	if err != nil {
		e.fatal = fmt.Errorf("%w; execution stopped: %v", ErrStorageUnknown, err)
		for _, entry := range e.tasks {
			if entry.cancel != nil {
				entry.cancel()
			}
		}
		return e.fatal
	}
	return nil
}
