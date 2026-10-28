package runtime

// The disk index holds canonical facts, not an in-memory map of historical
// identities. Every update is replayable from the fsynced WAL. Only open Runs
// and unresolved executions have startup indexes; acknowledged results are
// loaded by identity or by bounded archive page when needed.
import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const maxRecordBytes = 32 << 20

type history struct{ root string }

func digest(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func (h *history) keyPath(kind, key string) string {
	hash := digest(key)
	if kind == "open" || kind == "unresolved" {
		return filepath.Join(h.root, kind, hash+".json")
	}
	return filepath.Join(h.root, kind, hash[:2], hash[2:4], hash[4:]+".json")
}

func (h *history) eventRoot(owner Owner) string {
	return h.keyPath("events", taskKey(owner, "")) + ".d"
}

func (h *history) eventPath(owner Owner, sequence uint64) string {
	digits := fmt.Sprintf("%016x", sequence)
	path := h.eventRoot(owner)
	for n := 0; n < 14; n += 2 {
		path = filepath.Join(path, digits[n:n+2])
	}
	return filepath.Join(path, digits[14:]+".json")
}

func readJSON(path string, value any) (bool, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxRecordBytes+1))
	if err != nil {
		return false, err
	}
	if len(data) > maxRecordBytes {
		return false, fmt.Errorf("history record too large")
	}
	if err := json.Unmarshal(data, value); err != nil {
		return false, fmt.Errorf("corrupt history %s: %w", path, err)
	}
	return true, nil
}

func writeJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > maxRecordBytes {
		return fmt.Errorf("history record too large")
	}
	if err := durableMkdir(filepath.Dir(path)); err != nil {
		return err
	}
	temporary := path + ".pending"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer os.Remove(temporary)
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = replaceDurable(temporary, path)
	}
	return err
}

func removeDurable(path string) error {
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func durableMkdir(path string) error {
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("not a directory: %s", path)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(path)
	if parent == path {
		return fmt.Errorf("missing filesystem root")
	}
	if err := durableMkdir(parent); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	return syncDirectory(parent)
}

// apply is idempotent. A crash at ANY file operation leaves the complete record
// in the WAL; replay repairs derived indexes before admitting any requests.
func (h *history) apply(item record) error {
	if item.Run != nil {
		state := item.Run
		key := taskKey(state.Owner, "")
		if err := writeJSON(h.keyPath("runs", key), state); err != nil {
			return err
		}
		live := h.keyPath("open", key)
		if state.Closed {
			if err := removeDurable(live); err != nil {
				return err
			}
		} else if err := writeJSON(live, state); err != nil {
			return err
		}
	}
	if item.Task != nil {
		status := item.Task
		key := taskKey(status.Task.Owner, status.Task.ID)
		if err := writeJSON(h.keyPath("tasks", key), status); err != nil {
			return err
		}
		live := h.keyPath("unresolved", key)
		if terminal(status.State) && status.State != "unknown" {
			if err := removeDurable(live); err != nil {
				return err
			}
		} else if err := writeJSON(live, status); err != nil {
			return err
		}
	}
	if item.Event != nil {
		event := item.Event
		if err := writeJSON(h.eventPath(event.Task.Owner, event.Sequence), event); err != nil {
			return err
		}
		// Monotonic sequence is part of durable identity, including after all
		// live events have been acknowledged or their Runs closed.
		var sequence uint64
		if _, err := readJSON(filepath.Join(h.root, "sequence.json"), &sequence); err != nil {
			return err
		}
		if event.Sequence > sequence {
			if err := writeJSON(filepath.Join(h.root, "sequence.json"), event.Sequence); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *history) status(owner Owner, id string) (Status, bool, error) {
	var status Status
	ok, err := readJSON(h.keyPath("tasks", taskKey(owner, id)), &status)
	if ok && (status.Task.Owner != owner || status.Task.ID != id) {
		return status, false, fmt.Errorf("history identity mismatch")
	}
	return status, ok, err
}

func (h *history) run(owner Owner) (RunState, bool, error) {
	var state RunState
	ok, err := readJSON(h.keyPath("runs", taskKey(owner, "")), &state)
	if ok && state.Owner != owner {
		return state, false, fmt.Errorf("history owner mismatch")
	}
	return state, ok, err
}

// Used ONLY for the bounded startup indexes, never for the historical corpus.
func (h *history) live(kind string, visit func(string) error) error {
	root := filepath.Join(h.root, kind)
	var walk func(string) error
	walk = func(path string) error {
		file, err := os.Open(path)
		if os.IsNotExist(err) && path == root {
			return nil
		}
		if err != nil {
			return err
		}
		defer file.Close()
		for {
			entries, err := file.ReadDir(128)
			if err != nil && err != io.EOF {
				return err
			}
			for _, entry := range entries {
				child := filepath.Join(path, entry.Name())
				if entry.IsDir() {
					if err := walk(child); err != nil {
						return err
					}
				} else if strings.HasSuffix(entry.Name(), ".json") {
					if err := visit(child); err != nil {
						return err
					}
				}
			}
			if err == io.EOF {
				return nil
			}
		}
	}
	return walk(root)
}

// Upgrade the original WAL without keeping a map of every historical Run.
// Recomputing from canonical event identities is idempotent after migration
// crashes; new versioned WAL records carry these counters transactionally.
func (h *history) migrateCounters() error {
	return h.live("runs", func(path string) error {
		var state RunState
		if _, err := readJSON(path, &state); err != nil {
			return err
		}
		state.Facts, state.Consumed = 0, 0
		if err := h.events(state.Owner, 0, ^uint64(0), func(event Completion) (bool, error) {
			state.Facts++
			if event.Sequence <= state.Ack {
				state.Consumed++
			}
			return false, nil
		}); err != nil {
			return err
		}
		return h.apply(record{Version: 2, Run: &state})
	})
}

// Each radix directory has at most 256 entries. Memory is bounded by radix
// depth + page, even when a single Run has millions of historical results.
func (h *history) events(owner Owner, after, through uint64, visit func(Completion) (bool, error)) error {
	var walk func(string, string) (bool, error)
	walk = func(path, prefix string) (bool, error) {
		entries, err := os.ReadDir(path)
		if os.IsNotExist(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			name := entry.Name()
			if strings.HasSuffix(name, ".pending") {
				continue
			}
			if len(prefix) == 14 {
				name = strings.TrimSuffix(name, ".json")
			}
			if len(name) != 2 {
				return false, fmt.Errorf("invalid event index entry")
			}
			part := prefix + name
			low, err := strconv.ParseUint(part+strings.Repeat("0", 16-len(part)), 16, 64)
			if err != nil {
				return false, err
			}
			high, _ := strconv.ParseUint(part+strings.Repeat("f", 16-len(part)), 16, 64)
			if high <= after {
				continue
			}
			if low > through {
				return true, nil
			}
			if len(part) < 16 {
				stop, err := walk(filepath.Join(path, entry.Name()), part)
				if stop || err != nil {
					return stop, err
				}
			} else {
				var event Completion
				ok, err := readJSON(filepath.Join(path, entry.Name()), &event)
				if err != nil {
					return false, err
				}
				if !ok || event.Sequence != low || event.Task.Owner != owner {
					return false, fmt.Errorf("corrupt event index identity")
				}
				stop, err := visit(event)
				if stop || err != nil {
					return stop, err
				}
			}
		}
		return false, nil
	}
	_, err := walk(h.eventRoot(owner), "")
	return err
}

type archiveCursor struct {
	Owner   Owner  `json:"owner"`
	After   uint64 `json:"after"`
	Through uint64 `json:"through"`
}

func encodeCursor(owner Owner, after, through uint64) string {
	data, _ := json.Marshal(archiveCursor{owner, after, through})
	return base64.RawURLEncoding.EncodeToString(data)
}

func (e *Engine) ArchivePage(owner Owner, options PageOptions) (ArchivePage, error) {
	return e.resultPage(owner, options, true)
}

func (e *Engine) EventPage(owner Owner, options PageOptions) (ArchivePage, error) {
	return e.resultPage(owner, options, false)
}

func (e *Engine) resultPage(owner Owner, options PageOptions, archive bool) (ArchivePage, error) {
	defer e.lock("page", owner, "")()
	return e.resultPageLocked(owner, options, archive)
}

func (e *Engine) resultPageLocked(owner Owner, options PageOptions, archive bool) (ArchivePage, error) {
	page := ArchivePage{Items: []Completion{}, Through: e.sequence}
	if e.fatal != nil {
		return page, e.fatal
	}
	state, ok, err := e.runState(owner)
	if err != nil {
		return page, err
	}
	if !ok || (archive && !state.Closed) || (!archive && state.Closed) {
		return page, fmt.Errorf("query requires the matching live/archive custodian")
	}
	if options.Limit == 0 {
		options.Limit = 100
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = 2 << 20
	}
	if options.Limit < 1 || options.Limit > 1000 || options.MaxBytes < 512 || options.MaxBytes > 4<<20 {
		return page, fmt.Errorf("page limit must be 1..1000; max_bytes must be 512..4194304")
	}
	after := options.After
	if options.Cursor != "" {
		var cursor archiveCursor
		data, err := base64.RawURLEncoding.DecodeString(options.Cursor)
		if err != nil || json.Unmarshal(data, &cursor) != nil || cursor.Owner != owner || cursor.After > cursor.Through || cursor.Through > e.sequence || options.After != 0 {
			return page, fmt.Errorf("invalid or cross-owner archive cursor")
		}
		after, page.Through = cursor.After, cursor.Through
	}
	if !archive && after < state.Ack {
		after = state.Ack
	}
	last := after
	stopped := false
	visit := func(event Completion) (bool, error) {
		if len(page.Items) == options.Limit {
			page.NextCursor = encodeCursor(owner, last, page.Through)
			stopped = true
			return true, nil
		}
		candidate := page
		candidate.Items = append(candidate.Items, event)
		candidate.NextCursor = encodeCursor(owner, event.Sequence, page.Through)
		data, _ := json.Marshal(candidate)
		if len(data) > options.MaxBytes {
			if len(page.Items) == 0 {
				return true, ErrPageTooSmall
			}
			page.NextCursor = encodeCursor(owner, last, page.Through)
			stopped = true
			return true, nil
		}
		page.Items, last = candidate.Items, event.Sequence
		return false, nil
	}
	if e.history != nil {
		err = e.history.events(owner, after, page.Through, visit)
		if err == nil && !stopped {
			for _, event := range e.journal.events {
				if event.Task.Owner == owner && event.Sequence > after && event.Sequence <= page.Through {
					stop, visitErr := visit(clone(event))
					if stop || visitErr != nil {
						err = visitErr
						break
					}
				}
			}
		}
	} else {
		for _, event := range e.events {
			if event.Task.Owner == owner && event.Sequence > after && event.Sequence <= page.Through {
				stop, visitErr := visit(clone(event))
				if stop || visitErr != nil {
					err = visitErr
					break
				}
			}
		}
	}
	return page, err
}
