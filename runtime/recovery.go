package runtime

import (
	"fmt"
	"path/filepath"
	"sort"
)

func (e *Engine) restore() error {
	h := e.history
	if _, err := readJSON(filepath.Join(h.root, "sequence.json"), &e.sequence); err != nil {
		return err
	}
	if err := h.live("open", func(path string) error {
		var state RunState
		if _, err := readJSON(path, &state); err != nil {
			return err
		}
		if state.Closed {
			return fmt.Errorf("closed Run in open index")
		}
		if len(e.runs) >= e.config.MaxActiveRuns {
			return fmt.Errorf("recovery exceeds active Run limit; increase max-active-runs")
		}
		e.runs[state.Owner] = state
		count := 0
		return h.events(state.Owner, state.Ack, e.sequence, func(event Completion) (bool, error) {
			count++
			if count > e.config.PerRunCapacity {
				return true, fmt.Errorf("recovery exceeds per-Run capacity; increase limit")
			}
			e.events = append(e.events, event)
			return false, nil
		})
	}); err != nil {
		return err
	}
	if err := h.live("unresolved", func(path string) error {
		var status Status
		if _, err := readJSON(path, &status); err != nil {
			return err
		}
		if terminal(status.State) && status.State != "unknown" {
			return fmt.Errorf("terminal task in unresolved index")
		}
		if len(e.tasks) >= e.config.Capacity {
			return fmt.Errorf("recovery exceeds task/fence capacity; increase capacity")
		}
		key := taskKey(status.Task.Owner, status.Task.ID)
		e.tasks[key] = &entry{Status: status}
		if status.State == "queued" {
			e.enqueue(status.Task.Owner, key)
		}
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(e.events, func(i, j int) bool { return e.events[i].Sequence < e.events[j].Sequence })
	charges := map[Owner]int{}
	for _, event := range e.events {
		charges[event.Task.Owner]++
	}
	for _, item := range e.tasks {
		if !terminal(item.State) {
			charges[item.Task.Owner]++
		}
	}
	for owner, count := range charges {
		if _, open := e.runs[owner]; open && count > e.config.PerRunCapacity {
			return fmt.Errorf("recovery exceeds combined per-Run capacity; increase limit")
		}
	}
	return nil
}
