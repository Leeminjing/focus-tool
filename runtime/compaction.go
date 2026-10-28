package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// The sole builder owns immutable data. Mutable lifecycle fields and the
// published watermark are protected by Engine.mu; no builder takes mutation.
type sealedSegment struct {
	walCache
	bytes               int64
	through, generation uint64
	done                chan struct{}
	finished            bool
	cause               record
	operation           string
}

func (e *Engine) compactionStep(phase string) error {
	if e.config.compactionHook != nil {
		return e.config.compactionHook(phase)
	}
	return nil
}

// Called only with the Engine mutex and mutation gate (or during startup).
// A full active segment plus one full sealed segment is the entire backlog.
func (e *Engine) startCompaction(cause record) error {
	j := e.journal
	if j.sealed != nil || j.bytes == 0 {
		return nil
	}
	start := time.Now()
	if err := j.file.Close(); err != nil {
		return err
	}
	j.file = nil
	if err := replaceDurable(j.path, j.path+".sealed"); err != nil {
		return err
	}
	if err := e.compactionStep("sealed"); err != nil {
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
	if err := e.compactionStep("active"); err != nil {
		return err
	}
	through := j.publishedThrough
	if len(j.events) > 0 {
		through = j.events[len(j.events)-1].Sequence
	}
	segment := &sealedSegment{walCache: j.walCache, bytes: j.bytes, through: through,
		generation: j.generation + 1, done: make(chan struct{}), cause: cause, operation: e.traceOperation}
	j.sealed, j.walCache, j.bytes = segment, emptyCache(), 0
	e.traceCompaction("compaction_seal", segment, "completed", start)
	go e.buildCompaction(j, segment)
	return nil
}

func (e *Engine) traceCompaction(phase string, segment *sealedSegment, state string, start time.Time) {
	if e.config.Trace == nil {
		return
	}
	end := time.Now()
	event := TraceEvent{Phase: phase, Operation: segment.operation, State: state,
		Generation: segment.generation, Sequence: segment.through,
		StartNS: start.UnixNano(), EndNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds(),
		Bytes: int(segment.bytes), Records: len(segment.tasks) + len(segment.runs) + len(segment.events)}
	if segment.cause.Run != nil {
		event.Owner = segment.cause.Run.Owner
	}
	if segment.cause.Task != nil {
		event.Owner, event.TaskID = segment.cause.Task.Task.Owner, segment.cause.Task.Task.ID
	}
	e.config.Trace(event)
}

func (e *Engine) buildCompaction(j *journal, segment *sealedSegment) {
	start := time.Now()
	wholeStart := start
	err := segment.walCache.index(e.history, e.compactionStep)
	if err == nil {
		err = e.compactionStep("built")
	}
	e.traceCompaction("compaction_build", segment, compactionState(err), start)
	if err == nil {
		start := time.Now()
		err = writeJSON(filepath.Join(e.history.root, "published.json"), publication{segment.generation, segment.through})
		if err == nil {
			err = e.compactionStep("published")
		}
		e.traceCompaction("compaction_publish", segment, compactionState(err), start)
	}
	if err == nil {
		unlock := e.lock("compaction_visible", Owner{}, "")
		j.publishedThrough, j.generation = segment.through, segment.generation
		unlock()
		err = e.compactionStep("visible")
	}
	if err == nil {
		start := time.Now()
		err = removeDurable(j.path + ".sealed")
		if err == nil {
			err = e.compactionStep("retired")
		}
		e.traceCompaction("compaction_retire", segment, compactionState(err), start)
	}
	unlock := e.lock("compaction_finish", Owner{}, "")
	segment.finished = true
	if err != nil {
		e.storageFailure(fmt.Errorf("background compaction: %w", err))
	} else {
		j.compactions++
		j.sealed = nil
	}
	// The complete span is emitted before done closes, so a flush barrier also
	// drains its diagnostics. Sub-spans never imply durable execution success.
	e.traceCompaction("compaction", segment, compactionState(err), wholeStart)
	close(segment.done)
	unlock()
}

func compactionState(err error) string {
	if err != nil {
		return "failed"
	}
	return "completed"
}

// Wait with the mutation order gate held, but without the Engine lock. This
// split emits two real lock intervals instead of claiming the wait held mu.
func (e *Engine) waitCompaction(segment *sealedSegment, reason string) {
	_ = e.joinCompaction(context.Background(), segment, reason)
}

func (e *Engine) joinCompaction(ctx context.Context, segment *sealedSegment, reason string) error {
	select {
	case <-segment.done:
		return nil
	default:
	}
	j := e.journal
	if reason == "capacity" {
		j.waits++
	}
	span := e.lockSpan
	if span == nil {
		panic("compaction wait outside an Engine mutation")
	}
	e.unlockSpan(span)
	start := time.Now()
	var err error
	select {
	case <-segment.done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	phase := "compaction_join"
	if reason == "capacity" {
		phase = "compaction_backpressure"
	}
	if e.config.Trace != nil {
		end := time.Now()
		e.config.Trace(TraceEvent{Phase: phase, Operation: span.operation, Owner: span.owner, TaskID: span.taskID,
			Generation: segment.generation, State: reason, StartNS: start.UnixNano(), EndNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds()})
	}
	acquire := time.Now()
	e.mu.Lock()
	span.start, span.acquired = acquire, time.Now()
	e.lockSpan, e.traceOperation = span, span.operation
	return err
}
