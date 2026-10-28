package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	tool "focus-tool-runtime/runtime"
)

func TestAdmissionWaitBudgetAndParentCancellation(t *testing.T) {
	for _, parentFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "configured-budget", true: "parent-deadline"}[parentFirst], func(t *testing.T) {
			var wire bytes.Buffer
			a := &approvals{waiting: map[uint64]chan string{}, out: &writer{encoder: json.NewEncoder(&wire)}, timeout: time.Millisecond}
			ctx := context.Background()
			if parentFirst {
				a.timeout = time.Hour
				parent, cancel := context.WithTimeout(ctx, time.Millisecond)
				defer cancel()
				ctx = parent
			}
			if err := a.check(ctx, tool.Task{}); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if len(a.waiting) != 0 {
				t.Fatal("expired admission registration retained")
			}
			if wire.Len() == 0 {
				t.Fatal("admission was bypassed")
			}
		})
	}
}
