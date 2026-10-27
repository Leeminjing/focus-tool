package main

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		if err := worker(os.Args[2:]); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestExperimentPreservesOutcomeAndIsolatesObsoleteResults(t *testing.T) {
	var baselineDigest string
	for _, mode := range []string{"serial", "barrier", "event"} {
		trial, err := runTrial(os.Args[0], mode, 32, 1)
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if !trial.Repaired || !trial.DuplicateRejected {
			t.Fatalf("%s: incomplete outcome", mode)
		}
		if mode != "event" {
			if baselineDigest == "" {
				baselineDigest = trial.Results[1].Output
			}
			if baselineDigest != trial.Results[1].Output {
				t.Fatal("baselines performed different scan work")
			}
			continue
		}
		if !trial.StaleRejected {
			t.Fatal("obsolete scan affected new plan")
		}
		decision, cancelRequested, rejected := -1, -1, -1
		for index, event := range trial.Events {
			if event.Kind == "decision" {
				decision = index
			}
			if event.Kind == "cancel_requested" {
				cancelRequested = index
			}
			if event.Kind == "consumed_stale" {
				rejected = index
			}
		}
		if decision < 0 || cancelRequested <= decision || rejected <= cancelRequested {
			t.Fatalf("incorrect causal ordering: decision=%d cancel=%d rejected=%d", decision, cancelRequested, rejected)
		}
	}
}
