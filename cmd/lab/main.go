// lab is a local, model-free experiment, including its own leaf worker processes.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"time"

	tool "focus-tool-runtime/internal/runtime"
)

type Config struct {
	Valid bool `json:"valid"`
}
type WorkerOutput struct {
	ConfigValid *bool  `json:"config_valid,omitempty"`
	Passes      int    `json:"passes,omitempty"`
	Digest      string `json:"digest,omitempty"`
}

func emit(v any) error { return json.NewEncoder(os.Stdout).Encode(v) }

func worker(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("worker requires kind and directory")
	}
	kind, dir := args[0], args[1]
	switch kind {
	case "check", "repair":
		path := filepath.Join(dir, "config.json")
		if kind == "repair" {
			// A per-trial output; the input snapshot used by scan remains immutable.
			path = filepath.Join(dir, "repaired.json")
			if err := os.WriteFile(path, []byte(`{"valid":true}`), 0600); err != nil {
				return err
			}
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var config Config
		if err := json.Unmarshal(data, &config); err != nil {
			return err
		}
		return emit(WorkerOutput{ConfigValid: &config.Valid})
	case "scan":
		if len(args) != 3 {
			return fmt.Errorf("scan requires passes")
		}
		passes, err := strconv.Atoi(args[2])
		if err != nil || passes < 1 {
			return fmt.Errorf("passes must be positive")
		}
		for pass := 1; pass <= passes; pass++ {
			file, err := os.Open(filepath.Join(dir, "corpus.txt"))
			if err != nil {
				return err
			}
			hash := sha256.New()
			_, copyErr := io.Copy(hash, file)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			if pass == 1 || pass == passes {
				if err := emit(WorkerOutput{Passes: pass, Digest: hex.EncodeToString(hash.Sum(nil))}); err != nil {
					return err
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown worker: %s", kind)
	}
}

type Trial struct {
	Mode              string        `json:"mode"`
	Repetition        int           `json:"repetition"`
	DecisionMS        float64       `json:"decision_ms"`
	TotalMS           float64       `json:"total_ms"`
	ConsumeLagMS      float64       `json:"consume_lag_ms"`
	ScanActiveMS      float64       `json:"scan_active_ms"`
	CancelAckMS       float64       `json:"cancel_ack_ms"`
	ScanState         string        `json:"scan_state"`
	Repaired          bool          `json:"repaired"`
	StaleRejected     bool          `json:"stale_rejected"`
	DuplicateRejected bool          `json:"duplicate_rejected"`
	Results           []tool.Result `json:"results"`
	Events            []tool.Event  `json:"events"`
}

func fixture() (string, error) {
	dir, err := os.MkdirTemp("", "focus-tool-lab-")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"valid":false}`), 0600); err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	// Fixed 8 MiB corpus; repeated SHA-256 passes are actual local I/O and CPU work.
	line := []byte("focus tool runtime immutable snapshot corpus line 0123456789abcdef\n")
	data := bytes.Repeat(line, (8*1024*1024/len(line))+1)[:8*1024*1024]
	if err := os.WriteFile(filepath.Join(dir, "corpus.txt"), data, 0600); err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	return dir, nil
}

func runTrial(executable, mode string, passes, repetition int) (Trial, error) {
	dir, err := fixture()
	if err != nil {
		return Trial{}, err
	}
	defer os.RemoveAll(dir)
	trace := tool.NewTrace()
	runner := tool.NewRunner(2, trace) // Same cap for barrier and event-driven modes.
	inbox := tool.NewInbox()
	inbox.Revision["context-A"] = 1
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	task := func(id string, revision int) tool.Task {
		command := []string{executable, "worker", id, dir}
		if id == "scan" {
			command = append(command, strconv.Itoa(passes))
		}
		return tool.Task{ID: id, ContextID: "context-A", Revision: revision, Command: command}
	}
	deliver := func(result tool.Result) string {
		trace.Add("delivered", result.Task, result.PID, "")
		status := inbox.Consume(result)
		trace.Add("consumed_"+status, result.Task, result.PID, "")
		return status
	}
	trial := Trial{Mode: mode, Repetition: repetition}
	check := runner.Start(ctx, task("check", 1))
	var checked, scanned tool.Result
	var scan *tool.Job
	if mode == "serial" {
		checked = <-check.Done
		scan = runner.Start(ctx, task("scan", 1))
		scanned = <-scan.Done
	} else {
		scan = runner.Start(ctx, task("scan", 1))
		checked = <-check.Done
		if mode == "barrier" {
			scanned = <-scan.Done
		}
	}
	if deliver(checked) != "accepted" {
		scan.Cancel()
		if mode == "event" {
			<-scan.Done
		}
		return trial, fmt.Errorf("check failed: %+v", checked)
	}
	var checkedOutput WorkerOutput
	if err := json.Unmarshal([]byte(checked.Output), &checkedOutput); err != nil || checkedOutput.ConfigValid == nil || *checkedOutput.ConfigValid {
		scan.Cancel()
		if mode == "event" {
			<-scan.Done
		}
		return trial, fmt.Errorf("expected invalid configuration, got %q", checked.Output)
	}
	trial.DuplicateRejected = deliver(checked) == "duplicate" // Deliberate replay.
	if mode != "event" {
		deliver(scanned)
	}
	trace.Add("decision", checked.Task, 0, "configuration invalid; revise plan")
	inbox.Revision["context-A"] = 2
	if mode == "event" {
		scan.Cancel()
	}
	// The continuation can run while the obsolete read-only scan is stopping.
	repair := runner.Start(ctx, task("repair", 2))
	fixed := <-repair.Done
	if mode == "event" {
		scanned = <-scan.Done
		trial.StaleRejected = deliver(scanned) == "stale"
	}
	if deliver(fixed) != "accepted" {
		return trial, fmt.Errorf("repair failed: %+v", fixed)
	}
	var fixedOutput WorkerOutput
	if err := json.Unmarshal([]byte(fixed.Output), &fixedOutput); err != nil {
		return trial, err
	}
	trial.Repaired = fixedOutput.ConfigValid != nil && *fixedOutput.ConfigValid
	trial.ScanState = scanned.State
	if mode != "event" && scanned.State != "completed" {
		return trial, fmt.Errorf("baseline scan failed: %+v", scanned)
	}
	if scanned.State == "completed" {
		var scanOutput WorkerOutput
		if err := json.Unmarshal([]byte(scanned.Output), &scanOutput); err != nil || scanOutput.Passes != passes || len(scanOutput.Digest) != 64 {
			return trial, fmt.Errorf("scan returned incomplete work: %q", scanned.Output)
		}
	}
	if mode == "event" && scanned.State != "completed" && scanned.State != "cancelled" {
		return trial, fmt.Errorf("scan failed instead of cancelled: %+v", scanned)
	}
	if !trial.Repaired || !trial.DuplicateRejected || (mode == "event" && !trial.StaleRejected) {
		return trial, fmt.Errorf("trial invariants failed")
	}
	trial.Results = []tool.Result{checked, scanned, fixed}
	trial.Events = trace.Events()
	var checkEnd, consume, scanStart, scanEnd, cancelAt float64
	for _, event := range trial.Events {
		if event.TaskID == "check" && event.Kind == "completed" {
			checkEnd = event.AtMS
		}
		if event.TaskID == "check" && event.Kind == "consumed_accepted" {
			consume = event.AtMS
		}
		if event.Kind == "decision" {
			trial.DecisionMS = event.AtMS
		}
		if event.TaskID == "scan" {
			switch event.Kind {
			case "started":
				scanStart = event.AtMS
			case "completed", "cancelled", "failed":
				scanEnd = event.AtMS
			case "cancel_requested":
				cancelAt = event.AtMS
			}
		}
		if event.AtMS > trial.TotalMS {
			trial.TotalMS = event.AtMS
		}
	}
	trial.ConsumeLagMS = consume - checkEnd
	if scanStart > 0 {
		trial.ScanActiveMS = scanEnd - scanStart
	}
	if cancelAt > 0 {
		trial.CancelAckMS = scanEnd - cancelAt
	}
	return trial, nil
}

type Report struct {
	Created     string  `json:"created"`
	GoVersion   string  `json:"go_version"`
	Platform    string  `json:"platform"`
	Concurrency int     `json:"concurrency"`
	CorpusBytes int     `json:"corpus_bytes"`
	ScanPasses  int     `json:"scan_passes"`
	Trials      []Trial `json:"trials"`
}

func median(values []float64) float64 {
	sort.Float64s(values)
	n := len(values)
	if n%2 == 0 {
		return (values[n/2-1] + values[n/2]) / 2
	}
	return values[n/2]
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		if err := worker(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	repeat := flag.Int("repeat", 3, "repetitions; mode order rotates per repetition")
	passes := flag.Int("passes", 256, "fixed SHA-256 scan passes over an 8 MiB local corpus")
	output := flag.String("out", "results/local/report.json", "raw JSON report")
	flag.Parse()
	if *repeat < 1 || *passes < 1 {
		fmt.Fprintln(os.Stderr, "repeat and passes must be positive")
		os.Exit(2)
	}
	executable, err := os.Executable()
	if err != nil {
		panic(err)
	}
	report := Report{Created: time.Now().Format(time.RFC3339), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, Concurrency: 2, CorpusBytes: 8 * 1024 * 1024, ScanPasses: *passes}
	modes := []string{"serial", "barrier", "event"}
	for repetition := 0; repetition < *repeat; repetition++ {
		for offset := 0; offset < len(modes); offset++ {
			mode := modes[(repetition+offset)%len(modes)]
			trial, err := runTrial(executable, mode, *passes, repetition+1)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			report.Trials = append(report.Trials, trial)
			fmt.Printf("%-7s #%d total=%8.2fms decision=%8.2fms consume_lag=%8.2fms scan=%s\n", mode, repetition+1, trial.TotalMS, trial.DecisionMS, trial.ConsumeLagMS, trial.ScanState)
		}
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		panic(err)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(*output, append(data, '\n'), 0600); err != nil {
		panic(err)
	}
	fmt.Println("\nMedian total / decision / consume lag (ms):")
	for _, mode := range modes {
		var totals, decisions, lags []float64
		for _, trial := range report.Trials {
			if trial.Mode == mode {
				totals = append(totals, trial.TotalMS)
				decisions = append(decisions, trial.DecisionMS)
				lags = append(lags, trial.ConsumeLagMS)
			}
		}
		fmt.Printf("%-7s %9.2f / %9.2f / %9.2f\n", mode, median(totals), median(decisions), median(lags))
	}
	fmt.Println("Raw trace:", *output)
}
