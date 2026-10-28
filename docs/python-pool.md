# Bounded Python worker reuse contract

Implements the next optimization selected by the `749109c3` short-tool profile.
The fresh per-task Python process remains the default. No grouped WAL fsync,
background compaction, provider API or Patrol lifecycle redesign is included.

## Trusted eligibility and capacity

Both opt-ins are required: `ToolDefinition(..., reuse=True)` in trusted adapter
registration and `RuntimeClient.start(..., python_workers=N)` / CLI
`-python-workers N`. Ordinary tool arguments cannot set `PythonCall.Reuse`.
The adapter compares that metadata at execution-time admission; unregistered raw
requests cannot launch an interpreter before permission checks. `reuse=False`
tools always retain fresh-process isolation. Direct Go embedding is trusted and
must supply its existing BeforeExecute authority hook.

One Engine owns the pool and closes it after its active executions finish; do not
share it between independently closed Engines. The pool has a single hard bound
across interpreter/workspace keys. Each worker
has one exclusive lease and at most one active call. A mismatched idle worker is
reaped before its slot starts a different interpreter/workspace. Keys use the
absolute invocation path (preserving virtualenv identity) and resolved workspace.
Worker environments are fixed at creation; replace the pool to reconfigure them.
Idle workers are recycled after 256 successful jobs by default
(`-python-worker-tasks`). Pool size bounds resident workers; fresh/process tools
remain independently bounded by scheduler concurrency. Mixed execution can thus
have at most pool-size + scheduler-concurrency managed child processes, excluding
unsupported descendants created by tools themselves.

## Admission, cancellation and cleanup

The scheduler still owns queueing, resources and running-state persistence.
Preparing an executor reserves capacity without starting a process, replacing a
worker or sending code. After this wait, the existing BeforeExecute hook checks
the trusted registration, permission and lease. Only then may execution start.
Denied preparation releases an unused reservation; it does not run the tool.

Worker lifetime uses plain exec.Command, never the first job's CommandContext.
The active call selects between its frame and its context; no job cancellation
watcher survives lease return. Late cancellation cannot kill a later job.
Each call has a fresh random response nonce; unexpected/duplicate/unsolicited
frames quarantine the worker instead of routing a previous result to a new job.
Pipe writes are cancellable through worker termination and joined before return.

Cancellation, exception, crash, output overflow or protocol failure retires the
worker. Kill is followed by actual Process.Wait and joins of pipe readers/writers
before the executor returns and scheduler resource ownership can be released.
Failed calls are never automatically resubmitted, even after a partial write;
side effects may already have occurred. A valid successful result means function
completion plus the cooperative quiescence contract, rather than process exit.
The executor Release runs before the Engine's finished notification. Engine
Close waits active executions, then closes idle workers and their I/O; close
returns final stats with live/reserved counts zero on success.

This remains leaf-process management. There is no generic process-tree support.
Hard runtime crashes retain existing unknown resource fences and do not imply
that a previously active worker has stopped. WAL/checkpoint/ack semantics and
archive custody remain unchanged; queued recovery admission still needs trusted
registration and the later pause/restore/resume handshake.

## Cooperative state isolation, not a Python sandbox

Reusable tools must be trusted and explicitly satisfy this contract:

- Tool modules and their new imports get fresh namespaces each call. Existing
  runtime modules cannot be declared as reusable tool modules. Shared standard
  library / extension globals, runtime internals, native process state, signal
  handlers, direct protocol descriptors and external descendants must not be
  mutated or used for background work. Use fresh mode for those tools.
- cwd, environment, sys.path/argv/importer cache and warning filters are restored.
  Per-call stdout/stderr are captured under a bounded buffer; one call's prints
  cannot be included in another's result. Logical result and error bodies stay
  bounded independently of protocol envelopes.
- Live Python threads or pending asyncio tasks fail the reuse contract. Managed
  async cleanup completes before response; cancellation-resistant work remains
  held until timeout/cancellation kills and reaps the process. Process-exit hooks
  are rejected. A poisoned worker is never returned to the idle set.

These checks do not prove isolation for arbitrary Python/native code. External
file writes are intentional effects governed by declared resources and authority,
not rolled back. Lifetime recycling also bounds accumulated interpreter caches;
it is not a security boundary or a general RSS guarantee.

## Measurement

Run alone: `python benchmarks/short_tools.py --pool-comparison --out
results/local/python-pool/report.json`, then `python
benchmarks/analyze_short_tools.py results/local/python-pool/report.json --out
results/local/python-pool/summary.json`.

The comparison interleaves fresh/pool × 1/4 Contexts × 64 KiB/8 MiB segments,
with equal eligible registrations, inputs, concurrency, fsync policy, checkpoints,
ack and read-back final artifacts. Cold worker calls in warmup and complete
start-through-shutdown lifecycle time are separate from the steady workload
window. Started/peak/live/reused counters and final zero-live shutdown are saved.
Tracing controls cover both modes. Fresh process-start frequency and pooled
startup counts are recorded separately from potentially recycled numeric PIDs.

Client high-resolution monotonic metrics include submit-to-consume, consumer,
ack and end-to-end completion. Use these for short warm calls. Go's parent
Python-call spans can quantize to zero on the measured Windows host; subtraction
of Python's finer tool-body span is therefore a signed diagnostic, not an exact
non-body latency. Retain its negative values/counts instead of clipping samples.
Cross-process wall-clock boundary differences are retained as signed diagnostic
distributions with negative counts/minima; they are not silently clipped into
plausible latencies. Old process-wait metrics are absent for successful resident
calls because the worker stays alive. Cold bootstrap/ready spans are reported
separately rather than pretending each warm call starts an interpreter.

Measured results, raw traces, clock limitations and the recorded post-collection
analysis correction are in [the pool comparison report](python-pool-results.md).
