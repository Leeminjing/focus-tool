# Short-tool performance measurement contract

This stage implements the measurement priority from the review of `860d5f3b`.
It changes diagnostics and experiments, not scheduling, worker reuse, WAL fsync
acknowledgements, compaction locking, execution admission or result custody.
Patrol activation/runtime ownership and queued recovery handshakes remain later
integration work.

## Observable boundaries

`Config.Trace` / `-trace <fresh-path>` is opt-in. The CLI buffers JSONL diagnostic
events without fsync; they do not replace durable WAL facts. Diagnostics include
Context/Run/task, request/sequence identities and span timestamps, never tool
arguments or outputs. Trace files are finite experiment artifacts, not a
production log-rotation service. Callbacks are concurrency-safe, must not call
back into Engine, and must not raise/panic.

| Boundary | Meaning |
|---|---|
| client request/write | Python request start, completed pipe write/drain, response received |
| server_request | Serial stdio handler start through response write; excludes time waiting unread in the pipe |
| engine_lock | Mutex request, acquisition, release; monotonic wait and hold durations |
| wal_sync | Encode + append + successful fsync; separate wal_encode/wal_append/wal_fsync subspans |
| dispatch_assigned | Slot assignment before running-state persistence, admission and process creation |
| admission | Actual registered-tool and workspace callback round trip; denied calls never start a worker |
| process_start | Entry/return of exec.Cmd.Start, not a claim that user code is running |
| worker_bootstrap | Worker script entry (after importing time) to parsing its call; includes its standard-library imports |
| module_import | Target module import and function lookup |
| tool_body | Function invocation, including asyncio.run for an awaitable result |
| result_encode | Original logical JSON output serialization |
| process_wait | Child wait/reap; resource release still follows actual executor return |
| completion_handoff | Executor return through scheduler receipt and completion-lock acquisition |
| compaction | Actual synchronous index construction / WAL rotation, with triggering owner/task and record count |
| client_receive / bench_job | Connection receipt, Run consumption, checkpoint, durable following action and ack completion |

Existing StartedNS/FinishedNS retain their old meanings; no second WAL write is
added to retroactively persist diagnostic endpoints. A queued wal_sync end is
the observed durable acceptance point; a completed wal_sync end is the observed
durable result point, before any compaction triggered by that frame.

The Python worker sends an internal diagnostic envelope only when enabled. The
Go executor unwraps its original JSON value before persistence/delivery, checks
the same logical output limit, preserves large integers, and stamps owner/task
itself. Worker-provided metadata cannot choose its result owner. Failed/killed
workers may not provide Python subspans; the benchmark requires successful tools
and checks all mandatory boundaries rather than inventing missing timings.

Go, client and worker durations each use their process's monotonic clock.
Unix-nanosecond timestamps correlate the same host's clock across processes;
do not subtract unrelated monotonic epochs. Negative stage intervals fail
analysis; client wall-minus-monotonic residuals are reported. Cross-process
correlation assumes no disruptive host-clock adjustment and is not a distributed
clock synchronization protocol.

## Workload and controls

`python benchmarks/short_tools.py --out results/local/short-tools/report.json`
runs real file reads, text searches and JSON parsing through the full adapter,
trusted-tool validation, local workspace admission callback, Go daemon, fsynced
WAL, Python leaf process, Run checkpoint, durable aggregate update and ack.
The aggregate update is the deterministic effective following action. It has
observable, validated output; no model, simulated sleep or actual Patrol is used.
The callback is not the cost of production Focus policy/lease/database checks.

Each measured trial uses equal fixture bytes, 240 tools, a global concurrency /
in-flight cap of four and the same final aggregate. Single Context has depth
four; four Contexts have depth one each. Thus consumer concurrency differs by
design; Context comparisons are whole-pipeline observations, not a pure mutex
microbenchmark. Setup/build and 24 warmup tasks are outside the timed window.
Every task checkpoint and following artifact is fsynced before ack; thread I/O
uses stdlib asyncio.to_thread. Startup/closing of the daemon is outside this
steady workload timer. Per-task pipeline latency includes submission through ack.

Matrix: 1/4 Contexts × 64 KiB/8 MiB WAL target × three repetitions, rotated in
order. Small segments deliberately exercise many real compactions; large
segments serve as a same-workload no-compaction control within this window.
They are not different durability policies. Independent 120-task controls
alternate tracing on/off in each repetition to estimate diagnostic perturbation;
both use the current implementation. Tiny always-present clock bookkeeping is
not removed by the off switch, so this is incremental tracing overhead rather
than a bit-for-bit comparison against the previous commit.
Both controls still write the common benchmark job/window records needed to
reproduce end-to-end statistics; "off" disables detailed client/Go/worker spans.

Run the performance experiment alone. Do not run tests, other benchmarks, builds
or integrations concurrently. The initial pilot is smoke evidence only; raw
results record repetitions, configuration, production-source fingerprints and
losslessly compressed Go/client JSONL traces. Tests are not build inputs and are
excluded from those fingerprints.

## Reanalysis and interpretation

`python benchmarks/analyze_short_tools.py <report.json> --out <summary.json>`
recomputes each trial's p50/p95/p99 (nearest rank), means, throughput, stage
durations, compaction duration and mutex holds/waits from raw data. Encoding /
append / fsync subspans overlap their wal_sync parent; bootstrap/import/start-API
subspans overlap Python startup; checkpoint/action overlap the consumer interval.
Do not add all reported metrics or percentile columns as though they were
disjoint. Stage means can locate major costs; tail overlap is association.

Requests are grouped by whether their RTT overlaps actual compaction. Other-
Context overlap is computed separately for completed pipe write → handler start
and mutex request → acquisition. A serial handler can delay a request before it
even attempts Engine.mu; reporting only mutex wait would miss this. Interval
intersections are unioned rather than double-counted. Compaction-triggering
requests, unrelated intervals and warmups remain distinguishable in raw traces.
Request-start → handler-start includes all measured requests. Completed-drain →
handler-start is also reported as a signed gap: a server may consume the frame
before drain returns. Negative/overlapping endpoints are counted explicitly;
the nonnegative post-write waiting distribution is labelled conditional.

Acceptance: matching final outputs, raw traces sufficient to reproduce every
stage/percentile, repeated single/multi-Context data, multiple real compactions,
explicit tracing controls and one next optimization chosen from measurements.
No worker pool, grouped fsync or background compaction is implemented this round.
