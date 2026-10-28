# Durable history and bounded active state

This stage implements the first priority from the review of `4f098e57`.
It preserves the original architecture and the execution-time authority/lease
hook. Python worker reuse, group commit and Patrol lifecycle integration remain
separate research stages.

## Acceptance contract

- Execute at least 10,000 real local tools while repeatedly opening/closing Runs.
- Bound resident historical state, including Python connection metadata.
- Page archives by both count and encoded JSON bytes, with resumable owner-scoped
  cursors; 17 results of 512 KiB must not break the shared connection.
- Compaction and restart preserve historical status, original-ID reconciliation,
  dependency outputs, closed Run custody/Ack and unknown resource fences.
- Recover interrupted snapshot updates and both sides of WAL rotation. Explicit
  submission uncertainty and checkpoint-before-ack remain unchanged.

## Retention policy and disk layout

In durable mode (`-journal` nonempty), the resident maps contain only open Runs,
nonterminal tasks, unresolved `unknown` executions and live unacknowledged
results. A second, bounded cache holds only the current WAL segment (default
8 MiB of serialized transitions plus at most one frame; experiments use 256
KiB). Its latest statuses and facts serve reads until indexing/rotation. Beyond
that recent segment, terminal identity/payload and old dependencies are read
from a SHA-256-keyed disk index instead of pinning history.
Acknowledgement evicts the live event; closing a Run evicts its remaining live
events and Run state. Canonical disk records remain with `runtime_archive`.

Limits: `-capacity` bounds nonterminal tasks **plus unknown fences**;
`-per-run-capacity` bounds that live Run's pending tasks and unacked facts;
`-max-active-runs` (default max(128, capacity)) bounds open Run records.
Unknown executions deliberately keep a capacity slot and their resource claims
until an explicit resolution mechanism is implemented. Compaction never
resolves them. A restart with limits below recoverable live state fails closed.
Memory-only mode preserves history in memory and makes no retention guarantee.

The disk index (`<journal>.store`) retains canonical task identities, terminal
results, Run revision/Ack/counters, and owner/sequence-indexed events indefinitely.
No disk TTL or destructive expiry is implemented: eternal deduplication requires
retaining old identities somewhere. Disk use grows with facts; this work bounds
resident history and redundant WAL transitions, not total durable storage.
Do not delete or move individual index files. Move/backup the journal, `.store`,
and any `.sealed` segment together while the runtime is stopped.

## WAL rotation and compaction

Each mutation appends and fsyncs one complete JSONL record before returning
success. It also updates the bounded current-segment cache. Compaction coalesces
intermediate statuses into their latest state, then writes canonical files and
live indexes using synced temporary files and atomic replacement. Every event
fact is retained. This batches derived index updates, **not** durable WAL
acknowledgements: each original mutation still waits for its own fsync.
On Unix, containing directories are fsynced; Windows uses synced files and
[`MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH)`](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw). Faulty hardware/power-loss
guarantees are not established by process-crash tests.

When `-segment-bytes` is reached (default 8 MiB), the WAL is indexed then sealed
as `<journal>.sealed`, a fresh active WAL is created and synced, and the redundant
sealed transitions are removed. `compact` triggers the same operation explicitly.
The stable `<journal>.lock` remains held across rotation. At startup, sealed then
active frames replay idempotently into disk indexes before loading live state.
Only a torn final frame is discarded; complete malformed frames fail closed.
Replay is streaming with a 32 MiB **per-frame** bound, replacing the old 128 MiB
whole-journal cap. Normal WAL size is segment target plus at most one frame.

Original unversioned journals migrate automatically: event facts reconstruct
Run counters without a resident map of every old Run. Stop the old binary before
upgrading; mixed-version concurrent writers are unsupported. Once compacted,
the original binary cannot read the new disk index; downgrade needs a backup.

## Pagination

`archive_events` now returns `{items, next_cursor?, through}`. Request fields:
`owner`, `cursor` (or initial `after`), `limit` (default 100, range 1..1000) and
`max_bytes` (default 2 MiB, range 512 bytes..4 MiB). The byte budget covers the
encoded page object; the small JSONL response envelope is additional. An event
larger than the budget returns an explicit error without consuming it. The 4 MiB
maximum keeps page responses below the client's 8 MiB line limit. A single event
over 4 MiB requires a future blob/chunk interface; it is not silently truncated.

The cursor encodes owner, last sequence and a fixed global high-water sequence.
It is stable across compaction/restart and rejected for another Context/Run.
It is a continuation token, not an authentication credential. Facts produced
after the first page's high-water mark require a new query. An eight-level radix
index reads at most 256 canonical directory entries per level, never listing an
entire historical Run in memory. Open/unresolved startup indexes contain only
live files, enumerated in bounded batches.

Python: `await run.archive_page(...)` exposes the cursor;
`async for event in run.iter_archived_events(...)` streams pages. The old
`await run.archived_events()` helper accepts only a single-page archive and
raises explicitly if continuation is required. `event_page` provides bounded
live-result recovery; the old wire `events` helper similarly rejects a history
that requires continuation. Archive readers cannot advance the closed Run's Ack.

## Reproduction

`python benchmarks/retention.py --out results/local/retention.json` runs 10,000
real Python leaf tools (file reads, text search and JSON parsing) through the
adapter, execution-time authorization, stdio daemon, fsynced storage, fsynced
consumer checkpoint and ack. Four Contexts churn Runs every 32 tasks, sampling
post-GC heap and all resident indexes every 1,000 tasks. The JSON report records
source fingerprints; a companion JSONL file records every completion timing and
consumption timestamp. Final compaction/restart reconciles the original task and
continues an archive cursor. This is a retention experiment, not an Agent
speedup claim or production Patrol soak test.

Regression tests separately cover dependency binding after eviction, closed
unknown fences after ack/compaction/restart, legacy migration, partially applied
indexes and sealed/new-segment recovery boundaries. The ordinary verification
entry point remains `python scripts/verify.py`.

Measured results for 10,000 tools / 320 Runs, raw traces and limitations:
[retention results](retention-results.md).
