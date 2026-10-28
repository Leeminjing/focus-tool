# Bounded background compaction contract

Spec for the research stage after `e615b00d`. Preserve every per-mutation WAL
fsync, original ownership, checkpoint-before-ack, idempotency and unknown fences.
Keep `sync` compaction as an explicit comparison mode; default becomes background.
No worker-pool, group-commit or Patrol lifecycle change is part of this stage.

## Visibility and publication

- One Engine owns one active WAL and at most one immutable `.sealed` segment.
  The immutable segment retains its coalesced task/Run cache and ordered facts
  while a single builder updates the existing derived index files.
- Reads prefer live Engine state, then active WAL cache, then sealed cache, then
  disk. Newer active state always wins. Partial disk updates are masked by the
  sealed overlay. Index file replacement and reads synchronize per record,
  not across the full build; this also avoids Windows open-file rename races.
- Pages read disk events only through the published sequence, followed by the
  ordered, newer sealed and active events. The owner/high-water cursor contract
  is unchanged. No partial build can duplicate or reorder a page fact.
- Seal and fsync a fresh active WAL under a short Engine critical section. Build
  and fsync derived files outside that lock. Fsync/atomically replace a publication
  record (generation and covered sequence), advance in-memory visibility, then
  durably remove the covered sealed WAL and release its cache. WAL acknowledgement
  never waits for derived-index publication unless bounded storage is saturated.

## Bounds and backpressure

Exactly one sealed segment/builder and one active segment. Each normally reaches
the configured segment target plus at most one legal frame (32 MiB maximum).
Resident recent-history caches therefore cover at most two such segments; disk
canonical history remains intentionally unexpired. A legacy oversized active WAL
is replayed streaming before service, not loaded into an unbounded cache.

If the active segment fills while the builder is busy, mutation processing waits
for publication before another append. It holds a mutation-order gate, releases
the Engine mutex, and reacquires it before continuing. Other mutations queue at
the gate; reads and the pending-result pump can continue. Diagnostics distinguish
mutation-gate waiting, actual Engine lock intervals, and compaction backpressure.
The stdio request handler remains serial, so a blocked mutating request also
queues subsequent client requests; this limitation must remain measurable.

## Recovery and shutdown

Before serving, replay sealed then active WAL into the derived files, regardless
of an interrupted publication marker. These records repair any partially built
index. Newer active records replay last. Only after repair is durable may sealed
records be removed. Complete malformed frames fail closed. Existing torn-final-
active-frame recovery and historical format migration remain supported.

Crash tests stop real processes after seal, active creation, partial build,
durable publication, visible publication and retirement. Verify historical ID
reconciliation, dependency output binding, paginated cursors and unknown fences.
Any background storage failure stops execution and rejects further success;
replayable WAL is retained. Shutdown joins the builder before closing the journal
lock or index ownership. Explicit `compact` waits for all prior facts to be indexed.
`Close(ctx)` can time out at mutation-gate admission, cancellation persistence or
builder join; it retains journal ownership and retries unfinished cancellation on
a later Close. It cannot interrupt an OS file fsync already in progress.

## Acceptance experiments

Same pooled tools/inputs, concurrency, WAL segment targets, per-record fsync,
checkpoint, action/readback and ack in sync/background arms. Keep 64 KiB pressure
and run enough tasks for the default 8 MiB target to compact multiple times.
Record raw stage traces, end-to-end p50/p95/p99, submission/completion/delivery/ack,
tool-end-to-action, cross-Context lock/gate waits and publication/retirement.
Cold startup and final builder/worker cleanup are included separately.
Trace-on/off controls and a deliberately stalled builder must establish that
one-segment backlog/cache bounds hold without hiding backpressure or claiming
steady memory from a finite run. Patrol cross-Run integration remains pending.
