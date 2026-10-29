"""Single-host result-driven Patrol wake loop, with durable state behind a port.

store.pending_wakes() returns context_id/loop_id/generation records that remain
pending until semantic consumption commits. Notifications only reduce latency;
startup and periodic reconciliation always read the durable source of truth.

patrol.prepare(loop_id, context_ids) returns False while a Loop is busy, or
commits one real Patrol decision. patrol.dispatch() obeys the host's RunPool;
patrol.reconcile() reaps completions and checkpoints/acks without blocking on
unfinished Runs. No model, scheduler framework or persistence dependency here.
"""
import asyncio
import time


class PatrolWakeService:
    def __init__(self, store, patrol, *, poll_seconds=0.1, coalesce_seconds=0.025,
                 max_planners=4, trace=None):
        if poll_seconds <= 0 or coalesce_seconds < 0 or max_planners < 1:
            raise ValueError("positive polling/planner bounds and nonnegative coalescing required")
        self.store, self.patrol = store, patrol
        self.poll_seconds, self.coalesce_seconds = poll_seconds, coalesce_seconds
        self.max_planners, self.trace = max_planners, trace
        self._notice = asyncio.Event()
        self._task = None
        self._planners = {}
        self._seen = {}
        self._turn = 0
        self.last_error = None

    def emit(self, kind, **fields):
        if self.trace:
            try:
                self.trace({"kind": kind, "time_ns": time.time_ns(), **fields})
            except Exception:
                pass  # Optional diagnostics never change ownership or ack.

    def notify(self, loop_id):
        self.emit("notified", loop_id=loop_id)
        self._notice.set()

    async def start(self):
        if self._task is not None:
            raise RuntimeError("wake service already started")
        self._task = asyncio.create_task(self._run(), name="patrol-result-wakes")

    async def _prepare(self, loop_id, contexts):
        if await self.patrol.prepare(loop_id, contexts):
            self.emit("prepared", loop_id=loop_id, context_ids=contexts)
            self._notice.set()

    def _reap(self):
        for loop_id, task in list(self._planners.items()):
            if not task.done():
                continue
            del self._planners[loop_id]
            if not task.cancelled():
                error = task.exception()
                if error:
                    self.last_error = str(error)
                    self.emit("prepare_failed", loop_id=loop_id, error=str(error))

    async def _step(self):
        self._reap()
        await self.patrol.reconcile()
        rows = await self.store.pending_wakes()
        current = {row["context_id"]: row["generation"] for row in rows}
        for row in rows:
            if self._seen.get(row["context_id"]) != row["generation"]:
                self.emit("wake_observed", **row)
        self._seen = current  # No closed-Context tombstones or history cache.
        groups = {}
        for row in rows:
            groups.setdefault(row["loop_id"], []).append(row["context_id"])
        loops = list(groups)
        if loops:
            offset = self._turn % len(loops)
            loops = loops[offset:] + loops[:offset]
            self._turn += 1
        for loop_id in loops:
            if len(self._planners) >= self.max_planners:
                break
            if loop_id not in self._planners:
                self._planners[loop_id] = asyncio.create_task(
                    self._prepare(loop_id, groups[loop_id]), name="patrol-prepare:" + loop_id)
        await self.patrol.dispatch()

    async def _run(self):
        while True:
            try:
                await self._step()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.last_error = str(error)
                self.emit("reconcile_failed", error=str(error))
            try:
                await asyncio.wait_for(self._notice.wait(), self.poll_seconds)
                self._notice.clear()
                if self.coalesce_seconds:
                    await asyncio.sleep(self.coalesce_seconds)
            except asyncio.TimeoutError:
                pass

    async def close(self):
        tasks = [*self._planners.values()]
        if self._task is not None:
            tasks.append(self._task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._planners.clear()
        self._seen.clear()
        self._task = None
