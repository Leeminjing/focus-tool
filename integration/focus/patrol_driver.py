"""Focus port and callable service for automatic, durable result wakes.

The scenario runner supplies startup inputs and assertions. This service owns
result pumps, Loop wake driving, RunPool dispatch/reaping and checkpointed ack.
"""
import asyncio
import time

from sqlalchemy import select, func, or_
from backend.app.desktop.persistence.models import DesktopRun
from backend.app.desktop.patrol.agent_loop.models import AgentLoop, LoopRound
from focus_tool import PatrolWakeService


class FocusPatrolPort:
    def __init__(self, harness, tools, trace=None):
        self.harness, self.tools, self.trace = harness, tools, trace
        self._waves, self._finishes = {}, set()
        self._drain = None
        self.owner_errors = {}

    def emit(self, kind, **fields):
        if self.trace:
            try:
                self.trace({"kind": kind, "time_ns": time.time_ns(), **fields})
            except Exception:
                pass

    async def active(self, *, loop_id=None, context_id=None):
        async with self.harness.sessions() as session:
            query = select(func.count()).select_from(DesktopRun).where(DesktopRun.loop_id.is_not(None),
                or_(DesktopRun.status.in_(("pending", "running")), DesktopRun.settled_at.is_(None)))
            if loop_id:
                query = query.where(DesktopRun.loop_id == loop_id)
            if context_id:
                query = query.where(DesktopRun.task_id == context_id)
            return int(await session.scalar(query) or 0)

    async def prepare(self, loop_id, contexts, *, initial=False):
        if self.tools._heartbeat_error is not None:
            return False
        if await self.active(loop_id=loop_id):
            return False
        async with self.harness.sessions() as session:
            loop = await session.get(AgentLoop, loop_id)
            if loop is None or loop.status != "running":
                return False
            round_row = await session.get(LoopRound, loop.current_round_id)
            if round_row.decision_id is not None and round_row.status != "settled":
                return False  # Committed directives wait in the real RunPool.
        if not initial:
            pending = {row["context_id"] for row in await self.tools.store.pending_wakes() if row["loop_id"] == loop_id}
            contexts = [context for context in contexts if context in pending and context not in self.owner_errors]
            if not contexts:
                return False
        owners = [self.tools.owners[context] for context in contexts]
        if any(item["loop_id"] != loop_id for item in owners):
            raise PermissionError("wake Context does not belong to Loop")
        # Recheck after another Context was consumed while this request waited.
        # Startup may explicitly prepare empty-inbox initial activations.
        self.emit("patrol_started", loop_id=loop_id, context_ids=contexts)
        if not await self.harness.schedule_many(owners):
            self.emit("patrol_deferred", loop_id=loop_id, reason="progress_waiting")
            return False
        self.emit("patrol_committed", loop_id=loop_id, context_ids=contexts)
        return True

    async def _finish_wave(self, round_id, task):
        await task
        selected = [item for item in self.harness.pending if item[1].round_id == round_id]
        self.harness.pending = [item for item in self.harness.pending if item[1].round_id != round_id]
        for item in selected:
            self._finishes.add(asyncio.create_task(self.harness.finish(*item), name="patrol-settle:" + item[2].run_id))

    def _register_waves(self):
        for round_id, (_, _, task) in self.harness.pool._tasks.items():
            if round_id not in self._waves:
                self._waves[round_id] = asyncio.create_task(self._finish_wave(round_id, task))

    async def _drain_owned(self):
        try:
            started = await self.harness.pool.drain()
        finally:
            # Native drain can create a launch before awaiting another Loop.
            # Ownership is registered even if that later database call fails.
            self._register_waves()
        if started:
            self.emit("dispatch_started", waves=started)

    async def dispatch(self):
        if self._drain is None:
            self._drain = asyncio.create_task(self._drain_owned(), name="owned-patrol-drain")
        # Stopping the wake driver cannot abandon a partially-started drain.
        drain = self._drain
        try:
            await asyncio.shield(drain)
        finally:
            if drain.done():
                self._drain = None  # A completed transient failure must be retryable.

    async def reconcile(self):
        for round_id, task in list(self._waves.items()):
            if task.done():
                del self._waves[round_id]
                task.result()
        for task in list(self._finishes):
            if task.done():
                self._finishes.remove(task)
                result = task.result()
                if result and result["status"] != "success":
                    self.owner_errors[result["context_id"]] = result["error"] or result["status"]
                    self.emit("activation_failed", **result)
        for context in self.tools.owners:
            if await self.active(context_id=context):
                continue
            try:
                if self.tools._heartbeat_error is None:
                    for action in await self.tools.store.actions(context):
                        await self.tools.apply(context, action)
                await self.tools.ack_ready(context)
                self.owner_errors.pop(context, None)
            except Exception as error:
                self.owner_errors[context] = str(error)
                self.emit("context_reconcile_failed", context_id=context, error=str(error))

    async def close(self):
        errors = []
        if self._drain is not None:
            errors.extend(item for item in await asyncio.gather(self._drain, return_exceptions=True) if isinstance(item, BaseException))
            self._drain = None
        self._register_waves()
        tasks = list(self._waves.values())
        # Call after the driver stops accepting new work. Gracefully wait for
        # already-launched activations, then reap their exceptions and leases.
        errors.extend(item for item in await asyncio.gather(*tasks, return_exceptions=True) if isinstance(item, BaseException))
        # A wave may just have registered its settlement tasks.
        errors.extend(item for item in await asyncio.gather(*self._finishes, return_exceptions=True) if isinstance(item, BaseException))
        self._waves.clear()
        self._finishes.clear()
        return [str(error) for error in errors]


class PatrolResultService:
    def __init__(self, tools, harness, *, trace=None, poll_seconds=0.1, coalesce_seconds=0.025):
        self.tools, self.harness = tools, harness
        self.port = FocusPatrolPort(harness, tools, trace)
        self.driver = PatrolWakeService(tools.store, self.port, poll_seconds=poll_seconds,
            coalesce_seconds=coalesce_seconds, trace=trace)
        harness.trace = trace
        self._started = False
        self.shutdown_errors = []

    async def start(self, initial_contexts=()):
        if self._started:
            raise RuntimeError("Patrol result service already started")
        await self.tools.restore()  # Paused -> trusted inventory/current hook -> resume.
        self.tools.on_published = self.driver.notify
        groups = {}
        for context in initial_contexts:
            owner = self.tools.owners[context]
            groups.setdefault(owner["loop_id"], []).append(context)
        await asyncio.gather(*(self.port.prepare(loop, contexts, initial=True) for loop, contexts in groups.items()))
        await self.driver.start()  # Always scans durable pending, even without notify.
        self.tools.start_pumps()
        self._started = True

    async def close(self):
        self.tools.on_published = None
        try:
            await self.driver.close()
            self.shutdown_errors = await self.port.close()
        finally:
            await self.tools.stop_pumps()
            self._started = False
        return self.shutdown_errors
