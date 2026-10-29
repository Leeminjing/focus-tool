import asyncio
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from focus_tool import PatrolWakeService, RemoteRejected
from focus_tool.custody import cancel_jobs


async def eventually(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.001)


class DurablePending:
    def __init__(self):
        self.rows = {}

    def publish(self, context, loop):
        row = self.rows.setdefault(context, {"context_id": context, "loop_id": loop, "generation": 0})
        row["generation"] += 1

    async def pending_wakes(self):
        return [dict(row) for row in self.rows.values()]


class PatrolPort:
    def __init__(self, store):
        self.store = store
        self.busy = set()
        self.capacity = False
        self.prepared, self.activated = [], []
        self.queued = {}

    async def prepare(self, loop, contexts):
        if loop in self.busy or loop in self.queued:
            return False
        contexts = [context for context in contexts if context in self.store.rows]
        if not contexts:
            return False
        self.prepared.append((loop, list(contexts)))
        self.queued[loop] = list(contexts)
        return True

    async def dispatch(self):
        if not self.capacity:
            return
        for loop, contexts in list(self.queued.items()):
            self.activated.append((loop, contexts))
            del self.queued[loop]
            for context in contexts:
                self.store.rows.pop(context, None)

    async def reconcile(self):
        pass


class WakeContracts(unittest.IsolatedAsyncioTestCase):
    async def start_driver(self, store, port):
        driver = PatrolWakeService(store, port, poll_seconds=0.005, coalesce_seconds=0.005)
        await driver.start()
        self.addAsyncCleanup(driver.close)
        return driver

    async def test_context_burst_coalesces_per_loop_without_duplicate_activation(self):
        store = DurablePending()
        port = PatrolPort(store)
        for _ in range(12):
            store.publish("A", "shared")
            store.publish("B", "shared")
        driver = await self.start_driver(store, port)
        for _ in range(20):
            driver.notify("shared")
        await eventually(lambda: len(port.prepared) == 1)
        self.assertEqual(set(port.prepared[0][1]), {"A", "B"})
        # The real port's committed decision waits in the RunPool rather than
        # recreating a decision each time pending work is scanned.
        await asyncio.sleep(0.03)
        self.assertEqual(len(port.prepared), 1)
        port.capacity = True
        await eventually(lambda: len(port.activated) == 1)
        await asyncio.sleep(0.02)
        self.assertEqual(len(port.activated), 1)

    async def test_busy_context_and_capacity_wait_preserve_other_context_progress(self):
        store = DurablePending()
        port = PatrolPort(store)
        port.busy.add("loop-B")
        store.publish("B", "loop-B")
        store.publish("A", "loop-A")
        await self.start_driver(store, port)
        await eventually(lambda: bool(port.prepared))
        self.assertEqual(port.prepared, [("loop-A", ["A"])])
        self.assertIn("A", store.rows)
        self.assertIn("B", store.rows)
        port.capacity = True
        await eventually(lambda: len(port.activated) == 1)
        self.assertEqual(port.activated[0][0], "loop-A")
        port.busy.clear()
        await eventually(lambda: len(port.activated) == 2)
        self.assertFalse(store.rows)

    async def test_restart_discovers_durable_pending_with_no_notification(self):
        store = DurablePending()
        port = PatrolPort(store)
        old = await self.start_driver(store, port)
        await old.close()
        store.publish("A", "loop-A")  # Commit succeeded; notify never happened.
        port.capacity = True
        recovered = await self.start_driver(store, port)
        await eventually(lambda: len(port.activated) == 1)
        self.assertIsNone(recovered.last_error)

    async def test_custody_loss_skips_unsubmitted_identity_and_reports_each_failure(self):
        class Run:
            owner = {"context_id": "A", "run_id": "logical"}
            calls = []

            async def status(self, job):
                if job == "pending":
                    raise RemoteRejected("unknown task")
                return {"state": "unknown" if job == "uncertain" else "running"}

            async def cancel(self, job):
                self.calls.append(job)
                if job == "broken":
                    raise RuntimeError("lost reply")

        run = Run()
        result = await cancel_jobs(run, ["pending", "broken", "live", "uncertain"])
        self.assertEqual(run.calls, ["broken", "live"])
        self.assertEqual([item["state"] for item in result], ["not_submitted", "failed", "cancel_requested", "unknown"])
        self.assertEqual(result[1]["error"], "lost reply")
