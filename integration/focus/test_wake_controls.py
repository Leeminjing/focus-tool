"""Fault/control-flow tests against the real Focus adapter classes, with stubs.

No database or provider connection. Run with the existing Focus environment.
Real PostgreSQL/worker execution is separately covered by run_automatic.py.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch


class WakeControls(unittest.IsolatedAsyncioTestCase):
    async def test_transient_drain_failure_does_not_poison_future_dispatch(self):
        from patrol_driver import FocusPatrolPort
        drain = AsyncMock(side_effect=[RuntimeError("transient database failure"), 0])
        port = FocusPatrolPort(SimpleNamespace(pool=SimpleNamespace(drain=drain, _tasks={})), None)
        with self.assertRaisesRegex(RuntimeError, "transient"):
            await port.dispatch()
        await port.dispatch()
        self.assertEqual(drain.await_count, 2)
        self.assertIsNone(port._drain)
        self.assertEqual(await port.close(), [])

    async def test_partial_drain_stays_owned_through_caller_cancellation(self):
        from patrol_driver import FocusPatrolPort
        launched, resume_drain = asyncio.Event(), asyncio.Event()
        finished = []
        record = SimpleNamespace(run_id="actual-started")
        directive = SimpleNamespace(round_id="round")
        harness = SimpleNamespace(pending=[])

        async def launch():
            harness.pending.append(({}, directive, record, None, None))
            launched.set()

        class Pool:
            _tasks = {}

            async def drain(self):
                self._tasks["round"] = ("loop", 1, asyncio.create_task(launch()))
                await resume_drain.wait()
                return 1

        async def finish(*item):
            finished.append(item[2].run_id)
            return {"context_id": "context", "status": "success", "error": None}

        harness.pool, harness.finish = Pool(), finish
        def broken_trace(event):
            raise RuntimeError("optional observer failed")
        port = FocusPatrolPort(harness, None, broken_trace)
        dispatch = asyncio.create_task(port.dispatch())
        await asyncio.wait_for(launched.wait(), 1)
        dispatch.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await dispatch
        closing = asyncio.create_task(port.close())
        await asyncio.sleep(0)
        self.assertFalse(closing.done())
        resume_drain.set()
        self.assertEqual(await asyncio.wait_for(closing, 1), [])
        self.assertEqual(finished, ["actual-started"])

    async def test_shutdown_collects_failure_waits_others_and_stops_pumps(self):
        from patrol_driver import FocusPatrolPort, PatrolResultService
        release = asyncio.Event()
        async def failed():
            raise RuntimeError("settlement failed")
        async def other():
            await release.wait()
        port = FocusPatrolPort(SimpleNamespace(pool=SimpleNamespace(_tasks={})), None)
        port._finishes = {asyncio.create_task(failed()), asyncio.create_task(other())}
        service = object.__new__(PatrolResultService)
        service.tools = SimpleNamespace(on_published=None, stop_pumps=AsyncMock())
        service.driver = SimpleNamespace(close=AsyncMock())
        service.port, service._started, service.shutdown_errors = port, True, []
        closing = asyncio.create_task(service.close())
        await asyncio.sleep(0.01)
        self.assertFalse(closing.done(), "one failure abandoned another activation")
        release.set()
        self.assertEqual(await asyncio.wait_for(closing, 1), ["settlement failed"])
        service.tools.stop_pumps.assert_awaited_once()
        self.assertFalse(service._started)

    async def test_failed_worker_is_settled_and_bad_trace_cannot_skip_release(self):
        from patrol_activation import PatrolHarness
        harness = object.__new__(PatrolHarness)
        row = SimpleNamespace()
        session = SimpleNamespace(get=AsyncMock(return_value=row), flush=AsyncMock())
        class Transaction:
            async def __aenter__(self):
                return session
            async def __aexit__(self, *args):
                pass
        harness.sessions = SimpleNamespace(begin=Transaction)
        harness.coordinator = SimpleNamespace(handle_run_settled=AsyncMock())
        harness.activations = []
        harness.trace = lambda event: (_ for _ in ()).throw(RuntimeError("diagnostic failure"))
        record = SimpleNamespace(run_id="failed", task=asyncio.create_task(asyncio.sleep(0)),
            loop_activity_task=asyncio.create_task(asyncio.sleep(0)), status=SimpleNamespace(value="error"),
            error="expected worker failure")
        graph = SimpleNamespace(aget_state=AsyncMock(return_value=SimpleNamespace(config={}, values={})))
        lease = SimpleNamespace(lease_id="lease", fencing_token=1)
        manager = SimpleNamespace(release=AsyncMock())
        with patch("patrol_activation.WorkspaceLeaseManager", return_value=manager):
            result = await harness.finish({"context_id": "ctx", "thread_id": "thread", "logical_run": "logical"},
                SimpleNamespace(loop_id="loop"), record, graph, lease)
        self.assertEqual(result["status"], "error")
        self.assertEqual(row.status, "error")
        self.assertIsNotNone(row.settled_at)
        harness.coordinator.handle_run_settled.assert_awaited_once()
        manager.release.assert_awaited_once()

    def test_native_page_ids_are_owner_scoped(self):
        from patrol_store import typed_inbox_ids
        items = [{"kind": "agent_collaboration", "origin": "collaborator", "source_refs": [
            {"kind": "inbox", "message_id": str(index), "task_id": "A", "recipient": "main:A"}]}
            for index in range(100)]
        items.append({"kind": "agent_collaboration", "origin": "collaborator", "source_refs": [
            {"kind": "inbox", "message_id": "other", "task_id": "B", "recipient": "main:B"}]})
        self.assertEqual(typed_inbox_ids(items, "A"), {str(index) for index in range(100)})
        self.assertNotIn("100", typed_inbox_ids(items, "A"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--focus-root", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    sys.dont_write_bytecode = True
    os.environ["FOCUS_DATABASE_URL"] = "postgresql+asyncpg://postgres@127.0.0.1:1/unused_control_tests"
    sys.path[:0] = [str(root / "python"), str(args.focus_root), str(args.focus_root / "backend/packages/harness")]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(WakeControls))
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps({"passed": result.wasSuccessful(), "tests": result.testsRun,
            "failures": len(result.failures), "errors": len(result.errors),
            "scope": "real adapter classes, stub controls, no database/provider connection"}, indent=2) + "\n", encoding="utf-8")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
