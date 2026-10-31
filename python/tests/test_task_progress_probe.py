import asyncio
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "integration/focus"))
from task_progress_probe import TaskProgressProbe, probe_summary, validate_probe_trace


class ProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_owners_nested_spans_and_original_result(self):
        events = []
        probe = TaskProgressProbe(events.append)
        class Runtime:
            async def claim(self, *, loop_id):
                await asyncio.sleep(0)
                return loop_id + "-work", 7
            async def body(self, work_id, fence):
                await asyncio.sleep(0)
            async def drain(self, *, loop_id):
                work, fence = await self.claim(loop_id=loop_id)
                await self.body(work, fence)
                return 1
        runtime = Runtime()
        original = runtime.claim
        probe.wrap(runtime, "claim", "claim", claimed=True)
        probe.wrap(runtime, "body", "body")
        result = await asyncio.gather(*(probe.drain(runtime, loop_id=x, schedule_id=x) for x in ("A", "B")))
        self.assertEqual(result, [1, 1])
        for event in events:
            self.assertEqual(event["work_id"], event["loop_id"] + "-work")
            self.assertEqual(event["fence"], 7)
        totals = probe_summary(events, loop_id="A")
        self.assertEqual(totals["work"]["drain"]["calls"], 1)
        self.assertEqual(totals["work"]["body"]["calls"], 1)
        self.assertGreaterEqual(totals["work"]["drain"]["total_ms"], totals["work"]["body"]["total_ms"])
        probe.close()
        self.assertEqual(runtime.claim, original)

    async def test_cancellation_errors_and_observer_do_not_change_behavior(self):
        entered = asyncio.Event()
        class Runtime:
            async def drain(self, *, loop_id):
                entered.set()
                await asyncio.Event().wait()
        events = []
        probe = TaskProgressProbe(events.append)
        task = asyncio.create_task(probe.drain(Runtime(), loop_id="A", schedule_id="s"))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(events[-1]["error"], "CancelledError")
        class Broken:
            def fail(self):
                raise ValueError("original")
        broken = Broken()
        probe = TaskProgressProbe(lambda event: (_ for _ in ()).throw(RuntimeError("observer")))
        probe.wrap(broken, "fail", "failure")
        class FailRuntime:
            async def drain(self, *, loop_id):
                broken.fail()
        with self.assertRaisesRegex(ValueError, "original"):
            await probe.drain(FailRuntime(), loop_id="A", schedule_id="s")
        probe.close()

    async def test_sql_execution_errors_and_accounting_transition_are_correlated(self):
        events = []
        probe = TaskProgressProbe(events.append)
        class Runtime:
            async def drain(self, *, loop_id):
                with probe.span("accounting_record") as parent:
                    parent["transition"] = "reserved"
                    with probe.span("accounting_view"):
                        context = SimpleNamespace()
                        probe._before_sql(None, None, "SELECT secret FROM test WHERE key = ?", ["private"], context, False)
                        probe._after_sql(None, None, None, None, context, False)
                        probe._before_sql(None, None, "SELECT failing", [], context, False)
                        probe._sql_error(SimpleNamespace(execution_context=context, original_exception=ValueError("private")))
                return 1
        await probe.drain(Runtime(), loop_id="A", schedule_id="s")
        sql = [x for x in events if x["kind"] == "task_progress_sql"]
        self.assertEqual(len(sql), 2)
        self.assertNotIn("private", repr(events))
        view = next(x for x in events if x.get("phase") == "accounting_view")
        self.assertEqual(view["sql_count"], 2)
        self.assertEqual(view["sql_errors"], 1)
        self.assertEqual(view["transition"], "reserved")
        self.assertEqual(sum(x["elapsed_ns"] for x in sql), view["sql_elapsed_ns"])
        validate_probe_trace(events)
        view["sql_count"] += 1
        with self.assertRaises(AssertionError):
            validate_probe_trace(events)

    def test_close_attempts_every_restore_and_retries_only_unfinished(self):
        probe, calls = TaskProgressProbe(None), []
        def failed_once():
            calls.append("retry")
            if calls.count("retry") == 1:
                raise RuntimeError("restore")
        probe._undo = [lambda: calls.append("done"), failed_once]
        with self.assertRaises(ExceptionGroup):
            probe.close()
        self.assertEqual(calls, ["retry", "done"])
        probe.close()
        self.assertEqual(calls, ["retry", "done", "retry"])

    def test_exclusive_time_uses_union_and_categories_do_not_mix(self):
        def span(i, parent, phase, start, end, work=1):
            return {"kind": "task_progress_span", "span_id": i, "parent_span_id": parent,
                "drain_id": "d", "loop_id": "A", "work_id": "w", "fence": 1,
                "phase": phase, "started_ns": start, "finished_ns": end,
                "started_monotonic_ns": start, "finished_monotonic_ns": end,
                "elapsed_ns": end-start, "work_items": work, "error": None,
                "sql_count": 0, "sql_elapsed_ns": 0, "sql_errors": 0}
        events = [span("p", None, "drain", 0, 100000000),
                  span("a", "p", "A", 10000000, 70000000),
                  span("b", "p", "B", 50000000, 90000000)]
        result = probe_summary(events)
        self.assertEqual(result["work"]["drain"]["exclusive_ms"], 20)
        self.assertEqual(result["empty"], {})
        clipped = probe_summary(events, after=20000000, through=80000000)
        self.assertEqual(clipped["work"]["drain"]["total_ms"], 100)
        self.assertEqual(clipped["work"]["drain"]["window_wall_ms"], 60)
        self.assertEqual(clipped["work"]["drain"]["crosses_boundary"], 2)
        # A shielded child can outlive its cancelled parent: subtract overlap only.
        result = probe_summary([span("p", None, "drain", 0, 100000000),
                                span("c", "p", "settlement", 80000000, 200000000)])
        self.assertEqual(result["work"]["drain"]["exclusive_ms"], 80)
