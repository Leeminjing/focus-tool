import copy
import json
from pathlib import Path
import sys
import tempfile
import tarfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "integration/focus"))
from benchmark_patrol import BatchWakeStore, analyze, phase_summary, preserve_workspace
from patrol_benchmark_tools import perform, digest_work, write_json


class PatrolBenchmarkTests(unittest.IsolatedAsyncioTestCase):
    async def test_barrier_waits_only_own_batch_and_never_delays_other_context(self):
        facts = {"A": [{"task": {"id": "check-A"}}], "B": [{"task": {"id": "only-B"}}]}
        async def lookup(context):
            return facts[context]
        store = SimpleNamespace(pending_wakes=AsyncMock(return_value=[{"context_id": "A"}, {"context_id": "B"}]), facts=lookup)
        batches = {"A": {"check-A", "scan-A"}, "B": {"only-B"}}
        barrier, event = BatchWakeStore(store, "barrier", batches), BatchWakeStore(store, "event", batches)
        self.assertEqual(await barrier.pending_wakes(), [{"context_id": "B"}])
        self.assertEqual(len(await event.pending_wakes()), 2)
        facts["A"].append({"task": {"id": "scan-A"}})
        self.assertEqual(len(await barrier.pending_wakes()), 2)

    def test_finite_work_and_canonical_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = b"finite work" * 100
            (root / "corpus.bin").write_bytes(data)
            write_json(root / "workload.json", {"passes": 3})
            write_json(root / "input.json", {"valid": False, "input_id": "A"})
            write_json(root / "scan-input.json", {"valid": False, "input_id": "A"})
            self.assertFalse(perform(root, "check")["valid"])
            scan = perform(root, "scan")
            self.assertEqual(scan["digest"], digest_work(data, 3))
            self.assertFalse((root / "final.json").exists())
            self.assertEqual(perform(root, "repair")["outcome"], "configuration repaired")
            self.assertTrue(perform(root, "valid_check")["valid"])
            perform(root, "scan")
            self.assertEqual(json.loads((root / "final.json").read_text())["outcome"], "configuration repaired")
            write_json(root / "scan-input.json", {"valid": True, "input_id": "A"})
            perform(root, "scan")
            self.assertEqual(json.loads((root / "final.json").read_text())["digest"], scan["digest"])

    def test_phase_calls_keep_parent_separate_and_analysis_rejects_mismatched_inputs(self):
        trace = [{"kind": "patrol_phase", "phase": phase, "loop_id": "A", "started_ns": 1,
                  "finished_ns": 5, "elapsed_ns": elapsed} for phase, elapsed in
                 (("schedule_total", 100000000), ("task_progress", 20000000), ("task_progress", 30000000))]
        phases = phase_summary(trace, loop_id="A")
        self.assertEqual(phases["schedule_total"]["total_ms"], 100)
        self.assertEqual({key: phases["task_progress"][key] for key in ("calls", "total_ms", "median_call_ms")},
                         {"calls": 2, "total_ms": 50, "median_call_ms": 25})
        samples = [{"measured": True, "scenario": scenario, "mode": mode, "passed": True,
            "repetition": 1, "shutdown_errors": [], "input_sha256": {"A": "same"},
            "canonical_artifacts": {"A": "same"}, "metrics": {"total": 100},
            "first_response_phases": phases, "scan_state": "completed", "owners": {"A": "A", "B": "B"}, "activations": [],
            "first_response_loops": {key: 1 for key in ("round_iterations", "progress_drain_calls", "progress_work_items")}}
            for scenario in ("replan", "necessary") for mode in ("barrier", "event")]
        self.assertEqual(analyze(samples, repeat=1)["replan"]["event"]["n"], 1)
        with self.assertRaises(AssertionError):
            analyze(samples, repeat=2)
        bad = copy.deepcopy(samples)
        bad[1]["input_sha256"]["A"] = "different"
        with self.assertRaises(AssertionError):
            analyze(bad, repeat=1)

    async def test_consumption_observer_runs_after_store_transaction_returns(self):
        returned = []
        async def consumed(*args):
            returned.append(time.time_ns())
        store = BatchWakeStore(SimpleNamespace(consume=consumed), "event", {})
        event = {"task": {"owner": {"context_id": "A", "run_id": "r"}}, "sequence": 3}
        await store.consume([event], {}, [], [], 1)
        self.assertGreaterEqual(store.consumptions[0]["committed_ns"], returned[0])
        self.assertEqual(store.consumptions[0]["sequence"], 3)

    def test_boundary_spans_and_stopped_journal_companions_are_retained(self):
        phases = phase_summary([{"kind": "patrol_phase", "phase": "task_progress", "loop_id": "A",
            "started_ns": 0, "finished_ns": 100000000, "elapsed_ns": 100000000}], after=25000000, through=75000000)
        self.assertEqual(phases["task_progress"]["calls"], 1)
        self.assertEqual(phases["task_progress"]["total_ms"], 100)
        self.assertEqual(phases["task_progress"]["window_wall_ms"], 50)
        self.assertEqual(phases["task_progress"]["crosses_start"], 1)
        self.assertEqual(phases["task_progress"]["crosses_end"], 1)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "work"
            root.mkdir()
            (root / "runtime.jsonl").write_text("WAL")
            (root / "runtime.jsonl.sealed").write_text("sealed")
            (root / "runtime.jsonl.store").mkdir()
            (root / "runtime.jsonl.store" / "published.json").write_text("index")
            target = Path(directory) / "failed.tar.gz"
            preserve_workspace(root, target)
            with tarfile.open(target) as archive:
                self.assertTrue({"workspace/runtime.jsonl", "workspace/runtime.jsonl.sealed",
                    "workspace/runtime.jsonl.store/published.json"}.issubset(archive.getnames()))
