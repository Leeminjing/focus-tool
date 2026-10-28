import gzip
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "benchmarks"))
from analyze_short_tools import analyze_trial, distribution, overlap_ns


class ShortToolAnalysisTests(unittest.TestCase):
    def test_nearest_rank_percentiles_and_union_overlap(self):
        result = distribution(list(range(1, 21)))
        self.assertEqual((result["p50"], result["p95"], result["p99"]), (10, 19, 20))
        self.assertEqual(overlap_ns(0, 10, [(1, 3), (2, 6), (8, 12)]), 7)
        self.assertIsNone(distribution([])["p99"])

    def test_ack_pipe_wait_for_other_context_is_separate_from_mutex_wait(self):
        who = {"context_id": "B", "run_id": "r"}
        def event(phase, start, end, **fields):
            return {"phase": phase, "start_ns": start, "end_ns": end, "duration_ns": end-start,
                    "owner": who, "task_id": "j", **fields}
        go = [event("wal_sync", 8, 10, state="queued"), event("wal_sync", 11, 12, state="running"),
              event("wal_sync", 55, 60, state="completed"), event("dispatch_assigned", 11, 11),
              event("admission", 12, 14), event("process_start", 15, 16), event("tool_body", 40, 42),
              event("python_call", 15, 49, duration_ns=1),
              event("worker_bootstrap", 20, 30), event("module_import", 31, 39), event("result_encode", 43, 44),
              event("completion_handoff", 50, 51), event("process_wait", 16, 49), event("wal_sync", 94, 95, operation="ack", task_id=""),
              event("compaction", 80, 90, owner={"context_id": "A", "run_id": "a"}, state="completed"),
              event("engine_lock", 78, 91, acquired_ns=78, hold_ns=13, wait_ns=0, operation="dispatch", owner={}),
              event("engine_lock", 93, 96, acquired_ns=93, hold_ns=3, wait_ns=0, operation="ack"),
              event("server_request", 2, 20, operation="submit", request_id=1),
              event("server_request", 93, 99, operation="ack", request_id=2)]
        for row in list(go):
            if row["phase"] == "wal_sync":
                for phase in ("wal_encode", "wal_append", "wal_fsync"):
                    part = {**row, "phase": phase}
                    if phase != "wal_fsync":
                        part.update(end_ns=row["start_ns"], duration_ns=0)
                    go.append(part)
        client = [event("client_request", 0, 21, operation="submit", request_id=1),
                  event("client_write", 0, 3, operation="submit", request_id=1),
                  event("client_request", 74, 100, operation="ack", request_id=2, sequence=1),
                  event("client_write", 74, 75, operation="ack", request_id=2),
                  event("bench_window", 0, 100, jobs=1),
                  event("bench_job", 0, 100, measured=True, sequence=1, consumed_ns=70,
                        checkpoint_start_ns=71, checkpoint_end_ns=72, checkpoint_ns=1,
                        action_start_ns=72, action_end_ns=73, action_ns=1, ack_start_ns=74)]
        with tempfile.TemporaryDirectory() as name:
            directory = Path(name)
            for path, rows in (("go.gz", go), ("client.gz", client)):
                with gzip.open(directory / path, "wt", encoding="utf-8") as stream:
                    for row in rows:
                        stream.write(json.dumps(row) + "\n")
            trial = {"id": "synthetic", "contexts": 4, "segment_bytes": 64,
                "jobs": 1, "tracing": True, "elapsed_ns": 100, "throughput_per_s": 10000000, "compactions": 1,
                "client_trace": "client.gz", "go_trace": "go.gz"}
            report = analyze_trial(directory, trial)
            # Ack identities remain usable when wall endpoints do not nest in
            # the client's interval. Legacy unlabelled traces above still work.
            for row in go:
                if row.get("operation") == "ack" and row["phase"].startswith("wal_"):
                    row.update(sequence=1, start_ns=row["start_ns"]-40, end_ns=row["end_ns"]-40)
            with gzip.open(directory / "go.gz", "wt", encoding="utf-8") as stream:
                for row in go:
                    stream.write(json.dumps(row)+"\n")
            identified = analyze_trial(directory, trial)
            self.assertEqual(identified["stages_ms"]["wal_sync_per_job"]["mean"], 9 / 1e6)
        self.assertEqual(report["request_rtt_and_queue_ms"]["ack"]["other_context_compaction_in_prehandle"]["mean"], 10 / 1e6)
        self.assertEqual(report["other_context_compaction_in_lock_wait_ms"]["count"], 0)
        self.assertEqual(report["stages_ms"]["wal_sync_per_job"]["mean"], 9 / 1e6)
        self.assertEqual(report["stages_ms"]["python_call_minus_body_signed"]["mean"], -1 / 1e6)
        self.assertEqual(report["signed_cross_clock_quality"]["python_call_minus_body_signed"]["negative_count"], 1)
        submit = report["request_rtt_and_queue_ms"]["submit"]
        self.assertEqual(submit["request_start_to_server_handle"]["count"], 1)
        self.assertEqual(submit["server_handle_before_drain_end"]["count"], 1)
        self.assertEqual(submit["write_end_to_server_handle_signed"]["p50"], -1 / 1e6)
