"""Real Patrol barrier/event comparison. Run alone in the existing Focus env.

Only decision eligibility differs: barrier waits the original batch of ONE
Context; event exposes each useful result. B is independent and always finite.
All execution/authority, PostgreSQL, WAL, checkpoints, receipts and ack stay real.
"""
import argparse
import asyncio
from collections import Counter, defaultdict
from datetime import UTC, datetime
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

from run_patrol import ROOT, FOCUS_COMMIT, command, environment
from patrol_benchmark_tools import digest_work, write_json
from task_progress_probe import TaskProgressProbe, probe_summary, validate_probe_trace


class BatchWakeStore:
    """Experiment-only eligibility port; facts and receipts remain untouched."""
    def __init__(self, store, mode, batches, trace=None):
        self.store, self.mode, self.batches = store, mode, batches
        self.consumptions = []
        self.trace = trace

    def __getattr__(self, name):
        return getattr(self.store, name)

    async def pending_wakes(self):
        rows = await self.store.pending_wakes()
        if self.mode == "event":
            return rows
        allowed = []
        for row in rows:
            completed = {event["task"]["id"] for event in await self.store.facts(row["context_id"])}
            if self.batches[row["context_id"]].issubset(completed):
                allowed.append(row)
        return allowed

    async def consume(self, events, checkpoint, decisions, actions, epoch):
        started = time.time_ns()
        await self.store.consume(events, checkpoint, decisions, actions, epoch)
        committed = time.time_ns()  # Actual successful transaction exit, not receipt.created_at.
        for event in events:
            item = {"owner": event["task"]["owner"], "sequence": event["sequence"], "started_ns": started, "committed_ns": committed}
            self.consumptions.append(item)
            if self.trace:
                try:
                    self.trace({"kind": "semantic_consume_committed", "time_ns": committed, **item})
                except Exception:
                    pass


def phase_summary(trace, *, loop_id=None, after=0, through=None):
    selected = [item for item in trace if item["kind"] == "patrol_phase"
        and (loop_id is None or item["loop_id"] == loop_id)
        and max(item["started_ns"], item["finished_ns"]) >= after
        and (through is None or min(item["started_ns"], item["finished_ns"]) <= through)]
    grouped = defaultdict(list)
    for item in selected:
        grouped[item["phase"]].append(item)
    return {phase: {"calls": len(values), "total_ms": sum(item["elapsed_ns"] for item in values) / 1e6,
        "median_call_ms": statistics.median(item["elapsed_ns"] / 1e6 for item in values),
        "window_wall_ms": sum(max(0, min(item["finished_ns"], through if through is not None else item["finished_ns"])
            - max(item["started_ns"], after)) for item in values) / 1e6,
        "crosses_start": sum(item["started_ns"] < after for item in values),
        "crosses_end": sum(through is not None and item["finished_ns"] > through for item in values),
        "wall_clock_anomalies": sum(item["finished_ns"] < item["started_ns"] for item in values)}
        for phase, values in sorted(grouped.items())}


def preserve_workspace(workspace, path):
    with tarfile.open(path, "w:gz") as archive:
        archive.add(workspace, arcname="workspace")  # Includes WAL, .store and any .sealed, after shutdown.


def source_fingerprints():
    paths = [path for directory in (ROOT / "runtime", ROOT / "cmd/runtime", ROOT / "python/focus_tool", ROOT / "integration/focus")
             for path in directory.rglob("*") if path.suffix in {".py", ".go"}]
    return {path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}


def corpus(label, size):
    block = ("focus-patrol-benchmark-v1:" + label + "\n").encode()
    return (block * (size // len(block) + 1))[:size]


def attempts(root):
    return {path.stem: [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            for path in root.glob("*.attempts")}


async def trial(args, mode, scenario, workload):
    setup_start = time.perf_counter_ns()
    engine, saver, original_store, tools, harness = await environment(args)
    from focus.runtime.checkpointer import dispose_checkpointer
    from focus_tool import ToolDefinition
    from patrol_driver import PatrolResultService
    trace, app, probe = [], None, None
    record = {"mode": mode, "scenario": scenario, "passed": False,
              "measured": args.measured, "repetition": args.repetition, "index": args.index}
    clock_start = time.time_ns()
    measure_start = None
    try:
        if args.task_progress_probe:
            from patrol_activation import LocalProgress
            probe = TaskProgressProbe(trace.append).install(harness.progress, engine, LocalProgress)
            harness.progress_probe = probe
        a, b = await harness.bootstrap("A"), await harness.bootstrap("B")
        check = "check" if scenario == "replan" else "valid_check"
        harness.initial_operations[a["context_id"]] = (check, "scan")
        # A finite benchmark does not wait for tools to start/finish inside a
        # fixture gate; only real submission and native checkpoints precede exit.
        harness.wait_started = False
        tools.adapter.tools["start_local"] = ToolDefinition("python", module="patrol_benchmark_tools",
            function="perform", interpreter=sys.executable, timeout_ms=30000)
        input_hashes = {}
        for owner, label in ((a, "A"), (b, "B")):
            root = Path(owner["job_root"])
            config = {"valid": scenario == "necessary" or label == "B", "input_id": "fixed-" + label}
            write_json(root / "input.json", config)
            write_json(root / "scan-input.json", config)
            write_json(root / "workload.json", {"passes": workload["passes"][label]})
            (root / "corpus.bin").write_bytes(corpus(label, workload["corpus_bytes"]))
            input_hashes[label] = {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
                for name in ("input.json", "scan-input.json", "workload.json", "corpus.bin")}
        batches = {a["context_id"]: {tools.adapter.job_id(a["context_id"] + ":" + op, op) for op in (check, "scan")},
                   b["context_id"]: {tools.adapter.job_id(b["context_id"] + ":independent", "independent")}}
        expected_jobs = {**batches, a["context_id"]: batches[a["context_id"]] | (
            {tools.adapter.job_id(a["context_id"] + ":repair", "repair")} if scenario == "replan" else set())}
        store = BatchWakeStore(original_store, mode, batches, trace.append)
        tools.store, harness.store = store, store
        app = PatrolResultService(tools, harness, trace=trace.append)
        record.update(setup_ms=(time.perf_counter_ns() - setup_start) / 1e6, input_sha256=input_hashes,
                      owners={"A": a["context_id"], "B": b["context_id"]},
                      loops={"A": a["loop_id"], "B": b["loop_id"]},
                      original_batches={key: sorted(value) for key, value in batches.items()})
        measure_start, wall_start = time.perf_counter_ns(), time.time_ns()
        trace.append({"kind": "measurement_start", "time_ns": wall_start, "monotonic_ns": measure_start})
        await app.start([a["context_id"], b["context_id"]])
        async with asyncio.timeout(120):
            while True:
                if app.driver.last_error or app.port.owner_errors:
                    raise RuntimeError({"wake": app.driver.last_error, "owners": app.port.owner_errors})
                if any(item["status"] != "success" for item in harness.activations):
                    raise RuntimeError("a real Focus activation failed")
                roots = {label: Path(owner["job_root"]) for label, owner in (("A", a), ("B", b))}
                if all((root / "final.json").exists() for root in roots.values()):
                    if not await original_store.pending_wakes() and not await app.port.active():
                        inventory = {owner["context_id"]: {event["task"]["id"] for event in await store.facts(owner["context_id"])} for owner in (a, b)}
                        if inventory == expected_jobs and all([not await store.facts(owner["context_id"], unacked=True) for owner in (a, b)]):
                            break
                await asyncio.sleep(0.05)
        trace.append({"kind": "terminal_inventory_ready", "time_ns": time.time_ns()})
        facts = {label: await store.facts(owner["context_id"]) for label, owner in (("A", a), ("B", b))}
        observed = {label: attempts(root) for label, root in roots.items()}
        expected_operations = {check, "scan", "repair"} if scenario == "replan" else {check, "scan"}
        assert not set(observed["A"]) - expected_operations
        assert all(len(calls) == 1 for calls in observed["A"].values())
        assert len(observed["A"].get(check, [])) == 1
        if scenario == "replan":
            assert len(observed["A"].get("repair", [])) == 1
        assert {key: len(value) for key, value in observed["B"].items()} == {"independent": 1}
        assert len(facts["A"]) == (3 if scenario == "replan" else 2) and len(facts["B"]) == 1
        scan = next(event for event in facts["A"] if event["task"]["python"]["args"]["operation"] == "scan")
        checked = next(event for event in facts["A"] if event["task"]["python"]["args"]["operation"] == check)
        assert checked["result"]["state"] == "completed"
        assert scan["result"]["state"] in ({"completed", "cancelled"} if mode == "event" and scenario == "replan" else {"completed"})
        if scan["result"]["state"] == "completed":
            assert len(observed["A"].get("scan", [])) == 1
        assert all(event["result"]["state"] == "completed" for event in facts["B"])
        canonical = {"A": json.loads((roots["A"] / "final.json").read_text(encoding="utf-8")),
                     "A_config": json.loads((roots["A"] / "input.json").read_text(encoding="utf-8")),
                     "B": json.loads((roots["B"] / "final.json").read_text(encoding="utf-8"))}
        expected_a = ({"valid": True, "outcome": "configuration repaired", "input_id": "fixed-A"} if scenario == "replan"
            else {"valid": True, "digest": workload["expected_digest"]["A"], "passes": workload["passes"]["A"], "input_id": "fixed-A"})
        assert canonical["A"] == expected_a
        assert canonical["A_config"] == {"valid": True, "input_id": "fixed-A"}
        assert canonical["B"] == {"valid": True, "digest": workload["expected_digest"]["B"],
            "passes": workload["passes"]["B"], "input_id": "fixed-B"}
        trace.append({"kind": "artifact_readback_complete", "time_ns": time.time_ns(),
            "sha256": hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()})
        receipts = {label: await store.receipts(owner["context_id"]) for label, owner in (("A", a), ("B", b))}
        assert all(row["acked"] and row["receipt"] for rows in receipts.values() for row in rows)
        stats = await tools.client.request("stats")
        assert stats["running"] == 0 and stats["queued"] == 0 and stats["unknown_fences"] == 0
        quiescent, wall_end = time.perf_counter_ns(), time.time_ns()
        trace.append({"kind": "measurement_quiescent", "time_ns": wall_end, "monotonic_ns": quiescent})
        metrics = {"service_start_to_quiescent_ms": (quiescent - measure_start) / 1e6}
        earliest_body = min(call["started_ns"] for owner in observed.values() for rows in owner.values() for call in rows)
        metrics["first_body_to_quiescent_ms"] = (wall_end - earliest_body) / 1e6
        first_commit = next(item["time_ns"] for item in trace if item["kind"] == "patrol_committed"
            and item["loop_id"] == a["loop_id"] and item["time_ns"] >= checked["timing"]["finished_ns"])
        response_phases = phase_summary(trace, loop_id=a["loop_id"], after=checked["timing"]["finished_ns"], through=first_commit)
        if scenario == "replan":
            effective = observed["A"]["repair"][0]["started_ns"]
            definition = "actual repair function entry"
        else:
            effective = next(row["committed_ns"] for row in store.consumptions
                if row["owner"]["context_id"] == a["context_id"] and row["sequence"] == scan["sequence"])
            definition = "necessary scan accepted in durable semantic receipt"
        metrics["check_finished_to_effective_ms"] = (effective - checked["timing"]["finished_ns"]) / 1e6
        record.update(passed=True, metrics=metrics, canonical_artifacts=canonical,
            effective_followup={"time_ns": effective, "definition": definition}, attempts=observed,
            facts=facts, receipts=receipts, activations=harness.activations,
            first_response_phases=response_phases, all_phases=phase_summary(trace),
            first_response_window={"loop_id": a["loop_id"], "after_ns": checked["timing"]["finished_ns"], "through_ns": first_commit},
            first_response_loops={key: sum(item[key] for item in trace if item["kind"] == "patrol_loops"
                and item["loop_id"] == a["loop_id"] and checked["timing"]["finished_ns"] <= item["time_ns"] <= first_commit)
                for key in ("round_iterations", "progress_drain_calls", "progress_work_items")},
            loop_counts=[item for item in trace if item["kind"] == "patrol_loops"], runtime_stats=stats,
            scan_state=scan["result"]["state"], publications=tools.published, actions=tools.action_trace)
        record["consumptions"] = store.consumptions
    except BaseException as error:
        record["error"] = repr(error)
        raise
    finally:
        shutdown_start = time.perf_counter_ns()
        cleanup_errors = []
        tools_closed = False
        closers = [("patrol", app.close)] if app else []
        closers.extend([("harness", harness.close), ("tools", tools.close)])
        for name, close in closers:
            try:
                closed_stats = await close()
                if name == "tools":
                    tools_closed = bool(closed_stats and closed_stats.get("running") == 0 and closed_stats.get("unknown_fences") == 0)
            except Exception as error:
                cleanup_errors.append(name + ": " + repr(error))
                if name == "tools":
                    try:
                        await tools.client.close()  # Runtime cleanup must not depend on SQL diagnostics.
                    except Exception as runtime_error:
                        cleanup_errors.append("runtime fallback: " + repr(runtime_error))
        if not record["passed"] or cleanup_errors or (app and app.shutdown_errors):
            evidence = {"activations": harness.activations, "publications": tools.published,
                        "actions": tools.action_trace, "owners": {}, "snapshot_errors": []}
            for context, owner in tools.owners.items():
                item = {"owner": owner}
                try:
                    item["attempts"] = attempts(Path(owner["job_root"]))
                except Exception as error:
                    evidence["snapshot_errors"].append(context + " attempts: " + repr(error))
                try:
                    item["facts"] = await original_store.facts(context)
                    item["receipts"] = await original_store.receipts(context)
                    saved = await saver.aget_tuple({"configurable": {"thread_id": owner["thread_id"]}})
                    if saved:
                        values = saved.checkpoint["channel_values"]
                        item["latest_typed_checkpoint"] = {"config": saved.config,
                            "values": {key: values.get(key) for key in ("execution_items", "facts", "decisions", "actions")}}
                except Exception as error:
                    evidence["snapshot_errors"].append(context + ": " + repr(error))
                evidence["owners"][context] = item
            record["failure_evidence"] = evidence
            archive = args.sample.with_suffix(".workspace.tar.gz")
            try:
                preserve_workspace(args.work, archive)
                record["failed_workspace"] = archive.name
            except Exception as error:
                cleanup_errors.append("workspace archive: " + repr(error))
        if probe:
            try:
                probe.close()
                harness.progress_probe = None
            except Exception as error:
                cleanup_errors.append("progress probe: " + repr(error))
        for name, close in (("checkpointer", lambda: dispose_checkpointer(saver)), ("database", engine.dispose)):
            try:
                await close()
            except Exception as error:
                cleanup_errors.append(name + ": " + repr(error))
        if cleanup_errors and "failed_workspace" not in record:
            archive = args.sample.with_suffix(".workspace.tar.gz")
            try:
                preserve_workspace(args.work, archive)
                record["failed_workspace"] = archive.name
            except Exception as error:
                cleanup_errors.append("workspace archive: " + repr(error))
        if "failed_workspace" in record:
            record["backup_state"] = {"journal_writer_stopped": tools.client.process.returncode is not None,
                "tool_cleanup_confirmed": tools_closed,
                "consistency": "stopped_after_confirmed_cleanup" if tools_closed else "best_effort_unconfirmed_cleanup"}
        record["shutdown_errors"] = [*(app.shutdown_errors if app else []), *cleanup_errors]
        record["passed"] = record["passed"] and not record["shutdown_errors"]
        record["shutdown_ms"] = (time.perf_counter_ns() - shutdown_start) / 1e6
        record["trial_wall_start_ns"] = clock_start
        if measure_start is not None:
            closed = time.perf_counter_ns()
            record.setdefault("metrics", {})["service_start_through_shutdown_ms"] = (closed - measure_start) / 1e6
            trace.append({"kind": "shutdown_finished", "time_ns": time.time_ns(), "monotonic_ns": closed})
        if probe:
            record["task_progress_all"] = probe_summary(trace)
            if "first_response_window" in record:
                window = record["first_response_window"]
                record["task_progress_first_response"] = probe_summary(trace, loop_id=window["loop_id"],
                    after=window["after_ns"], through=window["through_ns"])
        args.sample.parent.mkdir(parents=True, exist_ok=True)
        try:
            with gzip.open(args.sample.with_suffix(".trace.jsonl.gz"), "wt", encoding="utf-8") as output:
                for event in trace:
                    output.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception as error:
            record["passed"] = False
            record.setdefault("export_errors", []).append("trace: " + repr(error))
        try:
            args.sample.write_text(json.dumps(record, indent=2, ensure_ascii=False, default=repr) + "\n", encoding="utf-8")
        except Exception as error:
            print("Sample export failed: " + repr(error) + "; original trial error: " + record.get("error", "none"), file=sys.stderr)
            raise
    assert not record["shutdown_errors"], record["shutdown_errors"]
    return record


def analyze(samples, repeat=None):
    measured = [sample for sample in samples if sample["measured"]]
    result = {}
    for scenario in ("replan", "necessary"):
        result[scenario] = {}
        expected_input, expected_output = None, None
        for mode in ("barrier", "event"):
            rows = [sample for sample in measured if sample["scenario"] == scenario and sample["mode"] == mode]
            assert rows and all(row["passed"] and not row["shutdown_errors"] for row in rows)
            if repeat is not None:
                assert len(rows) == repeat and sorted(row["repetition"] for row in rows) == list(range(1, repeat + 1)), "incomplete or duplicate measured cohort"
            for row in rows:
                if expected_input is None:
                    expected_input, expected_output = row["input_sha256"], row["canonical_artifacts"]
                assert row["input_sha256"] == expected_input and row["canonical_artifacts"] == expected_output
            stages = {phase for row in rows for phase in row["first_response_phases"]}
            result[scenario][mode] = {"n": len(rows),
                "medians_ms": {key: statistics.median(row["metrics"][key] for row in rows) for key in rows[0]["metrics"]},
                "first_response_phase_medians": {phase: {
                    "total_ms": statistics.median(row["first_response_phases"].get(phase, {}).get("total_ms", 0) for row in rows),
                    "window_wall_ms": statistics.median(row["first_response_phases"].get(phase, {}).get("window_wall_ms", 0) for row in rows),
                    "crosses_boundary": sum(row["first_response_phases"].get(phase, {}).get("crosses_start", 0)
                        + row["first_response_phases"].get(phase, {}).get("crosses_end", 0) for row in rows),
                    "calls": statistics.median(row["first_response_phases"].get(phase, {}).get("calls", 0) for row in rows)} for phase in sorted(stages)},
                "first_response_loop_medians": {key: statistics.median(row["first_response_loops"][key] for row in rows)
                    for key in ("round_iterations", "progress_drain_calls", "progress_work_items")},
                "activation_count_medians": {label: statistics.median(sum(item["context_id"] == row["owners"][label]
                    for item in row["activations"]) for row in rows) for label in ("A", "B")},
                "scan_states": dict(Counter(row["scan_state"] for row in rows))}
            if any("task_progress_first_response" in row for row in rows):
                assert all("task_progress_first_response" in row for row in rows)
                details = {}
                for category in ("work", "empty", "failed"):
                    phases = {phase for row in rows for phase in row["task_progress_first_response"][category]}
                    details[category] = {phase: {**{key: statistics.median(
                        row["task_progress_first_response"][category].get(phase, {}).get(key, 0) for row in rows)
                        for key in ("calls", "total_ms", "exclusive_ms", "window_wall_ms", "sql_count", "sql_ms")},
                        "sql_errors_total": sum(row["task_progress_first_response"][category].get(phase, {}).get("sql_errors", 0) for row in rows)}
                        for phase in sorted(phases)}
                result[scenario][mode]["task_progress_phase_medians"] = details
    return result


def load_collection(directory):
    config = json.loads((directory / "config.json").read_text(encoding="utf-8"))
    paths = sorted(path for path in directory.glob("*.json") if re.fullmatch(r"\d+-(sample|warmup)-(replan|necessary)-(barrier|event)\.json", path.name))
    assert len(paths) == 4 * (config["repeat"] + config["warmup_per_arm"]), "incomplete collection"
    rows = []
    for path in paths:
        row = json.loads(path.read_text(encoding="utf-8"))
        assert row["passed"] and not row["shutdown_errors"], (path.name, "failed trial")
        with gzip.open(path.with_suffix(".trace.jsonl.gz"), "rt", encoding="utf-8") as source:
            trace = [json.loads(line) for line in source]
        markers = {item["kind"]: item for item in trace if item["kind"] in {"measurement_start", "measurement_quiescent", "shutdown_finished"}}
        start = markers["measurement_start"]["monotonic_ns"]
        for name, kind in (("service_start_to_quiescent_ms", "measurement_quiescent"), ("service_start_through_shutdown_ms", "shutdown_finished")):
            actual = (markers[kind]["monotonic_ns"] - start) / 1e6
            assert math.isclose(actual, row["metrics"][name], abs_tol=1e-6), (path.name, name)
        earliest = min(call["started_ns"] for owner in row["attempts"].values() for calls in owner.values() for call in calls)
        assert math.isclose((markers["measurement_quiescent"]["time_ns"] - earliest) / 1e6,
            row["metrics"]["first_body_to_quiescent_ms"], abs_tol=1e-6)
        checked = next(event for event in row["facts"]["A"] if event["task"]["python"]["args"]["operation"] in {"check", "valid_check"})
        if row["scenario"] == "replan":
            effective = row["attempts"]["A"]["repair"][0]["started_ns"]
        else:
            scan = next(event for event in row["facts"]["A"] if event["task"]["python"]["args"]["operation"] == "scan")
            effective = next(item["committed_ns"] for item in trace if item["kind"] == "semantic_consume_committed"
                and item["owner"]["context_id"] == row["owners"]["A"] and item["sequence"] == scan["sequence"])
        assert effective == row["effective_followup"]["time_ns"]
        assert math.isclose((effective - checked["timing"]["finished_ns"]) / 1e6,
            row["metrics"]["check_finished_to_effective_ms"], abs_tol=1e-6)
        readback = next(item for item in trace if item["kind"] == "artifact_readback_complete")
        assert readback["time_ns"] <= markers["measurement_quiescent"]["time_ns"]
        assert readback["sha256"] == hashlib.sha256(json.dumps(row["canonical_artifacts"], sort_keys=True).encode()).hexdigest()
        window = row["first_response_window"]
        phases = phase_summary(trace, loop_id=window["loop_id"], after=window["after_ns"], through=window["through_ns"])
        assert set(phases) == set(row["first_response_phases"])
        for phase, values in phases.items():
            expected = row["first_response_phases"][phase]
            assert values["calls"] == expected["calls"]
            assert all(math.isclose(values[key], expected[key], abs_tol=1e-6) for key in ("total_ms", "median_call_ms", "window_wall_ms"))
            assert all(values[key] == expected[key] for key in ("crosses_start", "crosses_end", "wall_clock_anomalies"))
        loops = {key: sum(item[key] for item in trace if item["kind"] == "patrol_loops"
            and item["loop_id"] == window["loop_id"] and window["after_ns"] <= item["time_ns"] <= window["through_ns"])
            for key in ("round_iterations", "progress_drain_calls", "progress_work_items")}
        assert loops == row["first_response_loops"]
        if config.get("task_progress_probe"):
            validate_probe_trace(trace)
            assert row["task_progress_first_response"] == probe_summary(trace,
                loop_id=window["loop_id"], after=window["after_ns"], through=window["through_ns"])
            assert row["task_progress_all"] == probe_summary(trace)
            assert row["task_progress_all"]["work"]["accounting_view"]["calls"] > 0
            assert row["task_progress_all"]["work"]["transaction_commit"]["calls"] > 0
        rows.append(row)
    for scenario in ("replan", "necessary"):
        for mode in ("barrier", "event"):
            warmups = [row for row in rows if not row["measured"] and row["scenario"] == scenario and row["mode"] == mode]
            assert sorted(row["repetition"] for row in warmups) == list(range(1, config["warmup_per_arm"] + 1)), "incomplete warmups"
    return config, rows, analyze(rows, repeat=config["repeat"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--focus-root", type=Path)
    parser.add_argument("--analyze", type=Path, help="rederive immutable collection from raw markers/spans; standard library only")
    parser.add_argument("--out", type=Path, default=ROOT / "results/local/patrol-performance")
    parser.add_argument("--repeat", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=1, help="unmeasured trials per scenario/mode, retained")
    parser.add_argument("--scan-seconds", type=float, default=8.0, help="one-time local calibration target, not a tool timer")
    parser.add_argument("--passes", type=int, help="explicit fixed SHA-256 passes, skip duration calibration")
    parser.add_argument("--task-progress-probe", action="store_true", help="observe native TaskProgress methods, SQL and DBAPI commits")
    args = parser.parse_args()
    if args.analyze:
        _, _, summary = load_collection(args.analyze)
        print(json.dumps(summary, indent=2))
        return
    if not args.focus_root:
        parser.error("--focus-root is required for collection")
    if args.repeat < 1 or args.warmup < 0 or args.scan_seconds <= 0 or (args.passes is not None and args.passes < 1):
        parser.error("invalid experiment sizes")
    args.focus_root = args.focus_root.resolve()
    args.out = args.out.resolve()
    if command("git", "-C", str(args.focus_root), "rev-parse", "HEAD") != FOCUS_COMMIT:
        raise RuntimeError("pinned Focus checkout required: " + FOCUS_COMMIT)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    args.out.mkdir(parents=True, exist_ok=False)  # Raw collections are immutable.
    fingerprints = source_fingerprints()
    data = corpus("A", 8 << 20)
    start = time.perf_counter_ns()
    digest_work(data, 32)
    calibration_seconds = (time.perf_counter_ns() - start) / 1e9
    passes = args.passes or max(1, round(32 * args.scan_seconds / calibration_seconds))
    workload = {"corpus_bytes": len(data), "passes": {"A": passes, "B": max(1, passes // 4)},
        "calibration": {"probe_passes": 32, "probe_seconds": calibration_seconds, "target_seconds": args.scan_seconds,
                        "explicit_passes": args.passes},
        "expected_digest": {label: digest_work(corpus(label, len(data)), count) for label, count in (("A", passes), ("B", max(1, passes // 4)))}}
    config = {"focus_commit": FOCUS_COMMIT, "base_commit": command("git", "rev-parse", "HEAD"),
        "source_sha256": fingerprints, "workload": workload, "repeat": args.repeat, "warmup_per_arm": args.warmup,
        "platform": platform.platform(), "python": sys.version, "go": command("go", "version"),
        "tool_concurrency": 4, "run_concurrency": 2, "journal": "per-mutation fsync; default background 8 MiB",
        "task_progress_probe": args.task_progress_probe,
        "decision_rule": "barrier waits only same-Context original batch; B is independent and finite",
        "started_at_utc": datetime.now(UTC).isoformat()}
    (args.out / "config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    container, started = "focus-tool-perf-" + uuid.uuid4().hex[:12], False
    samples = []
    try:
        command("docker", "run", "-d", "--rm", "--pull", "never", "--name", container,
            "-p", "127.0.0.1::5432", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "postgres:17-alpine")
        started = True
        port = command("docker", "port", container, "5432/tcp").rsplit(":", 1)[1]
        for _ in range(60):
            if subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"], capture_output=True).returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("isolated PostgreSQL not ready")
        with tempfile.TemporaryDirectory(prefix="focus-tool-patrol-performance-") as directory:
            base = Path(directory)
            args.binary = base / ("runtime.exe" if os.name == "nt" else "runtime")
            subprocess.run(["go", "build", "-o", str(args.binary), "./cmd/runtime"], cwd=ROOT, check=True)
            order = []
            for warmup in range(args.warmup):
                order.extend((False, warmup + 1, scenario, mode) for scenario in ("replan", "necessary") for mode in ("barrier", "event"))
            for repetition in range(1, args.repeat + 1):
                scenarios = ("replan", "necessary") if repetition % 2 else ("necessary", "replan")
                modes = ("barrier", "event") if repetition % 2 else ("event", "barrier")
                order.extend((True, repetition, scenario, mode) for scenario in scenarios for mode in modes)
            for index, (measured, repetition, scenario, mode) in enumerate(order):
                database = "trial_" + str(index)
                command("docker", "exec", container, "createdb", "-U", "postgres", database)
                os.environ["FOCUS_TOOL_TEST_DSN"] = "postgresql://postgres@127.0.0.1:" + port + "/" + database
                args.work = base / database
                args.work.mkdir()
                args.sample = args.out / (f"{index:03d}-{'sample' if measured else 'warmup'}-{scenario}-{mode}.json")
                args.measured, args.repetition, args.index = measured, repetition, index
                record = asyncio.run(trial(args, mode, scenario, workload))
                samples.append(record)
                print(f"{index+1}/{len(order)} {scenario} {mode} {'sample' if measured else 'warmup'} {repetition}: "
                    f"total={record['metrics']['service_start_to_quiescent_ms']:.2f}ms "
                    f"response={record['metrics']['check_finished_to_effective_ms']:.2f}ms scan={record['scan_state']}", flush=True)
                command("docker", "exec", container, "dropdb", "-U", "postgres", database)
        assert source_fingerprints() == fingerprints, "sources changed during collection"
        _, _, recomputed = load_collection(args.out)
        summary = {"passed": True, "config": config, "sample_count": len(samples), "summary": recomputed,
                   "samples": [path.name for path in sorted(args.out.glob("*-*.json"))], "finished_at_utc": datetime.now(UTC).isoformat()}
        (args.out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(summary["summary"], indent=2), flush=True)
    except BaseException as error:
        logs = subprocess.run(["docker", "logs", "--tail", "150", container], capture_output=True, text=True,
                              encoding="utf-8", errors="replace") if started else None
        (args.out / "failure.json").write_text(json.dumps({"error": repr(error), "completed_trials": len(samples),
            "current_sample": str(args.sample.name) if hasattr(args, "sample") else None,
            "sources_unchanged": source_fingerprints() == fingerprints,
            "postgres_log": (logs.stdout + logs.stderr) if logs else None}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        raise
    finally:
        if started:
            subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
