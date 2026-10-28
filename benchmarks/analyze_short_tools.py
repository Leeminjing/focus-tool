"""Recompute stage/tail distributions from lossless short-tool traces."""
import argparse
from collections import defaultdict
import gzip
import hashlib
import json
import math
from pathlib import Path
import statistics


def distribution(values):
    values = sorted(values)
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "mean": None, "max": None}
    return {"count": len(values), **{f"p{p}": values[math.ceil(len(values) * p / 100) - 1] for p in (50, 95, 99)},
            "mean": statistics.mean(values), "max": values[-1]}


def read_trace(path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def owner(event):
    value = event.get("owner", {})
    return value.get("context_id", ""), value.get("run_id", "")


def key(event):
    return *owner(event), event.get("task_id", "")


def overlap_ns(start, end, intervals):
    """Union of intersections; do not double-count overlapping intervals."""
    pieces = sorted((max(start, a), min(end, b)) for a, b in intervals if a < end and b > start)
    total, last = 0, start
    for a, b in pieces:
        a = max(a, last)
        if b > a:
            total += b - a
            last = b
    return total


def analyze_trial(directory, trial):
    client = read_trace(directory / trial["client_trace"])
    jobs = [row for row in client if row["phase"] == "bench_job" and row["measured"]]
    assert len(jobs) == trial["jobs"], "missing measured jobs"
    windows = [row for row in client if row["phase"] == "bench_window"]
    assert len(windows) == 1 and windows[0]["duration_ns"] > 0, "missing measurement window"
    window = windows[0]
    assert window["duration_ns"] == trial["elapsed_ns"], "inconsistent workload duration"
    assert all(window["start_ns"] <= row["start_ns"] <= row["end_ns"] <= window["end_ns"] for row in jobs)
    throughput = len(jobs) * 1e9 / window["duration_ns"]
    assert math.isclose(throughput, trial["throughput_per_s"], rel_tol=1e-12), "inconsistent throughput"
    result = {"id": trial["id"], "contexts": trial["contexts"], "segment_bytes": trial["segment_bytes"],
              "execution_mode": trial.get("execution_mode", "fresh"),
              "compaction_mode": trial.get("compaction_mode", "sync"),
              "tracing": trial["tracing"], "throughput_per_s": throughput,
              "e2e_ms": distribution([row["duration_ns"] / 1e6 for row in jobs])}
    if not trial["tracing"]:
        return result
    go = read_trace(directory / trial["go_trace"])
    calls = [row for row in go if row["phase"] == "python_call"]
    warmup_keys = {key(row) for row in client if row["phase"] == "bench_job" and not row["measured"]}
    cold = [row for row in calls if key(row) in warmup_keys and not row.get("reused", False)]
    result["cold_calls_ms"] = distribution([row["duration_ns"] / 1e6 for row in cold])
    result["worker_start_count"] = sum(row["phase"] == "process_start" for row in go)
    result["worker_pid_count"] = len({row["pid"] for row in calls if row.get("pid")})
    indexed = defaultdict(list)
    for event in go:
        indexed[key(event)].append(event)
    compactions = [row for row in go if row["phase"] == "compaction"]
    # Warmup/close compactions are retained in raw data but not workload stats.
    measured_start, measured_end = window["start_ns"], window["end_ns"]
    compactions = [row for row in compactions if row["start_ns"] >= measured_start and row["end_ns"] <= measured_end]
    assert len(compactions) == trial["compactions"], "missing compaction diagnostics"
    assert all(row.get("state") == "completed" for row in compactions)
    stages = defaultdict(list)
    by_tool = defaultdict(lambda: defaultdict(list))
    overlap_groups = defaultdict(list)
    clocks = []
    job_ids = {key(row) for row in jobs}
    job_sequences = {(*owner(row), row["sequence"]) for row in jobs}
    ack_by_owner, ack_by_sequence = defaultdict(list), defaultdict(list)
    identified_ack_owners = set()
    for event in go:
        if event.get("operation") == "ack" and event["phase"].startswith("wal_"):
            ack_by_owner[owner(event)].append(event)
            if event.get("sequence", 0):
                ack_by_sequence[(*owner(event), event["sequence"])].append(event)
                identified_ack_owners.add(owner(event))

    def one(events, phase, state=None):
        found = [event for event in events if event["phase"] == phase and (state is None or event.get("state") == state)]
        assert len(found) == 1, (phase, state, len(found))
        return found[0]

    for row in jobs:
        events = indexed[key(row)]
        accept = one(events, "wal_sync", "queued")
        assigned = one(events, "dispatch_assigned")
        admission = one(events, "admission")
        launches = [event for event in events if event["phase"] == "process_start"]
        call_events = [event for event in events if event["phase"] == "python_call"]
        assert len(call_events) <= 1, ("duplicate Python call timing", key(row))
        launch = call_events[0] if call_events else one(events, "process_start")
        body = one(events, "tool_body")
        reaped = [event for event in events if event["phase"] == "process_wait"]
        durable = one(events, "wal_sync", "completed")
        values = {
            "submit_to_durable_accept": accept["end_ns"] - row["start_ns"],
            "accepted_to_dispatch": assigned["start_ns"] - accept["end_ns"],
            "dispatch_to_admission": admission["start_ns"] - assigned["start_ns"],
            "admission": admission["duration_ns"],
            "executor_setup": launch["start_ns"] - admission["end_ns"],
            "python_startup_to_function": body["start_ns"] - launch["start_ns"],
            "tool_body": body["duration_ns"],
            "tool_return_to_durable_result": durable["end_ns"] - body["end_ns"],
            "durable_result_to_consume": row["consumed_ns"] - durable["end_ns"],
            "consumer_checkpoint_action_ack": row.get("consumer_ns", row["end_ns"] - row["consumed_ns"]),
            "checkpoint": row["checkpoint_ns"], "effective_action": row["action_ns"],
            "ack_roundtrip": row.get("ack_ns", row["end_ns"] - row["ack_start_ns"]),
            "tool_end_to_effective_action": row["action_end_ns"] - body["end_ns"],
            "process_start_api": sum(event["duration_ns"] for event in launches),
        }
        if reaped:
            values["tool_end_to_process_reaped"] = reaped[0]["end_ns"] - body["end_ns"]
        bootstraps = [event for event in events if event["phase"] == "worker_bootstrap"]
        values["worker_bootstrap"] = sum(event["duration_ns"] for event in bootstraps)
        if call_events:
            values["python_call_total"] = launch["duration_ns"]
            values["python_call_minus_body_signed"] = launch["duration_ns"] - body["duration_ns"]
        if "submit_to_consume_ns" in row:
            values["submit_to_consume"] = row["submit_to_consume_ns"]
            values["runtime_and_bridge_non_body"] = row["submit_to_consume_ns"]-body["duration_ns"]
        for phase in ("module_import", "result_encode", "completion_handoff"):
            values[phase] = one(events, phase)["duration_ns"]
        # Attribute Run-only ack persistence using its consumer interval.
        ack_events = ack_by_sequence[(*owner(row), row["sequence"])] if owner(row) in identified_ack_owners else [
            event for event in ack_by_owner[owner(row)] if event["start_ns"] >= row["ack_start_ns"] and event["end_ns"] <= row["end_ns"]]
        for phase in ("wal_sync", "wal_encode", "wal_append", "wal_fsync"):
            spans = [event for event in events + ack_events if event["phase"] == phase]
            assert len(spans) == 4, ("missing/duplicate durable mutation diagnostics", phase, len(spans), key(row))
            values[phase + "_per_job"] = sum(event["duration_ns"] for event in spans)
        for phase, value in values.items():
            cross_process = {"submit_to_durable_accept", "python_startup_to_function", "tool_return_to_durable_result",
                             "durable_result_to_consume", "tool_end_to_effective_action", "python_call_minus_body_signed"}
            assert value >= 0 or phase in cross_process, ("clock/order anomaly", trial["id"], key(row), phase, value)
            stages[phase].append(value / 1e6)
            if phase in ("python_startup_to_function", "tool_body", "tool_end_to_effective_action"):
                by_tool[row.get("tool", "unspecified")][phase].append(value / 1e6)
        by_tool[row.get("tool", "unspecified")]["e2e"].append(row["duration_ns"] / 1e6)
        clocks.append(((row["end_ns"] - row["start_ns"]) - row["duration_ns"]) / 1e6)
        overlapping = [event for event in compactions if overlap_ns(row["start_ns"], row["end_ns"], [(event["start_ns"], event["end_ns"])])]
        label = "overlaps_compaction" if overlapping else "no_compaction_overlap"
        overlap_groups[label].append(row["duration_ns"] / 1e6)
        if any(owner(event)[0] != owner(row)[0] for event in overlapping):
            overlap_groups["overlaps_other_context_compaction"].append(row["duration_ns"] / 1e6)

    server_requests = {row["request_id"]: row for row in go if row["phase"] == "server_request"}
    writes = {row["request_id"]: row for row in client if row["phase"] == "client_write"}
    requests = defaultdict(lambda: defaultdict(list))
    for event in client:
        if event["phase"] != "client_request" or event.get("operation") not in ("submit", "ack"):
            continue
        if event["operation"] == "submit" and key(event) not in job_ids:
            continue
        if event["operation"] == "ack" and (*owner(event), event.get("sequence")) not in job_sequences:
            continue
        server = server_requests[event["request_id"]]
        write = writes[event["request_id"]]
        others = [(row["start_ns"], row["end_ns"]) for row in compactions if owner(row)[0] != owner(event)[0]]
        overlap = overlap_ns(event["start_ns"], event["end_ns"], [(row["start_ns"], row["end_ns"]) for row in compactions])
        label = "overlaps_compaction" if overlap else "no_compaction_overlap"
        requests[event["operation"]][label].append(event["duration_ns"] / 1e6)
        # Includes pipe/serial main-loop queueing, NOT only Engine mutex wait.
        prehandle = server["start_ns"] - write["end_ns"]
        requests[event["operation"]]["request_start_to_server_handle"].append((server["start_ns"]-event["start_ns"])/1e6)
        requests[event["operation"]]["write_end_to_server_handle_signed"].append(prehandle/1e6)
        if prehandle >= 0:
            requests[event["operation"]]["post_write_wait_nonnegative_only"].append(prehandle / 1e6)
            blocked = overlap_ns(write["end_ns"], server["start_ns"], others)
            if blocked:
                requests[event["operation"]]["other_context_compaction_in_prehandle"].append(blocked / 1e6)
        else:
            # A server can consume the frame before drain() returns. Preserve
            # these fast/overlapping boundaries rather than censoring them.
            requests[event["operation"]]["server_handle_before_drain_end"].append(-prehandle/1e6)

    locks = [row for row in go if row["phase"] == "engine_lock" and row["start_ns"] >= measured_start and row["end_ns"] <= measured_end]
    gates = [row for row in go if row["phase"] == "mutation_gate" and row["start_ns"] >= measured_start and row["end_ns"] <= measured_end]
    gate_waits, cross_gate = defaultdict(list), []
    for event in gates:
        gate_waits[event.get("operation", "")].append(event["duration_ns"]/1e6)
        if owner(event)[0]:
            blocked = overlap_ns(event["start_ns"], event["end_ns"], [(row["start_ns"], row["end_ns"]) for row in compactions if owner(row)[0] != owner(event)[0]])
            if blocked:
                cross_gate.append(blocked/1e6)
    holds, waits, cross_waits = defaultdict(list), defaultdict(list), []
    for event in locks:
        contains = any(event["acquired_ns"] <= row["start_ns"] and row["end_ns"] <= event["end_ns"] for row in compactions)
        holds["contains_compaction" if contains else "no_compaction"].append(event.get("hold_ns", 0) / 1e6)
        waits[event["operation"]].append(event.get("wait_ns", 0) / 1e6)
        if owner(event)[0]:
            others = [(row["start_ns"], row["end_ns"]) for row in compactions if owner(row)[0] != owner(event)[0]]
            blocked = overlap_ns(event["start_ns"], event["acquired_ns"], others)
            if blocked:
                cross_waits.append(blocked / 1e6)
    result.update({"stages_ms": {name: distribution(values) for name, values in stages.items()},
        "signed_cross_clock_quality": {name: {"negative_count": sum(value < 0 for value in stages[name]), "min_ms": min(stages[name])}
            for name in ("submit_to_durable_accept", "python_startup_to_function", "tool_return_to_durable_result", "durable_result_to_consume", "tool_end_to_effective_action", "python_call_minus_body_signed") if name in stages},
        "by_tool_ms": {name: {phase: distribution(values) for phase, values in metrics.items()} for name, metrics in by_tool.items()},
        "job_e2e_by_compaction_overlap_ms": {name: distribution(values) for name, values in overlap_groups.items()},
        "request_rtt_and_queue_ms": {op: {name: distribution(values) for name, values in groups.items()} for op, groups in requests.items()},
        "compaction_ms": distribution([row["duration_ns"] / 1e6 for row in compactions]),
        "compaction_phases_ms": {phase: distribution([row["duration_ns"]/1e6 for row in go if row["phase"] == phase and measured_start <= row["start_ns"] <= row["end_ns"] <= measured_end])
            for phase in ("compaction_seal", "compaction_build", "compaction_publish", "compaction_retire", "compaction_backpressure", "compaction_join")},
        "mutation_gate_wait_ms": {op: distribution(values) for op, values in gate_waits.items()},
        "other_context_compaction_in_gate_wait_ms": distribution(cross_gate),
        "lock_hold_ms": {name: distribution(values) for name, values in holds.items()},
        "lock_wait_ms": {name: distribution(values) for name, values in waits.items()},
        "other_context_compaction_in_lock_wait_ms": distribution(cross_waits),
        "client_wall_minus_monotonic_ms": distribution(clocks)})
    return result


def analyze(report_path):
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report.get("complete", True) is not True:
        raise ValueError("collection is incomplete; retain failure traces but do not publish acceptance statistics")
    trials = [analyze_trial(report_path.parent, trial) for trial in report["trials"]]
    controls = [analyze_trial(report_path.parent, trial) for trial in report["tracing_controls"]]
    return {"quantiles": "nearest-rank; milliseconds; per-trial distributions, no claim of statistical significance",
            "clock_note": "Go short intervals may quantize to zero on this Windows host; call-minus-body and cross-process differences are signed diagnostics. Use client monotonic pipeline metrics for speed comparisons.",
            "analysis_source_sha256_lf_normalized": hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
            "trials": trials, "tracing_controls": controls}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    result = analyze(args.report)
    encoded = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.out:
        args.out.write_text(encoded, encoding="utf-8")
        print(args.out)
    else:
        print(encoded)


if __name__ == "__main__":
    main()
