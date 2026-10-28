"""Recompute stage/tail distributions from lossless short-tool traces."""
import argparse
from collections import defaultdict
import gzip
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
              "tracing": trial["tracing"], "throughput_per_s": throughput,
              "e2e_ms": distribution([row["duration_ns"] / 1e6 for row in jobs])}
    if not trial["tracing"]:
        return result
    go = read_trace(directory / trial["go_trace"])
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

    def one(events, phase, state=None):
        found = [event for event in events if event["phase"] == phase and (state is None or event.get("state") == state)]
        assert len(found) == 1, (phase, state, len(found))
        return found[0]

    for row in jobs:
        events = indexed[key(row)]
        accept = one(events, "wal_sync", "queued")
        assigned = one(events, "dispatch_assigned")
        admission = one(events, "admission")
        launch = one(events, "process_start")
        body = one(events, "tool_body")
        reaped = one(events, "process_wait")
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
            "consumer_checkpoint_action_ack": row["end_ns"] - row["consumed_ns"],
            "checkpoint": row["checkpoint_ns"], "effective_action": row["action_ns"],
            "ack_roundtrip": row["end_ns"] - row["ack_start_ns"],
            "tool_end_to_effective_action": row["action_end_ns"] - body["end_ns"],
            "process_start_api": launch["duration_ns"],
            "tool_end_to_process_reaped": reaped["end_ns"] - body["end_ns"],
        }
        for phase in ("worker_bootstrap", "module_import", "result_encode", "completion_handoff"):
            values[phase] = one(events, phase)["duration_ns"]
        # Attribute Run-only ack persistence using its consumer interval.
        ack_events = [event for event in go if owner(event) == owner(row) and event.get("operation") == "ack"
                      and event["start_ns"] >= row["ack_start_ns"] and event["end_ns"] <= row["end_ns"]]
        for phase in ("wal_sync", "wal_encode", "wal_append", "wal_fsync"):
            spans = [event for event in events + ack_events if event["phase"] == phase]
            assert len(spans) == 4, ("missing/duplicate durable mutation diagnostics", phase, len(spans), key(row))
            values[phase + "_per_job"] = sum(event["duration_ns"] for event in spans)
        for phase, value in values.items():
            assert value >= 0, ("clock/order anomaly", trial["id"], key(row), phase, value)
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
        "by_tool_ms": {name: {phase: distribution(values) for phase, values in metrics.items()} for name, metrics in by_tool.items()},
        "job_e2e_by_compaction_overlap_ms": {name: distribution(values) for name, values in overlap_groups.items()},
        "request_rtt_and_queue_ms": {op: {name: distribution(values) for name, values in groups.items()} for op, groups in requests.items()},
        "compaction_ms": distribution([row["duration_ns"] / 1e6 for row in compactions]),
        "lock_hold_ms": {name: distribution(values) for name, values in holds.items()},
        "lock_wait_ms": {name: distribution(values) for name, values in waits.items()},
        "other_context_compaction_in_lock_wait_ms": distribution(cross_waits),
        "client_wall_minus_monotonic_ms": distribution(clocks)})
    return result


def analyze(report_path):
    report = json.loads(report_path.read_text(encoding="utf-8"))
    trials = [analyze_trial(report_path.parent, trial) for trial in report["trials"]]
    controls = [analyze_trial(report_path.parent, trial) for trial in report["tracing_controls"]]
    return {"quantiles": "nearest-rank; milliseconds; per-trial distributions, no claim of statistical significance",
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
