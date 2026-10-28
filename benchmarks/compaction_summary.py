"""Additional streaming, unconditional request/lock analysis; raw data is unchanged."""
import argparse
from collections import defaultdict
import gzip
import hashlib
import json
from pathlib import Path

from analyze_short_tools import distribution, key, owner


def rows(path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def summarize_trial(directory, trial):
    client = directory / trial["client_trace"]
    jobs, sequences = set(), set()
    window = None
    for row in rows(client):
        if row["phase"] == "bench_job" and row["measured"]:
            jobs.add(key(row))
            sequences.add((*owner(row), row["sequence"]))
        if row["phase"] == "bench_window":
            window = row
    assert window is not None and len(jobs) == trial["jobs"]
    requests = defaultdict(list)
    for row in rows(client):
        if row["phase"] != "client_request":
            continue
        op = row.get("operation")
        if (op == "submit" and key(row) in jobs) or (op == "ack" and (*owner(row), row.get("sequence")) in sequences):
            requests[op].append(row["duration_ns"]/1e6)
    assert all(len(requests[op]) == trial["jobs"] for op in ("submit", "ack"))
    locks, gates = defaultdict(lambda: defaultdict(list)), defaultdict(list)
    automatic = 0
    if trial["go_trace"]:
        for row in rows(directory / trial["go_trace"]):
            if not window["start_ns"] <= row["start_ns"] <= row["end_ns"] <= window["end_ns"]:
                continue
            op = row.get("operation", "")
            if row["phase"] == "engine_lock":
                assert row.get("hold_ns",0)+row.get("wait_ns",0) == row["duration_ns"]
                for part in ("hold", "wait"):
                    locks[op][part].append(row.get(part+"_ns",0)/1e6)
            if row["phase"] == "mutation_gate":
                gates[op].append(row["duration_ns"]/1e6)
            if row["phase"] == "compaction" and op != "compact":
                automatic += 1
    if trial["segment_bytes"] == 8 << 20:
        assert automatic >= 2, "default segment lacks multiple automatic rotations"
    return {"id": trial["id"], "contexts": trial["contexts"], "segment_bytes": trial["segment_bytes"],
        "compaction_mode": trial["compaction_mode"], "automatic_compactions": automatic,
        "request_rtt_ms": {op: distribution(values) for op, values in requests.items()},
        "engine_lock_by_operation_ms": {op: {part: distribution(values) for part, values in parts.items()} for op, parts in locks.items()},
        "engine_lock_all_ms": {part: distribution([value for parts in locks.values() for value in parts[part]]) for part in ("hold", "wait")},
        "mutation_gate_by_operation_ms": {op: distribution(values) for op, values in gates.items()}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    if report.get("complete") is not True:
        raise ValueError("incomplete collection")
    result = {"source_sha256_lf_normalized": hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
        "scope": "unconditional measured submit/ack RTT; actual mutex and mutation-gate spans; no mixing conditional quantiles",
        "trials": [summarize_trial(args.report.parent, trial) for trial in report["trials"]]}
    args.out.write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")
    print(args.out)


if __name__ == "__main__":
    main()
