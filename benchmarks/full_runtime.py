"""Full runtime + adapter + journal comparison and sustained multi-Run load.

python benchmarks/full_runtime.py --out results/local/full-runtime-benchmark.json
"""
import argparse
import asyncio
import hashlib
import json
import math
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
from focus_tool import FocusToolAdapter, RuntimeClient, ToolDefinition


def checkpoint(directory, run, event):
    path = directory / (run.owner["run_id"] + "-checkpoint.json")
    with path.open("w", encoding="utf-8") as stream:
        json.dump(event, stream)
        stream.flush()
        os.fsync(stream.fileno())


def metrics(event, consumed_ns):
    timing = event["timing"]
    return {"queue_ms": (timing["started_ns"] - timing["queued_ns"]) / 1e6,
            "consume_lag_ms": (consumed_ns - timing["finished_ns"]) / 1e6}


def summary(values):
    values = sorted(values)
    return {"median": statistics.median(values), "p95": values[math.ceil(len(values) * .95) - 1],
            "p99": values[math.ceil(len(values) * .99) - 1], "min": values[0], "max": values[-1]}


def fixture(directory):
    shutil.copyfile(ROOT / "examples/demo_tools.py", directory / "demo_tools.py")
    (directory / "config.json").write_text('{"valid":false}')
    line = b"focus-runtime-benchmark\n"
    (directory / "corpus.bin").write_bytes((line * (8 * 1024 * 1024 // len(line) + 1))[:8 * 1024 * 1024])


def adapter_for(client, directory, passes):
    return FocusToolAdapter(client, {
        "start_check": ToolDefinition("python", module="demo_tools", function="read_config"),
        "start_repair": ToolDefinition("python", module="demo_tools", function="repair_config"),
        "start_echo": ToolDefinition("python", module="demo_tools", function="echo"),
        "start_scan": ToolDefinition("process", command=(sys.executable, "-u", str(ROOT / "benchmarks/scan.py"), str(passes))),
    }, workspace=directory, before_execute=lambda task: Path(task["directory"]).resolve() == directory.resolve())


async def trial(binary, mode, passes, replan=True):
    with tempfile.TemporaryDirectory(prefix="focus-full-trial-") as name:
        directory = Path(name)
        fixture(directory)
        if not replan:
            (directory / "config.json").write_text('{"valid":true}')
        expected_digest = hashlib.sha256((directory / "corpus.bin").read_bytes()).hexdigest()
        start = time.perf_counter_ns()
        client = await RuntimeClient.start([str(binary)], journal=directory / "runtime.jsonl", concurrency=2, capacity=32, per_run_capacity=16)
        events = []
        async with client:
            adapter = adapter_for(client, directory, passes)
            run = await client.open_run("A", "trial")
            replies = await adapter.submit_tool_calls(run, {"id": "plan-1", "tool_calls": [
                {"id": "check", "name": "start_check", "args": {}},
                {"id": "scan", "name": "start_scan", "args": {}},
            ]})
            assert all(json.loads(item["content"])["state"] == "accepted" for item in replies)
            check_id, scan_id = adapter.job_id("plan-1", "check"), adapter.job_id("plan-1", "scan")
            initial = []
            while len(initial) < (2 if mode == "barrier" else 1):
                event = await asyncio.wait_for(run.next_event(), 30)
                assert event["result"]["state"] == "completed", event
                initial.append(event)
            check = next((event for event in initial if event["task"]["id"] == check_id), None)
            assert check is not None, "scan beat the supposed decisive check; increase passes"
            assert json.loads(check["result"]["output"]) == {"valid": not replan}
            decision_ns = time.perf_counter_ns()
            check_consumed_ns = time.time_ns()
            for event in initial:
                checkpoint(directory, run, event)
                await run.ack(event)
                events.append(event)
            if mode == "barrier":
                scanned = next(event for event in initial if event["task"]["id"] == scan_id)
                assert json.loads(scanned["result"]["output"]) == {"passes": passes, "digest": expected_digest}
            if replan:
                await run.revise(2)
                await adapter.submit(run, parent_message_id="plan-2", call_id="repair", name="start_repair", args={})
                repaired = await asyncio.wait_for(run.next_event(), 30)
                assert json.loads(repaired["result"]["output"]) == {"valid": True}
                checkpoint(directory, run, repaired)
                await run.ack(repaired)
                events.append(repaired)
            elif mode == "event":
                scanned = await asyncio.wait_for(run.next_event(), 30)
                assert json.loads(scanned["result"]["output"]) == {"passes": passes, "digest": expected_digest}
                checkpoint(directory, run, scanned)
                await run.ack(scanned)
                events.append(scanned)
            while True:
                scan = await run.status(scan_id)
                if scan["state"] in ("completed", "cancelled"):
                    break
                await asyncio.sleep(.002)
            assert mode != "event" or not replan or scan["state"] == "cancelled", scan
            await run.close()
        total_ms = (time.perf_counter_ns() - start) / 1e6
        return {"mode": mode, "total_ms": total_ms, "decision_ms": (decision_ns - start) / 1e6,
                "check_consume_lag_ms": metrics(check, check_consumed_ns)["consume_lag_ms"],
                "scan_state": scan["state"], "scan_pid": scan["result"].get("pid"),
                "final_artifact": (directory / ("repaired.json" if replan else "config.json")).read_text(), "events": events}


async def sustained(binary, epochs, jobs, contexts):
    with tempfile.TemporaryDirectory(prefix="focus-full-load-") as name:
        directory = Path(name)
        fixture(directory)
        start = time.perf_counter()
        client = await RuntimeClient.start([str(binary)], journal=directory / "runtime.jsonl", concurrency=4, capacity=64, per_run_capacity=16)
        samples, epochs_report = [], []
        async with client:
            adapter = adapter_for(client, directory, 1)
            abandoned = await client.open_run("abandoned", "abandoned")
            # Fill only A's result quota and leave it unconsumed throughout load.
            for number in range(16):
                await adapter.submit(abandoned, parent_message_id="idle", call_id=str(number), name="start_echo", args={"value": number})
            while True:
                statuses = [await abandoned.status(adapter.job_id("idle", str(number))) for number in range(16)]
                if all(status["state"] == "completed" for status in statuses):
                    break
                await asyncio.sleep(.01)
            runs = [await client.open_run(f"C{i}", f"R{i}") for i in range(contexts)]

            async def produce(run, epoch):
                async def enqueue(number):
                    await adapter.submit(run, parent_message_id=f"epoch-{epoch}", call_id=str(number),
                                         name="start_echo", args={"value": {"run": run.owner["run_id"], "n": number}})
                for number in range(min(4, jobs)):
                    await enqueue(number)
                for number in range(jobs):
                    event = await asyncio.wait_for(run.next_event(), 30)
                    consumed_ns = time.time_ns()
                    assert event["task"]["owner"] == run.owner
                    assert event["result"]["state"] == "completed", event
                    assert json.loads(event["result"]["output"])["value"]["run"] == run.owner["run_id"]
                    samples.append({"epoch": epoch, "owner": run.owner, **metrics(event, consumed_ns)})
                    checkpoint(directory, run, event)
                    await run.ack(event)
                    if number + 4 < jobs:
                        await enqueue(number + 4)

            for epoch in range(epochs):
                before = len(samples)
                await asyncio.gather(*(produce(run, epoch) for run in runs))
                stats = await client.request("stats", collect=True)
                current = samples[before:]
                epochs_report.append({"epoch": epoch, "elapsed_s": time.perf_counter() - start, "stats": stats,
                    "queue_ms": summary([item["queue_ms"] for item in current]),
                    "consume_lag_ms": summary([item["consume_lag_ms"] for item in current]),
                    "client_registered_calls": len(adapter._submitted),
                    "client_buffered_events": sum(len(box.items) for box in client._mailboxes.values())})
            receipt = await abandoned.close()
            facts = await abandoned.archived_events()
            deadline = time.monotonic() + 15
            while len(facts) < 16 and time.monotonic() < deadline:
                await asyncio.sleep(.01)
                facts = await abandoned.archived_events()
            assert receipt["custodian"] == "runtime_archive" and len(facts) == 16
            after_archive = await client.request("stats", collect=True)
            assert after_archive["unacknowledged"] == 0
            for run in runs:
                await run.close()
        return {"epochs": epochs_report, "samples": samples, "archive_receipt": receipt,
                "archived_fact_count": len(facts), "after_archive": after_archive,
                "total_s": time.perf_counter() - start}


async def run(binary, args):
    # Unmeasured warmups use the same chain and journal configuration.
    for mode in ("barrier", "event"):
        await trial(binary, mode, args.passes, replan=not args.negative_control)
    trials = []
    for repetition in range(args.repeat):
        order = ("barrier", "event") if repetition % 2 == 0 else ("event", "barrier")
        for mode in order:
            result = await trial(binary, mode, args.passes, replan=not args.negative_control)
            result["repetition"] = repetition + 1
            trials.append(result)
            print(f"{mode} #{repetition+1}: total={result['total_ms']:.2f}ms decision={result['decision_ms']:.2f}ms", flush=True)
    return {"trials": trials, "sustained": None if args.negative_control else await sustained(binary, args.epochs, args.jobs, args.contexts)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--passes", type=int, default=512)
    parser.add_argument("--epochs", type=int, default=8)
    parser.add_argument("--jobs", type=int, default=32, help="jobs per Run per epoch")
    parser.add_argument("--contexts", type=int, default=4)
    parser.add_argument("--negative-control", action="store_true", help="valid config; scan stays necessary, no replan or load test")
    parser.add_argument("--out", default="results/local/full-runtime-benchmark.json")
    args = parser.parse_args()
    if min(args.repeat, args.passes, args.epochs, args.jobs, args.contexts) < 1:
        parser.error("all workload counts must be positive")
    sources = sorted([*ROOT.glob("runtime/*.go"), *ROOT.glob("runtime/*.py"),
                      *ROOT.glob("python/focus_tool/*.py"), ROOT / "cmd/runtime/main.go",
                      Path(__file__).resolve(), ROOT / "benchmarks/scan.py"])
    fingerprints = {str(path.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                    for path in sources}
    with tempfile.TemporaryDirectory(prefix="focus-benchmark-build-") as name:
        binary = Path(name) / ("runtime.exe" if os.name == "nt" else "runtime")
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/runtime"], cwd=ROOT, check=True)
        report = asyncio.run(run(binary, args))
    report["config"] = {**vars(args), "platform": platform.platform(), "python": sys.version,
                        "go": subprocess.check_output(["go", "version"], text=True).strip(),
                        "journal": "fsync enabled in all arms", "concurrency": 2, "warmup_per_mode": 1}
    report["config"]["corpus_bytes"] = 8 * 1024 * 1024
    report["source_sha256_lf_normalized"] = fingerprints
    report["summary"] = {mode: summary([trial["total_ms"] for trial in report["trials"] if trial["mode"] == mode])
                         for mode in ("barrier", "event")}
    path = ROOT / args.out
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], indent=2))
    print("Full-runtime benchmark:", path)


if __name__ == "__main__":
    main()
