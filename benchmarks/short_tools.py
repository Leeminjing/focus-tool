"""Isolated short-tool latency experiment. Standard libraries only.

python benchmarks/short_tools.py --out results/local/short-tools/report.json
"""
import argparse
import asyncio
import gzip
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
from focus_tool import FocusToolAdapter, RuntimeClient, ToolDefinition

NAMES = ("read_file", "search_text", "parse_config")


def write_durable(path, value):
    with path.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())


def fingerprint():
    paths = [*ROOT.glob("runtime/*.go"), *ROOT.glob("runtime/*.py"),
             *ROOT.glob("cmd/runtime/*.go"), *ROOT.glob("python/focus_tool/*.py"),
             *ROOT.glob("benchmarks/*short_tools*.py"), ROOT / "benchmarks/compaction.py"]
    return {path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            for path in sorted(paths) if not path.name.endswith("_test.go")}


def archive_trace(source, destination):
    with source.open("rb") as reader, gzip.open(destination, "wb") as writer:
        shutil.copyfileobj(reader, writer)


async def trial(binary, output, identity, *, contexts, jobs, segment_bytes, tracing, warmup, mode="fresh",
                compaction_mode="background", drain_compaction=True, sample_every=0):
    with tempfile.TemporaryDirectory(prefix="focus-short-tools-") as name:
        directory = Path(name)
        shutil.copyfile(ROOT / "benchmarks/short_tools_workload.py", directory / "short_tools_fixture.py")
        corpus = b"Patrol local tool runtime\n" * 256
        (directory / "corpus.txt").write_bytes(corpus)
        (directory / "config.json").write_text(json.dumps({"items": [{"x": n} for n in range(256)]}), encoding="utf-8")
        expected = {"read": len(corpus), "search": 256, "parse": sum(range(256))}
        go_trace, py_trace = directory / "go.jsonl", directory / "client.jsonl"
        stream = py_trace.open("w", encoding="utf-8", buffering=65536)

        def emit(event):
            stream.write(json.dumps(event, separators=(",", ":"), ensure_ascii=False) + "\n")

        lifecycle_start = time.perf_counter_ns()
        client = await RuntimeClient.start([str(binary)], journal=directory / "runtime.jsonl",
            concurrency=4, capacity=64, per_run_capacity=16, max_active_runs=8,
            segment_bytes=segment_bytes, trace=emit if tracing else None,
            trace_path=go_trace if tracing else None, python_workers=4 if mode == "pool" else 0,
            compaction_mode=compaction_mode)
        adapter = FocusToolAdapter(client, {name: ToolDefinition("python", module="short_tools_fixture", function=name, reuse=True)
            for name in NAMES}, workspace=directory,
            before_execute=lambda task: Path(task["directory"]).resolve() == directory.resolve())
        runs = []
        records = []
        artifacts = []
        samples = []
        per_context = jobs // contexts
        # Keep the same aggregate in-flight bound in both Context regimes.
        depth = 4 // contexts

        async def produce(run, count, prefix, measured):
            totals = {"read": 0, "search": 0, "parse": 0}
            starts = {}

            async def enqueue(index):
                wall, mono = time.time_ns(), time.perf_counter_ns()
                job_id = adapter.job_id(prefix, str(index))
                starts[job_id] = (wall, mono, index)
                reply = await adapter.submit(run, parent_message_id=prefix, call_id=str(index), name=NAMES[index % 3], args={})
                assert json.loads(reply["content"])["state"] == "accepted", reply

            # Consumer runs while submission receipts can still be pending.
            for index in range(min(depth, count)):
                await enqueue(index)
            for number in range(count):
                event = await asyncio.wait_for(run.next_event(), 60)
                consumed, consumed_mono = time.time_ns(), time.perf_counter_ns()
                assert event["result"]["state"] == "completed", event
                wall, mono, index = starts.pop(event["task"]["id"])
                value = json.loads(event["result"]["output"])
                assert value["value"] == expected[value["kind"]], value
                cp_wall, cp_mono = time.time_ns(), time.perf_counter_ns()
                await asyncio.to_thread(write_durable, directory / (run.owner["context_id"] + "-checkpoint.json"), event)
                checkpoint_end = time.time_ns()
                checkpoint_ns = time.perf_counter_ns() - cp_mono
                action_wall, action_mono = time.time_ns(), time.perf_counter_ns()
                # A real, deterministic following action: durable aggregate
                # changes only after its input checkpoint has been saved.
                totals[value["kind"]] += value["value"]
                await asyncio.to_thread(write_durable, directory / (run.owner["context_id"] + "-action.json"), totals)
                action_end = time.time_ns()
                action_ns = time.perf_counter_ns() - action_mono
                ack_start, ack_mono = time.time_ns(), time.perf_counter_ns()
                await run.ack(event)
                end_wall, end_mono = time.time_ns(), time.perf_counter_ns()
                record = {"phase": "bench_job", "clock": "python_client", "measured": measured,
                    "owner": run.owner, "task_id": event["task"]["id"], "sequence": event["sequence"],
                    "tool": NAMES[index % 3], "start_ns": wall, "end_ns": end_wall, "duration_ns": end_mono - mono,
                    "consumed_ns": consumed, "checkpoint_start_ns": cp_wall, "checkpoint_end_ns": checkpoint_end,
                    "checkpoint_ns": checkpoint_ns, "action_start_ns": action_wall, "action_end_ns": action_end,
                    "action_ns": action_ns, "ack_start_ns": ack_start, "timing": event["timing"]}
                record.update(submit_to_consume_ns=consumed_mono-mono, consumer_ns=end_mono-consumed_mono, ack_ns=end_mono-ack_mono)
                records.append(record)
                emit(record)
                if measured and sample_every and (number+1) % sample_every == 0:
                    sample = await client.request("stats")
                    samples.append(sample)
                    emit({"phase": "bench_stats", "clock": "python_client", "at_ns": time.time_ns(), "stats": sample})
                if number + depth < count:
                    await enqueue(number + depth)
            return totals

        try:
            runs = [await client.open_run(f"C{n}", "run") for n in range(contexts)]
            cold_start = time.perf_counter_ns()
            await asyncio.gather(*(produce(run, warmup // contexts, "warmup", False) for run in runs))
            warmup_ns = time.perf_counter_ns() - cold_start
            if drain_compaction:
                await client.request("compact")
            before = await client.request("stats")
            start_wall, start = time.time_ns(), time.perf_counter_ns()
            artifacts = await asyncio.gather(*(produce(run, per_context, "measured", True) for run in runs))
            drain_start = time.perf_counter_ns()
            if drain_compaction:
                await client.request("compact")
            drain_ns = time.perf_counter_ns()-drain_start if drain_compaction else 0
            end_wall, end = time.time_ns(), time.perf_counter_ns()
            elapsed = end - start
            emit({"phase": "bench_window", "clock": "python_client", "start_ns": start_wall,
                  "end_ns": end_wall, "duration_ns": elapsed, "jobs": jobs})
            after = await client.request("stats", collect=True)
            persisted = [json.loads((directory / (run.owner["context_id"] + "-action.json")).read_text(encoding="utf-8")) for run in runs]
            assert persisted == artifacts, (persisted, artifacts)
            artifacts = persisted
            for run in runs:
                await run.close()
        finally:
            shutdown = await client.close()
            stream.close()
        lifecycle_ns = time.perf_counter_ns() - lifecycle_start
        assert client.process.returncode == 0, client.stderr.decode(errors="replace")
        final = {kind: sum(artifact[kind] for artifact in artifacts) for kind in expected}
        target = {kind: expected[kind] * jobs // 3 for kind in expected}
        assert final == target, (final, target)
        assert after["unacknowledged"] == after["retained_tasks"] == 0, after
        if mode == "pool":
            assert after["python_pool"]["peak_live"] <= 4
            assert shutdown["python_pool"]["live"] == shutdown["python_pool"]["reserved"] == 0, shutdown
        destination = output.parent
        client_name = identity + ".client.jsonl.gz"
        archive_trace(py_trace, destination / client_name)
        go_name = None
        if tracing:
            go_name = identity + ".go.jsonl.gz"
            archive_trace(go_trace, destination / go_name)
        return {"id": identity, "contexts": contexts, "jobs": jobs, "segment_bytes": segment_bytes, "execution_mode": mode,
                "compaction_mode": compaction_mode, "drain_compaction": drain_compaction,
                "post_workload_drain_ns": drain_ns, "stats_samples": samples,
                "tracing": tracing, "elapsed_ns": elapsed, "throughput_per_s": jobs * 1e9 / elapsed,
                "compactions": after["compactions"] - before["compactions"], "before": before, "after": after,
                "final_artifact": final, "final_artifact_readback": True, "client_trace": client_name, "go_trace": go_name,
                "warmup_ns": warmup_ns, "lifecycle_ns": lifecycle_ns, "shutdown": shutdown,
                "jobs_ns": [record["duration_ns"] for record in records if record["measured"]]}


async def experiment(args, binary, output):
    modes = ("fresh", "pool") if args.pool_comparison else ("fresh",)
    cases = [(contexts, segment, mode) for contexts in (1, 4) for segment in (64 << 10, 8 << 20) for mode in modes]
    trials = []
    for repetition in range(args.repeat):
        order = cases[repetition % len(cases):] + cases[:repetition % len(cases)]
        for contexts, segment, mode in order:
            identity = f"r{repetition+1}-c{contexts}-s{segment}-{mode}"
            result = await trial(binary, output, identity, contexts=contexts, jobs=args.jobs,
                                 segment_bytes=segment, tracing=True, warmup=args.warmup, mode=mode)
            trials.append(result)
            print(identity, f"{result['throughput_per_s']:.2f}/s, compactions={result['compactions']}", flush=True)
    # A separately labelled control estimates perturbation of tracing itself.
    controls = []
    for repetition in range(args.repeat):
        for mode in modes:
            for tracing in ((False, True) if repetition % 2 == 0 else (True, False)):
                identity = f"overhead-r{repetition+1}-trace{int(tracing)}-{mode}"
                result = await trial(binary, output, identity, contexts=4, jobs=args.control_jobs,
                                     segment_bytes=8 << 20, tracing=tracing, warmup=args.warmup, mode=mode)
                controls.append(result)
                print(identity, f"{result['throughput_per_s']:.2f}/s", flush=True)
    return {"trials": trials, "tracing_controls": controls}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="results/local/short-tools/report.json")
    parser.add_argument("--jobs", type=int, default=240, help="equal measured jobs per trial; multiple of 12")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=24)
    parser.add_argument("--control-jobs", type=int, default=120)
    parser.add_argument("--pool-comparison", action="store_true", help="interleave fresh and bounded pooled execution with identical eligible tools")
    args = parser.parse_args()
    if min(args.jobs, args.repeat, args.warmup, args.control_jobs) < 1 or any(n % 12 for n in (args.jobs, args.warmup, args.control_jobs)):
        parser.error("positive counts; jobs, warmup and control-jobs must be multiples of 12")
    output = (ROOT / args.out).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    source = fingerprint()
    with tempfile.TemporaryDirectory(prefix="focus-short-build-") as name:
        binary = Path(name) / ("runtime.exe" if os.name == "nt" else "runtime")
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/runtime"], cwd=ROOT, check=True)
        report = asyncio.run(experiment(args, binary, output))
    report["config"] = {**vars(args), "platform": platform.platform(), "python": sys.version,
        "go": subprocess.check_output(["go", "version"], text=True).strip(), "concurrency": 4,
        "fsync": "every WAL mutation, consumer checkpoint and effective-action artifact",
        "clocks": "local host Unix ns for correlation; per-process monotonic durations",
        "scope": "deterministic local controller; no model or Patrol; no concurrent experiments",
        "source_sha256_lf_normalized": source}
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
