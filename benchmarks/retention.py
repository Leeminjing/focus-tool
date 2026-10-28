"""10,000 real short Python tools, Run churn, bounded live state and restart.

Uses the full adapter/authorization/stdio/fsynced runtime chain. No model API.
Run: python benchmarks/retention.py --out results/local/retention.json
"""
import argparse
import asyncio
import gc
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
from focus_tool import FocusToolAdapter, RuntimeClient, Run, ToolDefinition

TOOLS = '''
import json
from pathlib import Path

def read_file():
    text = Path("corpus.txt").read_text()
    return {"bytes": len(text)}

def search_text():
    text = Path("corpus.txt").read_text()
    return {"matches": text.count("Patrol")}

def parse_config():
    return json.loads(Path("config.json").read_text())
'''


def fingerprint():
    paths = sorted([*ROOT.glob("runtime/*.go"), ROOT / "cmd/runtime/main.go",
                    *ROOT.glob("python/focus_tool/*.py"), Path(__file__).resolve()])
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            for path in paths}


async def experiment(args, binary, directory, trace):
    (directory / "retention_tools.py").write_text(TOOLS)
    (directory / "corpus.txt").write_text("Patrol local tool runtime\n" * 128)
    (directory / "config.json").write_text('{"valid":true,"value":42}')
    names = ("read_file", "search_text", "parse_config")
    expected = ({"bytes": len("Patrol local tool runtime\n") * 128}, {"matches": 128}, {"valid": True, "value": 42})
    journal = directory / "runtime.jsonl"
    client = await RuntimeClient.start([str(binary)], journal=journal, concurrency=8, capacity=128,
                                       per_run_capacity=16, max_active_runs=8, segment_bytes=256 << 10)

    def adapter_for(connection):
        return FocusToolAdapter(connection, {name: ToolDefinition("python", module="retention_tools", function=name)
                                             for name in names}, workspace=directory,
                                before_execute=lambda task: Path(task["directory"]).resolve() == directory.resolve())

    adapter = adapter_for(client)
    samples = []
    first_task = first_owner = first_sequence = None
    count = runs = 0
    start = time.perf_counter()

    async def context_epoch(epoch, context):
        nonlocal count, runs, first_task, first_owner, first_sequence
        remaining = args.tasks // args.epochs // 4
        run_number = 0
        while remaining:
            run = await client.open_run(f"context-{context}", f"epoch-{epoch}-run-{run_number}")
            runs += 1
            this_run = min(32, remaining)
            checkpoint = directory / f"checkpoint-{context}.json"
            for offset in range(0, this_run, 8):
                batch = list(range(offset, min(offset + 8, this_run)))
                replies = await asyncio.gather(*(adapter.submit(run, parent_message_id="m", call_id=str(index),
                    name=names[index % 3], args={}) for index in batch))
                assert all(json.loads(reply["content"])["state"] == "accepted" for reply in replies)
                for _ in batch:
                    event = await asyncio.wait_for(run.next_event(), 60)
                    assert event["result"]["state"] == "completed", event
                    index = next(index for index in batch if adapter.job_id("m", str(index)) == event["task"]["id"])
                    assert json.loads(event["result"]["output"]) == expected[index % 3]
                    with checkpoint.open("w", encoding="utf-8") as stream:
                        json.dump(event, stream)
                        stream.flush()
                        os.fsync(stream.fileno())
                    consumed = time.time_ns()
                    await run.ack(event)
                    count += 1
                    if first_task is None:
                        first_task, first_owner, first_sequence = event["task"], dict(run.owner), event["sequence"]
                    trace.write(json.dumps({"n": count, "owner": run.owner, "job_id": event["task"]["id"],
                        "sequence": event["sequence"], "state": event["result"]["state"],
                        "timing": event["timing"], "consumed_ns": consumed}, separators=(",", ":")) + "\n")
            receipt = await run.close()
            assert receipt["pending_executions"] == receipt["unacknowledged_facts"] == 0
            remaining -= this_run
            run_number += 1

    try:
        for epoch in range(args.epochs):
            await asyncio.gather(*(context_epoch(epoch, context) for context in range(4)))
            gc.collect()
            stats = await client.request("stats", collect=True)
            assert stats["retained_tasks"] == stats["retained_events"] == stats["resident_runs"] == stats["queue_owners"] == 0, stats
            sample = {"tasks_completed": count, "runs_closed": runs, "elapsed_s": time.perf_counter() - start,
                      "runtime": stats, "adapter_registered": len(adapter._submitted),
                      "client_mailboxes": len(client._mailboxes), "client_subscriptions": len(client._subscriptions)}
            assert sample["adapter_registered"] == sample["client_mailboxes"] == sample["client_subscriptions"] == 0, sample
            samples.append(sample)
            trace.flush()
            print(f"{count}/{args.tasks}: runs={runs}, heap={stats['heap_bytes']}, compactions={stats['compactions']}", flush=True)
        await client.request("compact")
        archive = Run(client, first_owner, 1, subscribe=False)
        first_page = await archive.archive_page(limit=1)
        assert first_page["items"][0]["sequence"] == first_sequence
        cursor = first_page["next_cursor"]
        await client.close()
        client = await RuntimeClient.start([str(binary)], journal=journal, concurrency=8, capacity=128,
                                           per_run_capacity=16, max_active_runs=8, segment_bytes=256 << 10)
        adapter_for(client)
        status = await client.request("submit", task=first_task)
        assert status["state"] == "completed" and status["timing"]["finished_ns"] == first_page["items"][0]["timing"]["finished_ns"]
        archive = Run(client, first_owner, 1, subscribe=False)
        resumed = await archive.archive_page(cursor=cursor, limit=1)
        assert resumed["items"][0]["sequence"] > first_sequence
        restarted_stats = await client.request("stats", collect=True)
        assert restarted_stats["retained_tasks"] == restarted_stats["retained_events"] == restarted_stats["resident_runs"] == 0
        return {"tasks": count, "runs_closed": runs, "elapsed_s": time.perf_counter() - start,
                "samples": samples, "restart": restarted_stats,
                "old_identity_reconciled": True, "archive_cursor_resumed": True,
                "canonical_history_bytes": sum(path.stat().st_size for path in Path(str(journal) + ".store").rglob("*") if path.is_file()),
                "limits": {"concurrency": 8, "capacity": 128, "per_run_capacity": 16, "max_active_runs": 8, "segment_bytes": 256 << 10}}
    finally:
        await client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=10000)
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--out", default="results/local/retention.json")
    args = parser.parse_args()
    if args.tasks < 4 * args.epochs or args.tasks % (4 * args.epochs):
        parser.error("tasks must be a positive multiple of 4 * epochs")
    out = ROOT / args.out
    out.parent.mkdir(parents=True, exist_ok=True)
    trace_path = out.with_suffix(".trace.jsonl")
    source = fingerprint()
    with tempfile.TemporaryDirectory(prefix="focus-retention-") as name, trace_path.open("w", encoding="utf-8") as trace:
        directory = Path(name)
        binary = directory / ("runtime.exe" if os.name == "nt" else "runtime")
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/runtime"], cwd=ROOT, check=True)
        report = asyncio.run(experiment(args, binary, directory, trace))
    report.update({"platform": platform.platform(), "python": sys.version, "source_sha256": source,
                   "trace": str(trace_path.relative_to(ROOT)), "scope": "real Python leaf tools + adapter + authorization + fsync + ack; no Patrol or model"})
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(out)


if __name__ == "__main__":
    main()
