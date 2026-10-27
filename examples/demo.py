"""Run the complete architecture locally: python examples/demo.py."""
import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
from focus_tool import FocusToolAdapter, RuntimeClient, ToolDefinition


async def demo(binary, workspace):
    journal = workspace / "runtime.jsonl"
    start = time.perf_counter()
    client = await RuntimeClient.start([str(binary)], journal=journal, concurrency=3, capacity=16)
    events = []
    async with client:
        # A real Focus integration supplies its current authority/lease check here.
        def before_execute(task):
            return Path(task["directory"]).resolve() == workspace.resolve()

        claim = str(workspace / "config")
        adapter = FocusToolAdapter(client, {
            "start_check": ToolDefinition("python", module="demo_tools", function="read_config", resources=((claim, "read"),)),
            "start_repair": ToolDefinition("python", module="demo_tools", function="repair_config", resources=((claim, "write"),)),
            "start_echo": ToolDefinition("python", module="demo_tools", function="echo"),
            "start_increment": ToolDefinition("python", module="demo_tools", function="increment"),
            "start_scan": ToolDefinition("process", command=(sys.executable, "-u", "-c",
                "import hashlib,pathlib; data=pathlib.Path('corpus.bin').read_bytes(); "
                "[hashlib.sha256(data).digest() for _ in range(2048)]; print('scanned')")),
        }, workspace=workspace, before_execute=before_execute)
        a = await client.open_run("context-A", "run-A")
        b = await client.open_run("context-B", "run-B")
        await adapter.submit_tool_calls(a, {"id": "A-message-1", "tool_calls": [
            {"id": "check", "name": "start_check", "args": {}},
            {"id": "scan", "name": "start_scan", "args": {}},
        ]})
        await adapter.submit_tool_calls(b, {"id": "B-message-1", "tool_calls": [
            {"id": "echo", "name": "start_echo", "args": {"value": 42}},
        ]})

        async def consume(run):
            event = await asyncio.wait_for(run.next_event(), 15)
            events.append({"received_ms": (time.perf_counter() - start) * 1000, **event})
            # Explicit local decision checkpoint before consumption acknowledgment.
            (workspace / (run.owner["run_id"] + "-checkpoint.json")).write_text(json.dumps(event))
            await run.ack(event)
            if event["result"]["state"] != "completed":
                raise RuntimeError(event["result"])
            return event

        check = await consume(a)
        assert json.loads(check["result"]["output"]) == {"valid": False}
        await a.revise(2)
        await adapter.submit(a, parent_message_id="A-message-2", call_id="repair", name="start_repair", args={})
        repaired, echoed = await asyncio.gather(consume(a), consume(b))
        assert json.loads(repaired["result"]["output"]) == {"valid": True}
        assert json.loads(echoed["result"]["output"]) == {"value": 42}
        parent_id = echoed["task"]["id"]
        await adapter.submit(b, parent_message_id="B-message-2", call_id="increment", name="start_increment", args={},
                             dependencies=[parent_id], bindings={"value": {"task_id": parent_id, "path": ["value"]}})
        bound = await consume(b)
        assert json.loads(bound["result"]["output"]) == {"answer": 43}
        scan = await a.status(adapter.job_id("A-message-1", "scan"))
        assert scan["state"] == "cancelled", scan
        await a.close()
        await b.close()
    return {"total_ms": (time.perf_counter() - start) * 1000, "events": events,
            "obsolete_scan": scan, "journal": [json.loads(line) for line in journal.read_text().splitlines()],
            "checks": {"config_repaired": True, "cross_context_routing": True,
                       "dependency_bound": True, "obsolete_scan_cancelled": True,
                       "checkpoint_before_ack": True}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="results/local/full-runtime.json")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="focus-runtime-demo-") as directory:
        workspace = Path(directory)
        binary = workspace / ("runtime.exe" if os.name == "nt" else "runtime")
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/runtime"], cwd=ROOT, check=True)
        shutil.copyfile(ROOT / "examples/demo_tools.py", workspace / "demo_tools.py")
        (workspace / "config.json").write_text('{"valid":false}')
        (workspace / "corpus.bin").write_bytes(b"focus-tool-runtime\n" * (8 * 1024 * 1024 // 19))
        report = asyncio.run(demo(binary, workspace))
    output = ROOT / args.out
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"total_ms": round(report["total_ms"], 2), **report["checks"]}, indent=2))
    print("Full runtime trace:", output)


if __name__ == "__main__":
    main()
