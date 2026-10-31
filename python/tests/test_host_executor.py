import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from focus_tool import FocusToolAdapter, RuntimeClient, ToolDefinition


class HostProtocol(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.build = tempfile.TemporaryDirectory()
        cls.binary = Path(cls.build.name) / ("runtime.exe" if os.name == "nt" else "runtime")
        subprocess.run(["go", "build", "-o", str(cls.binary), "./cmd/runtime"], check=True,
                       cwd=Path(__file__).resolve().parents[2])

    @classmethod
    def tearDownClass(cls):
        cls.build.cleanup()

    async def test_dispatch_owner_cancel_and_close_keep_reader_responsive(self):
        with tempfile.TemporaryDirectory() as root:
            client = await RuntimeClient.start([str(self.binary)], journal=Path(root)/"wal")
            entered = asyncio.Event()
            calls = []
            async def handler(task, cancel):
                calls.append(task["owner"])
                entered.set()
                await cancel.wait()
                await asyncio.sleep(0.02)  # Represents owned cleanup, not a tool benchmark.
                return {"state": "cancelled", "host": {"process_cleanup": "confirmed"}}
            checks = []
            adapter = FocusToolAdapter(client, {"job": ToolDefinition("host", handler=handler)},
                workspace=root, before_execute=lambda task: checks.append(task["id"]))
            run = await client.open_run("A", "run")
            try:
                await adapter.submit(run, parent_message_id="p", call_id="c", name="job", args={})
                await entered.wait()
                await run.cancel(adapter.job_id("p", "c"))
                event = await asyncio.wait_for(run.next_fact(), 3)
                self.assertEqual(event["result"]["state"], "cancelled")
                self.assertEqual(calls, [run.owner])
                self.assertEqual(len(checks), 1)
                # Caller checkpoints before ack; the durable Go fact can then retire.
                (Path(root)/"checkpoint.json").write_text(json.dumps(event))
                await run.ack(event)
                entered.clear()
                await adapter.submit(run, parent_message_id="p2", call_id="c2", name="job", args={})
                await entered.wait()
                stats = await asyncio.wait_for(client.close(), 4)
                self.assertEqual(stats["unknown_fences"], 0)
                self.assertEqual(stats["running"], 0)
            finally:
                if client.process.returncode is None:
                    await client.close()

    async def test_large_or_failed_receipt_is_unknown_other_context_continues(self):
        with tempfile.TemporaryDirectory() as root:
            client = await RuntimeClient.start([str(self.binary)], journal=Path(root)/"wal")
            count = 0
            async def huge(task, cancel):
                nonlocal count
                count += 1
                return {"state": "completed", "output": "x"*(5<<20), "host": {"process_cleanup": "confirmed"}}
            async def normal(task, cancel):
                return {"state": "completed", "output": "done", "host": {"process_cleanup": "not_started"}}
            async def invalid(task, cancel):
                return {"state": 7, "host": {"process_cleanup": 1}}
            adapter = FocusToolAdapter(client, {"huge": ToolDefinition("host", handler=huge),
                "invalid": ToolDefinition("host", handler=invalid),
                "normal": ToolDefinition("host", handler=normal)}, workspace=root)
            a, b = await client.open_run("A", "r"), await client.open_run("B", "r")
            try:
                await adapter.submit(a, parent_message_id="p", call_id="big", name="huge", args={})
                event = await asyncio.wait_for(a.next_fact(), 3)
                self.assertEqual(event["result"]["state"], "unknown")
                await adapter.submit(a, parent_message_id="p", call_id="big", name="huge", args={})
                self.assertEqual(count, 1)
                await adapter.submit(a, parent_message_id="p2", call_id="invalid", name="invalid", args={})
                self.assertEqual((await asyncio.wait_for(a.next_fact(), 3))["result"]["state"], "unknown")
                await adapter.submit(b, parent_message_id="q", call_id="ok", name="normal", args={})
                self.assertEqual((await asyncio.wait_for(b.next_fact(), 3))["result"]["output"], "done")
            finally:
                await client.close()

    def test_registration_rejects_blocking_handler_and_constructor_is_atomic(self):
        with self.assertRaises(ValueError):
            ToolDefinition("host", handler=lambda task, cancel: {})
        sentinel = object()
        client = SimpleNamespace(authorize=None, host_execute=sentinel)
        with self.assertRaises(ValueError):
            FocusToolAdapter(client, {}, workspace=".")
        self.assertIsNone(client.authorize)
        self.assertIs(client.host_execute, sentinel)

    async def test_daemon_loss_fences_started_host_and_never_replays_it(self):
        with tempfile.TemporaryDirectory() as root:
            journal = Path(root)/"wal"
            started, stopped = asyncio.Event(), asyncio.Event()
            calls = []
            async def handler(task, cancel):
                calls.append(task["id"])
                started.set()
                await cancel.wait()
                stopped.set()
                return {"state": "cancelled", "host": {"process_cleanup": "confirmed"}}
            definition = ToolDefinition("host", handler=handler, resources=(("global", "write"),))
            client = await RuntimeClient.start([str(self.binary)], journal=journal)
            adapter = FocusToolAdapter(client, {"job": definition}, workspace=root)
            run = await client.open_run("A", "r")
            await adapter.submit(run, parent_message_id="p", call_id="c", name="job", args={})
            await started.wait()
            client.process.kill()
            await client.process.wait()
            await asyncio.wait_for(stopped.wait(), 2)
            await client.close()
            recovered = await RuntimeClient.start([str(self.binary)], journal=journal, start_paused=True)
            second = FocusToolAdapter(recovered, {"job": definition}, workspace=root)
            again = await recovered.open_run("A", "r")
            try:
                fact = await asyncio.wait_for(again.next_fact(), 2)
                self.assertEqual(fact["result"]["state"], "unknown")
                await second.submit(again, parent_message_id="p", call_id="c", name="job", args={})
                await second.submit(again, parent_message_id="p2", call_id="blocked", name="job", args={})
                await second.resume_dispatch()
                await asyncio.sleep(0.05)
                self.assertEqual(len(calls), 1)
                self.assertEqual((await again.status(second.job_id("p2", "blocked")))["state"], "queued")
            finally:
                await recovered.close()

    async def test_queued_host_recovery_requires_inventory_and_current_authority(self):
        with tempfile.TemporaryDirectory() as root:
            journal = Path(root)/"wal"
            calls, checks = [], []
            async def handler(task, cancel):
                calls.append(task["id"])
                return {"state": "completed", "host": {"process_cleanup": "not_started"}}
            definition = ToolDefinition("host", handler=handler)
            client = await RuntimeClient.start([str(self.binary)], journal=journal, start_paused=True)
            adapter = FocusToolAdapter(client, {"job": definition}, workspace=root)
            run = await client.open_run("A", "r")
            await adapter.submit(run, parent_message_id="p", call_id="c", name="job", args={})
            client.process.kill()
            await client.process.wait()
            await client.close()
            recovered = await RuntimeClient.start([str(self.binary)], journal=journal, start_paused=True)
            def denied(task):
                checks.append(task["id"])
                return False
            second = FocusToolAdapter(recovered, {"job": definition}, workspace=root, before_execute=denied)
            again = await recovered.open_run("A", "r")
            try:
                with self.assertRaises(RuntimeError):
                    await recovered.resume_dispatch([])
                await second.submit(again, parent_message_id="p", call_id="c", name="job", args={})
                await second.resume_dispatch()
                event = await asyncio.wait_for(again.next_fact(), 2)
                self.assertEqual(event["result"]["state"], "failed")
                self.assertEqual(event["result"]["host"]["process_cleanup"], "not_started")
                self.assertEqual(calls, [])
                self.assertEqual(len(checks), 1)
            finally:
                await recovered.close()
