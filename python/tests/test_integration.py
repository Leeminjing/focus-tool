import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from focus_tool import FocusToolAdapter, RuntimeClient, ToolDefinition


ROOT = Path(__file__).resolve().parents[2]
TOOLS = '''
import asyncio
import json
from pathlib import Path

def read_config():
    print("tool diagnostics must not corrupt stdout protocol")
    return json.loads(Path("config.json").read_text())

def repair_config():
    Path("repaired.json").write_text('{"valid":true}')
    return json.loads(Path("repaired.json").read_text())

async def echo(value):
    await asyncio.sleep(0)
    return {"value": value}

def increment(value):
    return {"answer": value + 1}

def fail():
    raise ValueError("expected tool failure")

def append_once():
    path = Path("counter.txt")
    path.write_text(path.read_text() + "x" if path.exists() else "x")
    return {"count": len(path.read_text())}

def large_result():
    return "x" * (512 * 1024)
'''


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.build_dir = tempfile.TemporaryDirectory(prefix="focus-runtime-build-")
        cls.binary = Path(cls.build_dir.name) / ("runtime.exe" if os.name == "nt" else "runtime")
        subprocess.run(["go", "build", "-o", str(cls.binary), "./cmd/runtime"], cwd=ROOT, check=True)

    @classmethod
    def tearDownClass(cls):
        cls.build_dir.cleanup()

    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="focus-runtime-test-")
        self.workspace = Path(self.directory.name)
        (self.workspace / "tools.py").write_text(TOOLS)
        (self.workspace / "config.json").write_text('{"valid":false}')
        self.clients = []

    async def asyncTearDown(self):
        for client in self.clients:
            if client.process.returncode is None:
                await client.close()
        self.directory.cleanup()

    async def start(self, *, journal="events.jsonl", hook=None, concurrency=2, capacity=16, trace_path=None, trace=None,
                    python_workers=0, allow_reuse=False, start_paused=False):
        client = await RuntimeClient.start([str(self.binary)], journal=self.workspace / journal,
                                           concurrency=concurrency, capacity=capacity, trace_path=trace_path, trace=trace,
                                           python_workers=python_workers, start_paused=start_paused)
        self.clients.append(client)
        tools = {name: ToolDefinition("python", module="tools", function=function, reuse=allow_reuse)
                 for name, function in {"start_check": "read_config", "start_repair": "repair_config",
                                        "start_echo": "echo", "start_increment": "increment",
                                        "start_failure": "fail", "start_once": "append_once",
                                        "start_large": "large_result"}.items()}
        tools["start_slow"] = ToolDefinition("process", command=(sys.executable, "-u", "-c",
            "import time; print('started', flush=True); time.sleep(60)"))
        tools["start_timeout"] = ToolDefinition("process", command=(sys.executable, "-c",
            "import time; time.sleep(60)"), timeout_ms=150)
        adapter = FocusToolAdapter(client, tools, workspace=self.workspace, before_execute=hook)
        return client, adapter

    async def event(self, run):
        return await asyncio.wait_for(run.next_event(), 8)

    async def test_two_contexts_replan_cancel_and_route_to_their_own_runs(self):
        client, adapter = await self.start()
        a = await client.open_run("A", "run-A")
        b = await client.open_run("B", "run-B")
        message = SimpleNamespace(id="message-1", tool_calls=[
            {"id": "check", "name": "start_check", "args": {}},
            {"id": "slow", "name": "start_slow", "args": {}},
        ])
        submissions = await adapter.submit_tool_calls(a, message)
        self.assertEqual([item["tool_call_id"] for item in submissions], ["check", "slow"])
        self.assertTrue(all(json.loads(item["content"])["state"] == "accepted" for item in submissions))
        await adapter.submit_tool_calls(b, {"id": "message-1", "tool_calls": [
            {"id": "check", "name": "start_echo", "args": {"value": "B-only"}},
        ]})
        check = await self.event(a)
        self.assertEqual(json.loads(check["result"]["output"]), {"valid": False})
        await a.ack(check)  # The controller has saved/accepted this decision input.
        await a.revise(2)
        await adapter.submit(a, parent_message_id="message-2", call_id="repair",
                             name="start_repair", args={})
        repaired = await self.event(a)
        self.assertEqual(repaired["task"]["revision"], 2)
        self.assertTrue(json.loads(repaired["result"]["output"])["valid"])
        await a.ack(repaired)
        other = await self.event(b)
        self.assertEqual(other["task"]["owner"], b.owner)
        self.assertEqual(json.loads(other["result"]["output"]), {"value": "B-only"})
        await b.ack(other)
        slow = await a.status(adapter.job_id("message-1", "slow"))
        self.assertEqual(slow["state"], "cancelled")

    async def test_dependency_output_binds_to_python_argument(self):
        client, adapter = await self.start()
        run = await client.open_run("A", "dependent")
        parent = await adapter.submit(run, parent_message_id="m1", call_id="p", name="start_echo", args={"value": 41})
        parent_id = json.loads(parent["content"])["job_id"]
        await adapter.submit(run, parent_message_id="m1", call_id="c", name="start_increment", args={},
                             dependencies=[parent_id], bindings={"value": {"task_id": parent_id, "path": ["value"]}})
        first = await self.event(run)
        self.assertEqual(first["task"]["id"], parent_id)
        await run.ack(first)
        second = await self.event(run)
        self.assertEqual(second["result"]["state"], "completed", second)
        self.assertEqual(json.loads(second["result"]["output"]), {"answer": 42})
        await run.ack(second)

    async def test_dependency_binding_preserves_large_integer_precision(self):
        client, adapter = await self.start()
        run = await client.open_run("A", "precision")
        parent = await adapter.submit(run, parent_message_id="m", call_id="parent",
                                      name="start_echo", args={"value": 9007199254740993})
        parent_id = json.loads(parent["content"])["job_id"]
        await adapter.submit(run, parent_message_id="m", call_id="child", name="start_increment", args={},
                             dependencies=[parent_id], bindings={"value": {"task_id": parent_id, "path": ["value"]}})
        first = await self.event(run)
        await run.ack(first)
        second = await self.event(run)
        self.assertEqual(json.loads(second["result"]["output"]), {"answer": 9007199254740994})
        await run.ack(second)

    async def test_admission_is_rechecked_when_queued_task_reaches_execution(self):
        entered, release = asyncio.Event(), asyncio.Event()
        permitted = True

        async def hook(task):
            if task["id"] == adapter.job_id("m", "first"):
                entered.set()
                await release.wait()
            return permitted

        client, adapter = await self.start(hook=hook, concurrency=1)
        run = await client.open_run("A", "permissions")
        await adapter.submit(run, parent_message_id="m", call_id="first", name="start_echo", args={"value": 1})
        await asyncio.wait_for(entered.wait(), 5)
        await adapter.submit(run, parent_message_id="m", call_id="second", name="start_once", args={})
        permitted = False
        release.set()
        events = []
        for _ in range(2):
            event = await self.event(run)
            await run.ack(event)
            events.append(event)
        self.assertTrue(all(event["result"]["state"] == "failed" for event in events))
        self.assertFalse((self.workspace / "counter.txt").exists())

    async def test_terminal_event_replays_after_restart_without_reexecuting_tool(self):
        client, adapter = await self.start()
        run = await client.open_run("A", "durable")
        await adapter.submit(run, parent_message_id="m", call_id="once", name="start_once", args={})
        original = await self.event(run)  # Deliberately do not ack before restart.
        self.assertEqual(json.loads(original["result"]["output"]), {"count": 1})
        await client.close()
        restarted, new_adapter = await self.start()
        new_run = await restarted.open_run("A", "durable")
        replay = await self.event(new_run)
        self.assertEqual(replay["sequence"], original["sequence"])
        await new_run.ack(replay)
        await new_adapter.submit(new_run, parent_message_id="m", call_id="once", name="start_once", args={})
        self.assertEqual((self.workspace / "counter.txt").read_text(), "x")
        self.assertEqual(await restarted.request("events", owner=new_run.owner), None)

    async def test_killed_queued_daemon_restores_registration_before_current_authority(self):
        for revoked in (False, True):
            with self.subTest(revoked=revoked):
                journal = "queued-" + str(revoked) + ".jsonl"
                client, adapter = await self.start(journal=journal, start_paused=True)
                run = await client.open_run("A", "recover")
                await adapter.submit(run, parent_message_id="stable", call_id="once", name="start_once", args={})
                self.assertEqual((await run.status(adapter.job_id("stable", "once")))["state"], "queued")
                client.process.kill()
                await client.process.wait()
                await client.close()

                checks = []
                async def current_authority(task):
                    checks.append(task["id"])
                    return not revoked
                recovered, trusted = await self.start(journal=journal, hook=current_authority)
                owner = await recovered.open_run("A", "recover")
                self.assertTrue((await recovered.request("stats"))["dispatch_paused"])
                with self.assertRaisesRegex(RuntimeError, "not been restored"):
                    await trusted.resume_dispatch()
                self.assertEqual(checks, [])
                await trusted.submit(owner, parent_message_id="stable", call_id="once", name="start_once", args={})
                await trusted.resume_dispatch()
                event = await self.event(owner)
                self.assertEqual(event["result"]["state"], "failed" if revoked else "completed")
                self.assertEqual(len(checks), 1)
                if revoked:
                    self.assertNotIn("pid", event["result"])
                await owner.ack(event)
                await recovered.close()
        self.assertEqual((self.workspace / "counter.txt").read_text(), "x")

    async def test_tool_exception_is_returned_to_owner_as_failure(self):
        client, adapter = await self.start()
        run = await client.open_run("A", "failure")
        await adapter.submit(run, parent_message_id="m", call_id="f", name="start_failure", args={})
        event = await self.event(run)
        self.assertEqual(event["result"]["state"], "failed")
        self.assertIn("expected tool failure", event["result"]["error"])
        await run.ack(event)

    async def test_conflicting_identity_and_cross_run_ack_are_rejected(self):
        client, adapter = await self.start()
        a, b = await client.open_run("A", "a"), await client.open_run("B", "b")
        await adapter.submit(a, parent_message_id="m", call_id="x", name="start_echo", args={"value": 1})
        with self.assertRaisesRegex(ValueError, "conflicting"):
            await adapter.submit(a, parent_message_id="m", call_id="x", name="start_echo", args={"value": 2})
        event = await self.event(a)
        with self.assertRaises(ValueError):
            await b.ack(event)
        await a.ack(event)

    async def test_run_close_cancels_running_work_and_rejects_new_work(self):
        client, adapter = await self.start()
        run = await client.open_run("A", "closing")
        await adapter.submit(run, parent_message_id="m", call_id="slow", name="start_slow", args={})
        await run.close()
        identity = adapter.job_id("m", "slow")
        for _ in range(200):
            if (await run.status(identity))["state"] == "cancelled":
                break
            await asyncio.sleep(.01)
        self.assertEqual((await run.status(identity))["state"], "cancelled")
        archived = await run.archived_events()
        self.assertEqual(archived[0]["result"]["state"], "cancelled")
        with self.assertRaises(RuntimeError):
            await adapter.submit(run, parent_message_id="m", call_id="new", name="start_echo", args={"value": 1})

    async def test_unregistered_raw_task_is_denied_before_process_starts(self):
        client, adapter = await self.start()
        run = await client.open_run("A", "unregistered")
        await client.request("submit", task={
            "id": "unregistered", "owner": run.owner, "revision": 1, "kind": "process",
            "directory": str(self.workspace), "command": [sys.executable, "-c",
                "from pathlib import Path; Path('forbidden.txt').write_text('bad')"],
        })
        event = await self.event(run)
        self.assertEqual(event["result"]["state"], "failed")
        self.assertNotIn("pid", event["result"])
        self.assertFalse((self.workspace / "forbidden.txt").exists())
        await run.ack(event)

    async def test_process_timeout_releases_slot_for_another_run(self):
        client, adapter = await self.start(concurrency=1)
        a = await client.open_run("A", "timeout")
        b = await client.open_run("B", "after-timeout")
        await adapter.submit(a, parent_message_id="m", call_id="slow", name="start_timeout", args={})
        await adapter.submit(b, parent_message_id="m", call_id="echo", name="start_echo", args={"value": 7})
        cancelled = await self.event(a)
        self.assertEqual(cancelled["result"]["state"], "cancelled")
        await a.ack(cancelled)
        event = await self.event(b)
        self.assertEqual(json.loads(event["result"]["output"]), {"value": 7})
        await b.ack(event)

    async def test_slow_consumer_applies_backpressure_until_checkpoint_ack(self):
        client, adapter = await self.start(concurrency=1, capacity=1)
        a = await client.open_run("A", "a")
        b = await client.open_run("B", "b")
        await adapter.submit(a, parent_message_id="m", call_id="one", name="start_echo", args={"value": 1})
        event = await self.event(a)
        with self.assertRaisesRegex(RuntimeError, "run capacity exhausted"):
            await adapter.submit(a, parent_message_id="m", call_id="two", name="start_echo", args={"value": 2})
        await adapter.submit(b, parent_message_id="m", call_id="two", name="start_echo", args={"value": 2})
        other = await self.event(b)
        self.assertEqual(json.loads(other["result"]["output"]), {"value": 2})
        await b.ack(other)
        await a.ack(event)

    async def test_partially_accepted_batch_preserves_every_tool_message_pair(self):
        client, adapter = await self.start(concurrency=1, capacity=1)
        run = await client.open_run("A", "batch")
        messages = await adapter.submit_tool_calls(run, {"id": "batch", "tool_calls": [
            {"id": "one", "name": "start_echo", "args": {"value": 1}},
            {"id": "two", "name": "start_echo", "args": {"value": 2}},
            {"id": "unknown", "name": "not_registered", "args": {}},
        ]})
        self.assertEqual([message["tool_call_id"] for message in messages], ["one", "two", "unknown"])
        self.assertEqual([json.loads(message["content"])["state"] for message in messages],
                         ["accepted", "rejected", "rejected"])
        event = await self.event(run)
        self.assertEqual(json.loads(event["result"]["output"]), {"value": 1})
        await run.ack(event)

    async def test_selector_compatible_transport_executes_real_daemon(self):
        with patch("asyncio.create_subprocess_exec", side_effect=NotImplementedError):
            client, adapter = await self.start()
        run = await client.open_run("A", "threaded")
        await adapter.submit(run, parent_message_id="m", call_id="x", name="start_echo", args={"value": 42})
        event = await self.event(run)
        self.assertEqual(json.loads(event["result"]["output"]), {"value": 42})
        await run.ack(event)
        await run.close()

    async def test_close_transfers_even_acknowledged_facts_to_inspectable_archive(self):
        client, adapter = await self.start()
        run = await client.open_run("A", "archive")
        await adapter.submit(run, parent_message_id="m", call_id="x", name="start_echo", args={"value": 42})
        event = await self.event(run)
        await run.ack(event)
        receipt = await run.close()
        self.assertEqual(receipt["custodian"], "runtime_archive")
        self.assertEqual(receipt["unacknowledged_facts"], 0)
        facts = await run.archived_events()
        self.assertEqual([fact["sequence"] for fact in facts], [event["sequence"]])
        with self.assertRaisesRegex(RuntimeError, "Run closed"):
            await run.next_event()

    async def test_large_archive_pages_keep_the_shared_connection_usable(self):
        client, adapter = await self.start(capacity=32)
        run = await client.open_run("large", "archive")
        sequences = []
        for index in range(17):
            await adapter.submit(run, parent_message_id="large", call_id=str(index), name="start_large", args={})
            event = await self.event(run)
            self.assertEqual(len(json.loads(event["result"]["output"])), 512 * 1024)
            sequences.append(event["sequence"])
            await run.ack(event)
        await run.close()
        page = await run.archive_page(limit=3, max_bytes=1200 * 1024)
        self.assertEqual(len(page["items"]), 2)
        self.assertLessEqual(len(json.dumps(page, separators=(",", ":")).encode()), 1200 * 1024)
        with self.assertRaisesRegex(ValueError, "exceeds one page"):
            await run.archived_events()
        cursor = page["next_cursor"]
        await client.request("compact")
        await client.close()
        restarted, new_adapter = await self.start(capacity=32)
        # Closed Runs retain archive custody and are not reopened as live Runs.
        from focus_tool import Run
        archived = Run(restarted, run.owner, 1, subscribe=False)
        resumed = [event async for event in archived.iter_archived_events(cursor=cursor, limit=3, max_bytes=1200 * 1024)]
        self.assertEqual([event["sequence"] for event in page["items"] + resumed], sequences)
        other = await restarted.open_run("other", "still-usable")
        await new_adapter.submit(other, parent_message_id="m", call_id="x", name="start_echo", args={"value": 42})
        event = await self.event(other)
        self.assertEqual(json.loads(event["result"]["output"]), {"value": 42})
        await other.ack(event)

    async def test_optional_trace_preserves_values_and_records_worker_and_durability_boundaries(self):
        path = self.workspace / "diagnostic.jsonl"
        client_trace = []
        client, adapter = await self.start(trace_path=path, trace=client_trace.append)
        run = await client.open_run("A", "traced")
        value = {"integer": 9007199254740993, "text": '\\"\n' * 50000}
        await adapter.submit(run, parent_message_id="m", call_id="x", name="start_echo", args={"value": value})
        event = await self.event(run)
        self.assertEqual(json.loads(event["result"]["output"]), {"value": value})
        await run.ack(event)
        await client.close()
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        job = adapter.job_id("m", "x")
        phases = {row["phase"] for row in rows if row.get("task_id") == job}
        self.assertTrue({"worker_bootstrap", "module_import", "tool_body", "result_encode", "process_start", "process_wait",
                         "admission", "wal_sync", "completion_handoff"} <= phases, phases)
        self.assertTrue(all(row["duration_ns"] >= 0 for row in rows))
        self.assertTrue(any(row["phase"] == "client_receive" and row["sequence"] == event["sequence"] for row in client_trace))
        self.assertTrue(any(row["phase"] == "client_request" and row["operation"] == "ack" for row in client_trace))

    async def test_worker_pool_reuses_only_registered_tools_and_keeps_fresh_mode(self):
        client, adapter = await self.start(python_workers=2, allow_reuse=True)
        run = await client.open_run("A", "pool")
        pids = []
        for number in range(12):
            value = {"number": number, "integer": 9007199254740993}
            await adapter.submit(run, parent_message_id="m", call_id=str(number), name="start_echo", args={"value": value})
            event = await self.event(run)
            self.assertEqual(json.loads(event["result"]["output"]), {"value": value})
            pids.append(event["result"]["pid"])
            await run.ack(event)
        self.assertEqual(len(set(pids)), 1)
        stats = (await client.request("stats"))["python_pool"]
        self.assertEqual(stats["started"], 1)
        self.assertLessEqual(stats["peak_live"], 2)
        adapter.tools["start_isolated"] = ToolDefinition("python", module="tools", function="echo", reuse=False)
        isolated = []
        for number in range(2):
            await adapter.submit(run, parent_message_id="fresh", call_id=str(number), name="start_isolated", args={"value": number})
            event = await self.event(run)
            isolated.append(event["result"]["pid"])
            await run.ack(event)
        self.assertNotEqual(isolated[0], isolated[1])
        self.assertNotIn(pids[0], isolated)

    async def test_call_arguments_and_raw_submission_cannot_opt_into_pool(self):
        client, adapter = await self.start(python_workers=2)
        run = await client.open_run("A", "no-opt-in")
        for number in range(2):
            await adapter.submit(run, parent_message_id="m", call_id=str(number), name="start_echo", args={"value": {"reuse": True}})
            event = await self.event(run)
            self.assertNotIn("reuse", event["task"]["python"])
            await run.ack(event)
        await client.request("submit", task={"id": "raw-reuse", "owner": run.owner, "revision": 1, "kind": "python",
            "directory": str(self.workspace), "python": {"interpreter": sys.executable, "module": "tools", "function": "append_once", "args": {}, "reuse": True}})
        event = await self.event(run)
        self.assertEqual(event["result"]["state"], "failed")
        self.assertNotIn("pid", event["result"])
        self.assertFalse((self.workspace / "counter.txt").exists())
        self.assertEqual((await client.request("stats"))["python_pool"]["started"], 0)
        await run.ack(event)


if __name__ == "__main__":
    unittest.main()
