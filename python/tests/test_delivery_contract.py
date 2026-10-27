"""Deterministic protocol-boundary reproductions; no tool/model dependencies."""
import asyncio
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from focus_tool import FocusToolAdapter, Run, RuntimeClient, ToolDefinition


class PipeProcess:
    def __init__(self, disconnect_after_accept=False):
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.stderr.feed_eof()
        self.stdin = self
        self.accepted = []
        self.disconnect_after_accept = disconnect_after_accept

    def write(self, data):
        request = json.loads(data)
        if request["method"] == "submit" and self.disconnect_after_accept:
            self.accepted.append(request["task"])
            self.stdout.feed_eof()  # Receipt lost after the backend accepted it.

    async def drain(self):
        await asyncio.sleep(0)

    def emit(self, sequence):
        event = {"sequence": sequence, "task": {"owner": {"context_id": "A", "run_id": "a"},
                 "revision": 1, "id": str(sequence)}, "result": {"state": "completed", "output": "true"}}
        self.stdout.feed_data(json.dumps({"type": "event", "event": event}).encode() + b"\n")


class DeliveryContracts(unittest.IsolatedAsyncioTestCase):
    async def make_client(self, disconnect=False):
        process = PipeProcess(disconnect)
        client = RuntimeClient(process)

        async def cleanup():
            client._reader.cancel()
            await asyncio.gather(client._reader, client._stderr_reader, return_exceptions=True)
        self.addAsyncCleanup(cleanup)
        return client, process, Run(client, {"context_id": "A", "run_id": "a"}, 1)

    async def test_cancelled_wait_preserves_the_taken_event_and_original_order(self):
        client, process, run = await self.make_client()
        waiter = asyncio.create_task(run.next_event())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        process.emit(1)
        await asyncio.sleep(0)  # Reader publishes; mailbox wait is about to resume.
        asyncio.get_running_loop().call_soon(waiter.cancel)
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        process.emit(2)
        first = await asyncio.wait_for(run.next_event(), .5)
        self.assertEqual(first["sequence"], 1, "cancelled waiter stole the first event")
        second = await asyncio.wait_for(run.next_event(), .5)
        self.assertEqual(second["sequence"], 2)

    async def test_lost_submission_receipt_is_not_a_rejection(self):
        client, process, run = await self.make_client(disconnect=True)
        adapter = FocusToolAdapter(client, {"start_write": ToolDefinition("python", module="tools", function="write")}, workspace=Path.cwd())
        replies = await adapter.submit_tool_calls(run, {"id": "parent", "tool_calls": [
            {"id": "write", "name": "start_write", "args": {"value": 1}},
        ]})
        self.assertEqual(len(process.accepted), 1)
        receipt = json.loads(replies[0]["content"])
        self.assertEqual(receipt["state"], "acceptance_unknown")
        self.assertEqual(receipt["job_id"], process.accepted[0]["id"])
        retry = await adapter.submit(run, parent_message_id="parent", call_id="write", name="start_write", args={"value": 1})
        self.assertEqual(json.loads(retry["content"])["state"], "acceptance_unknown")
        self.assertEqual(len(process.accepted), 1)

    async def test_explicit_server_rejection_is_still_rejected(self):
        client, process, run = await self.make_client()
        from focus_tool import RemoteRejected

        async def reject(method, **fields):
            raise RemoteRejected("run capacity exhausted")

        client.request = reject
        adapter = FocusToolAdapter(client, {"start_write": ToolDefinition("python", module="tools", function="write")}, workspace=Path.cwd())
        replies = await adapter.submit_tool_calls(run, {"id": "parent", "tool_calls": [
            {"id": "write", "name": "start_write", "args": {"value": 1}},
        ]})
        self.assertEqual(json.loads(replies[0]["content"])["state"], "rejected")
