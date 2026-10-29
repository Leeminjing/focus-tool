import asyncio
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from focus_tool import ResultBridge
import test_delivery_contract as contract


class BridgeContracts(unittest.IsolatedAsyncioTestCase):
    make_client = contract.DeliveryContracts.make_client

    async def test_failed_handover_requeues_fact_without_implicit_ack(self):
        client, process, run = await self.make_client()
        run.revision = 2
        entered = asyncio.Event()

        async def persist(event):
            entered.set()
            await asyncio.Event().wait()

        async def confirmed(event):
            return False

        bridge = ResultBridge(run, persist, confirmed)
        process.emit(1)
        delivery = asyncio.create_task(bridge.deliver_next())
        await asyncio.wait_for(entered.wait(), 1)
        delivery.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await delivery
        # Raw delivery preserves an obsolete revision for durable disposition.
        event = await asyncio.wait_for(run.next_fact(), 1)
        self.assertEqual(event["sequence"], 1)
        with self.assertRaisesRegex(RuntimeError, "checkpoint"):
            await asyncio.wait_for(bridge.ack_consumed(event), 1)
        self.assertEqual(run._last_ack, 0)
