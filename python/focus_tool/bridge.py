"""Durable result handover, independent of any Focus activation lifecycle."""
import asyncio
import copy


class ResultBridge:
    """The sink must upsert by owner/sequence and verify identical event content.

    confirmed(event) must validate a durable decision checkpoint and consumption
    receipt. Inbox insertion alone does not confirm semantic consumption.
    """

    def __init__(self, run, persist, confirmed):
        self.run, self.persist, self.confirmed = run, persist, confirmed
        self._delivery = asyncio.Lock()

    async def deliver_next(self):
        async with self._delivery:
            event = await self.run.next_fact()
            try:
                await self.persist(copy.deepcopy(event))
            except BaseException:
                self.run.requeue_fact(event)
                raise
            return event

    async def ack_consumed(self, event):
        if not await self.confirmed(copy.deepcopy(event)):
            raise RuntimeError("result has no durable decision checkpoint/consumption receipt")
        await self.run.ack(event)
