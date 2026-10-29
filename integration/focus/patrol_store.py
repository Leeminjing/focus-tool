"""Research-owned durable bridge records beside the real Focus inbox/checkpoints.

No production migrations: these tables exist only in the isolated experiment DB.
"""
import json
from datetime import UTC, datetime

from sqlalchemy import text, select
from sqlalchemy.dialects.postgresql import insert
from backend.app.desktop.persistence.models import AgentMessage, AgentMessageDelivery
from focus.history import content_hash


def identity(event):
    owner = event["task"]["owner"]
    return owner["context_id"], owner["run_id"], event["sequence"]


def message_id(event):
    return content_hash([*identity(event), event["task"]["id"], event["task"]["revision"]])[:32]


def typed_inbox_ids(items, context):
    return {ref["message_id"] for item in items
        if item["kind"] == "agent_collaboration" and item["origin"] == "collaborator"
        for ref in item["source_refs"] if ref.get("kind") == "inbox"
        and ref.get("task_id") == context and ref.get("recipient") == "main:" + context}


def typed_source(checkpoint, event):
    context = event["task"]["owner"]["context_id"]
    return message_id(event) in typed_inbox_ids(checkpoint.checkpoint["channel_values"].get("execution_items", []), context)


class BridgeStore:
    def __init__(self, sessions, saver):
        self.sessions, self.saver = sessions, saver

    async def setup(self):
        async with self.sessions.begin() as session:
            await session.execute(text("""CREATE TABLE IF NOT EXISTS research_bridge (
                context_id text NOT NULL, logical_run text NOT NULL, sequence bigint NOT NULL,
                event jsonb NOT NULL, message_id text NOT NULL UNIQUE,
                receipt jsonb, acked boolean NOT NULL DEFAULT false,
                PRIMARY KEY (context_id, logical_run, sequence))"""))
            await session.execute(text("""CREATE TABLE IF NOT EXISTS research_actions (
                action_id text PRIMARY KEY, context_id text NOT NULL,
                payload jsonb NOT NULL, done boolean NOT NULL DEFAULT false)"""))
            await session.execute(text("""CREATE TABLE IF NOT EXISTS research_owners (
                context_id text PRIMARY KEY, payload jsonb NOT NULL,
                epoch bigint NOT NULL DEFAULT 0)"""))
            await session.execute(text("""CREATE TABLE IF NOT EXISTS research_wakes (
                context_id text PRIMARY KEY REFERENCES research_owners(context_id),
                generation bigint NOT NULL, handled bigint NOT NULL DEFAULT 0)"""))
            await session.execute(text("""CREATE INDEX IF NOT EXISTS research_bridge_pending
                ON research_bridge(context_id,sequence) WHERE receipt IS NULL"""))
            # Upgrade/recovery: notifications or an old in-memory wake list are
            # never the source of truth. Reconstruct from durable inbox facts.
            await session.execute(text("""INSERT INTO research_wakes(context_id,generation,handled)
                SELECT context_id,max(sequence),coalesce(max(sequence) FILTER(WHERE receipt IS NOT NULL),0)
                FROM research_bridge GROUP BY context_id
                ON CONFLICT(context_id) DO UPDATE SET generation=greatest(research_wakes.generation,excluded.generation)"""))

    async def save_owner(self, owner):
        async with self.sessions.begin() as session:
            await session.execute(text("""INSERT INTO research_owners(context_id,payload) VALUES (:id,CAST(:payload AS jsonb))
                ON CONFLICT(context_id) DO UPDATE SET payload=excluded.payload"""),
                {"id": owner["context_id"], "payload": json.dumps(owner)})

    async def owners(self):
        async with self.sessions() as session:
            return list((await session.execute(text("SELECT payload FROM research_owners ORDER BY context_id"))).scalars())

    async def claim(self, context_id):
        async with self.sessions.begin() as session:
            return (await session.execute(text("UPDATE research_owners SET epoch=epoch+1 WHERE context_id=:id RETURNING epoch"), {"id": context_id})).scalar_one()

    async def fence(self, session, context_id, epoch):
        value = (await session.execute(text("SELECT epoch FROM research_owners WHERE context_id=:id FOR UPDATE"), {"id": context_id})).scalar_one()
        if value != epoch:
            raise PermissionError("superseded bridge service epoch")

    async def publish(self, event, epoch):
        context, logical, sequence = identity(event)
        mid = message_id(event)
        async with self.sessions.begin() as session:
            await self.fence(session, context, epoch)
            existing = (await session.execute(text("SELECT event FROM research_bridge WHERE context_id=:c AND logical_run=:r AND sequence=:s"),
                {"c": context, "r": logical, "s": sequence})).scalar_one_or_none()
            if existing is not None and existing != event:
                raise RuntimeError("conflicting owner/sequence execution fact")
            await session.execute(text("""INSERT INTO research_bridge(context_id,logical_run,sequence,event,message_id)
                VALUES (:c,:r,:s,CAST(:e AS jsonb),:m) ON CONFLICT DO NOTHING"""),
                {"c": context, "r": logical, "s": sequence, "e": json.dumps(event), "m": mid})
            await session.execute(insert(AgentMessage).values(message_id=mid, task_id=context,
                from_agent="tool-runtime", to_agent="main:" + context, kind="message", content=json.dumps(event)).on_conflict_do_nothing())
            await session.execute(text("""INSERT INTO research_wakes(context_id,generation) VALUES (:c,:s)
                ON CONFLICT(context_id) DO UPDATE SET generation=greatest(research_wakes.generation,excluded.generation)"""),
                {"c": context, "s": sequence})

    async def pending_wakes(self):
        async with self.sessions() as session:
            return [dict(row) for row in (await session.execute(text("""SELECT w.context_id,
                o.payload->>'loop_id' AS loop_id,w.generation FROM research_wakes w
                JOIN research_owners o USING(context_id)
                WHERE EXISTS(SELECT 1 FROM research_bridge b
                    WHERE b.context_id=w.context_id AND b.receipt IS NULL)
                ORDER BY w.generation,w.context_id"""))).mappings()]

    async def facts(self, context, *, unconsumed=False, unacked=False):
        where = " AND receipt IS NULL" if unconsumed else ""
        if unacked:
            where += " AND NOT acked"
        async with self.sessions() as session:
            return list((await session.execute(text("SELECT event FROM research_bridge WHERE context_id=:c" + where + " ORDER BY sequence"), {"c": context})).scalars())

    async def consume(self, events, checkpoint, decisions, actions, epoch):
        if not events:
            return
        exact = await self.saver.aget_tuple(checkpoint)
        if exact is None or exact.checkpoint["channel_values"].get("decisions") != decisions:
            raise RuntimeError("decision checkpoint does not contain the exact decisions")
        context = events[0]["task"]["owner"]["context_id"]
        async with self.sessions.begin() as session:
            await self.fence(session, context, epoch)
            for event in events:
                if not typed_source(exact, event):
                    raise RuntimeError("decision checkpoint lacks the owned typed inbox source")
                if event["task"]["owner"]["context_id"] != context:
                    raise RuntimeError("cross Context semantic consumption")
                mid = message_id(event)
                delivery = await session.scalar(select(AgentMessageDelivery).where(AgentMessageDelivery.message_id == mid))
                if delivery is None:
                    raise RuntimeError("decision precedes native typed inbox confirmation")
                key = list(identity(event))
                if not any(item["identity"] == key and item["event_hash"] == content_hash(event) for item in decisions):
                    raise RuntimeError("decision does not classify this exact execution fact")
                receipt = {"checkpoint": checkpoint, "decisions_hash": content_hash(decisions),
                    "event_hash": content_hash(event), "native_checkpoint": delivery.checkpoint_id,
                    "consumed_at": datetime.now(UTC).isoformat()}
                await session.execute(text("""UPDATE research_bridge SET receipt=COALESCE(receipt,CAST(:receipt AS jsonb))
                    WHERE context_id=:c AND logical_run=:r AND sequence=:s"""),
                    {"c": key[0], "r": key[1], "s": key[2], "receipt": json.dumps(receipt)})
            for action in actions:
                await self._action(session, context, action)
            await session.execute(text("""UPDATE research_wakes SET handled=greatest(handled,:s)
                WHERE context_id=:c AND NOT EXISTS(SELECT 1 FROM research_bridge
                    WHERE context_id=:c AND sequence<=:s AND receipt IS NULL)"""),
                {"c": context, "s": max(event["sequence"] for event in events)})

    async def _action(self, session, context, action):
        old = (await session.execute(text("SELECT payload FROM research_actions WHERE action_id=:id"), {"id": action["id"]})).scalar_one_or_none()
        if old is not None and old != action:
            raise RuntimeError("action identity has conflicting payload")
        await session.execute(text("""INSERT INTO research_actions(action_id,context_id,payload)
            VALUES (:id,:c,CAST(:p AS jsonb)) ON CONFLICT DO NOTHING"""), {"id": action["id"], "c": context, "p": json.dumps(action)})

    async def add_actions(self, context, actions, epoch):
        async with self.sessions.begin() as session:
            await self.fence(session, context, epoch)
            for action in actions:
                await self._action(session, context, action)

    async def actions(self, context, pending=True):
        async with self.sessions() as session:
            return list((await session.execute(text("SELECT payload FROM research_actions WHERE context_id=:c" + (" AND NOT done" if pending else "") + " ORDER BY action_id"), {"c": context})).scalars())

    async def action_done(self, context, action_id, epoch):
        async with self.sessions.begin() as session:
            await self.fence(session, context, epoch)
            await session.execute(text("UPDATE research_actions SET done=true WHERE context_id=:c AND action_id=:id"), {"c": context, "id": action_id})

    async def confirmed(self, event):
        c, r, s = identity(event)
        async with self.sessions() as session:
            receipt = (await session.execute(text("SELECT receipt FROM research_bridge WHERE context_id=:c AND logical_run=:r AND sequence=:s"), {"c": c, "r": r, "s": s})).scalar_one_or_none()
            native = await session.scalar(select(AgentMessageDelivery).where(AgentMessageDelivery.message_id == message_id(event),
                AgentMessageDelivery.checkpoint_id == receipt["native_checkpoint"])) if receipt else None
        if receipt is None or native is None or native.checkpoint_id != receipt["native_checkpoint"] or receipt["event_hash"] != content_hash(event):
            return False
        exact = await self.saver.aget_tuple(receipt["checkpoint"])
        if exact is None or not typed_source(exact, event):
            return False
        decisions = exact.checkpoint["channel_values"].get("decisions", [])
        return content_hash(decisions) == receipt["decisions_hash"] and any(
            item["identity"] == [c, r, s] and item["event_hash"] == content_hash(event) for item in decisions)

    async def receipts(self, context):
        async with self.sessions() as session:
            return [dict(row) for row in (await session.execute(text("SELECT sequence,message_id,receipt,acked FROM research_bridge WHERE context_id=:c ORDER BY sequence"), {"c": context})).mappings()]

    async def acknowledged(self, event, epoch):
        c, r, s = identity(event)
        async with self.sessions.begin() as session:
            await self.fence(session, c, epoch)
            await session.execute(text("UPDATE research_bridge SET acked=true WHERE context_id=:c AND logical_run=:r AND sequence=:s"), {"c": c, "r": r, "s": s})
