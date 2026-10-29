"""Long-lived job/lease owner; intentionally separate from Focus activations."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sys
import time

from langchain.tools import tool
from langchain.tools.tool_node import ToolCallRequest, ToolRuntime
from langchain_core.messages import ToolMessage
from sqlalchemy import select
from backend.app.desktop.patrol.agent_loop.models import AgentLoop, LoopDelegationGrant
from backend.app.desktop.workspaces.coordination import WorkspaceLeaseManager, WorkspaceLeaseRequest
from focus.security.context import AuthorizationIdentity, ExecutionProfile, RoutingIdentity, derive_security_context
from focus.security.effects import declare_effect, structured_fs, ResolvedFsEffect
from focus.security.middleware import AccessPolicyMiddleware
from focus.security.policy import AccessMode
from focus_tool import FocusToolAdapter, RuntimeClient, Run, ResultBridge, ToolDefinition, RemoteRejected

from patrol_store import identity


class ToolService:
    def __init__(self, store, binary, root):
        self.store, self.binary, self.root = store, binary, root
        self.owners, self.runs, self.epochs, self.leases, self.bridges = {}, {}, {}, {}, {}
        self.checks, self.renewals, self.published = [], 0, []
        self.action_trace = []
        self.lease_manager = WorkspaceLeaseManager(store.sessions)
        self._heartbeat = None
        self._heartbeat_error = None
        self._pumps = []

    async def start(self):
        self.client = await RuntimeClient.start([str(self.binary)], journal=self.root / "runtime.jsonl",
            concurrency=4, capacity=64, start_paused=True)
        self.adapter = FocusToolAdapter(self.client, {"start_local": ToolDefinition("python", module="patrol_tools",
            function="perform", interpreter=sys.executable)}, workspace=Path(__file__).parent, before_execute=self.admit)
        for owner in await self.store.owners():
            await self.attach(owner)
        self._heartbeat = asyncio.create_task(self.renew())

    async def attach(self, owner):
        context = owner["context_id"]
        self.owners[context] = owner
        self.epochs[context] = await self.store.claim(context)
        # A semantic receipt/action outbox is the durable plan intent. Either
        # side may have committed before a crash; never reopen with an older
        # revision merely because the convenience owner row lagged the WAL.
        actions = await self.store.actions(context, pending=False)
        revision = max([owner.get("revision", 1), *(action["revision"] for action in actions)])
        if revision != owner.get("revision", 1):
            owner["revision"] = revision
            await self.store.save_owner(owner)
        run = await self.client.open_run(context, owner["logical_run"], revision=owner.get("revision", 1))
        # A new service may not reclaim an uncertain workspace merely because
        # its old lease expired. Reconcile execution facts before acquiring it.
        for action in actions:
            try:
                status = await run.status(self.adapter.job_id(action["id"], action["operation"]))
            except RemoteRejected:
                continue
            if status["state"] == "unknown":
                raise RuntimeError("unknown execution quarantines this job workspace; reconcile before attach")
        grant = await self.lease_manager.acquire(WorkspaceLeaseRequest(slot_id=owner["job_slot"],
            run_id=owner["service_run"], mode="write", ttl_seconds=10))
        self.leases[context] = grant
        self.runs[context] = run
        self.bridges[context] = ResultBridge(run,
            lambda event, c=context: self.store.publish(event, self.epochs[c]), self.store.confirmed)

    async def renew(self):
        try:
            while True:
                await asyncio.sleep(0.25)
                for context, lease in list(self.leases.items()):
                    async with self.store.sessions.begin() as session:
                        await self.store.fence(session, context, self.epochs[context])
                        await self.lease_manager.renew(lease.lease_id, lease.fencing_token, ttl_seconds=10)
                    self.renewals += 1
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._heartbeat_error = error
            # The old service cannot keep dispatching after it loses custody.
            await self.client.pause_dispatch()
            for context, run in self.runs.items():
                for action in await self.store.actions(context, pending=False):
                    if action["kind"] == "submit":
                        await run.cancel(self.adapter.job_id(action["id"], action["operation"]))

    async def admit(self, task):
        if self._heartbeat_error:
            raise PermissionError("job service lease renewal failed") from self._heartbeat_error
        context = task["owner"]["context_id"]
        owner, lease = self.owners[context], self.leases[context]
        root = Path(owner["job_root"])
        if Path(task["python"]["args"]["root"]).resolve() != root.resolve():
            raise PermissionError("job workspace does not match owner")

        async def mode():
            async with self.store.sessions.begin() as session:
                await self.store.fence(session, context, self.epochs[context])
                loop = await session.get(AgentLoop, owner["loop_id"])
                grant = await session.scalar(select(LoopDelegationGrant).where(LoopDelegationGrant.loop_id == loop.loop_id,
                    LoopDelegationGrant.revision == loop.authority_revision, LoopDelegationGrant.status == "active"))
                from datetime import UTC, datetime
                if loop.status != "running" or grant is None or context not in grant.context_scope or (
                    grant.expires_at is not None and grant.expires_at <= datetime.now(UTC)):
                    raise PermissionError("current Loop delegation no longer permits background jobs")
                return AccessMode.WORKSPACE_WRITE if "write" in grant.permission_scope else AccessMode.READ_ONLY

        async def guard(lease_id, token):
            await self.lease_manager.assert_valid(lease_id, token)
            self.checks.append({"context_id": context, "lease_id": lease_id, "token": token, "job_id": task["id"]})

        security = derive_security_context(ExecutionProfile(
            AuthorizationIdentity(root, (root,), ("read", "write"), AccessMode.WORKSPACE_WRITE, "main:" + context),
            RoutingIdentity(owner["thread_id"], owner["job_workspace"], "main:" + context, context, "", owner["service_run"])))
        security = replace(security, extras={**security.extras, "workspace_lease": lease.model_dump(),
            "workspace_lease_guard": guard, "session_mode_resolver": mode})

        @tool
        def descriptor(root: str, operation: str) -> str:
            """Execute the registered local leaf operation in its own leased workspace."""
            raise AssertionError("descriptor cannot execute caller code")

        descriptor = declare_effect(descriptor, structured_fs(lambda args, current:
            ResolvedFsEffect(writes=(Path(args["root"]),))))
        call = {"id": task["id"], "name": "start_local", "args": task["python"]["args"], "type": "tool_call"}
        runtime = ToolRuntime(state={}, context=security.to_runtime_context(), config={"configurable": {"thread_id": owner["thread_id"]}},
            stream_writer=lambda value: None, tool_call_id=call["id"], store=None)
        request = ToolCallRequest(tool_call=call, tool=descriptor, state={}, runtime=runtime)

        async def approved(request):
            return ToolMessage(content="admitted", tool_call_id=call["id"])

        result = await AccessPolicyMiddleware().awrap_tool_call(request, approved)
        if result.status == "error":
            raise PermissionError(result.content)
        return True

    async def apply(self, context, action, *, restore=False):
        started_ns = time.time_ns()
        run = self.runs[context]
        if action["kind"] != "submit":
            raise ValueError("unsupported durable action")
        revision = action["revision"]
        if revision > run.revision:
            owner = self.owners[context]
            owner["revision"] = revision
            await self.store.save_owner(owner)
            await run.revise(revision)
        for job in action.get("cancel", []):
            await run.cancel(job)
        job = self.adapter.job_id(action["id"], action["operation"])
        try:
            status = await run.status(job)
        except RemoteRejected:
            status = None
        if status is None or status["state"] == "queued":
            registered_run = Run(self.client, dict(run.owner), revision)
            receipt = await self.adapter.submit(registered_run, parent_message_id=action["id"], call_id=action["operation"],
                name="start_local", args={"root": self.owners[context]["job_root"], "operation": action["operation"]})
            if json.loads(receipt["content"])["state"] != "accepted":
                raise RuntimeError(receipt)
        elif status["state"] == "unknown":
            raise RuntimeError("unknown execution remains quarantined; operator reconciliation required")
        if not restore:
            await self.store.action_done(context, action["id"], self.epochs[context])
        self.action_trace.append({"context_id": context, "action_id": action["id"], "job_id": job,
            "revision": revision, "restore": restore, "started_ns": started_ns, "accepted_ns": time.time_ns()})
        return {"role": "tool", "tool_call_id": action["operation"], "name": "start_local",
            "content": json.dumps({"job_id": job, "state": "accepted"})}

    async def restore(self):
        # Completed outbox rows can still refer to queued runtime jobs. Rebuild
        # their exact trusted registrations before releasing the startup gate.
        for context in self.owners:
            for action in await self.store.actions(context, pending=False):
                await self.apply(context, action, restore=True)
        await self.adapter.resume_dispatch()

    async def pump(self, context, before_publish=None):
        bridge = self.bridges[context]
        if before_publish is not None:
            event = await bridge.run.next_fact()
            await before_publish(event)
            bridge.run.requeue_fact(event)
        while True:
            event = await bridge.deliver_next()
            self.published.append({"context_id": context, "sequence": event["sequence"], "published_ns": time.time_ns()})

    def start_pumps(self):
        self._pumps = [asyncio.create_task(self.pump(context)) for context in self.owners]

    async def ack_ready(self, context):
        for event in await self.store.facts(context, unacked=True):
            if not await self.store.confirmed(event):
                break
            await self.bridges[context].ack_consumed(event)
            await self.store.acknowledged(event, self.epochs[context])

    async def close(self):
        for task in self._pumps:
            task.cancel()
        await asyncio.gather(*self._pumps, return_exceptions=True)
        if self._heartbeat:
            self._heartbeat.cancel()
            await asyncio.gather(self._heartbeat, return_exceptions=True)
        # Graceful close waits for actual process exit, but unknown jobs do not
        # prove cleanup and their workspace leases must never be released here.
        states = {}
        for context, run in self.runs.items():
            states[context] = []
            for action in await self.store.actions(context, pending=False):
                job = self.adapter.job_id(action["id"], action["operation"])
                try:
                    status = (await run.status(job))["state"]
                    if status in {"queued", "running", "cancel_requested"}:
                        await run.cancel(job)
                        deadline = time.monotonic() + 5
                        while time.monotonic() < deadline:
                            status = (await run.status(job))["state"]
                            if status not in {"queued", "running", "cancel_requested"}:
                                break
                            await asyncio.sleep(0.02)
                    states[context].append(status)
                except RemoteRejected:
                    pass
        stats = await self.client.close()
        for context, lease in self.leases.items():
            if any(state not in {"completed", "failed", "cancelled", "skipped"} for state in states[context]):
                continue
            async with self.store.sessions.begin() as session:
                await self.store.fence(session, context, self.epochs[context])
                await self.lease_manager.release(lease.lease_id, lease.fencing_token)
        return stats
