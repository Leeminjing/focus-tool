"""Actual Focus Patrol admission/dispatch/worker with a local deterministic agent.

Only the Desktop launch adapter and decision provider are experimental ports.
Patrol, Kernel, ContextRunPool, coordinator, binder and Run worker are unchanged.
"""
import asyncio
from datetime import UTC, datetime
import json
from pathlib import Path
from types import SimpleNamespace
import uuid
import time
from contextlib import contextmanager

from langchain.agents.middleware.types import ModelRequest
from langchain.tools import tool
from langchain.tools.tool_node import ToolCallRequest, ToolRuntime
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.config import get_config
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from sqlalchemy import select
from typing_extensions import Annotated, TypedDict

from backend.app.desktop.agents.inbox import AgentInbox, DurableInboxMiddleware
from backend.app.desktop.contexts.evolution import ContextRevisionRepository
from backend.app.desktop.contexts.evolution.schemas import (ContextRevisionRef, ContextRevisionContract,
    ContextRevisionOriginKind, ContextRevisionPayloadMode, ContextRevisionProjectionStatus)
from backend.app.desktop.persistence.models import DesktopWorkspace, DesktopThread, DesktopRun
from backend.app.desktop.patrol.agent_loop.models import AgentLoop, LoopRound
from backend.app.desktop.patrol.agent_loop.service import AgentLoopService
from backend.app.desktop.patrol.agent_loop.schemas import LoopCreateRequest, PatrolDecisionIntent
from backend.app.desktop.patrol.agent_loop.coordinator import LoopCoordinator
from backend.app.desktop.patrol.agent_loop.execution.dispatch import LoopRunWorkspaceBinder, LoopWaveDispatcher
from backend.app.desktop.patrol.agent_loop.execution.run_execution import LoopRunExecutionBoundary
from backend.app.desktop.patrol.agent_loop.resources.context_run_pool import ContextRunPool
from backend.app.desktop.patrol.agent_loop.observations.observation_capture import LoopObservationService
from backend.app.desktop.patrol.agent_loop.patrol import PortfolioPatrol
from backend.app.desktop.patrol.agent_loop.kernel import LoopKernel
from backend.app.desktop.patrol.agent_loop.task_progress.runtime import TaskProgressRuntime
from backend.app.desktop.runs.orchestration.executor import RunExecutionResources, execute_prepared_run
from backend.app.desktop.runs.execution_attempts.tool import ToolExecutionLedger
from backend.app.desktop.workspaces.coordination import WorkspaceLeaseManager, WorkspaceSlot
from focus.history import content_hash
from focus.history.bridge import synchronize_items, replace_execution_items
from focus.runtime.runs.manager import RunManager
from focus.runtime.runs.worker import run_agent
from focus.runtime.stream_bridge.memory import MemoryStreamBridge
from focus.runtime.tool_attempts import ToolExecutionMiddleware
from focus.security.context import AuthorizationIdentity, RoutingIdentity, ExecutionProfile, derive_security_context
from focus.security.effects import declare_effect, NO_LOCAL_EFFECT
from focus.security.policy import AccessMode
from focus.security.middleware import AccessPolicyMiddleware

from patrol_store import identity, message_id, typed_inbox_ids


class State(TypedDict, total=False):
    messages: Annotated[list, add_messages]
    execution_items: Annotated[list, replace_execution_items]
    inbox_delivery: dict
    facts: list
    decisions: list
    actions: list


class LocalProgress:
    """Honest unknown assessments preserve evidence without claiming task success."""
    context_window_tokens = None
    max_output_tokens = 2048
    last_usage_reported = True
    last_usage = SimpleNamespace(model_calls=0, input_tokens=0, output_tokens=0)

    async def invoke(self, schema, system, payload):
        if isinstance(payload, str):
            payload = json.loads(payload)
        return schema(source_assessments=tuple({"source_key": item["source_key"], "disposition": "unknown",
            "explanation": "Deterministic integration retains this source; no semantic completion claim."}
            for item in payload["task_delta"]["sources"]))


class PatrolHarness:
    def __init__(self, sessions, saver, config, store, service, root):
        self.sessions, self.saver, self.config = sessions, saver, config
        self.store, self.service, self.root = store, service, root
        self.manager = RunManager()
        self.coordinator = LoopCoordinator(sessions)
        self.dispatcher = LoopWaveDispatcher(sessions, self.launch)
        self.pool = ContextRunPool(sessions, self.coordinator, self.dispatcher, concurrency=2)
        self.progress = TaskProgressRuntime(sessions, config, model_factory=lambda name: LocalProgress())
        self.revisions = ContextRevisionRepository()
        self.activations, self.records = [], []
        self.pending = []
        self.defer_actions = False
        self.wait_started = True
        self.trace = None
        self.progress_probe = None  # Optional observer; native methods stay authoritative.
        self.initial_operations = {}
        self.before_exit = None  # Local fixture can hold a live activation.
        self.after_inbox_snapshot = None
        self.context_modes = {}

    def emit(self, kind, **fields):
        if self.trace:
            try:
                self.trace({"kind": kind, "time_ns": time.time_ns(), **fields})
            except Exception:
                pass

    @contextmanager
    def phase(self, name, **identity):
        started, monotonic = time.time_ns(), time.perf_counter_ns()
        detail = {}
        try:
            yield detail
        except BaseException as error:
            detail["error_type"] = type(error).__name__
            raise
        finally:
            self.emit("patrol_phase", phase=name, started_ns=started, finished_ns=time.time_ns(),
                elapsed_ns=time.perf_counter_ns() - monotonic, **identity, **detail)

    def security(self, owner, run_id, equipment=None):
        root = Path(owner["activation_root"])
        equipment = equipment or {"permissions": ["read", "write"], "access_mode": "workspace-write"}
        return derive_security_context(ExecutionProfile(
            AuthorizationIdentity(root, (root,), tuple(equipment["permissions"]), AccessMode(equipment["access_mode"]), "main:" + owner["context_id"]),
            RoutingIdentity(owner["thread_id"], owner["workspace_id"], "main:" + owner["context_id"],
                owner["context_id"], "", run_id))).to_runtime_context()

    async def publish_revision(self, context, checkpoint, values, origin, run_id):
        async with self.sessions.begin() as session:
            old = await self.revisions.current(session, context)
            ref = ContextRevisionRef(context_id=context, revision_id=uuid.uuid4().hex,
                generation=(old.ref.generation + 1) if old else 1,
                execution_thread_id=checkpoint["configurable"]["thread_id"], checkpoint_ns="",
                checkpoint_id=checkpoint["configurable"]["checkpoint_id"], payload_mode=ContextRevisionPayloadMode.CHECKPOINT)
            await self.revisions.insert(session, ContextRevisionContract(ref=ref,
                content_hash=content_hash(values.get("execution_items", [])), projection_status=ContextRevisionProjectionStatus.VALID,
                origin_kind=origin, origin_id=run_id, created_at=datetime.now(UTC)))
            await self.revisions.switch_current(session, ref, old.ref if old else None)
        return ref

    async def bootstrap(self, label, *, join=None):
        context, workspace, initial, job_workspace, job_slot, service_run = (uuid.uuid4().hex for _ in range(6))
        activation_root = Path(join["activation_root"]) if join else self.root / label / "activation"
        workspace = join["workspace_id"] if join else workspace
        job_root = self.root / label / "jobs"
        activation_root.mkdir(parents=True, exist_ok=True)
        job_root.mkdir(parents=True)
        owner = {"label": label, "context_id": context, "workspace_id": workspace,
            "thread_id": "patrol-" + context, "logical_run": uuid.uuid4().hex, "service_run": service_run,
            "job_workspace": job_workspace, "job_slot": job_slot, "job_root": str(job_root),
            "activation_root": str(activation_root), "revision": 1}
        async with self.sessions.begin() as session:
            if not join:
                session.add(DesktopWorkspace(workspace_id=workspace, path=str(activation_root), display_name=label))
            session.add(DesktopWorkspace(workspace_id=job_workspace, path=str(job_root), display_name=label + " tool service"))
            await session.flush()
            session.add(DesktopThread(task_id=context, thread_id=owner["thread_id"], workspace_id=workspace, title=label))
            await session.flush()
            session.add(DesktopRun(run_id=initial, task_id=context, agent_id="main:" + context, kind="main", status="running",
                origin="direct_user", execution_thread_id=owner["thread_id"]))
            session.add(DesktopRun(run_id=service_run, task_id=context, agent_id="tool-service:" + context,
                kind="tool_service", status="running", origin="tool_runtime"))
            session.add(WorkspaceSlot(slot_id=job_slot, workspace_id=job_workspace, kind="authoritative",
                root_path=str(job_root), current_fingerprint="0" * 64))

        async def begin(state):
            messages = [*state["messages"], AIMessage(id=uuid.uuid4().hex, content="Local cross-activation research initialized.")]
            return {"messages": messages[-1:], "execution_items": synchronize_items(None, messages)}

        builder = StateGraph(State, context_schema=dict)
        builder.add_node("bootstrap", begin)
        builder.add_edge(START, "bootstrap")
        builder.add_edge("bootstrap", END)
        graph = builder.compile(checkpointer=self.saver)

        async def factory():
            return graph

        record = self.manager.create(owner["thread_id"], run_id=initial)
        await run_agent(record=record, bridge=MemoryStreamBridge(), run_manager=self.manager, app_config=self.config,
            graph_input={"messages": [HumanMessage(content="Initialize local tool research.")]},
            runnable_config={"configurable": {"thread_id": owner["thread_id"]}}, stream_modes=["values"],
            langgraph_context=self.security(owner, initial), agent_factory=factory, checkpointer=self.saver)
        assert record.status.value == "success", record.error
        state = await graph.aget_state({"configurable": {"thread_id": owner["thread_id"]}})
        ref = await self.publish_revision(context, state.config, state.values, ContextRevisionOriginKind.ROOT, initial)
        async with self.sessions.begin() as session:
            row = await session.get(DesktopRun, initial)
            row.status, row.settled_at, row.context_revision_id = "success", datetime.now(UTC), ref.revision_id
        if join:
            # Trusted scenario bootstrap, before any Patrol decision. Actual
            # Kernel/admission/dispatch still validate this persisted scope.
            from backend.app.desktop.contexts.curation.models import CurationLane
            from backend.app.desktop.patrol.agent_loop.models import LoopContextMembership, LoopDelegationGrant
            from backend.app.desktop.patrol.agent_loop.rounds.lifecycle import current_frontier_hash
            async with self.sessions.begin() as session:
                loop = await session.get(AgentLoop, join["loop_id"], with_for_update=True)
                grant = await session.scalar(select(LoopDelegationGrant).where(LoopDelegationGrant.loop_id == loop.loop_id,
                    LoopDelegationGrant.status == "active"))
                grant.context_scope = [*grant.context_scope, context]
                lane_id = uuid.uuid4().hex
                session.add(CurationLane(lane_id=lane_id, program_id=loop.program_id, managed_context_id=context,
                    purpose=label, normalized_purpose=label, lane_policy={"workspace_mode": "read_only"}))
                await session.flush()
                session.add(LoopContextMembership(membership_id=uuid.uuid4().hex, loop_id=loop.loop_id,
                    context_id=context, lane_id=lane_id, role="side", status="active"))
                await session.flush()
                round_row = await session.get(LoopRound, loop.current_round_id)
                round_row.frontier_hash = await current_frontier_hash(session, loop.loop_id)
            owner["loop_id"] = join["loop_id"]
        else:
            snapshot = await AgentLoopService(self.sessions).start(LoopCreateRequest(loop_id=uuid.uuid4().hex,
            workspace_id=workspace, initial_context_id=context, initial_run_id=initial, holder_id="tool-patrol",
            goal="Verify local tool facts across activations", task_contract="Use owned tool results only",
            acceptance_criteria=({"criterion_id": "artifact", "text": "Verified local artifact"},),
            capabilities=("continue_context", "request_completion"), context_scope=(context,), permission_scope=("read", "write"),
                equipment={"access_mode": "workspace-write"}))
            owner["loop_id"] = snapshot["loop_id"]
        await self.store.save_owner(owner)
        await self.service.attach(owner)
        return owner

    def action(self, owner, operation, revision=1, cancel=()):
        return {"id": owner["context_id"] + ":" + operation, "kind": "submit", "operation": operation,
            "revision": revision, "cancel": list(cancel)}

    async def submit(self, owner, actions, state, runtime):
        messages = list(state["messages"])
        from backend.app.desktop.patrol.agent_loop.execution.execution_ownership import RunOwnershipPolicy
        ledger = ToolExecutionLedger(self.sessions, authority_validator=RunOwnershipPolicy().assert_live)
        runtime.context["tool_execution_ledger"] = ledger
        for action in actions:
            ai = AIMessage(id=action["id"], content="", tool_calls=[{"id": action["operation"], "name": "start_local",
                "args": {"root": owner["job_root"], "operation": action["operation"]}, "type": "tool_call"}])
            @tool
            def start_local(root: str, operation: str) -> str:
                """Submit local background work; the receipt only acknowledges submission."""
                raise AssertionError("descriptor only")
            descriptor = declare_effect(start_local, NO_LOCAL_EFFECT)
            call = ai.tool_calls[0]
            current = {**state, "messages": [*messages, ai]}
            tr = ToolRuntime(state=current, context=runtime.context, config=get_config(), stream_writer=lambda value: None,
                tool_call_id=call["id"], store=None)
            request = ToolCallRequest(tool_call=call, tool=descriptor, state=current, runtime=tr)

            async def handler(request):
                return ToolMessage(**await self.service.apply(owner["context_id"], action))

            async def governed(request):
                return await ToolExecutionMiddleware().awrap_tool_call(request, handler)

            receipt = await AccessPolicyMiddleware().awrap_tool_call(request, governed)
            assert json.loads(receipt.content)["state"] == "accepted", receipt
            messages.extend([ai, receipt])
        return {"messages": messages[len(state["messages"]):], "execution_items": synchronize_items(state.get("execution_items"), messages)}

    async def graph(self, owner):
        middleware = DurableInboxMiddleware(AgentInbox(self.sessions, self.saver))

        async def prepare(state, runtime):
            updates = await middleware.abefore_model(state, runtime)
            items = synchronize_items(state.get("execution_items"), [*state["messages"], *updates.get("messages", [])])
            if self.after_inbox_snapshot:
                await self.after_inbox_snapshot(owner, state, updates)
            represented = typed_inbox_ids(items, owner["context_id"])
            # The native inbox has a page limit and its snapshot precedes this
            # query. Later/unpaged facts stay pending for another activation.
            facts = [event for event in await self.store.facts(owner["context_id"], unconsumed=True)
                if message_id(event) in represented]
            return {**updates, "facts": facts,
                "decisions": [], "actions": [],
                "execution_items": items}

        async def decide(state, runtime):
            actions, decisions = [], []
            existing = await self.store.actions(owner["context_id"], pending=False)
            if not existing:
                operations = ("check", "scan") if owner["label"] == "A" else (("independent",) if owner["label"] == "B" else ("recovery",))
                operations = self.initial_operations.get(owner["context_id"], operations)
                actions = [self.action(owner, operation) for operation in operations]
            for event in state["facts"]:
                operation = event["task"]["python"]["args"]["operation"]
                decision = "obsolete" if event["task"]["revision"] < self.service.runs[owner["context_id"]].revision else "consumed"
                if operation in {"check", "recovery"} and event["result"]["state"] == "completed":
                    scan = self.service.adapter.job_id(owner["context_id"] + ":scan", "scan")
                    actions.append(self.action(owner, "repair", 2, (scan,) if operation == "check" else ()))
                    decision = "redirect"
                decisions.append({"identity": list(identity(event)), "event_hash": content_hash(event), "decision": decision})

            async def local(request):
                return AIMessage(id=uuid.uuid4().hex, content=json.dumps({"decisions": decisions, "actions": actions}))

            message = await middleware.awrap_model_call(ModelRequest(model=None, messages=state["messages"], state=state, runtime=runtime), local)
            return {"messages": [message], "decisions": decisions, "actions": actions,
                "execution_items": synchronize_items(state.get("execution_items"), [*state["messages"], message])}

        async def commit(state, runtime):
            checkpoint_id = get_config()["configurable"]["checkpoint_map"][""]
            exact = {"configurable": {"thread_id": owner["thread_id"], "checkpoint_ns": "", "checkpoint_id": checkpoint_id}}
            if state["facts"]:
                await self.store.consume(state["facts"], exact, state["decisions"], state["actions"], self.service.epochs[owner["context_id"]])
            else:
                await self.store.add_actions(owner["context_id"], state["actions"], self.service.epochs[owner["context_id"]])
            if state["facts"] and self.defer_actions:
                return {}
            updates = await self.submit(owner, await self.store.actions(owner["context_id"]), state, runtime)
            if not state["facts"] and self.wait_started:
                # Wait only for leaf startup, never for its result. The gate is
                # released after this activation has completely exited.
                async with asyncio.timeout(10):
                    for action in state["actions"]:
                        path = Path(owner["job_root"]) / (action["operation"] + ".attempts")
                        while not path.exists():
                            await asyncio.sleep(0.01)
            if self.before_exit:
                await self.before_exit(owner, state)
            return updates

        builder = StateGraph(State, context_schema=dict)
        for name, function in (("prepare", prepare), ("decide", decide), ("commit", commit)):
            builder.add_node(name, function)
        builder.add_edge(START, "prepare")
        builder.add_edge("prepare", "decide")
        builder.add_edge("decide", "commit")
        builder.add_edge("commit", END)
        return builder.compile(checkpointer=self.saver)

    async def schedule(self, owner):
        async with asyncio.timeout(30):
            while not await self.schedule_many([owner]):
                await asyncio.sleep(0.05)

    async def schedule_many(self, owners):
        schedule_id = uuid.uuid4().hex
        with self.phase("schedule_total", schedule_id=schedule_id, loop_id=owners[0]["loop_id"],
                        context_ids=[owner["context_id"] for owner in owners]):
            return await self._schedule_many(owners, schedule_id)

    async def _schedule_many(self, owners, schedule_id):
        owner = owners[0]
        if any(item["loop_id"] != owner["loop_id"] for item in owners):
            raise ValueError("one Patrol decision must belong to one Loop")
        # Publish actual task-memory work with a deterministic evidence-preserving
        # provider, then freeze the real bounded observation for PortfolioPatrol.
        identity = {"schedule_id": schedule_id, "loop_id": owner["loop_id"],
                    "context_ids": [item["context_id"] for item in owners]}
        calls, work = 0, 0
        for iteration in range(1, 7):
            while True:
                calls += 1
                with self.phase("task_progress", **identity, iteration=iteration, call=calls) as detail:
                    if self.progress_probe is None:
                        processed = await self.progress.drain(loop_id=owner["loop_id"])
                    else:
                        processed = await self.progress_probe.drain(self.progress,
                            loop_id=owner["loop_id"], schedule_id=schedule_id)
                    detail["work_items"] = processed
                work += processed
                if not processed:
                    break
            with self.phase("round_maintenance", **identity, iteration=iteration):
                await self.coordinator.maintain_rounds()
            with self.phase("round_state_read", **identity, iteration=iteration):
                async with self.sessions() as session:
                    current_loop = await session.get(AgentLoop, owner["loop_id"])
                    current_round = await session.get(LoopRound, current_loop.current_round_id)
                    if current_round.status != "settled":
                        break
        self.emit("patrol_loops", **identity, round_iterations=iteration,
                  progress_drain_calls=calls, progress_work_items=work)
        if current_round.status == "settled":
            # Finite tools can make two Loops turn over concurrently. Never
            # capture the already-committed observation of a settled old Round.
            self.emit("patrol_deferred", **identity, reason="round_join_progress")
            return False
        with self.phase("round_identity_read", **identity):
            async with self.sessions() as session:
                loop = await session.get(AgentLoop, owner["loop_id"])
                round_id = loop.current_round_id
        with self.phase("observation_capture", **identity, round_id=round_id):
            observation = await LoopObservationService(self.sessions, self.saver).capture(owner["loop_id"], round_id)
        with self.phase("coordinator_claim", **identity, round_id=round_id):
            claim = await self.coordinator.claim_for_loop(owner["loop_id"], "local-patrol")
        if claim is not None and claim.round_id != round_id:
            with self.phase("coordinator_release", **identity, round_id=claim.round_id, reason="observation_changed"):
                await self.coordinator.release(claim)
            self.emit("patrol_deferred", **identity, reason="observation_changed")
            return False
        if claim is None:
            async with self.sessions() as session:
                loop = await session.get(AgentLoop, owner["loop_id"])
                round_row = await session.get(LoopRound, loop.current_round_id)
            if round_row.status == "settled" and loop.health == "progress_waiting":
                return False
            raise AssertionError({"round_status": round_row.status, "barrier": round_row.barrier,
                "health": loop.health, "waiting": loop.waiting_reason, "round_id": round_row.round_id})

        async def model(obs):
            async with self.sessions() as session:
                refs = {item["context_id"]: await self.revisions.current(session, item["context_id"]) for item in owners}
            return PatrolDecisionIntent(decision_id=uuid.uuid4().hex, idempotency_key="tool-patrol:" + obs.round_id,
                loop_id=obs.loop_id, loop_revision=obs.loop_revision, round_id=obs.round_id, holder_id="tool-patrol",
                grant_id=obs.grant["grant_id"], grant_revision=obs.authority_revision, goal_revision=obs.goal_revision,
                fencing_token=int(claim.fencing_token), observed_frontier_hash=obs.observed_frontier_hash,
                observed_workspace_revision=obs.workspace["revision"], observed_projection_sequence=obs.projection_sequence,
                base_entity_revisions=obs.base_entity_revisions, rationale="Durable owned tool facts require a successor activation.",
                actions=tuple({"action": "continue_context", "context_id": item["context_id"],
                    "context_revision_id": refs[item["context_id"]].ref.revision_id,
                    "message": "Submit or consume owned background work."} for item in owners),
                context_permissions=tuple({"context_id": item["context_id"],
                    "access_mode": self.context_modes.get(item["context_id"], "workspace-write") if item["context_id"] in refs else None}
                    for item in obs.portfolio_frontier))

        try:
            with self.phase("patrol_decision", **identity, round_id=round_id):
                proposal = await PortfolioPatrol(self.sessions, model).decide(observation, "tool-patrol", fencing_token=int(claim.fencing_token))
            with self.phase("kernel_commit", **identity, round_id=round_id):
                result = await LoopKernel(self.sessions, require_fencing=True).commit(proposal)
            assert result.status == "committed", result
        finally:
            with self.phase("coordinator_release", **identity, round_id=round_id):
                await self.coordinator.release(claim)
        return True

    async def drain(self):
        previous = len(self.activations)
        started = await self.pool.drain()
        assert started > 0, "Patrol produced no dispatchable work"
        tasks = [task for _, _, task in self.pool._tasks.values()]
        await asyncio.gather(*tasks)
        for task in tasks:
            assert task.exception() is None
        pending, self.pending = self.pending, []
        await asyncio.gather(*(self.finish(*item) for item in pending))
        if len(self.activations) == previous:
            from backend.app.desktop.patrol.agent_loop.models import LoopDirective
            async with self.sessions() as session:
                rows = list(await session.scalars(select(LoopDirective)))
            raise AssertionError([(row.status, row.queued_reason) for row in rows])

    async def launch(self, directive, message, slot_id):
        owner = self.service.owners[directive.target_context_id]
        run_id = uuid.uuid4().hex
        async with self.sessions.begin() as session:
            ref = await self.revisions.current(session, owner["context_id"])
            from backend.app.desktop.patrol.agent_loop.execution.directive_equipment import resolve_directive_equipment
            loop = await session.get(AgentLoop, directive.loop_id)
            equipment = await resolve_directive_equipment(session, loop, directive)
            row = DesktopRun(run_id=run_id, task_id=owner["context_id"], agent_id="main:" + owner["context_id"],
                kind="main", status="pending", origin="loop_delegated", execution_thread_id=owner["thread_id"],
                context_revision_id=ref.ref.revision_id, context_checkpoint_id=ref.ref.checkpoint_id,
                loop_id=directive.loop_id, round_id=directive.round_id, directive_id=directive.directive_id,
                origin_message_id=directive.message_id, action_id=directive.action_id,
                equipment=equipment)
            from backend.app.desktop.patrol.agent_loop.execution.execution_ownership import RunOwnershipPolicy
            await RunOwnershipPolicy().admit(session, row)
            session.add(row)
        context = self.security(owner, run_id, equipment)
        context.update({"run_id": run_id, "checkpoint_id": ref.ref.checkpoint_id})
        body = SimpleNamespace(context=context, input={"messages": [message]}, stream_mode=["values"], resume=None)
        _, lease = await LoopRunWorkspaceBinder(self.sessions).bind(run_id=run_id, loop_id=directive.loop_id,
            body=body, slot_id=slot_id, directive_id=directive.directive_id)
        graph = await self.graph(owner)

        async def factory():
            return graph

        assembly = SimpleNamespace(run_id=run_id, body=body, thread_id=owner["thread_id"], agent_factory=factory)
        resources = RunExecutionResources(bridge=MemoryStreamBridge(), run_manager=self.manager,
            checkpointer=self.saver, store=None, app_config=self.config)
        record = await LoopRunExecutionBoundary(self.sessions).start(assembly, resources, execute_prepared_run)
        self.emit("activation_started", loop_id=directive.loop_id, context_id=owner["context_id"], run_id=run_id)
        self.records.append(record)
        self.pending.append((owner, directive, record, graph, lease))
        return run_id

    async def finish(self, owner, directive, record, graph, lease):
        run_id = record.run_id
        try:
            await record.task
            await record.loop_activity_task
            final = await graph.aget_state({"configurable": {"thread_id": owner["thread_id"]}})
            status = record.status.value
            next_ref = await self.publish_revision(owner["context_id"], final.config, final.values,
                ContextRevisionOriginKind.RUN_SETTLED, run_id) if status == "success" else None
            async with self.sessions.begin() as session:
                row = await session.get(DesktopRun, run_id)
                row.status, row.settled_at, row.error = status, datetime.now(UTC), record.error
                await session.flush()
                await self.coordinator.handle_run_settled(SimpleNamespace(event_type="RunSettled", run_id=run_id,
                    event_id=uuid.uuid4().hex, payload={"context_revision": next_ref.model_dump(mode="json")} if next_ref else {}), session)
            self.activations.append({"context_id": owner["context_id"], "run_id": run_id, "checkpoint": final.config,
                "status": status, "error": record.error,
                "logical_run": owner["logical_run"], "exited_ns": time.time_ns(),
                "activation_lease_id": lease.lease_id, "activation_fencing_token": lease.fencing_token,
                "decisions": final.values.get("decisions", [])})
        finally:
            heartbeat = getattr(record, "workspace_lease_task", None)
            if heartbeat:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            await WorkspaceLeaseManager(self.sessions).release(lease.lease_id, lease.fencing_token)
        self.emit("activation_exited", loop_id=directive.loop_id, context_id=owner["context_id"], run_id=run_id)
        return {"context_id": owner["context_id"], "status": status, "error": record.error}

    async def close(self):
        await self.pool.close()
