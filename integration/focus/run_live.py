"""Exercise real Focus worker, security, leases, inbox and PostgreSQL checkpoints.

Run with the existing Focus harness Python environment, not the stdlib-only core
environment. No Focus sources or desktop database are modified. The decision
graph is deterministic local code; no model inference/provider API is involved.
"""
import argparse
import asyncio
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]


async def exercise(focus_root, binary, workspace, dsn):
    # Import only after the isolated database is configured. No dotenv/config
    # discovery or desktop application startup is used.
    os.environ["FOCUS_DATABASE_URL"] = dsn.replace("postgresql://", "postgresql+asyncpg://")
    sys.dont_write_bytecode = True
    sys.path[:0] = [str(ROOT / "python"), str(focus_root), str(focus_root / "backend/packages/harness")]
    from langchain.agents.middleware.types import ModelRequest
    from langchain.tools import tool
    from langchain.tools.tool_node import ToolCallRequest, ToolRuntime
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
    from langgraph.graph import END, START, StateGraph
    from langgraph.graph.message import add_messages
    from sqlalchemy import select
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from typing_extensions import Annotated, TypedDict

    import backend.app.desktop.persistence.registry
    from backend.app.desktop.agents.inbox import AgentInbox, DurableInboxMiddleware
    from backend.app.desktop.persistence.models import AgentMessage, AgentMessageDelivery, DesktopRun, DesktopThread, DesktopWorkspace, ToolExecutionAttempt
    from backend.app.desktop.runs.execution_attempts.tool import ToolExecutionLedger
    from backend.app.desktop.workspaces.coordination import WorkspaceLeaseManager, WorkspaceLeaseRequest, WorkspaceSlot
    from focus.config.app_config import AppConfig
    from focus.config.database_config import DatabaseConfig
    from focus.history import content_hash
    from focus.history.bridge import replace_execution_items, synchronize_items
    from focus.persistence.base import Base
    from focus.runtime.checkpointer import create_checkpointer, dispose_checkpointer
    from focus.runtime.runs.manager import RunManager
    from focus.runtime.runs.worker import run_agent
    from focus.runtime.stream_bridge.memory import MemoryStreamBridge
    from focus.security.context import AuthorizationIdentity, ExecutionProfile, RoutingIdentity, derive_security_context
    from focus.security.effects import NO_LOCAL_EFFECT, ResolvedFsEffect, declare_effect, structured_fs
    from focus.security.middleware import AccessPolicyMiddleware
    from focus.security.policy import AccessMode
    from focus.runtime.tool_attempts import ToolExecutionMiddleware
    from focus_tool import FocusToolAdapter, RuntimeClient, ToolDefinition

    engine = create_async_engine(os.environ["FOCUS_DATABASE_URL"])
    # This selected Run chain does not use semantic vector storage. Its dynamic
    # embedding column is initialized by separate migrations; checkpoint tables
    # are initialized by the actual saver below. All desktop tables keep their
    # real ORM definitions, constraints and foreign keys.
    desktop_tables = [table for name, table in Base.metadata.tables.items()
                      if not name.startswith("checkpoint") and name not in {"store", "store_vectors"}]
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=desktop_tables))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    config = AppConfig(models=[], database=DatabaseConfig(backend="postgres", url=os.environ["FOCUS_DATABASE_URL"]))
    saver = await create_checkpointer(config)
    await saver.setup()
    cases = []
    client = await RuntimeClient.start([str(binary)], journal=workspace / "runtime.jsonl", concurrency=2, capacity=16)
    try:
        for case in ("allowed", "permission_revoked", "lease_released"):
            case_root = workspace / case
            case_root.mkdir()
            (case_root / "integration_tool.py").write_text(
                "from pathlib import Path\n"
                "def write_output(path, content):\n"
                "    Path(path).write_text(content, encoding='utf-8')\n"
                "    return {'written': True, 'content': Path(path).read_text(encoding='utf-8')}\n",
                encoding="utf-8",
            )
            context_id, workspace_id, run_id, slot_id = (uuid.uuid4().hex for _ in range(4))
            thread_id = "focus-tool-" + context_id
            manager = RunManager()
            record = manager.create(thread_id, run_id=run_id)
            async with sessions.begin() as session:
                session.add(DesktopWorkspace(workspace_id=workspace_id, path=str(case_root), display_name="focus-tool integration"))
                await session.flush()
                session.add(DesktopThread(task_id=context_id, thread_id=thread_id, workspace_id=workspace_id, title="runtime integration"))
                await session.flush()
                session.add(DesktopRun(run_id=run_id, task_id=context_id, agent_id="main", kind="main", status="running", execution_thread_id=thread_id))
                session.add(WorkspaceSlot(slot_id=slot_id, workspace_id=workspace_id, kind="authoritative", root_path=str(case_root), current_fingerprint="0" * 64))
            leases = WorkspaceLeaseManager(sessions)
            grant = await leases.acquire(WorkspaceLeaseRequest(slot_id=slot_id, run_id=run_id, mode="write"))
            mode = AccessMode.WORKSPACE_WRITE
            guard_calls = []

            async def mode_resolver():
                return mode

            async def lease_guard(lease_id, fencing_token):
                guard_calls.append({"lease_id": lease_id, "fencing_token": fencing_token})
                return await leases.assert_valid(lease_id, fencing_token)

            security = derive_security_context(ExecutionProfile(
                AuthorizationIdentity(case_root, (case_root,), ("read", "write"), AccessMode.WORKSPACE_WRITE, "main"),
                RoutingIdentity(thread_id, workspace_id, "main", context_id, "", run_id),
            ))
            ledger = ToolExecutionLedger(sessions)
            security = replace(security, extras={**security.extras, "session_mode_resolver": mode_resolver,
                "workspace_lease": grant.model_dump(), "workspace_lease_guard": lease_guard, "tool_execution_ledger": ledger})
            context = security.to_runtime_context()
            runtime_run = await client.open_run(context_id, run_id)
            policy = AccessPolicyMiddleware()

            @tool
            def start_write(path: str, content: str) -> str:
                """Start a background file write; its ToolMessage is a submission receipt."""
                raise AssertionError("descriptor must not run in the Focus process")

            submission_tool = declare_effect(start_write, NO_LOCAL_EFFECT)
            execution_tool = declare_effect(start_write.model_copy(), structured_fs(
                lambda args, current: ResolvedFsEffect(writes=(Path(args["path"]).resolve(),))
            ))
            submitted_state = None

            def tool_request(tool_object, call, current):
                tool_runtime = ToolRuntime(state=submitted_state or {}, context=current,
                    config={"configurable": {"thread_id": thread_id}}, stream_writer=lambda value: None,
                    tool_call_id=call["id"], store=None)
                return ToolCallRequest(tool_call=call, tool=tool_object, state=submitted_state or {}, runtime=tool_runtime)

            async def admission(task):
                nonlocal mode
                # Mutate the REAL current source after submission, immediately
                # before the actual execution boundary rechecks it.
                if case == "permission_revoked":
                    mode = AccessMode.READ_ONLY
                elif case == "lease_released":
                    await leases.release(grant.lease_id, grant.fencing_token)
                call = {"id": task["id"], "name": "start_write", "args": task["python"]["args"], "type": "tool_call"}

                async def approved(request):
                    return ToolMessage(content="admitted", tool_call_id=call["id"])

                outcome = await policy.awrap_tool_call(tool_request(execution_tool, call, context), approved)
                if outcome.status == "error":
                    raise PermissionError(outcome.content)
                return True

            # A connection may carry several adapters over time; prior case is
            # already drained and closed before assigning the next registration.
            client.authorize = None
            adapter = FocusToolAdapter(client, {"start_write": ToolDefinition("python", module="integration_tool", function="write_output", interpreter=sys.executable)},
                                       workspace=case_root, before_execute=admission)
            inbox = AgentInbox(sessions, saver)
            inbox_middleware = DurableInboxMiddleware(inbox)
            event_wait = {}
            class State(TypedDict, total=False):
                messages: Annotated[list, add_messages]
                execution_items: Annotated[list, replace_execution_items]
                inbox_delivery: dict
                result_event: dict

            async def submit_node(state, runtime):
                nonlocal submitted_state
                ai = AIMessage(id="parent-" + run_id, content="", tool_calls=[{"id": "write", "name": "start_write",
                    "args": {"path": str(case_root / "output.txt"), "content": "Focus runtime result"}, "type": "tool_call"}])
                submitted_state = {**state, "messages": [*state["messages"], ai]}
                request = tool_request(submission_tool, ai.tool_calls[0], runtime.context)

                async def submit_handler(admitted):
                    messages = await adapter.submit_tool_calls(runtime_run, ai)
                    return ToolMessage(**messages[0])

                async def governed(admitted):
                    return await ToolExecutionMiddleware().awrap_tool_call(admitted, submit_handler)

                receipt = await policy.awrap_tool_call(request, governed)
                if json.loads(receipt.content)["state"] != "accepted":
                    raise AssertionError(receipt.content)
                messages = [*state["messages"], ai, receipt]
                return {"messages": [ai, receipt], "execution_items": synchronize_items(state.get("execution_items"), messages)}

            async def await_result(state):
                event_wait["waiting"] = time.perf_counter()
                event = await asyncio.wait_for(runtime_run.next_event(), 15)
                event_wait["awakened"] = time.perf_counter()
                message_id = content_hash([run_id, event["sequence"]])[:32]
                async with sessions.begin() as session:
                    await session.execute(insert(AgentMessage).values(message_id=message_id, task_id=context_id,
                        from_agent="tool-runtime", to_agent="main", kind="message", content=json.dumps(event)).on_conflict_do_nothing())
                event_wait["message_id"] = message_id
                return {"result_event": event}

            async def prepare_inbox(state, runtime):
                updates = await inbox_middleware.abefore_model(state, runtime)
                return {**updates, "execution_items": synchronize_items(state.get("execution_items"), [*state["messages"], *updates["messages"]])}

            async def decide(state, runtime):
                # The real middleware verifies the exact parent checkpoint's
                # typed inbox sources, then writes a durable DB delivery receipt.
                request = ModelRequest(model=None, messages=state["messages"], state=state, runtime=runtime)

                async def local_decision(request):
                    async with sessions() as session:
                        source = await session.get(AgentMessage, event_wait["message_id"])
                        assert source.read_at is not None, "decision ran before durable inbox receipt"
                    return AIMessage(id="decision-" + run_id, content="result consumed: " + state["result_event"]["result"]["state"])

                decision = await inbox_middleware.awrap_model_call(request, local_decision)
                await runtime_run.ack(state["result_event"])
                return {"messages": [decision], "execution_items": synchronize_items(state.get("execution_items"), [*state["messages"], decision])}

            builder = StateGraph(State, context_schema=dict)
            builder.add_node("submit", submit_node)
            builder.add_node("await_result", await_result)
            builder.add_node("prepare_inbox", prepare_inbox)
            builder.add_node("decide", decide)
            builder.add_edge(START, "submit")
            builder.add_edge("submit", "await_result")
            builder.add_edge("await_result", "prepare_inbox")
            builder.add_edge("prepare_inbox", "decide")
            builder.add_edge("decide", END)
            graph = builder.compile(checkpointer=saver)

            async def factory():
                return graph

            bridge = MemoryStreamBridge()
            await run_agent(record=record, bridge=bridge, run_manager=manager, app_config=config,
                graph_input={"messages": [HumanMessage(content="Write the local test artifact using the background runtime.")]},
                runnable_config={"configurable": {"thread_id": thread_id}}, stream_modes=["values"],
                langgraph_context=context, agent_factory=factory, checkpointer=saver)
            assert record.status.value == "success", record.error
            final = await graph.aget_state({"configurable": {"thread_id": thread_id}})
            event = final.values["result_event"]
            expected = "completed" if case == "allowed" else "failed"
            assert event["result"]["state"] == expected, event
            assert (case_root / "output.txt").exists() == (case == "allowed")
            if case == "allowed":
                assert (case_root / "output.txt").read_text(encoding="utf-8") == "Focus runtime result"
                assert event["result"].get("pid", 0) > 0
            else:
                assert not event["result"].get("pid"), "denied work reached a process"
                marker = "FILE_POLICY_DENIED" if case == "permission_revoked" else "workspace lease"
                assert marker in event["result"]["error"], event["result"]
            async with sessions() as session:
                receipts = (await session.scalars(select(AgentMessageDelivery).where(AgentMessageDelivery.message_id == event_wait["message_id"]))).all()
                attempts = (await session.scalars(select(ToolExecutionAttempt).where(ToolExecutionAttempt.run_id == run_id))).all()
            assert len(receipts) == 1 and len(attempts) == 1
            assert attempts[0].status == "completed"
            exact = await saver.aget_tuple({"configurable": {"thread_id": thread_id, "checkpoint_ns": "", "checkpoint_id": receipts[0].checkpoint_id}})
            assert exact is not None
            assert any(item["kind"] == "agent_collaboration" for item in exact.checkpoint["channel_values"]["execution_items"])
            assert len(guard_calls) >= 2, "lease was not rechecked at execution"
            cases.append({"case": case, "focus_run_id": run_id, "focus_status": record.status.value,
                "runtime_status": event["result"]["state"], "tool_error": event["result"].get("error"),
                "lease_checks": len(guard_calls), "checkpoint_id": receipts[0].checkpoint_id,
                "delivery_receipts": len(receipts), "submission_ledger_attempts": len(attempts),
                "event_wake_ms": (event_wait["awakened"] - event_wait["waiting"]) * 1000,
                "artifact_exists": (case_root / "output.txt").exists()})
            await runtime_run.close()
            if case != "lease_released":
                await leases.release(grant.lease_id, grant.fencing_token)
    finally:
        await client.close()
        await dispose_checkpointer(saver)
        await engine.dispose()
    return cases


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--focus-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "results/local/focus-integration.json")
    args = parser.parse_args()
    focus_root = args.focus_root.resolve()
    commit = command("git", "-C", str(focus_root), "rev-parse", "HEAD")
    container = "focus-tool-integration-" + uuid.uuid4().hex[:12]
    started = False
    try:
        command("docker", "run", "-d", "--rm", "--pull", "never", "--name", container,
                "-p", "127.0.0.1::5432", "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
                "-e", "POSTGRES_DB=focus_tool_integration", "postgres:17-alpine")
        started = True
        port = command("docker", "port", container, "5432/tcp").rsplit(":", 1)[1]
        for _ in range(60):
            ready = subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"], capture_output=True)
            if ready.returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("isolated PostgreSQL did not become ready")
        with tempfile.TemporaryDirectory(prefix="focus-live-integration-") as directory:
            workspace = Path(directory)
            binary = workspace / ("runtime.exe" if os.name == "nt" else "runtime")
            subprocess.run(["go", "build", "-o", str(binary), "./cmd/runtime"], cwd=ROOT, check=True)
            if sys.platform == "win32":
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            cases = asyncio.run(exercise(focus_root, binary, workspace, "postgresql://postgres@127.0.0.1:" + port + "/focus_tool_integration"))
        report = {"focus_commit": commit, "focus_root": str(focus_root), "python": sys.version,
                  "decision_source": "deterministic local StateGraph; no model inference", "cases": cases, "passed": True}
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(json.dumps({"focus_commit": commit, "passed": True, "cases": [case["case"] for case in cases]}))
        print("Integration record:", args.out)
    finally:
        if started:
            subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
