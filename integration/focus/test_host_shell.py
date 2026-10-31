"""Latest native ToolRuntime/Shell vertical slice; no model or desktop DB.

Run with the existing Focus environment and a 0349f562 checkout. PostgreSQL is
an isolated cached container. This is not the old 340bf2b8 Patrol fixture.
"""
import argparse
import asyncio
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
FOCUS_COMMIT = "0349f562b488eb06647a1204634e067903e6132b"


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE).strip()


def assert_exited(pid):
    if os.name != "nt":
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        raise AssertionError(f"process {pid} still exists")
    import ctypes
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
    kernel.OpenProcess.restype = ctypes.c_void_p
    kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    handle = kernel.OpenProcess(0x100000, False, pid)
    if handle:
        try:
            assert kernel.WaitForSingleObject(handle, 0) == 0, f"process {pid} alive"
        finally:
            kernel.CloseHandle(handle)
    else:
        assert ctypes.get_last_error() == 87


async def exercise(args, dsn, root, binary):
    os.environ["FOCUS_DATABASE_URL"] = dsn.replace("postgresql://", "postgresql+asyncpg://")
    sys.dont_write_bytecode = True
    if args.context_index_root:
        sys.path.insert(0, str(args.context_index_root/"src"))
    sys.path[:0] = [str(ROOT/"python"), str(args.focus_root), str(args.focus_root/"backend/packages/harness")]
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    import backend.app.desktop.persistence.registry
    from backend.app.desktop.persistence.models import DesktopWorkspace, DesktopThread, DesktopRun, ToolExecutionAttempt
    from backend.app.desktop.runs.execution_attempts.tool import ToolExecutionLedger
    from backend.app.desktop.workspaces.coordination import WorkspaceLeaseManager, WorkspaceLeaseRequest, WorkspaceSlot
    from focus.persistence.base import Base
    from focus.config.app_config import AppConfig
    from focus.config.database_config import DatabaseConfig
    from focus.runtime.checkpointer import create_checkpointer, dispose_checkpointer
    from focus.sandbox.recovery import ShellRecoveryEvidence, windows_boot
    from focus.sandbox import WindowsAclBackend
    from focus.security.context import AuthorizationIdentity, RoutingIdentity, ExecutionProfile, derive_security_context
    from focus.security.policy import AccessMode
    from focus.tools.builtins import workspace_tools
    from focus.history.bridge import synchronize_items
    from langchain.tools import ToolRuntime
    from langchain.tools.tool_node import ToolCallRequest
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
    from langgraph.checkpoint.base import empty_checkpoint
    from focus_tool import RuntimeClient, FocusToolAdapter, ToolDefinition
    from host_shell import NativeShellHost

    engine = create_async_engine(os.environ["FOCUS_DATABASE_URL"])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        tables = [table for name, table in Base.metadata.tables.items()
                  if not name.startswith("checkpoint") and name not in {"store", "store_vectors"}]
        await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=tables))
    saver = await create_checkpointer(AppConfig(models=[], database=DatabaseConfig(backend="postgres", url=os.environ["FOCUS_DATABASE_URL"])))
    await saver.setup()
    evidence = ShellRecoveryEvidence(root/"receipts")
    ledger = ToolExecutionLedger(sessions, shell_evidence=evidence)
    leases = WorkspaceLeaseManager(sessions)
    requests, seeds, invocations = {}, {}, []
    backend = WindowsAclBackend(prepared_registry_path=root/"prepared.json", temp_root=root)
    original_backend = workspace_tools._SHELL_BACKEND
    workspace_tools._SHELL_BACKEND = backend  # Real native backend, isolated registry.
    client = await RuntimeClient.start([str(binary)], journal=root/"wal", concurrency=2, capacity=32)

    def resolve(task):
        args.host_invoked = True
        invocations.append(task["id"])
        return requests[task["id"]]

    adapter = FocusToolAdapter(client, {"native_shell_job": NativeShellHost(resolve).definition()}, workspace=root)
    reports = args.observed
    shell = workspace_tools.powershell if os.name == "nt" else workspace_tools.sh

    async def seed(label, script, *, selected=shell):
        identity = uuid.uuid4().hex
        path = root/label
        path.mkdir()
        async with sessions.begin() as session:
            session.add(DesktopWorkspace(workspace_id=identity, path=str(path), display_name=label))
            await session.flush()
            session.add(DesktopThread(task_id=identity, thread_id=identity, workspace_id=identity, title=label))
            await session.flush()
            session.add(DesktopRun(run_id=identity, task_id=identity, agent_id="main", kind="main", status="running", execution_thread_id=identity))
            session.add(WorkspaceSlot(slot_id=identity, workspace_id=identity, kind="authoritative", root_path=str(path), current_fingerprint="0"*64))
        lease = await leases.acquire(WorkspaceLeaseRequest(slot_id=identity, run_id=identity, mode="write"))
        async with sessions.begin() as session:
            row = await session.get(DesktopRun, identity)
            row.workspace_anchor = {"slot_id": identity, "lease_id": lease.lease_id, "mode": "write"}
        checks = []
        async def guard(lease_id, fencing_token):
            checks.append(lease_id)
            await leases.assert_valid(lease_id, fencing_token)
        security = derive_security_context(ExecutionProfile(
            AuthorizationIdentity(path, (path,), ("read", "write", "host_command"), AccessMode.FULL, "main"),
            RoutingIdentity(identity, identity, "main", identity, "", identity)))
        security = replace(security, extras={**security.extras, "tool_execution_ledger": ledger,
            "workspace_lease": lease.model_dump(), "workspace_lease_guard": guard})
        call = {"name": selected.name, "args": {"command": script}, "id": "shell-"+identity, "type": "tool_call"}
        parent = "parent-"+identity
        ai = AIMessage(id=parent, content="", tool_calls=[call])
        runtime = ToolRuntime(state={"messages": [HumanMessage(content="local deterministic shell test", id="human"), ai]},
            context=security.to_runtime_context(), config={}, stream_writer=None,
            tool_call_id=call["id"], store=None, tools=[])
        job = adapter.job_id(parent, call["id"])
        requests[job] = ToolCallRequest(tool_call=call, tool=selected, state=runtime.state, runtime=runtime)
        run = await client.open_run(identity, identity)
        spec = {"run": run, "job": job, "parent": parent, "call": call, "lease": lease,
                "path": path, "checks": checks, "ai": ai, "label": label, "released": False}
        seeds[label] = spec
        return spec

    async def submit(spec):
        await adapter.submit(spec["run"], parent_message_id=spec["parent"], call_id=spec["call"]["id"],
            name="native_shell_job", args=spec["call"]["args"])

    async def consume(spec, state, *, launched=True):
        fact = await asyncio.wait_for(spec["run"].next_fact(), 30)
        assert fact["result"]["state"] == state, fact
        host = fact["result"]["host"]
        assert host["process_cleanup"] in {"confirmed", "not_started"}
        assert spec["checks"], "native execution-time lease guard not called"
        message = ToolMessage.model_validate(host["tool_message"])
        assert message.tool_call_id == spec["call"]["id"]
        native = host["facts"]
        assert native["ledger_claims"] == (1 if launched else 0)
        assert native["ledger_completions"] == (1 if launched else 0)
        # Actual native typed checkpoint precedes runtime consumption ack.
        checkpoint = empty_checkpoint()
        messages = [spec["ai"], message]
        checkpoint["channel_values"] = {"messages": messages, "execution_items": synchronize_items([], messages)}
        checkpoint["channel_versions"] = {"messages": "1", "execution_items": "1"}
        config = await saver.aput({"configurable": {"thread_id": spec["run"].owner["context_id"], "checkpoint_ns": ""}},
            checkpoint, {"source": "input", "step": 0, "parents": {}}, checkpoint["channel_versions"])
        saved = await saver.aget_tuple(config)
        assert saved and saved.checkpoint["channel_values"]["execution_items"]
        restored = saved.checkpoint["channel_values"]["messages"][-1]
        assert restored.content == message.content and restored.tool_call_id == message.tool_call_id
        await spec["run"].ack(fact)
        proof = None
        if launched:
            recovery = native["recovery_identity"]
            raw = json.loads(evidence.path(recovery).read_text(encoding="utf-8"))
            assert all(raw["execution"][key] == value for key, value in recovery.items())
            verified = evidence.verify(raw["execution"], current_boot=windows_boot())
            proof = {"kind": verified["kind"], "job_drained": raw.get("jobDrained"),
                     "process_group_drained": raw.get("processGroupDrained"), "receipt_id": recovery["receipt_id"]}
            async with sessions() as session:
                row = await session.get(ToolExecutionAttempt, native["ledger_identity"])
                owner = await session.get(DesktopRun, spec["run"].owner["run_id"])
                assert row.status == "completed" and row.result is not None
                assert not owner.workspace_result["shell_cleanup"]
        reports.append({"case": spec["label"], "result": fact["result"],
            "completion": fact, "runtime_ack": True,
            "checkpoint_id": config["configurable"]["checkpoint_id"], "proof": proof,
            "lease_checks": len(spec["checks"]), "passed": True})
        return fact

    try:
        success = await seed("completed", "echo native host ok" if os.name == "nt" else "printf 'native host ok'",
                             selected=workspace_tools.cmd if os.name == "nt" else shell)
        await submit(success)
        await consume(success, "completed")
        before = list(invocations)
        await submit(success)
        assert invocations == before, "terminal retry executed native tool twice"
        failed = await seed("exit_nonzero", "exit 7")
        await submit(failed)
        await consume(failed, "failed")

        # Real native Shell launches an actual parent+child, then cancellation
        # must reap both before Go releases the opaque global Shell resource.
        long_root = root/"cancel_tree"
        payload = root/"tree.py"
        payload.write_text("import os,sys,time,subprocess\nfrom pathlib import Path\nr=Path.cwd()\n"
            "(r/'parent.pid').write_text(str(os.getpid()))\n"
            "subprocess.Popen([sys.executable,'-c',\"import os,time; from pathlib import Path; Path('child.pid').write_text(str(os.getpid())); time.sleep(60)\"])\n"
            "time.sleep(60)\n", encoding="utf-8")
        argv = [sys.executable, "-B", str(payload)]
        script = ("& " + " ".join("'"+value.replace("'", "''")+"'" for value in argv)) if os.name == "nt" else shlex.join(argv)
        long = await seed("cancel_tree", script)
        await submit(long)
        async with asyncio.timeout(25):
            while not all((long_root/name).exists() for name in ("parent.pid", "child.pid")):
                await asyncio.sleep(0.02)
        pids = [int((long_root/name).read_text()) for name in ("parent.pid", "child.pid")]
        denied = await seed("revoked_while_queued", "'must not execute'" if os.name == "nt" else "echo must-not-execute")
        await submit(denied)
        assert (await denied["run"].status(denied["job"]))["state"] == "queued"
        await leases.release(denied["lease"].lease_id, denied["lease"].fencing_token)
        denied["released"] = True
        await long["run"].cancel(long["job"])
        await consume(long, "cancelled")
        for pid in pids:
            assert_exited(pid)
        reports[-1]["verified_exited_pids"] = pids
        denied_fact = await asyncio.wait_for(denied["run"].next_fact(), 15)
        assert denied_fact["result"]["state"] == "failed", denied_fact
        assert denied_fact["result"]["host"]["process_cleanup"] == "not_started"
        assert denied_fact["result"]["host"]["facts"]["ledger_claims"] == 0
        reports.append({"case": "revoked_while_queued", "result": denied_fact["result"],
            "completion": denied_fact, "runtime_ack": False, "passed": True})
        return reports
    finally:
        failure, errors = sys.exc_info()[1], []
        try:
            stats = await client.close()
            args.cleanup_uncertain = bool(stats and stats["unknown_fences"])
        except Exception as error:
            args.cleanup_uncertain = True
            errors.append(error)
        for spec in seeds.values():
            try:
                async with sessions() as session:
                    row = await session.get(DesktopRun, spec["run"].owner["run_id"])
                    pending = (row.workspace_result or {}).get("shell_cleanup")
                if pending or args.cleanup_uncertain:
                    args.cleanup_uncertain = True
                    continue  # Never release a workspace whose cleanup is unknown.
                if not spec["released"]:
                    await leases.release(spec["lease"].lease_id, spec["lease"].fencing_token)
            except Exception as error:
                args.cleanup_uncertain = True
                errors.append(error)
        workspace_tools._SHELL_BACKEND = original_backend
        try:
            warnings = backend.close()
            if warnings:
                args.cleanup_uncertain = True
                errors.append(RuntimeError(str(warnings)))
        except Exception as error:
            errors.append(error)
        for close in (lambda: dispose_checkpointer(saver), engine.dispose):
            try:
                await close()
            except Exception as error:
                errors.append(error)
        if errors:
            if failure:
                failure.add_note("Cleanup failures: " + repr(errors))
            else:
                raise ExceptionGroup("native Shell cleanup failures", errors)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--focus-root", type=Path, required=True)
    parser.add_argument("--context-index-root", type=Path, help="existing checkout of Focus's pinned optional desktop dependency")
    parser.add_argument("--out", type=Path, default=ROOT/"results/local/native-host-shell.json")
    args = parser.parse_args()
    args.focus_root = args.focus_root.resolve()
    assert command("git", "-C", str(args.focus_root), "rev-parse", "HEAD") == FOCUS_COMMIT
    if args.context_index_root:
        assert command("git", "-C", str(args.context_index_root), "rev-parse", "HEAD") == "af8e65658d9c5351ae5ca0d68ec2ac094f5f240c"
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    name = "focus-host-shell-" + uuid.uuid4().hex[:12]
    args.observed, args.host_invoked, args.cleanup_uncertain = [], False, False
    started = False
    root = None
    passed = False
    try:
        command("docker", "run", "-d", "--rm", "--pull", "never", "--name", name,
            "-p", "127.0.0.1::5432", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "postgres:17-alpine")
        started = True
        port = command("docker", "port", name, "5432/tcp").rsplit(":", 1)[1]
        for _ in range(60):
            if subprocess.run(["docker", "exec", name, "pg_isready", "-U", "postgres"], capture_output=True).returncode == 0:
                break
            time.sleep(0.25)
        root = Path(tempfile.mkdtemp(prefix="focus-native-host-"))
        binary = root/("runtime.exe" if os.name == "nt" else "runtime")
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/runtime"], cwd=ROOT, check=True)
        cases = asyncio.run(exercise(args, "postgresql://postgres@127.0.0.1:"+port+"/postgres", root, binary))
        assert not args.cleanup_uncertain
        report = {"passed": True, "focus_commit": FOCUS_COMMIT, "decision_source": "local deterministic ToolRuntime requests; no model API",
            "cases": cases, "platform": sys.platform, "context_index_commit": "af8e65658d9c5351ae5ca0d68ec2ac094f5f240c",
            "scope": "native host Shell vertical slice, not production Agent/Patrol middleware integration"}
        paths = [path for directory in (ROOT/"runtime", ROOT/"cmd/runtime", ROOT/"python/focus_tool")
                 for path in directory.rglob("*") if path.suffix in {".py", ".go"}]
        paths.extend([ROOT/"integration/focus/host_shell.py", Path(__file__)])
        report["source_sha256"] = {path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2, ensure_ascii=False)+"\n", encoding="utf-8")
        print(json.dumps({"passed": True, "cases": [case["case"] for case in cases]}))
        passed = True
    except BaseException as error:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps({"passed": False, "error": repr(error), "cases": args.observed,
            "focus_commit": FOCUS_COMMIT, "cleanup_uncertain": args.cleanup_uncertain,
            "retained_workspace": str(root) if args.host_invoked else None,
            "retained_container": name if args.host_invoked else None}, indent=2, ensure_ascii=False)+"\n", encoding="utf-8")
        raise
    finally:
        if started and (passed or not args.host_invoked):
            subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if root and (passed or not args.host_invoked):
            expected = Path(tempfile.gettempdir()).resolve()
            if root.resolve().parent != expected or not root.name.startswith("focus-native-host-"):
                raise RuntimeError("refuse cleanup outside the test workspace")
            shutil.rmtree(root)


if __name__ == "__main__":
    main()
