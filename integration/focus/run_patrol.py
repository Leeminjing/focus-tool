"""Reproduce actual Patrol cross-activation dispatch in an isolated local DB."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid
import hashlib
from datetime import UTC, datetime

ROOT = Path(__file__).resolve().parents[2]
FOCUS_COMMIT = "340bf2b8607dfb536d9376d400a02b122a4542bb"


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE).strip()


async def environment(args):
    os.environ["FOCUS_DATABASE_URL"] = os.environ["FOCUS_TOOL_TEST_DSN"].replace("postgresql://", "postgresql+asyncpg://")
    sys.dont_write_bytecode = True
    sys.path[:0] = [str(ROOT / "python"), str(args.focus_root), str(args.focus_root / "backend/packages/harness")]
    import backend.app.desktop.persistence.registry
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from focus.persistence.base import Base
    from focus.config.app_config import AppConfig
    from focus.config.database_config import DatabaseConfig
    from focus.runtime.checkpointer import create_checkpointer
    from patrol_store import BridgeStore
    from patrol_service import ToolService
    from patrol_activation import PatrolHarness

    engine = create_async_engine(os.environ["FOCUS_DATABASE_URL"])
    tables = [table for name, table in Base.metadata.tables.items() if not name.startswith("checkpoint") and name not in {"store", "store_vectors"}]
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=tables))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    config = AppConfig(models=[], database=DatabaseConfig(backend="postgres", url=os.environ["FOCUS_DATABASE_URL"]))
    saver = await create_checkpointer(config)
    await saver.setup()
    store = BridgeStore(sessions, saver)
    await store.setup()
    service = ToolService(store, args.binary, args.work)
    await service.start()
    harness = PatrolHarness(sessions, saver, config, store, service, args.work)
    return engine, saver, store, service, harness


async def until(predicate, description, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(0.02)
    raise TimeoutError(description)


async def normal(args):
    engine, saver, store, service, harness = await environment(args)
    from focus.runtime.checkpointer import dispose_checkpointer
    try:
        a, b = await harness.bootstrap("A"), await harness.bootstrap("B")
        await service.adapter.resume_dispatch()
        await asyncio.gather(harness.schedule(a), harness.schedule(b))
        await harness.drain()
        assert len(harness.activations) == 2, harness.activations
        # First activations have actually exited before any short tool can end.
        for owner in (a, b):
            assert not await store.facts(owner["context_id"])
        await service.adapter.resume_dispatch()

        async def scans_started():
            return (Path(a["job_root"]) / "scan.attempts").exists()
        await until(scans_started, "scan never started")
        service.start_pumps()
        for owner in (a, b):
            (Path(owner["job_root"]) / "activation-exited").write_text("exited", encoding="utf-8")

        async def both_results():
            return all([await store.facts(owner["context_id"], unconsumed=True) for owner in (a, b)])
        await until(both_results, "results did not enter Context inboxes")
        inbox_only_ack_rejections = 0
        for owner in (a, b):
            event = (await store.facts(owner["context_id"], unconsumed=True))[0]
            try:
                await service.bridges[owner["context_id"]].ack_consumed(event)
            except RuntimeError:
                inbox_only_ack_rejections += 1
        assert inbox_only_ack_rejections == 2
        await asyncio.gather(harness.schedule(a), harness.schedule(b))
        await harness.drain()
        assert len(harness.activations) == 4, harness.activations

        async def followups():
            facts = await store.facts(a["context_id"])
            return facts if len(facts) == 3 else None
        await until(followups, "repair/cancellation facts did not arrive")
        await service.ack_ready(a["context_id"])
        await service.ack_ready(b["context_id"])
        await harness.schedule(a)
        await harness.drain()
        await service.ack_ready(a["context_id"])
        facts_a, facts_b = await store.facts(a["context_id"]), await store.facts(b["context_id"])
        by_operation = {e["task"]["python"]["args"]["operation"]: e for e in facts_a}
        assert by_operation["scan"]["result"]["state"] == "cancelled", by_operation
        assert by_operation["scan"]["result"].get("pid", 0) > 0
        assert by_operation["repair"]["result"]["state"] == "completed"
        assert Path(a["job_root"], "repair.txt").read_text(encoding="utf-8") == "verified:repair"
        assert Path(b["job_root"], "independent.txt").read_text(encoding="utf-8") == "verified:independent"
        for owner, facts in ((a, facts_a), (b, facts_b)):
            assert not await store.facts(owner["context_id"], unconsumed=True)
            assert not await store.facts(owner["context_id"], unacked=True)
            assert all(e["task"]["owner"] == service.runs[owner["context_id"]].owner for e in facts)
            first_exit = next(item["exited_ns"] for item in harness.activations if item["context_id"] == owner["context_id"])
            for event in facts:
                if event["task"]["revision"] == 1:
                    assert event["timing"]["started_ns"] < first_exit < event["timing"]["finished_ns"]
            assert all(item["lease_id"] == service.leases[owner["context_id"]].lease_id
                for item in service.checks if item["context_id"] == owner["context_id"])
        assert service.renewals > 0
        repair_action = next(item for item in service.action_trace if item["context_id"] == a["context_id"] and item["revision"] == 2)
        return {"case": "two_context_redirect", "passed": True, "activations": harness.activations,
            "facts": facts_a + facts_b, "execution_checks": service.checks, "lease_renewals": service.renewals,
            "action_trace": service.action_trace, "publication_trace": service.published,
            "inbox_only_ack_rejections": inbox_only_ack_rejections,
            "receipts": {o["label"]: await store.receipts(o["context_id"]) for o in (a, b)},
            "job_leases": {o["label"]: service.leases[o["context_id"]].model_dump(mode="json") for o in (a, b)},
            "check_finished_to_repair_action_ms": (repair_action["started_ns"] - by_operation["check"]["timing"]["finished_ns"]) / 1e6,
            "attempts": {owner["label"]: {p.stem: len(p.read_text().splitlines()) for p in Path(owner["job_root"]).glob("*.attempts")} for owner in (a, b)}}
    finally:
        await harness.close()
        await service.close()
        await dispose_checkpointer(saver)
        await engine.dispose()


def durable_json(path, value):
    with path.open("w", encoding="utf-8") as output:
        json.dump(value, output, indent=2, ensure_ascii=False)
        output.flush()
        os.fsync(output.fileno())


async def recovery_child(args):
    engine, saver, store, service, harness = await environment(args)
    from focus.runtime.checkpointer import dispose_checkpointer
    from focus_tool import RemoteRejected
    from patrol_store import identity
    try:
        if args.stage == "prepare":
            owner = await harness.bootstrap("R")
            harness.wait_started = not args.case.startswith("queued")
            if harness.wait_started:
                await service.adapter.resume_dispatch()
            await harness.schedule(owner)
            await harness.drain()
            assert len(harness.activations) == 1
            if not args.case.startswith("queued"):
                await service.adapter.resume_dispatch()
                (Path(owner["job_root"]) / "activation-exited").write_text("exited", encoding="utf-8")
                job = service.adapter.job_id(owner["context_id"] + ":recovery", "recovery")
                async def completed():
                    return (await service.runs[owner["context_id"]].status(job))["state"] == "completed"
                await until(completed, "recovery tool never completed")
                if args.case in {"after_receipt", "receipt_pending", "revision_reconcile"}:
                    service.start_pumps()
                    async def published():
                        return await store.facts(owner["context_id"], unconsumed=True)
                    await until(published, "first result not published")
                    harness.defer_actions = args.case != "after_receipt"
                    await harness.schedule(owner)
                    await harness.drain()
                    if args.case == "after_receipt":
                        repair = service.adapter.job_id(owner["context_id"] + ":repair", "repair")
                        async def repair_finished():
                            return (await service.runs[owner["context_id"]].status(repair))["state"] == "completed"
                        await until(repair_finished, "followup not completed before restart")
                    else:
                        assert not Path(owner["job_root"], "repair.attempts").exists()
                        assert len(await store.actions(owner["context_id"])) == 1
                        if args.case == "revision_reconcile":
                            # Reproduce the old split explicitly: durable action
                            # intent is rev2, daemon advanced, SQL owner still1.
                            await service.runs[owner["context_id"]].revise(2)
                            assert (await store.owners())[0]["revision"] == 1
                    assert await store.confirmed((await store.facts(owner["context_id"]))[0])
            else:
                job = service.adapter.job_id(owner["context_id"] + ":recovery", "recovery")
                assert (await service.runs[owner["context_id"]].status(job))["state"] == "queued"
            process = getattr(service.client.process, "process", service.client.process)
            marker = {"case": args.case, "daemon_pid": process.pid, "activations": harness.activations,
                "facts": await store.facts(owner["context_id"]), "job": job, "boundary_ns": time.time_ns()}
            durable_json(args.work / "boundary.json", marker)
            # Parent kills both real processes here. No graceful close, archive
            # transfer or checkpoint is smuggled into the crash boundary.
            await asyncio.Event().wait()
        else:
            owners = list(service.owners.values())
            assert len(owners) == 1
            owner = owners[0]
            context = owner["context_id"]
            before = await store.facts(context)
            confirmed = [list(identity(event)) for event in before if await store.confirmed(event)]
            stats = await service.client.request("stats")
            assert stats["dispatch_paused"]
            missing_rejected = False
            if args.case.startswith("queued"):
                try:
                    await service.adapter.resume_dispatch()
                except RemoteRejected:
                    missing_rejected = True
                assert missing_rejected
                (Path(owner["job_root"]) / "activation-exited").write_text("exited", encoding="utf-8")
                if args.case == "queued_revoked":
                    from sqlalchemy import select
                    from backend.app.desktop.patrol.agent_loop.models import LoopDelegationGrant
                    async with store.sessions.begin() as session:
                        grant = await session.scalar(select(LoopDelegationGrant).where(LoopDelegationGrant.loop_id == owner["loop_id"], LoopDelegationGrant.status == "active"))
                        grant.permission_scope = ["read"]
            await service.restore()
            service.start_pumps()
            await service.ack_ready(context)
            async def first_fact():
                return await store.facts(context, unconsumed=True)
            await until(first_fact, "durable runtime event did not recover to Focus inbox")
            await harness.schedule(owner)
            await harness.drain()
            await service.ack_ready(context)
            if args.case != "queued_revoked":
                async def repair_fact():
                    facts = await store.facts(context, unconsumed=True)
                    return facts if any(e["task"]["python"]["args"]["operation"] == "repair" for e in facts) else None
                # after_receipt consumes the already-published repair on its one
                # successor activation; other cases produce repair in this Run.
                if args.case not in {"after_receipt", "receipt_pending", "revision_reconcile"}:
                    await until(repair_fact, "repair completion missing")
                    await harness.schedule(owner)
                    await harness.drain()
                    await service.ack_ready(context)
                assert Path(owner["job_root"], "repair.txt").read_text(encoding="utf-8") == "verified:repair"
            facts = await store.facts(context)
            assert not await store.facts(context, unacked=True)
            assert not await store.facts(context, unconsumed=True)
            attempts = {path.stem: len(path.read_text().splitlines()) for path in Path(owner["job_root"]).glob("*.attempts")}
            if args.case == "queued_revoked":
                assert attempts == {} and facts[0]["result"]["state"] == "failed", (attempts, facts)
                assert not facts[0]["result"].get("pid")
            else:
                assert attempts == {"recovery": 1, "repair": 1}, attempts
            consumed_after = [item["identity"] for activation in harness.activations for item in activation["decisions"]]
            assert not any(key in consumed_after for key in confirmed), "decision repeated after a durable receipt"
            durable_json(args.work / "resumed.json", {"case": args.case, "passed": True, "facts": facts,
                "activations": harness.activations, "attempts": attempts, "prior_confirmed": confirmed,
                "missing_inventory_rejected": missing_rejected, "execution_checks": service.checks,
                "receipts": await store.receipts(context),
                "action_trace": service.action_trace, "publication_trace": service.published})
    finally:
        await harness.close()
        await service.close()
        await dispose_checkpointer(saver)
        await engine.dispose()


def kill_daemon(pid):
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=True, capture_output=True)
    else:
        import signal
        os.kill(pid, signal.SIGKILL)


def recovery(args, case, *, script=None):
    work = args.work / case
    work.mkdir()
    base = [sys.executable, "-u", str(script or Path(__file__).resolve()), "--focus-root", str(args.focus_root),
        "--work", str(work), "--binary", str(args.binary), "--case", case]
    log = work / "prepare.log"
    marker = work / "boundary.json"
    with log.open("w", encoding="utf-8") as output:
        process = subprocess.Popen([*base, "--stage", "prepare"], stdout=output, stderr=output)
        try:
            deadline = time.monotonic() + 90
            while not marker.exists():
                if process.poll() is not None:
                    raise RuntimeError(log.read_text(encoding="utf-8"))
                if time.monotonic() > deadline:
                    raise TimeoutError("child never reached " + case + " boundary")
                time.sleep(0.05)
            boundary = json.loads(marker.read_text(encoding="utf-8"))
            kill_daemon(boundary["daemon_pid"])
        finally:
            process.kill()
            process.wait(timeout=10)
    result = subprocess.run([*base, "--stage", "resume"], capture_output=True, text=True, timeout=90)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    report = json.loads((work / "resumed.json").read_text(encoding="utf-8"))
    report["crash_boundary"] = boundary
    report["interruption"] = "parent killed real Go daemon and Python bridge process; no graceful close"
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--focus-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "results/local/patrol.json")
    parser.add_argument("--stage", choices=("prepare", "resume"))
    parser.add_argument("--case")
    parser.add_argument("--work", type=Path)
    parser.add_argument("--binary", type=Path)
    args = parser.parse_args()
    args.focus_root = args.focus_root.resolve()
    commit = command("git", "-C", str(args.focus_root), "rev-parse", "HEAD")
    if commit != FOCUS_COMMIT:
        raise RuntimeError("This integration is pinned to " + FOCUS_COMMIT)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    if args.stage:
        asyncio.run(recovery_child(args))
        return
    container = "focus-tool-patrol-" + uuid.uuid4().hex[:12]
    started = False
    try:
        command("docker", "run", "-d", "--rm", "--pull", "never", "--name", container,
            "-p", "127.0.0.1::5432", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=focus_tool_patrol", "postgres:17-alpine")
        started = True
        port = command("docker", "port", container, "5432/tcp").rsplit(":", 1)[1]
        for _ in range(60):
            if subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"], capture_output=True).returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("isolated PostgreSQL did not become ready")
        os.environ["FOCUS_TOOL_TEST_DSN"] = "postgresql://postgres@127.0.0.1:" + port + "/focus_tool_patrol"
        with tempfile.TemporaryDirectory(prefix="focus-tool-patrol-") as directory:
            args.work = Path(directory)
            args.binary = args.work / ("runtime.exe" if os.name == "nt" else "runtime")
            subprocess.run(["go", "build", "-o", str(args.binary), "./cmd/runtime"], cwd=ROOT, check=True)
            if sys.platform == "win32":
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            cases = [asyncio.run(normal(args))]
            for case in ("before_inbox", "after_receipt", "receipt_pending", "revision_reconcile", "queued_allowed", "queued_revoked"):
                database = "patrol_" + case
                command("docker", "exec", container, "createdb", "-U", "postgres", database)
                os.environ["FOCUS_TOOL_TEST_DSN"] = "postgresql://postgres@127.0.0.1:" + port + "/" + database
                cases.append(recovery(args, case))
        sources = [path for directory in (ROOT / "runtime", ROOT / "cmd/runtime", ROOT / "python/focus_tool", ROOT / "integration/focus")
            for path in directory.rglob("*") if path.suffix in {".go", ".py"}]
        report = {"focus_commit": commit, "focus_tool_base_commit": command("git", "-C", str(ROOT), "rev-parse", "HEAD"),
            "collected_at_utc": datetime.now(UTC).isoformat(),
            "source_sha256": {path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(sources)},
            "decision_source": "local deterministic providers; no model API", "cases": cases, "passed": True}
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(json.dumps({"passed": True, "cases": [case["case"] for case in cases]}))
    finally:
        if started:
            subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
