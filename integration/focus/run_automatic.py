"""Single-host automatic Patrol wakes through unchanged real Focus components.

Scenarios start the service, control local leaf-tool fixtures and assert facts.
They never schedule successor activations or wait for A+B result barriers.
"""
import argparse
import asyncio
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid

from run_patrol import ROOT, FOCUS_COMMIT, command, environment, until, durable_json, recovery


async def settled(store, harness, count):
    if len(harness.activations) < count:
        return False
    return not await store.pending_wakes() and all([
        not await store.facts(owner["context_id"], unacked=True)
        for owner in await store.owners()])


def attempt_counts(owner):
    return {path.stem: len(path.read_text(encoding="utf-8").splitlines())
        for path in Path(owner["job_root"]).glob("*.attempts")}


def summarize(owner, tools, harness, trace):
    context = owner["context_id"]
    return {"context_id": context, "loop_id": owner["loop_id"], "attempts": attempt_counts(owner),
        "activations": [item for item in harness.activations if item["context_id"] == context],
        "publications": [item for item in tools.published if item["context_id"] == context],
        "actions": [item for item in tools.action_trace if item["context_id"] == context]}


def response_chain(owner, facts, tools, trace):
    context = owner["context_id"]
    check = next(item for item in facts if item["task"]["python"]["args"]["operation"] == "check")
    finished = check["timing"]["finished_ns"]
    publication = next(item for item in tools.published
        if item["context_id"] == context and item["sequence"] == check["sequence"])
    published = publication["published_ns"]
    wake = next(item["time_ns"] for item in trace if item["kind"] == "wake_observed"
        and item["context_id"] == context and item["generation"] >= check["sequence"])
    def after(kind):
        return next(item["time_ns"] for item in trace if item["kind"] == kind and item["time_ns"] >= wake
            and (item.get("loop_id") == owner["loop_id"] or item.get("context_id") == context))
    actual = json.loads(Path(owner["job_root"], "repair.attempts").read_text(encoding="utf-8").splitlines()[0])
    return {"clock": "same-host Unix ns", "tool_finished_ns": finished,
        "inbox_persist_started_ns": publication["persist_started_ns"], "inbox_commit_returned_ns": published,
        "notification_ns": publication.get("notified_ns"), "wake_observed_ns": wake,
        "wake_minus_commit_return_ns": wake - published,
        "patrol_started_ns": after("patrol_started"),
        "patrol_committed_ns": after("patrol_committed"), "successor_started_ns": after("activation_started"),
        "repair_function_started_ns": actual["started_ns"],
        "finished_to_effective_action_ms": (actual["started_ns"] - finished) / 1e6}


async def independent(args, *, capacity_wait=False):
    engine, saver, store, tools, harness = await environment(args)
    from focus.runtime.checkpointer import dispose_checkpointer
    from patrol_driver import PatrolResultService
    trace = []
    app = PatrolResultService(tools, harness, trace=trace.append)
    release_activation = asyncio.Event()
    try:
        a, b = await harness.bootstrap("A"), await harness.bootstrap("B")
        if capacity_wait:
            harness.pool._concurrency = 1  # Actual global ContextRunPool limit.
            async def hold(owner, state):
                if owner["context_id"] == b["context_id"] and not state["facts"]:
                    await release_activation.wait()
            harness.before_exit = hold
        await app.start([a["context_id"], b["context_id"]])
        async def started():
            return all(Path(owner["job_root"], operation + ".attempts").exists()
                for owner, operation in ((a, "scan"), (b, "independent")))
        await until(started, "initial local tools never started", 60)
        async def a_exited():
            return any(item["context_id"] == a["context_id"] for item in harness.activations)
        await until(a_exited, "A initial activation never exited", 60)
        (Path(a["job_root"]) / "activation-exited").write_text("released", encoding="utf-8")
        pending_snapshot = None
        if capacity_wait:
            async def committed():
                return sum(item["kind"] == "patrol_committed" and item.get("loop_id") == a["loop_id"] for item in trace) == 2
            await until(committed, "A decision never reached capacity wait", 60)
            pending_snapshot = await store.pending_wakes()
            assert any(row["context_id"] == a["context_id"] for row in pending_snapshot)
            assert sum(item["kind"] == "activation_started" and item.get("context_id") == a["context_id"] for item in trace) == 1
            assert await app.port.active(context_id=b["context_id"]) == 1
            release_activation.set()
        async def repaired():
            return Path(a["job_root"], "repair.txt").exists()
        await until(repaired, "A did not automatically repair while B remained running", 90)
        b_job = tools.adapter.job_id(b["context_id"] + ":independent", "independent")
        b_status_at_repair = await tools.runs[b["context_id"]].status(b_job)
        assert b_status_at_repair["state"] == "running", b_status_at_repair
        assert not Path(b["job_root"], "independent.txt").exists()
        assert not await store.facts(b["context_id"]), "A waited for B's result"
        (Path(b["job_root"]) / "activation-exited").write_text("released after A repair", encoding="utf-8")
        await until(lambda: settled(store, harness, 5), "automatic facts did not all consume/ack", 90)
        assert app.driver.last_error is None, app.driver.last_error
        facts = await store.facts(a["context_id"])
        scan = next(item for item in facts if item["task"]["python"]["args"]["operation"] == "scan")
        assert scan["result"]["state"] == "cancelled" and scan["result"].get("pid", 0) > 0
        assert attempt_counts(a) == {"check": 1, "scan": 1, "repair": 1}
        identities = [tuple(item["identity"]) for activation in harness.activations for item in activation["decisions"]]
        assert len(identities) == len(set(identities))
        return {"case": "capacity_wait" if capacity_wait else "A_repairs_before_B_finishes", "passed": True,
            "contexts": [summarize(owner, tools, harness, trace) for owner in (a, b)], "trace": trace,
            "facts": facts + await store.facts(b["context_id"]), "b_status_at_repair": b_status_at_repair,
            "pending_during_capacity_wait": pending_snapshot, "response_chain": response_chain(a, facts, tools, trace)}
    finally:
        release_activation.set()
        await app.close()
        await harness.close()
        await tools.close()
        await dispose_checkpointer(saver)
        await engine.dispose()


async def burst(args):
    engine, saver, store, tools, harness = await environment(args)
    from focus.runtime.checkpointer import dispose_checkpointer
    from patrol_driver import PatrolResultService
    trace = []
    app = PatrolResultService(tools, harness, trace=trace.append)
    release = asyncio.Event()
    try:
        owner = await harness.bootstrap("burst")
        context = owner["context_id"]
        harness.initial_operations[context] = ("burst0", "burst1", "burst2")
        async def hold(current, state):
            if not state["facts"]:
                await release.wait()
        harness.before_exit = hold
        await app.start([context])
        async def tools_running():
            return len(attempt_counts(owner)) == 3
        await until(tools_running, "burst tools never started", 60)
        Path(owner["job_root"], "activation-exited").write_text("burst while activation is busy", encoding="utf-8")
        async def persisted():
            return len(await store.facts(context, unconsumed=True)) == 3
        await until(persisted, "burst results did not persist", 60)
        assert len(harness.activations) == 0
        assert sum(item["kind"] == "activation_started" for item in trace) == 1
        pending = await store.pending_wakes()
        release.set()
        await until(lambda: settled(store, harness, 2), "burst not consumed automatically", 90)
        await asyncio.sleep(0.4)
        assert len(harness.activations) == 2, harness.activations
        assert len(harness.activations[-1]["decisions"]) == 3
        assert app.driver.last_error is None, app.driver.last_error
        return {"case": "burst_coalesced_while_activation_busy", "passed": True,
            "context": summarize(owner, tools, harness, trace), "trace": trace,
            "pending_before_activation_exit": pending, "facts": await store.facts(context),
            "receipts": await store.receipts(context)}
    finally:
        release.set()
        await app.close()
        await harness.close()
        await tools.close()
        await dispose_checkpointer(saver)
        await engine.dispose()


async def custody_loss(args):
    engine, saver, store, tools, harness = await environment(args)
    from focus.runtime.checkpointer import dispose_checkpointer
    from patrol_driver import PatrolResultService
    app = PatrolResultService(tools, harness)
    try:
        owner = await harness.bootstrap("A")
        context = owner["context_id"]
        await app.start([context])
        async def exited():
            return len(harness.activations) == 1
        await until(exited, "first activation did not exit", 60)
        await app.driver.close()  # Hold outbox dispatch during the custody-loss injection.
        pending = harness.action(owner, "recovery")
        pending["id"] = context + ":0-unsubmitted"
        await store.add_actions(context, [pending], tools.epochs[context])
        # The real lease manager rejects the next renewal. Its first outbox
        # identity has never reached runtime, followed by two running leaves.
        lease = tools.leases[context]
        await tools.lease_manager.release(lease.lease_id, lease.fencing_token)
        async def cancelled():
            if not tools.cleanup_report:
                return False
            states = [(await tools.runs[context].status(tools.adapter.job_id(context + ":" + op, op)))["state"] for op in ("check", "scan")]
            return all(state == "cancelled" for state in states) and len(await store.facts(context)) == 2
        await until(cancelled, "custody loss did not cancel later running jobs", 60)
        states = [item["state"] for item in tools.cleanup_report]
        assert states == ["not_submitted", "cancel_requested", "cancel_requested"], tools.cleanup_report
        assert len(harness.activations) == 1
        assert attempt_counts(owner) == {"check": 1, "scan": 1}
        return {"case": "lease_loss_skips_unsubmitted_and_cancels_all_live_jobs", "passed": True,
            "cleanup_report": tools.cleanup_report, "attempts": attempt_counts(owner),
            "facts_retained": await store.facts(context), "pending": await store.pending_wakes()}
    finally:
        await app.close()
        await harness.close()
        await tools.close()
        await dispose_checkpointer(saver)
        await engine.dispose()


async def arrival_during_consumption(args, *, snapshot_race=False):
    engine, saver, store, tools, harness = await environment(args)
    from focus.runtime.checkpointer import dispose_checkpointer
    from patrol_driver import PatrolResultService
    trace, held, release = [], asyncio.Event(), asyncio.Event()
    app = PatrolResultService(tools, harness, trace=trace.append)
    try:
        owner = await harness.bootstrap("late")
        context = owner["context_id"]
        root = Path(owner["job_root"])
        (root / "gates-per-operation").touch()
        harness.initial_operations[context] = ("burst0", "burst1")
        async def hold(current, state):
            if state["facts"] and any(item["task"]["python"]["args"]["operation"] == "burst0" for item in state["facts"]):
                held.set()
                await release.wait()
        if snapshot_race:
            injected = False
            async def hold_snapshot(current, state, updates):
                nonlocal injected
                if updates.get("messages") and not injected:
                    injected = True
                    held.set()
                    await release.wait()
            harness.after_inbox_snapshot = hold_snapshot
        else:
            harness.before_exit = hold
        await app.start([context])
        async def first_exit():
            return len(harness.activations) == 1
        await until(first_exit, "initial activation did not exit", 60)
        (root / "burst0.release").touch()
        await asyncio.wait_for(held.wait(), 60)
        (root / "burst1.release").touch()
        async def late_pending():
            return len(await store.facts(context, unconsumed=True)) == (2 if snapshot_race else 1)
        await until(late_pending, "new result lost during active consumption", 60)
        pending = await store.pending_wakes()
        assert len(pending) == 1
        assert len(harness.activations) == 1
        assert sum(item["kind"] == "activation_started" for item in trace) == 2
        release.set()
        await until(lambda: settled(store, harness, 3), "late result did not trigger next activation", 90)
        await asyncio.sleep(0.4)
        assert len(harness.activations) == 3
        decisions = [item for activation in harness.activations for item in activation["decisions"]]
        assert len(decisions) == 2 and len({tuple(item["identity"]) for item in decisions}) == 2
        assert not app.port.owner_errors and app.driver.last_error is None
        return {"case": "arrival_between_inbox_snapshot_and_fact_query" if snapshot_race else "new_result_during_checkpointed_consumption", "passed": True,
            "context": summarize(owner, tools, harness, trace), "trace": trace,
            "pending_while_consumer_active": pending, "facts": await store.facts(context),
            "receipts": await store.receipts(context)}
    finally:
        release.set()
        await app.close()
        await harness.close()
        await tools.close()
        await dispose_checkpointer(saver)
        await engine.dispose()


async def shared_loop(args):
    engine, saver, store, tools, harness = await environment(args)
    from focus.runtime.checkpointer import dispose_checkpointer
    from patrol_driver import PatrolResultService
    trace, release = [], asyncio.Event()
    app = PatrolResultService(tools, harness, trace=trace.append)
    try:
        a = await harness.bootstrap("shared-A")
        b = await harness.bootstrap("shared-B", join=a)
        for owner, operation in ((a, "burst0"), (b, "burst1")):
            harness.initial_operations[owner["context_id"]] = (operation,)
            # Both activations only submit/consume; independent service-owned
            # job workspaces retain their own explicit write grants and leases.
            harness.context_modes[owner["context_id"]] = "read-only"
        async def hold(current, state):
            if not state["facts"]:
                await release.wait()
        harness.before_exit = hold
        await app.start([a["context_id"], b["context_id"]])
        async def both_started():
            return attempt_counts(a) and attempt_counts(b)
        await until(both_started, "shared Loop activations never started", 60)
        for owner in (a, b):
            Path(owner["job_root"], "activation-exited").touch()
        async def both_pending():
            return len(await store.pending_wakes()) == 2
        await until(both_pending, "both shared Context facts did not persist", 60)
        pending = await store.pending_wakes()
        assert {row["loop_id"] for row in pending} == {a["loop_id"]}
        assert not harness.activations
        release.set()
        await until(lambda: settled(store, harness, 4), "shared Loop wake did not coalesce", 90)
        await asyncio.sleep(0.4)
        committed = [item for item in trace if item["kind"] == "patrol_committed"]
        assert len(committed) == 2 and all(len(item["context_ids"]) == 2 for item in committed), committed
        assert len(harness.activations) == 4
        assert all(item["status"] == "success" for item in harness.activations)
        assert not app.port.owner_errors and app.driver.last_error is None
        return {"case": "two_contexts_one_real_loop_coalesced", "passed": True, "trace": trace,
            "contexts": [summarize(owner, tools, harness, trace) for owner in (a, b)],
            "pending_while_activations_busy": pending,
            "facts": await store.facts(a["context_id"]) + await store.facts(b["context_id"])}
    finally:
        release.set()
        await app.close()
        await harness.close()
        await tools.close()
        await dispose_checkpointer(saver)
        await engine.dispose()


async def restart_child(args):
    engine, saver, store, tools, harness = await environment(args)
    from focus.runtime.checkpointer import dispose_checkpointer
    from patrol_driver import PatrolResultService
    trace = []
    app = PatrolResultService(tools, harness, trace=trace.append)
    try:
        if args.stage == "prepare":
            owner = await harness.bootstrap("R")
            context = owner["context_id"]
            await app.start([context])
            async def exited():
                return len(harness.activations) == 1
            await until(exited, "initial activation never exited", 60)
            # Freeze this inter-component crash window so periodic fallback
            # cannot heal it before the parent actually kills both processes.
            await app.driver.close()
            async def before_notify(event):
                assert not any(item["kind"] == "notified" for item in trace)
                process = getattr(tools.client.process, "process", tools.client.process)
                durable_json(args.work / "boundary.json", {"case": args.case, "daemon_pid": process.pid,
                    "facts": await store.facts(context), "pending": await store.pending_wakes(),
                    "activations": harness.activations, "trace": trace})
                await asyncio.Event().wait()
            tools.before_notify = before_notify
            Path(owner["job_root"], "activation-exited").write_text("released", encoding="utf-8")
            await asyncio.Event().wait()
        else:
            owners = list(tools.owners.values())
            assert len(owners) == 1
            owner = owners[0]
            pending_at_start = await store.pending_wakes()
            assert len(pending_at_start) == 1
            await app.start()
            tools.on_published = None  # Lost notifications remain safe after restart too.
            await until(lambda: settled(store, harness, 2), "restart did not automatically resume durable pending", 90)
            assert app.driver.last_error is None, app.driver.last_error
            assert attempt_counts(owner) == {"recovery": 1, "repair": 1}
            assert not any(item["kind"] == "notified" for item in trace)
            durable_json(args.work / "resumed.json", {"case": args.case, "passed": True,
                "pending_at_start": pending_at_start, "trace": trace, "facts": await store.facts(owner["context_id"]),
                "context": summarize(owner, tools, harness, trace), "receipts": await store.receipts(owner["context_id"])})
    finally:
        await app.close()
        await harness.close()
        await tools.close()
        await dispose_checkpointer(saver)
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--focus-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "results/local/automatic-patrol.json")
    parser.add_argument("--stage", choices=("prepare", "resume"))
    parser.add_argument("--case")
    parser.add_argument("--work", type=Path)
    parser.add_argument("--binary", type=Path)
    args = parser.parse_args()
    args.focus_root = args.focus_root.resolve()
    if command("git", "-C", str(args.focus_root), "rev-parse", "HEAD") != FOCUS_COMMIT:
        raise RuntimeError("Focus source must match pinned commit " + FOCUS_COMMIT)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    if args.stage:
        asyncio.run(restart_child(args))
        return
    container = "focus-tool-auto-" + uuid.uuid4().hex[:12]
    started = False
    try:
        command("docker", "run", "-d", "--rm", "--pull", "never", "--name", container,
            "-p", "127.0.0.1::5432", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "postgres:17-alpine")
        started = True
        port = command("docker", "port", container, "5432/tcp").rsplit(":", 1)[1]
        for _ in range(60):
            if subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"], capture_output=True).returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("isolated PostgreSQL did not become ready")
        with tempfile.TemporaryDirectory(prefix="focus-tool-auto-") as directory:
            base = Path(directory)
            args.binary = base / ("runtime.exe" if os.name == "nt" else "runtime")
            subprocess.run(["go", "build", "-o", str(args.binary), "./cmd/runtime"], cwd=ROOT, check=True)
            cases = []
            for name, scenario in (("independent", independent), ("burst", burst), ("late", arrival_during_consumption),
                ("snapshot", lambda args: arrival_during_consumption(args, snapshot_race=True)), ("shared", shared_loop),
                ("capacity", lambda args: independent(args, capacity_wait=True)), ("custody", custody_loss), ("inbox_before_notify", None)):
                database = "automatic_" + name
                command("docker", "exec", container, "createdb", "-U", "postgres", database)
                os.environ["FOCUS_TOOL_TEST_DSN"] = "postgresql://postgres@127.0.0.1:" + port + "/" + database
                args.work = base / name
                args.work.mkdir()
                cases.append(asyncio.run(scenario(args)) if scenario else recovery(args, name, script=Path(__file__).resolve()))
        sources = [path for directory in (ROOT / "runtime", ROOT / "cmd/runtime", ROOT / "python/focus_tool", ROOT / "integration/focus")
            for path in directory.rglob("*") if path.suffix in {".py", ".go"}]
        report = {"passed": True, "focus_commit": FOCUS_COMMIT, "base_commit": command("git", "rev-parse", "HEAD"),
            "collected_at_utc": datetime.now(UTC).isoformat(), "decision_source": "deterministic local providers, no model API",
            "source_sha256": {path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(sources)},
            "cases": cases}
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"passed": True, "cases": [case["case"] for case in cases]}))
    finally:
        if started:
            subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
