"""Optional, reversible observers of the pinned Focus implementation.

No business methods are copied or skipped. SQL/commit timings include driver
round trips, not a claim about server CPU or fsync. No SQL values are recorded.
"""
from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import hashlib
import inspect
import time
import uuid


_active = ContextVar("focus_tool_progress_probe", default=None)
_frames = ContextVar("focus_tool_progress_frames", default=())
_installed = None  # Reversible class hooks have one process-local owner.


class TaskProgressProbe:
    def __init__(self, trace):
        self.trace, self._undo = trace, []

    def emit(self, event):
        try:
            self.trace(event)
        except Exception:
            pass

    @contextmanager
    def span(self, phase):
        root = _active.get()
        if root is None or root["probe"] is not self:
            yield None
            return
        parents = _frames.get()
        frame = {"kind": "task_progress_span", "span_id": uuid.uuid4().hex,
            "parent_span_id": parents[-1]["span_id"] if parents else None, "phase": phase,
            "started_ns": time.time_ns(), "started_monotonic_ns": time.perf_counter_ns(),
            "sql_count": 0, "sql_elapsed_ns": 0, "sql_errors": 0, "error": None}
        for parent in reversed(parents):
            if "transition" in parent:
                frame["transition"] = parent["transition"]
                break
        token = _frames.set((*parents, frame))
        try:
            yield frame
        except BaseException as error:
            frame["error"] = type(error).__name__
            raise
        finally:
            frame["finished_monotonic_ns"], frame["finished_ns"] = time.perf_counter_ns(), time.time_ns()
            frame["elapsed_ns"] = frame["finished_monotonic_ns"] - frame["started_monotonic_ns"]
            _frames.reset(token)
            self._record(root, frame)

    def _record(self, root, event):
        if root["closed"]:
            self.emit({**event, **root["identity"], "work_items": root["work_items"]})
        else:
            root["events"].append(event)

    async def drain(self, runtime, *, loop_id, schedule_id):
        root = {"probe": self, "closed": False, "events": [], "work_items": None,
            "identity": {"loop_id": loop_id, "work_id": None, "fence": None,
                         "schedule_id": schedule_id, "drain_id": uuid.uuid4().hex}}
        active, frames = _active.set(root), _frames.set(())
        try:
            with self.span("drain"):
                result = await runtime.drain(loop_id=loop_id)
                root["work_items"] = result
                return result
        finally:
            root["closed"] = True
            _frames.reset(frames)
            _active.reset(active)
            for event in root["events"]:
                self._record(root, event)
            root["events"].clear()

    def wrap(self, target, name, phase, *, claimed=False):
        original = getattr(target, name)
        signature = inspect.signature(original)
        own = vars(target).get(name)
        had_own = name in vars(target)
        descriptor = inspect.getattr_static(target, name)

        def identify(args, kwargs):
            root = _active.get()
            if root is None or root["probe"] is not self:
                return None
            try:
                arguments = signature.bind_partial(*args, **kwargs).arguments
            except Exception:
                return None  # The original method owns argument validation.
            for key in ("loop_id", "work_id", "fence"):
                if arguments.get(key) is not None:
                    root["identity"][key] = arguments[key]
            work = arguments.get("work")
            if work is not None:
                root["identity"].update(loop_id=work.loop_id, work_id=work.work_id)
            inputs = arguments.get("inputs")
            if inputs is not None and hasattr(inputs, "work_id"):
                root["identity"]["work_id"] = inputs.work_id
            return arguments

        if inspect.iscoroutinefunction(original):
            @wraps(original)
            async def wrapped(*args, **kwargs):
                arguments = identify(args, kwargs)
                with self.span(phase) as frame:
                    if frame is not None and arguments is not None and "transition" in arguments:
                        frame["transition"] = arguments["transition"]
                    result = await original(*args, **kwargs)
                    if claimed and result is not None and arguments is not None:
                        _active.get()["identity"].update(work_id=result[0], fence=result[1])
                    return result
        else:
            @wraps(original)
            def wrapped(*args, **kwargs):
                identify(args, kwargs)
                with self.span(phase):
                    return original(*args, **kwargs)
        if isinstance(target, type) and isinstance(descriptor, staticmethod):
            wrapped = staticmethod(wrapped)
        elif isinstance(target, type) and isinstance(descriptor, classmethod):
            raise TypeError("classmethod probes require an explicit adapter")
        setattr(target, name, wrapped)
        self._undo.append(lambda: setattr(target, name, own) if had_own else delattr(target, name))

    def install(self, runtime, engine, provider_type):
        global _installed
        if _installed is not None:
            raise RuntimeError("one installed TaskProgress probe per research process")
        _installed = self
        # Imports stay in the optional Focus boundary, never the stdlib core.
        try:
            from sqlalchemy import event
            from backend.app.desktop.patrol.agent_loop.task_progress import runtime as module
            from backend.app.desktop.patrol.agent_loop.task_progress.work_queue import TaskProgressQueue
            from backend.app.desktop.patrol.agent_loop.journal.accounting_events import LoopAccountingEventRecorder
            from backend.app.desktop.patrol.agent_loop.journal.accounting_query import LoopAccountingQuery
            for name, phase in (("_claim", "claim"), ("_consolidate", "consolidate"),
                    ("_interpret", "interpret"), ("_reserve", "budget_reserve"),
                    ("_record_usage", "usage_settlement"), ("_record_validation", "validation_record"), ("_fail", "failure_record")):
                self.wrap(runtime, name, phase, claimed=name == "_claim")
            self.wrap(TaskProgressQueue, "_bind", "input_freeze")
            self.wrap(runtime._lease, "run", "lease_execution")
            self.wrap(runtime._lease, "_renew", "lease_renewal")
            for name, phase in (("consolidation_inputs", "input_assembly"),
                    ("record_validation", "validation_recheck"), ("publish", "publish")):
                self.wrap(runtime._repository, name, phase)
            self.wrap(runtime._consolidator, "apply", "validation_apply")
            for name, phase in (("interpretation_payload", "payload_assembly"),
                    ("candidate_schema", "candidate_schema"), ("estimate_request_budget", "budget_estimate")):
                self.wrap(module, name, phase)
            self.wrap(module.StructuredWorkerModel, "request_messages", "budget_messages")
            self.wrap(provider_type, "invoke", "provider")
            self.wrap(LoopAccountingEventRecorder, "record", "accounting_record")
            self.wrap(LoopAccountingQuery, "read", "accounting_view")
            self.wrap(engine.sync_engine.dialect, "do_commit", "transaction_commit")
            for name, handler in (("before_cursor_execute", self._before_sql),
                    ("after_cursor_execute", self._after_sql), ("handle_error", self._sql_error)):
                def safe(*args, _handler=handler, **kwargs):
                    try:
                        _handler(*args, **kwargs)
                    except Exception:
                        pass
                event.listen(engine.sync_engine, name, safe)
                self._undo.append(lambda name=name, handler=safe: event.remove(engine.sync_engine, name, handler))
            return self
        except BaseException as error:
            try:
                self.close()
            except Exception as cleanup_error:
                error.add_note("Probe installation rollback: " + repr(cleanup_error))
            raise

    def _before_sql(self, conn, cursor, statement, parameters, context, executemany):
        root = _active.get()
        if root is not None and root["probe"] is self:
            # No statement text or parameter values cross the diagnostic port.
            context._focus_tool_probe = (root, _frames.get(), time.perf_counter_ns(), time.time_ns(),
                hashlib.sha256(" ".join(statement.split()).encode()).hexdigest(), statement.split(None, 1)[0].upper())

    def _finish_sql(self, context, error=None):
        saved = getattr(context, "_focus_tool_probe", None)
        if saved is None:
            return
        del context._focus_tool_probe
        root, frames, start, wall, digest, verb = saved
        elapsed = time.perf_counter_ns() - start
        for frame in frames:
            frame["sql_count"] += 1
            frame["sql_elapsed_ns"] += elapsed
            frame["sql_errors"] += error is not None
        self._record(root, {"kind": "task_progress_sql", "parent_span_id": frames[-1]["span_id"] if frames else None,
            "started_ns": wall, "finished_ns": time.time_ns(), "elapsed_ns": elapsed,
            "statement_sha256": digest, "verb": verb, "error": type(error).__name__ if error else None})

    def _after_sql(self, conn, cursor, statement, parameters, context, executemany):
        self._finish_sql(context)

    def _sql_error(self, exception_context):
        self._finish_sql(exception_context.execution_context, exception_context.original_exception)

    def close(self):
        global _installed
        errors, unfinished = [], []
        for undo in reversed(self._undo):
            try:
                undo()
            except Exception as error:
                errors.append(error)
                unfinished.append(undo)
        self._undo = list(reversed(unfinished))
        if _installed is self and not unfinished:
            _installed = None
        if errors:
            raise ExceptionGroup("probe restoration failed", errors)


def _union(intervals):
    total, end = 0, None
    for left, right in sorted(intervals):
        total += max(0, right - max(left, end if end is not None else left))
        end = max(right, end if end is not None else right)
    return total


def validate_probe_trace(trace):
    """Rederive inclusive SQL attribution from leaf execution facts."""
    spans = {x["span_id"]: x for x in trace if x["kind"] == "task_progress_span"}
    assert len(spans) == sum(x["kind"] == "task_progress_span" for x in trace)
    actual = defaultdict(lambda: [0, 0, 0])
    for sql in (x for x in trace if x["kind"] == "task_progress_sql"):
        parent, seen = sql["parent_span_id"], set()
        while parent is not None:
            assert parent not in seen and parent in spans
            seen.add(parent)
            span = spans[parent]
            assert all(sql[key] == span[key] for key in ("drain_id", "loop_id", "work_id", "fence"))
            count = actual[parent]
            count[0] += 1
            count[1] += sql["elapsed_ns"]
            count[2] += sql["error"] is not None
            parent = span["parent_span_id"]
    for span in spans.values():
        assert span["elapsed_ns"] == span["finished_monotonic_ns"] - span["started_monotonic_ns"]
        assert actual[span["span_id"]] == [span["sql_count"], span["sql_elapsed_ns"], span["sql_errors"]]


def probe_summary(trace, *, loop_id=None, after=0, through=None):
    spans = [x for x in trace if x["kind"] == "task_progress_span" and (loop_id is None or x["loop_id"] == loop_id)]
    children = defaultdict(list)
    for item in spans:
        children[item["parent_span_id"]].append((item["started_monotonic_ns"], item["finished_monotonic_ns"]))
    grouped = {key: defaultdict(list) for key in ("work", "empty", "failed")}
    for item in spans:
        if max(item["started_ns"], item["finished_ns"]) < after or (through is not None and min(item["started_ns"], item["finished_ns"]) > through):
            continue
        category = "failed" if item["work_items"] is None else "work" if item["work_items"] else "empty"
        grouped[category][item["phase"]].append(item)
    return {category: {phase: {"calls": len(items),
        "total_ms": sum(x["elapsed_ns"] for x in items) / 1e6,
        "exclusive_ms": sum(max(0, x["elapsed_ns"] - _union([
            (max(left, x["started_monotonic_ns"]), min(right, x["finished_monotonic_ns"]))
            for left, right in children[x["span_id"]]
            if min(right, x["finished_monotonic_ns"]) > max(left, x["started_monotonic_ns"])])) for x in items) / 1e6,
        "window_wall_ms": sum(max(0, min(x["finished_ns"], through if through is not None else x["finished_ns"]) - max(x["started_ns"], after)) for x in items) / 1e6,
        "crosses_boundary": sum(int(x["started_ns"] < after) + int(through is not None and x["finished_ns"] > through) for x in items),
        "sql_count": sum(x["sql_count"] for x in items), "sql_ms": sum(x["sql_elapsed_ns"] for x in items) / 1e6,
        "sql_errors": sum(x["sql_errors"] for x in items)} for phase, items in sorted(phases.items())}
        for category, phases in grouped.items()}
