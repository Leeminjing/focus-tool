"""Cooperative reuse contract for registered, trusted leaf tools; not a sandbox."""
import time
_boot_wall, _boot_mono = time.time_ns(), time.perf_counter_ns()
import asyncio
import atexit
import importlib
import inspect
import json
import os
import sys
import threading
import traceback
import warnings

_wire = sys.stdout
_dumps, _loads = json.dumps, json.loads
_cwd = os.getcwd()
_environment = dict(os.environ)
_path, _argv = list(sys.path), list(sys.argv)
_importers = dict(sys.path_importer_cache)
_warnings = list(warnings.filters)
_register = atexit.register
_policy = asyncio.get_event_loop_policy()
asyncio.set_event_loop(None)
_baseline_modules = dict(sys.modules)


class Capture:
    encoding = "utf-8"

    def __init__(self, limit):
        self.limit, self.data, self.overflow = limit, bytearray(), False
        self.lock = threading.Lock()

    def write(self, value):
        data = value.encode("utf-8", errors="replace")
        with self.lock:
            remaining = max(0, self.limit - len(self.data))
            self.overflow |= len(data) > remaining
            self.data.extend(data[:remaining])
        return len(value)

    def flush(self):
        pass

    def text(self):
        with self.lock:
            return bytes(self.data).decode("utf-8", errors="replace")


def forbid_exit_hook(*args, **kwargs):
    raise RuntimeError("reuse contract forbids process exit hooks; use fresh mode")


async def await_quiescent(value):
    result = await value
    if any(task is not asyncio.current_task() and not task.done() for task in asyncio.all_tasks()):
        raise RuntimeError("reuse contract forbids pending background asyncio tasks")
    return result


def send(value):
    _wire.write(_dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
    _wire.flush()


def execute(call):
    limit = call["limit"]
    capture = Capture(limit)
    spans = []
    def span(phase, wall, mono):
        if call.get("trace"):
            spans.append({"phase": phase, "start_ns": wall, "end_ns": time.time_ns(),
                          "duration_ns": time.perf_counter_ns() - mono, "clock": "python_worker"})
    state, output, error = "completed", "", ""
    sys.stdout = sys.stderr = capture
    atexit.register = forbid_exit_hook
    try:
        if call["module"] in _baseline_modules:
            raise RuntimeError("reuse contract requires a fresh tool module namespace; use fresh mode for runtime modules")
        wall, mono = time.time_ns(), time.perf_counter_ns()
        function = getattr(importlib.import_module(call["module"]), call["function"])
        span("module_import", wall, mono)
        wall, mono = time.time_ns(), time.perf_counter_ns()
        value = function(**(call.get("args") or {}))
        if inspect.isawaitable(value):
            value = asyncio.run(await_quiescent(value))
        span("tool_body", wall, mono)
        wall, mono = time.time_ns(), time.perf_counter_ns()
        output = _dumps(value, ensure_ascii=False, allow_nan=False) + "\n"
        span("result_encode", wall, mono)
        if len(output.encode("utf-8")) > limit:
            raise RuntimeError("tool output exceeded configured limit")
        if capture.overflow:
            raise RuntimeError("tool diagnostics exceeded configured limit")
        if asyncio.get_event_loop_policy() is not _policy:
            raise RuntimeError("reuse contract forbids replacing the asyncio policy")
        try:
            leftover_loop = asyncio.get_event_loop()
        except RuntimeError:
            leftover_loop = None
        if leftover_loop is not None:
            if not leftover_loop.is_closed() and any(not task.done() for task in asyncio.all_tasks(leftover_loop)):
                raise RuntimeError("reuse contract forbids pending tasks on a synchronous tool's loop")
            if not leftover_loop.is_closed():
                leftover_loop.close()
            asyncio.set_event_loop(None)
        if any(thread is not threading.main_thread() and thread.is_alive() for thread in threading.enumerate()):
            raise RuntimeError("reuse contract forbids live background threads")
    except BaseException:
        state, error = "failed", traceback.format_exc()
    finally:
        # Workspace modules get fresh namespaces, including their local imports.
        # Shared stdlib/extension mutation is forbidden by the opt-in contract.
        for name in list(sys.modules):
            if name not in _baseline_modules:
                del sys.modules[name]
        sys.modules.update(_baseline_modules)
        sys.path[:] = _path
        sys.argv[:] = _argv
        sys.path_importer_cache.clear()
        sys.path_importer_cache.update(_importers)
        warnings.filters[:] = _warnings
        os.environ.clear()
        os.environ.update(_environment)
        os.chdir(_cwd)
        atexit.register = _register
    if state == "failed":
        error = (error + "\n" + capture.text()).encode("utf-8", errors="replace")[:limit].decode("utf-8", errors="replace")
        output = output.encode("utf-8", errors="replace")[:limit].decode("utf-8", errors="replace")
    return {"id": call["id"], "state": state, "output": output, "error": error,
            "reusable": state == "completed", "spans": spans}


send({"type": "ready", "bootstrap": {"phase": "worker_bootstrap", "start_ns": _boot_wall,
      "end_ns": time.time_ns(), "duration_ns": time.perf_counter_ns() - _boot_mono, "clock": "python_worker"}})
for line in sys.stdin:
    call = _loads(line)
    reply = execute(call)
    send(reply)
    if not reply["reusable"]:
        # Bypass atexit/background threads on a poisoned worker. Go still waits
        # for this process and all pipe readers before releasing its lease.
        os._exit(0)
    sys.stdout, sys.stderr = _wire, sys.__stderr__
