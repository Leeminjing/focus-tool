"""One trusted, registered Python call in a cancellable leaf worker process."""
import time
_boot_wall = time.time_ns()
_boot_mono = time.perf_counter_ns()
import asyncio
import contextlib
import importlib
import inspect
import json
import sys
import traceback


def main():
    call = json.load(sys.stdin)
    spans = []
    def span(phase, wall, mono):
        if call.get("_trace"):
            spans.append({"phase": phase, "start_ns": wall, "end_ns": time.time_ns(),
                          "duration_ns": time.perf_counter_ns() - mono, "clock": "python_worker"})
    span("worker_bootstrap", _boot_wall, _boot_mono)
    # Keep the JSON protocol on stdout; tool prints and tracebacks use stderr.
    with contextlib.redirect_stdout(sys.stderr):
        wall, mono = time.time_ns(), time.perf_counter_ns()
        function = getattr(importlib.import_module(call["module"]), call["function"])
        span("module_import", wall, mono)
        wall, mono = time.time_ns(), time.perf_counter_ns()
        result = function(**(call.get("args") or {}))
        if inspect.isawaitable(result):
            result = asyncio.run(result)
        span("tool_body", wall, mono)
    wall, mono = time.time_ns(), time.perf_counter_ns()
    output = json.dumps(result, ensure_ascii=False, allow_nan=False) + "\n"
    span("result_encode", wall, mono)
    if call.get("_trace"):
        output = json.dumps({"trace_version": 1, "output": output, "spans": spans}, ensure_ascii=False) + "\n"
    sys.stdout.write(output)
    sys.stdout.flush()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
