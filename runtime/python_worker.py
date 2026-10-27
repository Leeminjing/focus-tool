"""One trusted, registered Python call in a cancellable leaf worker process."""
import asyncio
import contextlib
import importlib
import inspect
import json
import sys
import traceback


def main():
    call = json.load(sys.stdin)
    # Keep the JSON protocol on stdout; tool prints and tracebacks use stderr.
    with contextlib.redirect_stdout(sys.stderr):
        function = getattr(importlib.import_module(call["module"]), call["function"])
        result = function(**(call.get("args") or {}))
        if inspect.isawaitable(result):
            result = asyncio.run(result)
    sys.stdout.write(json.dumps(result, ensure_ascii=False, allow_nan=False) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
