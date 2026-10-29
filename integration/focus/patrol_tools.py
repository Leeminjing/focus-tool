"""Local leaf tools for the cross-activation experiment, never imported by Focus."""
from pathlib import Path
import os
import time
import json


def perform(root, operation):
    root = Path(root)
    # Each attempted execution is observable independently of result delivery.
    with (root / (operation + ".attempts")).open("a", encoding="utf-8") as output:
        output.write(json.dumps({"pid": os.getpid(), "started_ns": time.time_ns()}) + "\n")
        output.flush()
        os.fsync(output.fileno())
    if operation == "scan":
        while True:
            time.sleep(0.01)
    gate = root / ((operation + ".release") if (root / "gates-per-operation").exists() else "activation-exited")
    while not gate.exists():
        time.sleep(0.01)
    if operation == "check":
        return {"configuration_valid": False}
    if operation in {"repair", "independent", "recovery"} or operation in {"burst0", "burst1", "burst2"}:
        path = root / (operation + ".txt")
        path.write_text("verified:" + operation, encoding="utf-8")
        return {"artifact": path.name, "content": path.read_text(encoding="utf-8")}
    raise ValueError("unregistered operation")
