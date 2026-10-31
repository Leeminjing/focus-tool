"""Finite standard-library workloads; no sleeps, gates, model or service calls."""
import hashlib
import json
import os
from pathlib import Path
import time


def digest_work(data, passes):
    digest = hashlib.sha256()
    for _ in range(passes):
        digest.update(data)
    return digest.hexdigest()


def write_json(path, value):
    with path.open("w", encoding="utf-8") as output:
        json.dump(value, output, sort_keys=True, separators=(",", ":"))
        output.flush()
        os.fsync(output.fileno())


def perform(root, operation):
    root = Path(root)
    with (root / (operation + ".attempts")).open("a", encoding="utf-8") as output:
        output.write(json.dumps({"pid": os.getpid(), "started_ns": time.time_ns()}) + "\n")
        output.flush()
        os.fsync(output.fileno())
    config = json.loads((root / ("scan-input.json" if operation in {"scan", "independent"} else "input.json")).read_text(encoding="utf-8"))
    if operation in {"check", "valid_check"}:
        return {"valid": config["valid"]}
    if operation == "repair":
        config["valid"] = True
        write_json(root / "input.json", config)
        final = {"valid": True, "outcome": "configuration repaired", "input_id": config["input_id"]}
        write_json(root / "final.json", final)
        return final
    if operation in {"scan", "independent"}:
        parameters = json.loads((root / "workload.json").read_text(encoding="utf-8"))
        result = {"digest": digest_work((root / "corpus.bin").read_bytes(), parameters["passes"]),
                  "passes": parameters["passes"], "input_id": config["input_id"]}
        write_json(root / "scan-fact.json", result)
        # Invalid scans are intermediate evidence, not the final deliverable.
        if config["valid"]:
            write_json(root / "final.json", {**result, "valid": True})
        return result
    raise ValueError("unsupported finite workload")
