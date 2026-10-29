"""Run local checks without third-party packages: python scripts/verify.py."""
import argparse
import json
import platform
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="results/local/verification.json")
    parser.add_argument("--demo-out", default="results/local/full-runtime.json")
    parser.add_argument("--focus-root", help="optional real Focus checkout; needs its dependencies and cached PostgreSQL Docker image")
    parser.add_argument("--focus-python", default=sys.executable)
    parser.add_argument("--focus-out", default="results/local/focus-integration.json")
    parser.add_argument("--focus-patrol", action="store_true", help="also run real Patrol cross-activation/crash tests in a second isolated PostgreSQL container")
    parser.add_argument("--patrol-out", default="results/local/patrol-integration.json")
    parser.add_argument("--focus-automatic", action="store_true", help="also verify durable automatic Patrol wakes")
    parser.add_argument("--automatic-out", default="results/local/automatic-patrol.json")
    parser.add_argument("--automatic-controls-out", default="results/local/automatic-controls.json")
    args = parser.parse_args()
    if (args.focus_patrol or args.focus_automatic) and not args.focus_root:
        parser.error("optional Focus scenarios require --focus-root and its existing environment")
    commands = [
        ["go", "test", "-json", "./...", "-count=1", "-timeout=90s"],
        ["go", "vet", "./..."],
        [sys.executable, "-m", "unittest", "discover", "-s", "python/tests", "-v"],
        [sys.executable, "examples/demo.py", "--out", args.demo_out],
    ]
    if args.focus_root:
        commands.append([args.focus_python, "integration/focus/run_live.py", "--focus-root", args.focus_root, "--out", args.focus_out])
        if args.focus_patrol:
            commands.append([args.focus_python, "integration/focus/run_patrol.py", "--focus-root", args.focus_root, "--out", args.patrol_out])
        if args.focus_automatic:
            commands.append([args.focus_python, "integration/focus/test_wake_controls.py", "--focus-root", args.focus_root,
                "--out", args.automatic_controls_out])
            commands.append([args.focus_python, "integration/focus/run_automatic.py", "--focus-root", args.focus_root, "--out", args.automatic_out])
    report = {"platform": platform.platform(), "python": sys.version, "checks": []}
    success = True
    for command in commands:
        start = time.perf_counter()
        completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
            env={**os.environ, "PYTHONUTF8": "1"})
        report["checks"].append({"command": command, "exit_code": completed.returncode,
                                  "duration_ms": (time.perf_counter() - start) * 1000,
                                  "stdout": completed.stdout, "stderr": completed.stderr})
        print(("PASS " if completed.returncode == 0 else "FAIL ") + " ".join(command))
        if completed.returncode != 0:
            print(completed.stdout, completed.stderr)
            success = False
            break
    report["passed"] = success
    output = ROOT / args.out
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("Verification record:", output)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
