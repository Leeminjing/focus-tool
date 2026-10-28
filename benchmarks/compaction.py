"""Run alone: same pooled short tools, synchronous vs bounded background indexing."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

from short_tools import ROOT, fingerprint, trial


async def experiment(args, binary, output):
    cases = [(1, 64 << 10, args.jobs), (4, 64 << 10, args.jobs)]
    if not args.skip_long:
        cases.append((4, 8 << 20, args.long_jobs))
    trials, controls = [], []
    for repetition in range(args.repeat):
        for contexts, segment, jobs in cases:
            for mode in (("sync", "background") if repetition % 2 == 0 else ("background", "sync")):
                identity = f"r{repetition+1}-c{contexts}-s{segment}-{mode}"
                result = await trial(binary, output, identity, contexts=contexts, jobs=jobs,
                    segment_bytes=segment, tracing=True, warmup=args.warmup, mode="pool",
                    compaction_mode=mode, drain_compaction=True, sample_every=128)
                # Count completed automatic generations as well as the final
                # explicit tail barrier. The default segment must rotate twice.
                if segment == 8 << 20:
                    assert result["compactions"] >= 3, "increase long-jobs: default WAL must rotate multiple times plus final drain"
                assert result["after"]["wal_pending_segments"] == 0
                assert result["after"]["compaction_workers"] == 0
                assert all(sample["wal_pending_segments"] <= 1 and sample["compaction_workers"] <= 1 for sample in result["stats_samples"])
                trials.append(result)
                print(identity, f"{result['throughput_per_s']:.2f}/s, compactions={result['compactions']}, drain={result['post_workload_drain_ns']/1e6:.2f}ms", flush=True)
    for repetition in range(args.repeat):
        for mode in ("sync", "background"):
            for tracing in ((False, True) if repetition % 2 == 0 else (True, False)):
                identity = f"overhead-r{repetition+1}-trace{int(tracing)}-{mode}"
                result = await trial(binary, output, identity, contexts=4, jobs=args.control_jobs,
                    segment_bytes=64 << 10, tracing=tracing, warmup=args.warmup, mode="pool",
                    compaction_mode=mode, drain_compaction=True)
                controls.append(result)
                print(identity, f"{result['throughput_per_s']:.2f}/s", flush=True)
    return {"trials": trials, "tracing_controls": controls}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="results/local/compaction/report.json")
    parser.add_argument("--jobs", type=int, default=240)
    parser.add_argument("--long-jobs", type=int, default=8196)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=24)
    parser.add_argument("--control-jobs", type=int, default=120)
    parser.add_argument("--skip-long", action="store_true", help="development smoke only; not default-segment acceptance")
    args = parser.parse_args()
    if args.repeat < 1 or any(n < 1 or n % 12 for n in (args.jobs, args.long_jobs, args.warmup, args.control_jobs)):
        parser.error("positive repeat; job counts must be positive multiples of 12")
    output = (ROOT / args.out).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    source = fingerprint()
    with tempfile.TemporaryDirectory(prefix="focus-compaction-build-") as directory:
        binary = Path(directory) / ("runtime.exe" if os.name == "nt" else "runtime")
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/runtime"], cwd=ROOT, check=True)
        report = asyncio.run(experiment(args, binary, output))
    report["config"] = {**vars(args), "platform": platform.platform(), "python": sys.version,
        "go": subprocess.check_output(["go", "version"], text=True).strip(), "concurrency": 4,
        "fsync": "every WAL mutation, checkpoint and effective-action artifact; final derived-index drain in both arms",
        "scope": "local deterministic controller; pooled trusted tools; no concurrent experiments; no model or Patrol",
        "source_sha256_lf_normalized": source}
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
