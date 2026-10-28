# Incomplete collection — excluded from acceptance statistics

This first attempt used source commit `24f8564` and the initial default settings:
3 repeats, 240 small-segment jobs, 8196 default-segment jobs, 24 warmup jobs,
120 control jobs, four pooled workers and the existing 30 s tool/admission budget.
Local Windows / Go 1.26.5 / Python 3.12.10; no concurrent experiments.

It stopped in `r3-c4-s8388608-sync` at runtime sequence 3328 with a cancelled
tool: `execution admission: context deadline exceeded`. Its timing was
started_ns=1793194107997040100, finished_ns=1793194139909194700. This is evidence
of a long synchronous pause reaching the existing admission/execution budget,
not a successful comparative trial. Completed preceding raw traces are retained;
the original driver did not persist per-trial report metadata and the failed
trial's temporary directory was cleaned, so there is no complete acceptance
report for this attempt. Do not merge these files into the later collection.

The final collector persists each completed trial, retains failed traces, rejects
incomplete reports during analysis, and explicitly sets both arms to a 300 s
research tool/admission budget. The daemon default remains 30 s. The later default
segment workload is 7296 jobs, still required to rotate automatically at least
twice plus drain its final tail; inputs and limits are equal within each pair.
