# Research repository

- The architecture image at `docs/images/tool-runtime-original.png` is part of the user's initial requirements and is the architecture acceptance standard. Preserve its module topology: Runs → Focus tool adapter → unified scheduler → Python/process executors → result events and ownership routing → owning Runs. Track incomplete modules explicitly; M1 alone does not complete this architecture.
- Build and experiment entirely locally using Go and Python standard libraries. Do not add model calls, hosted queues, agent frameworks or runtime dependencies without a concrete research need and user agreement.
- Preserve the concurrent barrier baseline. Compare equal inputs, concurrency caps and final outcomes; count cancellation cleanup in total time.
- Never infer that cancellation succeeded from a request alone. Current workers are leaf processes; do not claim process-tree support.
- Record raw traces and distinguish execution facts from result consumption. Version checks and duplicate filtering must be scoped to Context.
- Run `go test ./...` and `go vet ./...` after behavioral changes. Use `go run ./cmd/lab` to reproduce the local experiment.
- The shared architecture lives in `runtime`, `cmd/runtime`, and `python/focus_tool`; preserve `internal/runtime` and `cmd/lab` as the M1 baseline. Register trusted Python or process tools in FocusToolAdapter; never bypass the execution-time authority/lease hook. The caller checkpoints before Run.ack. Test the bridge with `python -m unittest discover -s python/tests -v`; reproduce the full loop with `python examples/demo.py`.
- `python scripts/verify.py` runs the complete local verification. GitHub Actions also runs Linux race detection; local Windows race detection requires a configured cgo compiler.
- Keep measured results separate from performance hypotheses. M1 numbers do not establish real Agent speedup.
