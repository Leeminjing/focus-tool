# Patrol cross-activation bridge contract

This stage follows `a2ef99d4`. The local research integration uses fixed Focus
source, its existing Python environment and an isolated PostgreSQL container.
No model/provider calls, production database or Focus source edits are involved.

The logical runtime owner survives several Focus activations. Ending an
activation never calls runtime Run.close. A service owns the connection, trusted
tool registrations, result pumps and execution-time authority checks. It writes
results idempotently to the actual Context AgentInbox. Actual ContextRunPool,
LoopCoordinator, LoopWaveDispatcher and Kernel admission drive successor Runs;
the local launch port uses real Run workers and deterministic decision graphs.

The experiment separates activation workspaces from per-Context job workspaces.
The job service has an explicit durable lease-anchor row (research-only
DesktopRun kind `tool_service`, not a claimed model activation). Real
WorkspaceLeaseManager grants/renewals/fencing guard those independent slots.
The service checks current Loop/grant authority on every tool boundary and owns
renewal/release; old activation tokens are released and never reused. This is a
research service-principal adapter, not production Desktop lifecycle support.

Execution facts, inbox delivery and semantic consumption are distinct. Native
AgentMessageDelivery proves typed checkpoint inclusion, not a completed decision.
The bridge also validates a durable decision checkpoint and persists a unique
consumption receipt plus deterministic action outbox. Only then may runtime ack
advance. Outbox submissions reuse original identities; restart must reconcile
them, never create new IDs for a possibly accepted write. Obsolete facts are
durably classified, not silently dropped by a plan-version filter.

Recovery starts dispatch paused if queued tasks exist. The service restores
trusted registrations and current lease/policy before an explicit inventory
handshake resumes dispatch. The inventory does not bypass per-task authority.
`unknown` remains quarantined and is never replayed automatically.

The semantic receipt and action outbox commit together before execution. The
outbox's maximum intended revision is authoritative during service attachment;
the convenience owner row can lag the runtime after a crash. Persist the desired
owner revision before advancing the runtime, and reconcile from the outbox on
restart. This is ordered, idempotent recovery, not a distributed transaction.

Acceptance: two Contexts submit and exit their first activations; A's check
result wakes a successor that revises/cancels the real leaf scan and submits a
verified repair, while B progresses independently. Kill/restart before inbox
publication and after decision checkpoint/receipt but before runtime ack. Verify
redelivery, original-ID reconciliation, action deduplication, Context scoping,
lease ownership/renewal and actual cancellation exit. Separately recover queued
work paused, reject incomplete registration and recheck revoked permission.

Additional hard-kill cases leave the action outbox pending after the Focus
activation has exited, and explicitly recreate a stale SQL owner revision behind
the runtime revision. Recovery must execute the original repair once and never
consume the original decision again. See [executed results](patrol-results.md).
