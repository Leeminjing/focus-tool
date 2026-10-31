"""Optional native Shell host. The resolver owns original ToolCallRequests.

Use outside an existing native middleware chain: this adapter composes policy
and ledger exactly once at dispatch time. Do not install it inside the default
make_lead_agent additional_middlewares chain (which already owns that ledger).
"""
import asyncio
from dataclasses import replace
import json
import threading

from langchain_core.messages import ToolMessage
from focus.history import content_hash
from focus.security.context import security_context_of
from focus.security.middleware import AccessPolicyMiddleware
from focus.runtime.tool_attempts import ToolExecutionMiddleware


class _LedgerObserver:
    """Delegates all native operations, retaining the actual mutable receipt."""
    def __init__(self, ledger):
        self.ledger, self.receipt = ledger, None
        self.claims = self.completions = 0
        self.cached = False
        self.completed = False

    def __getattr__(self, name):
        return getattr(self.ledger, name)

    async def claim(self, *args, **kwargs):
        self.claims += 1
        result = await self.ledger.claim(*args, **kwargs)
        self.cached = result is not None
        return result

    async def shell_receipt(self, *args):
        self.receipt = await self.ledger.shell_receipt(*args)
        return self.receipt

    async def complete(self, *args, **kwargs):
        self.completions += 1
        result = await self.ledger.complete(*args, **kwargs)
        self.completed = True
        return result


class NativeShellHost:
    RESOURCE = "native-shell:global-host-effects"

    def __init__(self, request_for):
        self.request_for = request_for  # Trusted host-side lookup, not JSON deserialization.

    def definition(self):
        from focus_tool import ToolDefinition
        return ToolDefinition("host", handler=self, resources=((self.RESOURCE, "write"),))

    async def __call__(self, task, cancel):
        if {"name": self.RESOURCE, "mode": "write"} not in task.get("resources", []):
            raise PermissionError("native Shell requires the trusted opaque global resource")
        request = self.request_for(task)
        if request.tool.name not in {"bash", "sh", "cmd", "powershell"}:
            raise ValueError("native Shell registration required")
        security = security_context_of(request.runtime.context)
        route = security.routing
        parent = next(message.id for message in reversed(request.runtime.state["messages"])
                      if getattr(message, "tool_calls", ()))
        from focus_tool import FocusToolAdapter
        if (task["owner"] != {"context_id": route.task_id, "run_id": route.run_id}
                or task["id"] != FocusToolAdapter.job_id(parent, request.tool_call["id"])
                or task["host"]["args"] != request.tool_call["args"]):
            raise PermissionError("native request does not match host job")
        ledger = _LedgerObserver(request.runtime.context["tool_execution_ledger"])
        signal = threading.Event()
        security = replace(security, extras={**security.extras,
            "tool_execution_ledger": ledger, "sandbox_cancel_event": signal})
        request = request.override(runtime=replace(request.runtime,
            context={**request.runtime.context, **security.to_runtime_context()}))

        async def leaf(bound):
            # Use the actual BaseTool dispatch/validation/ToolMessage formatting.
            # The native, governed runtime is injected by the host, never JSON.
            call = {**bound.tool_call, "type": "tool_call",
                    "args": {**bound.tool_call["args"], "runtime": bound.runtime}}
            return await bound.tool.ainvoke(call, config=bound.runtime.config)

        async def recorded(bound):
            return await ToolExecutionMiddleware().awrap_tool_call(bound, leaf)

        async def cancellation():
            await cancel.wait()
            signal.set()

        watcher = asyncio.create_task(cancellation())
        operation = asyncio.create_task(AccessPolicyMiddleware().awrap_tool_call(request, recorded))
        result, error = None, ""
        try:
            while True:
                try:
                    result = await asyncio.shield(operation)
                    break
                except asyncio.CancelledError:
                    signal.set()  # Keep owning the native operation until its cleanup settles.
                    if operation.cancelled():
                        error = "native execution cancelled without a result"
                        break
        except Exception as failure:
            error = type(failure).__name__ + ": " + str(failure)
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
        facts = {}
        if isinstance(result, ToolMessage):
            try:
                facts = json.loads(result.content)
                if not isinstance(facts, dict):
                    facts = {}
            except (TypeError, ValueError):
                pass
        # Receipt is the same server object used by ToolExecutionMiddleware.
        # Canonical replay can use the persisted native ToolMessage instead.
        cleanup = (ledger.receipt or {}).get("process_cleanup")
        if cleanup is None:
            cleanup = facts.get("process_cleanup", "unknown") if ledger.cached else (
                "not_started" if ledger.claims == 0 else "unknown")
        identity = content_hash([route.thread_id, route.checkpoint_ns, route.agent_id, parent, request.tool_call["id"]])
        facts.update(ledger_identity=identity, ledger_claims=ledger.claims,
                     ledger_completions=ledger.completions, ledger_cached=ledger.cached)
        if ledger.receipt and ledger.receipt.get("execution"):
            facts["recovery_identity"] = {key: ledger.receipt["execution"][key]
                for key in ("run_id", "attempt_id", "call_id", "receipt_id")}
        if cleanup not in {"confirmed", "not_started"} or (ledger.claims and not ledger.cached and not ledger.completed):
            state = "unknown"
        elif facts.get("status") == "cancelled":
            state = "cancelled"
        elif error or facts.get("exit_code") != 0:
            state = "failed"
        else:
            state = "completed"
        return {"state": state, "output": result.content if isinstance(result, ToolMessage) else "",
                "error": error, "host": {"process_cleanup": cleanup,
                    "tool_message": result.model_dump(mode="json") if isinstance(result, ToolMessage) else None,
                    "facts": facts}}
