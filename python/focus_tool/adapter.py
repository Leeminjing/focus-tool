import copy
import hashlib
import inspect
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from .client import RemoteRejected
import asyncio


@dataclass(frozen=True)
class ToolDefinition:
    """Trusted local registration. Expose these tools with explicit start-job semantics."""

    kind: str
    module: str = ""
    function: str = ""
    command: tuple = ()
    interpreter: str = sys.executable
    resources: tuple = ()  # Pairs of explicit shared resource name and read/write mode.
    timeout_ms: int = 30000
    reuse: bool = False  # Trusted stateless/leaf contract; never inferred from call arguments.
    handler: object = None  # Host-only callable(task, cancellation_event); kept in this process.

    def __post_init__(self):
        if type(self.reuse) is not bool or (self.reuse and self.kind != "python"):
            raise ValueError("reuse must be a boolean trusted Python-tool declaration")
        if self.kind == "host" and not (inspect.iscoroutinefunction(self.handler) or
                inspect.iscoroutinefunction(getattr(self.handler, "__call__", None))):
            raise ValueError("host tools require a trusted async handler")


class FocusToolAdapter:
    """Accept dict or AIMessage.tool_calls without importing a model framework.

    submit_tool_calls returns one submission ToolMessage per model call. Actual
    completion travels through result routing to Run.next_event, never as a second
    ToolMessage for that call. The caller retains the Focus checkpoint/ledger.
    """

    def __init__(self, client, tools, *, workspace, before_execute=None):
        if client.authorize is not None:
            raise ValueError("client already has an admission hook; pass it to the adapter instead")
        if getattr(client, "host_execute", None) is not None:
            raise ValueError("client already has a host executor")
        self.client = client
        self.tools = dict(tools)
        self.workspace = str(Path(workspace).resolve())
        self.before_execute = before_execute
        self._submitted = {}
        self._uncertain = set()
        self._host_registrations = {}
        client.authorize = self._authorize
        client.on_ack = self._acknowledged
        client.on_run_closed = self._run_closed
        if any(definition.kind == "host" for definition in self.tools.values()):
            client.host_execute = self._execute_host

    def _acknowledged(self, event):
        key = self._key(event["task"])
        self._submitted.pop(key, None)
        self._uncertain.discard(key)
        self._host_registrations.pop(key, None)

    def _run_closed(self, owner):
        for key in list(self._submitted):
            if key[:2] == (owner["context_id"], owner["run_id"]):
                self._submitted.pop(key)
                self._uncertain.discard(key)
                self._host_registrations.pop(key, None)

    @staticmethod
    def job_id(parent_message_id, call_id):
        identity = json.dumps([parent_message_id, call_id], ensure_ascii=False).encode()
        return hashlib.sha256(identity).hexdigest()

    def _task(self, run, parent_message_id, call_id, name, args, dependencies, bindings):
        if not parent_message_id or not call_id:
            raise ValueError("stable parent message ID and tool call ID are required")
        definition = self.tools[name]
        if not isinstance(args, dict):
            raise ValueError("tool arguments must be an object")
        task = {
            "id": self.job_id(parent_message_id, call_id), "owner": dict(run.owner),
            "revision": run.revision, "kind": definition.kind, "directory": self.workspace,
            "timeout_ms": definition.timeout_ms,
            "resources": [{"name": name, "mode": mode} for name, mode in definition.resources],
            "dependencies": list(dependencies), "bindings": copy.deepcopy(bindings),
        }
        if definition.kind == "python":
            task["python"] = {"interpreter": definition.interpreter, "module": definition.module,
                              "function": definition.function, "args": copy.deepcopy(args)}
            if definition.reuse:
                task["python"]["reuse"] = True
        elif definition.kind == "process":
            argv = args.get("argv", [])
            if set(args) - {"argv"} or not isinstance(argv, list) or not all(isinstance(part, str) for part in argv):
                raise ValueError("process arguments must be an argv list of strings")
            task["command"] = [*definition.command, *argv]
        elif definition.kind == "host":
            task["host"] = {"tool": name, "args": copy.deepcopy(args)}
        else:
            raise ValueError("unknown executor kind")
        # Fail before execution on values that cannot cross the local protocol.
        return json.loads(json.dumps(task, allow_nan=False))

    async def submit(self, run, *, parent_message_id, call_id, name, args, dependencies=(), bindings=None):
        task = self._task(run, parent_message_id, call_id, name, args, dependencies, bindings or {})
        key = self._key(task)
        previous = self._submitted.get(key)
        if previous is not None and previous != task:
            raise ValueError("tool call identity has conflicting arguments")
        self._submitted[key] = copy.deepcopy(task)
        if task["kind"] == "host":
            self._host_registrations.setdefault(key, self.tools[name].handler)
        try:
            status = await self.client.request("submit", task=task)
        except RemoteRejected as error:
            if key in self._uncertain:
                return self._unknown_receipt(task, call_id, name, error)
            if previous is None:
                self._submitted.pop(key, None)
                self._host_registrations.pop(key, None)
            raise
        except RuntimeError as error:
            # Keep the registered identity so a delayed execution check/retry can
            # still be reconciled. Never invent a replacement call ID.
            self._uncertain.add(key)
            return self._unknown_receipt(task, call_id, name, error)
        except asyncio.CancelledError:
            self._uncertain.add(key)
            raise
        self._uncertain.discard(key)
        if status and status.get("state") in {"completed", "failed", "cancelled", "skipped", "unknown"}:
            # A historical retry will not emit a fresh event/Ack. It cannot
            # require execution authorization again, so retain no registration.
            self._submitted.pop(key, None)
            self._host_registrations.pop(key, None)
        return {"role": "tool", "tool_call_id": call_id, "name": name,
                "content": json.dumps({"job_id": task["id"], "state": "accepted"})}

    @staticmethod
    def _unknown_receipt(task, call_id, name, error):
        return {"role": "tool", "tool_call_id": call_id, "name": name,
                "content": json.dumps({"job_id": task["id"], "state": "acceptance_unknown",
                                       "error": str(error), "reconcile": "query or resubmit the original job_id"})}

    async def reconcile(self, run, parent_message_id, call_id):
        identity = self.job_id(parent_message_id, call_id)
        status = await run.status(identity)
        self._uncertain.discard((run.owner["context_id"], run.owner["run_id"], identity))
        if status["state"] in {"completed", "failed", "cancelled", "skipped", "unknown"}:
            self._submitted.pop((run.owner["context_id"], run.owner["run_id"], identity), None)
            self._host_registrations.pop((run.owner["context_id"], run.owner["run_id"], identity), None)
        return status

    async def resume_dispatch(self):
        """Call after original-ID submissions rebuild trusted registrations.

        The daemon rejects an incomplete queued inventory; its execution-time
        hook still verifies the exact parameters and current permission/lease.
        """
        registered = [{"owner": {"context_id": context, "run_id": run}, "task_id": job}
                      for context, run, job in self._submitted]
        await self.client.resume_dispatch(registered)

    async def submit_tool_calls(self, run, message):
        parent = message.get("id") if isinstance(message, dict) else message.id
        calls = message.get("tool_calls", []) if isinstance(message, dict) else message.tool_calls
        if not parent:
            raise ValueError("stable parent message ID is required")
        seen = set()
        for call in calls:
            if not call["id"] or call["id"] in seen:
                raise ValueError("duplicate tool call ID in model message")
            seen.add(call["id"])
        # Submission is not atomic. Preserve one paired message even when some
        # calls are accepted and another hits capacity or fails registration.
        messages = []
        for call in calls:
            try:
                message = await self.submit(run, parent_message_id=parent, call_id=call["id"],
                                            name=call["name"], args=call["args"])
            except (RemoteRejected, ValueError, KeyError) as error:
                message = {"role": "tool", "tool_call_id": call["id"], "name": call["name"],
                           "content": json.dumps({"state": "rejected", "error": str(error)})}
            messages.append(message)
        return messages

    @staticmethod
    def _key(task):
        owner = task["owner"]
        return owner["context_id"], owner["run_id"], task["id"]

    async def _authorize(self, task, *, host_entry=False):
        expected = copy.deepcopy(self._submitted.get(self._key(task)))
        if expected is None:
            raise PermissionError("task is not registered by this adapter")
        # The runtime may bind declared dependency values immediately before execution.
        # All other identity, command, workspace and resource fields must still match.
        actual = copy.deepcopy(task)
        for value in (expected, actual):
            if value.get("python"):
                for argument in value.get("bindings", {}):
                    value["python"]["args"].pop(argument, None)
        # Go omits zero-valued optional fields when serializing a task.
        canonical = lambda value: {key: item for key, item in value.items() if item not in (None, {}, [], "", 0)}
        if canonical(actual) != canonical(expected):
            raise PermissionError("execution parameters differ from registered call")
        # Host admission runs at actual host entry, after its dispatch wait.
        if self.before_execute is not None and (task["kind"] != "host" or host_entry):
            result = self.before_execute(copy.deepcopy(task))
            if inspect.isawaitable(result):
                result = await result
            if result is False:
                raise PermissionError("workspace lease or permission no longer valid")
        return True

    async def _execute_host(self, task, cancel):
        handler = self._host_registrations.get(self._key(task))
        try:
            await self._authorize(task, host_entry=True)
        except Exception as error:
            return {"state": "failed", "error": str(error), "host": {"process_cleanup": "not_started"}}
        if cancel.is_set():
            return {"state": "cancelled", "host": {"process_cleanup": "not_started"}}
        if handler is None:
            return {"state": "failed", "error": "host registration unavailable", "host": {"process_cleanup": "not_started"}}
        return await handler(copy.deepcopy(task), cancel)
