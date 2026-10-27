import copy
import hashlib
import inspect
import json
import sys
from dataclasses import dataclass
from pathlib import Path


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


class FocusToolAdapter:
    """Accept dict or AIMessage.tool_calls without importing a model framework.

    submit_tool_calls returns one submission ToolMessage per model call. Actual
    completion travels through result routing to Run.next_event, never as a second
    ToolMessage for that call. The caller retains the Focus checkpoint/ledger.
    """

    def __init__(self, client, tools, *, workspace, before_execute=None):
        if client.authorize is not None:
            raise ValueError("client already has an admission hook; pass it to the adapter instead")
        self.client = client
        self.tools = dict(tools)
        self.workspace = str(Path(workspace).resolve())
        self.before_execute = before_execute
        self._submitted = {}
        client.authorize = self._authorize

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
        elif definition.kind == "process":
            argv = args.get("argv", [])
            if set(args) - {"argv"} or not isinstance(argv, list) or not all(isinstance(part, str) for part in argv):
                raise ValueError("process arguments must be an argv list of strings")
            task["command"] = [*definition.command, *argv]
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
        try:
            await self.client.request("submit", task=task)
        except BaseException:
            if previous is None:
                self._submitted.pop(key, None)
            raise
        return {"role": "tool", "tool_call_id": call_id, "name": name,
                "content": json.dumps({"job_id": task["id"], "state": "accepted"})}

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
            except (RuntimeError, ValueError, KeyError) as error:
                message = {"role": "tool", "tool_call_id": call["id"], "name": call["name"],
                           "content": json.dumps({"state": "rejected", "error": str(error)})}
            messages.append(message)
        return messages

    @staticmethod
    def _key(task):
        owner = task["owner"]
        return owner["context_id"], owner["run_id"], task["id"]

    async def _authorize(self, task):
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
        if self.before_execute is not None:
            result = self.before_execute(copy.deepcopy(task))
            if inspect.isawaitable(result):
                result = await result
            if result is False:
                raise PermissionError("workspace lease or permission no longer valid")
        return True
