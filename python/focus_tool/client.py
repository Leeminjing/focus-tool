import asyncio
import inspect
import json
from collections import defaultdict, deque
from .transport import ThreadedProcess


class RemoteRejected(RuntimeError):
    """An explicit response: this request was not accepted."""


class TransportError(RuntimeError):
    """The request may have reached the runtime; reconcile using its original ID."""


class _Mailbox:
    def __init__(self):
        self.items = deque()
        self.ready = asyncio.Event()

    def put(self, event):
        self.items.append(event)
        self.ready.set()

    def take(self):
        event = self.items.popleft()
        if not self.items:
            self.ready.clear()
        return event


class RuntimeClient:
    """One daemon connection shared by many Context/Run mailboxes."""

    def __init__(self, process, authorize=None):
        self.process = process
        self.authorize = authorize
        self._serial = 0
        self._pending = {}
        self._mailboxes = defaultdict(_Mailbox)
        self._closed_runs = set()
        self.on_ack = None
        self.on_run_closed = None
        self._closed = asyncio.Event()
        self._authorization_tasks = set()
        self.stderr = bytearray()
        self._reader = asyncio.create_task(self._read())
        self._stderr_reader = asyncio.create_task(self._read_stderr())

    @classmethod
    async def start(cls, command, *, journal, concurrency=4, capacity=128, per_run_capacity=0, cwd=None, authorize=None):
        argv = [*command, "-journal", str(journal), "-concurrency", str(concurrency),
                "-capacity", str(capacity), "-per-run-capacity", str(per_run_capacity), "-require-approval=true"]
        try:
            process = await asyncio.create_subprocess_exec(*argv, cwd=cwd, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, limit=8 << 20)
        except NotImplementedError:
            process = ThreadedProcess(argv, cwd=cwd)
        return cls(process, authorize)

    async def request(self, method, **fields):
        if self._closed.is_set():
            raise RemoteRejected("request not sent; runtime connection closed: " + self.stderr.decode(errors="replace"))
        self._serial += 1
        identity = self._serial
        frame = json.dumps({"id": identity, "method": method, **fields}, allow_nan=False).encode() + b"\n"
        future = asyncio.get_running_loop().create_future()
        self._pending[identity] = future
        try:
            self.process.stdin.write(frame)
            await self.process.stdin.drain()
            return await future
        except (BrokenPipeError, ConnectionError, OSError) as error:
            raise TransportError("connection failed; request acceptance is unknown") from error
        finally:
            self._pending.pop(identity, None)

    async def _read_stderr(self):
        while data := await self.process.stderr.read(4096):
            self.stderr.extend(data)
            del self.stderr[:-8192]

    async def _read(self):
        failure = "runtime connection closed"
        try:
            while line := await self.process.stdout.readline():
                frame = json.loads(line)
                if frame["type"] == "response":
                    future = self._pending.get(frame["id"])
                    if future is not None and not future.done():
                        if frame.get("error"):
                            error_type = TransportError if frame.get("error_code") == "acceptance_unknown" else RemoteRejected
                            future.set_exception(error_type(frame["error"]))
                        else:
                            future.set_result(frame.get("result"))
                elif frame["type"] == "event":
                    event = frame["event"]
                    owner = event["task"]["owner"]
                    key = owner["context_id"], owner["run_id"]
                    if key not in self._closed_runs:
                        self._mailboxes[key].put(event)
                elif frame["type"] == "authorize":
                    task = asyncio.create_task(self._answer_authorization(frame))
                    self._authorization_tasks.add(task)
                    task.add_done_callback(self._authorization_tasks.discard)
                else:
                    raise RuntimeError("unknown runtime frame")
        except Exception as error:
            failure = str(error)
        finally:
            self._closed.set()
            for future in list(self._pending.values()):
                if not future.done():
                    future.set_exception(TransportError(failure + "; request acceptance is unknown"))

    async def _answer_authorization(self, frame):
        allowed, reason = False, "no execution admission hook registered"
        try:
            if self.authorize is not None:
                result = self.authorize(frame["task"])
                if inspect.isawaitable(result):
                    result = await result
                allowed = result is not False
                reason = "" if allowed else "execution admission denied"
        except Exception as error:
            reason = str(error)
        try:
            await self.request("authorize", token=frame["token"], allowed=allowed, reason=reason)
        except RuntimeError:
            # Cancellation can expire a gate while its policy hook is running.
            pass

    async def open_run(self, context_id, run_id, revision=1):
        owner = {"context_id": context_id, "run_id": run_id}
        await self.request("open_run", owner=owner, revision=revision)
        return Run(self, owner, revision)

    async def _next(self, owner):
        key = owner["context_id"], owner["run_id"]
        mailbox = self._mailboxes[key]
        while True:
            if key in self._closed_runs:
                raise RemoteRejected("Run closed; remaining results belong to runtime_archive")
            if mailbox.items:
                # No suspension between dequeue and handing the event to Run.
                return mailbox.take()
            if self._closed.is_set():
                raise TransportError("runtime closed while waiting for a result")
            ready = asyncio.create_task(mailbox.ready.wait())
            closed = asyncio.create_task(self._closed.wait())
            try:
                await asyncio.wait((ready, closed), return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in (ready, closed):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(ready, closed, return_exceptions=True)

    def _restore(self, owner, event):
        key = owner["context_id"], owner["run_id"]
        if key not in self._closed_runs:
            mailbox = self._mailboxes[key]
            mailbox.items.appendleft(event)
            mailbox.ready.set()

    async def close(self):
        try:
            if not self._closed.is_set():
                await asyncio.wait_for(self.request("close"), timeout=12)
            await asyncio.wait_for(self.process.wait(), timeout=5)
        finally:
            if self.process.returncode is None:
                self.process.kill()
                await self.process.wait()
            self._reader.cancel()
            for task in list(self._authorization_tasks):
                task.cancel()
            await asyncio.gather(self._reader, self._stderr_reader, *self._authorization_tasks, return_exceptions=True)
            if hasattr(self.process, "close_pipes"):
                self.process.close_pipes()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()


class Run:
    """The result consumer in the original architecture; ack follows checkpointing."""

    def __init__(self, client, owner, revision):
        self.client, self.owner, self.revision = client, dict(owner), revision
        self._last_ack = 0
        self._delivered = set()

    async def revise(self, revision):
        await self.client.request("open_run", owner=self.owner, revision=revision)
        self.revision = revision

    async def next_event(self):
        while True:
            event = await self.client._next(self.owner)
            if event["task"]["owner"] != self.owner:
                raise RuntimeError("cross-run result delivery")
            sequence = event["sequence"]
            if sequence <= self._last_ack or sequence in self._delivered:
                continue
            if event["task"]["revision"] != self.revision:
                # Rejection is determined by the already-durable plan revision.
                try:
                    await self.ack(event)
                except BaseException:
                    self.client._restore(self.owner, event)
                    raise
                continue
            self._delivered.add(sequence)
            return event

    async def ack(self, event):
        if event["task"]["owner"] != self.owner:
            raise ValueError("cannot acknowledge another run's result")
        await self.client.request("ack", owner=self.owner, sequence=event["sequence"])
        self._last_ack = max(self._last_ack, event["sequence"])
        self._delivered.discard(event["sequence"])
        if self.client.on_ack is not None:
            self.client.on_ack(event)

    async def cancel(self, job_id):
        await self.client.request("cancel", owner=self.owner, task_id=job_id)

    async def status(self, job_id):
        return await self.client.request("status", owner=self.owner, task_id=job_id)

    async def close(self):
        receipt = await self.client.request("close_run", owner=self.owner)
        key = self.owner["context_id"], self.owner["run_id"]
        self.client._closed_runs.add(key)
        mailbox = self.client._mailboxes.pop(key, None)
        if mailbox is not None:
            mailbox.ready.set()
        self._delivered.clear()
        if self.client.on_run_closed is not None:
            self.client.on_run_closed(self.owner)
        return receipt

    async def archived_events(self, after=0):
        return await self.client.request("archive_events", owner=self.owner, after=after)
