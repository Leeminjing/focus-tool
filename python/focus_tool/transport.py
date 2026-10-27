"""Threaded real pipes for Windows Selector loops used by psycopg/Focus."""
import asyncio
import subprocess


class PipeReader:
    def __init__(self, pipe):
        self.pipe = pipe

    async def readline(self):
        return await asyncio.to_thread(self.pipe.readline, 8 << 20)

    async def read(self, size):
        return await asyncio.to_thread(getattr(self.pipe, "read1", self.pipe.read), size)


class PipeWriter:
    def __init__(self, pipe):
        self.pipe = pipe
        self.pending = bytearray()
        self.lock = asyncio.Lock()

    def write(self, data):
        self.pending.extend(data)

    def _flush(self, data):
        self.pipe.write(data)
        self.pipe.flush()

    async def drain(self):
        async with self.lock:
            data = bytes(self.pending)
            self.pending.clear()
            if not data:
                return
            operation = asyncio.create_task(asyncio.to_thread(self._flush, data))
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                # A background write must finish before another frame can write.
                await operation
                raise


class ThreadedProcess:
    def __init__(self, command, cwd=None):
        self.process = subprocess.Popen(command, cwd=cwd, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.stdin, self.stdout, self.stderr = PipeWriter(self.process.stdin), PipeReader(self.process.stdout), PipeReader(self.process.stderr)

    @property
    def returncode(self):
        return self.process.poll()

    async def wait(self):
        return await asyncio.to_thread(self.process.wait)

    def kill(self):
        self.process.kill()

    def close_pipes(self):
        for pipe in (self.process.stdin, self.process.stdout, self.process.stderr):
            pipe.close()
