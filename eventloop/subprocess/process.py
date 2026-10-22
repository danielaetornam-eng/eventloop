"""
Subprocess management for async execution.
"""

from __future__ import annotations

import sys
import os
import signal
import subprocess
import asyncio
from collections.abc import Callable, Awaitable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import Any, Optional, Union, Sequence

from ..core.types import GodLoopError
from ..core.loop import EventLoop
from ..core.context import current_loop
from ..core.future import Future
from ..core.task import create_task
from ..streams.reader import StreamReader
from ..streams.writer import StreamWriter

__all__ = [
    "Process",
    "create_subprocess_exec",
    "create_subprocess_shell",
    "SubprocessTransport",
    "SubprocessProtocol",
]


class SubprocessError(GodLoopError):
    """Subprocess related errors."""
    pass


class SubprocessProtocol(asyncio.SubprocessProtocol):
    """Protocol for subprocess communication."""
    
    def __init__(self, process: "Process | None" = None) -> None:
        # asyncio.SubprocessProtocol does not set up the base attributes, so
        # do it here.
        self._loop: Any = None
        self._extra: dict[str, Any] = {}
        self._process = process
        self._stdin_writer: StreamWriter | None = None
        self._stdout_reader: StreamReader | None = None
        self._stderr_reader: StreamReader | None = None
    
    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        pass
    
    def pipe_connection_made(self, fd: int, transport: asyncio.BaseTransport) -> None:
        from ..core.context import current_loop
        loop = self._loop or current_loop()
        self._loop = loop
        if fd == 0:
            self._stdin_writer = StreamWriter(transport, self, loop=loop)
        elif fd == 1:
            self._stdout_reader = StreamReader(loop=loop)
            transport.set_protocol(self._stdout_reader)
        elif fd == 2:
            self._stderr_reader = StreamReader(loop=loop)
            transport.set_protocol(self._stderr_reader)
    
    def pipe_data_received(self, fd: int, data: bytes) -> None:
        if fd == 1 and self._stdout_reader:
            self._stdout_reader.feed_data(data)
        elif fd == 2 and self._stderr_reader:
            self._stderr_reader.feed_data(data)
    
    def pipe_connection_lost(self, fd: int, exc: Exception | None) -> None:
        if fd == 1 and self._stdout_reader:
            if exc:
                self._stdout_reader.set_exception(exc)
            else:
                self._stdout_reader.feed_eof()
        elif fd == 2 and self._stderr_reader:
            if exc:
                self._stderr_reader.set_exception(exc)
            else:
                self._stderr_reader.feed_eof()
    
    def process_exited(self) -> None:
        # The Process wrapper may not be wired up yet (the process can exit
        # before it is created), so guard against it.
        if self._process is None:
            return
        self._process._returncode = self._transport.get_returncode()
        if self._process._wait_future and not self._process._wait_future.done():
            self._process._wait_future.set_result(self._process._returncode)
    
    @property
    def stdin(self) -> StreamWriter | None:
        return self._stdin_writer
    
    @property
    def stdout(self) -> StreamReader | None:
        return self._stdout_reader
    
    @property
    def stderr(self) -> StreamReader | None:
        return self._stderr_reader


class SubprocessTransport:
    """Transport for subprocess communication."""
    
    def __init__(self, process: "Process", transport: asyncio.BaseTransport) -> None:
        self._process = process
        self._transport = transport
    
    def get_pid(self) -> int:
        return self._transport.get_pid()
    
    def get_returncode(self) -> int:
        return self._transport.get_returncode()
    
    def close(self) -> None:
        self._transport.close()
    
    def send_signal(self, sig: int) -> None:
        self._transport.send_signal(sig)
    
    def terminate(self) -> None:
        self._transport.terminate()
    
    def kill(self) -> None:
        self._transport.kill()


class Process:
    """
    Async process wrapper.
    
    Provides high-level async interface for subprocess management.
    """
    
    __slots__ = (
        "_loop",
        "_transport",
        "_protocol",
        "_pid",
        "_returncode",
        "_wait_future",
        "_stdin",
        "_stdout",
        "_stderr",
    )
    
    def __init__(
        self,
        loop: EventLoop,
        transport: asyncio.BaseTransport,
        protocol: SubprocessProtocol,
    ) -> None:
        self._loop = loop
        self._transport = transport
        self._protocol = protocol
        self._pid = transport.get_pid()
        self._returncode: int | None = None
        self._wait_future: Future[int] | None = None
        self._stdin = protocol.stdin
        self._stdout = protocol.stdout
        self._stderr = protocol.stderr
    
    @property
    def pid(self) -> int:
        return self._pid
    
    @property
    def returncode(self) -> int | None:
        return self._returncode
    
    @property
    def stdin(self) -> StreamWriter | None:
        return self._stdin
    
    @property
    def stdout(self) -> StreamReader | None:
        return self._stdout
    
    @property
    def stderr(self) -> StreamReader | None:
        return self._stderr
    
    async def wait(self) -> int:
        """Wait for process to exit."""
        if self._returncode is not None:
            return self._returncode
        
        # The transport owns the exit future, so it works even if the
        # process exited before this wrapper was created.
        wait_future = getattr(self._transport, "_wait_future", None)
        if wait_future is None:
            if self._wait_future is None:
                self._wait_future = self._loop.create_future()
            wait_future = self._wait_future
        
        code = await wait_future
        self._returncode = code
        return code
    
    def send_signal(self, sig: int) -> None:
        """Send a signal to the process."""
        self._transport.send_signal(sig)
    
    def terminate(self) -> None:
        """Terminate the process."""
        self._transport.terminate()
    
    def kill(self) -> None:
        """Kill the process."""
        self._transport.kill()
    
    async def communicate(self, input: bytes | None = None) -> tuple[bytes, bytes]:
        """Communicate with process (send input, get output)."""
        if input and self._stdin:
            self._stdin.write(input)
            await self._stdin.drain()
            self._stdin.write_eof()
        
        # Read both streams at the same time so that a full pipe on one of
        # them cannot deadlock the other.
        async def _read_all(reader: StreamReader | None) -> bytes:
            if reader is None:
                return b""
            return await reader.read()
        
        stdout_task = create_task(_read_all(self._stdout))
        stderr_task = create_task(_read_all(self._stderr))
        
        stdout_data = await stdout_task
        stderr_data = await stderr_task
        
        await self.wait()
        
        return stdout_data, stderr_data
    
    def __repr__(self) -> str:
        return f"<Process pid={self._pid} returncode={self._returncode}>"


async def _create_subprocess(
    loop: EventLoop,
    args: Sequence[str],
    *,
    stdin: int | None = subprocess.PIPE,
    stdout: int | None = subprocess.PIPE,
    stderr: int | None = subprocess.PIPE,
    shell: bool = False,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    **kwargs: Any,
) -> Process:
    """Internal function to create a subprocess."""
    
    protocol_factory = lambda: SubprocessProtocol(None)  # Will be replaced
    
    if shell:
        transport, protocol = await loop._subprocess_shell(
            protocol_factory,
            " ".join(args),
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            cwd=cwd,
            env=env,
            **kwargs,
        )
    else:
        transport, protocol = await loop._subprocess_exec(
            protocol_factory,
            *args,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            cwd=cwd,
            env=env,
            **kwargs,
        )
    
    # Fix protocol reference
    process = Process(loop, transport, protocol)
    protocol._process = process
    
    return process


async def create_subprocess_exec(
    *args: str,
    loop: EventLoop | None = None,
    stdin: int | None = subprocess.PIPE,
    stdout: int | None = subprocess.PIPE,
    stderr: int | None = subprocess.PIPE,
    **kwargs: Any,
) -> Process:
    """
    Create a subprocess with exec.
    
    Args:
        *args: Command and arguments
        loop: Event loop to use
        stdin/stdout/stderr: Pipe configuration
        **kwargs: Additional arguments (cwd, env, etc.)
    """
    loop = loop or current_loop()
    return await _create_subprocess(
        loop, args, stdin=stdin, stdout=stdout, stderr=stderr,
        shell=False, **kwargs
    )


async def create_subprocess_shell(
    cmd: str,
    *,
    loop: EventLoop | None = None,
    stdin: int | None = subprocess.PIPE,
    stdout: int | None = subprocess.PIPE,
    stderr: int | None = subprocess.PIPE,
    **kwargs: Any,
) -> Process:
    """
    Create a subprocess with shell.
    
    Args:
        cmd: Shell command string
        loop: Event loop to use
        stdin/stdout/stderr: Pipe configuration
        **kwargs: Additional arguments (cwd, env, etc.)
    """
    loop = loop or current_loop()
    return await _create_subprocess(
        loop, [cmd], stdin=stdin, stdout=stdout, stderr=stderr,
        shell=True, **kwargs
    )