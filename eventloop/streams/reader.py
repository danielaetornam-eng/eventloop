"""
Stream reader implementation for async I/O.
"""

from __future__ import annotations

import sys
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Optional, Union

from ..core.types import GodLoopError
from ..core.future import Future
from ..core.loop import EventLoop
from ..core.context import current_loop

__all__ = [
    "StreamReader",
    "StreamReaderProtocol",
    "LimitOverrunError",
    "IncompleteReadError",
]


class LimitOverrunError(GodLoopError):
    """Raised when stream exceeds buffer limit."""
    
    def __init__(self, message: str, consumed: int) -> None:
        super().__init__(message)
        self.consumed = consumed


class IncompleteReadError(GodLoopError):
    """Raised when stream ends before expected bytes are read."""
    
    def __init__(self, partial: bytes, expected: int) -> None:
        super().__init__(f"Incomplete read: got {len(partial)} bytes, expected {expected}")
        self.partial = partial
        self.expected = expected


class StreamReaderProtocol:
    """Protocol for stream reader data source."""
    
    def feed_data(self, data: bytes) -> None: ...
    def feed_eof(self) -> None: ...
    def set_exception(self, exc: BaseException) -> None: ...


@dataclass
class StreamReader(StreamReaderProtocol):
    """
    Async stream reader with buffering and flow control.
    
    Provides high-level async reading methods with configurable buffer limits.
    """
    
    __slots__ = (
        "_buffer",
        "_buffer_size",
        "_limit",
        "_eof",
        "_waiter",
        "_exception",
        "_loop",
        "_paused",
    )
    
    def __init__(self, limit: int = 2**16, loop: EventLoop | None = None) -> None:
        self._buffer = bytearray()
        self._buffer_size = 0
        self._limit = limit
        self._eof = False
        self._waiter: Future[None] | None = None
        self._exception: BaseException | None = None
        self._loop = loop or current_loop()
        self._paused = False
    
    @property
    def limit(self) -> int:
        return self._limit
    
    @property
    def buffer(self) -> bytearray:
        return self._buffer
    
    @property
    def at_eof(self) -> bool:
        return self._eof and self._buffer_size == 0
    
    @property
    def exception(self) -> BaseException | None:
        return self._exception
    
    def feed_data(self, data: bytes) -> None:
        """Feed data into the buffer."""
        if self._eof:
            return
        
        if self._buffer_size + len(data) > self._limit:
            # Buffer limit exceeded
            self._exception = LimitOverrunError(
                "Buffer limit exceeded", self._buffer_size
            )
            self._wake_waiter()
            return
        
        self._buffer.extend(data)
        self._buffer_size += len(data)
        self._wake_waiter()
    
    def feed_eof(self) -> None:
        """Signal end of stream."""
        self._eof = True
        self._wake_waiter()
    
    def set_exception(self, exc: BaseException) -> None:
        """Set an exception on the stream."""
        self._exception = exc
        self._wake_waiter()
    
    def _wake_waiter(self) -> None:
        """Wake up any waiting coroutine."""
        # Capture the waiter first: resolving it can re-enter the reading
        # coroutine, which may install a *new* waiter. Only clear the slot
        # if it still holds the one we woke, otherwise the new waiter would
        # be stranded and the reader would never resume.
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        if self._waiter is waiter:
            self._waiter = None
    
    def _create_waiter(self) -> Future[None]:
        """Create a new waiter future."""
        future: Future[None] = self._loop.create_future()
        self._waiter = future
        return future
    
    async def _wait(self, func: str) -> None:
        """Wait for data or EOF."""
        if self._exception:
            raise self._exception
        
        if self._eof and self._buffer_size == 0:
            return
        
        # A read can be satisfied immediately if data is already buffered, or
        # if the stream has ended (the caller then sees EOF).
        if self._buffer_size > 0 or self._eof:
            return
        
        waiter = self._create_waiter()
        try:
            await waiter
        except Exception:
            waiter.cancel()
            raise
        
        if self._exception:
            raise self._exception
    
    async def read(self, n: int = -1) -> bytes:
        """Read up to n bytes."""
        if n == 0:
            return b""
        
        if n < 0:
            # Read until EOF. Reading in chunks keeps the buffer bounded.
            blocks: list[bytes] = []
            while True:
                block = await self.read(self._limit)
                if not block:
                    break
                blocks.append(block)
            return b"".join(blocks)
        
        # Read *up to* n bytes: return as soon as any data is available.
        # Use readexactly() when exactly n bytes are required.
        while not self._buffer_size:
            await self._wait("read")
            if self._eof:
                break
        
        if self._buffer_size >= n:
            data = bytes(self._buffer[:n])
            del self._buffer[:n]
            self._buffer_size -= n
            return data
        
        # EOF (or fewer bytes than requested) - return what we have.
        data = bytes(self._buffer)
        self._buffer.clear()
        self._buffer_size = 0
        return data
    
    async def readexactly(self, n: int) -> bytes:
        """Read exactly n bytes."""
        if n < 0:
            raise ValueError("readexactly() n must be >= 0")
        
        chunks: list[bytes] = []
        remaining = n
        while remaining > 0:
            chunk = await self.read(remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        
        data = b"".join(chunks)
        if len(data) < n:
            raise IncompleteReadError(data, n)
        return data
    
    async def readuntil(self, separator: bytes) -> bytes:
        """Read until separator is found."""
        if not separator:
            raise ValueError("Separator cannot be empty")
        
        sep_len = len(separator)
        
        while True:
            # Check if separator is in buffer
            idx = self._buffer.find(separator)
            if idx != -1:
                # Found separator
                end = idx + sep_len
                data = bytes(self._buffer[:end])
                del self._buffer[:end]
                self._buffer_size -= end
                return data
            
            # Wait for more data
            await self._wait("readuntil")
            
            if self._eof:
                # EOF without finding separator
                data = bytes(self._buffer)
                self._buffer.clear()
                self._buffer_size = 0
                raise IncompleteReadError(data, -1)
    
    async def readline(self) -> bytes:
        """Read a line (ending with newline)."""
        return await self.readuntil(b"\n")
    
    async def readchunk(self) -> bytes:
        """Read a chunk of data (whatever is available)."""
        await self._wait("readchunk")
        if self._buffer_size == 0:
            return b""
        
        data = bytes(self._buffer)
        self._buffer.clear()
        self._buffer_size = 0
        return data
    
    def __aiter__(self) -> "StreamReader":
        return self
    
    async def __anext__(self) -> bytes:
        # Stop iterating cleanly at end of stream.
        if self._eof and self._buffer_size == 0:
            raise StopAsyncIteration
        try:
            line = await self.readline()
        except IncompleteReadError:
            # The stream ended without a trailing newline.
            raise StopAsyncIteration from None
        if not line:
            raise StopAsyncIteration
        return line
    
    def __repr__(self) -> str:
        return f"<StreamReader buffer={self._buffer_size} eof={self._eof} limit={self._limit}>"