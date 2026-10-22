"""
Stream writer implementation for async I/O.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Awaitable
from dataclasses import dataclass, field
from typing import Any, Optional, Union

from ..core.types import GodLoopError
from ..core.future import Future
from ..core.loop import EventLoop
from ..core.context import current_loop
from ..core.handle import IOHandle

__all__ = [
    "StreamWriter",
    "FlowControlMixin",
]


class StreamWriter:
    """
    Async stream writer with flow control.
    
    Provides high-level async writing methods with backpressure support.
    """
    
    __slots__ = (
        "_transport",
        "_protocol",
        "_loop",
        "_buffer",
        "_buffer_size",
        "_high_water",
        "_low_water",
        "_paused",
        "_closing",
        "_closed",
        "_waiter",
        "_write_callback",
    )
    
    def __init__(
        self,
        transport: Any,
        protocol: Any,
        loop: EventLoop | None = None,
        limit: int = 2**16,
    ) -> None:
        self._transport = transport
        self._protocol = protocol
        self._loop = loop or current_loop()
        self._buffer = bytearray()
        self._buffer_size = 0
        self._high_water = limit
        self._low_water = limit // 2
        self._paused = False
        self._closing = False
        self._closed = False
        self._waiter: Future[None] | None = None
        self._write_callback: Callable[[], None] | None = None
    
    @property
    def transport(self) -> Any:
        return self._transport
    
    @property
    def protocol(self) -> Any:
        return self._protocol
    
    @property
    def loop(self) -> EventLoop:
        return self._loop
    
    def write(self, data: bytes) -> None:
        """Write data to the stream (non-blocking)."""
        if self._closing:
            return
        
        self._buffer.extend(data)
        self._buffer_size += len(data)
        
        # Check high water mark
        if self._buffer_size >= self._high_water and not self._paused:
            self._pause_writing()
        
        # Try to flush immediately
        self._flush()
    
    def writelines(self, lines: list[bytes]) -> None:
        """Write a list of lines."""
        for line in lines:
            self.write(line)
    
    async def drain(self) -> None:
        """Wait until the write buffer is drained below low water mark."""
        if self._buffer_size <= self._low_water:
            return
        
        if self._closing:
            return
        
        # Create waiter
        future: Future[None] = self._loop.create_future()
        self._waiter = future
        
        try:
            await future
        except Exception:
            future.cancel()
            raise
    
    def _flush(self) -> None:
        """Flush the buffer to the transport."""
        if self._buffer_size == 0:
            return
        
        try:
            # Write to transport
            data = bytes(self._buffer)
            self._transport.write(data)
            self._buffer.clear()
            self._buffer_size = 0
            
            # Check low water mark
            if self._paused and self._buffer_size <= self._low_water:
                self._resume_writing()
            
            # Wake waiter. Capture it first: resolving it can re-enter the
            # draining coroutine, which may install a new waiter.
            waiter = self._waiter
            if waiter is not None and not waiter.done():
                waiter.set_result(None)
            if self._waiter is waiter:
                self._waiter = None
        except Exception as e:
            self._handle_error(e)
    
    def _pause_writing(self) -> None:
        """Pause writing due to high water mark."""
        self._paused = True
        if hasattr(self._transport, "pause_writing"):
            self._transport.pause_writing()
    
    def _resume_writing(self) -> None:
        """Resume writing after low water mark."""
        self._paused = False
        if hasattr(self._transport, "resume_writing"):
            self._transport.resume_writing()
    
    def _handle_error(self, exc: BaseException) -> None:
        """Handle write error."""
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_exception(exc)
        if self._waiter is waiter:
            self._waiter = None
        
        if self._protocol and hasattr(self._protocol, "connection_lost"):
            self._protocol.connection_lost(exc)
    
    def close(self) -> None:
        """Close the writer."""
        if self._closed:
            return
        
        self._closing = True
        
        # Flush remaining data
        if self._buffer_size > 0:
            self._flush()
        
        # Close transport
        if hasattr(self._transport, "close"):
            self._transport.close()
        
        self._closed = True
    
    def can_write_eof(self) -> bool:
        """Check if transport supports write_eof."""
        return hasattr(self._transport, "write_eof")
    
    def write_eof(self) -> None:
        """Write EOF to the transport."""
        if self._closing or self._closed:
            return
        
        if hasattr(self._transport, "write_eof"):
            self._transport.write_eof()
        else:
            self.close()
    
    def is_closing(self) -> bool:
        return self._closing
    
    async def wait_closed(self) -> None:
        """Wait until the writer is fully closed."""
        if self._closed:
            return
        
        # Wait for transport to close
        future: Future[None] = self._loop.create_future()
        
        def _on_close() -> None:
            if not future.done():
                future.set_result(None)
        
        if hasattr(self._transport, "add_close_callback"):
            self._transport.add_close_callback(_on_close)
        else:
            # Poll-based fallback
            async def _poll() -> None:
                while not self._closed:
                    await asyncio.sleep(0.01)
                future.set_result(None)
            self._loop.create_task(_poll())
        
        try:
            await future
        except Exception:
            future.cancel()
            raise
    
    def get_write_buffer_size(self) -> int:
        return self._buffer_size
    
    def get_write_buffer_limits(self) -> tuple[int, int]:
        return (self._low_water, self._high_water)
    
    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        if high is not None:
            self._high_water = high
        if low is not None:
            self._low_water = low
        if low is not None and high is not None and low >= high:
            raise ValueError("Low water mark must be less than high water mark")
    
    def __repr__(self) -> str:
        return f"<StreamWriter buffer={self._buffer_size} closing={self._closing} closed={self._closed}>"


class FlowControlMixin:
    """Mixin for protocols that need flow control."""
    
    def pause_writing(self) -> None:
        """Called when write buffer exceeds high water mark."""
        pass
    
    def resume_writing(self) -> None:
        """Called when write buffer drops below low water mark."""
        pass
    
    def connection_lost(self, exc: BaseException | None) -> None:
        """Called when connection is lost."""
        pass