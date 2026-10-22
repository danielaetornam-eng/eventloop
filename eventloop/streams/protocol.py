"""
Protocol base classes for async I/O.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Optional

from ..core.loop import EventLoop
from ..core.context import current_loop
from .reader import StreamReader
from .writer import StreamWriter

__all__ = [
    "Protocol",
    "BaseProtocol",
    "DatagramProtocol",
    "BufferedProtocol",
]


class BaseProtocol(ABC):
    """Base protocol class."""
    
    __slots__ = ("_loop", "_transport")
    
    def __init__(self, loop: EventLoop | None = None) -> None:
        self._loop = loop or current_loop()
        self._transport: Any = None
    
    @property
    def loop(self) -> EventLoop:
        return self._loop
    
    @property
    def transport(self) -> Any:
        return self._transport
    
    def connection_made(self, transport: Any) -> None:
        """Called when a connection is made."""
        self._transport = transport
    
    def connection_lost(self, exc: BaseException | None) -> None:
        """Called when the connection is lost."""
        self._transport = None
    
    def pause_writing(self) -> None:
        """Called when the transport's buffer goes over the high-water mark."""
        pass
    
    def resume_writing(self) -> None:
        """Called when the transport's buffer drains below the low-water mark."""
        pass


class Protocol(BaseProtocol):
    """
    Stream protocol for TCP and pipe connections.
    
    Provides data_received and eof_received callbacks.
    """
    
    def data_received(self, data: bytes) -> None:
        """Called when data is received."""
        pass
    
    def eof_received(self) -> bool:
        """Called when EOF is received. Return True to close transport."""
        return False


class BufferedProtocol(BaseProtocol):
    """
    Buffered protocol for high-performance I/O.
    
    Allows the protocol to manage its own receive buffer.
    """
    
    def get_buffer(self, sizehint: int) -> memoryview:
        """Return a buffer for receiving data."""
        return memoryview(bytearray(sizehint))
    
    def buffer_updated(self, nbytes: int) -> None:
        """Called when the buffer has been updated with nbytes of data."""
        pass
    
    def eof_received(self) -> bool:
        return False


class DatagramProtocol(BaseProtocol):
    """
    Datagram protocol for UDP connections.
    """
    
    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        """Called when a datagram is received."""
        pass
    
    def error_received(self, exc: Exception) -> None:
        """Called when an error is received."""
        pass


class StreamReaderProtocol(Protocol):
    """Protocol that feeds data to a StreamReader."""
    
    def __init__(
        self,
        reader: StreamReader,
        loop: EventLoop | None = None,
    ) -> None:
        super().__init__(loop)
        self._reader = reader
    
    def data_received(self, data: bytes) -> None:
        self._reader.feed_data(data)
    
    def eof_received(self) -> bool:
        self._reader.feed_eof()
        return True
    
    def connection_lost(self, exc: BaseException | None) -> None:
        if exc:
            self._reader.set_exception(exc)
        else:
            self._reader.feed_eof()
        super().connection_lost(exc)


class StreamWriterProtocol(Protocol):
    """Protocol for stream writer."""
    
    def __init__(
        self,
        writer: StreamWriter,
        loop: EventLoop | None = None,
    ) -> None:
        super().__init__(loop)
        self._writer = writer
    
    def pause_writing(self) -> None:
        self._writer._pause_writing()
    
    def resume_writing(self) -> None:
        self._writer._resume_writing()
    
    def connection_lost(self, exc: BaseException | None) -> None:
        if exc:
            self._writer._handle_error(exc)
        super().connection_lost(exc)