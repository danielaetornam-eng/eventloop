"""
Connection utilities for opening connections and starting servers.
"""

from __future__ import annotations

import socket
import ssl
import ssl as ssl_module
import asyncio
from collections.abc import Callable, Awaitable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Optional, Union

from ..core.types import GodLoopError
from ..core.loop import EventLoop
from ..core.context import current_loop
from .reader import StreamReader
from .writer import StreamWriter
from .protocol import StreamReaderProtocol, StreamWriterProtocol, Protocol

__all__ = [
    "open_connection",
    "start_server",
    "open_unix_connection",
    "start_unix_server",
    "Server",
    "UnixServer",
    "ConnectionError",
]


class ConnectionError(GodLoopError, OSError):
    """Connection related errors.
    
    Also derives from :class:`OSError` so that generic ``except OSError``
    handlers catch connection problems.
    """
    pass


@dataclass
class Server(AbstractAsyncContextManager):
    """TCP server wrapper."""
    
    _server: Any
    _loop: EventLoop
    _sockets: list[socket.socket]
    
    async def __aenter__(self) -> "Server":
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
        await self.wait_closed()
    
    def close(self) -> None:
        """Close the server."""
        if hasattr(self._server, "close"):
            self._server.close()
    
    async def wait_closed(self) -> None:
        """Wait for server to close."""
        if hasattr(self._server, "wait_closed"):
            await self._server.wait_closed()
    
    def get_sockets(self) -> list[socket.socket]:
        return self._sockets
    
    @property
    def sockets(self) -> list[socket.socket]:
        """The listening sockets (asyncio compatible)."""
        return self._sockets
    
    def __repr__(self) -> str:
        return f"<Server sockets={len(self._sockets)}>"


@dataclass
class UnixServer(AbstractAsyncContextManager):
    """Unix domain socket server wrapper."""
    
    _server: Any
    _loop: EventLoop
    _path: str
    
    async def __aenter__(self) -> "UnixServer":
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
        await self.wait_closed()
    
    def close(self) -> None:
        if hasattr(self._server, "close"):
            self._server.close()
    
    async def wait_closed(self) -> None:
        if hasattr(self._server, "wait_closed"):
            await self._server.wait_closed()
    
    def get_path(self) -> str:
        return self._path
    
    @property
    def path(self) -> str:
        """The path of the socket (asyncio compatible)."""
        return self._path
    
    def __repr__(self) -> str:
        return f"<UnixServer path={self._path!r}>"


async def start_unix_server(
    client_connected_cb: Callable[[StreamReader, StreamWriter], Awaitable[None]],
    path: str | None = None,
    *,
    loop: EventLoop | None = None,
    limit: int = 2**16,
    **kwargs: Any,
) -> UnixServer:
    """Start a Unix domain socket server."""
    loop = loop or current_loop()
    
    def _create_protocol() -> Protocol:
        reader = StreamReader(limit=limit, loop=loop)
        protocol = StreamReaderProtocol(reader, loop=loop)
        
        original_connection_made = protocol.connection_made
        
        def connection_made(transport: Any) -> None:
            original_connection_made(transport)
            writer = StreamWriter(transport, protocol, loop=loop, limit=limit)
            loop.create_task(client_connected_cb(reader, writer))
        
        protocol.connection_made = connection_made
        return protocol
    
    server = await loop._create_unix_server(_create_protocol, path)
    return UnixServer(server, loop, path or "")


async def open_connection(
    host: str,
    port: int,
    *,
    loop: EventLoop | None = None,
    ssl: ssl.SSLContext | bool | None = None,
    server_hostname: str | None = None,
    family: int = 0,
    proto: int = 0,
    flags: int = 0,
    local_addr: tuple[str, int] | None = None,
    limit: int = 2**16,
    **kwargs: Any,
) -> tuple[StreamReader, StreamWriter]:
    """
    Open a TCP connection.
    
    Returns a pair of (reader, writer).
    """
    loop = loop or current_loop()
    
    # Validate the ssl argument eagerly so that a bad value is reported
    # as a TypeError instead of a connection failure.
    if ssl is not None and ssl is not False and not isinstance(ssl, ssl_module.SSLContext) and ssl is not True:
        raise TypeError(
            "ssl must be an ssl.SSLContext, True, False or None, "
            f"not {type(ssl).__name__}"
        )
    
    reader = StreamReader(limit=limit, loop=loop)
    writer: StreamWriter | None = None
    
    # Create protocol
    protocol = StreamReaderProtocol(reader, loop=loop)
    
    # Create connection
    try:
        transport, _ = await loop._create_connection(
            lambda: protocol,
            host,
            port,
            ssl=ssl,
            server_hostname=server_hostname,
            family=family,
            proto=proto,
            flags=flags,
            local_addr=local_addr,
        )
        
        writer_protocol = StreamWriterProtocol(writer, loop=loop)
        writer = StreamWriter(transport, writer_protocol, loop=loop, limit=limit)
        
        return reader, writer
    except Exception as e:
        reader.set_exception(e)
        raise ConnectionError(f"Failed to connect to {host}:{port}") from e


async def open_unix_connection(
    path: str,
    *,
    loop: EventLoop | None = None,
    limit: int = 2**16,
    **kwargs: Any,
) -> tuple[StreamReader, StreamWriter]:
    """Open a Unix domain socket connection."""
    loop = loop or current_loop()
    
    reader = StreamReader(limit=limit, loop=loop)
    
    protocol = StreamReaderProtocol(reader, loop=loop)
    
    try:
        transport, _ = await loop._create_unix_connection(
            lambda: protocol,
            path,
        )
        
        writer_protocol = StreamWriterProtocol(None, loop=loop)
        writer = StreamWriter(transport, writer_protocol, loop=loop, limit=limit)
        
        return reader, writer
    except Exception as e:
        reader.set_exception(e)
        raise ConnectionError(f"Failed to connect to unix socket {path}") from e


async def start_server(
    client_connected_cb: Callable[[StreamReader, StreamWriter], Awaitable[None]],
    host: str | None = None,
    port: int = 0,
    *,
    loop: EventLoop | None = None,
    ssl: ssl.SSLContext | None = None,
    family: int = 0,
    flags: int = 0,
    sock: socket.socket | None = None,
    backlog: int = 100,
    limit: int = 2**16,
    reuse_address: bool = True,
    reuse_port: bool = False,
    **kwargs: Any,
) -> Server:
    """
    Start a TCP server.
    
    The client_connected_cb will be called for each new connection
    with (reader, writer) pair.
    """
    loop = loop or current_loop()
    
    def _create_protocol() -> Protocol:
        reader = StreamReader(limit=limit, loop=loop)
        protocol = StreamReaderProtocol(reader, loop=loop)
        
        # Wrap the callback
        original_connection_made = protocol.connection_made
        
        def connection_made(transport: Any) -> None:
            original_connection_made(transport)
            writer = StreamWriter(transport, protocol, loop=loop, limit=limit)
            loop.create_task(client_connected_cb(reader, writer))
        
        protocol.connection_made = connection_made
        return protocol
    
    # Create server
    server = await loop._create_server(
        _create_protocol,
        host,
        port,
        ssl=ssl,
        family=family,
        flags=flags,
        sock=sock,
        backlog=backlog,
        reuse_address=reuse_address,
        reuse_port=reuse_port,
    )
    
    # Get sockets
    sockets = []
    if hasattr(server, "sockets"):
        sockets = list(server.sockets)
    
    return Server(server, loop, sockets)


async def open_pipe(
    *,
    loop: EventLoop | None = None,
    limit: int = 2**16,
) -> tuple[StreamReader, StreamWriter]:
    """Create a pipe pair (reader, writer)."""
    loop = loop or current_loop()
    
    reader = StreamReader(limit=limit, loop=loop)
    writer_transport, reader_transport = await loop._create_pipe()
    
    reader_protocol = StreamReaderProtocol(reader, loop=loop)
    reader_protocol.connection_made(reader_transport)
    
    writer = StreamWriter(writer_transport, None, loop=loop, limit=limit)
    
    return reader, writer