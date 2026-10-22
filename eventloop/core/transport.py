"""
Transport and server implementations for GodLoop.
"""

from __future__ import annotations

import errno
import os
import socket
import ssl
import sys
import threading
import time
from collections import deque
from collections.abc import Callable, Sequence
from typing import Any, Optional

from ..core.types import GodLoopError

__all__ = [
    "Transport",
    "SelectorTransport",
    "SelectorServer",
    "ReadTransport",
    "WriteTransport",
    "TransportClosedError",
]

class TransportClosedError(GodLoopError):
    """Raised when writing to a closed transport."""
    pass


class _NullProtocol:
    """A protocol that discards everything sent to it."""
    
    def connection_made(self, transport: Any) -> None:
        pass
    
    def connection_lost(self, exc: BaseException | None) -> None:
        pass
    
    def data_received(self, data: bytes) -> None:
        pass
    
    def eof_received(self) -> bool:
        return False
    
    def pause_writing(self) -> None:
        pass
    
    def resume_writing(self) -> None:
        pass


class ReadTransport:
    """Mixin for transports that support reading."""
    
    def pause_reading(self) -> None:
        """Pause delivery of incoming data."""
        raise NotImplementedError
    
    def resume_reading(self) -> None:
        """Resume delivery of incoming data."""
        raise NotImplementedError
    
    def is_reading(self) -> bool:
        """Return True if reading is enabled."""
        raise NotImplementedError


class WriteTransport:
    """Mixin for transports that support writing."""
    
    def write(self, data: bytes) -> None:
        """Write data to the transport."""
        raise NotImplementedError
    
    def writelines(self, list_of_data: Sequence[bytes]) -> None:
        """Write a list of data bytes to the transport."""
        raise NotImplementedError
    
    def can_write_eof(self) -> bool:
        """Return True if the transport supports write_eof()."""
        raise NotImplementedError
    
    def write_eof(self) -> None:
        """Close the write end after flushing buffered data."""
        raise NotImplementedError
    
    def pause_writing(self) -> None:
        """Pause the flow of data through the transport."""
        raise NotImplementedError
    
    def resume_writing(self) -> None:
        """Resume the flow of data through the transport."""
        raise NotImplementedError


class Transport(ReadTransport, WriteTransport):
    """
    Base class for all transports.
    
    A transport moves data between the event loop and an I/O resource
    (usually a socket or a pipe).
    """
    
    def __init__(self, loop: Any, protocol: Any) -> None:
        self._loop = loop
        self._protocol = protocol
        self._closing = False
        self._closed = False
        self._paused = False
        self._extra: dict[str, Any] = {}
    
    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """Get optional transport information."""
        return self._extra.get(name, default)
    
    def set_extra_info(self, name: str, value: Any) -> None:
        """Set optional transport information."""
        self._extra[name] = value
    
    def get_protocol(self) -> Any:
        return self._protocol
    
    def set_protocol(self, protocol: Any) -> None:
        self._protocol = protocol
    
    def is_closing(self) -> bool:
        return self._closing
    
    def close(self) -> None:
        """Close the transport."""
        if self._closing:
            return
        self._closing = True
        self._call_connection_lost(None)
    
    def abort(self) -> None:
        """Close the transport immediately, discarding buffered data."""
        if self._closed:
            return
        self._closing = True
        self._call_connection_lost(ConnectionResetError("Transport aborted"))
    
    def _call_connection_lost(self, exc: BaseException | None) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._protocol.connection_lost(exc)
        except Exception:
            pass
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} closing={self._closing}>"


class SelectorTransport(Transport):
    """
    A transport backed by a non-blocking socket registered with the
    event loop's selector.
    """
    
    def __init__(
        self,
        loop: Any,
        protocol: Any,
        sock: socket.socket,
        *,
        server: "SelectorServer | None" = None,
        sslcontext: ssl.SSLContext | None = None,
        server_side: bool = False,
        server_hostname: str | None = None,
    ) -> None:
        super().__init__(loop, protocol)
        self._sock = sock
        self._server = server
        self._sslcontext = sslcontext
        self._server_side = server_side
        self._server_hostname = server_hostname
        self._buffer = bytearray()
        self._conn_lost = 0
        self._read_paused = False
        self._write_paused = False
        self._eof_written = False
        self._peername: Any = None
        self._sockname: Any = None
        self._addr_family = sock.family
        
        # TLS state
        self._sslobject: ssl.SSLSocket | None = None
        self._ssl_handshake_done = False
        self._ssl_handshake_started = False
        self._ssl_pending: bytearray = bytearray()
        self._handshake_future: Any = None
        
        try:
            self._peername = sock.getpeername()
        except OSError:
            pass
        try:
            self._sockname = sock.getsockname()
        except OSError:
            pass
        
        self._extra["socket"] = sock
        self._extra["sockname"] = self._sockname
        self._extra["peername"] = self._peername
        
        if sslcontext is not None:
            self._extra["ssl_object"] = None
            self._start_tls()
    
    # -- TLS ----------------------------------------------------------------
    def _start_tls(self) -> None:
        """Wrap the socket in a TLS layer."""
        assert self._sslcontext is not None
        try:
            sslobject = self._sslcontext.wrap_socket(
                self._sock,
                server_side=self._server_side,
                server_hostname=self._server_hostname,
                do_handshake_on_connect=False,
            )
        except ssl.SSLError as exc:
            self._fatal_error(exc, message="Failed to set up TLS")
            return
        self._sslobject = sslobject
        # wrap_socket() detaches the original socket, so the SSL socket has
        # to be used for all file-descriptor operations from now on.
        self._sock = sslobject
        self._extra["socket"] = sslobject
        self._handshake_future = self._loop.create_future()
    
    def _do_handshake(self) -> bool:
        """Advance the TLS handshake. Returns True when it has completed."""
        if self._sslobject is None or self._ssl_handshake_done:
            return True
        
        self._ssl_handshake_started = True
        try:
            self._sslobject.do_handshake()
        except ssl.SSLWantReadError:
            # Wait for more data to arrive.
            return False
        except ssl.SSLWantWriteError:
            # Retry as soon as the socket is writable.
            try:
                self._loop.add_writer(self._sock.fileno(), self._handshake_ready)
            except Exception:
                pass
            return False
        except (ssl.SSLError, OSError) as exc:
            self._handshake_failed(exc)
            return False
        
        self._ssl_handshake_done = True
        self._extra["ssl_object"] = self._sslobject
        try:
            self._loop.remove_writer(self._sock.fileno())
        except Exception:
            pass
        
        future = self._handshake_future
        if future is not None and not future.done():
            future.set_result(None)
        
        # Flush application data that was written before the handshake.
        if self._ssl_pending:
            pending = bytes(self._ssl_pending)
            self._ssl_pending.clear()
            self.write(pending)
        
        return True
    
    def _handshake_ready(self) -> None:
        """Retry the handshake when the socket becomes writable."""
        if self._closing:
            return
        self._do_handshake()
    
    def start_handshake(self) -> None:
        """Start the TLS handshake.
        
        A client has to send its ClientHello without waiting for the socket
        to become readable, otherwise the handshake would deadlock: the
        server only replies once it has received the ClientHello.
        """
        if self._sslobject is None or self._ssl_handshake_done:
            return
        self._do_handshake()
    
    def _handshake_failed(self, exc: BaseException) -> None:
        """Report a failed TLS handshake."""
        future = self._handshake_future
        if future is not None and not future.done():
            future.set_exception(exc)
        self._fatal_error(exc, message="TLS handshake failed")
    
    async def wait_for_handshake(self) -> None:
        """Wait until the TLS handshake has completed."""
        future = self._handshake_future
        if future is None:
            return
        await future
    
    def _ssl_recv(self, size: int) -> bytes:
        """Receive (and decrypt) data."""
        if self._sslobject is None:
            return self._sock.recv(size)
        return self._sslobject.recv(size)
    
    def _ssl_send(self, data: bytes) -> int:
        """Send (and encrypt) data."""
        if self._sslobject is None:
            return self._sock.send(data)
        return self._sslobject.send(data)
    
    # -- Reading -----------------------------------------------------------
    def pause_reading(self) -> None:
        if self._closing or self._read_paused:
            return
        self._read_paused = True
        self._loop.remove_reader(self._sock.fileno())
    
    def resume_reading(self) -> None:
        if self._closing or not self._read_paused:
            return
        self._read_paused = False
        self._loop.add_reader(self._sock.fileno(), self._read_ready)
    
    def is_reading(self) -> bool:
        return not self._read_paused and not self._closing
    
    # -- Writing -----------------------------------------------------------
    def write(self, data: bytes) -> None:
        if not data:
            return
        if self._closing or self._eof_written:
            raise TransportClosedError("Cannot write to closing transport")
        
        if self._sslobject is not None and not self._ssl_handshake_done:
            # The handshake has to complete before application data can be
            # sent, so buffer the data in the meantime.
            self._ssl_pending.extend(data)
            self._do_handshake()
            return
        
        if not self._buffer:
            # Try to send immediately.
            try:
                sent = self._ssl_send(data)
            except (BlockingIOError, InterruptedError, ssl.SSLWantWriteError):
                sent = 0
            except OSError as exc:
                self._fatal_error(exc)
                return
            
            if sent == len(data):
                return
            data = data[sent:]
        
        self._buffer.extend(data)
        self._maybe_pause_protocol()
        self._ensure_writer()
    
    def writelines(self, list_of_data: Sequence[bytes]) -> None:
        self.write(b"".join(list_of_data))
    
    def can_write_eof(self) -> bool:
        return True
    
    def write_eof(self) -> None:
        if self._eof_written or self._closing:
            return
        self._eof_written = True
        if not self._buffer:
            self._shutdown_write()
    
    def pause_writing(self) -> None:
        self._write_paused = True
    
    def resume_writing(self) -> None:
        self._write_paused = False
        self._maybe_resume_protocol()
    
    def _maybe_pause_protocol(self) -> None:
        if self._write_paused:
            return
        high = self._loop._buffer_high_water if hasattr(self._loop, "_buffer_high_water") else 65536
        if len(self._buffer) > high:
            self._write_paused = True
            try:
                self._protocol.pause_writing()
            except Exception:
                pass
    
    def _maybe_resume_protocol(self) -> None:
        if not self._write_paused:
            return
        low = self._loop._buffer_low_water if hasattr(self._loop, "_buffer_low_water") else 32768
        if len(self._buffer) <= low:
            self._write_paused = False
            try:
                self._protocol.resume_writing()
            except Exception:
                pass
    
    # -- Selector plumbing -------------------------------------------------
    def _ensure_writer(self) -> None:
        """Make sure the socket is watched for writability while buffered."""
        if self._closing or self._eof_written:
            return
        if not self._buffer:
            return
        try:
            self._loop.add_writer(self._sock.fileno(), self._write_ready)
        except Exception:
            pass
    
    def _read_ready(self) -> None:
        if self._closing:
            return
        if self._sslobject is not None and not self._ssl_handshake_done:
            if not self._do_handshake():
                return
        profiler = getattr(self._loop, "profiler", None)
        start = time.perf_counter() if profiler is not None else 0.0
        try:
            data = self._ssl_recv(65536)
        except (BlockingIOError, InterruptedError, ssl.SSLWantReadError):
            return
        except ssl.SSLZeroReturnError:
            self._eof_received()
            return
        except OSError as exc:
            self._fatal_error(exc)
            return
        
        if profiler is not None:
            profiler.record_io(self._sock.fileno(), time.perf_counter() - start)
        
        if not data:
            self._eof_received()
            return
        
        try:
            self._protocol.data_received(data)
        except Exception as exc:
            self._fatal_error(exc, message="Fatal error: protocol.data_received() call failed.")
    
    def _write_ready(self) -> None:
        if self._closing:
            return
        if self._sslobject is not None and not self._ssl_handshake_done:
            # Still handshaking: the handshake callback drives this.
            self._do_handshake()
            return
        if not self._buffer:
            self._stop_writer()
            return
        
        try:
            sent = self._ssl_send(bytes(self._buffer))
        except (BlockingIOError, InterruptedError, ssl.SSLWantWriteError):
            return
        except OSError as exc:
            self._fatal_error(exc)
            return
        
        if sent:
            del self._buffer[:sent]
        
        if not self._buffer:
            self._stop_writer()
            self._maybe_resume_protocol()
            if self._eof_written:
                self._shutdown_write()
    
    def _stop_writer(self) -> None:
        try:
            self._loop.remove_writer(self._sock.fileno())
        except Exception:
            pass
    
    def _eof_received(self) -> None:
        try:
            keep_open = self._protocol.eof_received()
        except Exception:
            keep_open = False
        if not keep_open:
            self.close()
    
    def _shutdown_write(self) -> None:
        try:
            self._sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass
    
    def _fatal_error(self, exc: BaseException, message: str = "Fatal error on transport") -> None:
        try:
            self._loop.call_exception_handler({
                "message": message,
                "exception": exc,
                "transport": self,
                "protocol": self._protocol,
            })
        except Exception:
            pass
        self._close(exc)
    
    def _close(self, exc: BaseException | None = None) -> None:
        if self._closed:
            return
        self._closing = True
        
        # Detach from the selector.
        try:
            self._loop.remove_reader(self._sock.fileno())
        except Exception:
            pass
        try:
            self._loop.remove_writer(self._sock.fileno())
        except Exception:
            pass
        
        try:
            self._sock.close()
        except OSError:
            pass
        
        if self._server is not None:
            self._server._detach(self)
        
        self._call_connection_lost(exc)
    
    def close(self) -> None:
        """Close the transport, flushing what is already buffered."""
        if self._closing:
            return
        self._closing = True
        
        # Flush the buffer.
        while self._buffer:
            try:
                sent = self._sock.send(bytes(self._buffer))
            except (BlockingIOError, InterruptedError):
                break
            except OSError:
                break
            if not sent:
                break
            del self._buffer[:sent]
        
        self._close(None)
    
    def __repr__(self) -> str:
        return (
            f"<SelectorTransport fd={self._sock.fileno()} "
            f"peername={self._peername} closing={self._closing}>"
        )


class _PipeTransport:
    """A transport that stands in for one of a subprocess' pipes."""
    
    __slots__ = ("_loop", "_fileobj", "_fd", "_protocol", "_closing", "_extra")
    
    def __init__(self, loop: Any, fileobj: Any, fd: int) -> None:
        self._loop = loop
        self._fileobj = fileobj
        self._fd = fd
        self._protocol: Any = None
        self._closing = False
        self._extra: dict[str, Any] = {}
    
    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return self._extra.get(name, default)
    
    def get_protocol(self) -> Any:
        return self._protocol
    
    def set_protocol(self, protocol: Any) -> None:
        self._protocol = protocol
    
    def write(self, data: bytes) -> None:
        try:
            self._fileobj.write(data)
            self._fileobj.flush()
        except (OSError, ValueError):
            pass
    
    def writelines(self, list_of_data: Sequence[bytes]) -> None:
        self.write(b"".join(list_of_data))
    
    def can_write_eof(self) -> bool:
        return self._fd == 0
    
    def write_eof(self) -> None:
        try:
            self._fileobj.close()
        except (OSError, ValueError):
            pass
    
    def pause_reading(self) -> None:
        pass
    
    def resume_reading(self) -> None:
        pass
    
    def is_reading(self) -> bool:
        return True
    
    def pause_writing(self) -> None:
        pass
    
    def resume_writing(self) -> None:
        pass
    
    def is_closing(self) -> bool:
        return self._closing
    
    def close(self) -> None:
        self._closing = True
        try:
            self._fileobj.close()
        except (OSError, ValueError):
            pass
    
    def abort(self) -> None:
        self.close()
    
    def __repr__(self) -> str:
        return f"<PipeTransport fd={self._fd}>"


class _SubprocessTransport(Transport):
    """
    A transport that drives a child process.
    
    It exposes the process' stdin/stdout/stderr as file descriptors that
    the event loop can watch.
    """
    
    def __init__(self, loop: Any, protocol: Any, proc: Any) -> None:
        super().__init__(loop, protocol)
        self._proc = proc
        self._pid = proc.pid
        self._returncode: int | None = None
        self._pipes: dict[int, Any] = {}
        self._readers: dict[int, int] = {}
        self._writers: dict[int, int] = {}
        self._exited = False
        self._write_queue: deque[bytes] = deque()
        self._write_event = threading.Event()
        # The transport owns the "process exited" future, so it does not
        # depend on the protocol having a reference to the Process object
        # (which is only wired up after the process has been spawned).
        self._wait_future = loop.create_future()
        
        self._extra["subprocess"] = proc
        self._extra["pid"] = proc.pid
    
    def get_pid(self) -> int:
        return self._pid
    
    def get_returncode(self) -> int | None:
        return self._returncode
    
    def _attach_pipe(
        self,
        fd: int,
        fileobj: Any,
        mode: str,
        pipe_transport: "_PipeTransport | None" = None,
    ) -> None:
        """Attach a pipe file object to the given descriptor number.
        
        Pipes are plain file handles, which cannot be watched by a selector
        on every platform (Windows in particular), so they are serviced by
        dedicated threads that hand the data back to the event loop.
        """
        self._pipes[fd] = fileobj
        try:
            fileno = fileobj.fileno()
        except (AttributeError, OSError, ValueError):
            return
        
        if mode == "read":
            self._readers[fd] = fileno
            thread = threading.Thread(
                target=self._pump_reader,
                args=(fd, fileobj),
                daemon=True,
                name=f"GodLoop-ProcRead-{fd}",
            )
            thread.start()
        else:
            self._writers[fd] = fileno
            thread = threading.Thread(
                target=self._pump_writer,
                args=(fd, fileobj),
                daemon=True,
                name=f"GodLoop-ProcWrite-{fd}",
            )
            thread.start()
    
    def _pump_reader(self, fd: int, fileobj: Any) -> None:
        """Read from a pipe and deliver the data to the protocol."""
        # ``read()`` on a pipe blocks until the full buffer is filled, so use
        # ``read1()`` (or a raw read) to get whatever is available.
        reader = getattr(fileobj, "read1", None)
        if reader is None:
            def reader(size: int = 65536) -> bytes:  # type: ignore[misc]
                try:
                    return os.read(fileobj.fileno(), size)
                except (OSError, ValueError):
                    return b""
        
        while True:
            try:
                data = reader(65536)
            except (OSError, ValueError):
                data = b""
            if not data:
                # EOF on this pipe.
                try:
                    self._loop.call_soon_threadsafe(self._pipe_lost, fd, None)
                except Exception:
                    pass
                return
            try:
                self._loop.call_soon_threadsafe(self._pipe_data, fd, data)
            except Exception:
                return
    
    def _pipe_data(self, fd: int, data: bytes) -> None:
        """Deliver pipe data to the protocol (event loop thread)."""
        try:
            self._protocol.pipe_data_received(fd, data)
        except Exception as exc:
            self._loop.call_exception_handler({
                "message": "Exception in protocol.pipe_data_received()",
                "exception": exc,
            })
    
    def _pipe_lost(self, fd: int, exc: BaseException | None) -> None:
        """Signal that a pipe reached EOF (event loop thread)."""
        self._readers.pop(fd, None)
        try:
            self._protocol.pipe_connection_lost(fd, exc)
        except Exception:
            pass
    
    def _pump_writer(self, fd: int, fileobj: Any) -> None:
        """Write queued data to a pipe."""
        while True:
            self._write_event.wait()
            self._write_event.clear()
            while self._write_queue:
                chunk = self._write_queue[0]
                try:
                    fileobj.write(chunk)
                    fileobj.flush()
                except (OSError, ValueError):
                    self._write_queue.clear()
                    return
                self._write_queue.popleft()
    
    def _write_pipe(self, fd: int) -> None:
        fileobj = self._pipes.get(fd)
        if fileobj is None:
            return
        try:
            fileobj.flush()
        except (OSError, ValueError):
            pass
    
    def _start_waiting(self) -> None:
        """Start watching for the process to exit."""
        def _on_exit() -> None:
            self._returncode = self._proc.wait()
            self._exited = True
            # Resolve the wait future from the loop thread.
            try:
                self._loop.call_soon_threadsafe(self._process_exited)
            except Exception:
                pass
        
        # Use a watcher thread: the event loop must not block on wait().
        thread = threading.Thread(target=_on_exit, daemon=True, name="GodLoop-ProcWait")
        thread.start()
    
    def _process_exited(self) -> None:
        """Called in the event loop thread once the process has exited."""
        if not self._wait_future.done():
            self._wait_future.set_result(self._returncode)
        try:
            self._protocol.process_exited()
        except Exception:
            pass
    
    def write(self, data: bytes) -> None:
        """Write to the process' stdin."""
        if 0 not in self._pipes:
            raise TransportClosedError("Process stdin is not available")
        self._write_queue.append(data)
        self._write_event.set()
    
    def writelines(self, list_of_data: Sequence[bytes]) -> None:
        self.write(b"".join(list_of_data))
    
    def can_write_eof(self) -> bool:
        return 0 in self._pipes
    
    def write_eof(self) -> None:
        fileobj = self._pipes.get(0)
        if fileobj is None:
            return
        # Let the writer thread drain what is left, then close.
        self._write_event.set()
        self._writers.pop(0, None)
    
    def pause_reading(self) -> None:
        pass
    
    def resume_reading(self) -> None:
        pass

    def is_reading(self) -> bool:
        return bool(self._readers)
    
    def pause_writing(self) -> None:
        for fileno in self._writers.values():
            self._loop.remove_writer(fileno)
    
    def resume_writing(self) -> None:
        pass
    
    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        
        # Stop the writer thread.
        self._write_event.set()
        
        if self._proc.poll() is None:
            try:
                self._proc.terminate()
            except OSError:
                pass
        
        self._call_connection_lost(None)
    
    def abort(self) -> None:
        if self._closed:
            return
        self._closing = True
        if self._proc.poll() is None:
            try:
                self._proc.kill()
            except OSError:
                pass
        self._call_connection_lost(ConnectionResetError("Transport aborted"))
    
    def send_signal(self, sig: int) -> None:
        self._proc.send_signal(sig)
    
    def terminate(self) -> None:
        self._proc.terminate()
    
    def kill(self) -> None:
        self._proc.kill()
    
    def __repr__(self) -> str:
        return f"<SubprocessTransport pid={self._pid} returncode={self._returncode}>"


class SelectorServer:
    """
    A TCP server that accepts connections on one or more sockets.
    """
    
    def __init__(
        self,
        loop: Any,
        sockets: Sequence[socket.socket],
        protocol_factory: Callable[[], Any],
        *,
        sslcontext: ssl.SSLContext | None = None,
    ) -> None:
        self._loop = loop
        self._sockets = list(sockets)
        self._protocol_factory = protocol_factory
        self._sslcontext = sslcontext
        self._active_count = 0
        self._serving = False
        self._closed = False
        self._waiters: list[Any] = []
        
        self._start_serving()
    
    def _start_serving(self) -> None:
        if self._closed or self._serving:
            return
        self._serving = True
        for sock in self._sockets:
            sock.setblocking(False)
            self._loop.add_reader(
                sock.fileno(), lambda s=sock: self._accept_connection(s)
            )
    
    def _accept_connection(self, sock: socket.socket) -> None:
        try:
            conn, addr = sock.accept()
        except (BlockingIOError, InterruptedError):
            return
        except OSError as exc:
            if exc.errno in (errno.EMFILE, errno.ENFILE):
                self._loop.call_exception_handler({
                    "message": "Socket accept() failed.",
                    "exception": exc,
                })
            return
        
        conn.setblocking(False)
        try:
            protocol = self._protocol_factory()
        except Exception as exc:
            conn.close()
            self._loop.call_exception_handler({
                "message": "Accepting connection failed.",
                "exception": exc,
            })
            return
        
        transport = SelectorTransport(
            self._loop,
            protocol,
            conn,
            server=self,
            sslcontext=self._sslcontext,
            server_side=self._sslcontext is not None,
        )
        self._active_count += 1
        try:
            protocol.connection_made(transport)
        except Exception as exc:
            transport.close()
            self._loop.call_exception_handler({
                "message": "Accepting connection failed.",
                "exception": exc,
            })
            return
        
        # Start reading from the new connection. The transport may have
        # replaced the socket with a TLS wrapper, so its file descriptor is
        # used rather than the one of the accepted socket.
        self._loop.add_reader(transport._sock.fileno(), transport._read_ready)
    
    def _detach(self, transport: SelectorTransport) -> None:
        self._active_count -= 1
        if self._active_count <= 0:
            self._wakeup_waiters()
    
    def get_loop(self) -> Any:
        return self._loop
    
    def is_serving(self) -> bool:
        return self._serving
    
    def start_serving(self) -> None:
        self._start_serving()
    
    def serve_forever(self) -> None:
        """Serve until the server is closed."""
        self._start_serving()
        try:
            while not self._closed:
                self._loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self.close()
    
    def get_sockets(self) -> list[socket.socket]:
        return list(self._sockets)
    
    @property
    def sockets(self) -> list[socket.socket]:
        """The listening sockets (asyncio compatible)."""
        return list(self._sockets)
    
    def close(self) -> None:
        """Stop serving and close the listening sockets."""
        if self._closed:
            return
        self._closed = True
        self._serving = False
        
        for sock in self._sockets:
            try:
                self._loop.remove_reader(sock.fileno())
            except Exception:
                pass
            try:
                sock.close()
            except OSError:
                pass
        
        self._wakeup_waiters()
    
    def _wakeup_waiters(self) -> None:
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._waiters.clear()
    
    async def wait_closed(self) -> None:
        """Wait until the server is closed and all connections are done."""
        if self._closed and self._active_count <= 0:
            return
        waiter = self._loop.create_future()
        self._waiters.append(waiter)
        try:
            await waiter
        finally:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
    
    def __repr__(self) -> str:
        return f"<SelectorServer sockets={len(self._sockets)} active={self._active_count}>"
