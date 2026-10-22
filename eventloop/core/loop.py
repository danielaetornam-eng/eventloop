"""
Core EventLoop implementation for GodLoop.
"""

from __future__ import annotations

import sys
import os
import time
import errno
import signal
import socket
import selectors
import ssl
import subprocess
import threading
import weakref
import traceback
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable, Coroutine, Awaitable
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from typing import (
    Any,
    Generic,
    TypeVar,
    Optional,
    Union,
    overload,
)
from typing_extensions import Self

from .types import (
    LoopState,
    HandleState,
    LoopConfig,
    GodLoopError,
    LoopClosedError,
    InvalidStateError,
    CancelledError,
)
from .context import (
    LoopContext,
    ContextManager,
    current_loop,
    set_loop_context,
    reset_loop_context,
    get_loop_context,
    run_with_context,
)
from .handle import (
    Handle,
    TimerHandle,
    IOHandle,
    CallbackHandle,
    HandleFactory,
    create_handle,
    create_timer_handle,
    create_io_handle,
)
from .future import Future, ensure_future
from .task import Task, create_task, current_task, all_tasks
from .transport import (
    SelectorTransport,
    SelectorServer,
    _NullProtocol,
    _SubprocessTransport,
    _PipeTransport,
)
from ..timers import TimerWheel, HierarchicalTimerWheel
from ..timers.deadline import DeadlineTracker
from ..scheduler import WorkStealingScheduler, SchedulerConfig

__all__ = [
    "EventLoop",
    "new_event_loop",
    "get_event_loop",
    "set_event_loop",
    "run",
    "SelectorBackend",
    "create_selector_backend",
]

T = TypeVar("T")


class SelectorBackend(ABC):
    """Abstract base class for I/O selector backends."""
    
    @abstractmethod
    def register(self, fd: int, events: int, handle: IOHandle) -> None: ...
    
    @abstractmethod
    def unregister(self, fd: int) -> None: ...
    
    @abstractmethod
    def modify(self, fd: int, events: int, handle: IOHandle | None = None) -> None: ...
    
    @abstractmethod
    def select(self, timeout: float | None) -> list[tuple[IOHandle, int]]: ...
    
    @abstractmethod
    def close(self) -> None: ...
    
    @property
    @abstractmethod
    def name(self) -> str: ...


class SelectorBackendImpl(SelectorBackend):
    """Standard selector backend using Python's selectors module."""
    
    def __init__(self) -> None:
        self._selector = selectors.DefaultSelector()
        self._handles: dict[int, IOHandle] = {}
    
    def register(self, fd: int, events: int, handle: IOHandle) -> None:
        self._selector.register(fd, events, handle)
        self._handles[fd] = handle
    
    def unregister(self, fd: int) -> None:
        try:
            self._selector.unregister(fd)
        except (KeyError, ValueError, OSError):
            pass
        self._handles.pop(fd, None)
    
    def modify(self, fd: int, events: int, handle: IOHandle | None = None) -> None:
        if handle is not None:
            self._handles[fd] = handle
        self._selector.modify(fd, events)
    
    def select(self, timeout: float | None) -> list[tuple[IOHandle, int]]:
        events = self._selector.select(timeout)
        return [(key.data, key.events) for key, _ in events]
    
    def close(self) -> None:
        self._selector.close()
        self._handles.clear()
    
    @property
    def name(self) -> str:
        return "selectors.DefaultSelector"


class IoUringBackend(SelectorBackendImpl):
    """io_uring backend for Linux (requires the io-uring package).
    
    Note: submitting real I/O operations through io_uring (SQE/CQE) is not
    implemented yet. The backend therefore falls back to the regular
    selector for I/O readiness, so enabling it never breaks networking.
    It is the place where a true io_uring implementation belongs.
    """
    
    def __init__(self, entries: int = 256) -> None:
        super().__init__()
        self._entries = entries
        self._ring: Any = None
        try:
            import io_uring
            self._ring = io_uring.IoUring(entries)
        except ImportError:
            self._ring = None
    
    @property
    def available(self) -> bool:
        """Whether the io_uring package could be imported."""
        return self._ring is not None
    
    def close(self) -> None:
        if self._ring is not None:
            try:
                self._ring.close()
            except Exception:
                pass
            self._ring = None
        super().close()
    
    @property
    def name(self) -> str:
        return "io_uring" if self.available else "selector (io_uring unavailable)"


def _resolve_sslcontext(ssl_arg: Any, *, server_side: bool) -> ssl.SSLContext | None:
    """Turn the ``ssl`` argument of the connection helpers into a context.
    
    ``None`` and ``False`` mean "no TLS". ``True`` means "use the default
    context" (a client context with certificate verification enabled, or a
    server context without it).
    """
    if ssl_arg is None or ssl_arg is False:
        return None
    if ssl_arg is True:
        if server_side:
            return ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        return ssl.create_default_context()
    if isinstance(ssl_arg, ssl.SSLContext):
        return ssl_arg
    raise TypeError(
        "ssl must be an ssl.SSLContext, True, False or None, "
        f"not {type(ssl_arg).__name__}"
    )


def create_selector_backend(config: LoopConfig) -> SelectorBackend:
    """Create the appropriate selector backend based on config and platform."""
    if config.use_io_uring and sys.platform == "linux":
        return IoUringBackend(config.io_uring_entries)
    
    # On Windows the selector backend is used. A dedicated IOCP/Proactor
    # backend is not implemented yet.
    return SelectorBackendImpl()


@dataclass
class EventLoop:
    """
    The main event loop class.
    
    This is the central coordinator for all asynchronous operations.
    It manages I/O, timers, tasks, and callbacks.
    """
    
    _config: LoopConfig = field(default_factory=LoopConfig)
    _state: LoopState = field(default=LoopState.CREATED, init=False)
    _selector: SelectorBackend = field(init=False)
    _timer_wheel: TimerWheel | HierarchicalTimerWheel = field(init=False)
    _deadline_tracker: DeadlineTracker = field(init=False)
    _context_manager: ContextManager = field(init=False)
    _handle_factory: HandleFactory = field(init=False)
    
    # Task management
    _pending_tasks: deque[Task[Any]] = field(default_factory=deque, init=False)
    _running_tasks: set[Task[Any]] = field(default_factory=set, init=False)
    _all_tasks: set[Task[Any]] = field(default_factory=set, init=False)
    _task_counter: int = field(default=0, init=False)
    
    # Callback queues
    _ready_callbacks: deque[CallbackHandle] = field(default_factory=deque, init=False)
    _scheduled_callbacks: list[TimerHandle] = field(default_factory=list, init=False)
    
    # I/O handles
    _io_handles: dict[int, IOHandle] = field(default_factory=dict, init=False)
    _registered_fds: set[int] = field(default_factory=set, init=False)
    
    # Self-pipe used to wake the loop from other threads
    _wakeup_read: Any = field(default=None, init=False)
    _wakeup_write: Any = field(default=None, init=False)
    
    # Exception handling
    _exception_handler: Callable[[dict[str, Any]], None] | None = field(default=None, init=False)
    
    # Debug
    _debug: bool = field(default=False, init=False)
    _slow_callback_duration: float = field(default=0.1, init=False)
    _slow_task_duration: float = field(default=0.1, init=False)
    
    # Scheduler
    _scheduler: WorkStealingScheduler | None = field(default=None, init=False)
    
    # Instrumentation (tracer / profiler), set via set_tracer / set_profiler
    _tracer: Any = field(default=None, init=False)
    _profiler: Any = field(default=None, init=False)
    
    # Signal handling
    _signal_handlers: dict[int, list[Callable]] = field(default_factory=dict, init=False)
    
    # Thread safety
    _thread_id: int = field(default_factory=threading.get_ident, init=False)
    _closing: bool = field(default=False, init=False)
    
    # Weak reference for internal use
    _weakref: weakref.ref = field(init=False)
    
    def __post_init__(self) -> None:
        self._selector = create_selector_backend(self._config)
        self._context_manager = ContextManager(self)
        self._handle_factory = HandleFactory(self)
        self._deadline_tracker = DeadlineTracker()
        self._weakref = weakref.ref(self)
        
        # Initialize timer wheel
        if self._config.use_hierarchical_wheel:
            self._timer_wheel = HierarchicalTimerWheel(
                tick_duration=self._config.timer_resolution,
            )
        else:
            self._timer_wheel = TimerWheel(
                tick_duration=self._config.timer_resolution,
            )
        
        # Initialize scheduler if enabled
        if self._config.enable_work_stealing:
            scheduler_config = SchedulerConfig(
                num_workers=self._config.worker_threads,
                queue_size=self._config.max_tasks_per_worker,
            )
            self._scheduler = WorkStealingScheduler(self, scheduler_config)
        
        # Set up the cross-thread wakeup pipe.
        self._setup_wakeup()
    
    @property
    def config(self) -> LoopConfig:
        return self._config
    
    @property
    def state(self) -> LoopState:
        return self._state
    
    @property
    def selector(self) -> SelectorBackend:
        return self._selector
    
    @property
    def timer_wheel(self) -> TimerWheel | HierarchicalTimerWheel:
        return self._timer_wheel
    
    @property
    def deadline_tracker(self) -> DeadlineTracker:
        return self._deadline_tracker
    
    @property
    def scheduler(self) -> WorkStealingScheduler | None:
        return self._scheduler
    
    @property
    def tracer(self) -> Any:
        """The tracer attached to this loop (if any)."""
        return self._tracer
    
    @property
    def profiler(self) -> Any:
        """The profiler attached to this loop (if any)."""
        return self._profiler
    
    def set_tracer(self, tracer: Any) -> None:
        """Attach (or detach, with None) a tracer to this loop."""
        self._tracer = tracer
    
    def set_profiler(self, profiler: Any) -> None:
        """Attach (or detach, with None) a profiler to this loop."""
        self._profiler = profiler
    
    def get_debug(self) -> bool:
        return self._debug
    
    def set_debug(self, enabled: bool) -> None:
        self._debug = enabled
    
    def is_running(self) -> bool:
        return self._state == LoopState.RUNNING
    
    def is_closed(self) -> bool:
        return self._state == LoopState.CLOSED
    
    def _check_closed(self) -> None:
        if self.is_closed():
            raise LoopClosedError("Event loop is closed")
    
    def _check_thread(self) -> None:
        if threading.get_ident() != self._thread_id:
            raise RuntimeError("Event loop accessed from wrong thread")
    
    # Loop control
    def run_forever(self) -> None:
        """Run the event loop until stop() is called."""
        self._check_closed()
        
        if self._state == LoopState.RUNNING:
            raise RuntimeError("Event loop is already running")
        
        # Bind the loop to the thread that is actually running it. A loop may
        # be constructed in one thread and run in another.
        self._thread_id = threading.get_ident()
        self._state = LoopState.RUNNING
        
        # Set as current loop
        loop_token = set_loop_context(self)
        
        # Start scheduler
        if self._scheduler:
            self._scheduler.start()
        
        try:
            while self._state == LoopState.RUNNING:
                self._run_once()
        finally:
            self._state = LoopState.STOPPED
            if self._scheduler:
                self._scheduler.stop()
            reset_loop_context(loop_token)
    
    def run_until_complete(self, future: "Future[T] | Task[T] | Awaitable[T]") -> T:
        """
        Run the event loop until the given future completes.
        
        This is the primary entry point for running a single unit of work to
        completion, mirroring :func:`asyncio.run`.
        """
        self._check_closed()
        
        if self._state == LoopState.RUNNING:
            raise RuntimeError("Event loop is already running")
        
        future_task: Task[Any]
        if isinstance(future, Task):
            future_task = future
        elif isinstance(future, Future):
            # Bridge the future into a task so the loop can drive it.
            async def _await_future() -> Any:
                return await future
            future_task = self.create_task(_await_future())
        else:
            # A bare awaitable (coroutine, generator-based coroutine, ...).
            async def _await_awaitable() -> Any:
                return await future
            future_task = self.create_task(_await_awaitable())
        
        def _on_done(t: Future[Any]) -> None:
            if self._state == LoopState.RUNNING:
                self.stop()
        
        future_task.add_done_callback(_on_done)
        
        try:
            self.run_forever()
        finally:
            # Make sure the callback does not linger on the future.
            future_task.remove_done_callback(_on_done)
        
        if future_task.cancelled():
            raise CancelledError("Task was cancelled")
        
        return future_task.result()
    
    def _run_once(self) -> None:
        """Run one iteration of the event loop."""
        # Process ready callbacks first
        self._process_ready_callbacks()
        
        # Run pending tasks
        self._run_pending_tasks()
        
        # Advance timers that have already expired, so that the timeout
        # calculation below sees an accurate picture.
        now = time.monotonic()
        expired_timers = self._timer_wheel.advance(now)
        for timer in expired_timers:
            if not timer.cancelled:
                self._ready_callbacks.append(timer)
        
        # The timeout must be computed *after* running the tasks: a task may
        # have scheduled a timer, and the selector has to know about it,
        # otherwise the loop would block in select() and never fire it.
        timeout = self._calculate_timeout()
        
        # The loop may have been stopped while the tasks ran (for instance
        # because the main task completed). Do not block in the selector.
        if self._state != LoopState.RUNNING:
            return
        
        # Handle I/O events
        if self._io_handles:
            io_events = self._selector.select(timeout)
            for handle, events in io_events:
                handle._run(events)
        else:
            # No I/O handles, just sleep
            if timeout is not None and timeout > 0:
                time.sleep(min(timeout, 0.1))
            elif timeout == 0:
                # Something is ready; yield to the scheduler.
                time.sleep(0)
        
        # Check deadlines
        expired_deadlines = self._deadline_tracker.get_expired()
        for deadline in expired_deadlines:
            # Handle deadline expiration (would cancel associated operations)
            pass
    
    def _calculate_timeout(self) -> float | None:
        """Calculate the timeout for the next selector call."""
        # If there is work ready to be done, do not block in the selector:
        # the ready queue has to be drained first.
        if self._ready_callbacks or self._pending_tasks:
            return 0.0
        
        # Check timers
        next_timer = self._timer_wheel.next_expiration()
        if next_timer is not None:
            timeout = next_timer - time.monotonic()
            if timeout < 0:
                timeout = 0
        else:
            timeout = None
        
        # Check deadlines
        next_deadline = self._deadline_tracker.get_next()
        if next_deadline is not None:
            dl_timeout = next_deadline.remaining
            if timeout is None or dl_timeout < timeout:
                timeout = dl_timeout
        
        # Cap timeout
        if timeout is not None and timeout > 3600:  # 1 hour max
            timeout = 3600
        
        return timeout
    
    def _process_ready_callbacks(self) -> None:
        """Process all ready callbacks."""
        profiler = self._profiler
        tracer = self._tracer
        while self._ready_callbacks:
            handle = self._ready_callbacks.popleft()
            if handle.state == HandleState.ACTIVE and not handle.cancelled:
                start = time.perf_counter() if profiler is not None else 0.0
                if tracer is not None:
                    tracer.trace_callback(handle, "start")
                try:
                    handle._run()
                except Exception as e:
                    self.call_exception_handler({
                        "message": "Exception in callback",
                        "exception": e,
                        "handle": handle,
                    })
                finally:
                    if profiler is not None:
                        profiler.record_callback(repr(handle.callback), time.perf_counter() - start)
                    if tracer is not None:
                        tracer.trace_callback(handle, "stop")
    
    def _run_pending_tasks(self) -> None:
        """Run all pending tasks."""
        while self._pending_tasks:
            task = self._pending_tasks.popleft()
            if task.done():
                continue
            
            self._run_task_step(task)
    
    def _run_task_step(self, task: Task[Any]) -> None:
        """Run a single task step, with instrumentation.
        
        This is shared by the event loop thread and by work-stealing worker
        threads, so that both paths behave identically.
        """
        self._running_tasks.add(task)
        profiler = self._profiler
        tracer = self._tracer
        start = time.perf_counter() if profiler is not None else 0.0
        if tracer is not None:
            tracer.trace_task(task, "start")
        try:
            task._run_pending_step()
        finally:
            if profiler is not None:
                profiler.record_task(task.name, time.perf_counter() - start)
            if tracer is not None:
                tracer.trace_task(task, "stop")
            self._running_tasks.discard(task)
            # A worker thread may have completed the task that the loop is
            # blocked on, so make sure the loop notices.
            if threading.get_ident() != self._thread_id:
                self._wakeup()
    
    def _schedule_task(self, task: Task[Any]) -> None:
        """Schedule a task for execution."""
        self._all_tasks.add(task)
        task._schedule_step()
    
    def _schedule_task_step(self, task: Task[Any]) -> None:
        """Queue the next step of a task for the event loop thread."""
        self._pending_tasks.append(task)
        if threading.get_ident() != self._thread_id:
            # Scheduled from another thread: wake the loop so the step runs
            # without waiting for the next I/O event.
            self._wakeup()
    
    def _remove_pending_task(self, task: Task[Any]) -> None:
        """Remove a task from pending queue."""
        self._pending_tasks = deque(
            entry for entry in self._pending_tasks if entry is not task
        )
    
    def _task_finished(self, task: Task[Any]) -> None:
        """Called when a task finishes."""
        self._all_tasks.discard(task)
        self._running_tasks.discard(task)
    
    def stop(self) -> None:
        """Stop the event loop."""
        if self._state != LoopState.RUNNING:
            return
        self._state = LoopState.STOPPING
        # The loop may be blocked in the selector; wake it so it notices
        # the new state immediately. This matters when stop() is called from
        # a work-stealing worker thread.
        self._wakeup()
    
    def close(self) -> None:
        """Close the event loop."""
        if self._state == LoopState.CLOSED:
            return
        
        if self._state == LoopState.RUNNING:
            self.stop()
            # Wait for stop to complete
            while self._state == LoopState.RUNNING:
                time.sleep(0.001)
        
        self._state = LoopState.CLOSED
        self._closing = True
        
        # Cancel all pending tasks
        for task in list(self._all_tasks):
            task.cancel("Event loop closed")
        
        # Close the wakeup pipe
        for sock in (self._wakeup_read, self._wakeup_write):
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
        self._wakeup_read = None
        self._wakeup_write = None
        
        # Close selector
        self._selector.close()
        
        # Clear all handles
        self._io_handles.clear()
        self._registered_fds.clear()
        self._ready_callbacks.clear()
        self._pending_tasks.clear()
        self._all_tasks.clear()
        self._running_tasks.clear()
    
    # Callback scheduling
    def call_soon(
        self,
        callback: Callable[..., Any],
        *args: Any,
        context: Context | None = None,
        **kwargs: Any,
    ) -> CallbackHandle:
        """Schedule a callback to run on the next iteration."""
        self._check_closed()
        handle = create_handle(self, callback, *args, **kwargs)
        self._ready_callbacks.append(handle)
        if threading.get_ident() != self._thread_id:
            # Called from another thread (e.g. a work-stealing worker):
            # wake the loop so the callback is not delayed until the next
            # I/O event.
            self._wakeup()
        return handle
    
    def call_soon_threadsafe(
        self,
        callback: Callable[..., Any],
        *args: Any,
        context: Context | None = None,
        **kwargs: Any,
    ) -> CallbackHandle:
        """Thread-safe version of call_soon."""
        self._check_closed()
        handle = create_handle(self, callback, *args, **kwargs)
        # In real implementation, would use thread-safe queue
        self._ready_callbacks.append(handle)
        # Wake up the event loop if in another thread
        self._wakeup()
        return handle
    
    def call_later(
        self,
        delay: float,
        callback: Callable[..., Any],
        *args: Any,
        context: Context | None = None,
    ) -> TimerHandle:
        """Schedule a callback to run after a delay."""
        self._check_closed()
        when = time.monotonic() + delay
        handle = create_timer_handle(self, when, callback, *args)
        self._timer_wheel.schedule(handle)
        return handle
    
    def call_at(
        self,
        when: float,
        callback: Callable[..., Any],
        *args: Any,
        context: Context | None = None,
    ) -> TimerHandle:
        """Schedule a callback to run at a specific time."""
        self._check_closed()
        handle = create_timer_handle(self, when, callback, *args)
        self._timer_wheel.schedule(handle)
        return handle
    
    def _wakeup(self) -> None:
        """Wake up the event loop from another thread."""
        if self._wakeup_write is None:
            return
        try:
            self._wakeup_write.send(b"\x00")
        except (BlockingIOError, InterruptedError):
            pass
        except OSError:
            pass
    
    def _setup_wakeup(self) -> None:
        """Create the self-pipe used to wake the loop from other threads."""
        try:
            self._wakeup_read, self._wakeup_write = socket.socketpair()
        except (AttributeError, OSError):
            self._wakeup_read = None
            self._wakeup_write = None
            return
        
        self._wakeup_read.setblocking(False)
        self._wakeup_write.setblocking(False)
        self.add_reader(self._wakeup_read.fileno(), self._drain_wakeup)
    
    def _drain_wakeup(self) -> None:
        """Drain the wakeup pipe."""
        if self._wakeup_read is None:
            return
        try:
            while True:
                if not self._wakeup_read.recv(4096):
                    break
        except (BlockingIOError, InterruptedError):
            pass
        except OSError:
            pass
    
    # I/O handling
    def add_reader(
        self,
        fd: int,
        callback: Callable[..., Any],
        *args: Any,
    ) -> IOHandle:
        """Register a callback to be called when the fd is readable."""
        self._check_closed()
        handle = self._io_handles.get(fd)
        if handle is None:
            handle = create_io_handle(self, fd, 0, None, *args)
            self._io_handles[fd] = handle
        handle.add_reader(callback)
        return handle
    
    def add_writer(
        self,
        fd: int,
        callback: Callable[..., Any],
        *args: Any,
    ) -> IOHandle:
        """Register a callback to be called when the fd is writable."""
        self._check_closed()
        handle = self._io_handles.get(fd)
        if handle is None:
            handle = create_io_handle(self, fd, 0, None, *args)
            self._io_handles[fd] = handle
        handle.add_writer(callback)
        return handle
    
    def remove_reader(self, fd: int) -> bool:
        """Stop watching the fd for readability."""
        handle = self._io_handles.get(fd)
        if handle is None or handle.reader is None:
            return False
        handle.remove_reader()
        return True
    
    def remove_writer(self, fd: int) -> bool:
        """Stop watching the fd for writability."""
        handle = self._io_handles.get(fd)
        if handle is None or handle.writer is None:
            return False
        handle.remove_writer()
        return True
    
    def _update_io_events(self, handle: IOHandle) -> None:
        """Push an updated event mask to the selector."""
        fd = handle.fd
        events = handle.events
        try:
            if fd in self._registered_fds:
                if events:
                    self._selector.modify(fd, events, handle)
                else:
                    self._selector.unregister(fd)
                    self._registered_fds.discard(fd)
            elif events:
                self._selector.register(fd, events, handle)
                self._registered_fds.add(fd)
        except (KeyError, ValueError, OSError):
            # The fd may have been closed underneath us.
            self._registered_fds.discard(fd)
            self._io_handles.pop(fd, None)
    
    def _remove_io_handle(self, handle: IOHandle) -> None:
        """Internal method to remove an I/O handle."""
        try:
            if handle.fd in self._registered_fds:
                self._selector.unregister(handle.fd)
        except (KeyError, ValueError, OSError):
            pass
        self._registered_fds.discard(handle.fd)
        self._io_handles.pop(handle.fd, None)
    
    # Exception handling
    def set_exception_handler(
        self,
        handler: Callable[[dict[str, Any]], None] | None,
    ) -> None:
        """Set the exception handler."""
        self._exception_handler = handler
    
    def get_exception_handler(
        self,
    ) -> Callable[[dict[str, Any]], None] | None:
        """Get the current exception handler."""
        return self._exception_handler
    
    def call_exception_handler(self, context: dict[str, Any]) -> None:
        """Call the exception handler."""
        if self._exception_handler:
            try:
                self._exception_handler(context)
            except Exception:
                # Exception in exception handler - print to stderr
                traceback.print_exc()
        else:
            # Default behavior: print to stderr
            traceback.print_exc()
    
    def default_exception_handler(self, context: dict[str, Any]) -> None:
        """Default exception handler."""
        traceback.print_exc()
    
    # Task creation
    def create_task(
        self,
        coro: Coroutine[Any, Any, T],
        *,
        name: str | None = None,
    ) -> Task[T]:
        """Create a task from a coroutine."""
        return create_task(coro, loop=self, name=name)
    
    # ------------------------------------------------------------------
    # Low level I/O primitives
    # ------------------------------------------------------------------
    async def _create_connection(
        self,
        protocol_factory: Callable[[], Any],
        host: str | None = None,
        port: int | None = None,
        *,
        ssl: Any = None,
        family: int = 0,
        proto: int = 0,
        flags: int = 0,
        sock: socket.socket | None = None,
        local_addr: tuple[str, int] | None = None,
        server_hostname: str | None = None,
        **kwargs: Any,
    ) -> tuple[Any, Any]:
        """Create a socket connection and start the protocol."""
        if sock is None:
            if host is None or port is None:
                raise ValueError("host and port are required")
            sock = socket.socket(family or socket.AF_INET, socket.SOCK_STREAM, proto)
            sock.setblocking(False)
            if local_addr is not None:
                sock.bind(local_addr)
            await self._sock_connect(sock, (host, port))
        else:
            sock.setblocking(False)
        
        transport = SelectorTransport(
            self,
            protocol_factory(),
            sock,
            sslcontext=_resolve_sslcontext(ssl, server_side=False),
            server_side=False,
            server_hostname=(
                (server_hostname or host) if ssl else None
            ),
        )
        protocol = transport.get_protocol()
        try:
            protocol.connection_made(transport)
        except Exception:
            transport.close()
            raise
        
        self._register_transport_reader(transport)
        
        # Wait for the TLS handshake to finish before handing the connection
        # over to the caller. The client has to send its ClientHello first,
        # otherwise the handshake would deadlock.
        if transport._sslobject is not None:
            transport.start_handshake()
            await transport.wait_for_handshake()
        
        return transport, protocol
    
    async def _sock_connect(
        self,
        sock: socket.socket,
        address: tuple[str, int],
    ) -> None:
        """Connect a non-blocking socket, waiting for writability."""
        err = sock.connect_ex(address)
        if err in (0, errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK):
            if err == 0:
                return
            # Wait until the socket is writable.
            future: Future[None] = self.create_future()
            
            def _on_writable() -> None:
                if not future.done():
                    future.set_result(None)
            
            self.add_writer(sock.fileno(), _on_writable)
            try:
                await future
            finally:
                self.remove_writer(sock.fileno())
            
            # Check for connection errors.
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err != 0:
                raise OSError(err, os.strerror(err))
        else:
            raise OSError(err, os.strerror(err))
    
    async def _create_server(
        self,
        protocol_factory: Callable[[], Any],
        host: str | None = None,
        port: int = 0,
        *,
        ssl: Any = None,
        family: int = 0,
        proto: int = 0,
        flags: int = 0,
        sock: socket.socket | None = None,
        backlog: int = 100,
        reuse_address: bool = True,
        reuse_port: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Create a listening server."""
        if sock is None:
            if family == 0 and host is not None:
                # Try IPv6 first, fall back to IPv4.
                try:
                    family = socket.AF_INET6
                    infos = socket.getaddrinfo(
                        host, port, family, socket.SOCK_STREAM, 0, socket.AI_PASSIVE
                    )
                except socket.gaierror:
                    family = socket.AF_INET
                    infos = socket.getaddrinfo(
                        host, port, family, socket.SOCK_STREAM, 0, socket.AI_PASSIVE
                    )
            else:
                family = family or socket.AF_INET
                infos = socket.getaddrinfo(
                    host, port, family, socket.SOCK_STREAM, 0, socket.AI_PASSIVE
                )
            
            if not infos:
                raise OSError("getaddrinfo returned an empty list")
            
            sockets = []
            for af, socktype, proto_, canonname, sa in infos:
                try:
                    s = socket.socket(af, socktype, proto_)
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    if reuse_port and hasattr(socket, "SO_REUSEPORT"):
                        try:
                            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                        except OSError:
                            pass
                    s.setblocking(False)
                    s.bind(sa)
                    sockets.append(s)
                except OSError:
                    for s in sockets:
                        s.close()
                    raise
            
            # Bind the first socket to the requested port and apply it to
            # the others so that they share the port.
            if port != 0 and len(sockets) > 1:
                first_addr = sockets[0].getsockname()
                bound_port = first_addr[1]
                for s in sockets[1:]:
                    try:
                        s.close()
                    except OSError:
                        pass
                sockets = sockets[:1]
            
            for s in sockets:
                s.listen(backlog)
        else:
            sockets = [sock]
            sockets[0].setblocking(False)
        
        return SelectorServer(
            self,
            sockets,
            protocol_factory,
            sslcontext=_resolve_sslcontext(ssl, server_side=True),
        )
    
    async def _create_unix_connection(
        self,
        protocol_factory: Callable[[], Any],
        path: str,
        **kwargs: Any,
    ) -> tuple[Any, Any]:
        """Connect to a Unix domain socket."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.setblocking(False)
        await self._sock_connect(sock, path)
        
        transport = SelectorTransport(self, protocol_factory(), sock)
        protocol = transport.get_protocol()
        try:
            protocol.connection_made(transport)
        except Exception:
            transport.close()
            raise
        
        self._register_transport_reader(transport)
        return transport, protocol
    
    async def _create_unix_server(
        self,
        protocol_factory: Callable[[], Any],
        path: str,
        **kwargs: Any,
    ) -> Any:
        """Create a Unix domain socket server."""
        try:
            os.unlink(path)
        except OSError:
            pass
        
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.bind(path)
        sock.listen(100)
        return SelectorServer(self, [sock], protocol_factory)
    
    async def _create_pipe(self) -> tuple[Any, Any]:
        """Create a connected pair of pipes."""
        read_fd, write_fd = os.pipe()
        read_sock = socket.socket(fileno=read_fd)
        write_sock = socket.socket(fileno=write_fd)
        read_sock.setblocking(False)
        write_sock.setblocking(False)
        
        # The transport is the write end; the read end is returned raw so
        # the caller can wrap it in a StreamReader.
        transport = SelectorTransport(self, _NullProtocol(), write_sock)
        return transport, read_sock
    
    def _register_transport_reader(self, transport: Any) -> None:
        """Start reading from a transport's socket."""
        sock = transport._sock
        self.add_reader(sock.fileno(), transport._read_ready)
    
    # -- Subprocess support -------------------------------------------------
    async def _subprocess_exec(
        self,
        protocol_factory: Callable[[], Any],
        *args: str,
        stdin: int | None = subprocess.PIPE,
        stdout: int | None = subprocess.PIPE,
        stderr: int | None = subprocess.PIPE,
        **kwargs: Any,
    ) -> tuple[Any, Any]:
        """Spawn a subprocess."""
        return await self._subprocess(
            protocol_factory,
            args,
            shell=False,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            **kwargs,
        )
    
    async def _subprocess_shell(
        self,
        protocol_factory: Callable[[], Any],
        cmd: str,
        *,
        stdin: int | None = subprocess.PIPE,
        stdout: int | None = subprocess.PIPE,
        stderr: int | None = subprocess.PIPE,
        **kwargs: Any,
    ) -> tuple[Any, Any]:
        """Spawn a subprocess through the shell."""
        return await self._subprocess(
            protocol_factory,
            (cmd,),
            shell=True,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            **kwargs,
        )
    
    async def _subprocess(
        self,
        protocol_factory: Callable[[], Any],
        args: tuple[str, ...],
        *,
        shell: bool = False,
        stdin: int | None = subprocess.PIPE,
        stdout: int | None = subprocess.PIPE,
        stderr: int | None = subprocess.PIPE,
        **kwargs: Any,
    ) -> tuple[Any, Any]:
        """Spawn a subprocess and wire up its pipes to the protocol."""
        protocol = protocol_factory()
        
        # The protocol needs the transport, so build it after the process
        # exists.
        proc = subprocess.Popen(
            args,
            shell=shell,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            **kwargs,
        )
        
        transport = _SubprocessTransport(self, protocol, proc)
        try:
            protocol.connection_made(transport)
        except Exception:
            transport.close()
            raise
        
        # Wire up the pipes. The protocol is told about each one so it can
        # set up the matching stream objects.
        for fd, fileobj, mode in (
            (0, proc.stdin, "write"),
            (1, proc.stdout, "read"),
            (2, proc.stderr, "read"),
        ):
            if fileobj is None:
                continue
            pipe_transport = _PipeTransport(self, fileobj, fd)
            try:
                protocol.pipe_connection_made(fd, pipe_transport)
            except Exception:
                transport.close()
                raise
            transport._attach_pipe(fd, fileobj, mode, pipe_transport)
        
        # Watch for the process exiting.
        transport._start_waiting()
        
        return transport, protocol
    
    def create_future(self) -> Future[T]:
        """Create a new Future."""
        return Future(loop=self)
    
    # Context management
    def get_context(self) -> Context:
        return self._context_manager.context
    
    def run_in_executor(
        self,
        executor: Any | None,
        func: Callable[..., T],
        *args: Any,
    ) -> Future[T]:
        """Run a function in an executor and return a future for its result."""
        from ..threadpool.pool import ThreadPool, get_default_executor, set_default_executor

        if executor is None:
            default = get_default_executor()
            if default is None:
                default = ThreadPool(loop=self)
                set_default_executor(default)
            executor = default.executor

        future: Future[T] = self.create_future()

        def _run() -> None:
            try:
                result = func(*args)
                if isinstance(result, Awaitable):
                    # Handle async functions
                    async def _wrap() -> T:
                        return await result

                    def _finish(t: Any) -> None:
                        if t.cancelled():
                            future.cancel()
                            return
                        exc = t.exception()
                        if exc is not None:
                            future.set_exception(exc)
                        else:
                            future.set_result(t.result())

                    self.call_soon_threadsafe(
                        lambda: self.create_task(_wrap()).add_done_callback(_finish)
                    )
                else:
                    self.call_soon_threadsafe(lambda: future.set_result(result))
            except Exception as exc:
                # Bind explicitly: ``except ... as exc`` unbinds the name when
                # the block is left, so a closure would not see it.
                error = exc
                self.call_soon_threadsafe(lambda err=error: future.set_exception(err))

        executor.submit(_run)
        return future
    
    # Signal handling
    def add_signal_handler(
        self,
        sig: int,
        callback: Callable[..., Any],
        *args: Any,
    ) -> None:
        """Add a signal handler."""
        if sig not in self._signal_handlers:
            self._signal_handlers[sig] = []
            try:
                signal.signal(sig, self._signal_handler)
            except (ValueError, OSError):
                # Signal not supported on this platform
                pass
        self._signal_handlers[sig].append((callback, args))
    
    def remove_signal_handler(self, sig: int) -> bool:
        """Remove signal handlers."""
        if sig in self._signal_handlers:
            del self._signal_handlers[sig]
            return True
        return False
    
    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Internal signal handler."""
        handlers = self._signal_handlers.get(signum, [])
        for callback, args in handlers:
            self.call_soon_threadsafe(callback, *args)
    
    def __enter__(self) -> Self:
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
    
    def __repr__(self) -> str:
        return f"<EventLoop state={self._state.name} tasks={len(self._all_tasks)}>"


# Global loop management
_current_loop: EventLoop | None = None
_loop_lock = threading.Lock()


def new_event_loop(config: LoopConfig | None = None) -> EventLoop:
    """Create a new event loop."""
    return EventLoop(config or LoopConfig())


def get_event_loop() -> EventLoop:
    """Get the current event loop."""
    global _current_loop
    with _loop_lock:
        if _current_loop is None:
            _current_loop = new_event_loop()
        return _current_loop


def set_event_loop(loop: EventLoop | None) -> None:
    """Set the current event loop."""
    global _current_loop
    with _loop_lock:
        _current_loop = loop


def run(coro: Coroutine[Any, Any, T], *, loop: EventLoop | None = None) -> T:
    """Run a coroutine and return its result."""
    loop = loop or new_event_loop()
    set_event_loop(loop)
    
    task = loop.create_task(coro)
    return loop.run_until_complete(task)