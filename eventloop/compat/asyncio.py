"""
Asyncio compatibility layer.
"""

from __future__ import annotations

import sys
import asyncio
import socket
import ssl
from collections.abc import Callable, Awaitable, Coroutine
from contextlib import AbstractAsyncContextManager
from typing import Any, TypeVar, Optional, Union, Sequence

from ..core.types import CancelledError as GodLoopCancelledError
from ..core.loop import (
    EventLoop,
    run,
    get_event_loop as core_get_event_loop,
    set_event_loop,
    new_event_loop,
)
from ..core.task import Task, create_task, current_task, all_tasks, sleep
from ..core.future import Future, ensure_future
from ..core.context import copy_context, current_loop
from ..concurrency import (
    Lock,
    Event,
    Condition,
    Semaphore,
    BoundedSemaphore,
    Barrier,
    BrokenBarrierError,
)
from ..streams import (
    StreamReader,
    StreamWriter,
    open_connection,
    start_server,
)
from ..subprocess import (
    Process,
    create_subprocess_exec,
    create_subprocess_shell,
)
from ..threadpool import run_in_executor, to_thread

__all__ = [
    # Core functions
    "ensure_future",
    "gather",
    "wait",
    "wait_for",
    "as_completed",
    "sleep",
    "run",
    "create_task",
    "current_task",
    "all_tasks",
    # Synchronization
    "Event",
    "Lock",
    "Condition",
    "Semaphore",
    "BoundedSemaphore",
    "Barrier",
    "Queue",
    "PriorityQueue",
    "LifoQueue",
    # Streams
    "StreamReader",
    "StreamWriter",
    "open_connection",
    "start_server",
    # Subprocess
    "create_subprocess_exec",
    "create_subprocess_shell",
    # Thread pool
    "run_in_executor",
    "to_thread",
    # Loop management
    "get_running_loop",
    "get_event_loop",
    "set_event_loop",
    "new_event_loop",
    # Abstract classes
    "AbstractEventLoop",
    "AbstractServer",
    "AbstractChildWatcher",
    "FastChildWatcher",
    "SafeChildWatcher",
    "ThreadedChildWatcher",
    "PidfdChildWatcher",
    "SelectorEventLoop",
    "ProactorEventLoop",
]

T = TypeVar("T")


# Core asyncio-compatible functions
def ensure_future(
    awaitable: Awaitable[T],
    *,
    loop: asyncio.AbstractEventLoop | None = None,
) -> asyncio.Future[T]:
    """Wrap awaitable in a Future (asyncio compatible)."""
    god_loop = loop or get_event_loop()
    
    # Already a task or a future: hand it back unchanged, like asyncio does.
    if isinstance(awaitable, (Task, Future)):
        return awaitable
    
    future = god_loop.create_future()
    
    async def _wrap() -> None:
        try:
            result = await awaitable
        except GodLoopCancelledError:
            if not future.done():
                future.cancel()
            raise
        except Exception as e:
            if not future.done():
                future.set_exception(e)
        else:
            if not future.done():
                future.set_result(result)
    
    god_loop.create_task(_wrap())
    return future


async def gather(*awaitables: Awaitable[T], return_exceptions: bool = False) -> list[T | BaseException]:
    """Gather multiple awaitables (asyncio compatible)."""
    tasks = [ensure_future(a) for a in awaitables]
    results = []
    
    for task in tasks:
        try:
            result = await task
            results.append(result)
        except Exception as e:
            if return_exceptions:
                results.append(e)
            else:
                # Cancel remaining
                for t in tasks:
                    if not t.done():
                        t.cancel()
                raise
    
    return results


async def wait(
    aws: Sequence[Awaitable[T]],
    *,
    timeout: float | None = None,
    return_when: str = "ALL_COMPLETED",
) -> tuple[set[asyncio.Future[T]], set[asyncio.Future[T]]]:
    """Wait for awaitables (asyncio compatible).
    
    Returns a ``(done, pending)`` pair of sets of futures.
    """
    futures = [ensure_future(a) for a in aws]
    if not futures:
        return set(), set()
    
    if return_when not in ("ALL_COMPLETED", "FIRST_COMPLETED", "FIRST_EXCEPTION"):
        raise ValueError(f"Invalid return_when value: {return_when!r}")
    
    loop = get_event_loop()
    waiter = loop.create_future()
    
    def _snapshot() -> tuple[set[Any], set[Any]]:
        done = {f for f in futures if f.done()}
        pending = {f for f in futures if not f.done()}
        return done, pending
    
    def _check(_fut: Any = None) -> None:
        if waiter.done():
            return
        done, pending = _snapshot()
        if return_when == "ALL_COMPLETED":
            if not pending:
                waiter.set_result((done, pending))
        elif return_when == "FIRST_COMPLETED":
            if done:
                waiter.set_result((done, pending))
        else:  # FIRST_EXCEPTION
            errors = {f for f in done if not f.cancelled() and f.exception() is not None}
            if errors:
                waiter.set_result((errors, pending))
            elif not pending:
                waiter.set_result((done, pending))
    
    for f in futures:
        f.add_done_callback(_check)
    
    if timeout is None:
        return await waiter
    
    # Race the waiter against the timeout.
    def _on_timeout() -> None:
        if not waiter.done():
            waiter.set_exception(asyncio.TimeoutError())
    
    handle = loop.call_later(timeout, _on_timeout)
    try:
        return await waiter
    finally:
        handle.cancel()


async def wait_for(aw: Awaitable[T], timeout: float | None) -> T:
    """Wait for a single awaitable with timeout (asyncio compatible)."""
    future = ensure_future(aw)
    
    if timeout is None:
        return await future
    
    loop = get_event_loop()
    timer = loop.create_future()
    timed_out = False
    
    def _on_timeout() -> None:
        nonlocal timed_out
        timed_out = True
        if not timer.done():
            timer.set_result(None)
    
    handle = loop.call_later(timeout, _on_timeout)
    try:
        done, _pending = await wait(
            [future, timer], return_when="FIRST_COMPLETED"
        )
        if timed_out:
            if not future.done():
                future.cancel()
            raise asyncio.TimeoutError()
        return future.result()
    finally:
        handle.cancel()


async def as_completed(aws: Sequence[Awaitable[T]]) -> Any:
    """Return iterator of futures as they complete (asyncio compatible)."""
    futures = [ensure_future(a) for a in aws]
    
    while futures:
        done, pending = await wait(futures, return_when="FIRST_COMPLETED")
        for f in done:
            yield f
        futures = list(pending)


# Loop management
def get_running_loop() -> EventLoop:
    """Get the running event loop (asyncio compatible)."""
    from ..core.context import current_loop
    loop = current_loop()
    if loop is None or not loop.is_running():
        raise RuntimeError("No running event loop")
    return loop


def get_event_loop() -> EventLoop:
    """Get the current event loop (asyncio compatible).
    
    Inside a running loop the running loop is returned, which keeps the
    compatibility layer working with per-loop state.
    """
    running = current_loop()
    if running is not None:
        return running
    
    return core_get_event_loop()


def set_event_loop(loop: EventLoop | None) -> None:
    """Set the current event loop (asyncio compatible)."""
    from ..core.loop import set_event_loop as core_set_event_loop
    return core_set_event_loop(loop)


def new_event_loop() -> EventLoop:
    """Create a new event loop (asyncio compatible)."""
    from ..core.loop import new_event_loop as core_new_event_loop
    return core_new_event_loop()


# Abstract classes (stubs for compatibility)
class AbstractEventLoop:
    """Abstract base class for event loops."""
    pass


class AbstractServer:
    """Abstract base class for servers."""
    pass


class AbstractChildWatcher:
    """Abstract base class for child watchers."""
    pass


class FastChildWatcher(AbstractChildWatcher):
    pass


class SafeChildWatcher(AbstractChildWatcher):
    pass


class ThreadedChildWatcher(AbstractChildWatcher):
    pass


class PidfdChildWatcher(AbstractChildWatcher):
    pass


class SelectorEventLoop(EventLoop):
    """Selector-based event loop (asyncio compatible)."""
    pass


class ProactorEventLoop(EventLoop):
    """Proactor-based event loop (Windows, asyncio compatible)."""
    pass


# Queue implementations. These are GodLoop's own queues - the stdlib asyncio
# queues cannot be used because they are bound to asyncio's event loop.
from ..concurrency.queue import Queue, PriorityQueue, LifoQueue  # noqa: E402


# Compatibility aliases
asyncio_ensure_future = ensure_future
asyncio_gather = gather
asyncio_wait = wait
asyncio_wait_for = wait_for
asyncio_as_completed = as_completed
asyncio_sleep = sleep
asyncio_run = run
asyncio_create_task = create_task
asyncio_current_task = current_task
asyncio_all_tasks = all_tasks
asyncio_Event = Event
asyncio_Lock = Lock
asyncio_Condition = Condition
asyncio_Semaphore = Semaphore
asyncio_BoundedSemaphore = BoundedSemaphore
asyncio_Barrier = Barrier
asyncio_Queue = Queue
asyncio_PriorityQueue = PriorityQueue
asyncio_LifoQueue = LifoQueue
asyncio_StreamReader = StreamReader
asyncio_StreamWriter = StreamWriter
asyncio_open_connection = open_connection
asyncio_start_server = start_server
asyncio_create_subprocess_exec = create_subprocess_exec
asyncio_create_subprocess_shell = create_subprocess_shell
asyncio_run_in_executor = run_in_executor
asyncio_to_thread = to_thread
asyncio_get_running_loop = get_running_loop
asyncio_get_event_loop = get_event_loop
asyncio_set_event_loop = set_event_loop
asyncio_new_event_loop = new_event_loop