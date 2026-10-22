"""
Thread pool integration for running blocking code.
"""

from __future__ import annotations

import concurrent.futures
import threading
from collections.abc import Callable, Awaitable
from concurrent.futures import ThreadPoolExecutor, Executor
from dataclasses import dataclass
from typing import Any, TypeVar, Optional

from ..core.types import GodLoopError
from ..core.loop import EventLoop
from ..core.context import current_loop
from ..core.future import Future
from ..core.task import Task

__all__ = [
    "ThreadPool",
    "run_in_executor",
    "set_default_executor",
    "get_default_executor",
    "ThreadPoolError",
]


class ThreadPoolError(GodLoopError):
    """Thread pool errors."""
    pass


T = TypeVar("T")


@dataclass
class ThreadPool:
    """
    Managed thread pool for running blocking operations.
    
    Provides lifecycle management and integration with the event loop.
    """
    
    _executor: ThreadPoolExecutor
    _loop: EventLoop
    _default: bool = False
    
    def __init__(
        self,
        max_workers: int | None = None,
        thread_name_prefix: str = "GodLoop-ThreadPool",
        loop: EventLoop | None = None,
    ) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
        )
        self._loop = loop or current_loop()
        self._default = False
    
    @property
    def executor(self) -> ThreadPoolExecutor:
        return self._executor
    
    @property
    def loop(self) -> EventLoop:
        return self._loop
    
    def submit(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> concurrent.futures.Future[T]:
        """Submit a function to the thread pool."""
        return self._executor.submit(func, *args, **kwargs)
    
    async def run(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run a function in the thread pool and await result."""
        future: Future[T] = self._loop.create_future()
        
        def _run() -> None:
            try:
                result = func(*args, **kwargs)
                if isinstance(result, Awaitable):
                    # Handle async functions
                    async def _wrap() -> T:
                        return await result
                    
                    def _finish(t: Task[T]) -> None:
                        if t.cancelled():
                            future.cancel()
                        else:
                            exc = t.exception()
                            if exc is not None:
                                future.set_exception(exc)
                            else:
                                future.set_result(t.result())
                    
                    self._loop.call_soon_threadsafe(
                        lambda: self._loop.create_task(_wrap()).add_done_callback(_finish)
                    )
                else:
                    self._loop.call_soon_threadsafe(lambda: future.set_result(result))
            except Exception as exc:
                # Bind explicitly: ``except ... as exc`` unbinds the name
                # when the block is left, so a closure would not see it.
                error = exc
                self._loop.call_soon_threadsafe(lambda err=error: future.set_exception(err))
        
        self._executor.submit(_run)
        return await future
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the thread pool."""
        self._executor.shutdown(wait=wait)
    
    def __enter__(self) -> "ThreadPool":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.shutdown(wait=True)


_default_executor: ThreadPool | None = None
_default_executor_lock = threading.Lock()


def set_default_executor(executor: ThreadPool | None) -> None:
    """Set the default thread pool executor."""
    global _default_executor
    with _default_executor_lock:
        _default_executor = executor


def get_default_executor() -> ThreadPool | None:
    """Get the default thread pool executor."""
    global _default_executor
    with _default_executor_lock:
        return _default_executor


async def run_in_executor(
    func: Callable[..., T] | None,
    *args: Any,
    executor: Executor | None = None,
    loop: EventLoop | None = None,
    **kwargs: Any,
) -> T:
    """
    Run a function in an executor and await its result.

    Two calling conventions are supported::

        await run_in_executor(func, *args)              # simple form
        await run_in_executor(executor, func, *args)    # asyncio-style form

    The asyncio-style form is detected automatically: when the first
    positional argument is ``None`` or exposes a ``submit`` method it is
    treated as the executor and the callable is taken from the next
    positional argument.
    """
    # Detect the asyncio-style ``(executor, func, *args)`` calling form.
    if func is None or not callable(func):
        if not args:
            raise TypeError("run_in_executor() requires a callable")
        candidate = func
        func = args[0]
        args = args[1:]
        if candidate is not None:
            executor = candidate

    assert func is not None
    loop = loop or current_loop()

    if executor is None:
        default = get_default_executor()
        if default is None:
            # Create default thread pool
            default = ThreadPool(loop=loop)
            set_default_executor(default)
        executor = default.executor

    future: Future[T] = loop.create_future()

    def _run() -> None:
        try:
            result = func(*args, **kwargs)
            if isinstance(result, Awaitable):
                async def _wrap() -> T:
                    return await result

                def _finish(t: Task[T]) -> None:
                    if t.cancelled():
                        future.cancel()
                    else:
                        exc = t.exception()
                        if exc is not None:
                            future.set_exception(exc)
                        else:
                            future.set_result(t.result())

                loop.call_soon_threadsafe(
                    lambda: loop.create_task(_wrap()).add_done_callback(_finish)
                )
            else:
                loop.call_soon_threadsafe(lambda: future.set_result(result))
        except Exception as exc:
            # Bind the exception explicitly: ``except ... as exc`` unbinds the
            # name when the block is left, so a closure would not see it.
            error = exc
            loop.call_soon_threadsafe(lambda err=error: future.set_exception(err))

    executor.submit(_run)
    return await future


async def to_thread(
    func: Callable[..., T],
    *args: Any,
    **kwargs: Any,
) -> T:
    """Run a function in a thread (convenience wrapper)."""
    return await run_in_executor(func, *args, **kwargs)