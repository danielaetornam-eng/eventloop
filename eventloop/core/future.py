"""
Future implementation for GodLoop.
"""

from __future__ import annotations

import sys
import weakref
from collections.abc import Callable, Coroutine
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, final
from typing_extensions import Self

from .types import FutureState, CancelledError, InvalidStateError, GodLoopError
from .context import current_loop

__all__ = [
    "Future",
    "FutureResult",
    "ensure_future",
    "wrap_future",
    "is_future",
]

T = TypeVar("T")


@dataclass(slots=True)
class FutureResult(Generic[T]):
    """Result container for Future."""
    
    value: T | None = None
    _exception: BaseException | None = None
    state: FutureState = FutureState.PENDING
    
    def set_result(self, value: T) -> None:
        if self.state != FutureState.PENDING:
            # Already cancelled or finished, ignore silently
            return
        self.value = value
        self.state = FutureState.FINISHED
    
    def set_exception(self, exception: BaseException) -> None:
        if self.state != FutureState.PENDING:
            # Already cancelled or finished, ignore silently
            return
        self._exception = exception
        self.state = FutureState.FINISHED
    
    def cancel(self) -> bool:
        if self.state != FutureState.PENDING:
            return False
        self._exception = CancelledError("Future cancelled")
        self.state = FutureState.CANCELLED
        return True
    
    def result(self) -> T:
        if self.state == FutureState.PENDING:
            raise InvalidStateError("Result not ready")
        if self.state == FutureState.CANCELLED:
            raise CancelledError("Future was cancelled")
        if self._exception is not None:
            raise self._exception
        return self.value  # type: ignore
    
    def exception(self) -> BaseException | None:
        if self.state == FutureState.PENDING:
            raise InvalidStateError("Exception not ready")
        return self._exception
    
    def _get_exception(self) -> BaseException | None:
        if self.state == FutureState.PENDING:
            raise InvalidStateError("Exception not ready")
        return self._exception
    
    def done(self) -> bool:
        return self.state != FutureState.PENDING
    
    def cancelled(self) -> bool:
        return self.state == FutureState.CANCELLED


class Future(Generic[T]):
    """
    A Future represents an eventual result of an asynchronous operation.
    
    This implementation is compatible with asyncio.Future but adds
    additional features like context tracking and better debugging.
    """
    
    __slots__ = (
        "_loop",
        "_result",
        "_callbacks",
        "_cancel_requested",
        "_cancel_message",
        "_context",
        "_debug_info",
        "__weakref__",
        "_source_traceback",
    )
    
    def __init__(self, *, loop: "EventLoop | None" = None) -> None:
        self._loop = loop or current_loop()
        if self._loop is None:
            raise RuntimeError("No current event loop")
        
        self._result = FutureResult()
        self._callbacks: list[Callable[[Future[T]], None]] = []
        self._cancel_requested = False
        self._cancel_message: str | None = None
        # The context in which done-callbacks run. It is captured lazily,
        # the first time a callback is registered, to avoid copying a
        # context for every future that is created.
        self._context: Context | None = None
        self._debug_info: dict[str, Any] = {}
        self._source_traceback = None
        
        if self._loop.get_debug():
            import traceback
            self._source_traceback = traceback.extract_stack()
    
    def _get_context(self) -> Context:
        """Return the context used to run done-callbacks."""
        if self._context is None:
            self._context = copy_context()
        return self._context
    
    @property
    def loop(self) -> "EventLoop":
        return self._loop
    
    def done(self) -> bool:
        return self._result.done()
    
    def cancelled(self) -> bool:
        return self._result.cancelled()
    
    def result(self) -> T:
        return self._result.result()
    
    def exception(self) -> BaseException | None:
        return self._result._get_exception()
    
    def add_done_callback(
        self,
        callback: Callable[[Future[T]], None],
        *,
        context: Context | None = None,
    ) -> None:
        """Add a callback to be run when the future is done."""
        if self.done():
            # Schedule callback immediately
            ctx = context or self._get_context()
            self._loop.call_soon_threadsafe(
                lambda: self._run_callback(callback, ctx)
            )
        else:
            self._callbacks.append((callback, context or self._get_context()))
    
    def remove_done_callback(
        self,
        callback: Callable[[Future[T]], None],
    ) -> int:
        """Remove a done callback. Returns number of callbacks removed."""
        count = 0
        new_callbacks = []
        for cb, ctx in self._callbacks:
            if cb is callback:
                count += 1
            else:
                new_callbacks.append((cb, ctx))
        self._callbacks = new_callbacks
        return count
    
    def _run_callback(self, callback: Callable[[Future[T]], None], context: Context) -> None:
        """Run a single callback with the given context."""
        try:
            context.run(callback, self)
        except Exception as e:
            self._loop.call_exception_handler({
                "message": "Exception in future callback",
                "exception": e,
                "future": self,
            })
    
    def _run_callbacks(self) -> None:
        """Run all done callbacks."""
        callbacks = self._callbacks
        self._callbacks = []
        for callback, context in callbacks:
            self._run_callback(callback, context)
    
    def set_result(self, value: T) -> None:
        """Mark the future as finished with a result."""
        if self.done():
            raise InvalidStateError(f"Future already in state {self._result.state.name}")
        self._result.set_result(value)
        self._run_callbacks()
    
    def set_exception(self, exception: BaseException) -> None:
        """Mark the future as finished with an exception."""
        if self.done():
            raise InvalidStateError(f"Future already in state {self._result.state.name}")
        if isinstance(exception, type):
            exception = exception()
        self._result.set_exception(exception)
        self._run_callbacks()
    
    def cancel(self, msg: str | None = None) -> bool:
        """Cancel the future."""
        if self.done():
            return False
        self._cancel_requested = True
        self._cancel_message = msg
        self._result.cancel()
        self._run_callbacks()
        return True
    
    def cancel_requested(self) -> bool:
        return self._cancel_requested
    
    def cancel_message(self) -> str | None:
        return self._cancel_message
    
    def __await__(self) -> Coroutine[Any, Any, T]:
        yield self  # This makes it awaitable
        if not self.done():
            # This will be resumed when the future is done
            from .task import current_task
            task = current_task()
            if task:
                task._awaiting_on = self
        return self.result()
    
    def __iter__(self) -> Any:
        # For compatibility with "yield from" syntax
        return self.__await__()
    
    def __repr__(self) -> str:
        state = self._result.state.name
        if state == "FINISHED":
            if self._result.exception:
                return f"<Future finished exception={self._result.exception!r}>"
            return f"<Future finished result={self._result.value!r}>"
        elif state == "CANCELLED":
            return f"<Future cancelled msg={self._cancel_message!r}>"
        return f"<Future pending>"


def ensure_future(
    awaitable: Awaitable[T],
    *,
    loop: "EventLoop | None" = None,
) -> Future[T]:
    """Wrap an awaitable in a Future."""
    from .context import current_loop
    from .task import Task, create_task
    
    loop = loop or current_loop()
    if loop is None:
        raise RuntimeError("No current event loop")
    
    if isinstance(awaitable, Future):
        return awaitable
    
    if isinstance(awaitable, Coroutine):
        return create_task(awaitable, loop=loop)
    
    # For other awaitables, wrap in a task
    async def _wrap() -> T:
        return await awaitable
    
    return create_task(_wrap(), loop=loop)


def wrap_future(
    future: "asyncio.Future[T]",
    *,
    loop: "EventLoop | None" = None,
) -> Future[T]:
    """Wrap an asyncio.Future in a GodLoop Future."""
    import asyncio
    
    if not isinstance(future, asyncio.Future):
        raise TypeError("Expected asyncio.Future")
    
    loop = loop or current_loop()
    if loop is None:
        raise RuntimeError("No current event loop")
    
    god_future: Future[T] = Future(loop=loop)
    
    def _on_done(asyncio_fut: asyncio.Future[T]) -> None:
        if asyncio_fut.cancelled():
            god_future.cancel()
        elif asyncio_fut.exception():
            god_future.set_exception(asyncio_fut.exception())
        else:
            god_future.set_result(asyncio_fut.result())
    
    future.add_done_callback(_on_done)
    return god_future


def is_future(obj: Any) -> bool:
    """Check if an object is a Future."""
    return isinstance(obj, Future)