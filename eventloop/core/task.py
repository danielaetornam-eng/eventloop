"""
Task implementation for GodLoop - represents a running coroutine.
"""

from __future__ import annotations

import sys
import threading
import time
import traceback
import weakref
from collections.abc import Callable, Coroutine, Generator
from contextlib import AbstractAsyncContextManager
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from typing import (
    Any,
    Generic,
    TypeVar,
    final,
)
from typing_extensions import Self

from .types import TaskState, TaskCancelledError, TaskDestroyedError, GodLoopError, CancelledError
from .future import Future
from .context import (
    TaskContext,
    current_loop,
    set_task_context,
    reset_task_context,
    get_task_context,
    run_with_context,
    set_loop_context,
    reset_loop_context,
)
from .handle import Handle

__all__ = [
    "Task",
    "create_task",
    "current_task",
    "all_tasks",
    "sleep",
    "sleep_forever",
    "shield",
    "TaskGroup",
    "TaskGroupError",
]

T = TypeVar("T")


class Task(Generic[T]):
    """
    A Task wraps a coroutine and schedules it on the event loop.
    
    This is the core scheduling unit in GodLoop. It manages the lifecycle
    of a coroutine, handles cancellation, exception propagation, and
    integration with the event loop's scheduler.
    """
    
    __slots__ = (
        "_loop",
        "_coro",
        "_future",
        "_state",
        "_name",
        "_context",
        "_context_token",
        "_task_ctx",
        "_awaiting_on",
        "_cancel_requested",
        "_cancel_message",
        "_cancel_stack",
        "_cancel_scope",
        "_cancel_delivery_pending",
        "_exception",
        "_exception_handled",
        "_destroyed",
        "_debug_info",
        "_source_traceback",
        "__weakref__",
        "_created_at",
        "_scheduled_at",
        "_started_at",
        "_finished_at",
        "_parent_task",
        "_children",
        "_cancellation_scope",
        "_callback_wrappers",
        "_step_state_lock",
        "_step_run_lock",
        "_step_in_flight",
        "_has_pending_step",
        "_pending_value",
        "_pending_exc",
    )
    
    _task_counter: int = 0
    
    def __init__(
        self,
        coro: Coroutine[Any, Any, T],
        *,
        loop: "EventLoop | None" = None,
        name: str | None = None,
        context: Context | None = None,
        parent: "Task[Any] | None" = None,
    ) -> None:
        self._loop = loop or current_loop()
        if self._loop is None:
            raise RuntimeError("No current event loop")
        
        if not isinstance(coro, Coroutine):
            raise TypeError(f"Expected coroutine, got {type(coro).__name__}")
        
        Task._task_counter += 1
        self._coro = coro
        self._future: Future[T] = Future(loop=self._loop)
        self._state = TaskState.PENDING
        self._name = name or f"Task-{Task._task_counter}"
        self._context = context or copy_context()
        self._context_token = None
        self._task_ctx: TaskContext | None = None
        self._awaiting_on: Future[Any] | None = None
        self._cancel_requested = False
        self._cancel_message: str | None = None
        self._cancel_stack: list[tuple[str, int]] = []
        # The cancellation scope that requested a cancellation of this task,
        # and whether that cancellation is still waiting to be delivered.
        self._cancel_scope: Any = None
        self._cancel_delivery_pending = False
        self._exception: BaseException | None = None
        self._exception_handled = False
        self._destroyed = False
        self._debug_info: dict[str, Any] = {}
        self._source_traceback = None
        self._created_at = time.monotonic()
        self._scheduled_at = 0.0
        self._started_at = 0.0
        self._finished_at = 0.0
        self._parent_task = parent
        self._children: set[Task[Any]] = set()
        self._cancellation_scope: CancellationScope | None = None
        # Wrappers of the done-callbacks registered on this task, so that
        # the callbacks can be removed again by their original callable.
        self._callback_wrappers: list[tuple[Callable[[Task[Any]], None], Callable[[Future[Any]], None]]] = []
        # Step bookkeeping. A task may be stepped by the event loop thread or
        # by a work-stealing worker thread, but never by two threads at once.
        self._step_state_lock = threading.Lock()
        self._step_run_lock = threading.Lock()
        self._step_in_flight = False
        self._has_pending_step = False
        self._pending_value: Any = None
        self._pending_exc: BaseException | None = None
        
        if parent:
            parent._children.add(self)
        
        if self._loop.get_debug():
            self._source_traceback = traceback.extract_stack()
        
        # Create task context
        self._task_ctx = TaskContext(
            task=self,
            loop=self._loop,
            context=self._context,
            name=self._name,
            coro=coro,
            created_at=self._created_at,
        )
        
        # Schedule the task to run
        self._loop._schedule_task(self)
    
    @property
    def loop(self) -> "EventLoop":
        return self._loop
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def state(self) -> TaskState:
        return self._state
    
    @property
    def coro(self) -> Coroutine[Any, Any, T]:
        return self._coro
    
    @property
    def context(self) -> Context:
        return self._context
    
    @property
    def cancel_message(self) -> str | None:
        return self._cancel_message
    
    @property
    def parent(self) -> "Task[Any] | None":
        return self._parent_task
    
    @property
    def children(self) -> set["Task[Any]"]:
        return self._children
    
    @property
    def cancellation_scope(self) -> "CancellationScope | None":
        return self._cancellation_scope
    
    def done(self) -> bool:
        return self._state in (TaskState.DONE, TaskState.CANCELLED, TaskState.DESTROYED)
    
    def cancelled(self) -> bool:
        return self._state == TaskState.CANCELLED
    
    def result(self) -> T:
        return self._future.result()
    
    def exception(self) -> BaseException | None:
        return self._future.exception()
    
    def add_done_callback(
        self,
        callback: Callable[[Task[T]], None],
        *,
        context: Context | None = None,
    ) -> None:
        # The callback must receive the *task*, not the underlying future.
        def _wrapper(_future: Future[Any]) -> None:
            callback(self)
        
        self._callback_wrappers.append((callback, _wrapper))
        self._future.add_done_callback(_wrapper, context=context)
    
    def remove_done_callback(
        self,
        callback: Callable[[Task[T]], None],
    ) -> int:
        removed = 0
        for original, wrapper in list(self._callback_wrappers):
            if original is callback:
                self._callback_wrappers.remove((original, wrapper))
                self._future.remove_done_callback(wrapper)
                removed += 1
        return removed
    
    def get_stack(self, *, limit: int | None = None) -> list[traceback.FrameSummary]:
        """Get the stack trace for this task."""
        if self._source_traceback:
            return self._source_traceback[:limit] if limit else self._source_traceback
        return []
    
    def get_coro(self) -> Coroutine[Any, Any, T]:
        return self._coro
    
    def cancel(self, msg: str | None = None) -> bool:
        """Request cancellation of this task.
        
        A task that currently sits inside a shielded cancellation scope is
        immune to cancellation.
        """
        if self.done():
            return False
        
        scope = self._cancellation_scope
        if scope is not None and scope.shield:
            # Shielded - refuse the cancellation.
            return False
        
        return self._cancel_unchecked(msg)
    
    def _cancel_unchecked(
        self,
        msg: str | None = None,
        scope: Any = None,
    ) -> bool:
        """Cancel the task, bypassing the shielded-scope check.
        
        *scope* is the cancellation scope that requested the cancellation.
        It is remembered so that the scope can withdraw the cancellation
        if it was never delivered (see ``CancellationScope.__aexit__``).
        """
        if self.done():
            return False
        
        self._cancel_requested = True
        self._cancel_message = msg
        if scope is not None:
            self._cancel_scope = scope
        
        # If task is waiting on a future, cancel it. The done callback will
        # then throw CancelledError into the coroutine.
        if self._awaiting_on and not self._awaiting_on.done():
            self._awaiting_on.cancel(msg)
        elif self._state == TaskState.RUNNING:
            # Task is running but not suspended on a future - schedule the
            # cancellation to be delivered on the next loop iteration.
            self._cancel_delivery_pending = True
            self._loop.call_soon(self._deliver_cancellation, msg)
        
        # Cancel child tasks (shielded children are immune).
        for child in list(self._children):
            child_scope = child._cancellation_scope
            if child_scope is not None and child_scope.shield:
                continue
            child._cancel_unchecked(msg)
        
        # If we have a cancellation scope, trigger it
        if self._cancellation_scope:
            self._cancellation_scope.cancel()
        
        # If task is not running yet, mark as cancelled immediately
        if self._state == TaskState.PENDING:
            self._state = TaskState.CANCELLED
            self._future.cancel(msg)
            self._loop._remove_pending_task(self)
            # The coroutine was never started, so close it explicitly.
            # Otherwise Python emits a "coroutine was never awaited" warning.
            if self._coro is not None:
                try:
                    self._coro.close()
                except Exception:
                    pass
                self._coro = None  # type: ignore[assignment]

        return True
    
    def _deliver_cancellation(self, msg: str | None = None) -> None:
        """Deliver a pending cancellation into the coroutine."""
        if self.done() or self._state == TaskState.PENDING:
            return
        if not self._cancel_delivery_pending:
            # The cancellation was withdrawn (for example because the
            # cancellation scope that requested it has been left).
            return
        self._cancel_delivery_pending = False
        self._cancel_scope = None
        if self._awaiting_on and not self._awaiting_on.done():
            # Now waiting on something - let that future's cancellation
            # drive the delivery.
            self._awaiting_on.cancel(msg)
            return
        self._schedule_step(None, CancelledError(msg or "Task cancelled"))
    
    def cancel_requested(self) -> bool:
        return self._cancel_requested
    
    def _dispatch_step(self) -> None:
        """Hand the pending step to the event loop or to a worker thread."""
        scheduler = self._loop._scheduler
        if scheduler is not None:
            # The scheduler falls back to the event loop when it is not
            # running, so this is always safe.
            scheduler.submit_step(self)
        else:
            self._loop._schedule_task_step(self)

    def _schedule_step(
        self,
        value: Any = None,
        exc: BaseException | None = None,
    ) -> None:
        """Request the next step of this task.
        
        The request is coalesced: if a step is already in flight the new
        value replaces the pending one instead of queueing a second step.
        This keeps the task correct when it is resumed from a different
        thread than the one that ran the previous step.
        """
        with self._step_state_lock:
            self._pending_value = value
            self._pending_exc = exc
            self._has_pending_step = True
            if self._step_in_flight:
                # A step is currently running; it will pick this up when it
                # finishes, so there is nothing more to dispatch.
                return
            self._step_in_flight = True
        
        self._dispatch_step()

    def _run_pending_step(self) -> None:
        """Execute the pending step of this task, if there is one.
        
        Called by the event loop thread and by work-stealing worker threads.
        """
        with self._step_state_lock:
            if not self._has_pending_step:
                # Another thread is already running the step (or it already
                # ran). Do not touch the in-flight flag: it belongs to the
                # step that is currently executing.
                return
            self._has_pending_step = False
            value = self._pending_value
            exc = self._pending_exc
        
        try:
            self._step(value, exc)
        finally:
            with self._step_state_lock:
                self._step_in_flight = False
                more_work = self._has_pending_step
            if more_work:
                # A new request arrived while the step was running.
                with self._step_state_lock:
                    if self._step_in_flight:
                        more_work = False
                    else:
                        self._step_in_flight = True
                if more_work:
                    self._dispatch_step()

    def _step(self, value: Any = None, exc: BaseException | None = None) -> None:
        """Execute one step of the coroutine."""
        if self._destroyed:
            return

        if self._state != TaskState.PENDING and self._state != TaskState.RUNNING:
            return

        # A task must never be stepped by two threads concurrently. The
        # pending-step protocol normally guarantees this; the lock is a
        # defensive guard.
        if not self._step_run_lock.acquire(blocking=False):
            return
        try:
            self._execute_step(value, exc)
        finally:
            self._step_run_lock.release()

    def _execute_step(self, value: Any = None, exc: BaseException | None = None) -> None:
        """Perform a single coroutine step."""
        self._state = TaskState.RUNNING
        self._started_at = time.monotonic()
        
        # Set up context variables
        task_token = set_task_context(self._task_ctx)
        loop_token = set_loop_context(self._loop)
        
        try:
            if exc is not None:
                # Throw exception into coroutine
                try:
                    value = self._coro.throw(exc)
                except (StopIteration, StopAsyncIteration) as e:
                    value = e.value
                except BaseException as e:
                    self._handle_exception(e)
                    return
            else:
                # Send value into coroutine
                try:
                    value = self._coro.send(value)
                except (StopIteration, StopAsyncIteration) as e:
                    value = e.value
                except BaseException as e:
                    self._handle_exception(e)
                    return
            
            # Check if coroutine yielded a Future
            if isinstance(value, Future):
                if value.done():
                    # Future already done, continue immediately
                    self._awaiting_on = None
                    self._continue(value)
                else:
                    # Wait for future to complete
                    self._awaiting_on = value
                    value.add_done_callback(self._on_future_done)
            elif hasattr(value, "__await__"):
                # Handle other awaitables
                awaitable = value.__await__()
                self._awaiting_on = Future(loop=self._loop)
                awaitable.send(None)  # Start the awaitable
                # This is simplified - real implementation would need proper awaitable handling
            else:
                # Coroutine returned a value directly (shouldn't happen normally)
                self._finish(value)
        finally:
            reset_task_context(task_token)
            reset_loop_context(loop_token)
    
    def _on_future_done(self, future: Future[Any]) -> None:
        """Callback when a future we're waiting on completes."""
        self._awaiting_on = None
        self._continue(future)
    
    def _continue(self, future: Future[Any]) -> None:
        """Continue execution after a future completes."""
        if future.cancelled():
            # The cancellation was delivered: the scope that requested it
            # has no further authority over this task.
            self._cancel_scope = None
            self._cancel_delivery_pending = False
            # Throw CancelledError into the coroutine so that it can run
            # its own cleanup (except/finally blocks).
            self._schedule_step(None, CancelledError(f"Future cancelled: {future.cancel_message()}"))
        elif future.exception():
            # Propagate the exception into the coroutine.
            self._schedule_step(None, future.exception())
        else:
            self._schedule_step(future.result())
    
    def _handle_exception(self, exc: BaseException) -> None:
        """Handle an exception from the coroutine."""
        self._exception = exc
        self._exception_handled = False
        
        if isinstance(exc, CancelledError):
            self._state = TaskState.CANCELLED
            self._future.cancel(self._cancel_message)
        else:
            self._state = TaskState.DONE
            self._future.set_exception(exc)
        
        self._finish_cleanup()
    
    def _finish(self, value: T) -> None:
        """Finish the task with a result value."""
        self._state = TaskState.DONE
        self._future.set_result(value)
        self._finish_cleanup()
    
    def _finish_cleanup(self) -> None:
        """Cleanup after task finishes."""
        self._finished_at = time.monotonic()
        self._loop._task_finished(self)
        
        # Notify parent
        if self._parent_task:
            self._parent_task._children.discard(self)
    
    def destroy(self) -> None:
        """Forcefully destroy the task."""
        if not self.done():
            raise TaskDestroyedError("Task destroyed while pending", task=self)
        self._destroyed = True
        self._coro = None  # type: ignore
        self._future = None  # type: ignore
        self._context = None  # type: ignore
        self._task_ctx = None
    
    def __await__(self) -> Generator[Any, None, T]:
        return self._future.__await__()
    
    def __repr__(self) -> str:
        state = self._state.name
        info = f"name={self._name!r} state={state}"
        if self._cancel_message:
            info += f" cancel_msg={self._cancel_message!r}"
        return f"<Task {info}>"


# Task creation and management
def create_task(
    coro: Coroutine[Any, Any, T],
    *,
    loop: "EventLoop | None" = None,
    name: str | None = None,
    context: Context | None = None,
) -> Task[T]:
    """Create a new task from a coroutine.
    
    The new task is parented to the currently running task (if any) so that
    structured cancellation can propagate down the task tree.
    """
    parent = current_task()
    return Task(coro, loop=loop, name=name, context=context, parent=parent)


def current_task(loop: "EventLoop | None" = None) -> Task[Any] | None:
    """Get the currently running task."""
    ctx = get_task_context()
    return ctx.task if ctx else None


def all_tasks(loop: "EventLoop | None" = None) -> set[Task[Any]]:
    """Get all tasks for a loop."""
    loop = loop or current_loop()
    if loop is None:
        return set()
    return loop._all_tasks.copy()


async def sleep(delay: float, result: Any = None) -> Any:
    """Sleep for a specified delay."""
    loop = current_loop()
    if loop is None:
        raise RuntimeError("No current event loop")
    
    if delay <= 0:
        # Yield control immediately without timer
        return await _yield()
    
    future: Future[Any] = Future(loop=loop)
    
    def _set_result():
        try:
            if not future.done():
                future.set_result(result)
        except InvalidStateError:
            # Future already cancelled or finished, ignore
            pass
    
    handle = loop.call_later(delay, _set_result)
    
    try:
        return await future
    finally:
        handle.cancel()


async def _yield() -> Any:
    """Yield control to the event loop."""
    loop = current_loop()
    if loop is None:
        raise RuntimeError("No current event loop")
    
    future: Future[Any] = Future(loop=loop)
    # Schedule to run on next iteration
    loop.call_soon(future.set_result, None)
    return await future


async def sleep_forever() -> None:
    """Sleep forever (until cancelled)."""
    from .context import current_loop
    loop = current_loop()
    if loop is None:
        raise RuntimeError("No current event loop")
    
    future: Future[None] = Future(loop=loop)
    # No handle to wake it up - will only complete on cancellation
    try:
        await future
    except CancelledError:
        raise


def shield(awaitable: Awaitable[T]) -> Awaitable[T]:
    """Shield an awaitable from cancellation."""
    # This is a simplified version - real implementation would use CancellationScope
    async def _shielded() -> T:
        return await awaitable
    return _shielded()


# Forward reference for CancellationScope
class CancellationScope:
    pass