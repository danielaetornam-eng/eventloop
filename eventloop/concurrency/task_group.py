"""
Structured concurrency primitives for GodLoop.
"""

from __future__ import annotations

import sys
import traceback
import weakref
from collections.abc import Callable, Coroutine, AsyncIterator, Awaitable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from typing import (
    Any,
    Generic,
    TypeVar,
    Optional,
    Union,
    overload,
    final,
)
from typing_extensions import Self

from ..core.types import (
    CancelledError,
    TaskCancelledError,
    TaskDestroyedError,
    GodLoopError,
)
from ..core.task import Task, create_task, current_task, all_tasks
from ..core.loop import EventLoop
from ..core.context import get_task_context, set_task_context

__all__ = [
    "CancellationScope",
    "CancellationError",
    "shield",
    "TaskGroup",
    "TaskGroupError",
    "Nursery",
    "NurseryError",
    "open_nursery",
    "create_task_group",
]

T = TypeVar("T")


class CancellationScope(AbstractAsyncContextManager):
    """
    A cancellation scope provides structured cancellation.
    
    When a scope is cancelled, all tasks within it are cancelled.
    Scopes can be nested, and cancellation propagates to children.
    """
    
    __slots__ = (
        "_parent",
        "_children",
        "_cancelled",
        "_cancel_called",
        "_shield",
        "_deadline",
        "_task",
        "_context",
        "_entered",
        "_exited",
        "_cancel_callbacks",
        "_weakref",
        "__weakref__",
        "_previous_scope",
    )
    
    def __init__(
        self,
        *,
        deadline: float | None = None,
        shield: bool = False,
    ) -> None:
        self._parent: CancellationScope | None = None
        self._children: set[CancellationScope] = set()
        self._cancelled = False
        self._cancel_called = False
        self._shield = shield
        self._deadline = deadline
        self._task: Task[Any] | None = None
        self._context: Context | None = None
        self._entered = False
        self._exited = False
        self._cancel_callbacks: list[Callable[[], None]] = []
        self._previous_scope: "CancellationScope | None" = None
        self._weakref = weakref.ref(self)
    
    @property
    def cancelled(self) -> bool:
        return self._cancelled
    
    @property
    def cancel_called(self) -> bool:
        return self._cancel_called
    
    @property
    def shield(self) -> bool:
        return self._shield
    
    @property
    def deadline(self) -> float | None:
        return self._deadline
    
    @property
    def parent(self) -> "CancellationScope | None":
        return self._parent
    
    @property
    def children(self) -> set["CancellationScope"]:
        return self._children.copy()
    
    def cancel(self) -> None:
        """Cancel this scope and all child scopes."""
        if self._cancelled:
            return
        
        self._cancelled = True
        self._cancel_called = True
        
        # Cancel all child scopes (shielded scopes are immune).
        for child in list(self._children):
            if child.shield:
                continue
            child.cancel()
        
        if self._task and not self._task.done():
            # Cancel the host task's unprotected children.
            for child in list(self._task._children):
                child_scope = child._cancellation_scope
                if child_scope is not None and child_scope.shield:
                    continue
                child._cancel_unchecked("Scope cancelled")
            
            # Cancel the host task itself, unless it has moved into a
            # shielded scope of its own (in which case it is immune).
            current = self._task._cancellation_scope
            if current is None or current is self or not current.shield:
                self._task._cancel_unchecked("Scope cancelled", scope=self)
        
        # Run cancel callbacks
        for callback in self._cancel_callbacks:
            try:
                callback()
            except Exception:
                pass  # Ignore errors in cancel callbacks
    
    def add_cancel_callback(self, callback: Callable[[], None]) -> None:
        """Add a callback to run when scope is cancelled."""
        self._cancel_callbacks.append(callback)
    
    def remove_cancel_callback(self, callback: Callable[[], None]) -> bool:
        """Remove a cancel callback."""
        try:
            self._cancel_callbacks.remove(callback)
            return True
        except ValueError:
            return False
    
    def _check_deadline(self) -> None:
        """Check if deadline has passed."""
        if self._deadline is not None:
            import time
            if time.monotonic() >= self._deadline:
                self.cancel()
    
    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError("CancellationScope already entered")
        
        self._entered = True
        
        # Capture current task and context
        self._task = current_task()
        if self._task:
            # Remember the scope we are nested in so it can be restored.
            self._previous_scope = self._task._cancellation_scope
            self._task._cancellation_scope = self
        
        self._context = copy_context()
        
        # Link to parent scope
        if self._task:
            # Find parent scope from task's parent chain
            parent_task = self._task.parent
            while parent_task:
                if parent_task._cancellation_scope:
                    self._parent = parent_task._cancellation_scope
                    self._parent._children.add(self)
                    break
                parent_task = parent_task.parent
        
        return self
    
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        self._exited = True
        
        # Restore the scope we were nested in.
        if self._task is not None:
            self._task._cancellation_scope = self._previous_scope
            self._previous_scope = None
            
            # If this scope requested a cancellation of the host task but
            # the cancellation was never delivered (the host task was
            # running when it was requested and left the scope without
            # suspending), withdraw it: the authority of a cancellation
            # scope ends with the scope. Otherwise the cancellation would
            # be delivered at some unrelated later suspension point.
            if self._task._cancel_scope is self:
                self._task._cancel_scope = None
                self._task._cancel_delivery_pending = False
                self._task._cancel_requested = False
                self._task._cancel_message = None
        
        # Unlink from parent
        if self._parent:
            self._parent._children.discard(self)
            self._parent = None
        
        # If we were cancelled, suppress CancelledError
        if self._cancelled and exc_type is CancelledError:
            return True
        
        return False
    
    def __repr__(self) -> str:
        return f"<CancellationScope cancelled={self._cancelled} shield={self._shield}>"


class CancellationError(GodLoopError, BaseException):
    """Raised when an operation is cancelled due to scope cancellation."""
    pass


def shield(awaitable: Awaitable[T]) -> Awaitable[T]:
    """
    Shield an awaitable from cancellation.
    
    The awaitable will still run to completion even if the enclosing
    cancellation scope is cancelled.
    """
    async def _shielded() -> T:
        # Create a shielded scope
        async with CancellationScope(shield=True):
            return await awaitable
    return _shielded()


class TaskGroup(AbstractAsyncContextManager):
    """
    A task group manages a collection of tasks with structured concurrency.
    
    All tasks in the group are awaited when the group exits.
    If any task fails with an exception (other than CancelledError),
    all other tasks are cancelled and the exception is raised.
    """
    
    __slots__ = (
        "_tasks",
        "_errors",
        "_cancelled",
        "_completed",
        "_loop",
        "_name",
        "_context",
    )
    
    def __init__(self, *, name: str | None = None) -> None:
        self._tasks: set[Task[Any]] = set()
        self._errors: list[BaseException] = []
        self._cancelled = False
        self._completed = False
        self._loop: EventLoop | None = None
        self._name = name
        self._context = copy_context()
    
    @property
    def name(self) -> str | None:
        return self._name
    
    @property
    def tasks(self) -> set[Task[Any]]:
        return self._tasks.copy()
    
    def create_task(
        self,
        coro: Coroutine[Any, Any, T],
        *,
        name: str | None = None,
    ) -> Task[T]:
        """Create a task in this group."""
        if self._completed:
            raise RuntimeError("Cannot create tasks after group completed")
        
        task = create_task(coro, name=name)
        self._tasks.add(task)
        
        # Add done callback to track completion
        def _on_done(t: Task[Any]) -> None:
            self._tasks.discard(t)
            if t.cancelled():
                pass  # Ignore cancelled tasks
            elif t.exception():
                self._errors.append(t.exception())
        
        task.add_done_callback(_on_done)
        return task
    
    async def __aenter__(self) -> Self:
        from ..core.context import current_loop
        self._loop = current_loop()
        return self
    
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        self._completed = True
        
        # If we're exiting due to an exception, cancel all tasks
        if exc_type is not None and exc_type is not CancelledError:
            self.cancel()
        
        # Wait for all tasks to complete
        while self._tasks:
            # Wait for any task to complete
            done, pending = await self._wait_tasks()
            
            # Remove completed tasks from the group
            for task in done:
                self._tasks.discard(task)
            
            # If we have errors, cancel remaining
            if self._errors:
                self.cancel()
        
        # Raise first error if any
        if self._errors:
            if len(self._errors) == 1:
                raise self._errors[0]
            else:
                raise TaskGroupError(
                    f"Task group {self._name!r} had multiple errors",
                    errors=self._errors,
                )
        
        return False  # Don't suppress exceptions
    
    async def _wait_tasks(self) -> tuple[set[Task[Any]], set[Task[Any]]]:
        """Wait for at least one task to complete."""
        if not self._tasks:
            return set(), set()
        
        # Take a snapshot of the tasks we are waiting for: completed tasks
        # are removed from self._tasks by their done-callback while we are
        # waiting, so self._tasks itself is not a stable basis.
        waiting = set(self._tasks)
        done = {task for task in waiting if task.done()}
        pending = waiting - done
        
        if done:
            return done, pending
        
        # Wait for the first task to complete
        while not done:
            await self._wait_any(waiting)
            done = {task for task in waiting if task.done()}
            pending = waiting - done
        
        return done, pending
    
    async def _wait_any(self, tasks: set[Task[Any]] | None = None) -> None:
        """Wait for any task to complete."""
        if tasks is None:
            tasks = set(self._tasks)
        if not tasks:
            return
        
        # Create a future that will be set when any task completes
        future = self._loop.create_future()
        
        def _on_task_done(task):
            if not future.done():
                try:
                    future.set_result(task)
                except Exception:
                    # Ignore any errors (future already done, cancelled, etc.)
                    pass
        
        # Add callback to all pending tasks
        for task in list(tasks):
            if not task.done():
                task.add_done_callback(_on_task_done)
        
        try:
            await future
        finally:
            # Remove callbacks
            for task in list(tasks):
                if not task.done():
                    task.remove_done_callback(_on_task_done)
    
    def cancel(self) -> None:
        """Cancel all tasks in the group."""
        if self._cancelled:
            return
        self._cancelled = True
        for task in list(self._tasks):
            task.cancel("Task group cancelled")


class TaskGroupError(GodLoopError):
    """Exception raised when a task group has multiple errors."""
    
    def __init__(
        self,
        message: str,
        *,
        errors: list[BaseException],
    ) -> None:
        super().__init__(message)
        self.errors = errors
    
    def __repr__(self) -> str:
        return f"<TaskGroupError {len(self.errors)} errors>"


class Nursery(AbstractAsyncContextManager):
    """
    A nursery is a lightweight alternative to TaskGroup.
    
    It provides fire-and-forget task spawning with automatic
    cleanup on scope exit. Unlike TaskGroup, it doesn't collect
    results by default.
    """
    
    __slots__ = (
        "_tasks",
        "_errors",
        "_cancelled",
        "_completed",
        "_loop",
        "_name",
        "_strict",
    )
    
    def __init__(self, *, name: str | None = None, strict: bool = False) -> None:
        self._tasks: set[Task[Any]] = set()
        self._errors: list[BaseException] = []
        self._cancelled = False
        self._completed = False
        self._loop: EventLoop | None = None
        self._name = name
        self._strict = strict  # If True, raise on any error
    
    @property
    def name(self) -> str | None:
        return self._name
    
    def start_soon(
        self,
        coro: Coroutine[Any, Any, Any] | Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        name: str | None = None,
    ) -> Task[Any]:
        """Start a task in the nursery (fire and forget).
        
        Either a coroutine or a coroutine function (plus its arguments) can
        be given, like trio's ``nursery.start_soon``.
        """
        if self._completed:
            raise NurseryError("Cannot start tasks after nursery completed")
        
        if not isinstance(coro, Coroutine) and callable(coro):
            coro = coro(*args)
        
        task = create_task(coro, name=name)
        self._tasks.add(task)
        
        def _on_done(t: Task[Any]) -> None:
            self._tasks.discard(t)
            if t.cancelled():
                pass
            elif t.exception():
                self._errors.append(t.exception())
                # A failing child cancels its siblings, like trio's nursery.
                self.cancel()
        
        task.add_done_callback(_on_done)
        return task
    
    async def __aenter__(self) -> Self:
        from ..core.context import current_loop
        self._loop = current_loop()
        return self
    
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        self._completed = True
        
        if exc_type is not None and exc_type is not CancelledError:
            self.cancel()
        
        # Wait for all tasks
        while self._tasks:
            if self._errors:
                # A child failed: cancel the remaining siblings.
                self.cancel()
            # A snapshot is used because completed tasks are removed from
            # self._tasks by their done-callback.
            await self._wait_any(set(self._tasks))
        
        if self._errors:
            if len(self._errors) == 1 and not self._strict:
                # A single child error is propagated unchanged.
                raise self._errors[0]
            raise NurseryError(
                f"Nursery {self._name!r} had {len(self._errors)} error(s)",
                errors=self._errors,
            )
        
        return False
    
    async def _wait_any(self, tasks: set[Task[Any]] | None = None) -> None:
        """Wait until one of the nursery's tasks completes."""
        if tasks is None:
            tasks = set(self._tasks)
        if not tasks:
            return
        
        if self._loop is None:
            import asyncio
            await asyncio.sleep(0)
            return
        
        future = self._loop.create_future()
        
        def _on_task_done(task: Task[Any]) -> None:
            if not future.done():
                try:
                    future.set_result(task)
                except Exception:
                    pass
        
        for task in list(tasks):
            if not task.done():
                task.add_done_callback(_on_task_done)
        
        try:
            await future
        finally:
            for task in list(tasks):
                if not task.done():
                    task.remove_done_callback(_on_task_done)
    
    def cancel(self) -> None:
        if self._cancelled:
            return
        self._cancelled = True
        for task in list(self._tasks):
            task.cancel("Nursery cancelled")


class NurseryError(GodLoopError):
    """Exception raised by nursery errors."""
    
    def __init__(
        self,
        message: str,
        *,
        errors: list[BaseException] | None = None,
    ) -> None:
        super().__init__(message)
        self.errors = errors or []


async def open_nursery(
    *,
    name: str | None = None,
    strict: bool = False,
) -> Nursery:
    """Open a new nursery."""
    return Nursery(name=name, strict=strict)


def create_task_group(*, name: str | None = None) -> TaskGroup:
    """Create a new task group."""
    return TaskGroup(name=name)