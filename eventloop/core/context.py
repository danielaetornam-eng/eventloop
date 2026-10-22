"""
Context management utilities for GodLoop.
"""

from __future__ import annotations

import contextvars
import sys
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import Context, ContextVar, copy_context
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
    overload,
)
from typing_extensions import Self

from .types import LoopConfig

__all__ = [
    "LoopContext",
    "TaskContext",
    "ContextManager",
    "current_loop",
    "current_task_ctx",
    "set_loop_context",
    "get_loop_context",
    "run_with_context",
    "copy_current_context",
]

T = TypeVar("T")
R = TypeVar("R")

# Context variables for tracking current loop and task
_current_loop: ContextVar["EventLoop | None"] = ContextVar("_current_loop", default=None)
_current_task_ctx: ContextVar["TaskContext | None"] = ContextVar("_current_task_ctx", default=None)


@dataclass(slots=True)
class LoopContext:
    """Context associated with an event loop."""
    
    loop: "EventLoop"
    config: LoopConfig
    debug: bool = False
    _context: Context = field(default_factory=contextvars.copy_context)
    
    def run(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run a function within this loop's context."""
        return self._context.run(func, *args, **kwargs)
    
    def copy(self) -> Context:
        """Copy this loop's context."""
        return self._context.copy()


@dataclass(slots=True)
class TaskContext:
    """Context associated with a task."""
    
    task: "Task[Any]"
    loop: "EventLoop"
    context: Context
    name: str | None = None
    coro: types.CoroutineType[Any, Any, Any] | None = None
    
    # Cancellation state
    cancelled: bool = False
    cancelling: bool = False
    cancel_message: str | None = None
    
    # Exception handling
    exception: BaseException | None = None
    exception_handled: bool = False
    
    # Debug info
    created_at: float = 0.0
    scheduled_at: float = 0.0
    started_at: float = 0.0
    finished_at: float = 0.0


class ContextManager:
    """Manages context for the event loop and tasks."""
    
    def __init__(self, loop: "EventLoop") -> None:
        self._loop = loop
        self._context = contextvars.copy_context()
    
    @property
    def loop(self) -> "EventLoop":
        return self._loop
    
    @property
    def context(self) -> Context:
        return self._context
    
    def run(self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        """Run a function in the loop's context."""
        return self._context.run(func, *args, **kwargs)
    
    def copy(self) -> Context:
        """Copy the current context."""
        return self._context.copy()


# Context variable accessors
def current_loop() -> "EventLoop | None":
    """Get the currently running event loop."""
    return _current_loop.get()


def current_task_ctx() -> TaskContext | None:
    """Get the current task context."""
    return _current_task_ctx.get()


def set_loop_context(loop: "EventLoop | None") -> ContextVar.Token["EventLoop | None"]:
    """Set the current loop context."""
    return _current_loop.set(loop)


def get_loop_context() -> "EventLoop | None":
    """Get the current loop context without setting."""
    return _current_loop.get()


def set_task_context(ctx: TaskContext | None) -> ContextVar.Token[TaskContext | None]:
    """Set the current task context."""
    return _current_task_ctx.set(ctx)


def reset_task_context(token: ContextVar.Token[TaskContext | None]) -> None:
    """Restore the previous task context."""
    _current_task_ctx.reset(token)


def reset_loop_context(token: ContextVar.Token["EventLoop | None"]) -> None:
    """Restore the previous loop context."""
    _current_loop.reset(token)


def get_task_context() -> TaskContext | None:
    """Get the current task context without setting."""
    return _current_task_ctx.get()


def copy_current_context() -> Context:
    """Copy the current context."""
    return copy_context()


@overload
def run_with_context(context: Context, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T: ...
@overload
def run_with_context(context: Context, func: Callable[P, Awaitable[T]], *args: P.args, **kwargs: P.kwargs) -> Awaitable[T]: ...


def run_with_context(context: Context, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run a function with a specific context."""
    return context.run(func, *args, **kwargs)


@asynccontextmanager
async def context_manager(loop: "EventLoop") -> AbstractAsyncContextManager["EventLoop"]:
    """Async context manager for setting loop context."""
    token = set_loop_context(loop)
    try:
        yield loop
    finally:
        _current_loop.reset(token)