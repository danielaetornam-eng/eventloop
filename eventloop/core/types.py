"""
Core type definitions and base classes for GodLoop.
"""

from __future__ import annotations

import sys
import types
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Coroutine, Generator, Iterator
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from contextvars import Context, ContextVar, copy_context
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    ClassVar,
    Final,
    Generic,
    Literal,
    NoReturn,
    ParamSpec,
    Protocol,
    Self,
    TypeAlias,
    TypeVar,
    overload,
    final,
)
from typing_extensions import Self as SelfType

if sys.version_info >= (3, 11):
    from typing import Never
else:
    Never = type("Never", (), {"__instancecheck__": lambda *_: False})

__all__ = [
    # Type variables
    "T",
    "T_co",
    "T_contra",
    "P",
    "R",
    # Type aliases
    "CoroutineType",
    "GeneratorType",
    "AwaitableType",
    "CallableType",
    "ContextManagerType",
    "AsyncContextManagerType",
    # Enums
    "LoopState",
    "TaskState",
    "FutureState",
    "HandleState",
    # Protocols
    "Runnable",
    "AwaitableProtocol",
    "AsyncIteratorProtocol",
    "AsyncContextManagerProtocol",
    # Dataclasses
    "LoopConfig",
    "TimerConfig",
    "SchedulerConfig",
    # Exceptions
    "GodLoopError",
    "LoopClosedError",
    "TaskCancelledError",
    "TaskDestroyedError",
    "InvalidStateError",
    "TimeoutError",
    "CancelledError",
    "ShutdownError",
]

# Type variables
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)
P = ParamSpec("P")
R = TypeVar("R")

# Type aliases
CoroutineType: TypeAlias = Coroutine[Any, Any, T]
GeneratorType: TypeAlias = Generator[Any, Any, T]
AwaitableType: TypeAlias = Awaitable[T]
CallableType: TypeAlias = Callable[P, R]
ContextManagerType: TypeAlias = AbstractContextManager[T]
AsyncContextManagerType: TypeAlias = AbstractAsyncContextManager[T]

# Protocols
class Runnable(Protocol):
    """Protocol for objects that can be scheduled and run."""
    
    def __call__(self) -> None: ...
    
    def send(self, value: Any) -> Any: ...
    
    def throw(self, typ: type[BaseException], val: BaseException | None = None, tb: types.TracebackType | None = None) -> Any: ...
    
    def close(self) -> None: ...


class AwaitableProtocol(Protocol[T_co]):
    """Protocol for awaitable objects."""
    
    def __await__(self) -> Generator[Any, None, T_co]: ...


class AsyncIteratorProtocol(Protocol[T_co]):
    """Protocol for async iterators."""
    
    def __aiter__(self) -> Self: ...
    
    async def __anext__(self) -> T_co: ...


class AsyncContextManagerProtocol(Protocol[T_co]):
    """Protocol for async context managers."""
    
    async def __aenter__(self) -> T_co: ...
    
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> bool | None: ...


# Enums
class LoopState(Enum):
    """Event loop lifecycle states."""
    
    CREATED = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()
    CLOSED = auto()


class TaskState(Enum):
    """Task lifecycle states."""
    
    PENDING = auto()
    RUNNING = auto()
    DONE = auto()
    CANCELLED = auto()
    DESTROYED = auto()


class FutureState(Enum):
    """Future lifecycle states."""
    
    PENDING = auto()
    FINISHED = auto()
    CANCELLED = auto()


class HandleState(Enum):
    """Handle lifecycle states."""
    
    ACTIVE = auto()
    CANCELLED = auto()
    EXECUTED = auto()


# Dataclasses
@dataclass(frozen=True, slots=True)
class LoopConfig:
    """Configuration for EventLoop behavior."""
    
    # I/O backend selection
    use_io_uring: bool = False
    io_uring_entries: int = 256
    
    # Timer configuration
    timer_resolution: float = 0.001  # 1ms default
    use_hierarchical_wheel: bool = True
    
    # Scheduler configuration
    enable_work_stealing: bool = False
    worker_threads: int = 0  # 0 = auto (CPU count)
    max_tasks_per_worker: int = 1024
    
    # Debug configuration
    debug: bool = False
    slow_callback_duration: float = 0.1  # 100ms
    enable_tracing: bool = False
    
    # Compatibility
    asyncio_compat: bool = True
    
    # Platform-specific
    use_proactor_on_windows: bool = True


@dataclass(frozen=True, slots=True)
class TimerConfig:
    """Configuration for timer wheel."""
    
    tick_duration: float = 0.001  # 1ms
    wheel_size: int = 512
    num_levels: int = 4
    max_interval: float = 86400.0  # 24 hours


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    """Configuration for work-stealing scheduler."""
    
    num_workers: int = 0  # 0 = auto
    queue_size: int = 1024
    steal_batch_size: int = 16
    idle_timeout: float = 0.01  # 10ms
    enable_affinity: bool = False


# Base Exceptions
class GodLoopError(Exception):
    """Base exception for all GodLoop errors."""
    
    def __init__(self, message: str, *, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self._cause = cause
    
    @property
    def cause(self) -> BaseException | None:
        return self._cause


class LoopClosedError(GodLoopError):
    """Raised when operating on a closed event loop."""
    pass


class TaskCancelledError(GodLoopError):
    """Raised when a task is cancelled."""
    
    def __init__(self, message: str = "Task was cancelled", *, task: Task[Any] | None = None) -> None:
        super().__init__(message)
        self.task = task


class TaskDestroyedError(GodLoopError):
    """Raised when a task is destroyed while pending."""
    
    def __init__(self, message: str = "Task was destroyed while pending", *, task: Task[Any] | None = None) -> None:
        super().__init__(message)
        self.task = task


class InvalidStateError(GodLoopError):
    """Raised when an operation is invalid for the current state."""
    pass


class TimeoutError(GodLoopError):
    """Raised when an operation times out."""
    
    def __init__(self, message: str = "Operation timed out", *, timeout: float | None = None) -> None:
        super().__init__(message)
        self.timeout = timeout


class CancelledError(GodLoopError, BaseException):
    """Base cancellation exception (inherits from BaseException like asyncio.CancelledError)."""
    
    def __init__(self, message: str = "Operation cancelled", *, task: Task[Any] | None = None) -> None:
        super().__init__(message)
        self.task = task


class ShutdownError(GodLoopError):
    """Raised during graceful shutdown."""
    pass


# Forward references for type hints
class Task(Generic[T]):
    pass


class AbstractAsyncContextManager(Generic[T_co]):
    """Abstract base class for async context managers."""
    
    @abstractmethod
    async def __aenter__(self) -> T_co: ...
    
    @abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> bool | None: ...