"""
Handle system for callbacks, timers, and I/O events.
"""

from __future__ import annotations

import sys
import time
import weakref
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, Protocol, TypeVar, final
from typing_extensions import Self

from .types import HandleState, LoopConfig

__all__ = [
    "Handle",
    "TimerHandle",
    "IOHandle",
    "CallbackHandle",
    "HandleCallback",
    "HandleFactory",
    "create_handle",
    "create_timer_handle",
    "create_io_handle",
]

T = TypeVar("T")


class HandleCallback(Protocol):
    """Protocol for handle callbacks."""
    
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


class Handle(ABC):
    """Base class for all handles."""
    
    __slots__ = (
        "_loop",
        "_callback",
        "_args",
        "_kwargs",
        "_state",
        "_cancelled",
        "_debug_info",
    )
    
    def __init__(
        self,
        loop: "EventLoop",
        callback: HandleCallback,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._loop = loop
        self._callback = callback
        self._args = args
        self._kwargs = kwargs
        self._state: HandleState = HandleState.ACTIVE
        self._cancelled = False
        self._debug_info: dict[str, Any] = {}
    
    @property
    def loop(self) -> "EventLoop":
        return self._loop
    
    @property
    def callback(self) -> HandleCallback:
        return self._callback
    
    @property
    def args(self) -> tuple[Any, ...]:
        return self._args
    
    @property
    def kwargs(self) -> dict[str, Any]:
        return self._kwargs
    
    @property
    def state(self) -> HandleState:
        return self._state
    
    @property
    def cancelled(self) -> bool:
        return self._cancelled
    
    @property
    def debug_info(self) -> dict[str, Any]:
        return self._debug_info
    
    def cancel(self) -> bool:
        """Cancel this handle. Returns True if was cancelled, False if already done."""
        if self._state != HandleState.ACTIVE:
            return False
        self._cancelled = True
        self._state = HandleState.CANCELLED
        self._on_cancel()
        return True
    
    def _on_cancel(self) -> None:
        """Hook called when handle is cancelled."""
        pass
    
    @abstractmethod
    def _run(self) -> None:
        """Execute the handle's callback."""
        pass
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} state={self._state.name} callback={self._callback!r}>"


class TimerHandle(Handle):
    """Handle for timer callbacks."""
    
    __slots__ = ("_when", "_periodic", "_interval", "__weakref__")
    
    def __init__(
        self,
        loop: "EventLoop",
        when: float,
        callback: HandleCallback,
        *args: Any,
        periodic: bool = False,
        interval: float = 0.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(loop, callback, *args, **kwargs)
        self._when = when
        self._periodic = periodic
        self._interval = interval
    
    @property
    def when(self) -> float:
        return self._when
    
    @property
    def periodic(self) -> bool:
        return self._periodic
    
    @property
    def interval(self) -> float:
        return self._interval
    
    def reschedule(self, when: float) -> None:
        """Reschedule this timer."""
        if self._state != HandleState.ACTIVE:
            raise RuntimeError("Cannot reschedule cancelled or executed timer")
        self._when = when
        self._loop._timer_wheel.reschedule(self)
    
    def _run(self) -> None:
        if self._cancelled:
            return
        try:
            self._callback(*self._args, **self._kwargs)
        except Exception as e:
            if self._loop is not None:
                self._loop.call_exception_handler({
                    "message": "Exception in timer callback",
                    "exception": e,
                    "handle": self,
                })
        finally:
            if self._periodic and not self._cancelled:
                self._when += self._interval
                if self._loop is not None:
                    self._loop._timer_wheel.schedule(self)
            else:
                self._state = HandleState.EXECUTED
    
    def __lt__(self, other: "TimerHandle") -> bool:
        return self._when < other._when
    
    def __repr__(self) -> str:
        return f"<TimerHandle when={self._when:.6f} periodic={self._periodic} state={self._state.name}>"


class IOHandle(Handle):
    """Handle for I/O events.
    
    An I/O handle owns a file descriptor and tracks whether the loop is
    interested in reading and/or writing on it. The selector is registered
    with the combined event mask.
    """
    
    __slots__ = ("_fd", "_events", "_reader", "_writer", "__weakref__")
    
    def __init__(
        self,
        loop: "EventLoop",
        fd: int,
        events: int = 0,
        callback: HandleCallback | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(loop, callback or (lambda: None), *args, **kwargs)
        self._fd = fd
        self._events = events
        self._reader: HandleCallback | None = None
        self._writer: HandleCallback | None = None
    
    @property
    def fd(self) -> int:
        return self._fd
    
    @property
    def events(self) -> int:
        """The selector event mask currently in effect."""
        return self._events
    
    @property
    def reader(self) -> HandleCallback | None:
        return self._reader
    
    @property
    def writer(self) -> HandleCallback | None:
        return self._writer
    
    def add_reader(self, callback: HandleCallback) -> None:
        """Register a read callback and update the selector mask."""
        self._reader = callback
        self._update_events()
    
    def add_writer(self, callback: HandleCallback) -> None:
        """Register a write callback and update the selector mask."""
        self._writer = callback
        self._update_events()
    
    def remove_reader(self) -> None:
        """Unregister the read callback and update the selector mask."""
        self._reader = None
        self._update_events()
    
    def remove_writer(self) -> None:
        """Unregister the write callback and update the selector mask."""
        self._writer = None
        self._update_events()
    
    def _update_events(self) -> None:
        """Recompute the selector event mask from the registered callbacks."""
        import selectors
        
        events = 0
        if self._reader is not None:
            events |= selectors.EVENT_READ
        if self._writer is not None:
            events |= selectors.EVENT_WRITE
        
        if events == self._events:
            return
        
        self._events = events
        if events == 0:
            self._loop._remove_io_handle(self)
        else:
            self._loop._update_io_events(self)
    
    def _run(self, events: int) -> None:
        if self._cancelled:
            return
        try:
            if events & 1 and self._reader:  # READ
                self._reader()
            if events & 2 and self._writer:  # WRITE
                self._writer()
        except Exception as e:
            self._loop.call_exception_handler({
                "message": "Exception in I/O callback",
                "exception": e,
                "handle": self,
            })
    
    def _on_cancel(self) -> None:
        self._loop._remove_io_handle(self)
    
    def __repr__(self) -> str:
        return f"<IOHandle fd={self._fd} events={self._events} state={self._state.name}>"


class CallbackHandle(Handle):
    """Handle for immediate callbacks (call_soon, call_soon_threadsafe)."""
    
    __slots__ = ("__weakref__",)
    
    def _run(self) -> None:
        if self._cancelled:
            return
        try:
            self._callback(*self._args, **self._kwargs)
        except Exception as e:
            self._loop.call_exception_handler({
                "message": "Exception in callback",
                "exception": e,
                "handle": self,
            })
        finally:
            self._state = HandleState.EXECUTED
    
    def __repr__(self) -> str:
        return f"<CallbackHandle state={self._state.name} callback={self._callback!r}>"


class HandleFactory:
    """Factory for creating handles."""
    
    def __init__(self, loop: "EventLoop") -> None:
        self._loop = loop
    
    def create_callback(
        self,
        callback: HandleCallback,
        *args: Any,
        **kwargs: Any,
    ) -> CallbackHandle:
        return CallbackHandle(self._loop, callback, *args, **kwargs)
    
    def create_timer(
        self,
        when: float,
        callback: HandleCallback,
        *args: Any,
        periodic: bool = False,
        interval: float = 0.0,
        **kwargs: Any,
    ) -> TimerHandle:
        return TimerHandle(
            self._loop, when, callback, *args, periodic=periodic, interval=interval, **kwargs
        )
    
    def create_io(
        self,
        fd: int,
        events: int,
        callback: HandleCallback,
        *args: Any,
        **kwargs: Any,
    ) -> IOHandle:
        return IOHandle(self._loop, fd, events, callback, *args, **kwargs)


def create_handle(
    loop: "EventLoop",
    callback: HandleCallback,
    *args: Any,
    **kwargs: Any,
) -> CallbackHandle:
    return CallbackHandle(loop, callback, *args, **kwargs)


def create_timer_handle(
    loop: "EventLoop",
    when: float,
    callback: HandleCallback,
    *args: Any,
    periodic: bool = False,
    interval: float = 0.0,
    **kwargs: Any,
) -> TimerHandle:
    return TimerHandle(
        loop, when, callback, *args, periodic=periodic, interval=interval, **kwargs
    )


def create_io_handle(
    loop: "EventLoop",
    fd: int,
    events: int = 0,
    callback: HandleCallback | None = None,
    *args: Any,
    **kwargs: Any,
) -> IOHandle:
    return IOHandle(loop, fd, events, callback, *args, **kwargs)