"""
Debug tracing for event loop.
"""

from __future__ import annotations

import sys
import time
import threading
import traceback
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Optional
from typing_extensions import Self

from ..core.loop import EventLoop
from ..core.handle import Handle, TimerHandle, IOHandle, CallbackHandle
from ..core.task import Task

__all__ = [
    "EventLoopTracer",
    "TraceEvent",
    "trace_loop",
    "TraceContext",
]


@dataclass(frozen=True, slots=True)
class TraceEvent:
    """A single trace event."""
    timestamp: float
    event_type: str
    details: dict[str, Any]
    thread_id: int
    stack: list[str] | None = None


class EventLoopTracer:
    """
    Event loop tracer for debugging and profiling.
    
    Records all significant events in the event loop for later analysis.
    """
    
    __slots__ = (
        "_loop",
        "_enabled",
        "_events",
        "_max_events",
        "_callbacks",
        "_lock",
    )
    
    def __init__(
        self,
        loop: EventLoop,
        max_events: int = 10000,
    ) -> None:
        self._loop = loop
        self._enabled = False
        self._events: list[TraceEvent] = []
        self._max_events = max_events
        self._callbacks: list[Callable[[TraceEvent], None]] = []
        self._lock = threading.Lock()
    
    @property
    def enabled(self) -> bool:
        return self._enabled
    
    @property
    def events(self) -> list[TraceEvent]:
        with self._lock:
            return self._events.copy()
    
    def enable(self) -> None:
        """Enable tracing."""
        self._enabled = True
    
    def disable(self) -> None:
        """Disable tracing and detach from the loop."""
        self._enabled = False
        if getattr(self._loop, "tracer", None) is self:
            self._loop.set_tracer(None)
    
    def clear(self) -> None:
        """Clear recorded events."""
        with self._lock:
            self._events.clear()
    
    def add_callback(self, callback: Callable[[TraceEvent], None]) -> None:
        """Add a callback for each trace event."""
        self._callbacks.append(callback)
    
    def remove_callback(self, callback: Callable[[TraceEvent], None]) -> bool:
        """Remove a trace callback."""
        try:
            self._callbacks.remove(callback)
            return True
        except ValueError:
            return False
    
    def record(
        self,
        event_type: str,
        details: dict[str, Any],
        capture_stack: bool = False,
    ) -> None:
        """Record a trace event."""
        if not self._enabled:
            return
        
        stack = None
        if capture_stack:
            stack = traceback.format_stack()[:-1]
        
        event = TraceEvent(
            timestamp=time.monotonic(),
            event_type=event_type,
            details=details,
            thread_id=threading.get_ident(),
            stack=stack,
        )
        
        with self._lock:
            self._events.append(event)
            if len(self._events) > self._max_events:
                self._events.pop(0)
        
        # Call callbacks
        for callback in self._callbacks:
            try:
                callback(event)
            except Exception:
                pass  # Ignore callback errors
    
    # Convenience methods for common events
    def trace_callback(self, handle: Handle, phase: str) -> None:
        self.record("callback", {
            "handle_type": handle.__class__.__name__,
            "phase": phase,
            "callback": repr(handle.callback),
            "state": handle.state.name,
        })
    
    def trace_task(self, task: Task[Any], phase: str) -> None:
        self.record("task", {
            "task_name": task.name,
            "phase": phase,
            "state": task.state.name,
            "coro": repr(task.coro),
        })
    
    def trace_timer(self, timer: TimerHandle, phase: str) -> None:
        self.record("timer", {
            "when": timer.when,
            "periodic": timer.periodic,
            "interval": timer.interval,
            "phase": phase,
        })
    
    def trace_io(self, handle: IOHandle, events: int, phase: str) -> None:
        self.record("io", {
            "fd": handle.fd,
            "events": events,
            "phase": phase,
        })
    
    def trace_schedule(self, task: Task[Any]) -> None:
        self.record("schedule", {
            "task_name": task.name,
            "task_id": id(task),
        })
    
    def trace_loop_iteration(self, duration: float) -> None:
        self.record("loop_iteration", {
            "duration": duration,
        })
    
    def get_stats(self) -> dict[str, Any]:
        """Get tracing statistics."""
        with self._lock:
            by_type: dict[str, int] = {}
            for event in self._events:
                by_type[event.event_type] = by_type.get(event.event_type, 0) + 1
            
            return {
                "total_events": len(self._events),
                "by_type": by_type,
                "max_events": self._max_events,
                "enabled": self._enabled,
            }
    
    def export_json(self) -> list[dict[str, Any]]:
        """Export events as JSON-serializable list."""
        with self._lock:
            return [
                {
                    "timestamp": e.timestamp,
                    "event_type": e.event_type,
                    "details": e.details,
                    "thread_id": e.thread_id,
                    "stack": e.stack,
                }
                for e in self._events
            ]


class TraceContext:
    """Context manager for temporary tracing."""
    
    def __init__(self, tracer: EventLoopTracer, enabled: bool = True) -> None:
        self._tracer = tracer
        self._enabled = enabled
        _previous = tracer.enabled
    
    def __enter__(self) -> EventLoopTracer:
        if self._enabled:
            self._tracer.enable()
        return self._tracer
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._enabled:
            self._tracer.disable()


def trace_loop(loop: EventLoop, max_events: int = 10000) -> EventLoopTracer:
    """Create and enable a tracer for a loop.
    
    The tracer is attached to the loop, so it records every task step and
    callback execution automatically.
    """
    tracer = EventLoopTracer(loop, max_events)
    tracer.enable()
    loop.set_tracer(tracer)
    return tracer


@contextmanager
def tracing(loop: EventLoop, max_events: int = 10000) -> EventLoopTracer:
    """Context manager for temporary loop tracing."""
    tracer = trace_loop(loop, max_events)
    try:
        yield tracer
    finally:
        tracer.disable()