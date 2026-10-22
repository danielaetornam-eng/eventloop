"""
Deadline tracking for precise timeout management.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional

from ..core.types import TimeoutError

__all__ = [
    "Deadline",
    "DeadlineTracker",
    "TimeoutContext",
    "TimeoutError",
    "timeout",
    "move_on_after",
    "fail_after",
]


@dataclass(frozen=True, slots=True)
class Deadline:
    """Immutable deadline representation."""
    
    timestamp: float
    
    @property
    def expired(self) -> bool:
        return time.monotonic() >= self.timestamp
    
    @property
    def remaining(self) -> float:
        return max(0.0, self.timestamp - time.monotonic())
    
    def __lt__(self, other: "Deadline") -> bool:
        return self.timestamp < other.timestamp
    
    def __le__(self, other: "Deadline") -> bool:
        return self.timestamp <= other.timestamp
    
    def __gt__(self, other: "Deadline") -> bool:
        return self.timestamp > other.timestamp
    
    def __ge__(self, other: "Deadline") -> bool:
        return self.timestamp >= other.timestamp


class DeadlineTracker:
    """
    Tracks deadlines for operations with automatic cleanup.
    
    Used by the event loop to manage timeouts for I/O operations,
    task execution, and other timed operations.
    """
    
    __slots__ = ("_deadlines", "_heap", "_counter")
    
    def __init__(self) -> None:
        self._deadlines: dict[int, Deadline] = {}
        self._heap: list[tuple[float, int]] = []  # (timestamp, id)
        self._counter = 0
    
    def add(self, timeout: float) -> int:
        """Add a new deadline. Returns deadline ID."""
        self._counter += 1
        deadline = Deadline(time.monotonic() + timeout)
        self._deadlines[self._counter] = deadline
        import heapq
        heapq.heappush(self._heap, (deadline.timestamp, self._counter))
        return self._counter
    
    def remove(self, deadline_id: int) -> bool:
        """Remove a deadline. Returns True if found."""
        if deadline_id in self._deadlines:
            del self._deadlines[deadline_id]
            return True
        return False
    
    def get(self, deadline_id: int) -> Deadline | None:
        """Get a deadline by ID."""
        return self._deadlines.get(deadline_id)
    
    def get_next(self) -> Deadline | None:
        """Get the nearest deadline."""
        import heapq
        while self._heap:
            timestamp, deadline_id = self._heap[0]
            if deadline_id in self._deadlines:
                return self._deadlines[deadline_id]
            heapq.heappop(self._heap)
        return None
    
    def get_expired(self) -> list[Deadline]:
        """Get all expired deadlines."""
        now = time.monotonic()
        expired = []
        import heapq
        while self._heap and self._heap[0][0] <= now:
            timestamp, deadline_id = heapq.heappop(self._heap)
            if deadline_id in self._deadlines:
                expired.append(self._deadlines.pop(deadline_id))
        return expired
    
    def clear(self) -> None:
        """Clear all deadlines."""
        self._deadlines.clear()
        self._heap.clear()
    
    def __len__(self) -> int:
        return len(self._deadlines)
    
    def __bool__(self) -> bool:
        return bool(self._deadlines)


@dataclass(slots=True)
class TimeoutContext:
    """Context manager for timeout handling."""
    
    deadline: Deadline
    _tracker: DeadlineTracker
    _deadline_id: int
    _cancelled: bool = False
    
    def __enter__(self) -> "TimeoutContext":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._tracker.remove(self._deadline_id)
        return False
    
    @property
    def expired(self) -> bool:
        return self.deadline.expired
    
    @property
    def remaining(self) -> float:
        return self.deadline.remaining
    
    def cancel(self) -> None:
        """Cancel this timeout."""
        if not self._cancelled:
            self._tracker.remove(self._deadline_id)
            self._cancelled = True


class TimeoutContextManager:
    """Async context manager that enforces a deadline.
    
    When the deadline expires the task hosting the context is cancelled,
    which surfaces a :class:`CancelledError` at its next await point.
    
    The three flavours differ in what happens on expiry:
    
    - ``timeout``      - the cancellation propagates to the caller.
    - ``move_on_after`` - the cancellation is swallowed and execution simply
      continues after the block.
    - ``fail_after``   - a :class:`TimeoutError` is raised instead.
    """
    
    def __init__(
        self,
        tracker: DeadlineTracker,
        timeout: float,
        *,
        raise_on_timeout: bool = False,
        suppress_on_timeout: bool = False,
    ) -> None:
        self._tracker = tracker
        self._timeout = timeout
        self._context: TimeoutContext | None = None
        self._handle = None
        self._task = None
        self._timed_out = False
        self._raise_on_timeout = raise_on_timeout
        self._suppress_on_timeout = suppress_on_timeout
    
    async def __aenter__(self) -> TimeoutContext:
        from ..core.context import current_loop
        from ..core.task import current_task
        
        loop = current_loop()
        if loop is None:
            raise RuntimeError("No current event loop")
        
        deadline_id = self._tracker.add(self._timeout)
        deadline = self._tracker.get(deadline_id)
        if deadline is None:
            raise RuntimeError("Failed to create deadline")
        self._context = TimeoutContext(deadline, self._tracker, deadline_id)
        self._task = current_task()
        self._timed_out = False
        
        # Arm a timer that cancels the hosting task when the deadline hits.
        def _on_timeout() -> None:
            self._timed_out = True
            if self._task is not None and not self._task.done():
                self._task.cancel(f"Timeout after {self._timeout}s")
        
        self._handle = loop.call_later(self._timeout, _on_timeout)
        return self._context
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None
        if self._context:
            self._context.__exit__(exc_type, exc_val, exc_tb)
        
        if self._timed_out and exc_type is not None and _is_cancellation(exc_type):
            if self._raise_on_timeout:
                raise TimeoutError(f"Operation timed out after {self._timeout}s") from None
            if self._suppress_on_timeout:
                return True
        return False


def _is_cancellation(exc_type: type[BaseException]) -> bool:
    """Check whether an exception type represents a cancellation."""
    from ..core.types import CancelledError as GodLoopCancelledError
    
    if issubclass(exc_type, GodLoopCancelledError):
        return True
    # Also treat the stdlib cancellation as one.
    import asyncio
    return issubclass(exc_type, asyncio.CancelledError)


def timeout(seconds: float) -> TimeoutContextManager:
    """Create a timeout context manager.
    
    On expiry the hosting task is cancelled and the cancellation propagates.
    """
    from ..core.context import current_loop
    loop = current_loop()
    if loop is None:
        raise RuntimeError("No current event loop")
    return TimeoutContextManager(loop._deadline_tracker, seconds)


def move_on_after(seconds: float) -> TimeoutContextManager:
    """Create a timeout that moves on when it expires.
    
    On expiry the hosting task is cancelled but the cancellation is swallowed,
    so execution continues after the block.
    """
    from ..core.context import current_loop
    loop = current_loop()
    if loop is None:
        raise RuntimeError("No current event loop")
    return TimeoutContextManager(
        loop._deadline_tracker, seconds, suppress_on_timeout=True
    )


def fail_after(seconds: float) -> TimeoutContextManager:
    """Create a timeout that raises on expiry (fail after).
    
    On expiry a :class:`TimeoutError` is raised in place of the cancellation.
    """
    from ..core.context import current_loop
    loop = current_loop()
    if loop is None:
        raise RuntimeError("No current event loop")
    return TimeoutContextManager(
        loop._deadline_tracker, seconds, raise_on_timeout=True
    )