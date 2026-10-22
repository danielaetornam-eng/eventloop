"""
Timer wheel implementation for efficient timer management.

The wheels are based on the classic timer wheel algorithm (Varghese & Lauck,
1987) with a hierarchical extension for long timers.

Each level keeps an *absolute*, grid-aligned base time instead of a free
running tick counter. That keeps the level in lock-step with the wall clock:
the base time is always a multiple of the level's tick duration, so the
number of slots to advance is exact and the counter can never drift.

Timers are filed with a ceiling, so a timer is never placed in a slot that
is reached before its deadline.
"""

from __future__ import annotations

import heapq
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional

from ..core.handle import TimerHandle

__all__ = [
    "TimerWheel",
    "HierarchicalTimerWheel",
    "TimerWheelLevel",
]


@dataclass(slots=True)
class TimerWheelLevel:
    """A single level in a hierarchical timer wheel.
    
    Slots are addressed by *absolute* time index (``floor(when / tick)``) so
    that the level can never drift out of step with the wall clock. A timer is
    filed under the ceiling of its deadline, so it is never visited before
    it is actually due.
    """
    
    size: int
    tick_duration: float
    slots: list[deque[TimerHandle]] = field(default_factory=list)
    base_time: float = 0.0
    
    def __post_init__(self) -> None:
        self.slots = [deque() for _ in range(self.size)]
        self.base_time = time.monotonic()
    
    def _index(self, when: float) -> int:
        """The absolute slot index for a timestamp."""
        return int(math.floor(when / self.tick_duration))
    
    def add_timer(self, handle: TimerHandle, deadline: float) -> int:
        """Add a timer to this level. Returns the slot index."""
        # Round *up* to the next tick: a timer must never be filed in a slot
        # that is visited before its deadline.
        index = int(math.ceil(deadline / self.tick_duration))
        # If that slot has already been visited, file the timer in the next
        # slot to be visited so it is picked up on the next advance.
        current = self._index(self.base_time)
        if index <= current:
            index = current + 1
        slot = index % self.size
        self.slots[slot].append(handle)
        return slot
    
    def advance(self, now: float) -> list[TimerHandle]:
        """Advance the level to the given time, returning expired timers."""
        start = self._index(self.base_time) + 1
        end = self._index(now)
        if end < start:
            self.base_time = now
            return []
        
        # A full rotation visits every slot, so capping the range cannot
        # lose a timer - at worst it is reported a little late.
        if end - start + 1 > self.size:
            start = end - self.size + 1
        
        expired: list[TimerHandle] = []
        for index in range(start, end + 1):
            slot = self.slots[index % self.size]
            while slot:
                expired.append(slot.popleft())
        
        self.base_time = now
        return expired
    
    def take_index(self, index: int) -> list[TimerHandle]:
        """Take and return every timer filed in the given absolute index."""
        slot = self.slots[index % self.size]
        taken = list(slot)
        slot.clear()
        return taken
    
    def next_index(self) -> int:
        """The absolute index of the slot that will be visited next."""
        return self._index(self.base_time) + 1
    
    def remove_timer(self, handle: TimerHandle) -> bool:
        """Remove a specific timer from this level."""
        for slot in self.slots:
            try:
                slot.remove(handle)
                return True
            except ValueError:
                continue
        return False


class TimerWheel:
    """
    A single-level timer wheel for managing timeouts.
    
    Based on the classic timer wheel algorithm (Varghese & Lauck, 1987)
    with O(1) timer insertion and O(1) amortized timer expiration.
    """
    
    def __init__(
        self,
        tick_duration: float = 0.001,  # 1ms
        size: int = 512,
        max_interval: float = 86400.0,  # 24 hours
    ) -> None:
        self._tick_duration = tick_duration
        self._size = size
        self._max_interval = max_interval
        self._wheel = TimerWheelLevel(size, tick_duration)
        self._pending: list[tuple[float, int, TimerHandle]] = []
        self._counter = 0
    
    @property
    def tick_duration(self) -> float:
        return self._tick_duration
    
    @property
    def size(self) -> int:
        return self._size
    
    @property
    def current_time(self) -> float:
        return self._wheel.base_time
    
    def schedule(self, handle: TimerHandle) -> None:
        """Schedule a timer handle."""
        now = time.monotonic()
        when = handle.when
        
        if when <= now:
            # Expired immediately
            handle._run()
            return
        
        if when - now <= self._max_interval:
            self._wheel.add_timer(handle, when)
        else:
            # Too far in the future, use the heap.
            self._counter += 1
            heapq.heappush(self._pending, (when, self._counter, handle))
    
    def reschedule(self, handle: TimerHandle) -> None:
        """Reschedule an existing timer."""
        self.cancel(handle)
        self.schedule(handle)
    
    def cancel(self, handle: TimerHandle) -> bool:
        """Cancel a timer."""
        if self._wheel.remove_timer(handle):
            return True
        
        for i, (when, _, pending) in enumerate(self._pending):
            if pending is handle:
                del self._pending[i]
                heapq.heapify(self._pending)
                return True
        return False
    
    def advance(self, now: float | None = None) -> list[TimerHandle]:
        """Advance the wheel to the given time, returning expired timers."""
        if now is None:
            now = time.monotonic()
        
        candidates = self._wheel.advance(now)
        
        # Check the pending heap.
        while self._pending and self._pending[0][0] <= now:
            candidates.append(heapq.heappop(self._pending)[2])
        
        # A timer must never fire before its deadline. Slot arithmetic is
        # approximate (a timer is filed under a tick, not at an exact time),
        # so anything that is not actually due yet is put back.
        expired = []
        for handle in candidates:
            if handle.cancelled:
                continue
            if handle.when <= now:
                expired.append(handle)
            else:
                self.schedule(handle)
        
        return expired
    
    def next_expiration(self) -> float | None:
        """Get the deadline of the soonest pending timer."""
        best: float | None = None
        
        for slot in self._wheel.slots:
            if not slot:
                continue
            for handle in slot:
                if handle.cancelled:
                    continue
                if best is None or handle.when < best:
                    best = handle.when
        
        if self._pending:
            heap_when = self._pending[0][0]
            if best is None or heap_when < best:
                best = heap_when
        
        return best
    
    def __len__(self) -> int:
        return sum(len(slot) for slot in self._wheel.slots) + len(self._pending)
    
    def __repr__(self) -> str:
        return f"<TimerWheel size={self._size} tick={self._tick_duration*1000:.1f}ms timers={len(self)}>"


class HierarchicalTimerWheel:
    """
    Hierarchical timer wheel with multiple levels for efficient handling
    of both short and long-term timers.
    
    Level 0 has the finest resolution; each subsequent level covers a range
    that is ``size`` times larger than the level below it.
    """
    
    def __init__(
        self,
        tick_duration: float = 0.001,  # 1ms
        wheel_size: int = 512,
        num_levels: int = 4,
        max_interval: float = 86400.0,  # 24 hours
    ) -> None:
        self._tick_duration = tick_duration
        self._wheel_size = wheel_size
        self._num_levels = num_levels
        self._max_interval = max_interval
        
        self._levels: list[TimerWheelLevel] = []
        self._pending: list[tuple[float, int, TimerHandle]] = []
        self._counter = 0
        
        # Build the hierarchy: each level is `wheel_size` times coarser.
        level_tick = tick_duration
        for _ in range(num_levels):
            self._levels.append(TimerWheelLevel(wheel_size, level_tick))
            level_tick *= wheel_size
    
    @property
    def current_time(self) -> float:
        return self._levels[0].base_time
    
    def schedule(self, handle: TimerHandle) -> None:
        """Schedule a timer handle at the appropriate level."""
        now = time.monotonic()
        when = handle.when
        
        if when <= now:
            handle._run()
            return
        
        delta = when - now
        
        # Find the appropriate level.
        for level in self._levels:
            max_level_time = level.tick_duration * level.size
            if delta <= max_level_time:
                level.add_timer(handle, when)
                return
        
        # Beyond all levels, use the heap.
        self._counter += 1
        heapq.heappush(self._pending, (when, self._counter, handle))
    
    def reschedule(self, handle: TimerHandle) -> None:
        """Reschedule an existing timer."""
        self.cancel(handle)
        self.schedule(handle)
    
    def cancel(self, handle: TimerHandle) -> bool:
        """Cancel a timer from any level."""
        for level in self._levels:
            if level.remove_timer(handle):
                return True
        
        for i, (when, _, pending) in enumerate(self._pending):
            if pending is handle:
                del self._pending[i]
                heapq.heapify(self._pending)
                return True
        return False
    
    def advance(self, now: float | None = None) -> list[TimerHandle]:
        """Advance all levels, cascading expired timers down."""
        if now is None:
            now = time.monotonic()
        
        candidates: list[TimerHandle] = []
        
        # Walk the levels from coarse to fine.
        for i in range(len(self._levels) - 1, -1, -1):
            level = self._levels[i]
            level_expired = level.advance(now)
            
            if i == 0:
                # The finest level holds the timers that are actually due.
                candidates.extend(level_expired)
            else:
                lower = self._levels[i - 1]
                
                # Cascade the timers of the *next* slot as well: they are
                # close enough that the finer level below can take them over
                # with a much better resolution. Without this a timer would
                # not be seen until its whole slot had elapsed.
                for handle in level.take_index(level.next_index()):
                    if not handle.cancelled:
                        lower.add_timer(handle, handle.when)
                
                # Cascade the timers whose slot has just elapsed.
                for handle in level_expired:
                    if not handle.cancelled:
                        lower.add_timer(handle, handle.when)
        
        # Check the pending heap.
        while self._pending and self._pending[0][0] <= now:
            candidates.append(heapq.heappop(self._pending)[2])
        
        # A timer must never fire before its deadline. Slot arithmetic is
        # approximate (a timer is filed under a tick, not at an exact time),
        # so anything that is not actually due yet is put back.
        expired = []
        for handle in candidates:
            if handle.cancelled:
                continue
            if handle.when <= now:
                expired.append(handle)
            else:
                self.schedule(handle)
        
        return expired
    
    def next_expiration(self) -> float | None:
        """Get the deadline of the soonest pending timer."""
        best: float | None = None
        
        for level in self._levels:
            for slot in level.slots:
                if not slot:
                    continue
                for handle in slot:
                    if handle.cancelled:
                        continue
                    if best is None or handle.when < best:
                        best = handle.when
            if best is not None:
                break
        
        if self._pending:
            heap_when = self._pending[0][0]
            if best is None or heap_when < best:
                best = heap_when
        
        return best
    
    def __len__(self) -> int:
        total = sum(len(slot) for level in self._levels for slot in level.slots)
        return total + len(self._pending)
    
    def __repr__(self) -> str:
        return f"<HierarchicalTimerWheel levels={self._num_levels} timers={len(self)}>"
