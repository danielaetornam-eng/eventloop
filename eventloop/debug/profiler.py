"""
Event loop profiler for performance analysis.
"""

from __future__ import annotations

import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional
from contextlib import contextmanager

from ..core.loop import EventLoop
from ..core.task import Task

__all__ = [
    "EventLoopProfiler",
    "ProfileData",
    "profile_loop",
    "ProfilerContext",
]


@dataclass(frozen=True, slots=True)
class ProfileData:
    """Profile data for a single entity."""
    name: str
    total_time: float
    call_count: int
    min_time: float
    max_time: float
    avg_time: float
    
    @property
    def per_call(self) -> float:
        return self.avg_time


class EventLoopProfiler:
    """
    Event loop profiler for measuring task and callback performance.
    
    Tracks execution times of tasks, callbacks, and I/O operations.
    """
    
    __slots__ = (
        "_loop",
        "_enabled",
        "_task_profiles",
        "_callback_profiles",
        "_io_profiles",
        "_lock",
        "_start_time",
    )
    
    def __init__(self, loop: EventLoop) -> None:
        self._loop = loop
        self._enabled = False
        self._task_profiles: dict[str, list[float]] = defaultdict(list)
        self._callback_profiles: dict[str, list[float]] = defaultdict(list)
        self._io_profiles: dict[int, list[float]] = defaultdict(list)
        self._lock = threading.Lock()
        self._start_time = time.monotonic()
    
    @property
    def enabled(self) -> bool:
        return self._enabled
    
    def enable(self) -> None:
        self._enabled = True
        self._start_time = time.monotonic()
    
    def disable(self) -> None:
        """Disable profiling and detach from the loop."""
        self._enabled = False
        if getattr(self._loop, "profiler", None) is self:
            self._loop.set_profiler(None)
    
    def reset(self) -> None:
        with self._lock:
            self._task_profiles.clear()
            self._callback_profiles.clear()
            self._io_profiles.clear()
            self._start_time = time.monotonic()
    
    def record_task(self, task_name: str, duration: float) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._task_profiles[task_name].append(duration)
    
    def record_callback(self, callback_name: str, duration: float) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._callback_profiles[callback_name].append(duration)
    
    def record_io(self, fd: int, duration: float) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._io_profiles[fd].append(duration)
    
    def get_task_stats(self, top_n: int = 20) -> list[ProfileData]:
        with self._lock:
            profiles = self._task_profiles.copy()
        
        results = []
        for name, times in profiles.items():
            if times:
                results.append(ProfileData(
                    name=name,
                    total_time=sum(times),
                    call_count=len(times),
                    min_time=min(times),
                    max_time=max(times),
                    avg_time=sum(times) / len(times),
                ))
        
        results.sort(key=lambda p: p.total_time, reverse=True)
        return results[:top_n]
    
    def get_callback_stats(self, top_n: int = 20) -> list[ProfileData]:
        with self._lock:
            profiles = self._callback_profiles.copy()
        
        results = []
        for name, times in profiles.items():
            if times:
                results.append(ProfileData(
                    name=name,
                    total_time=sum(times),
                    call_count=len(times),
                    min_time=min(times),
                    max_time=max(times),
                    avg_time=sum(times) / len(times),
                ))
        
        results.sort(key=lambda p: p.total_time, reverse=True)
        return results[:top_n]
    
    def get_io_stats(self, top_n: int = 20) -> list[ProfileData]:
        with self._lock:
            profiles = self._io_profiles.copy()
        
        results = []
        for fd, times in profiles.items():
            if times:
                results.append(ProfileData(
                    name=f"fd_{fd}",
                    total_time=sum(times),
                    call_count=len(times),
                    min_time=min(times),
                    max_time=max(times),
                    avg_time=sum(times) / len(times),
                ))
        
        results.sort(key=lambda p: p.total_time, reverse=True)
        return results[:top_n]
    
    def get_summary(self) -> dict[str, Any]:
        with self._lock:
            task_count = sum(len(t) for t in self._task_profiles.values())
            callback_count = sum(len(t) for t in self._callback_profiles.values())
            io_count = sum(len(t) for t in self._io_profiles.values())
            elapsed = time.monotonic() - self._start_time
        
        return {
            "elapsed_time": elapsed,
            "task_calls": task_count,
            "callback_calls": callback_count,
            "io_calls": io_count,
            "unique_tasks": len(self._task_profiles),
            "unique_callbacks": len(self._callback_profiles),
            "unique_fds": len(self._io_profiles),
        }
    
    def print_stats(self, top_n: int = 10) -> None:
        """Print profiling statistics."""
        summary = self.get_summary()
        print(f"Event Loop Profile Summary ({summary['elapsed_time']:.3f}s)")
        print(f"  Tasks: {summary['task_calls']} calls ({summary['unique_tasks']} unique)")
        print(f"  Callbacks: {summary['callback_calls']} calls ({summary['unique_callbacks']} unique)")
        print(f"  I/O: {summary['io_calls']} calls ({summary['unique_fds']} unique)")
        print()
        
        print("Top Tasks by Total Time:")
        for i, p in enumerate(self.get_task_stats(top_n), 1):
            print(f"  {i}. {p.name}: {p.total_time:.4f}s total, "
                  f"{p.call_count} calls, avg {p.avg_time*1000:.2f}ms")
        
        print("\nTop Callbacks by Total Time:")
        for i, p in enumerate(self.get_callback_stats(top_n), 1):
            print(f"  {i}. {p.name}: {p.total_time:.4f}s total, "
                  f"{p.call_count} calls, avg {p.avg_time*1000:.2f}ms")


class ProfilerContext:
    """Context manager for profiling a code block."""
    
    def __init__(self, profiler: EventLoopProfiler, name: str) -> None:
        self._profiler = profiler
        self._name = name
        self._start: float = 0
    
    def __enter__(self) -> "ProfilerContext":
        self._start = time.monotonic()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        duration = time.monotonic() - self._start
        self._profiler.record_callback(self._name, duration)


def profile_loop(loop: EventLoop) -> EventLoopProfiler:
    """Create and enable a profiler for a loop.
    
    The profiler is attached to the loop, so it measures every task step and
    callback execution automatically.
    """
    profiler = EventLoopProfiler(loop)
    profiler.enable()
    loop.set_profiler(profiler)
    return profiler


@contextmanager
def profiling(loop: EventLoop) -> EventLoopProfiler:
    """Context manager for temporary loop profiling."""
    profiler = profile_loop(loop)
    try:
        yield profiler
    finally:
        profiler.disable()