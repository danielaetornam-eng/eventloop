"""
Event loop monitor for runtime observability.
"""

from __future__ import annotations

import time
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from contextlib import contextmanager

from ..core.loop import EventLoop
from ..core.task import Task

__all__ = [
    "EventLoopMonitor",
    "MonitorStats",
    "monitor_loop",
    "MonitorContext",
]


@dataclass(frozen=True, slots=True)
class MonitorStats:
    """Snapshot of event loop statistics."""
    timestamp: float
    pending_tasks: int
    running_tasks: int
    total_tasks: int
    ready_callbacks: int
    scheduled_timers: int
    io_handles: int
    loop_iteration_time: float
    loop_iteration_count: int
    cpu_percent: float
    memory_mb: float


class EventLoopMonitor:
    """
    Real-time event loop monitor.
    
    Provides continuous monitoring of event loop health and performance.
    """
    
    __slots__ = (
        "_loop",
        "_running",
        "_thread",
        "_interval",
        "_stats_history",
        "_max_history",
        "_callbacks",
        "_lock",
        "_iteration_count",
        "_last_iteration_time",
        "_start_time",
    )
    
    def __init__(
        self,
        loop: EventLoop,
        interval: float = 1.0,
        max_history: int = 3600,  # 1 hour at 1s interval
    ) -> None:
        self._loop = loop
        self._running = False
        self._thread: threading.Thread | None = None
        self._interval = interval
        self._stats_history: deque[MonitorStats] = deque(maxlen=max_history)
        self._max_history = max_history
        self._callbacks: list[Callable[[MonitorStats], None]] = []
        self._lock = threading.Lock()
        self._iteration_count = 0
        self._last_iteration_time = 0.0
        self._start_time = time.monotonic()
    
    @property
    def running(self) -> bool:
        return self._running
    
    @property
    def interval(self) -> float:
        return self._interval
    
    def start(self) -> None:
        """Start monitoring."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="GodLoop-Monitor",
        )
        self._thread.start()
    
    def stop(self) -> None:
        """Stop monitoring."""
        if not self._running:
            return
        
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None
    
    def _monitor_loop(self) -> None:
        while self._running:
            time.sleep(self._interval)
            if not self._running:
                break
            self._collect_stats()
    
    def _collect_stats(self) -> None:
        """Collect current statistics."""
        now = time.monotonic()
        
        # Get loop stats (thread-safe access)
        pending = len(self._loop._pending_tasks)
        running = len(self._loop._running_tasks)
        total = len(self._loop._all_tasks)
        ready = len(self._loop._ready_callbacks)
        timers = len(self._loop._timer_wheel)
        io_handles = len(self._loop._io_handles)
        
        # Calculate iteration time
        iteration_time = now - self._last_iteration_time if self._last_iteration_time > 0 else 0
        self._last_iteration_time = now
        self._iteration_count += 1
        
        # CPU and memory (simplified)
        cpu_percent = 0.0
        memory_mb = 0.0
        
        try:
            import psutil
            process = psutil.Process()
            cpu_percent = process.cpu_percent()
            memory_mb = process.memory_info().rss / (1024 * 1024)
        except ImportError:
            pass
        
        stats = MonitorStats(
            timestamp=now,
            pending_tasks=pending,
            running_tasks=running,
            total_tasks=total,
            ready_callbacks=ready,
            scheduled_timers=timers,
            io_handles=io_handles,
            loop_iteration_time=iteration_time,
            loop_iteration_count=self._iteration_count,
            cpu_percent=cpu_percent,
            memory_mb=memory_mb,
        )
        
        with self._lock:
            self._stats_history.append(stats)
        
        # Call callbacks
        for callback in self._callbacks:
            try:
                callback(stats)
            except Exception:
                pass
    
    def add_callback(self, callback: Callable[[MonitorStats], None]) -> None:
        """Add a callback for each stats snapshot."""
        self._callbacks.append(callback)
    
    def remove_callback(self, callback: Callable[[MonitorStats], None]) -> bool:
        """Remove a monitor callback."""
        try:
            self._callbacks.remove(callback)
            return True
        except ValueError:
            return False
    
    def get_latest(self) -> MonitorStats | None:
        """Get the latest stats snapshot."""
        with self._lock:
            if self._stats_history:
                return self._stats_history[-1]
        return None
    
    def get_history(self, duration: float = 60.0) -> list[MonitorStats]:
        """Get stats history for the last duration seconds."""
        cutoff = time.monotonic() - duration
        with self._lock:
            return [s for s in self._stats_history if s.timestamp >= cutoff]
    
    def get_summary(self) -> dict[str, Any]:
        """Get summary statistics."""
        with self._lock:
            history = list(self._stats_history)
        
        if not history:
            return {}
        
        latest = history[-1]
        elapsed = latest.timestamp - self._start_time
        
        return {
            "monitoring_duration": elapsed,
            "samples_collected": len(history),
            "current": {
                "pending_tasks": latest.pending_tasks,
                "running_tasks": latest.running_tasks,
                "total_tasks": latest.total_tasks,
                "ready_callbacks": latest.ready_callbacks,
                "scheduled_timers": latest.scheduled_timers,
                "io_handles": latest.io_handles,
                "cpu_percent": latest.cpu_percent,
                "memory_mb": latest.memory_mb,
            },
            "averages": {
                "pending_tasks": sum(s.pending_tasks for s in history) / len(history),
                "running_tasks": sum(s.running_tasks for s in history) / len(history),
                "loop_iteration_time": sum(s.loop_iteration_time for s in history) / len(history),
            },
            "peaks": {
                "pending_tasks": max(s.pending_tasks for s in history),
                "running_tasks": max(s.running_tasks for s in history),
                "total_tasks": max(s.total_tasks for s in history),
                "cpu_percent": max(s.cpu_percent for s in history),
                "memory_mb": max(s.memory_mb for s in history),
            },
        }
    
    def print_status(self) -> None:
        """Print current status."""
        stats = self.get_latest()
        if stats:
            print(f"Event Loop Monitor [{stats.timestamp:.1f}]")
            print(f"  Tasks: {stats.pending_tasks} pending, {stats.running_tasks} running, {stats.total_tasks} total")
            print(f"  Callbacks: {stats.ready_callbacks} ready, {stats.scheduled_timers} timers, {stats.io_handles} I/O")
            print(f"  Iteration: {stats.loop_iteration_time*1000:.2f}ms (count: {stats.loop_iteration_count})")
            print(f"  Resources: CPU {stats.cpu_percent:.1f}%, Memory {stats.memory_mb:.1f}MB")


class MonitorContext:
    """Context manager for temporary monitoring."""
    
    def __init__(self, monitor: EventLoopMonitor) -> None:
        self._monitor = monitor
    
    def __enter__(self) -> EventLoopMonitor:
        self._monitor.start()
        return self._monitor
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._monitor.stop()


def monitor_loop(loop: EventLoop, interval: float = 1.0) -> EventLoopMonitor:
    """Create and start a monitor for a loop."""
    monitor = EventLoopMonitor(loop, interval)
    monitor.start()
    return monitor


@contextmanager
def monitoring(loop: EventLoop, interval: float = 1.0) -> EventLoopMonitor:
    """Context manager for temporary loop monitoring."""
    monitor = monitor_loop(loop, interval)
    try:
        yield monitor
    finally:
        monitor.stop()