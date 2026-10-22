"""
Synchronization primitives for GodLoop.
"""

from __future__ import annotations

import collections
import threading
from collections.abc import Callable, Awaitable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, Optional

from ..core.types import GodLoopError
from ..core.future import Future
from ..core.task import current_task
from ..core.loop import EventLoop
from ..core.context import current_loop

__all__ = [
    "Lock",
    "Event",
    "Condition",
    "Semaphore",
    "BoundedSemaphore",
    "Barrier",
    "BrokenBarrierError",
]

T = TypeVar("T")


def _resolve_loop(loop: EventLoop | None) -> EventLoop:
    """Return the given loop, or the currently running one.
    
    Synchronisation primitives are often created before the event loop is
    running (at module level, for example), so the loop is resolved lazily
    the first time the primitive is actually used.
    """
    if loop is not None:
        return loop
    loop = current_loop()
    if loop is None:
        raise RuntimeError(
            "No running event loop: a synchronization primitive cannot be "
            "used outside of a running event loop"
        )
    return loop


class Lock(AbstractAsyncContextManager):
    """
    Async lock implementation.
    
    Provides mutual exclusion for async code.
    """
    
    __slots__ = ("_locked", "_waiters", "_loop")
    
    def __init__(self) -> None:
        self._locked = False
        self._waiters: collections.deque[Future[None]] = collections.deque()
        self._loop: EventLoop | None = None
    
    def _get_loop(self) -> EventLoop:
        self._loop = _resolve_loop(self._loop)
        return self._loop
    
    @property
    def locked(self) -> bool:
        return self._locked
    
    async def acquire(self) -> bool:
        """Acquire the lock."""
        if not self._locked and not any(not w.done() for w in self._waiters):
            self._locked = True
            return True
        
        # Create future to wait on
        future: Future[None] = self._get_loop().create_future()
        self._waiters.append(future)
        
        try:
            try:
                await future
            finally:
                # Remove ourselves from the queue, whatever happens.
                try:
                    self._waiters.remove(future)
                except ValueError:
                    pass
        except BaseException:
            # We were cancelled while waiting. If the lock was handed to us
            # we must pass it on, otherwise it would stay locked forever.
            if not self._locked:
                self._wake_up_first()
            raise
        
        self._locked = True
        return True
    
    def release(self) -> None:
        """Release the lock."""
        if not self._locked:
            raise RuntimeError("Lock not acquired")
        
        self._locked = False
        self._wake_up_first()
    
    def _wake_up_first(self) -> None:
        """Wake the first waiter that is still pending."""
        for future in self._waiters:
            if not future.done():
                future.set_result(None)
                return
    
    async def __aenter__(self) -> None:
        await self.acquire()
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()
    
    def __repr__(self) -> str:
        return f"<Lock locked={self._locked} waiters={len(self._waiters)}>"


class Event:
    """
    Async event implementation.
    
    Allows one task to signal an event and many tasks to wait for it.
    """
    
    __slots__ = ("_set", "_waiters", "_loop")
    
    def __init__(self) -> None:
        self._set = False
        self._waiters: collections.deque[Future[None]] = collections.deque()
        self._loop: EventLoop | None = None
    
    def _get_loop(self) -> EventLoop:
        self._loop = _resolve_loop(self._loop)
        return self._loop
    
    @property
    def is_set(self) -> bool:
        return self._set
    
    def set(self) -> None:
        """Set the event, waking all waiters."""
        self._set = True
        waiters = list(self._waiters)
        self._waiters.clear()
        for future in waiters:
            if not future.done():
                future.set_result(None)
    
    def clear(self) -> None:
        """Clear the event."""
        self._set = False
    
    async def wait(self) -> None:
        """Wait for the event to be set."""
        if self._set:
            return
        
        future: Future[None] = self._get_loop().create_future()
        self._waiters.append(future)
        
        try:
            await future
        except Exception:
            future.cancel()
            raise
    
    def __repr__(self) -> str:
        return f"<Event set={self._set} waiters={len(self._waiters)}>"


class Condition:
    """
    Async condition variable implementation.
    
    Allows tasks to wait for a condition to become true.
    """
    
    __slots__ = ("_lock", "_waiters", "_loop")
    
    def __init__(self, lock: Lock | None = None) -> None:
        self._lock = lock or Lock()
        self._waiters: collections.deque[Future[None]] = collections.deque()
        self._loop: EventLoop | None = None
    
    def _get_loop(self) -> EventLoop:
        self._loop = _resolve_loop(self._loop)
        return self._loop
    
    @property
    def lock(self) -> Lock:
        return self._lock
    
    async def acquire(self) -> None:
        await self._lock.acquire()
    
    def release(self) -> None:
        self._lock.release()
    
    async def wait(self) -> None:
        """Wait for the condition to be notified."""
        if not self._lock.locked:
            raise RuntimeError("Condition wait requires lock to be held")
        
        future: Future[None] = self._get_loop().create_future()
        self._waiters.append(future)
        
        # Release lock while waiting
        self._lock.release()
        
        try:
            try:
                await future
            finally:
                try:
                    self._waiters.remove(future)
                except ValueError:
                    pass
        except Exception:
            future.cancel()
            raise
        finally:
            # Re-acquire lock
            await self._lock.acquire()
    
    def notify(self, n: int = 1) -> None:
        """Wake up n waiters."""
        woken = 0
        while self._waiters and woken < n:
            future = self._waiters.popleft()
            if not future.done():
                future.set_result(None)
                woken += 1
    
    def notify_all(self) -> None:
        """Wake up all waiters."""
        while self._waiters:
            future = self._waiters.popleft()
            if not future.done():
                future.set_result(None)
    
    async def __aenter__(self) -> "Condition":
        await self.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()
    
    def __repr__(self) -> str:
        return f"<Condition lock={self._lock} waiters={len(self._waiters)}>"


class Semaphore:
    """
    Async semaphore implementation.
    
    Controls access to a finite number of resources.
    """
    
    __slots__ = ("_value", "_waiters", "_loop")
    
    def __init__(self, value: int = 1) -> None:
        if value < 0:
            raise ValueError("Semaphore value must be >= 0")
        self._value = value
        self._waiters: collections.deque[Future[None]] = collections.deque()
        self._loop: EventLoop | None = None
    
    def _get_loop(self) -> EventLoop:
        self._loop = _resolve_loop(self._loop)
        return self._loop
    
    @property
    def value(self) -> int:
        return self._value
    
    async def acquire(self) -> bool:
        """Acquire a semaphore slot."""
        if self._value > 0:
            self._value -= 1
            return True
        
        future: Future[None] = self._get_loop().create_future()
        self._waiters.append(future)
        
        try:
            try:
                await future
            finally:
                try:
                    self._waiters.remove(future)
                except ValueError:
                    pass
        except BaseException:
            # We were cancelled while waiting: the slot we were promised
            # has to be handed to somebody else.
            future.cancel()
            if self._value > 0:
                self._wake_up_next()
            raise
        
        self._value -= 1
        return True
    
    def release(self) -> None:
        """Release a semaphore slot."""
        self._value += 1
        self._wake_up_next()
    
    def _wake_up_next(self) -> None:
        """Wake the next waiter that is still pending."""
        while self._waiters:
            future = self._waiters.popleft()
            if not future.done():
                future.set_result(None)
                return
    
    async def __aenter__(self) -> None:
        await self.acquire()
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()
    
    def __repr__(self) -> str:
        return f"<Semaphore value={self._value} waiters={len(self._waiters)}>"


class BoundedSemaphore(Semaphore):
    """
    Bounded semaphore that raises if released too many times.
    """
    
    __slots__ = ("_initial_value",)
    
    def __init__(self, value: int = 1) -> None:
        super().__init__(value)
        self._initial_value = value
    
    def release(self) -> None:
        if self._value >= self._initial_value:
            raise ValueError("Semaphore released too many times")
        super().release()
    
    def __repr__(self) -> str:
        return f"<BoundedSemaphore value={self._value} max={self._initial_value} waiters={len(self._waiters)}>"


class BrokenBarrierError(GodLoopError):
    """Raised when a barrier is broken."""
    pass


class Barrier:
    """
    Async barrier implementation.
    
    Allows a fixed number of tasks to wait for each other.
    """
    
    __slots__ = (
        "_parties",
        "_count",
        "_waiters",
        "_broken",
        "_generation",
        "_loop",
    )
    
    def __init__(self, parties: int) -> None:
        if parties <= 0:
            raise ValueError("Parties must be > 0")
        self._parties = parties
        self._count = parties
        self._waiters: collections.deque[Future[None]] = collections.deque()
        self._broken = False
        self._generation = 0
        self._loop: EventLoop | None = None
    
    def _get_loop(self) -> EventLoop:
        self._loop = _resolve_loop(self._loop)
        return self._loop
    
    @property
    def parties(self) -> int:
        return self._parties
    
    @property
    def n_waiting(self) -> int:
        return self._parties - self._count
    
    @property
    def broken(self) -> bool:
        return self._broken
    
    async def wait(self) -> int:
        """Wait for all parties to reach the barrier."""
        if self._broken:
            raise BrokenBarrierError("Barrier is broken")
        
        generation = self._generation
        self._count -= 1
        
        try:
            if self._count == 0:
                # Last party - release all waiters
                self._generation += 1
                self._count = self._parties
                for future in self._waiters:
                    if not future.done():
                        future.set_result(None)
                self._waiters.clear()
                return 0  # Leader gets index 0
            
            # Wait for release
            future: Future[None] = self._get_loop().create_future()
            self._waiters.append(future)
            
            try:
                await future
            except Exception:
                future.cancel()
                raise
            
            # Return index
            return self._parties - self._count
        finally:
            if self._broken:
                raise BrokenBarrierError("Barrier is broken")
    
    def abort(self) -> None:
        """Abort the barrier, waking all waiters with BrokenBarrierError."""
        self._broken = True
        for future in self._waiters:
            if not future.done():
                future.set_exception(BrokenBarrierError("Barrier aborted"))
        self._waiters.clear()
    
    def reset(self) -> None:
        """Reset the barrier to initial state."""
        self._broken = False
        self._count = self._parties
        self._generation += 1
        for future in self._waiters:
            if not future.done():
                future.set_exception(BrokenBarrierError("Barrier reset"))
        self._waiters.clear()
    
    def __repr__(self) -> str:
        return f"<Barrier parties={self._parties} waiting={self.n_waiting} broken={self._broken}>"