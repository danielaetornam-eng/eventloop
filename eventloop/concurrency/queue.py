"""
Async queue implementations for GodLoop.
"""

from __future__ import annotations

import heapq
from collections import deque
from typing import Any, Generic, TypeVar

from ..core.future import Future
from ..core.types import GodLoopError

__all__ = [
    "Queue",
    "PriorityQueue",
    "LifoQueue",
    "QueueEmpty",
    "QueueFull",
]

T = TypeVar("T")


class QueueEmpty(GodLoopError):
    """Raised when getting from an empty queue with ``get_nowait``."""
    pass


class QueueFull(GodLoopError):
    """Raised when putting into a full queue with ``put_nowait``."""
    pass


class Queue(Generic[T]):
    """
    An async queue with an optional maximum size.
    
    Producers await :meth:`put` when the queue is full, consumers await
    :meth:`get` when it is empty.
    """
    
    __slots__ = (
        "_maxsize",
        "_items",
        "_getters",
        "_putters",
        "_unfinished_tasks",
        "_loop",
        "_joiners",
    )
    
    def __init__(self, maxsize: int = 0) -> None:
        self._maxsize = maxsize
        self._items: deque[T] = deque()
        self._getters: deque[Future[None]] = deque()
        self._putters: deque[Future[None]] = deque()
        self._unfinished_tasks = 0
        self._loop = None
        self._joiners: list[Future[None]] = []
    
    @property
    def maxsize(self) -> int:
        return self._maxsize
    
    def qsize(self) -> int:
        """Number of items currently in the queue."""
        return len(self._items)
    
    def empty(self) -> bool:
        """True if the queue holds no items."""
        return not self._items
    
    def full(self) -> bool:
        """True if the queue cannot accept more items."""
        if self._maxsize <= 0:
            return False
        return len(self._items) >= self._maxsize
    
    def _get_loop(self) -> Any:
        if self._loop is None:
            from ..core.context import current_loop
            self._loop = current_loop()
        if self._loop is None:
            raise RuntimeError("No current event loop")
        return self._loop
    
    def put_nowait(self, item: T) -> None:
        """Put an item on the queue without blocking."""
        if self.full():
            raise QueueFull("Queue is full")
        self._put(item)
    
    def get_nowait(self) -> T:
        """Remove and return an item without blocking."""
        if not self._items:
            raise QueueEmpty("Queue is empty")
        return self._get()
    
    def _put(self, item: T) -> None:
        self._items.append(item)
        self._unfinished_tasks += 1
        # Wake up a waiting consumer.
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                getter.set_result(None)
                break
            # The getter was cancelled - drop it and try the next one.
    
    def _get(self) -> T:
        item = self._items.popleft()
        # Wake up a waiting producer.
        while self._putters:
            putter = self._putters.popleft()
            if not putter.done():
                putter.set_result(None)
                break
        return item
    
    async def put(self, item: T) -> None:
        """Put an item on the queue, waiting if it is full."""
        while self.full():
            loop = self._get_loop()
            waiter: Future[None] = loop.create_future()
            self._putters.append(waiter)
            try:
                await waiter
            except Exception:
                # Remove ourselves from the waiter list.
                try:
                    self._putters.remove(waiter)
                except ValueError:
                    pass
                raise
        self._put(item)
    
    async def get(self) -> T:
        """Remove and return an item, waiting if the queue is empty."""
        while not self._items:
            loop = self._get_loop()
            waiter: Future[None] = loop.create_future()
            self._getters.append(waiter)
            try:
                await waiter
            except Exception:
                try:
                    self._getters.remove(waiter)
                except ValueError:
                    pass
                raise
        return self._get()
    
    def task_done(self) -> None:
        """Mark a formerly enqueued task as complete."""
        if self._unfinished_tasks <= 0:
            raise ValueError("task_done() called too many times")
        self._unfinished_tasks -= 1
        if self._unfinished_tasks == 0:
            self._wake_joiners()
    
    def _wake_joiners(self) -> None:
        # ``join`` is implemented with a dedicated future per waiter.
        for waiter in list(self._joiners):
            if not waiter.done():
                waiter.set_result(None)
    
    async def join(self) -> None:
        """Wait until all items have been processed."""
        if self._unfinished_tasks == 0:
            return
        loop = self._get_loop()
        waiter: Future[None] = loop.create_future()
        self._joiners.append(waiter)
        try:
            await waiter
        finally:
            try:
                self._joiners.remove(waiter)
            except ValueError:
                pass
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} size={len(self._items)} maxsize={self._maxsize}>"


class PriorityQueue(Queue[T]):
    """A queue that retrieves items in priority order (lowest first)."""
    
    __slots__ = ("_heap",)
    
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._heap: list[tuple[Any, int, T]] = []
        self._counter = 0
    
    def _put(self, item: T) -> None:
        heapq.heappush(self._heap, (item, self._counter, item))
        self._counter += 1
        self._unfinished_tasks += 1
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                getter.set_result(None)
                break
    
    def _get(self) -> T:
        _, _, item = heapq.heappop(self._heap)
        while self._putters:
            putter = self._putters.popleft()
            if not putter.done():
                putter.set_result(None)
                break
        return item
    
    def qsize(self) -> int:
        return len(self._heap)
    
    def empty(self) -> bool:
        return not self._heap


class LifoQueue(Queue[T]):
    """A queue that retrieves the most recently added item first."""
    
    __slots__ = ()
    
    def _put(self, item: T) -> None:
        self._items.append(item)
        self._unfinished_tasks += 1
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                getter.set_result(None)
                break
    
    def _get(self) -> T:
        item = self._items.pop()
        while self._putters:
            putter = self._putters.popleft()
            if not putter.done():
                putter.set_result(None)
                break
        return item
