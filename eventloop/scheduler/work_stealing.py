"""
Work-stealing scheduler for multi-threaded event loop execution.

The scheduler hands task steps to a pool of worker threads.  Each worker owns
a double-ended queue: it pushes work on the front and pops it from the front
(LIFO, which maximises cache locality), while idle workers steal work from
the *back* of a victim's queue (FIFO, which keeps the victim's hot data in
cache and avoids contention).

Tasks are never stepped by two threads at the same time: :class:`Task`
guards each step with its own lock, so a task may migrate freely between the
event loop thread and any worker thread.
"""

from __future__ import annotations

import os
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from ..core.types import SchedulerConfig

if TYPE_CHECKING:
    from ..core.loop import EventLoop
    from ..core.task import Task

__all__ = [
    "WorkStealingScheduler",
    "SchedulerConfig",
    "WorkerThread",
    "TaskQueue",
    "StepRequest",
]


@dataclass(slots=True)
class StepRequest:
    """A single pending step of a task."""

    task: "Task[Any]"
    value: Any = None
    exc: BaseException | None = None


class TaskQueue:
    """Thread-local double-ended queue of step requests.
    
    Local access uses the front of the deque, stealing uses the back.
    """

    def __init__(self, max_size: int = 1024) -> None:
        self._queue: deque[StepRequest] = deque()
        self._cond = threading.Condition()
        self._max_size = max_size

    @property
    def max_size(self) -> int:
        return self._max_size

    def push(self, request: StepRequest) -> bool:
        """Push onto the front of the queue. Returns False if the queue is full."""
        with self._cond:
            if len(self._queue) >= self._max_size:
                return False
            self._queue.appendleft(request)
            self._cond.notify()
            return True

    def pop(self, timeout: float | None = None) -> StepRequest | None:
        """Pop from the front of the queue, waiting up to *timeout* seconds."""
        with self._cond:
            if not self._queue:
                if timeout is None:
                    self._cond.wait(0.05)
                else:
                    self._cond.wait(timeout)
            if not self._queue:
                return None
            return self._queue.popleft()

    def steal(self, count: int = 16) -> list[StepRequest]:
        """Steal up to *count* requests from the back of the queue."""
        stolen: list[StepRequest] = []
        with self._cond:
            while self._queue and len(stolen) < count:
                stolen.append(self._queue.pop())
        return stolen

    def wake_all(self) -> None:
        """Wake every thread waiting on this queue."""
        with self._cond:
            self._cond.notify_all()

    def __len__(self) -> int:
        with self._cond:
            return len(self._queue)

    def empty(self) -> bool:
        with self._cond:
            return not self._queue


class WorkerThread(threading.Thread):
    """Worker thread that executes task steps for the scheduler."""

    def __init__(
        self,
        scheduler: "WorkStealingScheduler",
        worker_id: int,
        queue: TaskQueue,
    ) -> None:
        super().__init__(daemon=True, name=f"GodLoop-Worker-{worker_id}")
        self._scheduler = scheduler
        self._worker_id = worker_id
        self._queue = queue
        self._running = threading.Event()
        self._idle = threading.Event()
        self._idle.set()
        self._tasks_processed = 0
        self._steals_attempted = 0
        self._steals_succeeded = 0

    @property
    def worker_id(self) -> int:
        return self._worker_id

    @property
    def tasks_processed(self) -> int:
        return self._tasks_processed

    @property
    def steals_attempted(self) -> int:
        return self._steals_attempted

    @property
    def steals_succeeded(self) -> int:
        return self._steals_succeeded

    @property
    def is_idle(self) -> bool:
        return self._idle.is_set()

    def run(self) -> None:
        """Main worker loop."""
        self._running.set()

        while self._running.is_set():
            request = self._get_task()

            if request is None:
                self._idle.set()
                continue

            self._idle.clear()

            try:
                self._execute_task(request)
                self._tasks_processed += 1
            except Exception as exc:  # pragma: no cover - defensive
                self._scheduler._loop.call_exception_handler({
                    "message": "Exception in worker thread",
                    "exception": exc,
                    "task": request.task,
                    "worker": self._worker_id,
                })

    def _get_task(self) -> StepRequest | None:
        """Get a request from the local queue, or steal one from another worker."""
        request = self._queue.pop(timeout=self._scheduler._config.idle_timeout)
        if request is not None:
            return request

        return self._steal_task()

    def _steal_task(self) -> StepRequest | None:
        """Steal work from other workers."""
        self._steals_attempted += 1

        workers = list(self._scheduler._workers.values())
        if len(workers) < 2:
            return None
        random.shuffle(workers)

        for worker in workers:
            if worker.worker_id == self._worker_id:
                continue
            if worker._queue.empty():
                continue

            stolen = worker._queue.steal(self._scheduler._config.steal_batch_size)
            if stolen:
                self._steals_succeeded += 1
                # Keep the bulk of the stolen work locally, run the first one.
                for request in stolen[1:]:
                    if not self._queue.push(request):
                        # Local queue is full: hand the remainder back.
                        self._scheduler._loop._schedule_task_step(request.task)
                return stolen[0]

        return None

    def _execute_task(self, request: StepRequest) -> None:
        """Execute a single task step on this worker thread."""
        loop = self._scheduler._loop
        loop._run_task_step(request.task)

    def stop(self) -> None:
        """Stop the worker thread."""
        self._running.clear()
        self._queue.wake_all()


class WorkStealingScheduler:
    """
    Work-stealing scheduler that distributes task steps across worker threads.
    
    This allows the event loop to use multiple CPU cores for CPU-bound async
    tasks while keeping a single-threaded model for I/O readiness.
    """

    def __init__(
        self,
        loop: "EventLoop",
        config: SchedulerConfig | None = None,
    ) -> None:
        self._loop = loop
        self._config = config or SchedulerConfig()
        self._workers: dict[int, WorkerThread] = {}
        self._queues: dict[int, TaskQueue] = {}
        self._running = False
        self._lock = threading.Lock()

    @property
    def config(self) -> SchedulerConfig:
        return self._config

    @property
    def num_workers(self) -> int:
        return len(self._workers)

    @property
    def running(self) -> bool:
        return self._running

    def start(self) -> None:
        """Start the scheduler workers."""
        with self._lock:
            if self._running:
                return

            # A previous run may have left stopped workers behind.
            self._workers.clear()
            self._queues.clear()

            num_workers = self._config.num_workers or (os.cpu_count() or 4)

            for i in range(num_workers):
                queue = TaskQueue(max_size=self._config.queue_size)
                self._queues[i] = queue
                worker = WorkerThread(self, i, queue)
                self._workers[i] = worker
                worker.start()

            self._running = True

    def stop(self) -> None:
        """Stop all workers.
        
        The worker records are kept so that :meth:`stats` can still report
        what happened during the last run.
        """
        with self._lock:
            if not self._running:
                return

            for worker in self._workers.values():
                worker.stop()
            for worker in self._workers.values():
                worker.join(timeout=5.0)

            self._running = False

    def submit_step(
        self,
        task: "Task[Any]",
        value: Any = None,
        exc: BaseException | None = None,
    ) -> bool:
        """Schedule a step of *task* to be executed by a worker thread."""
        request = StepRequest(task, value, exc)

        with self._lock:
            if not self._running or not self._workers:
                # Scheduler is not running: let the event loop handle it.
                self._loop._schedule_task_step(task)
                return False

            # Give the work to the least loaded worker.
            best_worker = min(
                self._workers.values(),
                key=lambda w: len(w._queue),
            )
            queued = best_worker._queue.push(request)

        if not queued:
            # Every local queue is full: fall back to the event loop.
            self._loop._schedule_task_step(task)
            return False
        return True

    def submit(self, task: "Task[Any]", worker_id: int | None = None) -> bool:
        """Submit a task for execution (compatibility helper)."""
        if worker_id is not None and worker_id in self._queues:
            return self._queues[worker_id].push(StepRequest(task))
        return self.submit_step(task)

    def stats(self) -> dict[str, Any]:
        """Get scheduler statistics."""
        return {
            "num_workers": len(self._workers),
            "running": self._running,
            "workers": {
                w.worker_id: {
                    "tasks_processed": w.tasks_processed,
                    "steals_attempted": w.steals_attempted,
                    "steals_succeeded": w.steals_succeeded,
                    "is_idle": w.is_idle,
                    "queue_size": len(w._queue),
                }
                for w in self._workers.values()
            },
        }


def get_scheduler(loop: "EventLoop") -> WorkStealingScheduler | None:
    """Get the scheduler for a loop."""
    return getattr(loop, "_scheduler", None)
