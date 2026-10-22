"""
Tests for the work-stealing scheduler and the thread-pool executor.
"""

import threading
import time

import pytest

from eventloop import LoopConfig, gather, new_event_loop, sleep
from eventloop.core.context import current_loop
from eventloop.scheduler.work_stealing import (
    SchedulerConfig,
    TaskQueue,
    WorkStealingScheduler,
)


def _blocking_work(n: int) -> int:
    total = 0
    for i in range(n * 100_000):
        total += i
    return total


class TestTaskQueue:
    """Test the double-ended task queue used by the scheduler."""

    def test_push_and_pop(self):
        queue = TaskQueue(max_size=4)
        assert queue.empty()
        assert queue.push("a") is True
        assert queue.push("b") is True
        assert len(queue) == 2
        # Local access is LIFO.
        assert queue.pop() == "b"
        assert queue.pop() == "a"
        assert queue.pop() is None

    def test_respects_max_size(self):
        queue = TaskQueue(max_size=2)
        assert queue.push(1) is True
        assert queue.push(2) is True
        assert queue.push(3) is False

    def test_steal_takes_from_the_back(self):
        queue = TaskQueue(max_size=8)
        for item in ("a", "b", "c"):
            queue.push(item)
        stolen = queue.steal(2)
        assert len(stolen) == 2
        # Stealing removes the oldest entries (the tail of the deque).
        assert stolen == ["a", "b"]
        assert queue.pop() == "c"


class TestWorkStealingScheduler:
    """Test the work-stealing scheduler."""

    def test_scheduler_is_created_when_enabled(self):
        loop = new_event_loop(LoopConfig(enable_work_stealing=True))
        try:
            assert loop.scheduler is not None
            assert isinstance(loop.scheduler, WorkStealingScheduler)
        finally:
            loop.close()

    def test_scheduler_absent_by_default(self):
        loop = new_event_loop()
        try:
            assert loop.scheduler is None
        finally:
            loop.close()

    def test_tasks_run_to_completion_with_work_stealing(self):
        loop = new_event_loop(LoopConfig(enable_work_stealing=True))

        async def scenario():
            async def worker(n: int) -> int:
                total = 0
                for _ in range(n):
                    await sleep(0.001)
                    total += 1
                return total

            tasks = [current_loop().create_task(worker(10)) for _ in range(50)]
            return await gather(*tasks)

        try:
            results = loop.run_until_complete(scenario())
            assert len(results) == 50
            assert all(r == 10 for r in results)
        finally:
            loop.close()

    def test_tasks_are_distributed_across_worker_threads(self):
        loop = new_event_loop(LoopConfig(enable_work_stealing=True))
        seen: set[int] = set()
        main_thread = threading.get_ident()

        async def scenario():
            async def record() -> None:
                seen.add(threading.get_ident())

            tasks = [current_loop().create_task(record()) for _ in range(40)]
            await gather(*tasks)

        try:
            loop.run_until_complete(scenario())
            # The work must not all end up on the loop thread.
            assert len(seen) > 1
            assert seen != {main_thread}
        finally:
            loop.close()

    def test_worker_threads_are_started_and_stopped(self):
        loop = new_event_loop(LoopConfig(enable_work_stealing=True))
        observed: dict[str, object] = {}

        async def scenario():
            observed["num_workers"] = loop.scheduler.num_workers
            observed["running"] = loop.scheduler.running
            await sleep(0)

        try:
            loop.run_until_complete(scenario())
            assert observed["num_workers"] > 0
            assert observed["running"] is True
            # The workers are stopped together with the loop.
            assert loop.scheduler.running is False
            stats = loop.scheduler.stats()
            assert len(stats["workers"]) == observed["num_workers"]
        finally:
            loop.close()

    def test_stats_report_worker_activity(self):
        loop = new_event_loop(LoopConfig(enable_work_stealing=True))

        async def scenario():
            async def noop() -> int:
                return 1

            tasks = [current_loop().create_task(noop()) for _ in range(10)]
            await gather(*tasks)

        try:
            loop.run_until_complete(scenario())
            stats = loop.scheduler.stats()
            processed = sum(w["tasks_processed"] for w in stats["workers"].values())
            assert processed > 0
        finally:
            loop.close()

    def test_scheduler_submit_step_before_start_falls_back_to_loop(self):
        loop = new_event_loop(LoopConfig(enable_work_stealing=True))
        try:
            scheduler = loop.scheduler
            assert scheduler is not None
            # The scheduler has not been started yet, so the step must be
            # handed back to the event loop instead of being dropped.
            assert scheduler.submit_step(None) is False
        finally:
            loop.close()


class TestExecutor:
    """Test the thread-pool executor integration."""

    def test_run_in_executor_simple_form(self):
        from eventloop import run_in_executor

        async def scenario() -> int:
            return await run_in_executor(_blocking_work, 5)

        loop = new_event_loop()
        try:
            assert loop.run_until_complete(scenario()) == _blocking_work(5)
        finally:
            loop.close()

    def test_run_in_executor_asyncio_style_form(self):
        from eventloop import run_in_executor

        async def scenario() -> int:
            # asyncio style: executor first, then the callable.
            return await run_in_executor(None, _blocking_work, 5)

        loop = new_event_loop()
        try:
            assert loop.run_until_complete(scenario()) == _blocking_work(5)
        finally:
            loop.close()

    def test_to_thread_passes_kwargs(self):
        from eventloop import to_thread

        def add(a: int, b: int = 0) -> int:
            return a + b

        async def scenario() -> int:
            return await to_thread(add, 1, b=2)

        loop = new_event_loop()
        try:
            assert loop.run_until_complete(scenario()) == 3
        finally:
            loop.close()

    def test_executor_propagates_exceptions(self):
        from eventloop import run_in_executor

        def boom() -> None:
            raise ValueError("boom")

        async def scenario() -> None:
            await run_in_executor(boom)

        loop = new_event_loop()
        try:
            with pytest.raises(ValueError, match="boom"):
                loop.run_until_complete(scenario())
        finally:
            loop.close()

    def test_loop_run_in_executor_returns_a_future(self):
        loop = new_event_loop()

        async def scenario() -> int:
            fut = loop.run_in_executor(None, _blocking_work, 3)
            return await fut

        try:
            assert loop.run_until_complete(scenario()) == _blocking_work(3)
        finally:
            loop.close()

    def test_executor_does_not_leak_threads(self):
        from eventloop import to_thread

        async def scenario() -> int:
            return await to_thread(_blocking_work, 1)

        loop = new_event_loop()
        try:
            before = threading.active_count()
            for _ in range(5):
                loop.run_until_complete(scenario())
            # The default pool is shared, so it must not grow per call.
            assert threading.active_count() <= before + 4
        finally:
            loop.close()
