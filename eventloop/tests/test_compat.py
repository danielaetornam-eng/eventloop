"""
Tests for the asyncio compatibility layer.
"""

import asyncio

import pytest

from eventloop import new_event_loop, run, sleep
from eventloop.compat.asyncio import (
    as_completed,
    ensure_future,
    gather,
    get_event_loop,
    get_running_loop,
    wait,
    wait_for,
)


class TestEnsureFuture:
    """Test ensure_future."""

    def test_wraps_coroutine(self):
        async def test():
            async def value():
                await sleep(0.01)
                return 42

            fut = ensure_future(value())
            assert await fut == 42

        run(test())

    def test_returns_existing_task_unchanged(self):
        async def test():
            async def value():
                return 7

            task = ensure_future(value())
            again = ensure_future(task)
            assert again is task
            assert await again == 7

        run(test())

    def test_propagates_exception(self):
        async def test():
            async def boom():
                raise ValueError("boom")

            fut = ensure_future(boom())
            with pytest.raises(ValueError, match="boom"):
                await fut

        run(test())


class TestGather:
    """Test gather."""

    def test_collects_results_in_order(self):
        async def test():
            async def value(n, delay):
                await sleep(delay)
                return n

            results = await gather(
                value(1, 0.03),
                value(2, 0.01),
                value(3, 0.02),
            )
            assert results == [1, 2, 3]

        run(test())

    def test_runs_concurrently(self):
        async def test():
            import time

            async def value(delay):
                await sleep(delay)
                return delay

            start = time.monotonic()
            await gather(value(0.1), value(0.1), value(0.1))
            elapsed = time.monotonic() - start
            # Concurrently would be ~0.1s, sequentially ~0.3s.
            assert elapsed < 0.25

        run(test())

    def test_return_exceptions(self):
        async def test():
            async def ok():
                return 1

            async def boom():
                raise ValueError("boom")

            results = await gather(ok(), boom(), return_exceptions=True)
            assert results[0] == 1
            assert isinstance(results[1], ValueError)

        run(test())

    def test_raises_without_return_exceptions(self):
        async def test():
            async def boom():
                raise ValueError("boom")

            with pytest.raises(ValueError, match="boom"):
                await gather(boom())

        run(test())


class TestWait:
    """Test wait."""

    def test_all_completed(self):
        async def test():
            async def value(n):
                await sleep(0.01 * n)
                return n

            done, pending = await wait([value(1), value(2)])
            assert len(done) == 2
            assert not pending

        run(test())

    def test_first_completed(self):
        async def test():
            async def value(delay):
                await sleep(delay)
                return delay

            done, pending = await wait(
                [value(0.05), value(0.5)], return_when="FIRST_COMPLETED"
            )
            assert len(done) == 1
            assert len(pending) == 1

        run(test())

    def test_first_exception(self):
        async def test():
            async def boom():
                raise ValueError("boom")

            async def slow():
                await sleep(1.0)

            done, pending = await wait(
                [boom(), slow()], return_when="FIRST_EXCEPTION"
            )
            assert len(done) == 1
            assert len(pending) == 1

        run(test())

    def test_timeout(self):
        async def test():
            async def slow():
                await sleep(5.0)

            with pytest.raises(asyncio.TimeoutError):
                await wait([slow()], timeout=0.05)

        run(test())

    def test_empty(self):
        async def test():
            done, pending = await wait([])
            assert done == set()
            assert pending == set()

        run(test())

    def test_invalid_return_when(self):
        async def test():
            async def value():
                return 1

            with pytest.raises(ValueError):
                await wait([value()], return_when="NOPE")

        run(test())


class TestWaitFor:
    """Test wait_for."""

    def test_returns_result(self):
        async def test():
            async def value():
                await sleep(0.01)
                return 42

            assert await wait_for(value(), 1.0) == 42

        run(test())

    def test_times_out(self):
        async def test():
            async def slow():
                await sleep(5.0)

            with pytest.raises(asyncio.TimeoutError):
                await wait_for(slow(), 0.05)

        run(test())

    def test_no_timeout(self):
        async def test():
            async def value():
                await sleep(0.01)
                return 42

            assert await wait_for(value(), None) == 42

        run(test())


class TestAsCompleted:
    """Test as_completed."""

    def test_yields_in_completion_order(self):
        async def test():
            async def value(n, delay):
                await sleep(delay)
                return n

            results = []
            async for fut in as_completed([
                value(1, 0.06),
                value(2, 0.02),
                value(3, 0.04),
            ]):
                results.append(await fut)
            assert results == [2, 3, 1]

        run(test())


class TestLoopManagement:
    """Test the loop management helpers."""

    def test_get_running_loop_inside_loop(self):
        async def test():
            loop = get_running_loop()
            assert loop is not None
            assert loop.is_running()

        run(test())

    def test_get_running_loop_outside_loop(self):
        with pytest.raises(RuntimeError):
            get_running_loop()

    def test_get_event_loop_returns_running_loop(self):
        async def test():
            loop = get_event_loop()
            assert loop is get_running_loop()

        run(test())

    def test_get_event_loop_outside_loop(self):
        loop = get_event_loop()
        assert loop is not None
        assert not loop.is_running()
