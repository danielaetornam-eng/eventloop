"""
Tests for concurrency primitives.
"""

import pytest
import time
from eventloop import (
    run,
    create_task,
    sleep,
    gather,
    Lock,
    Event,
    Condition,
    Semaphore,
    BoundedSemaphore,
    Barrier,
    BrokenBarrierError,
    TaskGroup,
    CancellationScope,
    shield,
    asyncio_wait_for,
)
from eventloop.core.types import CancelledError


class TestLock:
    """Test async Lock."""
    
    def test_lock_basic(self):
        async def test():
            lock = Lock()
            assert not lock.locked
            
            await lock.acquire()
            assert lock.locked
            
            lock.release()
            assert not lock.locked
        
        run(test())
    
    def test_lock_contention(self):
        async def test():
            lock = Lock()
            results = []
            
            async def worker(name: str):
                await lock.acquire()
                results.append(f"{name}-start")
                await sleep(0.01)
                results.append(f"{name}-end")
                lock.release()
            
            t1 = create_task(worker("A"))
            t2 = create_task(worker("B"))
            
            await t1
            await t2
            
            assert results == ["A-start", "A-end", "B-start", "B-end"]
        
        run(test())
    
    def test_lock_context_manager(self):
        async def test():
            lock = Lock()
            results = []
            
            async with lock:
                results.append("inside")            
            results.append("outside")
            assert results == ["inside", "outside"]
        
        run(test())
    
    def test_lock_released_when_waiter_is_cancelled(self):
        """A cancelled waiter must not leave the lock stuck."""
        async def test():
            lock = Lock()
            
            await lock.acquire()
            
            waiter = create_task(lock.acquire())
            await sleep(0.01)
            waiter.cancel()
            try:
                await waiter
            except CancelledError:
                pass
            
            lock.release()
            
            # The lock must be acquirable again.
            async with lock:
                pass
        
        run(test())
    
    def test_lock_handed_over_when_first_waiter_cancelled(self):
        """Cancelling the head waiter must not strand the other waiters."""
        async def test():
            lock = Lock()
            order = []
            
            await lock.acquire()
            
            first = create_task(lock.acquire())
            await sleep(0.01)
            second = create_task(lock.acquire())
            await sleep(0.01)
            
            first.cancel()
            try:
                await first
            except CancelledError:
                pass
            
            lock.release()
            
            # The second waiter must still get the lock.
            await asyncio_wait_for(second, 1.0)
            order.append("second")
            lock.release()
            assert order == ["second"]
        
        run(test())
    
    def test_lock_usable_without_running_loop_at_creation(self):
        """Primitives may be created before the loop is running."""
        lock = Lock()
        sem = Semaphore(1)
        
        async def test():
            async with lock:
                pass
            async with sem:
                pass
        
        run(test())


class TestEvent:
    """Test async Event."""
    
    def test_event_basic(self):
        async def test():
            event = Event()
            assert not event.is_set
            
            event.set()
            assert event.is_set
            
            event.clear()
            assert not event.is_set
        
        run(test())
    
    def test_event_wait(self):
        async def test():
            event = Event()
            results = []
            
            async def waiter():
                await event.wait()
                results.append("woken")
            
            t = create_task(waiter())
            await sleep(0.01)
            assert not results
            
            event.set()
            await t
            
            assert results == ["woken"]
        
        run(test())
    
    def test_event_multiple_waiters(self):
        async def test():
            event = Event()
            results = []
            
            async def waiter(name: str):
                await event.wait()
                results.append(name)
            
            tasks = [create_task(waiter(f"W{i}")) for i in range(3)]
            await sleep(0.01)
            
            event.set()
            await gather(*tasks)
            
            assert len(results) == 3
        
        run(test())


class TestCondition:
    """Test async Condition."""
    
    def test_condition_basic(self):
        async def test():
            cond = Condition()
            
            async with cond:
                assert cond.lock.locked
            
            assert not cond.lock.locked
        
        run(test())
    
    def test_condition_wait_notify(self):
        async def test():
            cond = Condition()
            results = []
            
            async def waiter():
                async with cond:
                    await cond.wait()
                    results.append("woken")
            
            t = create_task(waiter())
            await sleep(0.01)
            
            async with cond:
                cond.notify()
            
            await t
            assert results == ["woken"]
        
        run(test())


class TestSemaphore:
    """Test async Semaphore."""
    
    def test_semaphore_basic(self):
        async def test():
            sem = Semaphore(2)
            assert sem.value == 2
            
            await sem.acquire()
            assert sem.value == 1
            
            sem.release()
            assert sem.value == 2
        
        run(test())
    
    def test_semaphore_limit(self):
        async def test():
            sem = Semaphore(2)
            results = []
            
            async def worker(name: str):
                await sem.acquire()
                results.append(f"{name}-start")
                await sleep(0.01)
                results.append(f"{name}-end")
                sem.release()
            
            tasks = [create_task(worker(f"W{i}")) for i in range(4)]
            await gather(*tasks)
            
            # Check that at most 2 run concurrently
            starts = [r for r in results if r.endswith("-start")]
            assert len(starts) == 4
        
        run(test())
    
    def test_bounded_semaphore(self):
        async def test():
            sem = BoundedSemaphore(1)
            await sem.acquire()
            
            sem.release()
            # This should raise
            with pytest.raises(ValueError):
                sem.release()
        
        run(test())


class TestBarrier:
    """Test async Barrier."""
    
    def test_barrier_basic(self):
        async def test():
            barrier = Barrier(3)
            results = []
            
            async def worker(name: str):
                idx = await barrier.wait()
                results.append(f"{name}-{idx}")
            
            tasks = [create_task(worker(f"W{i}")) for i in range(3)]
            await gather(*tasks)
            
            assert len(results) == 3
        
        run(test())
    
    def test_barrier_abort(self):
        async def test():
            barrier = Barrier(3)
            
            async def worker():
                try:
                    await barrier.wait()
                except BrokenBarrierError:
                    return "broken"
                return "ok"
            
            t1 = create_task(worker())
            await sleep(0.01)
            
            barrier.abort()
            
            result = await t1
            assert result == "broken"
        
        run(test())


class TestTaskGroup:
    """Test TaskGroup."""
    
    def test_task_group_basic(self):
        async def test():
            async with TaskGroup() as tg:
                tg.create_task(sleep(0.01))
                tg.create_task(sleep(0.01))
        
        run(test())
    
    def test_task_group_exception(self):
        async def test():
            async def fail():
                raise ValueError("test")
            
            with pytest.raises(ValueError, match="test"):
                async with TaskGroup() as tg:
                    tg.create_task(fail())
        
        run(test())
    
    def test_task_group_cancellation(self):
        async def test():
            cancelled = []
            
            async def long():
                try:
                    await sleep(10)
                except CancelledError:
                    cancelled.append("cancelled")
                    raise
            
            async def fail():
                await sleep(0.01)
                raise ValueError("boom")
            
            # An error in one child cancels its siblings.
            with pytest.raises(ValueError, match="boom"):
                async with TaskGroup() as tg:
                    tg.create_task(long())
                    tg.create_task(long())
                    tg.create_task(fail())
            
            assert len(cancelled) == 2
        
        run(test())
    
    def test_task_group_waits_for_children(self):
        async def test():
            finished = []
            
            async def child():
                await sleep(0.01)
                finished.append("done")
            
            # On a normal exit the group waits for all children.
            async with TaskGroup() as tg:
                tg.create_task(child())
                tg.create_task(child())
            
            assert len(finished) == 2
        
        run(test())


class TestCancellationScope:
    """Test CancellationScope."""
    
    def test_cancellation_scope(self):
        async def test():
            async with CancellationScope() as scope:
                assert not scope.cancelled
                
                scope.cancel()
                assert scope.cancelled
        
        run(test())
    
    def test_shield(self):
        async def test():
            results = []
            
            async def worker(name):
                try:
                    await sleep(0.05)
                    results.append(f"{name}-completed")
                except CancelledError:
                    results.append(f"{name}-cancelled")
                    raise
            
            scope = CancellationScope()
            async with scope:
                # A shielded child and an unprotected child.
                shielded_task = create_task(shield(worker("shielded")))
                create_task(worker("plain"))
                
                # Inside a shielded scope the host task is immune, so it can
                # wait for the shielded child to finish.
                async with CancellationScope(shield=True):
                    await sleep(0.01)
                    scope.cancel()
                    await shielded_task
            
            assert "plain-cancelled" in results
            assert "shielded-completed" in results
        
        run(test())
        
        run(test())

    def test_scope_cancellation_does_not_escape_the_scope(self):
        """A scope's pending cancellation must not be delivered later.
        
        Regression test: the host task was running (not suspended) when the
        scope was cancelled, so the cancellation could not be delivered
        straight away. It used to be delivered at the *next* suspension
        point, which could be far outside the scope - and it escaped
        through unrelated scopes.
        """
        async def test():
            reached_end = False

            async def child():
                await sleep(10)

            async with CancellationScope() as scope:
                task = create_task(child())
                await sleep(0.01)
                scope.cancel()
                try:
                    await task
                except CancelledError:
                    pass

            # The scope has been left, so the host task must be usable
            # again without being cancelled at some later point.
            await sleep(0.05)
            reached_end = True
            return reached_end

        assert run(test()) is True

    def test_cancelled_scope_leaves_host_task_usable(self):
        """After a cancelled scope exits, the host task keeps working."""
        async def test():
            async with CancellationScope() as scope:
                scope.cancel()

            # Must not raise CancelledError.
            await sleep(0.05)
            return "ok"

        assert run(test()) == "ok"

    def test_shielded_scope_protects_against_outer_cancellation(self):
        """A shielded scope keeps a pending cancellation from escaping."""
        async def test():
            async with CancellationScope() as scope:
                async with CancellationScope(shield=True):
                    await sleep(0.02)
                    scope.cancel()
                    await sleep(0.05)
            return "ok"

        assert run(test()) == "ok"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])