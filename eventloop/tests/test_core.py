"""
Tests for core event loop functionality.
"""

import pytest
import time
import asyncio
from eventloop import (
    EventLoop,
    new_event_loop,
    get_event_loop,
    set_event_loop,
    run,
    create_task,
    current_task,
    all_tasks,
    sleep,
    gather,
    Task,
    Future,
    ensure_future,
)
from eventloop.core.types import LoopState, CancelledError


class TestEventLoopBasics:
    """Test basic event loop functionality."""
    
    def test_new_event_loop(self):
        loop = new_event_loop()
        assert isinstance(loop, EventLoop)
        assert loop.state == LoopState.CREATED
        loop.close()
    
    def test_get_set_event_loop(self):
        loop = new_event_loop()
        set_event_loop(loop)
        assert get_event_loop() is loop
        loop.close()
        set_event_loop(None)
    
    def test_run_simple_coro(self):
        async def simple() -> int:
            return 42
        
        result = run(simple())
        assert result == 42
    
    def test_run_with_loop_arg(self):
        loop = new_event_loop()
        
        async def simple() -> int:
            return 100
        
        result = run(simple(), loop=loop)
        assert result == 100
        loop.close()
    
    def test_loop_state_transitions(self):
        loop = new_event_loop()
        assert loop.state == LoopState.CREATED
        
        # Run in background thread
        import threading
        def run_loop():
            loop.run_forever()
        
        thread = threading.Thread(target=run_loop, daemon=True)
        thread.start()
        
        # Wait for the loop to actually start running.
        deadline = time.monotonic() + 1.0
        while loop.state != LoopState.RUNNING and time.monotonic() < deadline:
            time.sleep(0.005)
        assert loop.state == LoopState.RUNNING
        
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=1.0)
        
        assert loop.state == LoopState.STOPPED
        loop.close()
        assert loop.state == LoopState.CLOSED


class TestTasks:
    """Test task creation and management."""
    
    def test_create_task(self):
        async def coro() -> str:
            return "hello"
        
        loop = new_event_loop()
        task = create_task(coro(), loop=loop)
        
        assert isinstance(task, Task)
        assert task.state.name == "PENDING"
        
        loop.run_until_complete(task)
        
        assert task.done()
        assert task.result() == "hello"
        loop.close()
    
    def test_current_task(self):
        async def check_current():
            task = current_task()
            assert task is not None
            return task.name
        
        loop = new_event_loop()
        task = create_task(check_current(), loop=loop)
        loop.run_until_complete(task)
        
        assert task.result() == task.name
        loop.close()
    
    def test_done_callback_receives_the_task(self):
        """A done-callback must be called with the task, not the future."""
        seen: list[object] = []
        
        async def child():
            return 42
        
        async def scenario():
            task = create_task(child())
            task.add_done_callback(lambda t: seen.append(t))
            await task
        
        run(scenario())
        assert len(seen) == 1
        assert isinstance(seen[0], Task)
    
    def test_remove_done_callback(self):
        seen: list[object] = []
        
        async def child():
            return 1
        
        def callback(t):
            seen.append(t)
        
        async def scenario():
            task = create_task(child())
            task.add_done_callback(callback)
            assert task.remove_done_callback(callback) == 1
            await task
        
        run(scenario())
        assert seen == []
    
    def test_all_tasks(self):
        async def child():
            await sleep(0.01)
        
        loop = new_event_loop()
        tasks = {create_task(child(), loop=loop) for _ in range(3)}
        
        # The tasks are tracked as soon as they are created.
        assert len(all_tasks(loop)) >= 3
        
        loop.run_until_complete(gather(*tasks))
        
        # ...and removed once they finish.
        assert len(all_tasks(loop)) == 0
        loop.close()
    
    def test_task_cancellation(self):
        async def long_task():
            await sleep(10)
        
        loop = new_event_loop()
        task = create_task(long_task(), loop=loop)
        
        def cancel_it():
            task.cancel("test cancellation")
        
        loop.call_soon(cancel_it)
        with pytest.raises(CancelledError):
            loop.run_until_complete(task)
        
        assert task.cancelled()
        loop.close()
    
    def test_task_exception(self):
        async def failing_task():
            raise ValueError("test error")
        
        loop = new_event_loop()
        task = create_task(failing_task(), loop=loop)
        
        with pytest.raises(ValueError, match="test error"):
            loop.run_until_complete(task)
        
        assert task.done()
        assert isinstance(task.exception(), ValueError)
        loop.close()


class TestFutures:
    """Test Future implementation."""
    
    def test_future_result(self):
        loop = new_event_loop()
        fut = Future(loop=loop)
        
        fut.set_result("test")
        assert fut.done()
        assert fut.result() == "test"
        loop.close()
    
    def test_future_exception(self):
        loop = new_event_loop()
        fut = Future(loop=loop)
        
        fut.set_exception(ValueError("test"))
        assert fut.done()
        assert isinstance(fut.exception(), ValueError)
        loop.close()
    
    def test_future_cancel(self):
        loop = new_event_loop()
        fut = Future(loop=loop)
        
        assert fut.cancel("cancelled")
        assert fut.cancelled()
        assert fut.cancel_message() == "cancelled"
        loop.close()
    
    def test_future_callback(self):
        loop = new_event_loop()
        fut = Future(loop=loop)
        results = []
        
        def callback(f):
            results.append(f.result())
        
        fut.add_done_callback(callback)
        fut.set_result("callback test")
        
        assert results == ["callback test"]
        loop.close()
    
    def test_ensure_future(self):
        async def coro() -> int:
            return 123
        
        loop = new_event_loop()
        fut = ensure_future(coro(), loop=loop)
        
        loop.run_until_complete(fut)
        
        assert fut.done()
        assert fut.result() == 123
        loop.close()


class TestSleep:
    """Test sleep functionality."""
    
    def test_sleep(self):
        async def sleep_test():
            start = time.monotonic()
            await sleep(0.05)
            elapsed = time.monotonic() - start
            return elapsed
        
        loop = new_event_loop()
        elapsed = run(sleep_test(), loop=loop)
        
        assert 0.04 < elapsed < 0.2
        loop.close()


class TestCallbackScheduling:
    """Test callback scheduling."""
    
    def test_call_soon(self):
        loop = new_event_loop()
        results = []
        
        def callback(arg):
            results.append(arg)
            loop.stop()
        
        loop.call_soon(callback, "test")
        loop.run_forever()
        
        assert results == ["test"]
        loop.close()
    
    def test_call_later(self):
        loop = new_event_loop()
        results = []
        
        def callback():
            results.append(time.monotonic())
            loop.stop()
        
        start = time.monotonic()
        loop.call_later(0.05, callback)
        loop.run_forever()
        
        elapsed = results[0] - start
        assert 0.04 < elapsed < 0.2
        loop.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])