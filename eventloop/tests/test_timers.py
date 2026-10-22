"""
Tests for timer wheel and deadline tracking.
"""

import pytest
import time
from eventloop import (
    run,
    sleep,
    new_event_loop,
    TimerWheel,
    HierarchicalTimerWheel,
    DeadlineTracker,
    timeout,
    move_on_after,
    fail_after,
)
from eventloop.core.handle import TimerHandle, create_timer_handle
from eventloop.core.types import CancelledError, TimeoutError


class TestTimerWheel:
    """Test TimerWheel."""
    
    def test_timer_wheel_basic(self):
        wheel = TimerWheel(tick_duration=0.01, size=100)
        
        results = []
        def callback(value):
            results.append(value)
        
        handle = TimerHandle(None, time.monotonic() + 0.05, callback, "test")
        wheel.schedule(handle)
        
        # Advance time
        time.sleep(0.06)
        expired = wheel.advance()
        
        assert len(expired) == 1
        expired[0]._run()
        assert results == ["test"]
    
    def test_timer_wheel_periodic(self):
        loop = new_event_loop()
        
        count = 0
        def callback():
            nonlocal count
            count += 1
            if count >= 5:
                loop.stop()
        
        # A periodic timer reschedules itself through the loop's wheel.
        handle = create_timer_handle(
            loop,
            time.monotonic() + 0.02,
            callback,
            periodic=True,
            interval=0.02,
        )
        loop._timer_wheel.schedule(handle)
        
        loop.run_forever()
        
        assert count == 5
        loop.close()
    
    def test_timer_wheel_cancel(self):
        wheel = TimerWheel(tick_duration=0.01, size=100)
        
        results = []
        def callback(value):
            results.append(value)
        
        handle = TimerHandle(None, time.monotonic() + 0.05, callback, "test")
        wheel.schedule(handle)
        
        # Cancel before expiry
        wheel.cancel(handle)
        
        time.sleep(0.06)
        expired = wheel.advance()
        
        assert len(expired) == 0
        assert results == []


class TestHierarchicalTimerWheel:
    """Test HierarchicalTimerWheel."""
    
    def test_hierarchical_wheel_basic(self):
        wheel = HierarchicalTimerWheel(
            tick_duration=0.001,
            wheel_size=100,
            num_levels=3,
        )
        
        results = []
        def callback(value):
            results.append(value)
        
        # Short timer (level 0)
        handle1 = TimerHandle(None, time.monotonic() + 0.01, callback, "short")
        wheel.schedule(handle1)
        
        # Long timer (level 1)
        handle2 = TimerHandle(None, time.monotonic() + 1.0, callback, "long")
        wheel.schedule(handle2)
        
        time.sleep(0.02)
        expired = wheel.advance()
        
        assert len(expired) == 1
        expired[0]._run()
        assert results == ["short"]
    
    def test_hierarchical_wheel_cascade(self):
        wheel = HierarchicalTimerWheel(
            tick_duration=0.001,
            wheel_size=10,
            num_levels=2,
        )
        
        # Timer that will cascade from level 1 to level 0
        results = []
        def callback(value):
            results.append(value)
        
        handle = TimerHandle(None, time.monotonic() + 0.015, callback, "cascade")
        wheel.schedule(handle)
        
        # Advance past first level tick
        time.sleep(0.02)
        expired = wheel.advance()
        
        # Should have cascaded and executed
        assert len(expired) == 1
        expired[0]._run()
        assert results == ["cascade"]


class TestDeadlineTracker:
    """Test DeadlineTracker."""
    
    def test_deadline_tracker_basic(self):
        tracker = DeadlineTracker()
        
        id1 = tracker.add(0.1)
        id2 = tracker.add(0.2)
        
        deadline1 = tracker.get(id1)
        deadline2 = tracker.get(id2)
        
        assert deadline1 is not None
        assert deadline2 is not None
        assert deadline1 < deadline2
    
    def test_deadline_expiration(self):
        tracker = DeadlineTracker()
        
        id1 = tracker.add(0.01)  # 10ms
        id2 = tracker.add(0.1)   # 100ms
        
        time.sleep(0.02)
        expired = tracker.get_expired()
        
        assert len(expired) == 1
        assert expired[0].timestamp <= time.monotonic()
    
    def test_deadline_removal(self):
        tracker = DeadlineTracker()
        
        id1 = tracker.add(0.1)
        assert tracker.remove(id1)
        assert tracker.get(id1) is None
        assert not tracker.remove(id1)  # Already removed


class TestTimeoutContext:
    """Test timeout context managers."""
    
    def test_timeout_success(self):
        async def test():
            async with timeout(0.1):
                await sleep(0.01)
        
        run(test())
    
    def test_timeout_expiry(self):
        async def test():
            with pytest.raises(CancelledError):
                async with timeout(0.01):
                    await sleep(0.1)
        
        run(test())
    
    def test_move_on_after(self):
        async def test():
            result = None
            async with move_on_after(0.01):
                await sleep(0.1)
                result = "completed"
            
            # Should move on after timeout
            assert result is None
        
        run(test())
    
    def test_fail_after(self):
        async def test():
            # fail_after raises a TimeoutError rather than propagating the
            # cancellation.
            with pytest.raises(TimeoutError):
                async with fail_after(0.01):
                    await sleep(0.1)
        
        run(test())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])