"""
Tests for debug module.
"""

import pytest
import time
from eventloop import (
    run,
    new_event_loop,
    EventLoopTracer,
    EventLoopProfiler,
    EventLoopMonitor,
    trace_loop,
    profile_loop,
    monitor_loop,
)


class TestTracer:
    """Test EventLoopTracer."""
    
    def test_tracer_basic(self):
        loop = new_event_loop()
        tracer = EventLoopTracer(loop, max_events=100)
        
        tracer.enable()
        tracer.record("test_event", {"key": "value"})
        tracer.disable()
        
        events = tracer.events
        assert len(events) == 1
        assert events[0].event_type == "test_event"
        assert events[0].details == {"key": "value"}
        
        loop.close()
    
    def test_tracer_callbacks(self):
        loop = new_event_loop()
        tracer = EventLoopTracer(loop)
        captured = []
        
        def callback(event):
            captured.append(event)
        
        tracer.add_callback(callback)
        tracer.enable()
        tracer.record("test", {})
        
        assert len(captured) == 1
        assert captured[0].event_type == "test"
        
        loop.close()
    
    def test_tracer_stats(self):
        loop = new_event_loop()
        tracer = EventLoopTracer(loop)
        
        tracer.enable()
        tracer.record("type1", {})
        tracer.record("type1", {})
        tracer.record("type2", {})
        
        stats = tracer.get_stats()
        assert stats["total_events"] == 3
        assert stats["by_type"]["type1"] == 2
        assert stats["by_type"]["type2"] == 1
        
        loop.close()


class TestProfiler:
    """Test EventLoopProfiler."""
    
    def test_profiler_basic(self):
        loop = new_event_loop()
        profiler = EventLoopProfiler(loop)
        
        profiler.enable()
        profiler.record_task("task1", 0.01)
        profiler.record_task("task1", 0.02)
        profiler.record_callback("cb1", 0.005)
        
        task_stats = profiler.get_task_stats()
        assert len(task_stats) == 1
        assert task_stats[0].name == "task1"
        assert task_stats[0].call_count == 2
        assert task_stats[0].total_time == 0.03
        
        cb_stats = profiler.get_callback_stats()
        assert len(cb_stats) == 1
        assert cb_stats[0].name == "cb1"
        
        loop.close()
    
    def test_profiler_summary(self):
        loop = new_event_loop()
        profiler = EventLoopProfiler(loop)
        
        profiler.enable()
        profiler.record_task("task1", 0.01)
        
        summary = profiler.get_summary()
        assert summary["task_calls"] == 1
        assert summary["unique_tasks"] == 1
        
        loop.close()


class TestMonitor:
    """Test EventLoopMonitor."""
    
    def test_monitor_basic(self):
        loop = new_event_loop()
        monitor = EventLoopMonitor(loop, interval=0.01)
        
        monitor.start()
        time.sleep(0.03)
        monitor.stop()
        
        stats = monitor.get_latest()
        assert stats is not None
        assert stats.pending_tasks >= 0
        
        loop.close()
    
    def test_monitor_history(self):
        loop = new_event_loop()
        monitor = EventLoopMonitor(loop, interval=0.01)
        
        monitor.start()
        time.sleep(0.03)
        monitor.stop()
        
        history = monitor.get_history(duration=1.0)
        assert len(history) >= 2
        
        loop.close()
    
    def test_monitor_summary(self):
        loop = new_event_loop()
        monitor = EventLoopMonitor(loop, interval=0.01)
        
        monitor.start()
        time.sleep(0.03)
        monitor.stop()
        
        summary = monitor.get_summary()
        assert "current" in summary
        assert "averages" in summary
        assert "peaks" in summary
        
        loop.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])