"""
Test configuration and fixtures.
"""

import pytest
import asyncio
from eventloop import EventLoop, new_event_loop, set_event_loop, get_event_loop

@pytest.fixture
def event_loop():
    """Create a new event loop for each test."""
    loop = new_event_loop()
    set_event_loop(loop)
    yield loop
    loop.close()
    set_event_loop(None)

@pytest.fixture
def running_loop(event_loop):
    """Run the event loop in a background thread."""
    import threading
    import time
    
    def run_loop():
        event_loop.run_forever()
    
    thread = threading.Thread(target=run_loop, daemon=True)
    thread.start()
    
    # Wait for loop to start
    time.sleep(0.01)
    
    yield event_loop
    
    event_loop.stop()
    thread.join(timeout=1.0)

# Pytest configuration
def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: mark test as asyncio test")

def pytest_collection_modifyitems(config, items):
    import inspect
    for item in items:
        if inspect.iscoroutinefunction(item.function):
            item.add_marker(pytest.mark.asyncio)