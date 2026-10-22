"""
Basic usage examples for GodLoop.
"""

import asyncio
from eventloop import (
    run,
    new_event_loop,
    create_task,
    sleep,
    gather,
    shield,
    Lock,
    Event,
    Semaphore,
    TaskGroup,
    CancellationScope,
    open_connection,
    start_server,
    StreamReader,
    StreamWriter,
)
from eventloop.core.types import CancelledError


# Example 1: Basic async/await
async def basic_example():
    """Basic async function."""
    print("Starting...")
    await sleep(0.1)
    print("After sleep")
    return "done"


# Example 2: Concurrent tasks
async def concurrent_example():
    """Run multiple tasks concurrently."""
    
    async def worker(name: str, delay: float):
        print(f"{name} starting")
        await sleep(delay)
        print(f"{name} done")
        return f"{name}-result"
    
    # Run concurrently
    results = await gather(
        worker("A", 0.1),
        worker("B", 0.05),
        worker("C", 0.15),
    )
    print(f"Results: {results}")



# Example 3: Synchronization primitives
async def sync_example():
    """Demonstrate synchronization primitives."""
    
    # Lock
    lock = Lock()
    counter = 0
    
    async def increment():
        nonlocal counter
        async with lock:
            for _ in range(100):
                counter += 1
                await sleep(0)  # Yield
    
    tasks = [create_task(increment()) for _ in range(10)]
    await gather(*tasks)
    print(f"Counter: {counter}")  # Should be 1000
    
    # Event
    event = Event()
    
    async def waiter(name: str):
        print(f"{name} waiting...")
        await event.wait()
        print(f"{name} woke up!")
    
    tasks = [create_task(waiter(f"W{i}")) for i in range(3)]
    await sleep(0.05)
    event.set()
    await gather(*tasks)
    
    # Semaphore
    sem = Semaphore(2)
    
    async def limited_worker(name: str):
        async with sem:
            print(f"{name} acquired semaphore")
            await sleep(0.05)
            print(f"{name} releasing semaphore")
    
    tasks = [create_task(limited_worker(f"L{i}")) for i in range(5)]
    await gather(*tasks)



# Example 4: Structured concurrency with TaskGroup
async def taskgroup_example():
    """Demonstrate TaskGroup for structured concurrency."""
    
    async def risky_task(name: str, should_fail: bool):
        print(f"{name} starting")
        await sleep(0.1)
        if should_fail:
            raise ValueError(f"{name} failed!")
        print(f"{name} succeeded")
        return name
    
    # All succeed
    async with TaskGroup() as tg:
        tg.create_task(risky_task("A", False))
        tg.create_task(risky_task("B", False))
    
    print("All tasks succeeded")
    
    # One fails - all cancelled
    try:
        async with TaskGroup() as tg:
            tg.create_task(risky_task("C", False))
            tg.create_task(risky_task("D", True))  # This will fail
            tg.create_task(risky_task("E", False))
    except ValueError as e:
        print(f"TaskGroup caught error: {e}")



# Example 5: Cancellation scopes
async def cancellation_example():
    """Demonstrate cancellation scopes."""
    
    async def long_task(name: str):
        try:
            print(f"{name} running...")
            await sleep(10)
            print(f"{name} completed")
        except CancelledError:
            print(f"{name} cancelled!")
            raise
    
    async with CancellationScope() as scope:
        t1 = create_task(long_task("Task1"))
        t2 = create_task(long_task("Task2"))
        
        await sleep(0.05)
        print("Cancelling scope...")
        scope.cancel()
        
        try:
            await t1
            await t2
        except CancelledError:
            pass
    
    print("Scope cancelled, tasks cleaned up")



# Example 6: Shield from cancellation
async def shield_example():
    """Demonstrate shield to protect work from cancellation."""
    
    async def critical_operation():
        print("Critical operation starting...")
        await sleep(0.2)
        print("Critical operation completed!")
        return "critical-result"
    
    async def normal_task():
        try:
            await sleep(10)
        except CancelledError:
            print("Normal task cancelled")
            raise
    
    scope = CancellationScope()
    async with scope:
        # This task is shielded from scope cancellation.
        shielded = create_task(shield(critical_operation()))
        
        # This task will be cancelled.
        normal = create_task(normal_task())
        
        # A shielded scope makes the host task immune too, so it can wait for
        # the shielded child to finish.
        async with CancellationScope(shield=True):
            await sleep(0.05)
            scope.cancel()
            
            # The shielded task runs to completion.
            result = await shielded
            print(f"Shielded result: {result}")
        
        # The unprotected task was cancelled by the scope.
        try:
            await normal
        except CancelledError:
            pass



# Example 7: TCP Echo Server
async def handle_client(reader: StreamReader, writer: StreamWriter):
    """Handle a client connection."""
    addr = writer.transport.get_extra_info("peername")
    print(f"Client connected: {addr}")
    
    try:
        while True:
            data = await reader.read(100)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"Client disconnected: {addr}")


async def server_example():
    """Run a TCP echo server."""
    server = await start_server(handle_client, "127.0.0.1", 8888)
    
    print("Server started on 127.0.0.1:8888")
    
    # Server runs until cancelled
    try:
        await asyncio.sleep(3600)  # Run for an hour
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        server.close()
        await server.wait_closed()


# Example 8: TCP Client
async def client_example():
    """Connect to a TCP server."""
    reader, writer = await open_connection("127.0.0.1", 8888)
    
    writer.write(b"Hello, server!")
    await writer.drain()
    
    data = await reader.read(100)
    print(f"Received: {data.decode()}")
    
    writer.close()
    await writer.wait_closed()


# Example 9: Custom event loop configuration
async def custom_loop_example():
    """Use custom event loop configuration."""
    from eventloop import LoopConfig
    
    config = LoopConfig(
        debug=True,
        slow_callback_duration=0.05,
        enable_work_stealing=True,
        worker_threads=4,
    )
    
    loop = new_event_loop(config)
    
    async def monitored_task():
        await sleep(0.1)
        return "done"
    
    result = run(monitored_task(), loop=loop)
    print(f"Result: {result}")
    
    loop.close()


# Example 10: Debugging and profiling
async def debug_example():
    """Use debugging and profiling tools."""
    from eventloop import trace_loop, profile_loop, monitor_loop
    
    loop = new_event_loop()
    
    # Tracing
    tracer = trace_loop(loop)
    
    async def traced_task():
        await sleep(0.05)
        return "traced"
    
    task = create_task(traced_task(), loop=loop)
    loop.run_until_complete(task)
    
    print(f"Trace stats: {tracer.get_stats()}")
    
    # Profiling
    profiler = profile_loop(loop)
    
    async def profiled_task():
        await sleep(0.01)
    
    tasks = [create_task(profiled_task(), loop=loop) for _ in range(10)]
    loop.run_until_complete(gather(*tasks))
    
    profiler.print_stats()
    
    # Monitoring
    monitor = monitor_loop(loop, interval=0.1)
    
    async def monitored():
        await sleep(0.3)
    
    task = create_task(monitored(), loop=loop)
    loop.run_until_complete(task)
    
    monitor.stop()
    monitor.print_status()
    print(f"Monitor summary: {monitor.get_summary()}")
    
    loop.close()


async def main():
    """Run every basic example in order."""
    print("=== Basic Example ===")
    result = await basic_example()
    print(f"Result: {result}")

    print("\n=== Concurrent Example ===")
    await concurrent_example()

    print("\n=== Sync Example ===")
    await sync_example()

    print("\n=== TaskGroup Example ===")
    await taskgroup_example()

    print("\n=== Cancellation Example ===")
    await cancellation_example()

    print("\n=== Shield Example ===")
    await shield_example()

    print("\n=== Custom Loop Example ===")
    await custom_loop_example()

    print("\n=== Debug Example ===")
    await debug_example()


if __name__ == "__main__":
    run(main())