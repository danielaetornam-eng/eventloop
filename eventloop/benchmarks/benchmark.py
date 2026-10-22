"""
Benchmarks for GodLoop event loop.
"""

import asyncio
import time
import statistics
from eventloop import (
    run,
    new_event_loop,
    create_task,
    sleep,
    gather,
    Lock,
    Semaphore,
    Queue,
    TaskGroup,
)
from eventloop.debug import EventLoopProfiler


def benchmark_task_creation(num_tasks: int = 10000) -> dict:
    """Benchmark task creation and execution."""
    
    async def simple_task():
        await sleep(0)
        return 1
    
    async def benchmark():
        tasks = [create_task(simple_task()) for _ in range(num_tasks)]
        await gather(*tasks)
    
    loop = new_event_loop()
    start = time.perf_counter()
    run(benchmark(), loop=loop)
    elapsed = time.perf_counter() - start
    loop.close()
    
    return {
        "name": "task_creation",
        "num_tasks": num_tasks,
        "time": elapsed,
        "tasks_per_sec": num_tasks / elapsed,
    }


def benchmark_task_switching(num_switches: int = 10000) -> dict:
    """Benchmark task context switching."""
    
    async def ping_pong():
        for _ in range(num_switches):
            await sleep(0)
    
    async def benchmark():
        await ping_pong()
    
    loop = new_event_loop()
    start = time.perf_counter()
    run(benchmark(), loop=loop)
    elapsed = time.perf_counter() - start
    loop.close()
    
    return {
        "name": "task_switching",
        "num_switches": num_switches,
        "time": elapsed,
        "switches_per_sec": num_switches / elapsed,
    }


def benchmark_lock_contention(num_tasks: int = 100, iterations: int = 1000) -> dict:
    """Benchmark lock contention."""
    
    lock = Lock()
    counter = 0
    
    async def worker():
        nonlocal counter
        for _ in range(iterations):
            async with lock:
                counter += 1
                await sleep(0)
    
    async def benchmark():
        tasks = [create_task(worker()) for _ in range(num_tasks)]
        await gather(*tasks)
    
    loop = new_event_loop()
    start = time.perf_counter()
    run(benchmark(), loop=loop)
    elapsed = time.perf_counter() - start
    loop.close()
    
    total_ops = num_tasks * iterations
    return {
        "name": "lock_contention",
        "num_tasks": num_tasks,
        "iterations": iterations,
        "total_ops": total_ops,
        "time": elapsed,
        "ops_per_sec": total_ops / elapsed,
    }


def benchmark_semaphore(num_tasks: int = 100, permits: int = 10, ops: int = 100) -> dict:
    """Benchmark semaphore."""
    
    sem = Semaphore(permits)
    counter = 0
    
    async def worker():
        nonlocal counter
        for _ in range(ops):
            async with sem:
                counter += 1
                await sleep(0)
    
    async def benchmark():
        tasks = [create_task(worker()) for _ in range(num_tasks)]
        await gather(*tasks)
    
    loop = new_event_loop()
    start = time.perf_counter()
    run(benchmark(), loop=loop)
    elapsed = time.perf_counter() - start
    loop.close()
    
    total_ops = num_tasks * ops
    return {
        "name": "semaphore",
        "num_tasks": num_tasks,
        "permits": permits,
        "total_ops": total_ops,
        "time": elapsed,
        "ops_per_sec": total_ops / elapsed,
    }


def benchmark_queue(num_producers: int = 10, num_consumers: int = 10, items: int = 1000) -> dict:
    """Benchmark queue operations."""
    
    queue: Queue[int] = Queue()
    produced = 0
    consumed = 0
    
    async def producer():
        nonlocal produced
        for i in range(items):
            await queue.put(i)
            produced += 1
            await sleep(0)
    
    async def consumer():
        nonlocal consumed
        for _ in range(items * num_producers // num_consumers):
            await queue.get()
            consumed += 1
            await sleep(0)
    
    async def benchmark():
        producers = [create_task(producer()) for _ in range(num_producers)]
        consumers = [create_task(consumer()) for _ in range(num_consumers)]
        await gather(*producers, *consumers)
    
    loop = new_event_loop()
    start = time.perf_counter()
    run(benchmark(), loop=loop)
    elapsed = time.perf_counter() - start
    loop.close()
    
    return {
        "name": "queue",
        "num_producers": num_producers,
        "num_consumers": num_consumers,
        "items": items,
        "total_ops": produced + consumed,
        "time": elapsed,
        "ops_per_sec": (produced + consumed) / elapsed,
    }


def benchmark_taskgroup(num_groups: int = 100, tasks_per_group: int = 10) -> dict:
    """Benchmark TaskGroup."""
    
    async def child():
        await sleep(0)
    
    async def benchmark():
        for _ in range(num_groups):
            async with TaskGroup() as tg:
                for _ in range(tasks_per_group):
                    tg.create_task(child())
    
    loop = new_event_loop()
    start = time.perf_counter()
    run(benchmark(), loop=loop)
    elapsed = time.perf_counter() - start
    loop.close()
    
    total_tasks = num_groups * tasks_per_group
    return {
        "name": "taskgroup",
        "num_groups": num_groups,
        "tasks_per_group": tasks_per_group,
        "total_tasks": total_tasks,
        "time": elapsed,
        "tasks_per_sec": total_tasks / elapsed,
    }


def benchmark_timer_wheel(num_timers: int = 10000) -> dict:
    """Benchmark timer wheel insertion and expiration."""
    from eventloop.timers import TimerWheel
    from eventloop.core.handle import create_timer_handle
    
    loop = new_event_loop()
    try:
        wheel = TimerWheel(tick_duration=0.001, size=10000)
        callbacks: list[int] = []
        
        def make_callback(i: int):
            def cb() -> None:
                callbacks.append(i)
            return cb
        
        now = time.monotonic()
        
        # Schedule timers
        start = time.perf_counter()
        for i in range(num_timers):
            handle = create_timer_handle(
                loop, now + 0.001 * (i % 1000), make_callback(i)
            )
            wheel.schedule(handle)
        schedule_time = time.perf_counter() - start
        
        # Advance and execute
        start = time.perf_counter()
        for step in range(1000):
            expired = wheel.advance(now + step * 0.001)
            for h in expired:
                h._run()
        execute_time = time.perf_counter() - start
        
        total_time = schedule_time + execute_time
        return {
            "name": "timer_wheel",
            "num_timers": num_timers,
            "fired": len(callbacks),
            "schedule_time": schedule_time,
            "execute_time": execute_time,
            "time": total_time,
            "total_time": total_time,
            "timers_per_sec": num_timers / total_time if total_time else 0.0,
        }
    finally:
        loop.close()


def benchmark_profiler_overhead(num_operations: int = 10000) -> dict:
    """Benchmark the overhead the profiler adds to the event loop."""
    
    async def simple_op():
        await sleep(0)
    
    async def scenario(loop, profiler):
        if profiler is not None:
            loop.set_profiler(profiler)
        tasks = [create_task(simple_op()) for _ in range(num_operations)]
        await gather(*tasks)
        if profiler is not None:
            loop.set_profiler(None)
    
    # Baseline: no profiler attached.
    loop = new_event_loop()
    start = time.perf_counter()
    run(scenario(loop, None), loop=loop)
    baseline = time.perf_counter() - start
    loop.close()
    
    # With the profiler attached.
    loop = new_event_loop()
    profiler = EventLoopProfiler(loop)
    profiler.enable()
    start = time.perf_counter()
    run(scenario(loop, profiler), loop=loop)
    elapsed = time.perf_counter() - start
    loop.close()
    
    overhead = elapsed - baseline
    return {
        "name": "profiler_overhead",
        "num_operations": num_operations,
        "time": elapsed,
        "baseline_time": baseline,
        "overhead": overhead,
        "overhead_per_op": overhead / num_operations if num_operations else 0.0,
        "ops_per_sec": num_operations / elapsed,
    }


def run_all_benchmarks():
    """Run all benchmarks and print results."""
    benchmarks = [
        benchmark_task_creation,
        benchmark_task_switching,
        benchmark_lock_contention,
        benchmark_semaphore,
        benchmark_queue,
        benchmark_taskgroup,
        benchmark_timer_wheel,
        benchmark_profiler_overhead,
    ]
    
    print("=" * 80)
    print("GodLoop Benchmarks")
    print("=" * 80)
    
    results = []
    for bench in benchmarks:
        print(f"\nRunning {bench.__name__}...")
        try:
            result = bench()
            results.append(result)
            print(f"  Time: {result.get('time', 0):.4f}s")
            if 'ops_per_sec' in result:
                print(f"  Ops/sec: {result['ops_per_sec']:,.0f}")
            if 'tasks_per_sec' in result:
                print(f"  Tasks/sec: {result['tasks_per_sec']:,.0f}")
            if 'switches_per_sec' in result:
                print(f"  Switches/sec: {result['switches_per_sec']:,.0f}")
            if 'timers_per_sec' in result:
                print(f"  Timers/sec: {result['timers_per_sec']:,.0f}")
            if 'overhead' in result:
                print(f"  Profiler overhead: {result['overhead']:.4f}s "
                      f"({result['overhead_per_op'] * 1e6:.2f}us/op)")
        except Exception as e:
            import traceback
            print(f"  ERROR: {e}")
            traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    
    for r in results:
        print(f"{r['name']:30s} : {r.get('time', 0):.4f}s")


if __name__ == "__main__":
    run_all_benchmarks()