"""
Comparison benchmarks: GodLoop vs asyncio.
"""

import asyncio
import time
import statistics
from eventloop import (
    run,
    new_event_loop,
    create_task,
    sleep as gl_sleep,
    gather as gl_gather,
    Lock as GLLock,
    Semaphore as GLSemaphore,
    Queue as GLQueue,
    TaskGroup,
)


async def asyncio_sleep(n: int):
    """Run n asyncio sleeps."""
    for _ in range(n):
        await asyncio.sleep(0)


async def godloop_sleep(n: int):
    """Run n GodLoop sleeps."""
    for _ in range(n):
        await gl_sleep(0)


def benchmark_asyncio_task_creation(num_tasks: int) -> float:
    """Benchmark asyncio task creation."""
    async def main():
        tasks = [asyncio.create_task(asyncio.sleep(0)) for _ in range(num_tasks)]
        await asyncio.gather(*tasks)
    
    start = time.perf_counter()
    asyncio.run(main())
    return time.perf_counter() - start


def benchmark_godloop_task_creation(num_tasks: int) -> float:
    """Benchmark GodLoop task creation."""
    async def main():
        tasks = [create_task(gl_sleep(0)) for _ in range(num_tasks)]
        await gl_gather(*tasks)
    
    loop = new_event_loop()
    start = time.perf_counter()
    run(main(), loop=loop)
    elapsed = time.perf_counter() - start
    loop.close()
    return elapsed


def benchmark_asyncio_lock(num_tasks: int, iterations: int) -> float:
    """Benchmark asyncio lock contention."""
    lock = asyncio.Lock()
    counter = 0
    
    async def worker():
        nonlocal counter
        for _ in range(iterations):
            async with lock:
                counter += 1
                await asyncio.sleep(0)
    
    async def main():
        tasks = [asyncio.create_task(worker()) for _ in range(num_tasks)]
        await asyncio.gather(*tasks)
    
    start = time.perf_counter()
    asyncio.run(main())
    return time.perf_counter() - start


def benchmark_godloop_lock(num_tasks: int, iterations: int) -> float:
    """Benchmark GodLoop lock contention."""
    lock = GLLock()
    counter = 0
    
    async def worker():
        nonlocal counter
        for _ in range(iterations):
            async with lock:
                counter += 1
                await gl_sleep(0)
    
    async def main():
        tasks = [create_task(worker()) for _ in range(num_tasks)]
        await gl_gather(*tasks)
    
    loop = new_event_loop()
    start = time.perf_counter()
    run(main(), loop=loop)
    elapsed = time.perf_counter() - start
    loop.close()
    return elapsed


def benchmark_asyncio_semaphore(num_tasks: int, permits: int, ops: int) -> float:
    """Benchmark asyncio semaphore."""
    sem = asyncio.Semaphore(permits)
    counter = 0
    
    async def worker():
        nonlocal counter
        for _ in range(ops):
            async with sem:
                counter += 1
                await asyncio.sleep(0)
    
    async def main():
        tasks = [asyncio.create_task(worker()) for _ in range(num_tasks)]
        await asyncio.gather(*tasks)
    
    start = time.perf_counter()
    asyncio.run(main())
    return time.perf_counter() - start


def benchmark_godloop_semaphore(num_tasks: int, permits: int, ops: int) -> float:
    """Benchmark GodLoop semaphore."""
    sem = GLSemaphore(permits)
    counter = 0
    
    async def worker():
        nonlocal counter
        for _ in range(ops):
            async with sem:
                counter += 1
                await gl_sleep(0)
    
    async def main():
        tasks = [create_task(worker()) for _ in range(num_tasks)]
        await gl_gather(*tasks)
    
    loop = new_event_loop()
    start = time.perf_counter()
    run(main(), loop=loop)
    elapsed = time.perf_counter() - start
    loop.close()
    return elapsed


def benchmark_asyncio_queue(num_producers: int, num_consumers: int, items: int) -> float:
    """Benchmark asyncio queue."""
    queue = asyncio.Queue()
    produced = 0
    consumed = 0
    
    async def producer():
        nonlocal produced
        for i in range(items):
            await queue.put(i)
            produced += 1
            await asyncio.sleep(0)
    
    async def consumer():
        nonlocal consumed
        for _ in range(items * num_producers // num_consumers):
            await queue.get()
            consumed += 1
            await asyncio.sleep(0)
    
    async def main():
        producers = [asyncio.create_task(producer()) for _ in range(num_producers)]
        consumers = [asyncio.create_task(consumer()) for _ in range(num_consumers)]
        await asyncio.gather(*producers, *consumers)
    
    start = time.perf_counter()
    asyncio.run(main())
    return time.perf_counter() - start


def benchmark_godloop_queue(num_producers: int, num_consumers: int, items: int) -> float:
    """Benchmark GodLoop queue."""
    queue = GLQueue()
    produced = 0
    consumed = 0
    
    async def producer():
        nonlocal produced
        for i in range(items):
            await queue.put(i)
            produced += 1
            await gl_sleep(0)
    
    async def consumer():
        nonlocal consumed
        for _ in range(items * num_producers // num_consumers):
            await queue.get()
            consumed += 1
            await gl_sleep(0)
    
    async def main():
        producers = [create_task(producer()) for _ in range(num_producers)]
        consumers = [create_task(consumer()) for _ in range(num_consumers)]
        await gl_gather(*producers, *consumers)
    
    loop = new_event_loop()
    start = time.perf_counter()
    run(main(), loop=loop)
    elapsed = time.perf_counter() - start
    loop.close()
    return elapsed


def run_comparison():
    """Run comparison benchmarks."""
    print("=" * 80)
    print("GodLoop vs asyncio Comparison")
    print("=" * 80)
    
    tests = [
        ("Task Creation (1000)", lambda: benchmark_asyncio_task_creation(1000), lambda: benchmark_godloop_task_creation(1000)),
        ("Task Creation (10000)", lambda: benchmark_asyncio_task_creation(10000), lambda: benchmark_godloop_task_creation(10000)),
        ("Lock Contention (10x100)", lambda: benchmark_asyncio_lock(10, 100), lambda: benchmark_godloop_lock(10, 100)),
        ("Lock Contention (50x100)", lambda: benchmark_asyncio_lock(50, 100), lambda: benchmark_godloop_lock(50, 100)),
        ("Semaphore (20x5x50)", lambda: benchmark_asyncio_semaphore(20, 5, 50), lambda: benchmark_godloop_semaphore(20, 5, 50)),
        ("Queue (5x5x100)", lambda: benchmark_asyncio_queue(5, 5, 100), lambda: benchmark_godloop_queue(5, 5, 100)),
    ]
    
    results = []
    
    for name, asyncio_bench, godloop_bench in tests:
        print(f"\n{name}:")
        
        # Run asyncio
        asyncio_times = []
        for _ in range(3):
            t = asyncio_bench()
            asyncio_times.append(t)
        asyncio_avg = statistics.mean(asyncio_times)
        print(f"  asyncio: {asyncio_avg:.4f}s (avg of 3)")
        
        # Run GodLoop
        godloop_times = []
        for _ in range(3):
            t = godloop_bench()
            godloop_times.append(t)
        godloop_avg = statistics.mean(godloop_times)
        print(f"  GodLoop: {godloop_avg:.4f}s (avg of 3)")
        
        # Compare
        ratio = asyncio_avg / godloop_avg
        if ratio > 1:
            print(f"  GodLoop is {ratio:.2f}x faster")
        else:
            print(f"  asyncio is {1/ratio:.2f}x faster")
        
        results.append({
            "name": name,
            "asyncio": asyncio_avg,
            "godloop": godloop_avg,
            "ratio": ratio,
        })
    
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    
    for r in results:
        faster = "GodLoop" if r["ratio"] > 1 else "asyncio"
        speedup = max(r["ratio"], 1/r["ratio"])
        print(f"{r['name']:30s} : {faster} {speedup:.2f}x faster")


if __name__ == "__main__":
    run_comparison()