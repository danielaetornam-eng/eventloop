"""
Advanced examples for GodLoop.
"""

import asyncio
import sys
import time
from eventloop import (
    run,
    new_event_loop,
    create_task,
    sleep,
    gather,
    Lock,
    Event,
    Condition,
    Semaphore,
    Queue,
    TaskGroup,
    CancellationScope,
    shield,
    open_connection,
    start_server,
    create_subprocess_exec,
    run_in_executor,
    to_thread,
)
from eventloop.streams import StreamReader, StreamWriter
from eventloop.core.types import CancelledError
from eventloop.debug import EventLoopTracer, EventLoopProfiler, EventLoopMonitor


# Example 1: Producer-Consumer with Queue
async def producer_consumer():
    """Classic producer-consumer pattern."""
    queue: Queue[int] = Queue(maxsize=10)
    results = []
    
    async def producer(name: str, count: int):
        for i in range(count):
            await queue.put(i)
            print(f"{name} produced {i}")
            await sleep(0.01)
        await queue.put(None)  # Sentinel
    
    async def consumer(name: str):
        while True:
            item = await queue.get()
            if item is None:
                await queue.put(None)  # Pass sentinel to next consumer
                break
            results.append(item)
            print(f"{name} consumed {item}")
            await sleep(0.02)
    
    await gather(
        producer("P1", 5),
        producer("P2", 5),
        consumer("C1"),
        consumer("C2"),
    )
    
    print(f"Total consumed: {len(results)}")


# Example 2: Rate Limiter
class RateLimiter:
    """Token bucket rate limiter."""
    
    def __init__(self, rate: float, burst: int):
        self._rate = rate
        self._burst = burst
        self._tokens = burst
        self._last = time.monotonic()
        self._lock = Lock()
    
    async def acquire(self, tokens: int = 1) -> None:
        async with self._lock:
            now = time.monotonic()
            # Add tokens based on time passed
            self._tokens = min(self._burst, self._tokens + (now - self._last) * self._rate)
            self._last = now
            
            if self._tokens >= tokens:
                self._tokens -= tokens
                return
            
            # Need to wait for tokens
            deficit = tokens - self._tokens
            wait_time = deficit / self._rate
            self._tokens = 0
        
        await sleep(wait_time)


async def rate_limiter_example():
    """Demonstrate rate limiting."""
    limiter = RateLimiter(rate=10, burst=5)  # 10 ops/sec, burst of 5
    
    async def worker(name: str):
        for i in range(10):
            await limiter.acquire()
            print(f"{name} request {i} at {time.monotonic():.3f}")
    
    await gather(worker("A"), worker("B"))


# Example 3: Connection Pool
class ConnectionPool:
    """Generic connection pool."""
    
    def __init__(self, factory, max_size: int = 10):
        self._factory = factory
        self._max_size = max_size
        self._pool: Queue = Queue(maxsize=max_size)
        self._created = 0
        self._lock = Lock()
    
    async def acquire(self):
        async with self._lock:
            if self._pool.empty() and self._created < self._max_size:
                conn = await self._factory()
                self._created += 1
                return conn
        
        # Wait for available connection
        return await self._pool.get()
    
    async def release(self, conn):
        await self._pool.put(conn)
    
    async def close_all(self):
        while not self._pool.empty():
            conn = await self._pool.get()
            if hasattr(conn, 'close'):
                await conn.close()


async def connection_pool_example():
    """Demonstrate connection pool."""
    
    async def create_connection():
        # Simulate connection creation
        await sleep(0.01)
        return f"conn-{time.monotonic()}"
    
    pool = ConnectionPool(create_connection, max_size=3)
    
    async def worker(name: str):
        conn = await pool.acquire()
        print(f"{name} got {conn}")
        await sleep(0.05)
        await pool.release(conn)
        print(f"{name} released {conn}")
    
    await gather(*[worker(f"W{i}") for i in range(5)])
    await pool.close_all()


# Example 4: Graceful Shutdown
class GracefulServer:
    """Server with graceful shutdown."""
    
    def __init__(self):
        self._shutdown = Event()
        self._tasks: set = set()
        self._lock = Lock()
    
    async def start(self):
        async def handle_client(reader, writer):
            task = create_task(self._handle(reader, writer))
            self._tasks.add(task)
            try:
                await task
            finally:
                self._tasks.discard(task)
        
        self._server = await start_server(handle_client, "127.0.0.1", 8888)
        print("Server started")
        
        # Wait for shutdown signal
        await self._shutdown.wait()
        
        print("Shutting down...")
        self._server.close()
        await self._server.wait_closed()
        
        # Wait for active connections
        if self._tasks:
            await gather(*self._tasks)
        
        print("Shutdown complete")
    
    async def _handle(self, reader, writer):
        try:
            while True:
                data = await reader.read(100)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
    
    def shutdown(self):
        self._shutdown.set()


async def graceful_shutdown_example():
    """Demonstrate graceful shutdown."""
    server = GracefulServer()
    
    # Start server in background
    server_task = create_task(server.start())
    
    # Simulate shutdown after delay
    await sleep(0.1)
    server.shutdown()
    
    await server_task


# Example 5: Subprocess Management
async def subprocess_example():
    """Run subprocesses asynchronously."""
    
    # A simple command. ``sys.executable`` is used so the example works on
    # every platform.
    proc = await create_subprocess_exec(
        sys.executable, "-c", "print('Hello from subprocess!')",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    
    stdout, stderr = await proc.communicate()
    print(f"Exit code: {proc.returncode}")
    print(f"Stdout: {stdout.decode().strip()}")
    
    # Multiple concurrent subprocesses
    async def run_cmd(n: int):
        proc = await create_subprocess_exec(
            sys.executable, "-c", f"print('Task {n}')",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        return proc.returncode, stdout.decode().strip()
    
    results = await gather(run_cmd(1), run_cmd(2), run_cmd(3))
    
    for code, out in results:
        print(f"Exit {code}: {out}")


# Example 6: Thread Pool Integration
async def threadpool_example():
    """Run blocking code in thread pool."""
    
    def blocking_cpu_work(n: int) -> int:
        """Simulate CPU-intensive work."""
        total = 0
        for i in range(n * 100000):
            total += i
        return total
    
    def blocking_io_work(path: str) -> str:
        """Simulate blocking I/O."""
        time.sleep(0.1)
        return f"Read from {path}"
    
    # Run CPU work in thread pool
    results = await gather(
        run_in_executor(None, blocking_cpu_work, 10),
        run_in_executor(None, blocking_cpu_work, 20),
        run_in_executor(None, blocking_cpu_work, 30),
    )
    print(f"CPU results: {results}")
    
    # Run I/O work in thread pool
    results = await gather(
        to_thread(blocking_io_work, "/file1.txt"),
        to_thread(blocking_io_work, "/file2.txt"),
        to_thread(blocking_io_work, "/file3.txt"),
    )
    print(f"I/O results: {results}")


# Example 7: Web Scraper with Semaphore
async def web_scraper_example():
    """Simulate web scraping with concurrency control."""
    
    semaphore = Semaphore(3)  # Max 3 concurrent requests
    results = []
    
    async def fetch(url: str) -> dict:
        async with semaphore:
            print(f"Fetching {url}...")
            await sleep(0.1)  # Simulate network delay
            result = {"url": url, "status": 200, "size": 1024}
            results.append(result)
            print(f"Fetched {url}")
            return result
    
    urls = [f"https://example.com/page{i}" for i in range(10)]
    
    async with TaskGroup() as tg:
        for url in urls:
            tg.create_task(fetch(url))
    
    print(f"Fetched {len(results)} pages")


# Example 8: Real-time Data Processing Pipeline
async def pipeline_example():
    """Data processing pipeline with stages."""
    
    # Stage queues
    raw_queue: Queue[str] = Queue()
    processed_queue: Queue[dict] = Queue()
    output_queue: Queue[str] = Queue()
    
    async def source():
        for i in range(20):
            await raw_queue.put(f"data-{i}")
            await sleep(0.01)
        await raw_queue.put(None)
    
    async def processor():
        while True:
            item = await raw_queue.get()
            if item is None:
                await processed_queue.put(None)
                break
            # Process
            processed = {"id": item, "value": len(item) * 2}
            await processed_queue.put(processed)
    
    async def sink():
        while True:
            item = await processed_queue.get()
            if item is None:
                await output_queue.put(None)
                break
            output = f"output-{item['id']}-{item['value']}"
            await output_queue.put(output)
    
    async def collector():
        while True:
            item = await output_queue.get()
            if item is None:
                break
            print(f"Collected: {item}")
    
    await gather(source(), processor(), sink(), collector())


# Example 9: Custom Scheduler Integration
async def custom_scheduler_example():
    """Run CPU-bound tasks on a work-stealing scheduler."""
    from eventloop import LoopConfig
    
    # A dedicated loop with the work-stealing scheduler enabled. The
    # scheduler is started automatically when the loop starts running.
    loop = new_event_loop(LoopConfig(
        enable_work_stealing=True,
        worker_threads=4,
    ))
    
    async def cpu_task(n: int) -> int:
        # Runs on a worker thread: the scheduler hands task steps to workers.
        total = sum(i * i for i in range(n * 1000))
        return total
    
    async def scenario():
        tasks = [create_task(cpu_task(i)) for i in range(100)]
        return await gather(*tasks)
    
    results = loop.run_until_complete(scenario())
    
    print(f"Completed {len(results)} CPU tasks")
    stats = loop.scheduler.stats()
    processed = sum(w["tasks_processed"] for w in stats["workers"].values())
    print(f"Steps executed by {stats['num_workers']} workers: {processed}")
    print(f"Steals: {sum(w['steals_succeeded'] for w in stats['workers'].values())}")
    
    loop.close()


# Example 10: Comprehensive Application Template
class Application:
    """Full-featured application template."""
    
    def __init__(self):
        self.loop = new_event_loop()
        self.server = None
        self.background_tasks: set = set()
    
    async def startup(self):
        """Application startup."""
        print("Starting application...")
        
        # Start HTTP server
        async def handle_request(reader, writer):
            writer.write(b"HTTP/1.1 200 OK\r\n\r\nHello!")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        
        self.server = await start_server(handle_request, "127.0.0.1", 8080)
        
        # Start background tasks
        self.background_tasks.add(
            create_task(self._health_check())
        )
        self.background_tasks.add(
            create_task(self._metrics_collector())
        )
        
        print("Application started")
    
    async def _health_check(self):
        while True:
            print("Health check OK")
            await sleep(10)
    
    async def _metrics_collector(self):
        while True:
            print("Collecting metrics...")
            await sleep(30)
    
    async def shutdown(self):
        """Graceful shutdown."""
        print("Shutting down...")
        
        # Stop accepting new connections
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        # Cancel background tasks
        for task in self.background_tasks:
            task.cancel()
        
        # Wait for tasks to finish
        if self.background_tasks:
            await gather(*self.background_tasks, return_exceptions=True)
        
        self.loop.close()
        print("Shutdown complete")
    
    def run(self):
        """Run the application."""
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.loop.call_soon_threadsafe(
                lambda: create_task(self.shutdown(), loop=self.loop)
            )


async def main():
    """Run all advanced examples."""
    print("=== Producer-Consumer ===")
    await producer_consumer()
    
    print("\n=== Rate Limiter ===")
    await rate_limiter_example()
    
    print("\n=== Connection Pool ===")
    await connection_pool_example()
    
    print("\n=== Graceful Shutdown ===")
    await graceful_shutdown_example()
    
    print("\n=== Subprocess ===")
    await subprocess_example()
    
    print("\n=== Thread Pool ===")
    await threadpool_example()
    
    print("\n=== Web Scraper ===")
    await web_scraper_example()
    
    print("\n=== Pipeline ===")
    await pipeline_example()
    
    print("\n=== Custom Scheduler ===")
    await custom_scheduler_example()


if __name__ == "__main__":
    run(main())