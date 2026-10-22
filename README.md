# eventloop - A High-Performance Async Event Loop for Python

A complete, dependency-free asynchronous event loop for Python 3.11+, with an
asyncio-compatible API, structured concurrency, a real work-stealing
scheduler, TLS support and a built-in tracer/profiler/monitor.

```bash
pip install .
```

## Quick start

```python
from eventloop import run, create_task, sleep, gather

async def main():
    async def worker(name):
        print(f"{name} starting")
        await sleep(0.1)
        return f"{name}-result"

    results = await gather(worker("A"), worker("B"), worker("C"))
    print(f"Results: {results}")

run(main())
```

## What is implemented

### Core event loop
- Selector based I/O backend (`selectors` module: epoll on Linux, kqueue on
  BSD/macOS, `select` on Windows) with a cross-thread self-pipe wake-up.
- Hierarchical timer wheel with O(1) scheduling, plus deadline helpers
  (`timeout`, `move_on_after`, `fail_after`).
- `Task` / `Future` / `Handle` primitives with context-variable propagation,
  task parenting and structured cancellation.
- Debug mode with source tracebacks, slow-callback and slow-task detection.

### Work-stealing scheduler
A genuine multi-threaded scheduler. Task steps are handed to a pool of worker
threads, each owning a double-ended queue; idle workers steal work from the
back of a victim's queue. A task may migrate between the event loop thread and
any worker thread, and is never stepped by two threads at once.

```python
from eventloop import LoopConfig, new_event_loop

loop = new_event_loop(LoopConfig(enable_work_stealing=True, worker_threads=4))
loop.run_until_complete(main())
```

> **Note:** the GIL means that pure-Python CPU-bound coroutines still do not
> run in parallel. The scheduler pays off for work that releases the GIL
> (C extensions, `numpy`, compression) and for spreading I/O-bound tasks over
> several threads. It is off by default.

### Structured concurrency
- `TaskGroup` and `Nursery`: child tasks are awaited on exit, and a failing
  child cancels its siblings.
- `CancellationScope` with nesting, shielding and `shield()`.
- A scope's cancellation is scoped to the scope: it is withdrawn if the host
  task leaves the scope before the cancellation was delivered.

### Synchronization primitives
`Lock`, `Event`, `Condition`, `Semaphore`, `BoundedSemaphore`, `Barrier`,
`Queue`, `PriorityQueue`, `LifoQueue`. They may be created before the loop is
running (the loop is resolved lazily) and they hand the lock/slot on when a
waiter is cancelled, so a cancelled waiter can never strand the primitive.

### Streams and networking
- `StreamReader` / `StreamWriter` with buffered reads, `readline()`, `readuntil()`
  and flow control.
- `open_connection()`, `start_server()`, `open_unix_connection()`.
- **TLS**: `ssl=` is fully supported on both the client and the server side.
  The handshake is driven by the event loop (non-blocking, `SSLWantRead` /
  `SSLWantWrite` aware) and `open_connection()` only returns once the
  handshake has completed.
- Unix domain sockets.

### Subprocesses
`create_subprocess_exec()` / `create_subprocess_shell()` with async
stdin/stdout/stderr and `communicate()`. Pipes are driven by dedicated reader
threads and the process is reaped through a watcher.

### Thread pool
`run_in_executor()` / `to_thread()` run blocking code in a shared,
lifecycle-managed thread pool. Both the simple
`run_in_executor(func, *args)` and the asyncio-style
`run_in_executor(executor, func, *args)` calling conventions are supported.

### Debugging and profiling
- `EventLoopTracer` - records every callback, task step and I/O event.
- `EventLoopProfiler` - per-task and per-callback timings.
- `EventLoopMonitor` - live health metrics and peak tracking.

```python
from eventloop import trace_loop, profile_loop, monitor_loop

loop = new_event_loop()
tracer = trace_loop(loop)
profiler = profile_loop(loop)
monitor = monitor_loop(loop, interval=1.0)
```

### asyncio compatibility
`ensure_future`, `gather`, `wait`, `wait_for`, `as_completed`, `sleep`,
`create_task`, `run_in_executor`, `to_thread`, `get_running_loop`,
`get_event_loop`, `set_event_loop`, `new_event_loop`, plus the
`StreamReader` / `StreamWriter` / `Lock` / `Event` / `Condition` /
`Semaphore` / `Queue` equivalents.

> The compatibility layer covers the commonly used surface, not the whole of
> the asyncio API. `asyncio.Future` / `asyncio.Task` are not interchangeable
> with the native types; use the `asyncio_*` aliases exported by the package
> when you need asyncio-shaped objects.

## Not implemented (yet)

These are placeholders or explicitly out of scope, and the package will tell
you so rather than silently misbehaving:

- **True io_uring I/O.** The `IoUringBackend` exists and is selected by
  `LoopConfig(use_io_uring=True)` on Linux, but submissions still go through
  the regular selector. It is safe to enable, but it is not faster yet.
- **IOCP / Proactor backend on Windows.** The selector backend is used.
- **Actor framework.** Not implemented.
- `asyncio` subprocess watchers are exposed for API compatibility, but the
  native subprocess implementation uses its own reaper.

## Architecture

```
eventloop/
├── core/           # Event loop, tasks, futures, handles, transports
├── scheduler/      # Work-stealing multi-threaded scheduler
├── timers/         # Timer wheels, deadline tracking
├── concurrency/    # TaskGroup, Nursery, CancellationScope, sync primitives
├── streams/        # StreamReader, StreamWriter, protocols, connections
├── subprocess/     # Async subprocess management
├── signals/        # Signal handling
├── threadpool/     # Thread pool integration
├── compat/         # asyncio compatibility layer
├── debug/          # Tracing, profiling, monitoring
├── tests/          # Test suite (126 tests)
├── examples/       # Usage examples
└── benchmarks/     # Performance benchmarks
```

## Running the tests

```bash
pytest eventloop/tests/ -v
```

## Running the examples and benchmarks

```bash
python -m eventloop.examples.basic
python -m eventloop.examples.advanced
python -m eventloop.benchmarks.benchmark
python -m eventloop.benchmarks.comparison
```

The comparison benchmark prints the measured difference against `asyncio` on
the same machine. The native loop is currently slower than `asyncio` for
task creation and primitive contention - it does considerably more work per
task (context tracking, task parenting, the pending-step protocol and the
thread-safety locks). The numbers are reported honestly rather than tuned.

## Requirements

- Python 3.11+
- No mandatory third-party dependencies
- Optional: `cryptography` (only used by the test suite to generate
  self-signed certificates for the TLS tests)

## License

MIT License
