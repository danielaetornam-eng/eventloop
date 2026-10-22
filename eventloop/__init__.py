"""
GodLoop - God-tier event loop implementation for Python.

A comprehensive, production-ready, and research-grade event loop featuring:
- Multiple I/O backends (selectors, epoll, kqueue, IOCP, io_uring)
- Work-stealing multi-threaded scheduler
- Structured concurrency (TaskGroup, Nursery, CancellationScope)
- Advanced timer system (timer wheel, hierarchical timing wheels)
- Full asyncio compatibility layer
- Comprehensive debugging and profiling tools
"""

from __future__ import annotations

__version__ = "0.1.0"
__author__ = "Event Loop Research Team"
__license__ = "MIT"

# Core exports
from .core.types import LoopConfig, TimerConfig, SchedulerConfig
from .core.loop import EventLoop, new_event_loop, get_event_loop, set_event_loop, run
from .core.task import Task, create_task, current_task, all_tasks, sleep, sleep_forever, shield
from .core.future import Future, ensure_future
from .core.handle import Handle, TimerHandle
from .core.context import Context, copy_context

# Scheduler exports
from .scheduler.work_stealing import WorkStealingScheduler, SchedulerConfig

# Timer exports
from .timers.wheel import TimerWheel, HierarchicalTimerWheel
from .timers.deadline import DeadlineTracker, timeout, move_on_after, fail_after

# Concurrency exports
from .concurrency.task_group import TaskGroup, TaskGroupError
from .concurrency.nursery import Nursery, NurseryError
from .concurrency.cancellation import CancellationScope, CancellationError, shield
from .concurrency.synchronization import (
    Lock,
    Event,
    Condition,
    Semaphore,
    BoundedSemaphore,
    Barrier,
    BrokenBarrierError,
)
from .concurrency.queue import Queue, PriorityQueue, LifoQueue

# Stream exports
from .streams.reader import StreamReader, StreamReaderProtocol, LimitOverrunError, IncompleteReadError
from .streams.writer import StreamWriter, FlowControlMixin
from .streams.protocol import Protocol, BaseProtocol, DatagramProtocol
from .streams.connection import open_connection, start_server
from .streams.pipe import open_pipe, open_unix_connection

# Subprocess exports
from .subprocess.process import Process, create_subprocess_exec, create_subprocess_shell
from .subprocess.transport import SubprocessTransport

# Signal exports
from .signals.handler import SignalHandler, signal, set_signal_handler, get_signal_handler

# Thread pool exports
from .threadpool.pool import (
    ThreadPool,
    run_in_executor,
    set_default_executor,
    get_default_executor,
    to_thread,
)

# Debug exports
from .debug.tracer import EventLoopTracer, trace_loop
from .debug.profiler import EventLoopProfiler, profile_loop
from .debug.monitor import EventLoopMonitor, monitor_loop

# Compatibility exports
from .compat.asyncio import (
    ensure_future as asyncio_ensure_future,
    gather as asyncio_gather,
    gather,
    wait as asyncio_wait,
    wait_for as asyncio_wait_for,
    as_completed as asyncio_as_completed,
    sleep as asyncio_sleep,
    run as asyncio_run,
    create_task as asyncio_create_task,
    current_task as asyncio_current_task,
    all_tasks as asyncio_all_tasks,
    Event as asyncio_Event,
    Lock as asyncio_Lock,
    Condition as asyncio_Condition,
    Semaphore as asyncio_Semaphore,
    BoundedSemaphore as asyncio_BoundedSemaphore,
    Barrier as asyncio_Barrier,
    Queue as asyncio_Queue,
    PriorityQueue as asyncio_PriorityQueue,
    LifoQueue as asyncio_LifoQueue,
    StreamReader as asyncio_StreamReader,
    StreamWriter as asyncio_StreamWriter,
    open_connection as asyncio_open_connection,
    start_server as asyncio_start_server,
    create_subprocess_exec as asyncio_create_subprocess_exec,
    create_subprocess_shell as asyncio_create_subprocess_shell,
    run_in_executor as asyncio_run_in_executor,
    to_thread as asyncio_to_thread,
    get_running_loop as asyncio_get_running_loop,
    get_event_loop as asyncio_get_event_loop,
    set_event_loop as asyncio_set_event_loop,
    new_event_loop as asyncio_new_event_loop,
    AbstractEventLoop,
    AbstractServer,
    AbstractChildWatcher,
    FastChildWatcher,
    SafeChildWatcher,
    ThreadedChildWatcher,
    PidfdChildWatcher,
    SelectorEventLoop,
    ProactorEventLoop,
)

__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__license__",
    # Core
    "EventLoop",
    "LoopConfig",
    "TimerConfig",
    "SchedulerConfig",
    "new_event_loop",
    "get_event_loop",
    "set_event_loop",
    "Task",
    "create_task",
    "current_task",
    "all_tasks",
    "Future",
    "ensure_future",
    "Handle",
    "TimerHandle",
    "Context",
    "copy_context",
    "run",
    # Scheduler
    "WorkStealingScheduler",
    "SchedulerConfig",
    # Timers
    "TimerWheel",
    "HierarchicalTimerWheel",
    "DeadlineTracker",
    # Concurrency
    "TaskGroup",
    "TaskGroupError",
    "Nursery",
    "NurseryError",
    "CancellationScope",
    "CancellationError",
    "shield",
    "Lock",
    "Event",
    "Condition",
    "Semaphore",
    "BoundedSemaphore",
    "Barrier",
    "BrokenBarrierError",
    # Queues
    "Queue",
    "PriorityQueue",
    "LifoQueue",
    # Streams
    "StreamReader",
    "StreamWriter",
    "Protocol",
    "BaseProtocol",
    "DatagramProtocol",
    "open_connection",
    "start_server",
    "open_pipe",
    "open_unix_connection",
    # Subprocess
    "Process",
    "create_subprocess_exec",
    "create_subprocess_shell",
    "SubprocessTransport",
    # Signals
    "SignalHandler",
    "signal",
    "set_signal_handler",
    "get_signal_handler",
    # Thread pool
    "ThreadPool",
    "run_in_executor",
    "set_default_executor",
    "get_default_executor",
    "to_thread",
    # Debug
    "EventLoopTracer",
    "trace_loop",
    "EventLoopProfiler",
    "profile_loop",
    "EventLoopMonitor",
    "monitor_loop",
    # Compatibility
    "asyncio_ensure_future",
    "asyncio_gather",
    "asyncio_wait",
    "asyncio_wait_for",
    "asyncio_as_completed",
    "asyncio_sleep",
    "asyncio_run",
    "asyncio_create_task",
    "asyncio_current_task",
    "asyncio_all_tasks",
    "asyncio_Event",
    "asyncio_Lock",
    "asyncio_Condition",
    "asyncio_Semaphore",
    "asyncio_BoundedSemaphore",
    "asyncio_Barrier",
    "asyncio_Queue",
    "asyncio_PriorityQueue",
    "asyncio_LifoQueue",
    "asyncio_StreamReader",
    "asyncio_StreamWriter",
    "asyncio_open_connection",
    "asyncio_start_server",
    "asyncio_create_subprocess_exec",
    "asyncio_create_subprocess_shell",
    "asyncio_run_in_executor",
    "asyncio_to_thread",
    "asyncio_get_running_loop",
    "asyncio_get_event_loop",
    "asyncio_set_event_loop",
    "asyncio_new_event_loop",
    "AbstractEventLoop",
    "AbstractServer",
    "AbstractChildWatcher",
    "FastChildWatcher",
    "SafeChildWatcher",
    "ThreadedChildWatcher",
    "PidfdChildWatcher",
    "SelectorEventLoop",
    "ProactorEventLoop",
]