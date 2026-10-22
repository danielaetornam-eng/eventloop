"""
Tests for the higher level features: nursery, signals, unix sockets,
subprocesses and TCP servers.
"""

import os
import signal
import socket
import sys
import tempfile

import pytest

from eventloop import (
    Nursery,
    create_task,
    current_task,
    gather,
    get_signal_handler,
    new_event_loop,
    run,
    set_signal_handler,
    sleep,
)
from eventloop.streams import (
    open_connection,
    open_unix_connection,
    start_server,
    start_unix_server,
)
from eventloop.subprocess import create_subprocess_exec


class TestNursery:
    """Test the Nursery (trio-style) structured concurrency helper."""

    def test_nursery_awaits_children(self):
        async def test():
            results: list[int] = []

            async def child(n: int) -> None:
                await sleep(0.01)
                results.append(n)

            async with Nursery() as nursery:
                nursery.start_soon(child, 1)
                nursery.start_soon(child, 2)

            return results

        results = run(test())
        assert sorted(results) == [1, 2]

    def test_nursery_cancels_siblings_on_failure(self):
        async def test():
            async def boom() -> None:
                await sleep(0.01)
                raise ValueError("boom")

            async def forever() -> None:
                await sleep(10)

            async with Nursery() as nursery:
                nursery.start_soon(boom)
                nursery.start_soon(forever)

        with pytest.raises(ValueError, match="boom"):
            run(test())

    def test_nursery_cancel(self):
        async def test():
            done: list[str] = []

            async def child() -> None:
                try:
                    await sleep(10)
                except BaseException:
                    done.append("cancelled")
                    raise

            async with Nursery() as nursery:
                nursery.start_soon(child)
                await sleep(0.01)
                nursery.cancel()

            return done

        assert run(test()) == ["cancelled"]


class TestSignals:
    """Test signal handling."""

    def test_set_and_get_signal_handler(self):
        async def test():
            received: list[int] = []

            def handler(signum, frame):
                received.append(signum)

            set_signal_handler(signal.SIGUSR1, handler)
            try:
                assert get_signal_handler(signal.SIGUSR1) is handler
                os.kill(os.getpid(), signal.SIGUSR1)
                await sleep(0.05)
            finally:
                set_signal_handler(signal.SIGUSR1, None)

            return received

        # Signal delivery is not supported on every platform; on Windows
        # SIGUSR1 does not exist, so the test is skipped there.
        if not hasattr(signal, "SIGUSR1"):
            pytest.skip("SIGUSR1 is not available on this platform")
        assert run(test()) == [signal.SIGUSR1]

    def test_get_signal_handler_returns_none_when_unset(self):
        async def test():
            return get_signal_handler(signal.SIGINT)

        assert run(test()) is None


class TestUnixSockets:
    """Test Unix domain socket support."""

    def test_unix_connection_roundtrip(self):
        if not hasattr(socket, "AF_UNIX"):
            pytest.skip("Unix sockets are not available on this platform")

        async def handle(reader, writer):
            data = await reader.read(1024)
            writer.write(b"echo:" + data)
            await writer.drain()
            writer.close()

        async def scenario():
            with tempfile.TemporaryDirectory() as tmpdir:
                path = os.path.join(tmpdir, "sock")
                server = await start_unix_server(handle, path)
                try:
                    reader, writer = await open_unix_connection(path)
                    writer.write(b"hello")
                    await writer.drain()
                    reply = await reader.read(1024)
                    writer.close()
                    return reply
                finally:
                    server.close()
                    await server.wait_closed()

        assert run(scenario()) == b"echo:hello"


class TestTcpServer:
    """Test the TCP server and client."""

    def test_echo_server(self):
        async def handle(reader, writer):
            try:
                while True:
                    data = await reader.read(1024)
                    if not data:
                        break
                    writer.write(data)
                    await writer.drain()
            finally:
                writer.close()

        async def scenario():
            server = await start_server(handle, "127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            try:
                reader, writer = await open_connection("127.0.0.1", port)
                writer.write(b"ping")
                await writer.drain()
                reply = await reader.read(4)
                writer.close()
                return reply
            finally:
                server.close()
                await server.wait_closed()

        assert run(scenario()) == b"ping"

    def test_multiple_concurrent_clients(self):
        async def handle(reader, writer):
            data = await reader.read(1024)
            writer.write(data)
            await writer.drain()
            writer.close()

        async def scenario():
            server = await start_server(handle, "127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            try:
                async def one(n: int) -> bytes:
                    reader, writer = await open_connection("127.0.0.1", port)
                    writer.write(str(n).encode())
                    await writer.drain()
                    reply = await reader.read(16)
                    writer.close()
                    return reply

                results = await gather(*[one(i) for i in range(10)])
                return results
            finally:
                server.close()
                await server.wait_closed()

        results = run(scenario())
        assert results == [str(i).encode() for i in range(10)]


class TestSubprocess:
    """Test the subprocess support."""

    def test_exec_captures_stdout(self):
        async def scenario():
            proc = await create_subprocess_exec(
                sys.executable,
                "-c",
                "print('hello from subprocess')",
                stdout=-1,
                stderr=-1,
            )
            stdout, stderr = await proc.communicate()
            return proc.returncode, stdout, stderr

        code, stdout, stderr = run(scenario())
        assert code == 0
        assert b"hello from subprocess" in stdout

    def test_exec_reports_failure(self):
        async def scenario():
            proc = await create_subprocess_exec(
                sys.executable,
                "-c",
                "import sys; sys.exit(3)",
                stdout=-1,
                stderr=-1,
            )
            await proc.communicate()
            return proc.returncode

        assert run(scenario()) == 3

    def test_exec_with_stdin(self):
        async def scenario():
            proc = await create_subprocess_exec(
                sys.executable,
                "-c",
                "import sys; data = sys.stdin.read(); sys.stdout.write(data.upper())",
                stdin=-1,
                stdout=-1,
                stderr=-1,
            )
            stdout, _stderr = await proc.communicate(b"hello")
            return stdout

        assert run(scenario()) == b"HELLO"


class TestTaskParenting:
    """Test that tasks are parented to the current task."""

    def test_child_is_parented(self):
        async def test():
            async def child() -> None:
                await sleep(0.01)

            task = create_task(child())
            parent = current_task()
            assert task.parent is parent
            await task

        run(test())
