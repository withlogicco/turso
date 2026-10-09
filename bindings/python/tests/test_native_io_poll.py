"""Caller-driven I/O must recover when no completion can wake it."""

import asyncio
import os
import sys
from dataclasses import dataclass
from unittest.mock import patch

import pytest
from turso.aio.native import Connection
from turso.lib import PyTursoStatusCode

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native io_uring tests require Linux")


@dataclass
class Result:
    status: object
    rows_changed: int = 0


class Statement:
    def __init__(self, waits=1, status=PyTursoStatusCode.Io) -> None:
        self.calls = 0
        self.waits = waits
        self.status = status

    def execute(self) -> Result:
        self.calls += 1
        status = self.status if self.calls <= self.waits else PyTursoStatusCode.Done
        return Result(status, 1)


class NativeConnection:
    def __init__(self, fd: int) -> None:
        self.fd = fd
        self.polls = 0

    def completion_fd(self) -> int:
        return self.fd

    def poll_io(self) -> None:
        self.polls += 1


class SubmittedConnection(NativeConnection):
    def __init__(self, read_fd, write_fd):
        super().__init__(read_fd)
        self.write_fd = write_fd

    def poll_io(self):
        super().poll_io()
        if self.polls == 1:
            os.write(self.write_fd, b"ready")


@pytest.mark.asyncio
async def test_pending_io_is_submitted_before_waiting():
    read_fd, write_fd = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
    native = SubmittedConnection(read_fd, write_fd)
    connection = Connection(None, native)
    try:
        await asyncio.wait_for(connection._wait_io(), 0.25)
    finally:
        os.close(read_fd)
        os.close(write_fd)
    assert native.polls == 2


@pytest.mark.asyncio
async def test_io_without_a_completion_is_polled_and_stepped_again():
    read_fd, write_fd = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
    native = NativeConnection(read_fd)
    connection = Connection(None, native)
    statement = Statement()
    try:
        changed = await asyncio.wait_for(connection.cursor()._execute_dml(statement), 0.25)
    finally:
        os.close(read_fd)
        os.close(write_fd)
    assert changed == 1
    assert statement.calls == 2
    assert native.polls == 2


@pytest.fixture
def quiet_connection():
    read_fd, write_fd = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
    native = NativeConnection(read_fd)
    try:
        yield native
    finally:
        os.close(read_fd)
        os.close(write_fd)


@pytest.mark.asyncio
async def test_empty_ring_resumes_repeated_io_without_parking(quiet_connection):
    connection = Connection(None, quiet_connection)
    statement = Statement(50)
    with (
        patch.object(quiet_connection, "poll_io", return_value=False),
        patch.object(connection._loop, "add_reader") as add,
    ):
        result = await asyncio.wait_for(connection.cursor()._execute_dml(statement), 0.1)
    assert result == 1
    assert statement.calls == 51
    add.assert_not_called()


@pytest.mark.asyncio
async def test_repeated_io_completes_with_bounded_polls(quiet_connection):
    connection = Connection(None, quiet_connection)
    statement = Statement(3)
    result = await asyncio.wait_for(connection.cursor()._execute_dml(statement), 0.25)
    assert result == 1
    assert statement.calls == 4
    assert quiet_connection.polls == 6


@pytest.mark.asyncio
async def test_cancelled_wait_removes_its_reader(quiet_connection):
    connection = Connection(None, quiet_connection)
    loop = asyncio.get_running_loop()
    with patch.object(loop, "remove_reader", wraps=loop.remove_reader) as remove:
        task = asyncio.create_task(connection._wait_io())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    remove.assert_called_once_with(quiet_connection.fd)
    assert quiet_connection.polls == 2


@pytest.mark.asyncio
async def test_repeated_yield_does_not_poll_or_register_a_reader(quiet_connection):
    connection = Connection(None, quiet_connection)
    statement = Statement(50, PyTursoStatusCode.Yield)
    with patch.object(connection._loop, "add_reader") as add:
        result = await asyncio.wait_for(connection.cursor()._execute_dml(statement), 0.1)
    assert result == 1
    assert statement.calls == 51
    assert quiet_connection.polls == 0
    add.assert_not_called()
