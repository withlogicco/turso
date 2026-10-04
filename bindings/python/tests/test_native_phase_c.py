"""Phase C acceptance tests for caller-driven native asyncio I/O."""

import asyncio
import os
import shutil
import sys
import threading
from unittest.mock import patch

import pytest
import turso
from turso.aio.native import connect

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native io_uring tests require Linux")

SQL = "SELECT name, value FROM items ORDER BY value"


def _threads() -> tuple[set[int], dict[str, str]]:
    python = {thread.ident for thread in threading.enumerate() if thread.ident}
    kernel = {task: _task_name(task) for task in os.listdir("/proc/self/task")}
    return python, kernel


def _task_name(task: str) -> str:
    with open(f"/proc/self/task/{task}/comm") as stream:
        return stream.read().strip()


def _assert_same(before: tuple[set[int], dict[str, str]]) -> None:
    python, kernel = _threads()
    assert python == before[0]
    added = {name for task, name in kernel.items() if task not in before[1]}
    assert all(name.startswith(("iou-sqp", "iou-wrk")) for name in added)


def _make_db(path: str) -> list[tuple[str, int]]:
    rows = [(f"item-{index}", index) for index in range(4096)]
    with turso.connect(path, vfs="io_uring", isolation_level=None) as db:
        db.execute("CREATE TABLE items (name TEXT, value INTEGER)")
        db.executemany("INSERT INTO items VALUES (?, ?)", rows)
        db.commit()
    return rows


def _copy_db(source: str, target: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        source_file = source + suffix
        if os.path.exists(source_file):
            shutil.copy2(source_file, target + suffix)


async def _run_native(path: str) -> tuple[list[tuple[object, ...]], object, int]:
    before = _threads()
    connection = await connect(path, vfs="io_uring")
    assert connection._connection.completion_fd() is not None
    assert connection._connection.poll_io() is False
    _assert_same(before)
    cursor = await _query(connection)
    rows = await cursor.fetchall()
    result = rows, cursor.description, cursor.rowcount
    _assert_same(before)
    await cursor.close()
    await connection.close()
    _assert_same(before)
    return result


async def _query(connection):
    pending = asyncio.Event()
    progress: list[int] = []
    original_wait = connection._wait_io

    async def wait_io() -> None:
        pending.set()
        await original_wait()

    connection._wait_io = wait_io
    query = asyncio.create_task(connection.execute(SQL))
    ticker = asyncio.create_task(_tick(progress, query))
    await asyncio.wait_for(pending.wait(), 5)
    assert progress
    cursor = await query
    await ticker
    return cursor


async def _tick(progress: list[int], query: asyncio.Task[object]) -> None:
    while not query.done():
        progress.append(len(progress))
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_native_phase_c_pending_io_fairness_and_census(tmp_path):
    path = str(tmp_path / "phase-c.db")
    expected = _make_db(path)
    sync_path = str(tmp_path / "sync.db")
    _copy_db(path, sync_path)
    loop = asyncio.get_running_loop()
    with (
        patch.object(loop, "run_in_executor", side_effect=AssertionError),
        patch.object(asyncio, "to_thread", side_effect=AssertionError),
    ):
        rows, description, rowcount = await _run_native(path)
    with turso.connect(sync_path, vfs="io_uring") as db:
        sync_cursor = db.execute(SQL)
        sync_rows = sync_cursor.fetchall()
        sync_description = sync_cursor.description
        sync_rowcount = sync_cursor.rowcount
    assert sync_rows == expected == rows
    assert description == sync_description
    assert rowcount == sync_rowcount == -1
