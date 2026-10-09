"""Native writes must resume after upstream Io statuses."""

import asyncio
import json
import sys
import threading
import time

import pytest
from turso.aio.native import Connection
from turso.lib import PyTursoDatabaseConfig, py_turso_database_open

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native io_uring tests require Linux")

# One table with six index b-trees, two of them partial and two unique. The
# index inserts are what reach the cursor-restore path that yields.
SCHEMA = (
    (
        "CREATE TABLE record ("
        " id INTEGER PRIMARY KEY, event_id BLOB NOT NULL, delivery_id BLOB NOT NULL,"
        " record_index INTEGER NOT NULL, received_at BIGINT NOT NULL,"
        " service_id INTEGER NOT NULL, severity SMALLINT NOT NULL,"
        " body TEXT NOT NULL, trace_id BLOB)"
    ),
    "CREATE INDEX ix_service ON record (service_id)",
    "CREATE INDEX ix_received ON record (received_at)",
    "CREATE INDEX ix_trace ON record (trace_id) WHERE trace_id IS NOT NULL",
    "CREATE INDEX ix_error ON record (id) WHERE severity >= 17",
    "CREATE UNIQUE INDEX uq_event ON record (event_id)",
    "CREATE UNIQUE INDEX uq_delivery ON record (delivery_id, record_index)",
)

ROWS_PER_TXN = 12
# The first yield lands around transaction 209 on this shape. Round up so the
# test keeps its footing if the b-tree splits a little differently.
TRANSACTIONS = 260
# A commit here takes under a millisecond. A second is a stall.
DEADLINE = 1.0


def _open(path: str, vfs: str) -> Connection:
    config = PyTursoDatabaseConfig(path, vfs=vfs, async_io=True)
    database = py_turso_database_open(config)
    connection = Connection(database, database.connect())
    assert connection._connection.completion_fd() is not None
    return connection


def _insert(base: int) -> tuple[str, list]:
    rows, params = [], []
    for index in range(ROWS_PER_TXN):
        number = base + index
        rows.append("(?,?,?,?,?,?,?,?)")
        params.extend(
            [
                number.to_bytes(16, "big"),
                (base // 7).to_bytes(16, "big"),
                index,
                1_755_000_000_000_000_000 + number,
                number % 11,
                17 if number % 2 else 9,
                f"connection refused postgres {number}",
                (number % 5).to_bytes(16, "big"),
            ]
        )
    sql = (
        "INSERT INTO record (event_id, delivery_id, record_index, received_at,"
        " service_id, severity, body, trace_id) VALUES " + ",".join(rows)
    )
    return sql, params


async def _create(path: str) -> None:
    connection = _open(path, "io_uring")
    for sql in ("PRAGMA journal_mode = WAL", "PRAGMA synchronous = FULL", *SCHEMA):
        await connection.execute(sql)
    await connection.commit()
    await connection.close()


@pytest.mark.asyncio
async def test_writes_resume_and_survive_reopen(tmp_path):
    assert threading.active_count() == 1
    path = str(tmp_path / "store.db")
    await _create(path)
    times = await _replay(_open(path, "io_uring"))
    reopened = _open(path, "io_uring")
    try:
        await _verify(reopened)
    finally:
        await reopened.close()
    assert threading.active_count() == 1
    print(
        json.dumps(
            {
                "transactions": len(times),
                "rows": TRANSACTIONS * ROWS_PER_TXN,
                "maximum_transaction_s": max(times),
                "application_threads": 1,
            }
        )
    )


async def _replay(connection):
    times = []
    try:
        for number in range(TRANSACTIONS):
            times.append(await asyncio.wait_for(_write(connection, number), DEADLINE))
    finally:
        for task in tuple(connection._pending):
            task.cancel()
        await asyncio.gather(*connection._pending, return_exceptions=True)
        await connection.close()
    return times


async def _write(connection, number):
    started = time.perf_counter()
    sql, params = _insert(number * ROWS_PER_TXN)
    await connection.execute("BEGIN IMMEDIATE")
    await connection.execute(sql, params)
    await connection.execute("COMMIT")
    return time.perf_counter() - started


async def _verify(connection):
    cursor = await connection.execute("SELECT event_id, received_at, service_id, body FROM record ORDER BY id")
    rows = await cursor.fetchall()
    expected = [
        (n.to_bytes(16, "big"), 1_755_000_000_000_000_000 + n, n % 11, f"connection refused postgres {n}")
        for n in range(TRANSACTIONS * ROWS_PER_TXN)
    ]
    assert rows == expected
