import asyncio
import sys

import pytest
import turso
from turso.aio.native import connect
from turso.lib import PyTursoDatabaseConfig, PyTursoEncryptionConfig

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native io_uring tests require Linux")


@pytest.mark.asyncio
async def test_native_phase_d_parameters_and_metadata():
    async with await connect(":memory:") as connection:
        await connection.execute("CREATE TABLE items (name TEXT, value INTEGER)")
        cursor = await connection.execute("INSERT INTO items VALUES (?, ?)", ("one", 1))
        assert cursor.rowcount == 1
        assert cursor.lastrowid == 1

        cursor = await connection.executemany(
            "INSERT INTO items VALUES (:name, :value)",
            ({"name": "two", "value": 2}, {"name": "three", "value": 3}),
        )
        assert cursor.rowcount == 2

        cursor = await connection.execute("SELECT name, value FROM items WHERE value >= ?", (2,))
        assert cursor.description[0][0] == "name"
        assert cursor.rowcount == -1
        assert await cursor.fetchall() == [("two", 2), ("three", 3)]


@pytest.mark.asyncio
async def test_native_phase_d_transactions():
    async with await connect(":memory:") as connection:
        await connection.execute("CREATE TABLE items (value INTEGER)")
        await connection.commit()

        await connection.execute("BEGIN")
        await connection.execute("INSERT INTO items VALUES (?)", (1,))
        await connection.rollback()
        cursor = await connection.execute("SELECT count(*) FROM items")
        assert await cursor.fetchone() == (0,)

        await connection.execute("BEGIN")
        await connection.execute("INSERT INTO items VALUES (?)", (2,))
        await connection.commit()
        cursor = await connection.execute("SELECT count(*) FROM items")
        assert await cursor.fetchone() == (1,)


@pytest.mark.asyncio
async def test_native_phase_d_serializes_and_closes():
    async with await connect(":memory:") as connection:
        tasks = [asyncio.create_task(connection.execute("SELECT ?", (value,))) for value in range(4)]
        cursors = await asyncio.gather(*tasks)
        assert [await cursor.fetchone() for cursor in cursors] == [
            (0,),
            (1,),
            (2,),
            (3,),
        ]
        await connection.close()
        await connection.close()
        cursor = connection.cursor()
        with pytest.raises(Exception, match="closed"):
            await cursor.execute("SELECT 1")


@pytest.mark.asyncio
async def test_native_phase_d_cancellation_drains_operation():
    async with await connect(":memory:") as connection:
        cursor = connection.cursor()
        started = asyncio.Event()
        release = asyncio.Event()
        original = cursor._select

        async def slow_select(statement):
            started.set()
            await release.wait()
            return await original(statement)

        cursor._select = slow_select
        task = asyncio.create_task(cursor.execute("SELECT 1"))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert connection._pending
        release.set()
        await cursor.close()
        await connection.close()


@pytest.mark.asyncio
async def test_native_phase_d_values_and_bounded_fetchmany():
    async with await connect(":memory:") as connection:
        await connection.execute("CREATE TABLE items (value)")
        await connection.executemany("INSERT INTO items VALUES (?)", [(None,), (b"blob",), (7,), ("text",)])
        cursor = await connection.execute("SELECT value FROM items ORDER BY rowid")
        assert await cursor.fetchmany(2) == [(None,), (b"blob",)]
        assert await cursor.fetchmany(2) == [(7,), ("text",)]
        assert await cursor.fetchmany(2) == []


@pytest.mark.asyncio
async def test_native_phase_d_commit_and_rollback_visibility():
    async with await connect(":memory:") as connection:
        await connection.execute("CREATE TABLE items (value INTEGER)")
        await connection.execute("BEGIN")
        await connection.execute("INSERT INTO items VALUES (?)", (1,))
        await connection.rollback()
        cursor = await connection.execute("SELECT value FROM items")
        assert await cursor.fetchall() == []
        await connection.execute("BEGIN")
        await connection.execute("INSERT INTO items VALUES (?)", (2,))
        await connection.commit()
        cursor = await connection.execute("SELECT value FROM items")
        assert await cursor.fetchall() == [(2,)]


@pytest.mark.asyncio
async def test_native_phase_d_concurrent_writes_are_serialized():
    async with await connect(":memory:") as connection:
        await connection.execute("CREATE TABLE items (value INTEGER)")
        await asyncio.gather(*(connection.execute("INSERT INTO items VALUES (?)", (value,)) for value in range(20)))
        cursor = await connection.execute("SELECT count(*) FROM items")
        assert await cursor.fetchone() == (20,)


@pytest.mark.asyncio
async def test_native_phase_d_errors_are_translated():
    async with await connect(":memory:") as connection:
        with pytest.raises(turso.DatabaseError):
            await connection.execute("SELECT missing FROM nowhere")
        await connection.execute("CREATE TABLE items (value INTEGER UNIQUE)")
        await connection.execute("INSERT INTO items VALUES (?)", (1,))
        with pytest.raises(turso.IntegrityError):
            await connection.execute("INSERT INTO items VALUES (?)", (1,))


@pytest.mark.asyncio
async def test_native_phase_d_cancellation_before_submission_and_after_completion():
    async with await connect(":memory:") as connection:
        cursor = connection.cursor()
        task = asyncio.create_task(cursor.execute("SELECT 1"))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        completed = await cursor.execute("SELECT 2")
        task = asyncio.create_task(completed.fetchone())
        assert await task == (2,)
        task.cancel()
        await cursor.close()
        await cursor.close()


def test_database_config_preserves_positional_encryption():
    encryption = PyTursoEncryptionConfig("aegis256", "00" * 32)
    PyTursoDatabaseConfig(":memory:", None, "memory", encryption)
    PyTursoDatabaseConfig(":memory:", None, "memory", None, async_io=True)
