"""Caller-driven native asyncio DB-API wrapper."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Coroutine, Iterable, Mapping, Sequence
from types import TracebackType
from typing import Any, ClassVar

from typing_extensions import Self

from ..lib import (
    Busy,
    Constraint,
    DatabaseError,
    IntegrityError,
    OperationalError,
    ProgrammingError,
    PyTursoDatabaseConfig,
    PyTursoStatusCode,
    TursoError,
    py_turso_database_open,
)

Params = Sequence[Any] | Mapping[str, Any]
FETCH_CHUNK = 200
IO_POLL_INTERVAL = 0.02


def _set_ready(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


def _drain(fd: int) -> None:
    try:
        os.read(fd, 8)
    except BlockingIOError:
        pass


class Connection:
    """Own a native Turso connection on one asyncio event loop."""

    capabilities: ClassVar[dict[str, bool]] = {
        "caller_driven_async": True,
        "uses_python_worker": False,
        "supports_cancellation": False,
    }

    def __init__(self, database: Any, connection: Any) -> None:
        enable = getattr(connection, "set_cooperative_yield", None)
        if enable is not None:
            enable(True)
        self._database = database
        self._connection = connection
        self._loop = asyncio.get_running_loop()
        self._lock = asyncio.Lock()
        self._pending: set[asyncio.Task[Any]] = set()
        self._cursors: set[Cursor] = set()
        self._closed = False

    def cursor(self) -> Cursor:
        cursor = Cursor(self)
        self._cursors.add(cursor)
        return cursor

    async def execute(self, sql: str, params: Params = ()) -> Cursor:
        cursor = self.cursor()
        return await cursor.execute(sql, params)

    async def executemany(self, sql: str, params: Iterable[Params]) -> Cursor:
        cursor = self.cursor()
        return await cursor.executemany(sql, params)

    async def commit(self) -> None:
        if self._connection.get_auto_commit():
            return
        await self._control("COMMIT")

    async def rollback(self) -> None:
        if self._connection.get_auto_commit():
            return
        await self._control("ROLLBACK")

    async def _control(self, sql: str) -> None:
        cursor = self.cursor()
        try:
            await cursor.execute(sql)
        finally:
            await cursor.close()

    def _track(self, operation: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        task = self._loop.create_task(operation)
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)
        return task

    async def _wait_io(self) -> None:
        fd = self._connection.completion_fd()
        if fd is None:
            raise RuntimeError("native driver has no completion descriptor")
        if self._connection.poll_io() is False:
            await asyncio.sleep(0)
            return
        await self._wait_fd(fd)

    async def _wait_fd(self, fd: int) -> None:
        future = self._loop.create_future()
        self._loop.add_reader(fd, _set_ready, future)
        try:
            try:
                await asyncio.wait_for(future, IO_POLL_INTERVAL)
            except asyncio.TimeoutError:
                pass
        finally:
            self._loop.remove_reader(fd)
            _drain(fd)
            self._connection.poll_io()

    async def close(self) -> None:
        if self._closed:
            return
        pending = tuple(task for task in self._pending if not task.done())
        if pending:
            if asyncio.get_running_loop() is not self._loop:
                raise RuntimeError("native connection has pending work on another loop")
            await asyncio.gather(*pending, return_exceptions=True)
        self._closed = True
        for cursor in self._cursors:
            cursor._closed = True
        self._cursors.clear()
        self._connection.close()
        self._connection = None
        self._database = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


class Cursor:
    """Native cursor with buffered rows and serialized operations."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount = -1
        self.lastrowid: int | None = None
        self._rows: list[tuple[Any, ...]] = []
        self._pos = 0
        self._statement: Any | None = None
        self._done = True
        self._operation: asyncio.Task[Any] | None = None
        self._closed = False

    async def _run(self, operation: Coroutine[Any, Any, Any]) -> Any:
        self._ensure_open()
        if self._operation is not None:
            await asyncio.shield(self._operation)
        task = self.connection._track(operation)
        self._operation = task
        task.add_done_callback(self._clear_operation)
        try:
            return await asyncio.shield(task)
        except Exception as exc:
            raise _map_error(exc) from exc

    def _clear_operation(self, task: asyncio.Task[Any]) -> None:
        if self._operation is task:
            self._operation = None

    async def execute(self, sql: str, params: Params = ()) -> Cursor:
        self._ensure_open()
        result = await self._run(self._execute(sql, params))
        self._rows, self.description, self.rowcount, self.lastrowid, self._statement = result
        self._done = self._statement is None
        self._pos = 0
        return self

    async def executemany(self, sql: str, params: Iterable[Params]) -> Cursor:
        total = 0
        lastrowid = self.lastrowid
        for item in params:
            await self.execute(sql, item)
            total += max(self.rowcount, 0)
            lastrowid = self.lastrowid
        self.description = None
        self.rowcount = total
        self.lastrowid = lastrowid
        return self

    async def _execute(self, sql: str, params: Params) -> Any:
        async with self.connection._lock:
            self._ensure_open()
            statement = self.connection._connection.prepare_single(sql)
            self._bind(statement, params)
            columns = tuple(statement.columns())
            if columns:
                rows, done = await self._select(statement)
                return rows, self._description(columns), -1, None, None if done else statement
            changed = await self._execute_dml(statement)
            lastrowid = self.connection._connection.last_insert_rowid()
            return [], None, changed, lastrowid if _is_insert(sql) else None, None

    async def _select(self, statement: Any) -> tuple[list[tuple[Any, ...]], bool]:
        return await self._step(statement, FETCH_CHUNK)

    async def _step(self, statement: Any, limit: int) -> tuple[list[tuple[Any, ...]], bool]:
        rows = []
        while len(rows) < limit:
            status = statement.step()
            match status:
                case PyTursoStatusCode.Row:
                    rows.append(tuple(statement.row()))
                case PyTursoStatusCode.Done:
                    return rows, True
                case PyTursoStatusCode.Yield:
                    await asyncio.sleep(0)
                case PyTursoStatusCode.Io:
                    await self.connection._wait_io()
                case _:
                    raise RuntimeError(f"unexpected native status: {status}")
        return rows, False

    async def _execute_dml(self, statement: Any) -> int:
        while True:
            result = statement.execute()
            match result.status:
                case PyTursoStatusCode.Done:
                    return int(result.rows_changed)
                case PyTursoStatusCode.Yield:
                    await asyncio.sleep(0)
                case PyTursoStatusCode.Io:
                    await self.connection._wait_io()
                case _:
                    raise RuntimeError(f"unexpected native status: {result.status}")

    @staticmethod
    def _description(columns: Sequence[str]) -> tuple[tuple[Any, ...], ...]:
        return tuple((name, None, None, None, None, None, None) for name in columns)

    @staticmethod
    def _bind(statement: Any, params: Params) -> None:
        if isinstance(params, Mapping):
            for name, value in params.items():
                position = _named_position(statement, str(name))
                statement.bind_positional(position, value)
            return
        if params:
            statement.bind(tuple(params))

    async def fetchone(self) -> tuple[Any, ...] | None:
        rows = await self.fetchmany(1)
        return rows[0] if rows else None

    async def fetchmany(self, size: int = 1) -> list[tuple[Any, ...]]:
        self._ensure_open()
        if size < 0:
            raise ValueError("fetchmany size must not be negative")
        rows: list[tuple[Any, ...]] = []
        while len(rows) < size:
            rows.extend(await self._take(size - len(rows)))
            if len(rows) >= size or self._done:
                return rows
            await self._run(self._fetch_more(min(size - len(rows), FETCH_CHUNK)))
        return rows

    async def fetchall(self) -> list[tuple[Any, ...]]:
        rows = []
        while not self._done or self._pos < len(self._rows):
            rows.extend(await self.fetchmany(FETCH_CHUNK))
        return rows

    async def _take(self, size: int) -> list[tuple[Any, ...]]:
        async with self.connection._lock:
            end = min(self._pos + size, len(self._rows))
            rows = self._rows[self._pos : end]
            self._pos = end
            return rows

    async def _fetch_more(self, size: int) -> None:
        if self._statement is None:
            self._done = True
            return
        rows, self._done = await self._step(self._statement, size)
        self._rows = rows
        self._pos = 0

    async def close(self) -> None:
        if self._closed:
            return
        if self._operation is not None:
            await asyncio.shield(self._operation)
        self._statement = None
        self._done = True
        self._closed = True
        self.connection._cursors.discard(self)

    def _ensure_open(self) -> None:
        if self._closed or self.connection._closed:
            raise ProgrammingError("Cannot operate on a closed cursor")

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


def _named_position(statement: Any, name: str) -> int:
    for prefix in ("", ":", "@", "$", "?"):
        try:
            return statement.named_position(f"{prefix}{name}")
        except Exception:  # noqa: BLE001, S112
            continue
    raise ProgrammingError(f"unknown parameter: {name}")


def _is_insert(sql: str) -> bool:
    return sql.lstrip().split(None, 1)[0].upper() in {"INSERT", "REPLACE"}


def _map_error(error: Exception) -> Exception:
    if isinstance(error, (DatabaseError, ProgrammingError)):
        return error
    if isinstance(error, Constraint):
        return IntegrityError(str(error))
    if isinstance(error, Busy):
        return OperationalError(str(error))
    if isinstance(error, TursoError):
        return DatabaseError(str(error))
    return error


async def connect(path: str, *, vfs: str | None = None, storage: str | None = None) -> Connection:
    vfs = vfs or ("memory" if path == ":memory:" else "io_uring")
    if storage is not None:
        raise ProgrammingError("vendor page storage is unsupported")
    config = PyTursoDatabaseConfig(path, vfs=vfs, async_io=True)
    database = py_turso_database_open(config)
    return Connection(database, database.connect())
