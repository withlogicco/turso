import asyncio
import sys
from collections import Counter
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from turso.aio.native import Cursor
from turso.lib import PyTursoStatusCode

from .test_native_yield import _create, _open, _replay, _verify

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native io_uring tests require Linux")


class ObservedStatement:
    def __init__(self, statement, counts, merged):
        self.statement = statement
        self.counts = counts
        self.merged = merged

    def execute(self):
        result = self.statement.execute()
        self.counts[str(result.status)] += 1
        if self.merged and result.status == PyTursoStatusCode.Yield:
            return SimpleNamespace(status=PyTursoStatusCode.Io, rows_changed=result.rows_changed)
        return result


@pytest.mark.asyncio
@pytest.mark.parametrize("merged, opted_in", [(False, True), (True, True), (False, False)])
async def test_real_yield_survives_cooperative_progress(tmp_path, merged, opted_in):
    path = str(tmp_path / "status.db")
    await _create(path)
    counts = Counter()
    original = Cursor._execute_dml

    async def observe(cursor, statement):
        return await original(cursor, ObservedStatement(statement, counts, merged))

    connection = _open(path, "io_uring")
    connection._connection.set_cooperative_yield(opted_in)
    with patch.object(Cursor, "_execute_dml", observe):
        await _replay(connection)
    assert bool(counts[str(PyTursoStatusCode.Yield)]) == opted_in
    await _reopen(path)
    print({"merged_into_io": merged, "opted_in": opted_in, "statuses": dict(counts)})


async def _reopen(path):
    connection = _open(path, "io_uring")
    try:
        await asyncio.wait_for(_verify(connection), 1)
    finally:
        await connection.close()
