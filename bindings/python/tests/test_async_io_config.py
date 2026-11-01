import pytest
from turso.lib import PyTursoDatabaseConfig, PyTursoStatusCode, py_turso_database_open


def test_yield_is_a_distinct_status_code():
    assert PyTursoStatusCode.Yield != PyTursoStatusCode.Io
    assert PyTursoStatusCode.Yield != PyTursoStatusCode.Row
    assert PyTursoStatusCode.Yield != PyTursoStatusCode.Done


def test_async_io_must_be_passed_by_keyword():
    with pytest.raises(TypeError):
        PyTursoDatabaseConfig(":memory:", None, None, None, True)


def test_memory_database_opens_with_async_io():
    config = PyTursoDatabaseConfig(":memory:", async_io=True)
    database = py_turso_database_open(config)
    connection = database.connect()
    # The memory backend has no completion descriptor.
    assert connection.completion_fd() is None
    # No I/O is queued, so polling succeeds. The memory backend always reports
    # possible pending work.
    assert connection.poll_io() is True
    connection.set_cooperative_yield(True)
    connection.close()


def test_default_config_still_runs_statements_to_completion():
    database = py_turso_database_open(PyTursoDatabaseConfig(":memory:"))
    connection = database.connect()
    statement = connection.prepare_single("SELECT 1")
    assert statement.step() == PyTursoStatusCode.Row
    assert statement.step() == PyTursoStatusCode.Done
    connection.close()
