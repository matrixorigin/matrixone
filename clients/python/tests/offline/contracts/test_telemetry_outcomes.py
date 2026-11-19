# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
import asyncio
import sys
from dataclasses import FrozenInstanceError, asdict
from unittest.mock import Mock

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

from matrixone import Client
from matrixone._telemetry import observe, phase, pool_wait
from matrixone.exceptions import QueryError, SnapshotError


@pytest.fixture
def observed_client():
    client = Client(logger=Mock(), sql_log_mode='off')
    engine = sa.create_engine('sqlite://')
    client._engine = engine
    events = []
    client.set_query_observer(events.append)
    try:
        yield client, events
    finally:
        engine.dispose()


def test_real_query_reports_measured_phases_without_sql_or_values(observed_client):
    client, events = observed_client
    assert client.execute('SELECT ? AS value', ('private-bound-value',)).scalar() == 'private-bound-value'
    assert len(events) == 1
    event = events[0]
    assert event.operation == 'query' and event.outcome == 'success' and event.rows == 1
    assert event.total_seconds >= event.execute_seconds >= 0
    assert event.fetch_seconds >= 0 and event.pool_wait_seconds >= 0
    assert event.conversion_seconds is None and event.commit_seconds is None
    assert len(event.correlation_id) == 32
    assert 'private' not in str(asdict(event)) and 'SELECT' not in str(asdict(event))
    with pytest.raises(FrozenInstanceError):
        event.rows = 42


def test_session_fetch_after_execute_is_not_reported_as_measured(observed_client):
    client, events = observed_client
    with client.session() as session:
        result = session.execute('SELECT 9')
        event = events[-1]
        assert result.scalar() == 9
        assert event.operation == 'session' and event.execute_seconds >= 0
        assert event.fetch_seconds is None and event.pool_wait_seconds is None


def test_observer_failure_or_recursive_query_cannot_change_work(observed_client):
    client, events = observed_client

    def callback(event):
        events.append(event)
        assert client.execute('SELECT 8').scalar() == 8
        raise RuntimeError('observer failed')

    client.set_query_observer(callback)
    assert client.execute('SELECT 7').scalar() == 7
    assert len(events) == 1
    with pytest.raises(QueryError):
        client.execute('SELECT nonexistent_column')
    assert len(events) == 2 and events[-1].outcome == 'error'


def test_disabled_observer_does_not_allocate_ids(observed_client, monkeypatch):
    import matrixone._telemetry as module

    client, events = observed_client
    client.set_query_observer(None)

    def forbidden():
        raise AssertionError('disabled instrumentation allocated an ID')

    monkeypatch.setattr(module.uuid, 'uuid4', forbidden)
    assert client.execute('SELECT 1').scalar() == 1 and events == []


@pytest.mark.asyncio
async def test_async_cancellation_emits_once_and_preserves_outcome():
    events, entered = [], asyncio.Event()

    class Owner:
        _engine = None

        @observe('arrow_upload')
        async def execute(self):
            try:
                pool_wait()
                entered.set()
                await asyncio.Event().wait()
            except asyncio.CancelledError as error:
                error.committed = False
                raise

    owner = Owner()
    owner._query_observer = events.append
    task = asyncio.create_task(owner.execute())
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task
    assert len(events) == 1
    # Older CPython Task.result() can replace CancelledError and drop attached
    # attributes. The observer still receives the original measured outcome.
    if hasattr(caught.value, 'committed'):
        assert caught.value.committed is False
    else:
        assert sys.version_info < (3, 11)
    event = events[0]
    assert event.outcome == 'cancelled' and event.committed is False
    assert event.pool_wait_seconds >= 0 and event.execute_seconds is None


def test_distinct_phases_without_wall_clock_thresholds(monkeypatch):
    import matrixone._telemetry as module

    values = iter(range(100))
    monkeypatch.setattr(module.time, 'perf_counter', lambda: next(values))
    events = []

    class Owner:
        _engine = None

        @observe('query')
        def execute(self):
            for name in ('execute', 'fetch', 'conversion', 'commit'):
                with phase(name):
                    pass
            return 3

    owner = Owner()
    owner._query_observer = events.append
    assert owner.execute() == 3 and len(events) == 1
    event = events[0]
    assert (event.execute_seconds, event.fetch_seconds, event.conversion_seconds, event.commit_seconds) == (1, 1, 1, 1)
    assert event.pool_wait_seconds is None and event.rows == 3


def test_cause_fields_preserve_existing_hierarchy_without_retry_claim():
    original = Exception(1142, 'private server message')
    original.sqlstate = '42000'
    wrapped = OperationalError('private sql', {}, original)
    try:
        try:
            raise QueryError('query rejected', phase='execute', committed=False) from wrapped
        except QueryError as error:
            raise SnapshotError('snapshot lookup failed') from error
    except SnapshotError as error:
        assert isinstance(error.orig, QueryError)
        assert error.errno == 1142 and error.sqlstate == '42000'
        assert error.committed is None and not hasattr(error, 'retryable')


def test_async_observer_is_rejected():
    async def callback(event):
        pass

    with pytest.raises(TypeError, match='synchronous callable'):
        Client().set_query_observer(callback)


@pytest.mark.parametrize('mode', ['off', 'auto', 'simple'])
def test_default_error_logging_does_not_render_driver_values(mode):
    from matrixone.logger import MatrixOneLogger
    from pymysql.err import OperationalError as DriverError

    logger = MatrixOneLogger(sql_log_mode=mode)
    logger._emit = Mock()
    original = DriverError(1062, 'Duplicate private-bound-value')
    error = OperationalError('INSERT private-sql', {'password': 'private-password'}, original)
    try:
        raise QueryError('wrapper private-bound-value') from error
    except QueryError as wrapped:
        logger.log_error(wrapped, context='query')
    text = str(logger._emit.call_args)
    assert '1062' in text
    assert 'private-bound-value' not in text and 'private-password' not in text and 'private-sql' not in text
    assert error.orig is original  # diagnostics do not mutate the original exception


def test_sql_logging_is_opt_in_for_both_clients_and_logger_factories():
    from matrixone import AsyncClient
    from matrixone.logger import MatrixOneLogger, create_default_logger

    for logger in (Client().logger, AsyncClient().logger, MatrixOneLogger(), create_default_logger()):
        assert logger.sql_log_mode == 'off'
        assert logger._format_sql_for_log("CREATE STAGE s CREDENTIALS={'password'='private'}", is_error=True) == ''


def test_sync_analytics_setup_failure_preserves_driver_error_and_known_outcome():
    from matrixone.analytics import _write_transaction

    engine = sa.create_engine('sqlite://')
    try:
        with pytest.raises(OperationalError) as caught:
            with _write_transaction(engine, rows=2):
                pytest.fail('unsupported setup must fail before user statements')
        error = caught.value
        assert error.orig is not None
        assert error.phase == 'setup' and error.rows == 2 and error.committed is False
        assert error.errno is None and error.sqlstate is None  # SQLite supplies neither.
        with engine.connect() as connection:
            assert connection.exec_driver_sql('SELECT 1').scalar() == 1
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_async_analytics_setup_failure_preserves_driver_error_and_releases_connection():
    from sqlalchemy.ext.asyncio import create_async_engine
    from matrixone import AsyncClient
    from matrixone.async_analytics import _write_transaction

    engine = create_async_engine('sqlite+aiosqlite://')
    # Exercise the transaction setup failure owner using real SQLAlchemy errors;
    # SQLite is a control engine here, not an advertised MatrixOne backend.
    client = AsyncClient()
    client._engine = engine
    try:
        with pytest.raises(OperationalError) as caught:
            async with _write_transaction(client.analytics, rows=2):
                pytest.fail('unsupported setup must fail before user statements')
        error = caught.value
        assert error.orig is not None
        assert error.phase == 'setup' and error.rows == 2 and error.committed is False
        assert error.errno is None and error.sqlstate is None
        async with engine.connect() as connection:
            assert (await connection.exec_driver_sql('SELECT 1')).scalar() == 1
    finally:
        await client.disconnect()


def test_raw_driver_evidence_annotation_keeps_exception_identity():
    from pymysql.err import OperationalError as DriverError
    from matrixone.exceptions import annotate_execution_error

    original = DriverError(1142, 'test denial')
    original.sqlstate = '42000'
    failure = OperationalError('test LOAD', {}, original)
    result = annotate_execution_error(failure, phase='load', rows=2, committed=False)
    assert result is failure and result.orig is original
    assert (result.errno, result.sqlstate, result.phase, result.rows, result.committed) == (
        1142,
        '42000',
        'load',
        2,
        False,
    )


@pytest.mark.parametrize(
    'statement', ["ALTER STAGE s CREDENTIALS={'password'='secret-value'}", "CALL p('secret-value')"]
)
def test_simple_log_fallback_contains_only_operation(statement):
    from matrixone.logger import MatrixOneLogger

    logger = MatrixOneLogger(logger=Mock(), sql_log_mode='simple')
    assert logger._format_sql_for_log(statement) == statement.split()[0]
