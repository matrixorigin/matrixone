"""Separate an acknowledged rejection from uncertain COMMIT outcomes."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pymysql.err import IntegrityError as DriverIntegrityError, OperationalError as DriverOperationalError
from sqlalchemy.exc import IntegrityError, OperationalError
from matrixone import analytics, async_analytics
from matrixone.analytics import AnalyticsCommitRejectedError, AnalyticsCommitUnknownError


def failure(kind):
    if kind == 'cancel':
        return asyncio.CancelledError()
    if kind == 'unclassified':
        return RuntimeError('unknown commit failure')
    if kind == 'transport':
        return OperationalError(None, None, DriverOperationalError(2013, 'lost connection'))
    if kind == 'wrong_wrapper':
        return OperationalError(None, None, DriverIntegrityError(1062, 'duplicate'))
    if kind == 'wrong_driver':
        return IntegrityError(None, None, ValueError(1062, 'not a server reply'))
    code = 1452 if kind == 'other_integrity' else 1062
    return IntegrityError(
        None, None, DriverIntegrityError(code, 'server rejection'), connection_invalidated=kind == 'invalidated'
    )


class AsyncTransaction:
    """SQLAlchemy begin() is both awaitable and an async context manager."""

    def __init__(self, error):
        self.commit = AsyncMock(side_effect=error)
        self.rollback = AsyncMock()
        self.is_active = True

    def __await__(self):
        return self.__aenter__().__await__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize(
    'kind',
    [
        'duplicate',
        'invalidated',
        'other_integrity',
        'transport',
        'wrong_wrapper',
        'wrong_driver',
        'unclassified',
        'cancel',
    ],
)
@pytest.mark.parametrize('cleanup_failure', [False, True])
async def test_commit_outcome_and_connection_discard(monkeypatch, asynchronous, kind, cleanup_failure):
    original = failure(kind)
    cleanup = RuntimeError('connection invalidation failed') if cleanup_failure else None
    connection = MagicMock(invalidated=False)
    result = Mock()
    result.one.return_value = ('+00:00', 0, '', 0)
    if asynchronous:
        transaction = AsyncTransaction(original)
        connection.begin.return_value = transaction
        connection.exec_driver_sql = AsyncMock(return_value=result)
        connection.invalidate = AsyncMock(side_effect=cleanup)
        connection.close = AsyncMock()
        manager = async_analytics.AsyncAnalyticsManager(
            SimpleNamespace(_engine=SimpleNamespace(connect=AsyncMock(return_value=connection)))
        )
        context = async_analytics._write_transaction(manager, rows=2)
    else:
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value = connection
        transaction = connection.begin.return_value
        transaction.commit.side_effect = original
        connection.exec_driver_sql.return_value = result
        discard = Mock(side_effect=cleanup)
        monkeypatch.setattr('matrixone._utils.discard_unrestored_connection', discard)
        context = analytics._write_transaction(engine, rows=2)
    expected = (
        asyncio.CancelledError
        if kind == 'cancel'
        else AnalyticsCommitRejectedError if kind == 'duplicate' else AnalyticsCommitUnknownError
    )
    with pytest.raises(expected) as caught:
        if asynchronous:
            async with context:
                pass
        else:
            with context:
                pass
    error = caught.value
    assert error.committed is (False if kind == 'duplicate' else None)
    assert error.phase == 'commit' and error.rows == 2
    if kind == 'cancel':
        assert error is original
    else:
        assert error.__cause__ is original
        assert error.errno == (getattr(original, 'orig', SimpleNamespace(args=())).args or (None,))[0]
    if asynchronous:
        transaction.commit.assert_awaited_once()
        transaction.rollback.assert_not_awaited()
        connection.invalidate.assert_awaited_once()
        connection.close.assert_awaited_once()
        connection.sync_connection._dbapi_connection.driver_connection.close.assert_called()
        if cleanup_failure:
            assert error.cleanup_error is cleanup
    else:
        transaction.commit.assert_called_once()
        transaction.rollback.assert_not_called()
        discard.assert_called_once_with(connection)
        if cleanup_failure:
            assert error.invalidation_error is cleanup
    sql = [call.args[0] for call in connection.exec_driver_sql.call_args_list]
    assert len(sql) == 4  # One read, three SETs: no probes, retries or rollback after COMMIT.
    assert not any('autocommit = 1' in statement for statement in sql)


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('kind', ['duplicate', 'transport'])
async def test_public_file_import_preserves_commit_outcome(monkeypatch, asynchronous, kind):
    original = failure(kind)
    connection = MagicMock(invalidated=False)
    result = Mock()
    result.one.return_value = ('+00:00', 0, '', 0)
    if asynchronous:
        engine = SimpleNamespace(connect=AsyncMock(return_value=connection))
        manager = async_analytics.AsyncAnalyticsManager(SimpleNamespace(_engine=engine, query_timeout=5))
        connection.engine = engine
        connection.begin.return_value = AsyncTransaction(original)
        connection.exec_driver_sql = AsyncMock(return_value=result)
        connection.invalidate = AsyncMock()
        connection.close = AsyncMock()
        relation = object.__new__(async_analytics.AsyncFileRelation)
        relation._manager = manager
        relation._sources = AsyncMock()
        relation._server_sources = AsyncMock()
    else:
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value = connection
        connection.begin.return_value.commit.side_effect = original
        connection.exec_driver_sql.return_value = result
        monkeypatch.setattr('matrixone._utils.discard_unrestored_connection', Mock())
        relation = object.__new__(analytics.FileRelation)
        relation._manager = SimpleNamespace(client=SimpleNamespace())
        relation._validate_sources = Mock()
        relation._validate_server_sources = Mock()
    relation._engine = engine
    relation._closed = False
    relation.schema = SimpleNamespace(names=['id'])
    relation._sql = 'owned_files'
    expected = AnalyticsCommitRejectedError if kind == 'duplicate' else AnalyticsCommitUnknownError
    with pytest.raises(expected) as caught:
        operation = relation.import_into('target')
        if asynchronous:
            await operation
    assert caught.value.__cause__ is original
    assert caught.value.committed is (False if kind == 'duplicate' else None)
    assert caught.value.phase == 'commit' and caught.value.rows is None
