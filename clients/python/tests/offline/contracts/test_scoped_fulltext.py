"""Setting restoration failures must not return contaminated pool connections."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from matrixone import _scoped_query
from matrixone.exceptions import QueryError


@pytest.mark.parametrize('primary', [None, ValueError('query failed'), asyncio.CancelledError()])
def test_fulltext_restore_failure_discards_and_preserves_primary(monkeypatch, primary):
    owner = MagicMock()
    connection = MagicMock()
    owner._engine.connect.return_value.__enter__.return_value = connection
    saved = Mock()
    saved.scalar.return_value = 'TF-IDF'
    restore = RuntimeError('restore failed')
    connection.exec_driver_sql.side_effect = [saved, Mock(), restore]
    result = Mock()
    result.keys.return_value = ['score']
    result.fetchall.return_value = [(1,)]
    owner._execute_with_logging.return_value = result
    owner._execute_with_logging.side_effect = primary
    # Client has no parent session owner.
    owner.client = None
    discard = Mock()
    monkeypatch.setattr(_scoped_query, 'discard_unrestored_connection', discard)
    with pytest.raises(type(primary) if primary is not None else QueryError) as caught:
        _scoped_query.execute_fulltext(owner, 'SELECT score', 'BM25')
    if primary is not None:
        assert caught.value is primary
    else:
        assert caught.value.__cause__ is restore
    discard.assert_called_once_with(connection)
    saved.close.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('primary', [None, ValueError('query failed'), asyncio.CancelledError()])
async def test_async_fulltext_restore_failure_discards_and_preserves_primary(primary):
    owner = MagicMock(client=None)
    connection = MagicMock()
    owner._engine.connect.return_value.__aenter__.return_value = connection
    saved = Mock()
    saved.scalar.return_value = 'TF-IDF'
    restore = RuntimeError('restore failed')
    connection.exec_driver_sql = AsyncMock(side_effect=[saved, Mock(), restore])
    connection.run_sync = AsyncMock()
    result = Mock()
    result.keys.return_value = ['score']
    result.fetchall.return_value = [(1,)]
    owner._execute_with_logging = AsyncMock(return_value=result, side_effect=primary)
    with pytest.raises(type(primary) if primary is not None else QueryError) as caught:
        await _scoped_query.execute_fulltext_async(owner, 'SELECT score', 'BM25')
    if primary is not None:
        assert caught.value is primary
    else:
        assert caught.value.__cause__ is restore
    connection.run_sync.assert_awaited_once_with(_scoped_query.discard_unrestored_connection)
    saved.close.assert_called_once()
