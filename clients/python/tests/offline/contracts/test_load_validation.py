"""Unsupported LOAD inputs fail before sending SQL."""

from unittest.mock import AsyncMock, Mock

import pytest
from matrixone.load_data import LoadDataManager, AsyncLoadDataManager


@pytest.mark.parametrize(
    'options',
    [
        {'compression': 'gzip'},
        {'parallel': True},
        {'encoding': 'utf8'},
        {'set_clause': {'id': '1'}},
        {'unknown': True},
        {'skiprows': -1},
        {'skiprows': True},
        {'escapechar': '\\'},
        {'names': []},
        {'ignore_lines': 1, 'skiprows': 1},
    ],
)
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.asyncio
async def test_inline_rejects_unsupported_options_before_io(options, asynchronous):
    client = Mock()
    if asynchronous:
        client.execute = AsyncMock()
        manager = AsyncLoadDataManager(client)
        with pytest.raises((ValueError, TypeError)):
            await manager.read_csv_inline('1,a\n', 't', **options)
    else:
        manager = LoadDataManager(client)
        with pytest.raises((ValueError, TypeError)):
            manager.read_csv_inline('1,a\n', 't', **options)
    client.execute.assert_not_called()


@pytest.mark.parametrize('data', ['1,back\\slash\n', '1,nul\0\n'])
def test_inline_rejects_mode_dependent_string_tokens(data):
    client = Mock()
    with pytest.raises(ValueError):
        LoadDataManager(client).read_csv_inline(data, 't')
    client.execute.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
async def test_wrappers_reject_conflicting_native_options_before_dispatch(asynchronous):
    client = Mock()
    client.execute = AsyncMock() if asynchronous else Mock()
    manager = AsyncLoadDataManager(client) if asynchronous else LoadDataManager(client)
    for method, kwargs, message in [
        ('read_csv', {'fields_terminated_by': '|'}, 'sep'),
        ('read_json', {'jsondata': 'object'}, 'orient'),
        ('read_parquet', {'format': 'csv'}, 'read_parquet'),
    ]:
        with pytest.raises(ValueError, match=message):
            result = getattr(manager, method)('file', 't', **kwargs)
            if asynchronous:
                await result
    client.execute.assert_not_called()


@pytest.mark.parametrize(
    'expression', ["'manual'", 'NOW()', 'NULLIF(other, 0)', "NULLIF(id, 'x') OR 1=1", "NULLIF(@id, 'x')"]
)
def test_load_set_rejects_unsupported_expressions(expression):
    client = Mock()
    with pytest.raises(ValueError, match='NULLIF'):
        LoadDataManager(client)._build_load_data_sql('file', 't', set_clause={'id': expression})
    client.execute.assert_not_called()


def test_load_set_accepts_target_and_literal():
    manager = LoadDataManager(Mock())
    sql = manager._build_load_data_sql('file', 't', set_clause={'order': "NULLIF(`order`, 'n''a')"})
    assert "SET `order`=NULLIF(`order`, 'n''a')" in sql


def test_load_set_rejects_non_mapping_even_when_empty():
    with pytest.raises(ValueError, match='set_clause'):
        LoadDataManager(Mock())._build_load_data_sql('file', 't', set_clause=[])


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
async def test_csv_forwards_unmapped_native_options(asynchronous):
    manager = AsyncLoadDataManager(Mock()) if asynchronous else LoadDataManager(Mock())
    manager.from_file = AsyncMock(return_value=object()) if asynchronous else Mock(return_value=object())
    result = manager.read_csv('file', 't', lines_terminated_by=';')
    if asynchronous:
        await result
    assert manager.from_file.call_args.kwargs['lines_terminated_by'] == ';'
