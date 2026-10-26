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
