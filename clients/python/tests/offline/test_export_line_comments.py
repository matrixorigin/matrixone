# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""A trailing SELECT comment must not consume the generated OUTFILE clause."""

import inspect
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import text

from matrixone.export import AsyncExportManager, ExportManager
from matrixone.orm import MatrixOneQuery


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('stage', [False, True])
@pytest.mark.parametrize('format', ['csv', 'jsonl'])
@pytest.mark.parametrize('comment', ['-- export rows', '# export rows', '// export rows'])
@pytest.mark.parametrize('input_kind', ['string', 'sqlalchemy', 'orm'])
async def test_outfile_starts_after_query_comment(asynchronous, stage, format, comment, input_kind):
    result = object()
    execute = AsyncMock(return_value=result) if asynchronous else Mock(return_value=result)
    client = Mock(execute=execute)
    exporter = AsyncExportManager(client) if asynchronous else ExportManager(client)
    expected_query = "SELECT * FROM items WHERE name = 'O''Reilly' " + comment
    if input_kind == 'orm':
        query = MatrixOneQuery('items', client).filter('name = ? ' + comment, "O'Reilly")
    elif input_kind == 'sqlalchemy':
        query = text('SELECT * FROM items WHERE name = :name ' + comment + '\n').bindparams(name="O'Reilly")
    else:
        query = expected_query + '\r\n  '

    method = getattr(exporter, 'to_' + format + ('_stage' if stage else ''))
    args = ('archive', 'output', query) if stage else ('/tmp/output', query)
    output = method(*args)
    if inspect.isawaitable(output):
        output = await output

    path = 'stage://archive/output' if stage else '/tmp/output'
    suffix = "FORMAT 'csv' fields terminated by ','" if format == 'csv' else "FORMAT 'jsonline'"
    expected = expected_query + "\nINTO OUTFILE '" + path + "' " + suffix
    assert output is result
    execute.assert_called_once_with(expected)
    if asynchronous:
        execute.assert_awaited_once_with(expected)
