# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Whitespace after a query terminator must not split an OUTFILE statement."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import text

from matrixone.export import AsyncExportManager, ExportManager, _query_to_sql


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('format', ['csv', 'jsonl'])
@pytest.mark.parametrize('source', ['string', 'sqlalchemy', 'builder'])
async def test_export_removes_terminator_before_trailing_whitespace(asynchronous, format, source):
    sql = "  SELECT 'semi;colon' AS value; \r\n\t"
    if source == 'sqlalchemy':
        query = text('  SELECT :value AS value; \r\n\t').bindparams(value='semi;colon')
    elif source == 'builder':
        query = SimpleNamespace(_build_sql=lambda: ('  SELECT ? AS value; \r\n\t', ['semi;colon']))
    else:
        query = sql
    result = object()
    execute = AsyncMock(return_value=result) if asynchronous else Mock(return_value=result)
    exporter = (AsyncExportManager if asynchronous else ExportManager)(SimpleNamespace(execute=execute))
    output = getattr(exporter, 'to_' + format)('/tmp/output', query)
    if asynchronous:
        output = await output
    assert output is result
    expected_format = 'jsonline' if format == 'jsonl' else 'csv'
    expected = f"SELECT 'semi;colon' AS value INTO OUTFILE '/tmp/output' FORMAT '{expected_format}'"
    if format == 'csv':
        expected += " fields terminated by ','"
    execute.assert_called_once_with(expected)
    if asynchronous:
        execute.assert_awaited_once_with(expected)


@pytest.mark.parametrize('suffix', ['', ';', ';;;', ' \n'])
def test_normalization_preserves_semicolons_inside_strings(suffix):
    assert _query_to_sql("  SELECT 'value;'" + suffix) == "SELECT 'value;'"
