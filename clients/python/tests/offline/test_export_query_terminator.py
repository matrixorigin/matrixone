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
    expected = f"SELECT 'semi;colon' AS value\nINTO OUTFILE '/tmp/output' FORMAT '{expected_format}'"
    if format == 'csv':
        expected += " fields terminated by ','"
    execute.assert_called_once_with(expected)
    if asynchronous:
        execute.assert_awaited_once_with(expected)


@pytest.mark.parametrize('suffix', ['', ';', ';;;', ' \n'])
def test_normalization_preserves_semicolons_inside_strings(suffix):
    assert _query_to_sql("  SELECT 'value;'" + suffix) == "SELECT 'value;'"


@pytest.mark.parametrize('number', ['1.0', '.1', '1e+2', '1.2e3'])
def test_numeric_dollar_alias_does_not_hide_statement_terminator(number):
    query = f'SELECT {number}$tag$alias // comment$tag$'
    assert _query_to_sql(query + ';') == query


@pytest.mark.parametrize(
    'mode,query',
    [
        ('NO_BACKSLASH_ESCAPES', "SELECT LENGTH('\\') AS value"),
        ('', "SELECT LENGTH('quote\\' ; // data') AS value"),
        ('ANSI_QUOTES', 'SELECT 1 AS "column\\"'),
    ],
)
def test_normalization_uses_the_executing_connection_sql_mode(mode, query):
    assert _query_to_sql(query + '; -- trailing', sql_mode=mode) == query + ' -- trailing'


@pytest.mark.parametrize('literal', ['$$a // b$$', '$tag$a -- b$tag$', '$tag$a # b$tag$', '$tag$a /* b */$tag$'])
@pytest.mark.parametrize('source', ['string', 'sqlalchemy', 'builder'])
def test_normalization_preserves_dollar_string_boundaries(literal, source):
    statement = f'SELECT {literal} AS value'
    query = statement + ';'
    if source == 'sqlalchemy':
        query = text(query)
    elif source == 'builder':
        query = SimpleNamespace(_build_sql=lambda: (statement + ';', []))
    assert _query_to_sql(query) == statement


def test_normalization_does_not_treat_identifier_dollars_as_string_openers():
    assert _query_to_sql('SELECT column$tag$ AS value; -- trailing') == 'SELECT column$tag$ AS value -- trailing'


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('comment', ['-- trailing', '# trailing', '// trailing'])
@pytest.mark.parametrize('terminator', ['', ';'])
async def test_export_places_outfile_after_trailing_line_comments(asynchronous, comment, terminator):
    execute = AsyncMock(return_value=object()) if asynchronous else Mock(return_value=object())
    exporter = (AsyncExportManager if asynchronous else ExportManager)(SimpleNamespace(execute=execute))
    query = f"SELECT 'data; -- # //' AS value{terminator} {comment}"
    output = exporter.to_csv('/tmp/output', query)
    if asynchronous:
        output = await output
    expected = (
        f"SELECT 'data; -- # //' AS value {comment}\n"
        "INTO OUTFILE '/tmp/output' FORMAT 'csv' fields terminated by ','"
    )
    execute.assert_called_once_with(expected)
