# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""CSV options survive both public exporters and their stage shortcuts."""

import inspect
from unittest.mock import AsyncMock, Mock

import pytest

from matrixone.export import AsyncExportManager, ExportManager


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('stage', [False, True])
@pytest.mark.parametrize(
    'sep,quotechar,enclosure',
    [
        (',', None, ''),
        (';', None, ''),
        (',', '"', "'\"'"),
        (';', '"', "'\"'"),
        (',', "'", "''''"),
        (';', "'", "''''"),
        ("'", None, ''),
        ("'", '"', "'\"'"),
        ('\t', '"', "'\"'"),
    ],
)
async def test_csv_options_are_preserved(asynchronous, stage, sep, quotechar, enclosure):
    result = object()
    execute = AsyncMock(return_value=result) if asynchronous else Mock(return_value=result)
    client = Mock(execute=execute)
    exporter = AsyncExportManager(client) if asynchronous else ExportManager(client)
    query = 'SELECT id, label FROM records'
    if stage:
        output = exporter.to_csv_stage('archive', 'data.csv', query, sep=sep, quotechar=quotechar)
        path = 'stage://archive/data.csv'
    else:
        output = exporter.to_csv('/tmp/data.csv', query, sep=sep, quotechar=quotechar)
        path = '/tmp/data.csv'
    if inspect.isawaitable(output):
        output = await output
    separator = "''''" if sep == "'" else f"'{sep}'"
    expected = f"{query} INTO OUTFILE '{path}' fields terminated by {separator}"
    if quotechar:
        expected += f" enclosed by {enclosure}"
    assert output is result
    execute.assert_called_once_with(expected)
    if asynchronous:
        execute.assert_awaited_once_with(expected)


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('stage', [False, True])
@pytest.mark.parametrize('option,value', [('quotechar', '\\'), ('quotechar', '\x00'), ('sep', '\\'), ('sep', '\x00')])
async def test_nonportable_csv_option_is_rejected_before_execution(asynchronous, stage, option, value):
    execute = AsyncMock() if asynchronous else Mock()
    exporter = AsyncExportManager(Mock(execute=execute)) if asynchronous else ExportManager(Mock(execute=execute))
    with pytest.raises(ValueError):
        if stage:
            output = exporter.to_csv_stage('archive', 'data.csv', 'SELECT 1', **{option: value})
        else:
            output = exporter.to_csv('/tmp/data.csv', 'SELECT 1', **{option: value})
        if inspect.isawaitable(output):
            await output
    execute.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('stage', [False, True])
@pytest.mark.parametrize('format', ['csv', 'jsonl'])
async def test_export_path_is_one_string_token(asynchronous, stage, format):
    execute = AsyncMock() if asynchronous else Mock()
    exporter = AsyncExportManager(Mock(execute=execute)) if asynchronous else ExportManager(Mock(execute=execute))
    # This must remain filename text, rather than become another OUTFILE option.
    filename = "customer' fields terminated by ';--." + format
    method = getattr(exporter, 'to_' + format + ('_stage' if stage else ''))
    args = ('archive', filename, 'SELECT 1') if stage else ('/tmp/' + filename, 'SELECT 1')
    output = method(*args)
    if inspect.isawaitable(output):
        await output
    path = ('stage://archive/' if stage else '/tmp/') + filename
    expected = "SELECT 1 INTO OUTFILE '" + path.replace("'", "''") + "'"
    expected += " fields terminated by ','" if format == 'csv' else " FORMAT 'jsonline'"
    execute.assert_called_once_with(expected)


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('stage', [False, True])
async def test_csv_line_terminator_is_one_string_token(asynchronous, stage):
    execute = AsyncMock() if asynchronous else Mock()
    exporter = AsyncExportManager(Mock(execute=execute)) if asynchronous else ExportManager(Mock(execute=execute))
    kwargs = dict(lineterminator="'\n")
    if stage:
        output = exporter.to_csv_stage('archive', 'data.csv', 'SELECT 1', **kwargs)
        path = 'stage://archive/data.csv'
    else:
        output = exporter.to_csv('/tmp/data.csv', 'SELECT 1', **kwargs)
        path = '/tmp/data.csv'
    if inspect.isawaitable(output):
        await output
    execute.assert_called_once_with(
        f"SELECT 1 INTO OUTFILE '{path}' fields terminated by ',' lines terminated by '''\n'"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('stage', [False, True])
@pytest.mark.parametrize('format,option', [('csv', 'path'), ('jsonl', 'path'), ('csv', 'lineterminator')])
@pytest.mark.parametrize('value', ['bad\\path', 'bad\x00path'])
async def test_nonportable_export_tokens_rejected(asynchronous, stage, format, option, value):
    execute = AsyncMock() if asynchronous else Mock()
    exporter = AsyncExportManager(Mock(execute=execute)) if asynchronous else ExportManager(Mock(execute=execute))
    method = getattr(exporter, 'to_' + format + ('_stage' if stage else ''))
    path = value if option == 'path' else 'data.' + format
    args = ('archive', path, 'SELECT 1') if stage else (path, 'SELECT 1')
    kwargs = {'lineterminator': value} if option == 'lineterminator' else {}
    with pytest.raises(ValueError):
        output = method(*args, **kwargs)
        if inspect.isawaitable(output):
            await output
    execute.assert_not_called()
