# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""LOAD must target the mapped table, including its database and quoted names."""

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.elements import quoted_name

from matrixone.load_data import AsyncLoadDataManager, LoadDataManager


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('route', ['file', 'local', 'csv_inline', 'json_inline', 'stage'])
@pytest.mark.parametrize('mapping', ['schema', 'metadata', 'explicit_table', 'special_names', 'plain'])
async def test_load_preserves_mapped_table_identity(asynchronous, route, mapping):
    schema = 'archive'
    name = 'items'
    if mapping == 'special_names':
        schema, name = 'archive.db', 'items`old'
    if mapping == 'plain':
        schema = None
    base = declarative_base(metadata=MetaData(schema=schema if mapping == 'metadata' else None))
    if mapping == 'explicit_table':
        model = type(
            'Item',
            (base,),
            {'__table__': Table(name, base.metadata, Column('id', Integer, primary_key=True), schema=schema)},
        )
    else:
        attrs = {'__tablename__': name, 'id': Column(Integer, primary_key=True)}
        if mapping not in ('metadata', 'plain'):
            attrs['__table_args__'] = {'schema': schema}
        model = type('Item', (base,), attrs)
    execute = AsyncMock() if asynchronous else Mock()
    client = SimpleNamespace(execute=execute, _connection_params={'local_infile': True})
    manager = (AsyncLoadDataManager if asynchronous else LoadDataManager)(client)
    if route == 'file':
        result = manager.from_file('/tmp/rows.csv', model)
    elif route == 'local':
        result = manager.from_local_file('/tmp/rows.csv', model)
    elif route == 'csv_inline':
        result = manager.read_csv_inline('1\n', model)
    elif route == 'json_inline':
        result = manager.read_json_inline('{"id":1}\n', model)
    else:
        result = manager.read_csv_stage('archive_stage', 'rows.csv', model)
    if inspect.isawaitable(result):
        await result
    execute.assert_called_once()
    sql = str(execute.call_args.args[0])
    expected = 'items' if schema is None else 'archive.items'
    if mapping == 'special_names':
        expected = '`archive.db`.`items``old`'
    assert sql.partition('INTO TABLE ')[2].split(' ', 1)[0] == expected
    if route == 'local':
        assert execute.call_args.args[0].get_execution_options()['_matrixone_local_path'] == '/tmp/rows.csv'
    if asynchronous:
        execute.assert_awaited_once()


@pytest.mark.parametrize(
    ('target', 'expected'),
    [('"jan"', '`"jan"`'), (quoted_name('ranked', True), '`ranked`'), ('archive.items', 'archive.items')],
)
def test_string_targets_keep_legacy_identifier_rendering(target, expected):
    client = Mock()
    LoadDataManager(client).from_file('/tmp/rows.csv', target)
    assert str(client.execute.call_args.args[0]).partition('INTO TABLE ')[2].split(' ', 1)[0] == expected
