# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""ORM shortcuts use the public exporters and preserve the execution owner."""

from unittest.mock import Mock

import pytest

from matrixone.orm import MatrixOneQuery


def export(query, route, **options):
    if route == 'file':
        return query.export_to_file('/tmp/output', **options)
    if route == 'stage':
        return query.export_to_stage('archive', 'output', **options)
    if route == 'smart_file':
        return query.export_to('/tmp/output', **options)
    return query.export_to('archive', 'output', **options)


@pytest.mark.parametrize('route', ['file', 'stage', 'smart_file', 'smart_stage'])
@pytest.mark.parametrize('format', ['csv', 'JSONLINE'])
@pytest.mark.parametrize('transaction', [False, True])
def test_export_shortcuts_execute_compiled_query(route, format, transaction):
    client, session = Mock(), Mock()
    executor = session if transaction else client
    result = object()
    executor.execute.return_value = result
    query = MatrixOneQuery('items', client, transaction_wrapper=session if transaction else None)
    query.select('id').filter('name = ?', "O'Reilly")
    assert export(query, route, format=format) is result
    path = 'stage://archive/output' if 'stage' in route else '/tmp/output'
    expected = f"SELECT id FROM items WHERE name = 'O''Reilly' INTO OUTFILE '{path}' FORMAT '{format.lower()}'"
    if format == 'csv':
        expected += " fields terminated by ','"
    executor.execute.assert_called_once_with(expected)
    (client if transaction else session).execute.assert_not_called()


@pytest.mark.parametrize('route', ['file', 'stage'])
def test_csv_shortcut_preserves_options_and_header_warning(route):
    client = Mock()
    query = MatrixOneQuery('items', client).select('id')
    with pytest.warns(UserWarning, match='header=True'):
        export(query, route, fields_terminated_by=';', fields_enclosed_by='"', lines_terminated_by='|', header=True)
    sql = client.execute.call_args.args[0]
    assert "fields terminated by ';' enclosed by '\"' lines terminated by '|'" in sql


@pytest.mark.parametrize(
    'options',
    [
        {'format': 'parquet'},
        {'max_file_size': 1024},
        {'force_quote': ['id']},
        {'format': 'jsonline', 'fields_terminated_by': ';'},
    ],
)
@pytest.mark.parametrize('route', ['file', 'stage'])
def test_unsupported_export_options_fail_before_execution(route, options):
    client = Mock()
    with pytest.raises(ValueError):
        export(MatrixOneQuery('items', client).select('id'), route, **options)
    client.execute.assert_not_called()


def test_stage_compression_is_not_silently_ignored():
    client = Mock()
    with pytest.raises(ValueError, match='compression'):
        MatrixOneQuery('items', client).export_to_stage('archive', 'output', compression='gzip')
    client.execute.assert_not_called()
