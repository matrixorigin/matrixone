# Copyright 2021 - 2022 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Exercise vector reflection through a connected SDK engine."""

import ast
import os
import re
from uuid import uuid4

import pytest
import sqlalchemy as sa

from matrixone.sqlalchemy_ext.vector_type import VectorType


def _skip_unsupported_block_vector(client, kind):
    # The released 4.2.5 grammar predates VECF8/VECF4.  Do not infer that
    # other or unknown versions lack support from a failed statement: that
    # could turn an actual server or reflection regression into a skip.
    if kind in ('vecf8', 'vecf4') and os.environ.get('MO_SDK_TARGET') == '4.2.5':
        rows = client.execute('SELECT version()').rows
        banner = rows[0][0] if rows else None
        match = re.search(r'(?:^|-)MatrixOne-v([0-9]+\.[0-9]+\.[0-9]+)(?:$|[-+])', banner or '')
        if match and match.group(1) == '4.2.5':
            pytest.skip('MatrixOne 4.2.5 does not support VECF8/VECF4')


@pytest.mark.parametrize(
    'target,version,kind,skipped',
    [('4.2.5', '8.0.30-MatrixOne-v4.2.5', 'vecf8', True),
     ('4.2.5', '8.0.30-MatrixOne-v4.2.5+build.1', 'vecf4', True),
     ('4.2.5', '8.0.30-MatrixOne-v4.2.5-foo', 'vecf8', True),
     ('4.2.5', '8.0.30-MatrixOne-v4.2.5', 'vecf16', False),
     ('4.2.5', '8.0.30-MatrixOne-v4.2.50', 'vecf8', False),
     ('4.2.5', '8.0.30-MatrixOne-v4.2.5dev', 'vecf8', False),
     ('4.2.5', None, 'vecf4', False),
     ('current-source', '8.0.30-MatrixOne-v4.2.5-current-source', 'vecf8', False),
     ('current-source', '8.0.30-MatrixOne-v4.2.5+build.1', 'vecf4', False),
     (None, '8.0.30-MatrixOne-v4.2.5', 'vecf8', False),
     ('unknown', '8.0.30-MatrixOne-v4.2.5', 'vecf4', False)],
)
def test_block_vector_version_gate(monkeypatch, target, version, kind, skipped):
    from types import SimpleNamespace

    if target is None:
        monkeypatch.delenv('MO_SDK_TARGET', raising=False)
    else:
        monkeypatch.setenv('MO_SDK_TARGET', target)
    client = SimpleNamespace(execute=lambda sql: SimpleNamespace(rows=[(version,)]))
    if skipped:
        with pytest.raises(pytest.skip.Exception):
            _skip_unsupported_block_vector(client, kind)
    else:
        _skip_unsupported_block_vector(client, kind)


@pytest.mark.online
@pytest.mark.parametrize('kind', ['vecf8', 'vecf4', 'vecf16', 'vecf32'])
def test_reflect_vector_table_and_views(test_client, kind):
    _skip_unsupported_block_vector(test_client, kind)
    suffix = uuid4().hex[:12]
    table_name = f'test_vector_reflect_{suffix}'
    view_name = f'test_vector_view_{suffix}'
    nested_name = f'test_vector_nested_{suffix}'
    engine = test_client.get_sqlalchemy_engine()
    try:
        test_client.execute(f'CREATE TABLE {table_name} (id INT PRIMARY KEY, v {kind}(4))')
        test_client.execute(f'CREATE VIEW {view_name} AS SELECT * FROM {table_name}')
        test_client.execute(f'CREATE VIEW {nested_name} AS SELECT * FROM {view_name}')
        table = sa.Table(table_name, sa.MetaData(), autoload_with=engine)
        # Bind through the reflected type; server, not SDK, quantizes values.
        with engine.begin() as conn:
            conn.execute(table.insert(), [
                {'id': 1, 'v': [1, -3, 0, 6]},
                {'id': 2, 'v': [1, -2.5, 0, 4]},
                {'id': 3, 'v': None},
            ])
        with engine.connect() as conn:
            raw = conn.execute(sa.text(f'SELECT id, v FROM {table_name} ORDER BY id')).fetchall()
            # Independent SQL literals bypass the reflected type's bind processor.
            literal_results = [
                conn.execute(sa.text(f"SELECT CAST('{value}' AS {kind}(4))")).scalar()
                for value in ('[1,-3,0,6]', '[1,-2.5,0,4]')
            ]
        expected = [(row[0], None if row[1] is None else ast.literal_eval(row[1])) for row in raw]
        for index, literal_result in enumerate(literal_results):
            assert expected[index][1] == pytest.approx(ast.literal_eval(literal_result))
        assert expected[2] == (3, None)
        for name in (table_name, view_name, nested_name):
            reflected = sa.Table(name, sa.MetaData(), autoload_with=engine)
            dtype = reflected.c.v.type
            assert isinstance(dtype, VectorType)
            assert (dtype.precision, dtype.dimension) == (kind[3:], 4)
            with engine.connect() as conn:
                rows = conn.execute(sa.select(reflected.c.id, reflected.c.v).order_by(reflected.c.id)).fetchall()
            assert [(row[0], row[1]) for row in rows] == expected
            scalar = sa.Table(name, sa.MetaData(), autoload_with=engine, include_columns=['id'])
            assert list(scalar.c.keys()) == ['id']
            with engine.connect() as conn:
                assert [row[0] for row in conn.execute(sa.select(scalar.c.id).order_by(scalar.c.id))] == [1, 2, 3]
        assert engine.pool.checkedout() == 0
    finally:
        test_client.execute(f'DROP VIEW IF EXISTS {nested_name}')
        test_client.execute(f'DROP VIEW IF EXISTS {view_name}')
        test_client.execute(f'DROP TABLE IF EXISTS {table_name}')


@pytest.mark.online
@pytest.mark.parametrize('kind', ['vecf8', 'vecf4'])
def test_adopt_engine_after_mysql_parser_was_used(test_client, kind):
    from matrixone import Client

    _skip_unsupported_block_vector(test_client, kind)
    suffix = uuid4().hex[:12]
    plain = f'test_vector_plain_{suffix}'
    vectors = f'test_vector_warm_{suffix}'
    engine = sa.create_engine(test_client.get_sqlalchemy_engine().url)
    try:
        test_client.execute(f'CREATE TABLE {plain} (id INT PRIMARY KEY)')
        test_client.execute(f'CREATE TABLE {vectors} (id INT PRIMARY KEY, v {kind}(4))')
        # This creates and uses MySQL's memoized parser before SDK adoption.
        assert [col['name'] for col in sa.inspect(engine).get_columns(plain)] == ['id']
        parser = engine.dialect._tabledef_parser
        adopted = Client.from_engine(engine)
        assert adopted.get_sqlalchemy_engine().dialect._tabledef_parser is parser
        # A new inspector ensures any cached schema data cannot hide the result.
        columns = sa.inspect(adopted.get_sqlalchemy_engine()).get_columns(vectors)
        dtype = columns[1]['type']
        assert isinstance(dtype, VectorType)
        assert (dtype.precision, dtype.dimension) == (kind[3:], 4)
    finally:
        engine.dispose()
        test_client.execute(f'DROP TABLE IF EXISTS {vectors}')
        test_client.execute(f'DROP TABLE IF EXISTS {plain}')
