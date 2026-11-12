# Copyright 2021 - 2022 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Reflection uses the SDK-adapted driver dialect, including its type copies."""

from unittest.mock import Mock

import pytest
from sqlalchemy import Column, MetaData, Table, create_engine
from sqlalchemy.dialects.mysql.base import ischema_names as mysql_ischema_names
from sqlalchemy.dialects.mysql.reflection import MySQLTableDefinitionParser
from sqlalchemy.schema import CreateTable

from matrixone import Client
from matrixone.sqlalchemy_ext.vector_type import VectorType


@pytest.mark.parametrize('kind', ['vecf8', 'vecf4', 'vecf16', 'vecf32'])
@pytest.mark.parametrize('uppercase', [False, True])
@pytest.mark.parametrize('dimension', [1, 4, 65535])
def test_sdk_reflection_parser_and_type_copy(monkeypatch, kind, uppercase, dimension):
    # from_engine is a public SDK entry point and adapts the actual MySQL driver.
    monkeypatch.setattr(Client, '_detect_backend_version', lambda self: None)
    mysql_registry = dict(mysql_ischema_names)
    engine = create_engine('mysql+pymysql://root:unused@localhost/test')
    client = Client.from_engine(engine, logger=Mock(), sql_log_mode='off')
    dialect = client.get_sqlalchemy_engine().dialect
    emitted = kind.upper() if uppercase else kind
    parser = MySQLTableDefinitionParser(dialect, dialect.identifier_preparer)
    state = parser.parse(
        f'CREATE TABLE `docs` (\n  `id` int NOT NULL,\n  `v` {emitted}({dimension}) DEFAULT NULL\n)',
        'utf8mb4',
    )
    dtype = state.columns[1]['type']
    assert isinstance(dtype, VectorType)
    assert (dtype.precision, dtype.dimension) == (kind[3:], dimension)
    copied = dtype.copy()
    assert (copied.precision, copied.dimension, str(copied)) == (kind[3:], dimension, f'{kind}({dimension})')
    table = Table('docs', MetaData(), Column('v', dtype))
    copied_table = table.to_metadata(MetaData())
    assert str(copied_table.c.v.type) == f'{kind}({dimension})'
    assert f'{kind}({dimension})' in str(CreateTable(copied_table).compile(dialect=dialect))
    assert mysql_ischema_names == mysql_registry
    assert 'vecf8' not in mysql_ischema_names and 'vecf4' not in mysql_ischema_names
    engine.dispose()


def test_reflected_block_types_do_not_share_a_compiled_cache_key(monkeypatch):
    import warnings
    import sqlalchemy as sa

    monkeypatch.setattr(Client, '_detect_backend_version', lambda self: None)
    engine = create_engine('mysql+pymysql://root:unused@localhost/test')
    dialect = Client.from_engine(engine, logger=Mock(), sql_log_mode='off').get_sqlalchemy_engine().dialect
    types = [dialect.ischema_names[name](4) for name in ('vecf8', 'vecf4')]
    for dtype in types:
        clone = dtype.copy()
        assert (clone.precision, clone.dimension) == (dtype.precision, 4)
        # VectorType has no cache_ok declaration; SQLAlchemy must disable caching
        # for each statement rather than reuse a key that loses the precision.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', sa.exc.SAWarning)
            assert sa.select(sa.column('v', clone))._generate_cache_key() is None
    assert str(types[0]) != str(types[1])
    engine.dispose()
