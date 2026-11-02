# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""JSON member expressions participate in SQLAlchemy statement traversal."""

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, bindparam, create_engine, select
from sqlalchemy.sql.util import ClauseAdapter

from matrixone.sqlalchemy_ext import JSON


@pytest.fixture
def database():
    engine = create_engine('sqlite:///:memory:')
    try:
        with engine.connect() as connection:
            yield connection
    finally:
        engine.dispose()


@pytest.mark.parametrize('nested', [False, True])
def test_json_member_params_replaces_source_without_mutating_original(database, nested):
    def document(value):
        return {'outer': {'value': value}} if nested else {'value': value}

    source = bindparam('document', document(1), type_=JSON)
    member = source['outer']['value'] if nested else source['value']
    original = select(member)
    for value in (2, 3):
        rebound = original.params(document=document(value))
        assert database.execute(rebound).scalar_one() == value
        assert database.execute(original).scalar_one() == 1


def test_json_member_unique_params_keeps_independent_values(database):
    statement = select(bindparam('document', {'value': 0}, type_=JSON)['value'])
    left = statement.unique_params(document={'value': 1}).scalar_subquery()
    right = statement.unique_params(document={'value': 2}).scalar_subquery()
    assert database.execute(select(left, right)).one() == (1, 2)


def test_json_member_cache_distinguishes_full_paths_and_rebinds_values(database):
    for offset in (0, 100, 200):
        source = bindparam('document', {'value': offset + 1, 'outer': {'value': offset + 2}}, type_=JSON)
        for member, expected in ((source['value'], offset + 1), (source['outer']['value'], offset + 2)):
            assert database.execute(select(member)).scalar_one() == expected


@pytest.mark.parametrize('nested', [False, True])
def test_json_member_adapts_source_to_table_alias(database, nested):
    metadata = MetaData()
    table = Table('documents', metadata, Column('id', Integer), Column('doc', JSON))
    metadata.create_all(database)
    database.execute(
        table.insert(),
        [
            {'id': 1, 'doc': {'value': 11, 'outer': {'value': 21}}},
            {'id': 2, 'doc': {'value': 12, 'outer': {'value': 22}}},
        ],
    )
    alias = table.alias('selected')
    member = table.c.doc['outer']['value'] if nested else table.c.doc['value']
    adapted = ClauseAdapter(alias).traverse(member)
    statement = select(adapted).select_from(alias).where(alias.c.id == 2)
    assert database.execute(statement).scalars().all() == ([22] if nested else [12])
