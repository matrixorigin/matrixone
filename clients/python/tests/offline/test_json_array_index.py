# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Integer JSON subscripts address arrays, while string subscripts address keys."""

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, literal, select
from sqlalchemy.sql.util import ClauseAdapter

from matrixone.sqlalchemy_ext import JSON


@pytest.mark.parametrize(
    'document, keys, expected, path',
    [
        ([11, 22], [0], 11, '$[0]'),
        ([11, 22], [1], 22, '$[1]'),
        ({'items': [11, 22]}, ['items', 1], 22, '$."items"[1]'),
        ([{'value': 7}], [0, 'value'], 7, '$[0]."value"'),
        ([[11, 22]], [0, 1], 22, '$[0][1]'),
        ({'0': 33}, ['0'], 33, '$."0"'),
        ({'items': []}, ['items', 0], None, '$."items"[0]'),
    ],
)
def test_json_integer_subscript_execution(document, keys, expected, path):
    expression = literal(document, type_=JSON)
    for key in keys:
        expression = expression[key]
    statement = select(expression)
    assert path in statement.compile().params.values()
    engine = create_engine('sqlite:///:memory:')
    try:
        with engine.connect() as connection:
            assert connection.execute(statement).scalar_one() == expected
    finally:
        engine.dispose()


def test_json_array_indices_survive_alias_adaptation_and_cache_reuse():
    docs = Table('docs', MetaData(), Column('id', Integer, primary_key=True), Column('data', JSON))
    engine = create_engine('sqlite:///:memory:')
    try:
        with engine.begin() as connection:
            docs.metadata.create_all(connection)
            connection.execute(docs.insert(), [{'id': 1, 'data': [11, 12]}, {'id': 2, 'data': [21, 22]}])
            alias = docs.alias('chosen')
            for index, expected in [(0, 21), (1, 22), (0, 21)]:
                member = ClauseAdapter(alias).traverse(docs.c.data[index])
                statement = select(member).where(alias.c.id == 2)
                assert connection.execute(statement).scalars().all() == [expected]
    finally:
        engine.dispose()


@pytest.mark.parametrize('nested', [False, True])
def test_json_negative_array_index_is_rejected(nested):
    expression = literal({'items': [1]}, type_=JSON)
    if nested:
        expression = expression['items']
    with pytest.raises(ValueError, match='non-negative'):
        expression[-1]
