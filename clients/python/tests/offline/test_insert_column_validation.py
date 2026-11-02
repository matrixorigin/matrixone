"""INSERT rows must not silently discard explicitly supplied fields."""

import sqlite3

import pytest

from matrixone.sql_builder import MatrixOneSQLBuilder


@pytest.mark.parametrize('explicit_columns', [False, True])
def test_extra_insert_column_is_rejected_without_changing_rows(explicit_columns):
    builder = MatrixOneSQLBuilder().insert_into('records')
    if explicit_columns:
        builder.insert_values(['id'], [[1]])
    else:
        builder.values(id=1)
    before = builder.build()

    with pytest.raises(ValueError, match='unexpected INSERT columns: payload'):
        builder.values(id=2, payload='must not disappear')

    assert builder.build() == before
    builder.values(id=3)
    sql, params = builder.build()
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE records (id INTEGER, payload TEXT)')
        db.execute(sql, params)
        assert db.execute('SELECT id, payload FROM records ORDER BY id').fetchall() == [
            (1, None),
            (3, None),
        ]


def test_insert_reordered_and_missing_columns_preserve_existing_null_behavior():
    builder = MatrixOneSQLBuilder().insert_into('records')
    builder.values(id=1, payload='first').values(payload='second', id=2).values(id=3)
    sql, params = builder.build()
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE records (id INTEGER, payload TEXT)')
        db.execute(sql, params)
        assert db.execute('SELECT id, payload FROM records ORDER BY id').fetchall() == [
            (1, 'first'),
            (2, 'second'),
            (3, None),
        ]
