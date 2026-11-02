"""Composed OR filters must remain bounded by adjacent AND filters."""

import sqlite3

import pytest

from matrixone.sql_builder import MatrixOneSQLBuilder


@pytest.mark.parametrize('operation', ['select', 'update', 'delete'])
@pytest.mark.parametrize('or_first', [True, False])
def test_where_or_respects_adjacent_tenant_filter(operation, or_first):
    builder = MatrixOneSQLBuilder()
    if operation == 'select':
        builder.select('id').from_table('records')
    elif operation == 'update':
        builder.update('records').set(marked=1)
    else:
        builder.delete_from('records')

    if or_first:
        builder.where_or('kind = 1', 'kind = 2').where_eq('tenant', 7)
    else:
        builder.where_eq('tenant', 7).where_or('kind = 1', 'kind = 2')
    sql, params = builder.build()

    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE records (id INTEGER, tenant INTEGER, kind INTEGER, marked INTEGER)')
        db.executemany(
            'INSERT INTO records VALUES (?, ?, ?, 0)',
            [
                (1, 7, 1),
                (2, 7, 2),
                (3, 8, 1),
                (4, 8, 2),
                (5, 7, 3),
            ],
        )
        result = db.execute(sql, params)
        if operation == 'select':
            assert sorted(result.fetchall()) == [(1,), (2,)]
        elif operation == 'update':
            assert db.execute('SELECT id FROM records WHERE marked = 1 ORDER BY id').fetchall() == [
                (1,),
                (2,),
            ]
        else:
            assert db.execute('SELECT id FROM records ORDER BY id').fetchall() == [(3,), (4,), (5,)]
