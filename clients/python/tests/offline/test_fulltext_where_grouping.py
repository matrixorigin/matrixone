# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Additional fulltext predicates remain conjuncts even when they contain OR."""

import sqlite3

import pytest

from matrixone.sqlalchemy_ext.fulltext_search import (
    FulltextQueryBuilder,
    FulltextSearchBuilder,
    FulltextSearchMode,
)


@pytest.mark.parametrize('builder_kind', ['query', 'search'])
@pytest.mark.parametrize(
    'conditions, expected',
    [
        ([], [1, 3, 4]),
        (["category = 'a' OR category = 'b'"], [1, 3]),
        (["category = 'a' OR category = 'b'", 'active = 1'], [1]),
        (["category = 'a'", 'active = 1 OR featured = 1'], [1]),
    ],
)
def test_fulltext_where_conditions_preserve_boolean_grouping(builder_kind, conditions, expected):
    if builder_kind == 'query':
        sql = (
            FulltextQueryBuilder()
            .must('needle')
            .as_sql('articles', ['content'], select_columns=['id'], where_conditions=conditions, order_by='id')
        )
    else:
        builder = (
            FulltextSearchBuilder(None)
            .table('articles')
            .columns(['content'])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term('needle', required=True)
            .select(['id'])
            .order_by('id', 'ASC')
        )
        for condition in conditions:
            builder.where(condition)
        sql = builder.explain()

    # Supply precomputed search scores to SQLite to exercise Boolean grouping
    # independently of MatrixOne's fulltext engine and tokenization.
    match = "MATCH(`content`) AGAINST('+needle' IN BOOLEAN MODE)"
    assert sql.count(match) == 1
    sql = sql.replace(match, 'search_score')
    connection = sqlite3.connect(':memory:')
    try:
        connection.execute('CREATE TABLE articles (id, search_score, category, active, featured)')
        connection.executemany(
            'INSERT INTO articles VALUES (?, ?, ?, ?, ?)',
            [(1, 1, 'a', 1, 0), (2, 0, 'b', 1, 1), (3, 1, 'b', 0, 0), (4, 1, 'c', 1, 1)],
        )
        assert [row[0] for row in connection.execute(sql)] == expected
    finally:
        connection.close()
