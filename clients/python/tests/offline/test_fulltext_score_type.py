# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""MATCH projections retain numeric scores through SQLAlchemy result processing."""

import pytest
from sqlalchemy import Column, Float, Integer, MetaData, Table, create_engine, event, select
from sqlalchemy.dialects import mysql

from matrixone.sqlalchemy_ext.fulltext_search import FulltextFilter, FulltextSearchMode, boolean_match, natural_match


@pytest.fixture
def score_connection():
    engine = create_engine('sqlite:///:memory:')
    docs = Table('docs', MetaData(), Column('id', Integer, primary_key=True), Column('search_score', Float))
    docs.metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            docs.insert(),
            [{'id': i, 'search_score': score} for i, score in enumerate([0.0, 0.09061906, 0.75, -0.25, None])],
        )
    try:
        with engine.connect() as connection:
            yield connection, docs
    finally:
        engine.dispose()


@pytest.mark.parametrize('mode', ['boolean', 'natural', 'expansion'])
@pytest.mark.parametrize('predicate', ['none', 'match', 'threshold'])
def test_fulltext_projection_preserves_scores(score_connection, mode, predicate):
    connection, docs = score_connection
    if mode == 'boolean':
        match = boolean_match('content').must('needle')
    elif mode == 'natural':
        match = natural_match('content', query='needle')
    else:
        match = FulltextFilter(['content'], FulltextSearchMode.QUERY_EXPANSION).must('needle')

    statement = select(docs.c.id, match).select_from(docs).order_by(docs.c.id)
    expected = [(0, 0.0), (1, 0.09061906), (2, 0.75), (3, -0.25), (4, None)]
    if predicate == 'match':
        statement = statement.where(match)
        expected = expected[1:4]
    elif predicate == 'threshold':
        statement = statement.where(match > 0.5)
        expected = [expected[2]]

    # Confirm production SQL still uses MATCH, including bare predicate use.
    sql = str(statement.compile(dialect=mysql.dialect()))
    assert match.compile() in sql
    if predicate == 'match':
        assert 'WHERE ' + match.compile() in sql
        assert '= 1' not in sql

    # Substitute only the server-side MATCH evaluation with stored scores.
    # Keep SQLAlchemy's actual projection, predicate and result processors.
    # This is an offline SDK contract, not a native fulltext engine test.
    @event.listens_for(connection, 'before_cursor_execute', retval=True)
    def substitute_scores(conn, cursor, sql, parameters, context, executemany):
        return sql.replace(match.compile(), 'search_score'), parameters

    rows = connection.execute(statement).all()
    assert rows == expected
    assert all(type(row[1]) is float for row in rows if row[1] is not None)
