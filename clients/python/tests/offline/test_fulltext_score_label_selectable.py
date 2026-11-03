"""FulltextFilter labels must be usable as SQLAlchemy SELECT expressions.

These are offline SQL-generation checks, not a MatrixOne server scoring test.
"""

import pytest
from sqlalchemy import Column, Float, Integer, MetaData, Table, Text, select
from sqlalchemy.dialects import mysql
from sqlalchemy.sql.elements import Label

from matrixone.sqlalchemy_ext.fulltext_search import boolean_match


@pytest.mark.parametrize("name", ["score", "relevance score", "匹配分数"])
def test_fulltext_label_is_selectable(name):
    docs = Table("docs", MetaData(), Column("id", Integer), Column("body", Text))
    match = boolean_match(docs.c.body).must("needle")
    score = match.label(name)

    assert isinstance(score, Label)
    assert isinstance(score.type, Float)

    statement = select(docs.c.id, score).where(match)
    sql = str(statement.compile(dialect=mysql.dialect()))
    assert "MATCH(" in sql
    assert "WHERE MATCH(" in sql
    assert " = 1" not in sql
    assert "FROM docs" in sql
    assert " AS " in sql
    if " " in name:
        assert f" AS `{name}`" in sql


@pytest.mark.parametrize("name", ["score", "relevance score", "匹配分数"])
def test_fulltext_label_preserves_legacy_standalone_compile(name):
    match = boolean_match("body").must("needle")
    label = match.label(name)
    assert label.compile() == f"{match.compile()} AS {name}"


def test_labeled_fulltext_expressions_can_be_selected_together():
    docs = Table("docs", MetaData(), Column("body", Text))
    left = boolean_match(docs.c.body).must("needle").label("left_score")
    right = boolean_match(docs.c.body).must("haystack").label("right_score")
    sql = str(select(left, right).compile(dialect=mysql.dialect()))

    assert " AS left_score" in sql
    assert " AS right_score" in sql
