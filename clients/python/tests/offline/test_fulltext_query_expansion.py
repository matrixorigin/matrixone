# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Switching to query expansion retains natural-language search text."""

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import mysql

from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match


@pytest.mark.parametrize(
    'query, token',
    [
        ('machine learning', "'machine learning'"),
        ("reader's guide", "'reader''s guide'"),
        ('机器学习', "'机器学习'"),
        ('+literal -words', "'+literal -words'"),
    ],
)
def test_natural_match_query_expansion_preserves_text(query, token):
    match = natural_match('content', query=query)
    original = f'MATCH(`content`) AGAINST({token})'
    assert match.compile() == original
    assert match.query_expansion() is match
    expanded = f'MATCH(`content`) AGAINST({token} WITH QUERY EXPANSION)'
    assert match.compile() == expanded
    assert str(select(match).compile(dialect=mysql.dialect())) == f'SELECT {expanded} AS anon_1'
    assert match.natural_language().compile() == original


def test_query_expansion_prefers_natural_text_over_boolean_terms():
    match = natural_match('content', query='original words').must('other')
    assert match.query_expansion().compile() == "MATCH(`content`) AGAINST('original words' WITH QUERY EXPANSION)"
    assert match.boolean_mode().compile() == "MATCH(`content`) AGAINST('+other' IN BOOLEAN MODE)"
    assert match.query_expansion().compile() == "MATCH(`content`) AGAINST('original words' WITH QUERY EXPANSION)"


def test_query_expansion_retains_builder_fallback():
    match = boolean_match('content').must('needle').query_expansion()
    assert match.compile() == "MATCH(`content`) AGAINST('+needle' WITH QUERY EXPANSION)"


def test_query_expansion_still_rejects_empty_query():
    with pytest.raises(ValueError, match='Query cannot be empty'):
        natural_match('content', query='').query_expansion().compile()
