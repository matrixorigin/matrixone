# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Reusing a fulltext group must not rewrite an already constructed query."""

import pytest

from matrixone.sqlalchemy_ext.fulltext_search import FulltextQueryBuilder, boolean_match, group

OPERATORS = [('must', '+'), ('must_not', '-'), ('encourage', ''), ('discourage', '~')]


@pytest.mark.parametrize('first_method, first_prefix', OPERATORS)
@pytest.mark.parametrize('second_method, second_prefix', OPERATORS)
def test_reused_group_preserves_each_query_operator(first_method, first_prefix, second_method, second_prefix):
    shared = group().medium('java', 'kotlin')
    first = getattr(boolean_match('content'), first_method)(shared)
    first_sql = f"MATCH(`content`) AGAINST('{first_prefix}(java kotlin)' IN BOOLEAN MODE)"
    assert first.compile() == first_sql
    second = getattr(boolean_match('content'), second_method)(shared)
    assert second.compile() == f"MATCH(`content`) AGAINST('{second_prefix}(java kotlin)' IN BOOLEAN MODE)"
    assert first.compile() == first_sql
    assert shared.group_type == 'or'
    assert shared.is_tilde is False


@pytest.mark.parametrize('method, prefix', OPERATORS)
def test_inserted_group_is_independent_of_later_nested_edits(method, prefix):
    child = group().high('fast')
    shared = group().medium('java').add_group(child)
    query = getattr(FulltextQueryBuilder(), method)(shared)
    expected = f'{prefix}(java (>fast))'
    assert query.build() == expected

    shared.medium('kotlin')
    child.low('slow')
    child.elements[0].content = 'changed'
    assert query.build() == expected

    # The copy also prevents changes to an owned query from mutating its source.
    query.main_group.groups[0].elements[0].content = 'owned'
    assert shared.elements[0].content == 'java'


def test_same_group_can_have_distinct_operators_in_one_query():
    shared = group().medium('java', 'kotlin')
    query = FulltextQueryBuilder().must(shared).discourage(shared)
    assert query.build() == '+(java kotlin) ~(java kotlin)'
