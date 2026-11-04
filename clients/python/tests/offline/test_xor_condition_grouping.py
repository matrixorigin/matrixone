# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Chained predicates must not change the scope of a caller's SQL XOR."""

import pytest

from matrixone.sql_builder import MatrixOneSQLBuilder


@pytest.mark.parametrize('operation', ['select', 'update', 'delete', 'having'])
@pytest.mark.parametrize('operator', ['XOR', 'xor'])
def test_xor_predicate_remains_a_conjunct(operation, operator):
    builder = MatrixOneSQLBuilder()
    if operation == 'update':
        builder.update('items').set(processed=1)
    elif operation == 'delete':
        builder.delete_from('items')
    else:
        builder.select('id').from_table('items')

    clause = 'WHERE'
    predicate = f'a = ? {operator} b = ?'
    if operation == 'having':
        builder.group_by('id')
        builder.having(predicate, 1, 1).having('enabled = ?', 1)
        clause = 'HAVING'
    else:
        builder.where(predicate, 1, 1).where('enabled = ?', 1)

    sql, params = builder.build()
    # AND binds more tightly than XOR in MatrixOne. Without these parentheses,
    # a=true, b=false, enabled=false would incorrectly satisfy the predicate.
    assert f'{clause} (a = ? {operator} b = ?) AND enabled = ?' in sql
    assert params == ([1, 1, 1, 1] if operation == 'update' else [1, 1, 1])
    assert f'{clause} (a = 1 {operator} b = 1) AND enabled = 1' in builder.build_with_parameter_substitution()


def test_xor_in_later_predicate_remains_grouped():
    builder = MatrixOneSQLBuilder().select('id').from_table('items')
    sql, params = builder.where('enabled = ?', 1).where('a = ? XOR b = ?', 1, 1).build()
    assert 'WHERE enabled = ? AND (a = ? XOR b = ?)' in sql
    assert params == [1, 1, 1]


@pytest.mark.parametrize('predicate', ['a = ? XOR b = ?', 'a = ? OR b = ?'])
def test_single_predicate_keeps_its_original_sql(predicate):
    builder = MatrixOneSQLBuilder().select('id').from_table('items').where(predicate, 1, 1)
    assert builder.build() == ('SELECT id FROM items WHERE ' + predicate, [1, 1])
