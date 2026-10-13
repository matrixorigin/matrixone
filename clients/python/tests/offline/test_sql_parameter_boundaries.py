# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

from datetime import date
from decimal import Decimal

import pytest

from matrixone._sql_literals import sql_literal, substitute_parameters
from matrixone.base_client import BaseMatrixOneClient
from matrixone.vector_manager import _VectorManagerBase


@pytest.mark.parametrize(
    'sql',
    [
        "SELECT '?' AS marker, ?",
        'SELECT "?", ?',
        'SELECT `?`, ?',
        'SELECT /* ? :id %s */ ?',
        'SELECT # ?\n ?',
        'SELECT -- ?\n ?',
        "SELECT 'it''s ?' AS marker, ?",
        "SELECT 'it\\\'s ?' AS marker, ?",
    ],
)
def test_only_executable_placeholders_replaced(sql):
    result = substitute_parameters(sql, (7,))
    assert result.endswith('7')
    assert result[:-1] == sql[:-1]


def test_named_boundaries_and_repeated_names():
    assert substitute_parameters('SELECT :id,:id2,:id', {'id': 1, 'id2': 2}) == 'SELECT 1,2,1'
    assert substitute_parameters("SELECT ':id', :id /* :id2 */", {'id': 7}) == "SELECT ':id', 7 /* :id2 */"


def test_values_are_not_rescanned():
    assert substitute_parameters('SELECT ?, ?', ('?:id%s', 'other')) == "SELECT '?:id%s', 'other'"


@pytest.mark.parametrize(
    'sql,params',
    [
        ('SELECT ?,?', (1,)),
        ('SELECT ?', (1, 2)),
        ('SELECT :id', {}),
        ('SELECT :id', {'id': 1, 'id2': 2}),
        ('SELECT 1', (1,)),
    ],
)
def test_mismatch_is_explicit(sql, params):
    with pytest.raises(ValueError):
        substitute_parameters(sql, params)


@pytest.mark.parametrize('value', ["O'Reilly", "x\\' OR 1=1 --", '\x00中文😀', 'C:\\new\\path'])
def test_safe_legacy_insert_rendering(value):
    for builder in [BaseMatrixOneClient()._build_insert_sql, _VectorManagerBase._build_insert_sql]:
        rendered = builder('t', {'v': value})
        assert sql_literal(value) in rendered
    if '\\' in value or '\x00' in value:
        assert sql_literal(value) == "CONVERT(X'%s' USING utf8mb4)" % value.encode().hex()
    else:
        assert "O''Reilly" in sql_literal(value)


@pytest.mark.parametrize(
    'value,expected',
    [
        (None, 'NULL'),
        (True, '1'),
        (Decimal('1.20'), '1.20'),
        (date(2026, 1, 2), "'2026-01-02'"),
        (b'\x00\xff', "X'00ff'"),
    ],
)
def test_typed_parameters(value, expected):
    assert sql_literal(value) == expected


@pytest.mark.parametrize('value', [float('nan'), float('inf'), Decimal('NaN'), object()])
def test_unsupported_parameters_fail(value):
    with pytest.raises((TypeError, ValueError)):
        sql_literal(value)


def test_sqlalchemy_does_not_rebind_quoted_colons():
    from sqlalchemy import text

    sql = substitute_parameters('SELECT ?', ('value:label',))
    statement = text(sql.replace(':', r'\:'))
    assert not statement._bindparams
    assert str(statement) == sql


def test_legacy_identifiers_cannot_change_statement_structure():
    builder = BaseMatrixOneClient()
    assert builder._build_insert_sql('db.t', {'a`b': 'safe'}) == "INSERT INTO db.t (`a``b`) VALUES ('safe')"
    assert (
        builder._build_insert_sql('t; DROP TABLE t', {'value': 1}) == 'INSERT INTO `t; DROP TABLE t` (value) VALUES (1)'
    )
    assert builder._build_insert_sql('t', {'select': 1}) == 'INSERT INTO t (`select`) VALUES (1)'
    with pytest.raises(ValueError):
        builder._build_insert_sql('bad\x00name', {'v': 1})
