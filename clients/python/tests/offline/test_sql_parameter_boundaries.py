# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import numpy as np
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


@pytest.mark.parametrize(
    'value,expected',
    [
        (np.int8(-3), '-3'),
        (np.int64(2**63 - 1), '9223372036854775807'),
        (np.uint64(2**64 - 1), '18446744073709551615'),
        (np.float16(1.25), '1.25'),
        (np.float32(1.25), '1.25'),
        (np.float64(1.25), '1.25'),
        (np.bool_(True), '1'),
        (np.bool_(False), '0'),
    ],
)
def test_numpy_scalars_preserve_sql_types_across_builders(value, expected):
    assert sql_literal(value) == expected
    assert substitute_parameters('SELECT ?', (value,)) == 'SELECT ' + expected
    builder = BaseMatrixOneClient()
    expected_insert = 'INSERT INTO t (v) VALUES (' + expected + ')'
    assert builder._build_insert_sql('t', {'v': value}) == expected_insert
    assert builder._build_batch_insert_sql('t', [{'v': value}]) == expected_insert
    assert _VectorManagerBase._build_insert_sql('t', {'v': value}) == expected_insert


@pytest.mark.parametrize('value', [np.float16('nan'), np.float32('inf'), np.float64('-inf')])
def test_nonfinite_numpy_scalars_rejected_before_write(value):
    with pytest.raises(ValueError, match='Non-finite'):
        sql_literal(value)


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


@pytest.mark.parametrize(
    'sql,params,expected',
    [
        ('SELECT ? // ? :ignored %s', (7,), 'SELECT 7 // ? :ignored %s'),
        ('SELECT :v // :ignored', {'v': 7}, 'SELECT 7 // :ignored'),
        ('SELECT ? // ?\n, ?', (7, 8), 'SELECT 7 // ?\n, 8'),
        ('SELECT :v // :ignored\n, :next', {'v': 7, 'next': 8}, 'SELECT 7 // :ignored\n, 8'),
        ("SELECT $$O'Reilly ? :ignored %s$$, ?", (7,), "SELECT $$O'Reilly ? :ignored %s$$, 7"),
        ("SELECT $txt$O'Reilly$txt$, :v", {'v': 7}, "SELECT $txt$O'Reilly$txt$, 7"),
        ('SELECT $9$? /* :ignored */$9$, ?', (7,), 'SELECT $9$? /* :ignored */$9$, 7'),
        ('SELECT $中文$? // :ignored$中文$, :v', {'v': 7}, 'SELECT $中文$? // :ignored$中文$, 7'),
        ('SELECT $$a\\b$$, ?', (7,), 'SELECT $$a\\b$$, 7'),
        ('SELECT 1 AS name$tag$, ?', (7,), 'SELECT 1 AS name$tag$, 7'),
        ('SELECT 1 AS name$$, :v', {'v': 7}, 'SELECT 1 AS name$$, 7'),
        ('SELECT $name, ?', (7,), 'SELECT $name, 7'),
        ('SELECT 1.0$$?$$, ?', (7,), 'SELECT 1.0$$?$$, 7'),
        ('SELECT .1$tag$? :ignored$tag$, :v', {'v': 7}, 'SELECT .1$tag$? :ignored$tag$, 7'),
        ('SELECT 1e+2$$?$$, ?', (7,), 'SELECT 1e+2$$?$$, 7'),
        ('SELECT 1.2e3$$?$$, ?', (7,), 'SELECT 1.2e3$$?$$, 7'),
        ('SELECT 1$tag$, ?', (7,), 'SELECT 1$tag$, 7'),
        ('SELECT 1e2$tag$, ?', (7,), 'SELECT 1e2$tag$, 7'),
        ('SELECT /*!50000$$?$$*/, ?', (7,), 'SELECT /*!50000$$?$$*/, 7'),
        ("SELECT '//', ?", (7,), "SELECT '//', 7"),
        ('SELECT /*!50000 $$?$$, ? */', (7,), 'SELECT /*!50000 $$?$$, 7 */'),
    ],
)
def test_native_comment_and_dollar_token_boundaries(sql, params, expected):
    assert substitute_parameters(sql, params) == expected


@pytest.mark.parametrize(
    'sql',
    ['SELECT $$open, ?', 'SELECT $tag$open$other$, ?', 'SELECT $Tag$x$tag$, ?', 'SELECT $$first$wrong$ trailing$$, ?'],
)
def test_invalid_dollar_strings_do_not_search_past_first_dollar(sql):
    with pytest.raises(ValueError, match='SQL dollar string'):
        substitute_parameters(sql, (7,))


@pytest.mark.parametrize('keyword', ['window', 'WINDOW', 'integer', 'rank', 'rows', 'constraint', 'over'])
def test_shared_builders_quote_mysql_keywords(keyword):
    row = {keyword: 7}
    expected = 'INSERT INTO db.t (`%s`) VALUES (7)' % keyword
    client = BaseMatrixOneClient()
    assert client._build_insert_sql('db.t', row) == expected
    assert client._build_batch_insert_sql('db.t', [row]) == expected
    assert _VectorManagerBase._build_insert_sql('db.t', row) == expected


@pytest.mark.parametrize(
    'value,expected',
    [
        (timedelta(), "'0:00:00'"),
        (timedelta(hours=1, minutes=2, seconds=3), "'1:02:03'"),
        (timedelta(microseconds=-1), "'-0:00:00.000001'"),
        (timedelta(minutes=-30), "'-0:30:00'"),
        (timedelta(hours=26, seconds=3, microseconds=4), "'26:00:03.000004'"),
        (-timedelta(hours=26, seconds=3, microseconds=4), "'-26:00:03.000004'"),
        (timedelta.min, "'-23999999976:00:00'"),
        (timedelta.max, "'23999999999:59:59.999999'"),
        (UUID('12345678-1234-5678-1234-567812345678'), "'12345678-1234-5678-1234-567812345678'"),
        (datetime(2026, 10, 25, 1, 2, 3), "'2026-10-25 01:02:03'"),
    ],
)
def test_duration_and_uuid_scalar_literals(value, expected):
    assert sql_literal(value) == expected


@pytest.mark.parametrize('value', [timedelta(seconds=3723), UUID(int=1)])
def test_duration_and_uuid_share_parameter_and_builder_encoding(value):
    literal = "'1:02:03'" if isinstance(value, timedelta) else "'00000000-0000-0000-0000-000000000001'"
    assert substitute_parameters('SELECT ?, :ignored', (value,)) == 'SELECT ' + literal + ', :ignored'
    assert substitute_parameters('SELECT :v', {'v': value}) == 'SELECT ' + literal
    expected = 'INSERT INTO t (v) VALUES (' + literal + ')'
    client = BaseMatrixOneClient()
    assert client._build_insert_sql('t', {'v': value}) == expected
    assert client._build_batch_insert_sql('t', [{'v': value}]) == expected
    assert _VectorManagerBase._build_insert_sql('t', {'v': value}) == expected


@pytest.mark.parametrize('value', [timedelta(seconds=1), UUID(int=1)])
def test_duration_and_uuid_do_not_expand_json_allowlist(value):
    with pytest.raises(TypeError, match='Unsupported JSON parameter type'):
        sql_literal({'nested': value})
