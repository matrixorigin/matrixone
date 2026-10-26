# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import sqlite3
from contextlib import closing
from datetime import date

import pytest

from matrixone._sql_literals import substitute_parameters
from matrixone.sql_builder import MatrixOneSQLBuilder


@pytest.mark.parametrize("bound", [False, True])
def test_substitution_ignores_question_marks_inside_sql_literals(bound):
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE records (id INTEGER, marker TEXT)")
        connection.execute("INSERT INTO records VALUES (7, '?')")

        builder = MatrixOneSQLBuilder().select("id").from_table("records").where("marker = '?' /* ? %s */")
        if bound:
            builder.where("id = ?", 7)
        sql = builder.build_with_parameter_substitution()

        assert connection.execute(sql).fetchall() == [(7,)]


@pytest.mark.parametrize(
    "value, stored_value, column_type",
    [
        (date(2026, 10, 26), "2026-10-26", "TEXT"),
        (b"\x00\xff", b"\x00\xff", "BLOB"),
    ],
)
def test_substitution_preserves_typed_values_during_execution(value, stored_value, column_type):
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute(f"CREATE TABLE records (value {column_type})")
        connection.execute("INSERT INTO records VALUES (?)", (stored_value,))

        sql = (
            MatrixOneSQLBuilder()
            .select("value")
            .from_table("records")
            .where("value = ?", value)
            .build_with_parameter_substitution()
        )

        assert connection.execute(sql).fetchall() == [(stored_value,)]


@pytest.mark.parametrize(
    "condition, params",
    [
        ("value = ? AND id = ?", ("one",)),
        ("value = ?", ("one", 2)),
        ("value = ?", ()),
        ("1 = 1", ("extra",)),
    ],
)
def test_substitution_rejects_placeholder_mismatches(condition, params):
    builder = MatrixOneSQLBuilder().select_all().from_table("records").where(condition, *params)

    with pytest.raises(ValueError):
        builder.build_with_parameter_substitution()


@pytest.mark.parametrize("expression, expected", [("value%size", 1), ("value%s", 3), ("value % size", 1)])
@pytest.mark.parametrize("bound", [False, True])
def test_builder_qmark_contract_preserves_modulo(expression, expected, bound):
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE records(value INT, size INT, s INT)")
        connection.execute("INSERT INTO records VALUES (7, 3, 4)")
        builder = MatrixOneSQLBuilder().select(expression).from_table("records")
        if bound:
            builder.where("value = ?", 7)
        assert connection.execute(builder.build_with_parameter_substitution()).fetchall() == [(expected,)]


def test_shared_renderer_retains_legacy_placeholder_conventions():
    assert substitute_parameters("SELECT %s, ?", (7, 8)) == "SELECT 7, 8"
    assert substitute_parameters("SELECT :v", {"v": 7}, allow_format_placeholders=False) == "SELECT 7"


def test_qmark_shortcut_does_not_accept_falsy_unsupported_parameters():
    with pytest.raises(TypeError, match="SQL parameters must be"):
        substitute_parameters("SELECT 1", 0, allow_format_placeholders=False)
