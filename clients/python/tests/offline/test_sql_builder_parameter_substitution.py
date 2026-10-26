# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import sqlite3
from datetime import date

import pytest

from matrixone.sql_builder import MatrixOneSQLBuilder


def test_substitution_ignores_question_marks_inside_sql_literals():
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE records (id INTEGER, marker TEXT)")
    connection.execute("INSERT INTO records VALUES (7, '?')")

    sql = (
        MatrixOneSQLBuilder()
        .select("id")
        .from_table("records")
        .where("marker = '?'")
        .where("id = ?", 7)
        .build_with_parameter_substitution()
    )

    assert connection.execute(sql).fetchall() == [(7,)]


@pytest.mark.parametrize(
    "value, stored_value, column_type",
    [
        (date(2026, 10, 26), "2026-10-26", "TEXT"),
        (b"\x00\xff", b"\x00\xff", "BLOB"),
    ],
)
def test_substitution_preserves_typed_values_during_execution(value, stored_value, column_type):
    connection = sqlite3.connect(":memory:")
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
    ],
)
def test_substitution_rejects_placeholder_mismatches(condition, params):
    builder = MatrixOneSQLBuilder().select_all().from_table("records").where(condition, *params)

    with pytest.raises(ValueError):
        builder.build_with_parameter_substitution()
