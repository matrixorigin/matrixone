# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import sqlite3
from contextlib import closing

import pytest

from matrixone.sql_builder import MatrixOneSQLBuilder


@pytest.mark.parametrize("operation", ["select", "update", "delete"])
@pytest.mark.parametrize("predicate, expected", [("where_eq", [1]), ("where_ne", [2])])
def test_null_equality_predicates_execute_with_adjacent_parameters(operation, predicate, expected):
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE records(id INT, tenant INT, value TEXT, marked INT)")
        connection.executemany(
            "INSERT INTO records VALUES (?, ?, ?, 0)",
            [(1, 7, None), (2, 7, "present"), (3, 8, None), (4, 8, "present")],
        )
        builder = MatrixOneSQLBuilder()
        if operation == "select":
            builder.select("id").from_table("records").order_by("id")
        elif operation == "update":
            builder.update("records").set(marked=1)
        else:
            builder.delete_from("records")
        builder.where_eq("tenant", 7)
        getattr(builder, predicate)("value", None)
        builder.where_gt("id", 0)
        sql, params = builder.build()
        result = connection.execute(sql, params)

        if operation == "select":
            affected = [row[0] for row in result.fetchall()]
        elif operation == "update":
            affected = [row[0] for row in connection.execute("SELECT id FROM records WHERE marked=1")]
        else:
            remaining = {row[0] for row in connection.execute("SELECT id FROM records")}
            affected = sorted({1, 2, 3, 4} - remaining)
        assert affected == expected
        assert None not in params


@pytest.mark.parametrize("predicate", ["where_eq", "where_ne"])
@pytest.mark.parametrize("value", [0, False, "", "NULL", "present"])
def test_nonnull_equality_keeps_parameter_binding(predicate, value):
    builder = MatrixOneSQLBuilder().select_all().from_table("records")
    getattr(builder, predicate)("value", value)

    sql, params = builder.build()

    operator = "=" if predicate == "where_eq" else "!="
    assert sql == f"SELECT * FROM records WHERE value {operator} ?"
    assert params == [value]
