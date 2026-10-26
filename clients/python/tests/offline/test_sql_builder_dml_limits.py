# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import pytest

from matrixone.sql_builder import MatrixOneSQLBuilder


@pytest.mark.parametrize("operation", ["update", "delete"])
@pytest.mark.parametrize("limit", [0, 1, 5])
@pytest.mark.parametrize("ordered", [False, True])
def test_dml_preserves_explicit_limit_and_order(operation, limit, ordered):
    builder = MatrixOneSQLBuilder()
    if operation == "update":
        builder.update("records").set(marked=1)
        expected = "UPDATE records SET marked = ? WHERE tenant = ?"
        expected_params = [1, 7]
    else:
        builder.delete_from("records")
        expected = "DELETE FROM records WHERE tenant = ?"
        expected_params = [7]
    builder.where_eq("tenant", 7)
    if ordered:
        builder.order_by("created_at DESC", "id ASC")
        expected += " ORDER BY created_at DESC, id ASC"
    builder.limit(limit)

    sql, params = builder.build()

    assert sql == expected + f" LIMIT {limit}"
    assert params == expected_params
    assert builder.clone().build() == (sql, params)


@pytest.mark.parametrize("operation", ["update", "delete"])
def test_dml_order_without_limit_and_reset(operation):
    builder = MatrixOneSQLBuilder()
    if operation == "update":
        builder.update("records").set(marked=1)
    else:
        builder.delete_from("records")
    sql, _ = builder.where_eq("tenant", 7).order_by("id DESC").build()

    assert sql.endswith("WHERE tenant = ? ORDER BY id DESC")
    assert "LIMIT" not in sql
    assert builder.reset().select_all().from_table("records").build() == ("SELECT * FROM records", [])


@pytest.mark.parametrize("operation", ["update", "delete"])
def test_zero_limit_without_where_is_not_discarded(operation):
    builder = MatrixOneSQLBuilder()
    if operation == "update":
        builder.update("records").set(marked=1)
    else:
        builder.delete_from("records")

    sql, _ = builder.limit(5).limit(0).build()

    assert sql.endswith(" LIMIT 0")
