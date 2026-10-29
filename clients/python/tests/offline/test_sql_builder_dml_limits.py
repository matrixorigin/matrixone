# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import pytest
from unittest.mock import AsyncMock, Mock

from matrixone.async_orm import AsyncMatrixOneQuery
from matrixone.orm import BaseMatrixOneQuery, MatrixOneQuery, Query
from matrixone.sql_builder import MatrixOneSQLBuilder, build_delete_query, build_update_query


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


class Record:
    _table_name = "records"


@pytest.mark.parametrize("query_class", [Query, BaseMatrixOneQuery, MatrixOneQuery])
@pytest.mark.parametrize("operation", ["update", "delete"])
@pytest.mark.parametrize("limit", [0, 1, 5])
def test_sync_orm_forwards_dml_order_and_limit(query_class, operation, limit):
    query = query_class(Record, None).filter("tenant = 7").order_by("id DESC").limit(limit)

    if operation == "update":
        sql, params = query.update(marked=1)._build_update_sql()
    else:
        sql, params = query._build_delete_sql()

    assert sql.endswith(f"WHERE tenant = 7 ORDER BY id DESC LIMIT {limit}")
    assert params == ([1] if operation == "update" else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["update", "delete"])
@pytest.mark.parametrize("limit", [0, 1, 5])
async def test_async_orm_forwards_dml_order_and_limit(operation, limit):
    client = Mock(execute=AsyncMock())
    query = AsyncMatrixOneQuery(Record, client).filter("tenant = 7").order_by("id DESC").limit(limit)

    if operation == "update":
        await query.update(marked=1).execute()
    else:
        await query.delete()

    sql, params = client.execute.call_args.args
    assert sql.endswith(f"WHERE tenant = 7 ORDER BY id DESC LIMIT {limit}")
    assert params == ([1] if operation == "update" else [])


@pytest.mark.parametrize("operation", ["update", "delete"])
def test_convenience_dml_preserves_order_and_limit(operation):
    kwargs = dict(table_name="records", where_conditions=["tenant = ?"], where_params=[7])
    if operation == "update":
        sql, params = build_update_query(**kwargs, set_values={"marked": 1}, order_by=["id DESC"], limit=0)
    else:
        sql, params = build_delete_query(**kwargs, order_by=["id DESC"], limit=0)

    assert sql.endswith("WHERE tenant = ? ORDER BY id DESC LIMIT 0")
    assert params == ([1, 7] if operation == "update" else [7])


def test_expression_update_preserves_assignments_and_limit():
    from sqlalchemy import func

    query = MatrixOneQuery(Record, None).update(marked=1, other=func.abs(-2))
    sql, params = query.filter("tenant = 7").order_by("id DESC").limit(1)._build_update_sql()

    assert "marked = ?, other = abs(-2)" in sql
    assert sql.endswith("WHERE tenant = 7 ORDER BY id DESC LIMIT 1")
    assert params == [1]
