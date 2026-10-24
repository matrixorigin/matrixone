# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise SDK query terminal operations against a real SQL executor.

SQLite executes the shared SELECT subset; this is not MatrixOne server BVT.
"""

import inspect
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from matrixone import AsyncClient, Client


Base = declarative_base()


class QueryRow(Base):
    __tablename__ = "query_rows"
    id = Column(Integer, primary_key=True)
    category = Column(String(10))


@pytest.fixture
def make_query():
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(
            "CREATE TABLE query_rows(id INTEGER, category TEXT);"
            "INSERT INTO query_rows VALUES (10,'a'),(20,'a'),(30,'b'),(40,'c');"
        )

        def execute(sql, params=None):
            return SimpleNamespace(rows=connection.execute(sql, params or []).fetchall())

        async def async_execute(sql, params=None):
            return execute(sql, params)

        def build(kind):
            if kind == "async":
                client = AsyncClient()
                client.execute = async_execute
                return client.query(QueryRow)
            client = Client()
            client.execute = execute
            return client.query(QueryRow)

        yield build
    finally:
        connection.close()


async def resolve(result):
    return await result if inspect.isawaitable(result) else result


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sync", "async"])
@pytest.mark.parametrize(
    "shape,expected",
    [("projection", 4), ("grouped", 3), ("distinct", 3), ("aggregate", 1), ("page", 2), ("empty", 0)],
)
async def test_count_reports_query_cardinality(make_query, kind, shape, expected):
    query = make_query(kind)
    if shape == "projection":
        query.select("id")
    elif shape == "grouped":
        query.select("category").group_by("category")
    elif shape == "distinct":
        query.select("DISTINCT category")
    elif shape == "aggregate":
        query.select("SUM(id) AS total")
    elif shape == "page":
        query.order_by("id").limit(2).offset(1)
    else:
        query.limit(0)
    assert await resolve(query.count()) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sync", "async"])
async def test_count_preserves_filter_parameters(make_query, kind):
    query = make_query(kind).filter("id > ?", 10).select("id").order_by("id")
    assert await resolve(query.count()) == 3
    assert [row.id for row in await resolve(query.all())] == [20, 30, 40]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sync", "async"])
async def test_first_preserves_original_page(make_query, kind):
    query = make_query(kind).order_by("id").limit(3).offset(1)
    assert (await resolve(query.first())).id == 20
    assert [row.id for row in await resolve(query.all())] == [20, 30, 40]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sync", "async"])
async def test_failed_first_preserves_original_page(make_query, kind):
    query = make_query(kind).order_by("id").limit(3).offset(1)
    with patch.object(query.client, "execute", side_effect=RuntimeError("executor failed")):
        with pytest.raises(RuntimeError, match="executor failed"):
            await resolve(query.first())
    assert [row.id for row in await resolve(query.all())] == [20, 30, 40]


@pytest.mark.asyncio
async def test_async_database_prefix_restored_after_build_failure(make_query):
    class InvalidProjection:
        def compile(self, **kwargs):
            raise ValueError("invalid projection")

    query = make_query("async")
    query.database = "main"  # SQLite's attached default database.
    query.select(InvalidProjection())
    with pytest.raises(ValueError, match="invalid projection"):
        await query.all()
    query.select("id").order_by("id")
    assert [row.id for row in await query.all()] == [10, 20, 30, 40]
