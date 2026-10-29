# Copyright 2021 - 2022 Matrix Origin
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

"""
MatrixOne Async ORM - SQLAlchemy-like interface for MatrixOne database (Async)
"""

from __future__ import annotations

from typing import Any, List, Optional, TypeVar

from .orm import BaseMatrixOneQuery, _count_sql, _first_query

T = TypeVar("T")


class AsyncMatrixOneQuery(BaseMatrixOneQuery):
    """Async MatrixOne Query builder that mimics SQLAlchemy Query interface"""

    def __init__(self, model_class, client, database: str = None, transaction_wrapper=None, snapshot=None):
        super().__init__(model_class, client, transaction_wrapper, snapshot)
        self.database = database

    def _qualified_table_name(self):
        table = super()._qualified_table_name()
        if not self.database:
            return table
        from ._sql_literals import sql_identifier

        if any((cte.get('name') if isinstance(cte, dict) else cte.name) == table for cte in self._ctes):
            return table  # A CTE is a statement-local relation, not a schema table.
        if not table:
            return table
        mapped_table = getattr(self.model_class, '__table__', None)
        if mapped_table is not None:
            if mapped_table.schema and mapped_table.schema != self.database:
                raise ValueError('Explicit database conflicts with mapped table schema')
            return sql_identifier(self.database) + '.' + sql_identifier(mapped_table.name)
        parts = table.split('.')
        if len(parts) > 1:
            if len(parts) != 2 or parts[0] != self.database:
                raise ValueError('Explicit database conflicts with qualified table name')
            return sql_identifier(table, qualified=True)
        return sql_identifier(self.database) + '.' + sql_identifier(table)

    async def _execute(self, sql, params=None):
        """Execute SQL using either transaction wrapper or client asynchronously"""
        if self.transaction_wrapper:
            return await self.transaction_wrapper.execute(sql, params)
        else:
            return await self.client.execute(sql, params)

    async def all(self) -> List:
        sql, params = self._build_sql()
        return self._materialize(await self._execute(sql, params))

    async def first(self) -> Optional:
        """Execute query and return first result - SQLAlchemy style"""
        results = await _first_query(self).all()
        return results[0] if results else None

    async def scalar(self):
        """Return the first projected value by position, preserving its type."""
        query = _first_query(self)
        sql, params = query._build_sql()
        result = await query._execute(sql, params)
        return query._scalar_from_result(result)

    async def count(self) -> int:
        """Execute query and return count of results - SQLAlchemy style"""
        sql, params = self._build_sql()
        result = await self._execute(_count_sql(sql), params)

        # Handle both AsyncResultSet and SQLAlchemy CursorResult
        if hasattr(result, 'rows'):
            # AsyncResultSet from client.execute()
            return result.rows[0][0] if result.rows else 0
        else:
            # SQLAlchemy CursorResult from session.execute()
            rows = result.fetchall()
            return rows[0][0] if rows else 0

    async def execute(self) -> Any:
        """Execute the query based on its type"""
        sql, params = self._build_current_sql()
        return await self._execute(sql, params)

    async def explain(self, verbose=False):
        """Explain the current statement, including UPDATE/DELETE when supported."""
        sql, params = self._build_current_sql()
        return await self._execute(('EXPLAIN VERBOSE ' if verbose else 'EXPLAIN ') + sql, params)

    async def explain_analyze(self, verbose=False):
        """Execute EXPLAIN ANALYZE; mutations can modify data."""
        sql, params = self._build_current_sql()
        prefix = 'EXPLAIN ANALYZE VERBOSE ' if verbose else 'EXPLAIN ANALYZE '
        return await self._execute(prefix + sql, params)
