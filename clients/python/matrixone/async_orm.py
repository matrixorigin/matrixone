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
        if not self.database:
            return super()._qualified_table_name()
        from ._sql_literals import sql_identifier

        if self._source_selectable is not None:
            from sqlalchemy import table as sqlalchemy_table
            from sqlalchemy.sql.elements import _anonymous_label, quoted_name
            from sqlalchemy.sql.selectable import AliasedReturnsRows, TableClause
            from ._query_compilation import compile_from

            physical = self._source_selectable
            while isinstance(physical, AliasedReturnsRows):
                physical = physical.element
            if not isinstance(physical, TableClause):
                raise ValueError('Explicit database requires a physical table source')
            if physical.schema and physical.schema != self.database:
                raise ValueError('Explicit database conflicts with mapped table schema')
            database_sql, table_sql = sql_identifier(self.database), sql_identifier(physical.name)
            qualified = database_sql + '.' + table_sql
            if self._root_selectable is not physical:
                name = self._root_selectable.name
                if not isinstance(name, _anonymous_label):
                    return qualified + ' AS ' + sql_identifier(name)
                name_in_db = quoted_name(str(physical.name), table_sql.startswith('`'))
                database = quoted_name(str(self.database), database_sql.startswith('`'))
                return compile_from(sqlalchemy_table(name_in_db, schema=database).alias(name))
            return qualified
        table = super()._qualified_table_name()
        if not table:
            return table
        if any(sql_identifier(cte.get('name') if isinstance(cte, dict) else cte.name) == table for cte in self._ctes):
            return table  # A CTE is statement-local, never a database table.
        from ._sql_literals import identifier_parts

        parts = identifier_parts(table, qualified=True)
        if len(parts) > 1:
            if len(parts) != 2 or parts[0] != self.database:
                raise ValueError('Explicit database conflicts with qualified table name')
            return '.'.join(sql_identifier(part) for part in parts)
        return sql_identifier(self.database) + '.' + sql_identifier(parts[0])

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
