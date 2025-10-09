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

from typing import Any, List, Optional, TypeVar

from .orm import BaseMatrixOneQuery

T = TypeVar("T")


class AsyncMatrixOneQuery(BaseMatrixOneQuery):
    """Async MatrixOne Query builder that mimics SQLAlchemy Query interface"""

    def __init__(self, model_class, client, database: str = None, transaction_wrapper=None, snapshot=None):
        super().__init__(model_class, client, transaction_wrapper, snapshot)
        self.database = database

    def _build_sql(self) -> tuple[str, List[Any]]:
        """Build SQL query with database prefix support"""
        # Add database prefix if provided
        original_table_name = self._table_name
        if self.database:
            self._table_name = f"{self.database}.{original_table_name}"

        # Call parent's _build_sql method
        sql, params = super()._build_sql()

        # Restore original table name
        self._table_name = original_table_name

        return sql, params

    async def _execute(self, sql, params=None):
        """Execute SQL using either transaction wrapper or client asynchronously"""
        if self.transaction_wrapper:
            return await self.transaction_wrapper.execute(sql, params)
        else:
            return await self.client.execute(sql, params)

    async def all(self) -> List:
        """Execute query and return all results - SQLAlchemy style"""
        sql, params = self._build_sql()
        result = await self._execute(sql, params)

        models = []
        for row in result.rows:
            # Check if this is an aggregate query (has custom select columns)
            if self._select_columns:
                # For aggregate queries, return raw row data as a simple object
                select_cols = self._extract_select_columns()
                row_data = self._create_row_data(row, select_cols)
                models.append(row_data)
            else:
                # Regular model query
                if self._is_sqlalchemy_model:
                    # For SQLAlchemy models, create instance directly
                    row_dict = {}
                    for i, col_name in enumerate(self._columns.keys()):
                        if i < len(row):
                            row_dict[col_name] = row[i]

                    # Create SQLAlchemy model instance
                    model = self.model_class(**row_dict)
                    models.append(model)
                else:
                    # For non-SQLAlchemy models, create instance directly
                    row_dict = {}
                    for i, col_name in enumerate(self._columns.keys()):
                        if i < len(row):
                            row_dict[col_name] = row[i]

                    # Create model instance
                    model = self.model_class(**row_dict)
                    models.append(model)

        return models

    async def first(self) -> Optional:
        """Execute query and return first result - SQLAlchemy style"""
        self._limit_count = 1
        results = await self.all()
        return results[0] if results else None

    async def count(self) -> int:
        """Execute query and return count of results - SQLAlchemy style"""
        sql, params = self._build_sql()
        # Replace SELECT * with COUNT(*)
        sql = sql.replace("SELECT *", "SELECT COUNT(*)")

        result = await self._execute(sql, params)
        return result.rows[0][0] if result.rows else 0

    async def execute(self) -> Any:
        """Execute the query based on its type"""
        if self._query_type == "SELECT":
            sql, params = self._build_sql()
            return await self._execute(sql, params)
        elif self._query_type == "INSERT":
            sql, params = self._build_insert_sql()
            return await self._execute(sql, params)
        elif self._query_type == "UPDATE":
            sql, params = self._build_update_sql()
            return await self._execute(sql, params)
        elif self._query_type == "DELETE":
            sql, params = self._build_delete_sql()
            return await self._execute(sql, params)
        else:
            raise ValueError(f"Unknown query type: {self._query_type}")
