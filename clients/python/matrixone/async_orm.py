"""
MatrixOne Async ORM - SQLAlchemy-like interface for MatrixOne database (Async)
"""

from typing import Any, List, Optional, Type, TypeVar

from .orm import Model

T = TypeVar("T")


class AsyncMatrixOneQuery:
    """Async MatrixOne Query builder that mimics SQLAlchemy Query interface"""

    def __init__(self, model_class: Type[Model], client, database: str = None):
        self.model_class = model_class
        self.client = client
        self.database = database
        self._snapshot_name = None
        self._where_conditions = []
        self._where_params = []
        self._order_by_columns = []
        self._limit_count = None
        self._offset_count = None

    def snapshot(self, snapshot_name: str) -> "AsyncMatrixOneQuery":
        """Add snapshot support - SQLAlchemy style chaining"""
        self._snapshot_name = snapshot_name
        return self

    def filter_by(self, **kwargs) -> "AsyncMatrixOneQuery":
        """Add WHERE conditions from keyword arguments - SQLAlchemy style"""
        for key, value in kwargs.items():
            if key in self.model_class._columns:
                if isinstance(value, str):
                    self._where_conditions.append(f"{key} = '{value}'")
                else:
                    self._where_conditions.append(f"{key} = {value}")
        return self

    def filter(self, condition: str, *params) -> "AsyncMatrixOneQuery":
        """Add WHERE condition - SQLAlchemy style"""
        # Replace ? placeholders with actual values
        formatted_condition = condition
        for param in params:
            if isinstance(param, str):
                formatted_condition = formatted_condition.replace("?", f"'{param}'", 1)
            else:
                formatted_condition = formatted_condition.replace("?", str(param), 1)
        self._where_conditions.append(formatted_condition)
        return self

    def order_by(self, *columns) -> "AsyncMatrixOneQuery":
        """Add ORDER BY clause - SQLAlchemy style"""
        for col in columns:
            if isinstance(col, str):
                self._order_by_columns.append(col)
            else:
                self._order_by_columns.append(str(col))
        return self

    def limit(self, count: int) -> "AsyncMatrixOneQuery":
        """Add LIMIT clause - SQLAlchemy style"""
        self._limit_count = count
        return self

    def offset(self, count: int) -> "AsyncMatrixOneQuery":
        """Add OFFSET clause - SQLAlchemy style"""
        self._offset_count = count
        return self

    def _build_sql(self) -> tuple[str, List[Any]]:
        """Build SQL query"""
        table_name = self.model_class._table_name

        # Add database prefix if provided
        if self.database:
            table_name = f"{self.database}.{table_name}"

        # Build FROM clause
        from_clause = f"FROM {table_name}"

        # Build snapshot hint (must come before WHERE clause)
        snapshot_hint = ""
        if self._snapshot_name:
            snapshot_hint = f"{{snapshot = '{self._snapshot_name}'}}"

        # Build WHERE clause
        where_clause = ""
        params = []
        if self._where_conditions:
            where_clause = "WHERE " + " AND ".join(self._where_conditions)

        # Build ORDER BY clause
        order_by_clause = ""
        if self._order_by_columns:
            order_by_clause = "ORDER BY " + ", ".join(self._order_by_columns)

        # Build LIMIT clause
        limit_clause = ""
        if self._limit_count is not None:
            limit_clause = f"LIMIT {self._limit_count}"

        # Build OFFSET clause
        offset_clause = ""
        if self._offset_count is not None:
            offset_clause = f"OFFSET {self._offset_count}"

        # Combine all clauses
        sql_parts = ["SELECT *", from_clause, snapshot_hint, where_clause, order_by_clause, limit_clause, offset_clause]

        sql = " ".join(filter(None, sql_parts))

        return sql, params

    async def all(self) -> List[Model]:
        """Execute query and return all results - SQLAlchemy style"""
        sql, params = self._build_sql()
        result = await self.client.execute(sql, params)

        models = []
        for row in result.rows:
            # Convert row to dictionary
            row_dict = {}
            for i, col_name in enumerate(self.model_class._columns.keys()):
                if i < len(row):
                    row_dict[col_name] = row[i]
            models.append(self.model_class.from_dict(row_dict))

        return models

    async def first(self) -> Optional[Model]:
        """Execute query and return first result - SQLAlchemy style"""
        self._limit_count = 1
        results = await self.all()
        return results[0] if results else None

    async def count(self) -> int:
        """Execute query and return count of results - SQLAlchemy style"""
        sql, params = self._build_sql()
        # Replace SELECT * with COUNT(*)
        sql = sql.replace("SELECT *", "SELECT COUNT(*)")

        result = await self.client.execute(sql, params)
        return result.rows[0][0] if result.rows else 0
