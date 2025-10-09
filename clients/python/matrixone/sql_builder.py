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
Unified SQL Builder for MatrixOne Client

This module provides a centralized SQL building system that eliminates
code duplication across different interfaces (ORM, Vector Search, Pinecone, etc.).
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union


class SQLClause(Enum):
    """SQL clause types"""

    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    JOIN = "JOIN"
    GROUP_BY = "GROUP BY"
    HAVING = "HAVING"
    ORDER_BY = "ORDER BY"
    LIMIT = "LIMIT"
    OFFSET = "OFFSET"
    WITH = "WITH"


class DistanceFunction(Enum):
    """Vector distance functions"""

    L2 = "l2_distance"
    L2_SQ = "l2_distance_sq"
    COSINE = "cosine_distance"
    INNER_PRODUCT = "inner_product"


class MatrixOneSQLBuilder:
    """
    Unified SQL Builder for MatrixOne operations.

    This builder provides a consistent interface for constructing SQL queries
    across all MatrixOne client interfaces, eliminating code duplication.
    """

    def __init__(self):
        self.reset()

    def reset(self) -> "MatrixOneSQLBuilder":
        """Reset the builder to initial state"""
        self._query_type = "SELECT"  # SELECT, INSERT, UPDATE, DELETE
        self._select_columns = []
        self._from_table = None
        self._from_snapshot = None
        self._joins = []
        self._where_conditions = []
        self._where_params = []
        self._group_by_columns = []
        self._having_conditions = []
        self._having_params = []
        self._order_by_columns = []
        self._limit_count = None
        self._offset_count = None
        self._ctes = []
        self._vector_query = None
        # INSERT specific
        self._insert_columns = []
        self._insert_values = []
        # UPDATE specific
        self._update_set_columns = []
        self._update_set_values = []
        return self

    # SELECT clause methods
    def select(self, *columns: Union[str, List[str]]) -> "MatrixOneSQLBuilder":
        """Add SELECT columns"""
        for col in columns:
            if isinstance(col, list):
                self._select_columns.extend(col)
            else:
                self._select_columns.append(str(col))
        return self

    def select_all(self) -> "MatrixOneSQLBuilder":
        """Select all columns (*)"""
        self._select_columns = ["*"]
        return self

    def add_vector_distance(
        self,
        vector_column: str,
        query_vector: List[float],
        distance_func: DistanceFunction = DistanceFunction.L2_SQ,
        alias: str = "distance",
    ) -> "MatrixOneSQLBuilder":
        """Add vector distance calculation to SELECT"""
        vector_str = "[" + ",".join(map(str, query_vector)) + "]"
        distance_expr = f"{distance_func.value}({vector_column}, '{vector_str}') as {alias}"
        self._select_columns.append(distance_expr)
        return self

    # INSERT methods
    def insert_into(self, table_name: str) -> "MatrixOneSQLBuilder":
        """Start INSERT operation"""
        self._query_type = "INSERT"
        self._from_table = table_name
        return self

    def values(self, **kwargs) -> "MatrixOneSQLBuilder":
        """Add values for INSERT"""
        if self._query_type != "INSERT":
            raise ValueError("values() can only be used with INSERT operations")

        if not self._insert_columns:
            self._insert_columns = list(kwargs.keys())

        values_row = []
        for col in self._insert_columns:
            if col in kwargs:
                values_row.append(kwargs[col])
            else:
                values_row.append(None)

        self._insert_values.append(values_row)
        return self

    def insert_values(self, columns: List[str], values: List[List[Any]]) -> "MatrixOneSQLBuilder":
        """Add multiple rows of values for INSERT"""
        if self._query_type != "INSERT":
            raise ValueError("insert_values() can only be used with INSERT operations")

        self._insert_columns = columns
        self._insert_values = values
        return self

    # UPDATE methods
    def update(self, table_name: str) -> "MatrixOneSQLBuilder":
        """Start UPDATE operation"""
        self._query_type = "UPDATE"
        self._from_table = table_name
        return self

    def set(self, **kwargs) -> "MatrixOneSQLBuilder":
        """Add SET clauses for UPDATE"""
        if self._query_type != "UPDATE":
            raise ValueError("set() can only be used with UPDATE operations")

        for key, value in kwargs.items():
            self._update_set_columns.append(f"{key} = ?")
            self._update_set_values.append(value)
        return self

    # DELETE methods
    def delete_from(self, table_name: str) -> "MatrixOneSQLBuilder":
        """Start DELETE operation"""
        self._query_type = "DELETE"
        self._from_table = table_name
        return self

    # FROM clause methods
    def from_table(self, table_name: str, snapshot: Optional[str] = None) -> "MatrixOneSQLBuilder":
        """Set FROM table with optional snapshot"""
        self._from_table = table_name
        self._from_snapshot = snapshot
        return self

    # JOIN clause methods
    def join(self, table: str, on: str, join_type: str = "INNER") -> "MatrixOneSQLBuilder":
        """Add JOIN clause"""
        join_clause = f"{join_type} JOIN {table} ON {on}"
        self._joins.append(join_clause)
        return self

    def left_join(self, table: str, on: str) -> "MatrixOneSQLBuilder":
        """Add LEFT JOIN clause"""
        return self.join(table, on, "LEFT")

    def right_join(self, table: str, on: str) -> "MatrixOneSQLBuilder":
        """Add RIGHT JOIN clause"""
        return self.join(table, on, "RIGHT")

    # WHERE clause methods
    def where(self, condition: str, *params: Any) -> "MatrixOneSQLBuilder":
        """Add WHERE condition"""
        self._where_conditions.append(condition)
        self._where_params.extend(params)
        return self

    def where_eq(self, column: str, value: Any) -> "MatrixOneSQLBuilder":
        """Add WHERE column = value"""
        return self.where(f"{column} = ?", value)

    def where_ne(self, column: str, value: Any) -> "MatrixOneSQLBuilder":
        """Add WHERE column != value"""
        return self.where(f"{column} != ?", value)

    def where_gt(self, column: str, value: Any) -> "MatrixOneSQLBuilder":
        """Add WHERE column > value"""
        return self.where(f"{column} > ?", value)

    def where_gte(self, column: str, value: Any) -> "MatrixOneSQLBuilder":
        """Add WHERE column >= value"""
        return self.where(f"{column} >= ?", value)

    def where_lt(self, column: str, value: Any) -> "MatrixOneSQLBuilder":
        """Add WHERE column < value"""
        return self.where(f"{column} < ?", value)

    def where_lte(self, column: str, value: Any) -> "MatrixOneSQLBuilder":
        """Add WHERE column <= value"""
        return self.where(f"{column} <= ?", value)

    def where_in(self, column: str, values: List[Any]) -> "MatrixOneSQLBuilder":
        """Add WHERE column IN (values)"""
        if not values:
            return self.where("1=0")  # Always false for empty IN
        placeholders = ",".join(["?" for _ in values])
        return self.where(f"{column} IN ({placeholders})", *values)

    def where_nin(self, column: str, values: List[Any]) -> "MatrixOneSQLBuilder":
        """Add WHERE column NOT IN (values)"""
        if not values:
            return self.where("1=1")  # Always true for empty NOT IN
        placeholders = ",".join(["?" for _ in values])
        return self.where(f"{column} NOT IN ({placeholders})", *values)

    def where_like(self, column: str, pattern: str) -> "MatrixOneSQLBuilder":
        """Add WHERE column LIKE pattern"""
        return self.where(f"{column} LIKE ?", pattern)

    def where_between(self, column: str, start: Any, end: Any) -> "MatrixOneSQLBuilder":
        """Add WHERE column BETWEEN start AND end"""
        return self.where(f"{column} BETWEEN ? AND ?", start, end)

    def where_and(self, *conditions: str) -> "MatrixOneSQLBuilder":
        """Add WHERE condition with AND logic"""
        if conditions:
            combined = " AND ".join(f"({cond})" for cond in conditions)
            return self.where(combined)
        return self

    def where_or(self, *conditions: str) -> "MatrixOneSQLBuilder":
        """Add WHERE condition with OR logic"""
        if conditions:
            combined = " OR ".join(f"({cond})" for cond in conditions)
            return self.where(combined)
        return self

    # GROUP BY clause methods
    def group_by(self, *columns: str) -> "MatrixOneSQLBuilder":
        """Add GROUP BY columns"""
        self._group_by_columns.extend(columns)
        return self

    # HAVING clause methods
    def having(self, condition: str, *params: Any) -> "MatrixOneSQLBuilder":
        """
        Add HAVING condition to the SQL query.

        The HAVING clause is used to filter groups after GROUP BY operations,
        similar to WHERE clause but applied to aggregated results.

        Args:

            condition (str): The HAVING condition as a string.
                           Can include '?' placeholders for parameter substitution.
            *params (Any): Parameters to replace '?' placeholders in the condition.

        Returns:

            MatrixOneSQLBuilder: Self for method chaining.

        Examples:

            # Basic HAVING with placeholders
            builder.group_by("department")
            builder.having("COUNT(*) > ?", 5)
            builder.having("AVG(age) > ?", 25)

            # HAVING without placeholders
            builder.group_by("department")
            builder.having("COUNT(*) > 5")
            builder.having("AVG(age) > 25")

            # Multiple HAVING conditions
            builder.group_by("department")
            builder.having("COUNT(*) > ?", 5)
            builder.having("AVG(age) > ?", 25)
            builder.having("MAX(age) < ?", 65)

            # Complex HAVING conditions
            builder.group_by("department", "status")
            builder.having("COUNT(*) > ? AND AVG(salary) > ?", 10, 50000)
            builder.having("SUM(revenue) > ?", 1000000)

        Notes:
            - HAVING clauses are typically used with GROUP BY operations
            - Use '?' placeholders for safer parameter substitution
            - Multiple HAVING conditions are combined with AND logic
            - This is a low-level SQL builder - for ORM usage, prefer MatrixOneQuery

        Raises:

            ValueError: If condition is not a string
        """
        self._having_conditions.append(condition)
        self._having_params.extend(params)
        return self

    # ORDER BY clause methods
    def order_by(self, *columns: str) -> "MatrixOneSQLBuilder":
        """Add ORDER BY columns"""
        self._order_by_columns.extend(columns)
        return self

    def order_by_distance(self, alias: str = "distance") -> "MatrixOneSQLBuilder":
        """Add ORDER BY distance (for vector queries)"""
        return self.order_by(alias)

    # LIMIT/OFFSET clause methods
    def limit(self, count: int) -> "MatrixOneSQLBuilder":
        """Add LIMIT clause"""
        self._limit_count = count
        return self

    def offset(self, count: int) -> "MatrixOneSQLBuilder":
        """Add OFFSET clause"""
        self._offset_count = count
        return self

    # CTE (Common Table Expression) methods
    def with_cte(self, name: str, sql: str, *params: Any) -> "MatrixOneSQLBuilder":
        """Add CTE (Common Table Expression)"""
        self._ctes.append({"name": name, "sql": sql, "params": list(params)})
        return self

    # Vector query specific methods
    def vector_similarity_search(
        self,
        table_name: str,
        vector_column: str,
        query_vector: List[float],
        distance_func: DistanceFunction = DistanceFunction.L2_SQ,
        limit: int = 10,
        select_columns: Optional[List[str]] = None,
        where_conditions: Optional[List[str]] = None,
        where_params: Optional[List[Any]] = None,
    ) -> "MatrixOneSQLBuilder":
        """Build vector similarity search query"""
        # Reset and build vector query
        self.reset()

        # Set up SELECT clause
        if select_columns:
            self.select(*select_columns)
            # Ensure vector column is included for distance calculation
            if vector_column not in select_columns:
                self.select(vector_column)
        else:
            self.select_all()

        # Add distance calculation
        self.add_vector_distance(vector_column, query_vector, distance_func)

        # Set FROM table
        self.from_table(table_name)

        # Add WHERE conditions if provided
        if where_conditions:
            for condition in where_conditions:
                self._where_conditions.append(condition)
            if where_params:
                self._where_params.extend(where_params)

        # Add ORDER BY distance and LIMIT
        self.order_by_distance().limit(limit)

        return self

    # SQL building methods
    def build(self) -> Tuple[str, List[Any]]:
        """Build the final SQL query and return (sql, params)"""
        if self._query_type == "INSERT":
            return self._build_insert()
        elif self._query_type == "UPDATE":
            return self._build_update()
        elif self._query_type == "DELETE":
            return self._build_delete()
        else:  # SELECT
            return self._build_select()

    def _build_select(self) -> Tuple[str, List[Any]]:
        """Build SELECT query"""
        sql_parts = []
        all_params = []

        # Build WITH clause (CTEs)
        if self._ctes:
            with_parts = []
            for cte in self._ctes:
                with_parts.append(f"{cte['name']} AS ({cte['sql']})")
                all_params.extend(cte["params"])
            sql_parts.append("WITH " + ", ".join(with_parts))

        # Build SELECT clause
        if self._select_columns:
            select_clause = "SELECT " + ", ".join(self._select_columns)
        else:
            select_clause = "SELECT *"
        sql_parts.append(select_clause)

        # Build FROM clause
        if self._from_table:
            from_clause = f"FROM {self._from_table}"
            if self._from_snapshot:
                from_clause += f"{{snapshot = '{self._from_snapshot}'}}"
            sql_parts.append(from_clause)

        # Build JOIN clauses
        if self._joins:
            sql_parts.append(" ".join(self._joins))

        # Build WHERE clause
        if self._where_conditions:
            where_clause = "WHERE " + " AND ".join(self._where_conditions)
            sql_parts.append(where_clause)
            all_params.extend(self._where_params)

        # Build GROUP BY clause
        if self._group_by_columns:
            group_clause = "GROUP BY " + ", ".join(self._group_by_columns)
            sql_parts.append(group_clause)

        # Build HAVING clause
        if self._having_conditions:
            having_clause = "HAVING " + " AND ".join(self._having_conditions)
            sql_parts.append(having_clause)
            all_params.extend(self._having_params)

        # Build ORDER BY clause
        if self._order_by_columns:
            order_clause = "ORDER BY " + ", ".join(self._order_by_columns)
            sql_parts.append(order_clause)

        # Build LIMIT clause
        if self._limit_count is not None:
            sql_parts.append(f"LIMIT {self._limit_count}")

        # Build OFFSET clause
        if self._offset_count is not None:
            sql_parts.append(f"OFFSET {self._offset_count}")

        # Combine all parts
        sql = " ".join(sql_parts)

        return sql, all_params

    def _build_insert(self) -> Tuple[str, List[Any]]:
        """Build INSERT query"""
        if not self._insert_values:
            raise ValueError("No values provided for INSERT")

        # Handle both dict and list formats
        if self._insert_columns and self._insert_values:
            # List format: columns and values are separate
            columns_str = ", ".join(self._insert_columns)
            values_parts = []
            params = []

            for values_row in self._insert_values:
                placeholders = []
                for value in values_row:
                    if value is None:
                        placeholders.append("NULL")
                    else:
                        placeholders.append("?")
                        params.append(value)
                values_parts.append(f"({', '.join(placeholders)})")

            values_str = ", ".join(values_parts)
            sql = f"INSERT INTO {self._from_table} ({columns_str}) VALUES {values_str}"
            return sql, params
        else:
            # Dict format: values are dictionaries
            all_columns = set()
            for values in self._insert_values:
                if isinstance(values, dict):
                    all_columns.update(values.keys())

            columns = list(all_columns)
            columns_str = ", ".join(columns)

            # Build VALUES clause
            values_parts = []
            params = []

            for values in self._insert_values:
                placeholders = []
                for col in columns:
                    if col in values:
                        placeholders.append("?")
                        params.append(values[col])
                    else:
                        placeholders.append("NULL")
                values_parts.append(f"({', '.join(placeholders)})")

            values_str = ", ".join(values_parts)
            sql = f"INSERT INTO {self._from_table} ({columns_str}) VALUES {values_str}"
            return sql, params

    def _build_update(self) -> Tuple[str, List[Any]]:
        """Build UPDATE query"""
        if not self._update_set_columns:
            raise ValueError("No SET clauses provided for UPDATE")

        # Build SET clause
        set_clause = ", ".join(self._update_set_columns)

        # Build WHERE clause
        where_clause = ""
        params = self._update_set_values.copy()
        if self._where_conditions:
            where_clause = "WHERE " + " AND ".join(self._where_conditions)
            params.extend(self._where_params)

        sql = f"UPDATE {self._from_table} SET {set_clause} {where_clause}"
        return sql, params

    def _build_delete(self) -> Tuple[str, List[Any]]:
        """Build DELETE query"""
        # Build WHERE clause
        where_clause = ""
        params = []
        if self._where_conditions:
            where_clause = "WHERE " + " AND ".join(self._where_conditions)
            params.extend(self._where_params)

        sql = f"DELETE FROM {self._from_table} {where_clause}"
        return sql, params

    def build_with_parameter_substitution(self) -> str:
        """
        Build SQL with direct parameter substitution for MatrixOne compatibility.

        MatrixOne doesn't support parameterized queries in all contexts,
        so this method substitutes parameters directly into the SQL string.
        """
        sql, params = self.build()

        # Replace parameters directly in SQL for MatrixOne compatibility
        if params:
            for param in params:
                if isinstance(param, str):
                    # Escape single quotes in string parameters
                    escaped_param = param.replace("'", "''")
                    sql = sql.replace("?", f"'{escaped_param}'", 1)
                else:
                    sql = sql.replace("?", str(param), 1)

        return sql

    # Utility methods
    def clone(self) -> "MatrixOneSQLBuilder":
        """Create a copy of the current builder"""
        clone = MatrixOneSQLBuilder()
        clone._select_columns = self._select_columns.copy()
        clone._from_table = self._from_table
        clone._from_snapshot = self._from_snapshot
        clone._joins = self._joins.copy()
        clone._where_conditions = self._where_conditions.copy()
        clone._where_params = self._where_params.copy()
        clone._group_by_columns = self._group_by_columns.copy()
        clone._having_conditions = self._having_conditions.copy()
        clone._having_params = self._having_params.copy()
        clone._order_by_columns = self._order_by_columns.copy()
        clone._limit_count = self._limit_count
        clone._offset_count = self._offset_count
        clone._ctes = self._ctes.copy()
        return clone

    def __str__(self) -> str:
        """String representation of the query"""
        return self.build_with_parameter_substitution()


# Convenience functions for common operations
def build_vector_similarity_query(
    table_name: str,
    vector_column: str,
    query_vector: List[float],
    distance_func: DistanceFunction = DistanceFunction.L2_SQ,
    limit: int = 10,
    select_columns: Optional[List[str]] = None,
    where_conditions: Optional[List[str]] = None,
    where_params: Optional[List[Any]] = None,
    use_parameter_substitution: bool = True,
) -> Union[str, Tuple[str, List[Any]]]:
    """
    Convenience function to build vector similarity search query.

    Args:

        table_name: Name of the table to query
        vector_column: Name of the vector column
        query_vector: Query vector for similarity search
        distance_func: Distance function to use
        limit: Maximum number of results
        select_columns: Columns to select (None for all)
        where_conditions: Additional WHERE conditions
        where_params: Parameters for WHERE conditions
        use_parameter_substitution: Whether to substitute parameters directly

    Returns:

        SQL string (if use_parameter_substitution=True) or (sql, params) tuple
    """
    builder = MatrixOneSQLBuilder().vector_similarity_search(
        table_name=table_name,
        vector_column=vector_column,
        query_vector=query_vector,
        distance_func=distance_func,
        limit=limit,
        select_columns=select_columns,
        where_conditions=where_conditions,
        where_params=where_params,
    )

    if use_parameter_substitution:
        return builder.build_with_parameter_substitution()
    else:
        return builder.build()


def build_select_query(
    table_name: str,
    select_columns: Optional[List[str]] = None,
    where_conditions: Optional[List[str]] = None,
    where_params: Optional[List[Any]] = None,
    order_by: Optional[List[str]] = None,
    limit: Optional[int] = None,
    use_parameter_substitution: bool = True,
) -> Union[str, Tuple[str, List[Any]]]:
    """
    Convenience function to build SELECT query.

    Args:

        table_name: Name of the table to query
        select_columns: Columns to select (None for all)
        where_conditions: WHERE conditions
        where_params: Parameters for WHERE conditions
        order_by: ORDER BY columns
        limit: LIMIT count
        use_parameter_substitution: Whether to substitute parameters directly

    Returns:

        SQL string (if use_parameter_substitution=True) or (sql, params) tuple
    """
    builder = MatrixOneSQLBuilder()

    if select_columns:
        builder.select(*select_columns)
    else:
        builder.select_all()

    builder.from_table(table_name)

    if where_conditions:
        for condition in where_conditions:
            builder._where_conditions.append(condition)
        if where_params:
            builder._where_params.extend(where_params)

    if order_by:
        builder.order_by(*order_by)

    if limit:
        builder.limit(limit)

    if use_parameter_substitution:
        return builder.build_with_parameter_substitution()
    else:
        return builder.build()


def build_insert_query(
    table_name: str,
    values: Union[Dict[str, Any], List[Dict[str, Any]]],
    use_parameter_substitution: bool = False,
) -> Union[str, Tuple[str, List[Any]]]:
    """
    Build INSERT query using unified SQL builder.

    Args:

        table_name: Name of the table
        values: Single row dict or list of row dicts
        use_parameter_substitution: Whether to substitute parameters directly

    Returns:

        SQL string (if use_parameter_substitution=True) or (sql, params) tuple
    """
    builder = MatrixOneSQLBuilder()
    builder.insert_into(table_name)

    if isinstance(values, dict):
        builder.values(**values)
    else:
        for row in values:
            builder.values(**row)

    if use_parameter_substitution:
        return builder.build_with_parameter_substitution()
    else:
        return builder.build()


def build_update_query(
    table_name: str,
    set_values: Dict[str, Any],
    where_conditions: Optional[List[str]] = None,
    where_params: Optional[List[Any]] = None,
    use_parameter_substitution: bool = False,
) -> Union[str, Tuple[str, List[Any]]]:
    """
    Build UPDATE query using unified SQL builder.

    Args:

        table_name: Name of the table
        set_values: Dictionary of column=value pairs to update
        where_conditions: WHERE conditions
        where_params: Parameters for WHERE conditions
        use_parameter_substitution: Whether to substitute parameters directly

    Returns:

        SQL string (if use_parameter_substitution=True) or (sql, params) tuple
    """
    builder = MatrixOneSQLBuilder()
    builder.update(table_name).set(**set_values)

    if where_conditions:
        for condition in where_conditions:
            builder._where_conditions.append(condition)
        if where_params:
            builder._where_params.extend(where_params)

    if use_parameter_substitution:
        return builder.build_with_parameter_substitution()
    else:
        return builder.build()


def build_delete_query(
    table_name: str,
    where_conditions: Optional[List[str]] = None,
    where_params: Optional[List[Any]] = None,
    use_parameter_substitution: bool = False,
) -> Union[str, Tuple[str, List[Any]]]:
    """
    Build DELETE query using unified SQL builder.

    Args:

        table_name: Name of the table
        where_conditions: WHERE conditions
        where_params: Parameters for WHERE conditions
        use_parameter_substitution: Whether to substitute parameters directly

    Returns:

        SQL string (if use_parameter_substitution=True) or (sql, params) tuple
    """
    builder = MatrixOneSQLBuilder()
    builder.delete_from(table_name)

    if where_conditions:
        for condition in where_conditions:
            builder._where_conditions.append(condition)
        if where_params:
            builder._where_params.extend(where_params)

    if use_parameter_substitution:
        return builder.build_with_parameter_substitution()
    else:
        return builder.build()


def build_create_index_query(
    index_name: str, table_name: str, column_name: str, index_type: str = "ivfflat", **kwargs
) -> str:
    """
    Convenience function to build CREATE INDEX query.

    Args:

        index_name: Name of the index
        table_name: Name of the table
        column_name: Name of the column to index
        index_type: Type of index (ivfflat, hnsw, etc.)
        **kwargs: Additional index parameters

    Returns:

        CREATE INDEX SQL string
    """
    sql_parts = [f"CREATE INDEX {index_name} USING {index_type} ON {table_name}({column_name})"]

    # Add parameters based on index type
    if index_type.lower() == "ivfflat" and "lists" in kwargs:
        sql_parts.append(f"lists = {kwargs['lists']}")
    elif index_type.lower() == "hnsw":
        if "m" in kwargs:
            sql_parts.append(f"M {kwargs['m']}")
        if "ef_construction" in kwargs:
            sql_parts.append(f"EF_CONSTRUCTION {kwargs['ef_construction']}")
        if "ef_search" in kwargs:
            sql_parts.append(f"EF_SEARCH {kwargs['ef_search']}")
    elif index_type.lower() == "fulltext":
        # For fulltext indexes, use CREATE FULLTEXT INDEX syntax
        sql_parts = [f"CREATE FULLTEXT INDEX {index_name} ON {table_name}({column_name})"]
        if "algorithm" in kwargs:
            sql_parts.append(f"WITH PARSER {kwargs['algorithm']}")

    # Add operation type
    if "op_type" in kwargs:
        sql_parts.append(f"op_type '{kwargs['op_type']}'")

    return " ".join(sql_parts)
