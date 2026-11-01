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
MatrixOne ORM - SQLAlchemy-like interface for MatrixOne database

This module provides a SQLAlchemy-compatible ORM interface for MatrixOne.
It supports both custom MatrixOne models and full SQLAlchemy integration.

For aggregate functions (COUNT, SUM, AVG, etc.), we recommend using SQLAlchemy's func module:
    from sqlalchemy import func
    query.select(func.count("id"))
    query.select(func.sum("amount"))

This provides better type safety and integration with SQLAlchemy.
"""

from __future__ import annotations

import logging
from ._query_compilation import (
    conjoin_conditions,
    compile_expression,
    compile_predicate,
    compile_from,
    selectable_of,
    statement_compilation,
    compile_cte,
    normalize_expression,
)
from ._sql_literals import sql_identifier, sql_literal, substitute_parameters
from copy import copy
from typing import Any, Dict, List, Optional, TypeVar

# SQLAlchemy compatibility import
try:
    from sqlalchemy.orm import declarative_base
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Export declarative_base for direct import
__all__ = ["declarative_base", "Query", "MatrixOneQuery"]


def _first_query(query):
    """Limit a private copy without changing the caller's reusable query."""
    return copy(query).limit(1)


def _count_sql(sql):
    """Count result rows, including projection, grouping and pagination."""
    return f"SELECT COUNT(*) FROM ({sql}) AS _mo_count"


# For SQL functions, we recommend using SQLAlchemy's func module for better integration:
#
# from sqlalchemy import func
#
# # For SQLAlchemy models:
# query.select(func.count(User.id))
# query.select(func.sum(Order.amount))
# query.select(func.avg(Product.price))
#
# # For MatrixOne models, you can use string column names:
# query.select(func.count("id"))
# query.select(func.sum("amount"))
# query.select(func.avg("price"))
#
# This provides better type safety, SQL generation, and integration with SQLAlchemy.


# Remove SimpleModel - use SQLAlchemy models directly


def _model_table_name(model, legacy=False):
    if isinstance(model, str):
        return model
    if legacy and getattr(model, '_table_name', None):
        return model._table_name
    from sqlalchemy import inspect

    inspected = inspect(model, raiseerr=False)
    if inspected is not None and getattr(inspected, 'is_aliased_class', False):
        return compile_from(model)
    table = getattr(model, '__table__', None)
    if table is not None:
        name = sql_identifier(table.name)
        return sql_identifier(table.schema) + '.' + name if table.schema else name
    return getattr(model, '__tablename__', None) or getattr(model, '_table_name', None) or model.__name__.lower()


def _require_single_table_mutation(joins):
    if joins:
        raise ValueError('Joined UPDATE/DELETE is not supported; use explicit SQL')


def create_query(query_type, client, columns, snapshot=None, transaction_wrapper=None):
    """Resolve projection and FROM once for every buffered query entry point."""
    from sqlalchemy import inspect
    from sqlalchemy.sql import select

    if not columns:
        raise ValueError('query() requires a table, model or expression')
    first = columns[0]
    if len(columns) == 1 and isinstance(first, str):
        return query_type(first, client, transaction_wrapper=transaction_wrapper, snapshot=snapshot)
    if len(columns) == 1 and isinstance(first, CTE):
        query = query_type(None, client, transaction_wrapper=transaction_wrapper, snapshot=snapshot)
        query._table_name = sql_identifier(first.name)
        query._select_columns = ['*']
        query._ctes = [first]
        return query
    model = None
    projection = []
    for item in columns:
        inspected = inspect(item, raiseerr=False)
        if inspected is not None and (
            getattr(inspected, 'is_mapper', False) or getattr(inspected, 'is_aliased_class', False)
        ):
            if model is None:
                model = item
            projection.extend(
                getattr(item, inspected.mapper.get_property_by_column(c).key) for c in inspected.mapper.columns
            )
        else:
            projection.append(normalize_expression(item))
    query = query_type(model, client, transaction_wrapper=transaction_wrapper, snapshot=snapshot)
    if len(columns) == 1 and model is first:
        return query
    query._select_columns = projection
    if model is None:
        expressions = [item for item in projection if not isinstance(item, str)]
        sources = select(*expressions).get_final_froms() if expressions else []
        if sources:
            query._source_selectable = query._root_selectable = sources[0]
            query._table_name = getattr(sources[0], 'name', None)
            query._check_projection_sources = True
        else:
            for item in projection:
                if isinstance(item, str) and '.' in item:
                    query._table_name = '.'.join(item.split('.')[:-1])
                    break
    return query


class Query:
    """
    Query builder for ORM operations with SQLAlchemy-style interface.

    This class provides a fluent interface for building SQL queries in a way that
    mimics SQLAlchemy's query builder. It supports both SQLAlchemy models and
    custom MatrixOne models.

    Key Features:

    - Fluent method chaining for building complex queries
    - Support for SELECT, INSERT, UPDATE, DELETE operations
    - SQLAlchemy expression support in filter(), having(), and other methods
    - Automatic SQL generation and parameter binding
    - Snapshot support for point-in-time queries

    Usage::

        # Basic query building
        query = client.query(User)
        results = query.filter(User.age > 25).order_by(User.name).limit(10).all()

        # Complex queries with joins and aggregations
        query = client.query(User, func.count(Order.id).label('order_count'))
        results = (query
            .join(Order, User.id == Order.user_id)
            .group_by(User.id)
            .having(func.count(Order.id) > 5)
            .all())

    Note: This is the legacy query builder. For new code, consider using MatrixOneQuery which
    provides enhanced SQLAlchemy compatibility.
    """

    def __init__(self, model_class, client, snapshot_name: Optional[str] = None):
        self.model_class = model_class
        self.client = client
        self.snapshot_name = snapshot_name
        self._select_columns = []
        self._where_conditions = []
        self._where_params = []
        self._joins = []
        self._group_by_columns = []
        self._having_conditions = []
        self._having_params = []
        self._order_by_columns = []
        self._limit_count = None
        self._offset_count = None
        self._query_type = "SELECT"
        # For INSERT
        self._insert_values = []
        # For UPDATE
        self._update_set_columns = []
        self._update_set_values = []

    def _execute(self, sql, params=None):
        return self.client.execute(sql, params)

    def snapshot(self, snapshot_name: str) -> "Query":
        """Set snapshot for this query - SQLAlchemy style chaining"""
        self.snapshot_name = snapshot_name
        return self

    def select(self, *columns) -> "Query":
        """Select specific columns"""
        if not columns:
            # Select all columns from the SQLAlchemy model
            if hasattr(self.model_class, "__table__"):
                self._select_columns = [col.name for col in self.model_class.__table__.columns]
            else:
                self._select_columns = []
        else:
            self._select_columns = [str(col) for col in columns]
        return self

    def filter(self, condition: str, *params) -> "Query":
        """Add WHERE condition"""
        self._where_conditions.append(condition)
        self._where_params.extend(params)
        return self

    def filter_by(self, **kwargs) -> "Query":
        """Add named predicates; reject unknown fields before modifying the query."""
        table = getattr(self.model_class, '__table__', None)
        mapper = getattr(self.model_class, '__mapper__', None)
        if isinstance(self.model_class, str):
            columns = None
        elif table is not None and mapper is not None:
            columns = {
                prop.key: prop.columns[0].name
                for prop in mapper.column_attrs
                if len(prop.columns) == 1 and any(prop.columns[0] is col for col in table.columns)
            }
        elif table is not None:
            columns = {col.name: col.name for col in table.columns}
        else:
            columns = getattr(self.model_class, '_columns', {})

        predicates, values = [], []
        for key, value in kwargs.items():
            if columns is not None and key not in columns:
                raise ValueError(f'Unknown model field: {key}')
            column = key if columns is None else columns[key]
            column = getattr(column, 'name', column)
            if value is None:
                predicates.append(f'{sql_identifier(column)} IS NULL')
            else:
                predicates.append(f'{sql_identifier(column)} = ?')
                values.append(value)
        self._where_conditions.extend(predicates)
        self._where_params.extend(values)
        return self

    def join(self, table: str, condition: str) -> "Query":
        """Add JOIN clause"""
        self._joins.append(f"JOIN {table} ON {condition}")
        return self

    def group_by(self, *columns) -> "Query":
        """
        Add GROUP BY clause to the query.

        The GROUP BY clause is used to group rows that have the same values in specified columns,
        typically used with aggregate functions like COUNT, SUM, AVG, etc.

        Args::

            *columns: Columns to group by as strings.
                    Can include column names, expressions, or functions.

        Returns::

            Query: Self for method chaining.

        Examples::

            # Basic GROUP BY
            query.group_by("department")
            query.group_by("department", "status")
            query.group_by("YEAR(created_at)")

            # Multiple columns
            query.group_by("department", "status", "category")

            # With expressions
            query.group_by("YEAR(created_at)", "MONTH(created_at)")

        Notes:
            - GROUP BY is typically used with aggregate functions (COUNT, SUM, AVG, etc.)
            - Multiple columns can be grouped together
            - For SQLAlchemy expression support, use MatrixOneQuery instead

        Raises::

            ValueError: If columns are not strings
        """
        self._group_by_columns.extend([str(col) for col in columns])
        return self

    def having(self, condition: str, *params) -> "Query":
        """
        Add HAVING condition to the query.

        The HAVING clause is used to filter groups after GROUP BY operations,
        similar to WHERE clause but applied to aggregated results.

        Args::

            condition (str): The HAVING condition as a string.
                           Can include '?' placeholders for parameter substitution.
            *params: Parameters to replace '?' placeholders in the condition.

        Returns::

            Query: Self for method chaining.

        Examples::

            # Basic HAVING with placeholders
            query.group_by(User.department)
            query.having("COUNT(*) > ?", 5)
            query.having("AVG(age) > ?", 25)

            # HAVING without placeholders
            query.group_by(User.department)
            query.having("COUNT(*) > 5")
            query.having("AVG(age) > 25")

            # Multiple HAVING conditions
            query.group_by(User.department)
            query.having("COUNT(*) > ?", 5)
            query.having("AVG(age) > ?", 25)
            query.having("MAX(age) < ?", 65)

        Notes:
            - HAVING clauses are typically used with GROUP BY operations
            - Use '?' placeholders for safer parameter substitution
            - Multiple HAVING conditions are combined with AND logic
            - For SQLAlchemy expression support, use MatrixOneQuery instead

        Raises::

            ValueError: If condition is not a string
        """
        self._having_conditions.append(condition)
        self._having_params.extend(params)
        return self

    def order_by(self, *columns) -> "Query":
        """
        Add ORDER BY clause to the query.

        The ORDER BY clause is used to sort the result set by one or more columns,
        either in ascending (ASC) or descending (DESC) order.

        Args::

            *columns: Columns to order by as strings.
                    Can include column names with optional ASC/DESC keywords.

        Returns::

            Query: Self for method chaining.

        Examples::

            # Basic ORDER BY
            query.order_by("name")
            query.order_by("created_at DESC")
            query.order_by("department ASC", "name DESC")

            # Multiple columns
            query.order_by("department", "name", "age DESC")

            # With expressions
            query.order_by("COUNT(*) DESC")
            query.order_by("AVG(salary) ASC")

        Notes:
            - ORDER BY sorts the result set in ascending order by default
            - Use "DESC" for descending order, "ASC" for explicit ascending order
            - Multiple columns are ordered from left to right
            - For SQLAlchemy expression support, use MatrixOneQuery instead

        Raises::

            ValueError: If columns are not strings
        """
        for col in columns:
            self._order_by_columns.append(str(col))
        return self

    def limit(self, count: int) -> "Query":
        """Add LIMIT clause"""
        self._limit_count = count
        return self

    def offset(self, count: int) -> "Query":
        """Add OFFSET clause"""
        self._offset_count = count
        return self

    def _build_select_sql(self) -> tuple[str, List[Any]]:
        """Build SELECT SQL query using unified SQL builder"""
        from .sql_builder import MatrixOneSQLBuilder

        builder = MatrixOneSQLBuilder()

        # Build SELECT clause
        if self._select_columns:
            builder.select(*self._select_columns)
        else:
            builder.select_all()

        # Build FROM clause with snapshot
        builder.from_table(_model_table_name(self.model_class, legacy=True), self.snapshot_name)

        # Add JOIN clauses
        builder._joins = self._joins.copy()

        # Add WHERE conditions and parameters
        builder._where_conditions = self._where_conditions.copy()
        builder._where_params = self._where_params.copy()

        # Add GROUP BY columns
        if self._group_by_columns:
            builder.group_by(*self._group_by_columns)

        # Add HAVING conditions and parameters
        builder._having_conditions = self._having_conditions.copy()
        builder._having_params = self._having_params.copy()

        # Add ORDER BY columns
        if self._order_by_columns:
            builder.order_by(*self._order_by_columns)

        # Add LIMIT and OFFSET
        if self._limit_count is not None:
            builder.limit(self._limit_count)
        if self._offset_count is not None:
            builder.offset(self._offset_count)

        return builder.build()

    def all(self) -> List:
        """Execute query and return all results using mapped attribute names."""
        sql, params = self._build_select_sql()
        result = self._execute(sql, params)
        columns = getattr(result, 'columns', None)
        if callable(columns):
            columns = list(result.keys())
        table = getattr(self.model_class, '__table__', None)
        names = self._select_columns or ([col.name for col in table.columns] if table is not None else [])
        columns = columns or names
        mapper = getattr(self.model_class, '__mapper__', None)
        attributes = (
            {
                col.name: mapper.get_property_by_column(col).key if mapper is not None else col.name
                for col in table.columns
            }
            if table is not None
            else {}
        )
        return [
            self.model_class(**{attributes.get(name, name): value for name, value in zip(columns, row)})
            for row in result.rows
        ]

    def first(self) -> Optional:
        """Execute query and return first result"""
        self._limit_count = 1
        results = self.all()
        return results[0] if results else None

    def one(self):
        """
        Execute query and return exactly one result.

        This method executes the query and expects exactly one row to be returned.
        If no rows are found or multiple rows are found, it raises appropriate exceptions.

        Returns::
            Model instance: The single result row as a model instance.

        Raises::
            NoResultFound: If no results are found.
            MultipleResultsFound: If more than one result is found.

        Examples::

            # Get a user by unique ID
            user = client.query(User).filter(User.id == 1).one()

            # Get a user by unique email
            user = client.query(User).filter(User.email == "admin@example.com").one()

        Notes::
            - Use this method when you expect exactly one result
            - For cases where zero or one result is acceptable, use one_or_none()
            - For cases where multiple results are acceptable, use all() or first()
        """
        results = self.all()
        if len(results) == 0:
            from sqlalchemy.exc import NoResultFound

            raise NoResultFound("No row was found for one()")
        elif len(results) > 1:
            from sqlalchemy.exc import MultipleResultsFound

            raise MultipleResultsFound("Multiple rows were found for one()")
        return results[0]

    def one_or_none(self):
        """
        Execute query and return exactly one result or None.

        This method executes the query and returns exactly one row if found,
        or None if no rows are found. If multiple rows are found, it raises an exception.

        Returns::
            Model instance or None: The single result row as a model instance,
                                   or None if no results are found.

        Raises::
            MultipleResultsFound: If more than one result is found.

        Examples::

            # Get a user by ID, return None if not found
            user = client.query(User).filter(User.id == 999).one_or_none()
            if user:
                print(f"Found user: {user.name}")

            # Get a user by email, return None if not found
            user = client.query(User).filter(User.email == "nonexistent@example.com").one_or_none()
            if user is None:
                print("User not found")

        Notes::
            - Use this method when zero or one result is acceptable
            - For cases where exactly one result is required, use one()
            - For cases where multiple results are acceptable, use all() or first()
        """
        results = self.all()
        if len(results) == 0:
            return None
        elif len(results) > 1:
            from sqlalchemy.exc import MultipleResultsFound

            raise MultipleResultsFound("Multiple rows were found for one_or_none()")
        return results[0]

    def scalar(self):
        """
        Execute query and return the first column of the first result.

        This method executes the query and returns the value of the first column
        from the first row, or None if no results are found. This is useful for
        getting single values like counts, sums, or specific column values.

        Returns::
            Any or None: The value of the first column from the first row,
                        or None if no results are found.

        Examples::

            # Get the count of all users
            count = client.query(User).select(func.count(User.id)).scalar()

            # Get the name of the first user
            name = client.query(User).select(User.name).first().scalar()

            # Get the maximum age
            max_age = client.query(User).select(func.max(User.age)).scalar()

            # Get a specific user's name by ID
            name = client.query(User).select(User.name).filter(User.id == 1).scalar()

        Notes::
            - This method is particularly useful with aggregate functions
            - For custom select queries, returns the first selected column value
            - For model queries, returns the first column value from the model
            - Returns None if no results are found
        """
        result = self.first()
        if result is None:
            return None

        # If result is a model instance, return the first column value
        if hasattr(result, '__dict__'):
            # Get the first column value from the model
            if hasattr(self.model_class, "__table__"):
                first_column = list(self.model_class.__table__.columns)[0]
                mapper = getattr(self.model_class, '__mapper__', None)
                attribute = mapper.get_property_by_column(first_column).key if mapper is not None else first_column.name
                return getattr(result, attribute)
            else:
                # For raw queries, return the first attribute
                attrs = [attr for attr in dir(result) if not attr.startswith('_')]
                if attrs:
                    return getattr(result, attrs[0])
                return None
        else:
            # For raw data, return the first element
            if isinstance(result, (list, tuple)) and len(result) > 0:
                return result[0]
            return result

    def count(self) -> int:
        """Execute query and return count of results"""
        sql, params = self._build_select_sql()
        result = self._execute(_count_sql(sql), params)
        return result.rows[0][0] if result.rows else 0

    def insert(self, **kwargs) -> "Query":
        """Start INSERT operation"""
        self._query_type = "INSERT"
        self._insert_values.append(kwargs)
        return self

    def bulk_insert(self, values_list: List[Dict[str, Any]]) -> "Query":
        """Bulk insert multiple records"""
        self._query_type = "INSERT"
        self._insert_values.extend(values_list)
        return self

    def _build_insert_sql(self) -> tuple[str, List[Any]]:
        """Build INSERT SQL query using unified SQL builder"""
        if self.snapshot_name:
            raise ValueError('Snapshot queries cannot mutate live data')
        from .sql_builder import build_insert_query

        if not self._insert_values:
            raise ValueError("No values provided for INSERT")

        return build_insert_query(
            table_name=_model_table_name(self.model_class, legacy=True), values=self._insert_values
        )

    def update(self, **kwargs) -> "Query":
        """Start UPDATE operation"""
        self._query_type = "UPDATE"
        for key, value in kwargs.items():
            self._update_set_columns.append(f"{key} = ?")
            self._update_set_values.append(value)
        return self

    def _build_update_sql(self) -> tuple[str, List[Any]]:
        """Build UPDATE SQL query using unified SQL builder"""
        if self.snapshot_name:
            raise ValueError('Snapshot queries cannot mutate live data')
        _require_single_table_mutation(self._joins)
        from .sql_builder import build_update_query

        if not self._update_set_columns:
            raise ValueError("No SET clauses provided for UPDATE")

        # Convert set columns and values to dict
        set_values = {}
        for i, col in enumerate(self._update_set_columns):
            # Extract column name from "column = ?" format
            col_name = col.split(" = ")[0]
            set_values[col_name] = self._update_set_values[i]

        return build_update_query(
            table_name=_model_table_name(self.model_class, legacy=True),
            set_values=set_values,
            where_conditions=self._where_conditions,
            where_params=self._where_params,
            order_by=self._order_by_columns,
            limit=self._limit_count,
        )

    def delete(self) -> Any:
        """Execute DELETE operation"""
        self._query_type = "DELETE"
        sql, params = self._build_delete_sql()
        return self._execute(sql, params)

    def _build_delete_sql(self) -> tuple[str, List[Any]]:
        """Build DELETE SQL query using unified SQL builder"""
        if self.snapshot_name:
            raise ValueError('Snapshot queries cannot mutate live data')
        _require_single_table_mutation(self._joins)
        from .sql_builder import build_delete_query

        return build_delete_query(
            table_name=_model_table_name(self.model_class, legacy=True),
            where_conditions=self._where_conditions,
            where_params=self._where_params,
            order_by=self._order_by_columns,
            limit=self._limit_count,
        )

    def execute(self) -> Any:
        """Execute the query based on its type"""
        if self._query_type == "SELECT":
            return self.all()
        elif self._query_type == "INSERT":
            sql, params = self._build_insert_sql()
            return self._execute(sql, params)
        elif self._query_type == "UPDATE":
            sql, params = self._build_update_sql()
            return self._execute(sql, params)
        elif self._query_type == "DELETE":
            sql, params = self._build_delete_sql()
            return self._execute(sql, params)
        else:
            raise ValueError(f"Unknown query type: {self._query_type}")


# CTE (Common Table Expression) support
class CTE:
    """CTE (Common Table Expression) class for MatrixOne queries"""

    def __init__(self, name: str, query, recursive: bool = False):
        self.name = name
        self.query = query
        self.recursive = recursive

    def _compile(self):
        """Compile the CTE query to SQL"""
        return compile_cte(self)

    @statement_compilation
    def as_sql(self) -> tuple[str, list]:
        """Get the compiled SQL and parameters for this CTE"""
        return self._compile()

    def __str__(self):
        return f"CTE({self.name})"


# Base Query Builder - SQLAlchemy style
class BaseMatrixOneQuery:
    """
    Base MatrixOne Query builder that contains common SQL building logic.

    This base class provides SQLAlchemy-compatible query building with:
    - Full SQLAlchemy expression support in having(), filter(), and other methods
    - Automatic SQL generation and parameter binding
    - Support for both SQLAlchemy expressions and string conditions
    - Method chaining for fluent query building

    Key Features:

    - SQLAlchemy expression support (e.g., func.count(User.id) > 5)
    - String condition support (e.g., "COUNT(*) > ?", 5)
    - Automatic column name resolution and SQL generation
    - Full compatibility with SQLAlchemy 1.4+ and 2.0+

    Note: This is a base class. For most use cases, use MatrixOneQuery instead.
    """

    def __init__(self, model_class, client, transaction_wrapper=None, snapshot=None):
        self.model_class = model_class
        self.client = client
        self.transaction_wrapper = transaction_wrapper
        self._snapshot_name = snapshot
        self._table_alias = None  # Add table alias support
        self._select_columns = []
        self._joins = []
        self._where_conditions = []
        self._where_params = []
        self._group_by_columns = []
        self._source_selectable = selectable_of(model_class)
        self._root_selectable = self._source_selectable
        self._joined_selectables = []
        self._check_projection_sources = False
        self._having_conditions = []
        self._having_params = []
        self._order_by_columns = []
        self._limit_count = None
        self._offset_count = None
        self._ctes = []  # List of CTE definitions
        self._query_type = "SELECT"
        # For INSERT
        self._insert_values = []
        # For UPDATE
        self._update_set_columns = []
        self._update_set_values = []

        # Handle None model_class (for column-only queries)
        if model_class is None:
            self._is_sqlalchemy_model = False
            self._table_name = None  # Will be set later by query() method
            self._columns = {}
        else:
            # Detect if this is a SQLAlchemy model
            self._is_sqlalchemy_model = self._detect_sqlalchemy_model()

            # Get table name and columns based on model type
            if isinstance(model_class, str):
                # String table name
                self._table_name = model_class
                self._columns = {}
            elif self._is_sqlalchemy_model:
                from sqlalchemy import inspect

                inspected = inspect(model_class)
                # The selectable owns alias identity and is rendered only when
                # building a statement, inside its binding scope.
                self._table_name = None if inspected.is_aliased_class else _model_table_name(model_class)
                mapper = inspected.mapper
                self._columns = {prop.key: getattr(model_class, prop.key) for prop in mapper.column_attrs}
            else:
                # Fallback to class name for non-SQLAlchemy model objects.
                self._table_name = getattr(model_class, "_table_name", model_class.__name__.lower())
                self._columns = getattr(model_class, "_columns", {})

    def _qualified_table_name(self):
        if self._root_selectable is not None:
            return compile_from(self._root_selectable)
        return self._table_name

    def _table_target(self):
        table = self._qualified_table_name()
        if not table:
            raise ValueError("Table name is required. Provide a model class or set table name manually.")
        if self._root_selectable is not None:
            return table
        return f"{table} AS {sql_identifier(self._table_alias)}" if self._table_alias else table

    def _append_clause(self, attribute, render):
        # One source of truth. SQLAlchemy expressions are compiled only when
        # their enclosing statement and all its sources are known.
        getattr(self, attribute).append(render)

    def _clauses(self, attribute):
        return [value() if callable(value) else value for value in getattr(self, attribute)]

    def _compile(self, expression, params=(), *, projection=False, predicate=False, join_limit=None, dialect=None):
        from sqlalchemy import table
        from sqlalchemy.sql import visitors
        from sqlalchemy.sql.elements import ClauseElement
        from sqlalchemy.sql.selectable import FromClause, ScalarSelect, Select, TableClause
        from ._sql_literals import identifier_parts

        if not (predicate and hasattr(expression, 'compile_for_query')):
            expression = normalize_expression(expression)
        contextual_root = self._root_selectable
        if isinstance(self.model_class, str):
            nodes = list(visitors.iterate(expression)) if isinstance(expression, ClauseElement) else []
            try:
                parts = identifier_parts(self._table_name, qualified=True)
            except ValueError:
                if any(isinstance(node, ScalarSelect) for node in nodes):
                    raise ValueError('Correlated expressions require an unambiguous table or mapped source')
            else:
                candidates = {}
                for node in nodes:
                    candidate = node if isinstance(node, FromClause) else getattr(node, 'table', None)
                    if candidate is not None and getattr(candidate, 'name', None) == parts[-1]:
                        if getattr(candidate, 'schema', None) == (parts[0] if len(parts) == 2 else None):
                            if not isinstance(candidate, TableClause):
                                raise ValueError('String table names cannot be replaced by derived or aliased sources')
                            candidates[id(candidate._deannotate())] = candidate._deannotate()
                if len(candidates) > 1:
                    raise ValueError('Ambiguous SQLAlchemy source for string table')
                if candidates:
                    selected = next(iter(candidates.values()))
                    if self._source_selectable is not None and selected is not self._source_selectable:
                        raise ValueError('Ambiguous SQLAlchemy source for string table')
                    if self._source_selectable is None:
                        self._source_selectable = selected
                        self._root_selectable = selected.alias(self._table_alias) if self._table_alias else selected
                    contextual_root = self._root_selectable
                elif self._root_selectable is None:
                    contextual_root = table(parts[-1], schema=parts[0] if len(parts) == 2 else None)
                    if self._table_alias:
                        contextual_root = contextual_root.alias(self._table_alias)
        visible = ([] if contextual_root is None else [contextual_root]) + [
            source for source in self._joined_selectables[:join_limit] if source is not None
        ]
        if any(source is None for source in self._joined_selectables[:join_limit]):
            known = {
                id(source._deannotate())
                for source in visible + ([] if self._source_selectable is None else [self._source_selectable])
            }
            if isinstance(expression, ClauseElement):
                for node in visitors.iterate(expression):
                    if not isinstance(node, Select) or not node.get_final_froms():
                        continue
                    explicit = node._correlate
                    excluded = node._correlate_except
                    if node._auto_correlate:
                        unsafe = True
                    elif excluded is not None:
                        excluded_ids = {id(source._deannotate()) for source in excluded}
                        unsafe = any(
                            id(source._deannotate()) not in known | excluded_ids for source in node.get_final_froms()
                        )
                    else:
                        unsafe = any(id(source._deannotate()) not in known for source in explicit)
                    if unsafe:
                        raise ValueError('Correlated subqueries with raw JOIN sources require SQLAlchemy selectables')
        context = dict(
            outer_froms=visible,
            source=self._source_selectable,
            target=self._root_selectable,
            compiler_dialect=dialect,
        )
        if predicate:
            if type(expression) is LogicalIn:
                if params:
                    raise ValueError('SQLAlchemy expressions carry their own parameters')

                def source():
                    return self._table_target() if self._root_selectable is not None else None

                return expression.compile(
                    model=self.model_class,
                    alias=self._table_alias,
                    table=self._table_name,
                    _table_supplier=source,
                    _column_compiler=lambda column: self._compile(column, join_limit=join_limit),
                )
            return compile_predicate(expression, params, self.model_class, self._table_alias, **context)
        return compile_expression(expression, self.model_class, self._table_alias, projection, **context)

    def _check_mutation_source(self):
        from sqlalchemy.sql.selectable import AliasedReturnsRows, TableClause

        source = self._source_selectable
        while isinstance(source, AliasedReturnsRows):
            source = source.element
        if source is not None and not isinstance(source, TableClause):
            raise ValueError('Derived query sources cannot mutate a physical table')

    def _execute(self, sql, params=None):
        """Execute SQL using either transaction wrapper or client"""
        if self.transaction_wrapper:
            return self.transaction_wrapper.execute(sql, params)
        else:
            return self.client.execute(sql, params)

    def _build_current_sql(self):
        builders = {
            'SELECT': '_build_sql',
            'INSERT': '_build_insert_sql',
            'UPDATE': '_build_update_sql',
            'DELETE': '_build_delete_sql',
        }
        if self._query_type not in builders:
            raise ValueError('Unknown query type: ' + self._query_type)
        if self._snapshot_name and self._query_type != 'SELECT':
            raise ValueError('Snapshot queries are read-only')
        return getattr(self, builders[self._query_type])()

    def _detect_sqlalchemy_model(self) -> bool:
        """Detect if the model class is a SQLAlchemy model"""
        return (
            hasattr(self.model_class, "__tablename__")
            and hasattr(self.model_class, "__mapper__")
            and hasattr(self.model_class, "__table__")
        )

    def select(self, *columns) -> "BaseMatrixOneQuery":
        """Add SELECT columns - SQLAlchemy style"""
        self._select_columns = [normalize_expression(column) for column in columns]
        return self

    def cte(self, name: str, recursive: bool = False) -> CTE:
        """Create a CTE (Common Table Expression) from this query - SQLAlchemy style

        Args::

            name: Name of the CTE
            recursive: Whether this is a recursive CTE

        Returns::

            CTE object that can be used in other queries

        Examples::

            # Create a CTE from a query
            user_stats = client.query(User).filter(User.active == True).cte("user_stats")

            # Use the CTE in another query
            result = client.query(user_stats).all()

            # Recursive CTE example
            hierarchy = client.query(Employee).filter(Employee.manager_id == None).cte("hierarchy", recursive=True)
        """
        return CTE(name, self, recursive)

    def join(self, target, onclause=None, isouter=False, full=False):
        kind = 'FULL OUTER JOIN' if full else 'LEFT OUTER JOIN' if isouter else 'INNER JOIN'
        return self._join(target, onclause, kind)

    def _join(self, target, onclause, kind):
        if onclause is not None and not hasattr(onclause, 'compile_for_query'):
            onclause = normalize_expression(onclause)
        right = selectable_of(target)
        index = len(self._joined_selectables)

        def inferred_condition():
            from sqlalchemy import inspect
            from sqlalchemy.sql.util import join_condition
            from sqlalchemy.exc import NoForeignKeysError, AmbiguousForeignKeysError

            root = self._root_selectable
            root_info = inspect(self.model_class, raiseerr=False)
            target_info = inspect(target, raiseerr=False)
            if (
                root_info is not None
                and target_info is not None
                and getattr(root_info, 'mapper', None) is getattr(target_info, 'mapper', None)
                and getattr(root_info, 'mapper', None) is not None
            ):
                raise ValueError('Self JOIN inference requires an explicit ON clause')
            if root is None or right is None:
                raise ValueError('JOIN requires an explicit ON clause without mapped foreign keys')
            sources = [root] + [source for source in self._joined_selectables[:index] if source is not None]
            if any(right is source for source in sources):
                raise ValueError('Self JOIN inference requires distinct selectables and an explicit ON clause')
            candidates = []
            for left in sources:
                try:
                    candidates.append(join_condition(left, right))
                except NoForeignKeysError:
                    continue
                except AmbiguousForeignKeysError as error:
                    raise ValueError('JOIN has ambiguous foreign keys; provide ON explicitly') from error
            if len(candidates) != 1:
                raise ValueError('JOIN requires one unambiguous foreign key relationship; provide ON explicitly')
            return candidates[0]

        if onclause is None:
            inferred_condition()  # Reject invalid admission before changing the query.

        def render():
            if isinstance(target, CTE):
                source = sql_identifier(target.name)
            elif right is not None:
                source = compile_from(right)
            else:
                name = getattr(target, '_table_name', str(target))
                source = name if isinstance(target, str) else sql_identifier(name, qualified=True)
            clause = f'{kind} {source}'
            condition = inferred_condition() if onclause is None else onclause
            clause += ' ON ' + self._compile(condition, predicate=True, join_limit=index + 1)
            return clause

        self._append_clause('_joins', render)
        self._joined_selectables.append(right)
        return self

    def innerjoin(self, target, onclause=None) -> "BaseMatrixOneQuery":
        """Add INNER JOIN clause - SQLAlchemy style (alias for join with isouter=False)"""
        return self.join(target, onclause, isouter=False)

    def leftjoin(self, target, onclause=None) -> "BaseMatrixOneQuery":
        """Add LEFT JOIN clause - SQLAlchemy style (alias for join with isouter=True)"""
        return self.join(target, onclause, isouter=True)

    def rightjoin(self, target, onclause=None):
        """Retain unmatched right rows using the server's native RIGHT JOIN."""
        return self._join(target, onclause, 'RIGHT JOIN')

    def fullouterjoin(self, target, onclause=None) -> "BaseMatrixOneQuery":
        """Add FULL OUTER JOIN clause - SQLAlchemy style (alias for join with full=True)"""
        return self.join(target, onclause, full=True)

    def outerjoin(self, target, onclause=None) -> "BaseMatrixOneQuery":
        """Add LEFT OUTER JOIN clause - SQLAlchemy style (alias for leftjoin)"""
        return self.leftjoin(target, onclause)

    def group_by(self, *columns):
        for column in columns:
            column = normalize_expression(column)
            if hasattr(column, 'compile'):
                self._append_clause('_group_by_columns', lambda c=column: self._compile(c))
            else:
                self._group_by_columns.append(str(column))
        return self

    def having(self, condition, *params):
        """Append one validated predicate without consuming another clause's values."""
        if not hasattr(condition, 'compile_for_query'):
            condition = normalize_expression(condition)
        if hasattr(condition, 'compile'):
            self._append_clause('_having_conditions', lambda: self._compile(condition, params, predicate=True))
        else:
            self._having_conditions.append(compile_predicate(condition, params))
        return self

    def snapshot(self, snapshot_name: str) -> "BaseMatrixOneQuery":
        """Add snapshot support - SQLAlchemy style chaining"""
        self._snapshot_name = snapshot_name
        return self

    def alias(self, alias_name: str) -> "BaseMatrixOneQuery":
        """Set table alias for this query - SQLAlchemy style chaining"""
        self._table_alias = alias_name
        if self._source_selectable is not None:
            self._root_selectable = self._source_selectable.alias(alias_name)
        return self

    def subquery(self, alias_name: str = None) -> str:
        """Convert this query to a subquery with optional alias"""
        sql, params = self._build_sql()
        sql = substitute_parameters(sql, params)
        if alias_name:
            return f"({sql}) AS {sql_identifier(alias_name)}"
        else:
            return f"({sql})"

    def filter(self, condition, *params):
        """Append one validated predicate without consuming another clause's values."""
        if not hasattr(condition, 'compile_for_query'):
            condition = normalize_expression(condition)
        if hasattr(condition, 'compile'):
            self._append_clause('_where_conditions', lambda: self._compile(condition, params, predicate=True))
        else:
            self._where_conditions.append(compile_predicate(condition, params))
        return self

    def filter_by(self, **kwargs):
        """Filter named columns; unknown model fields cannot broaden a query."""
        if not isinstance(self.model_class, str):
            for key in kwargs:
                if key not in self._columns:
                    raise ValueError(f'Unknown model field: {key}')
        renders = []
        for key, value in kwargs.items():
            physical_name = self._columns[key].name if self._is_sqlalchemy_model else key
            if self._is_sqlalchemy_model:
                from sqlalchemy.dialects.mysql import dialect

                column_type = self._columns[key].type.dialect_impl(dialect())
                processor = column_type.bind_processor(dialect())
                if processor is not None and value is not None:
                    value = processor(value)
            suffix = ' IS NULL' if value is None else ' = ' + sql_literal(value)

            def render(name=physical_name, suffix=suffix, mapped=self._columns.get(key)):
                column = sql_identifier(name)
                if self._is_sqlalchemy_model and (
                    self._table_alias or self._joins or self._source_selectable is not self.model_class.__table__
                ):
                    return self._compile(mapped) + suffix
                if self._table_alias:
                    column = sql_identifier(self._table_alias) + '.' + column
                elif self._joins and not isinstance(self.model_class, str):
                    column = sql_identifier(self._table_name, qualified=True) + '.' + column
                return column + suffix

            renders.append(render)
        for render in renders:
            self._append_clause('_where_conditions', render)
        return self

    def where(self, condition: str, *params) -> "BaseMatrixOneQuery":
        """Add WHERE condition - alias for filter method"""
        return self.filter(condition, *params)

    def logical_in(self, column, values):
        return self.filter(LogicalIn(column, values))

    def order_by(self, *columns):
        for column in columns:
            column = normalize_expression(column)
            if hasattr(column, 'compile'):
                self._append_clause('_order_by_columns', lambda c=column: self._compile(c))
            else:
                self._order_by_columns.append(str(column))
        return self

    def limit(self, count: int) -> "BaseMatrixOneQuery":
        """Add LIMIT clause - SQLAlchemy style"""
        self._limit_count = count
        return self

    def offset(self, count: int) -> "BaseMatrixOneQuery":
        """Add OFFSET clause - SQLAlchemy style"""
        self._offset_count = count
        return self

    def _validate_projection_sources(self):
        if self._check_projection_sources:
            from sqlalchemy.sql import select

            expressions = [normalize_expression(col) for col in self._select_columns if not isinstance(col, str)]
            sources = select(*expressions).get_final_froms() if expressions else []
            admitted = [self._source_selectable] + self._joined_selectables
            known = {id(source._deannotate()) for source in admitted if source is not None}
            if any(id(source._deannotate()) not in known for source in sources):
                raise ValueError('Ambiguous query sources; provide an explicit JOIN')

    @statement_compilation
    def _build_sql(self) -> tuple[str, List[Any]]:
        """Build SQL query using unified SQL builder"""
        from .sql_builder import MatrixOneSQLBuilder

        self._validate_projection_sources()
        builder = MatrixOneSQLBuilder()

        # Build CTE clause if any CTEs are defined
        if self._ctes:
            for cte in self._ctes:
                if isinstance(cte, CTE):
                    cte_sql, cte_params = cte.as_sql()
                    builder._ctes.append(
                        {'name': cte.name, 'sql': cte_sql, 'params': cte_params, 'recursive': cte.recursive}
                    )
                else:
                    # Handle legacy CTE format
                    builder._ctes.append(cte)

        if self._select_columns:
            builder._select_columns = [
                (self._compile(col, projection=True) if hasattr(col, 'compile') else str(col))
                for col in self._select_columns
            ]
        elif self._is_sqlalchemy_model:
            builder._select_columns = [self._compile(column, projection=True) for column in self._columns.values()]
        else:
            builder.select_all()

        # Build FROM clause with optional table alias and snapshot
        builder._from_table = self._table_target()

        builder._from_snapshot = None

        # Add JOIN clauses
        builder._joins = self._clauses('_joins')

        # Add WHERE conditions and parameters
        builder._where_conditions = self._clauses('_where_conditions')
        builder._where_params = self._where_params.copy()

        # Add GROUP BY columns
        if self._group_by_columns:
            builder.group_by(*self._clauses('_group_by_columns'))

        # Add HAVING conditions and parameters
        builder._having_conditions = self._clauses('_having_conditions')
        builder._having_params = self._having_params.copy()

        # Add ORDER BY columns
        if self._order_by_columns:
            builder.order_by(*self._clauses('_order_by_columns'))

        # Add LIMIT and OFFSET
        if self._limit_count is not None:
            builder.limit(self._limit_count)
        if self._offset_count is not None:
            builder.offset(self._offset_count)

        # Build the final SQL and combine parameters
        sql, params = builder.build()
        if self._snapshot_name:
            from ._snapshot_sql import rewrite_snapshot

            sql = rewrite_snapshot(sql, self._snapshot_name)

        # Parameters are already combined in builder.build() since we added CTEs to builder._ctes
        return sql, params

    def _create_row_data(self, row, select_cols):
        """Create RowData object for aggregate queries"""

        class RowData:
            def __init__(self, values, columns):
                for i, col in enumerate(columns):
                    if i < len(values):
                        # Replace spaces with underscores for valid Python attribute names
                        attr_name = col.replace(" ", "_")
                        setattr(self, attr_name, values[i])
                # Also support indexing for backward compatibility
                self._values = values

            def __getitem__(self, index):
                return self._values[index]

            def __len__(self):
                return len(self._values)

        return RowData(row, select_cols)

    def _extract_select_columns(self, result_names=None):
        """Extract column names from select columns"""
        select_cols = []
        for position, col in enumerate(self._select_columns):
            # Check if this is a SQLAlchemy function with label
            if hasattr(col, "compile") and hasattr(col, "name") and col.name:
                # Special handling for DISTINCT function to avoid reserved keyword issues
                if hasattr(col, "name") and col.name.upper() == "DISTINCT":
                    # For DISTINCT functions, use the original logic to extract column name
                    col_str = str(col.compile())
                    if "(" in col_str and ")" in col_str:
                        func_name = col_str.split("(")[0].strip()
                        col_name = col_str.split("(")[1].split(")")[0].strip()
                        col_name = col_name.strip("'\"")
                        select_cols.append(f"DISTINCT_{col_name}")
                    else:
                        select_cols.append(col.name)
                else:
                    # SQLAlchemy function with label - use the label name
                    select_cols.append(col.name)
            else:
                # Convert SQLAlchemy function objects to strings first
                if hasattr(col, "compile"):
                    if result_names is not None and position < len(result_names):
                        select_cols.append(str(result_names[position]))
                        continue
                    # Naming a result must never evaluate a callable/default.
                    col_str = str(col.compile())
                else:
                    col_str = str(col)

                if " as " in col_str.lower():
                    # Handle "column as alias" syntax - use case-insensitive split
                    parts = col_str.lower().split(" as ")
                    if len(parts) == 2:
                        # Find the actual alias in the original string
                        as_index = col_str.lower().find(" as ")
                        alias = col_str[as_index + 4 :].strip()
                        # Remove quotes from alias if present
                        alias = alias.strip("'\"")
                        select_cols.append(alias)
                    else:
                        # Fallback to original logic
                        alias = col_str.split(" as ")[-1].strip()
                        alias = alias.strip("'\"")
                        select_cols.append(alias)
                else:
                    # Handle function calls like "COUNT(id)" or "DISTINCT category"
                    if "(" in col_str and ")" in col_str:
                        # Extract the function name and column
                        func_name = col_str.split("(")[0].strip()
                        col_name = col_str.split("(")[1].split(")")[0].strip()
                        # Remove quotes from column name
                        col_name = col_name.strip("'\"")
                        # Handle special cases
                        if func_name.upper() == "DISTINCT":
                            attr_name = f"DISTINCT_{col_name}"
                        else:
                            attr_name = f"{func_name.upper()}_{col_name}"
                        select_cols.append(attr_name)
                    else:
                        # Handle table aliases in column names (e.g., "u.name" -> "name")
                        if "." in col_str and not col_str.startswith("("):
                            # Extract column name after the dot for attribute access
                            col_name = col_str.split(".")[-1]
                            select_cols.append(col_name)
                        else:
                            # For simple column names, use as-is
                            select_cols.append(col_str)
        return select_cols

    def update(self, **kwargs) -> "BaseMatrixOneQuery":
        """
        Start UPDATE operation - SQLAlchemy style

        This method allows you to update records in the database using a fluent interface
        similar to SQLAlchemy's update() method. It supports both SQLAlchemy expressions
        and simple key-value pairs for setting column values.

        Args::

            **kwargs: Column names and their new values to set

        Returns::

            Self for method chaining

        Examples::

            # Update with simple key-value pairs
            query = client.query(User)
            query.update(name="New Name", email="new@example.com").filter(User.id == 1).execute()

            # Update with SQLAlchemy expressions
            from sqlalchemy import func
            query = client.query(User)
            query.update(
                last_login=func.now(),
                login_count=User.login_count + 1
            ).filter(User.id == 1).execute()

            # Update multiple records with conditions
            query = client.query(User)
            query.update(status="inactive").filter(User.last_login < "2023-01-01").execute()

            # Update with complex conditions
            query = client.query(User)
            query.update(
                status="premium",
                premium_until=func.date_add(func.now(), func.interval(1, "YEAR"))
            ).filter(
                User.subscription_type == "paid",
                User.payment_status == "active"
            ).execute()
        """
        if self._is_sqlalchemy_model:
            unknown = set(kwargs) - self._columns.keys()
            if unknown:
                raise ValueError('Unknown mapped UPDATE attributes: ' + ', '.join(sorted(unknown)))
        self._query_type = "UPDATE"

        # Handle both SQLAlchemy expressions and simple values
        for key, value in kwargs.items():
            value = normalize_expression(value)
            if hasattr(value, "compile"):  # SQLAlchemy expression
                physical_name = self._columns[key].name if self._is_sqlalchemy_model else key
                self._append_clause(
                    '_update_set_columns',
                    lambda key=physical_name, value=value: f"{sql_identifier(key)} = "
                    + self._compile(value, dialect=self._result_dialect()),
                )
            elif self._is_sqlalchemy_model:
                from sqlalchemy import bindparam

                column = self._columns[key]
                self._update_set_columns.append(
                    sql_identifier(column.name)
                    + ' = '
                    + compile_expression(
                        bindparam(None, value, type_=column.type), compiler_dialect=self._result_dialect()
                    )
                )
            else:  # Simple value
                self._update_set_columns.append(f"{key} = ?")
                self._update_set_values.append(value)

        return self

    @statement_compilation
    def _build_update_sql(self) -> tuple[str, List[Any]]:
        """
        Build UPDATE SQL query directly to handle SQLAlchemy expressions

        Returns::

            Tuple of (SQL string, parameters list)

        Raises::

            ValueError: If no SET clauses are provided for UPDATE
        """
        if self._snapshot_name:
            raise ValueError('Snapshot queries are read-only')
        self._validate_projection_sources()
        _require_single_table_mutation(self._joins)
        self._check_mutation_source()
        if not self._update_set_columns:
            raise ValueError("No SET clauses provided for UPDATE")

        from .sql_builder import MatrixOneSQLBuilder

        # Assignment SQL and bound values are already recorded at construction;
        # marker-looking text inside a compiled literal is never a placeholder.
        builder = MatrixOneSQLBuilder().update(self._table_target())
        builder._update_set_columns = self._clauses('_update_set_columns')
        builder._update_set_values = self._update_set_values.copy()
        builder._where_conditions = self._clauses('_where_conditions')
        builder._where_params = self._where_params.copy()
        builder.order_by(*self._clauses('_order_by_columns'))
        if self._limit_count is not None:
            builder.limit(self._limit_count)
        return builder.build()

    def delete(self) -> Any:
        """Execute DELETE operation"""
        self._query_type = "DELETE"
        sql, params = self._build_delete_sql()
        return self._execute(sql, params)

    @statement_compilation
    def _build_delete_sql(self) -> tuple[str, List[Any]]:
        """Build DELETE SQL query using unified SQL builder"""
        if self._snapshot_name:
            raise ValueError('Snapshot queries are read-only')
        self._validate_projection_sources()
        _require_single_table_mutation(self._joins)
        self._check_mutation_source()
        from .sql_builder import build_delete_query

        return build_delete_query(
            table_name=self._table_target(),
            where_conditions=self._clauses('_where_conditions'),
            where_params=self._where_params,
            order_by=self._clauses('_order_by_columns'),
            limit=self._limit_count,
        )

    # MatrixOne Snapshot Query Builder - SQLAlchemy style
    def _result_dialect(self, result=None):
        from sqlalchemy.engine.interfaces import Dialect
        from .sqlalchemy_ext.dialect import MatrixOneDialect

        dialect = getattr(result, '_matrixone_dialect', None)
        if not isinstance(dialect, Dialect):
            dialect = getattr(getattr(result, 'context', None), 'dialect', None)
        if not isinstance(dialect, Dialect):
            dialect = getattr(getattr(self.client, '_engine', None), 'dialect', None)
        return dialect if isinstance(dialect, Dialect) else MatrixOneDialect()

    def _scalar_from_result(self, result):
        rows = self._materialize(result)
        if not rows:
            return None
        if self._select_columns or isinstance(self.model_class, str):
            return rows[0][0]
        return getattr(rows[0], next(iter(self._columns)))

    def _materialize(self, result):
        rows = result.rows if hasattr(result, 'rows') else result.fetchall()
        if not rows:
            return []
        dialect = self._result_dialect(result)
        names = getattr(result, 'columns', None)
        if names is None or callable(names):
            if hasattr(result, 'keys'):
                names = list(result.keys())
            else:
                names = (
                    self._extract_select_columns()
                    if self._select_columns
                    else [column.name for column in self._columns.values()]
                )
        by_name = {column.name: column for column in self._columns.values()}
        if any(len(row) != len(names) for row in rows):
            raise ValueError('Query result column metadata does not match row width')
        if not self._select_columns and self._is_sqlalchemy_model and not by_name.keys() <= set(names):
            raise ValueError('Mapped query result is missing declared columns')
        expressions = self._select_columns or [by_name.get(name) for name in names]
        # These buffered builders execute raw SQL, so SQLAlchemy has no typed
        # result metadata. Apply each declared processor exactly once here.
        processors = []
        description = getattr(result, '_matrixone_description', None)
        if description is None:
            description = getattr(getattr(result, 'cursor', None), 'description', None)
        compiled = getattr(getattr(result, 'context', None), 'compiled', None)
        typed = getattr(result, '_matrixone_typed', False) or bool(getattr(compiled, '_result_columns', ()))
        for position, expression in enumerate(expressions):
            dtype = getattr(expression, 'type', None)
            code = description[position][1] if description is not None and position < len(description) else None
            processors.append(
                dtype._cached_result_processor(dialect, code) if dtype is not None and not typed else None
            )
        rows = [
            tuple(
                processors[i](value) if i < len(processors) and processors[i] is not None else value
                for i, value in enumerate(row)
            )
            for row in rows
        ]
        if self._select_columns:
            columns = self._extract_select_columns(names)
        elif isinstance(self.model_class, str):
            columns = getattr(result, 'columns', None)
            if columns is None or callable(columns):
                columns = list(result.keys())
            if not columns or len(columns) != len(rows[0]):
                raise ValueError('String-table results require complete column metadata')
        else:
            attributes = {column.name: key for key, column in self._columns.items()}
            from sqlalchemy import inspect

            constructor = inspect(self.model_class).mapper.class_ if self._is_sqlalchemy_model else self.model_class
            return [
                constructor(**{attributes[name]: value for name, value in zip(names, row) if name in attributes})
                for row in rows
            ]
        return [self._create_row_data(row, columns) for row in rows]


class MatrixOneQuery(BaseMatrixOneQuery):
    """
    MatrixOne Query builder that mimics SQLAlchemy Query interface.

    This class provides full SQLAlchemy compatibility including:
    - SQLAlchemy expression support in having(), filter(), and other methods
    - Type-safe column references and function calls
    - Automatic SQL generation and parameter binding
    - Full integration with SQLAlchemy models and functions

    Key Features:

    - Supports SQLAlchemy expressions (e.g., func.count(User.id) > 5)
    - Supports traditional string conditions (e.g., "COUNT(*) > ?", 5)
    - Automatic column name resolution and SQL generation
    - Method chaining for fluent query building
    - Full compatibility with SQLAlchemy 1.4+ and 2.0+

    Examples
        # SQLAlchemy expression syntax (recommended)
        query = client.query(User)
        query.group_by(User.department)
        query.having(func.count(User.id) > 5)
        query.having(func.avg(User.age) > 25)

        # String-based syntax (also supported)
        query.having("COUNT(*) > ?", 5)
        query.having("AVG(age) > ?", 25)

        # Mixed syntax
        query.having(func.count(User.id) > 5)  # Expression
        query.having("AVG(age) > ?", 25)       # String
    """

    def __init__(self, model_class, client, transaction_wrapper=None, snapshot=None):
        super().__init__(model_class, client, transaction_wrapper, snapshot)

    def with_cte(self, *ctes) -> "MatrixOneQuery":
        """Add CTEs to this query - SQLAlchemy style

        Args::

            *ctes: CTE objects to add to the query

        Returns::

            Self for method chaining

        Examples::

            # Add a single CTE
            user_stats = client.query(User).filter(User.active == True).cte("user_stats")
            result = client.query(Article).with_cte(user_stats).join(user_stats, Article.user_id == user_stats.id).all()

            # Add multiple CTEs
            result = client.query(Article).with_cte(user_stats, category_stats).all()
        """
        for cte in ctes:
            if isinstance(cte, CTE):
                self._ctes.append(cte)
            else:
                raise ValueError("All arguments must be CTE objects")
        return self

    def all(self) -> List:
        sql, params = self._build_sql()
        return self._materialize(self._execute(sql, params))

    def first(self) -> Optional:
        """Execute query and return first result - SQLAlchemy style"""
        results = _first_query(self).all()
        return results[0] if results else None

    def one(self):
        """
        Execute query and return exactly one result - SQLAlchemy style.

        This method executes the query and expects exactly one row to be returned.
        If no rows are found or multiple rows are found, it raises appropriate exceptions.
        This method provides SQLAlchemy-compatible behavior for MatrixOne queries.

        Returns::
            Model instance: The single result row as a model instance.

        Raises::
            NoResultFound: If no results are found.
            MultipleResultsFound: If more than one result is found.

        Examples::

            # Get a user by unique ID using SQLAlchemy expressions
            from sqlalchemy import and_
            user = client.query(User).filter(and_(User.id == 1, User.active == True)).one()

            # Get a user by unique email with complex conditions
            user = client.query(User).filter(User.email == "admin@example.com").one()

        Notes::
            - Use this method when you expect exactly one result
            - For cases where zero or one result is acceptable, use one_or_none()
            - For cases where multiple results are acceptable, use all() or first()
            - This method supports SQLAlchemy expressions and operators
        """
        results = self.all()
        if len(results) == 0:
            from sqlalchemy.exc import NoResultFound

            raise NoResultFound("No row was found for one()")
        elif len(results) > 1:
            from sqlalchemy.exc import MultipleResultsFound

            raise MultipleResultsFound("Multiple rows were found for one()")
        return results[0]

    def one_or_none(self):
        """
        Execute query and return exactly one result or None - SQLAlchemy style.

        This method executes the query and returns exactly one row if found,
        or None if no rows are found. If multiple rows are found, it raises an exception.
        This method provides SQLAlchemy-compatible behavior for MatrixOne queries.

        Returns::
            Model instance or None: The single result row as a model instance,
                                   or None if no results are found.

        Raises::
            MultipleResultsFound: If more than one result is found.

        Examples::

            # Get a user by ID, return None if not found
            from sqlalchemy import and_
            user = client.query(User).filter(and_(User.id == 999, User.active == True)).one_or_none()
            if user:
                print(f"Found user: {user.name}")

            # Get a user by email with complex conditions
            user = client.query(User).filter(User.email == "nonexistent@example.com").one_or_none()
            if user is None:
                print("User not found")

        Notes::
            - Use this method when zero or one result is acceptable
            - For cases where exactly one result is required, use one()
            - For cases where multiple results are acceptable, use all() or first()
            - This method supports SQLAlchemy expressions and operators
        """
        results = self.all()
        if len(results) == 0:
            return None
        elif len(results) > 1:
            from sqlalchemy.exc import MultipleResultsFound

            raise MultipleResultsFound("Multiple rows were found for one_or_none()")
        return results[0]

    def scalar(self):
        """
        Execute query and return the first column of the first result - SQLAlchemy style.

        This method executes the query and returns the value of the first column
        from the first row, or None if no results are found. This is useful for
        getting single values like counts, sums, or specific column values.
        This method provides SQLAlchemy-compatible behavior for MatrixOne queries.

        Returns::
            Any or None: The value of the first column from the first row,
                        or None if no results are found.

        Examples::

            # Get the count of all users using SQLAlchemy functions
            from sqlalchemy import func
            count = client.query(User).select(func.count(User.id)).scalar()

            # Get the name of the first user
            name = client.query(User).select(User.name).first().scalar()

            # Get the maximum age with complex conditions
            max_age = client.query(User).select(func.max(User.age)).filter(User.active == True).scalar()

            # Get a specific user's name by ID
            name = client.query(User).select(User.name).filter(User.id == 1).scalar()

        Notes::
            - This method is particularly useful with aggregate functions
            - For custom select queries, returns the first selected column value
            - For model queries, returns the first column value from the model
            - Returns None if no results are found
            - This method supports SQLAlchemy expressions and operators
        """
        query = _first_query(self)
        sql, params = query._build_sql()
        result = query._execute(sql, params)
        return query._scalar_from_result(result)

    def count(self) -> int:
        """Execute query and return count of results - SQLAlchemy style"""
        sql, params = self._build_sql()
        result = self._execute(_count_sql(sql), params)

        # Handle both ResultSet and SQLAlchemy Result
        if hasattr(result, 'rows'):
            # ResultSet from client.execute()
            return result.rows[0][0] if result.rows else 0
        else:
            # SQLAlchemy Result from session.execute()
            rows = result.fetchall()
            return rows[0][0] if rows else 0

    def execute(self) -> Any:
        """Execute the query based on its type"""
        sql, params = self._build_current_sql()
        return self._execute(sql, params)

    def to_sql(self) -> str:
        """
            Generate the complete SQL statement for this query.

            Returns the SQL string that would be executed, with parameters
            properly substituted for better readability.

            Returns::

                str: The complete SQL statement as a string.

            Examples

        query = client.query(User).filter(User.age > 25).order_by(User.name)
                sql = query.to_sql()
                print(sql)  # "SELECT * FROM users WHERE age > 25 ORDER BY name"

                query = client.query(User).update(name="New Name").filter(User.id == 1)
                sql = query.to_sql()
                print(sql)  # "UPDATE users SET name = 'New Name' WHERE id = 1"

            Notes:
                - This method returns the SQL with parameters substituted
                - Use this for debugging or logging purposes
                - The returned SQL is ready to be executed directly
        """
        sql, params = self._build_current_sql()
        return substitute_parameters(sql, params)

    def explain(self, verbose: bool = False) -> Any:
        """
        Execute EXPLAIN statement for this query.

        Shows the query execution plan without actually executing the query.
        Useful for understanding how MatrixOne will execute the query and
        optimizing query performance.

        Args::

            verbose (bool): Whether to include verbose output.
                           Defaults to False.

        Returns::

            Any: The result set containing the execution plan.

        Examples::

            # Basic EXPLAIN
            plan = client.query(User).filter(User.age > 25).explain()

            # EXPLAIN with verbose output
            plan = client.query(User).filter(User.age > 25).explain(verbose=True)

            # EXPLAIN for complex queries
            plan = (client.query(User)
                   .filter(User.department == 'Engineering')
                   .order_by(User.salary.desc())
                   .explain(verbose=True))

        Notes:
            - EXPLAIN shows the execution plan without executing the query
            - Use verbose=True for more detailed information
            - Helpful for query optimization and performance tuning
        """
        sql, params = self._build_current_sql()

        # Build EXPLAIN statement
        if verbose:
            explain_sql = f"EXPLAIN VERBOSE {sql}"
        else:
            explain_sql = f"EXPLAIN {sql}"

        return self._execute(explain_sql, params)

    def explain_analyze(self, verbose: bool = False) -> Any:
        """
        Execute EXPLAIN ANALYZE statement for this query.

        Shows the query execution plan and actually executes the query,
        providing both the plan and actual execution statistics.
        Useful for understanding query performance with real data.

        Args::

            verbose (bool): Whether to include verbose output.
                           Defaults to False.

        Returns::

            Any: The result set containing the execution plan and statistics.

        Examples::

            # Basic EXPLAIN ANALYZE
            result = client.query(User).filter(User.age > 25).explain_analyze()

            # EXPLAIN ANALYZE with verbose output
            result = client.query(User).filter(User.age > 25).explain_analyze(verbose=True)

            # EXPLAIN ANALYZE for complex queries
            result = (client.query(User)
                    .filter(User.department == 'Engineering')
                    .order_by(User.salary.desc())
                    .explain_analyze(verbose=True))

        Notes:
            - EXPLAIN ANALYZE actually executes the query and shows statistics
            - Use verbose=True for more detailed information
            - Provides actual execution time and row counts
            - Use with caution on large datasets as it executes the full query
        """
        sql, params = self._build_current_sql()

        # Build EXPLAIN ANALYZE statement
        if verbose:
            explain_sql = f"EXPLAIN ANALYZE VERBOSE {sql}"
        else:
            explain_sql = f"EXPLAIN ANALYZE {sql}"

        return self._execute(explain_sql, params)

    def _get_export_manager(self):
        """Get the appropriate export manager for current context (sync/async, transaction/non-transaction)"""
        from .export import ExportManager

        return ExportManager(self.client if not self.transaction_wrapper else self.transaction_wrapper)

    def _parse_export_format(self, format: str):
        """Parse format string to ExportFormat enum"""
        from .export import ExportFormat

        return ExportFormat.CSV if format.lower() == 'csv' else ExportFormat.JSONLINE

    def export_to_file(
        self,
        filepath: str,
        format: str = 'csv',
        fields_terminated_by: Optional[str] = None,
        fields_enclosed_by: Optional[str] = None,
        lines_terminated_by: Optional[str] = None,
        header: bool = False,
        max_file_size: Optional[int] = None,
        force_quote: Optional[list] = None,
    ) -> Any:
        """
        Export query results to a file using SELECT ... INTO OUTFILE.

        This is a convenience method that builds the SELECT query and exports it
        to a file on the MatrixOne server's filesystem.

        Args:
            filepath: Absolute path on server filesystem where file will be created
            format: Export file format ('csv' or 'jsonline')
            fields_terminated_by: Field delimiter (default: ',' for CSV)
            fields_enclosed_by: Field enclosure character (default: '"' for CSV)
            lines_terminated_by: Line terminator (default: '\\n')
            header: Whether to include column headers (default: False)
            max_file_size: Maximum size per file in bytes (for splitting large exports)
            force_quote: List of column names/indices to always quote

        Returns:
            ResultSet with export operation results

        Examples:
            >>> # Export filtered users to CSV with headers
            >>> client.query(User).filter(User.age > 25).export_to_file(
            ...     filepath="/tmp/users_over_25.csv",
            ...     format='csv',
            ...     header=True
            ... )

            >>> # Export aggregated data
            >>> from sqlalchemy import func
            >>> client.query(Order, func.sum(Order.amount).label('total')) \\
            ...     .group_by(Order.customer_id) \\
            ...     .export_to_file("/tmp/customer_totals.csv", format='csv', header=True)

        Note:
            - The filepath must be on the MatrixOne server's filesystem
            - The MatrixOne server process must have write permissions
            - For more control, use client.export.to_file() with the query's SQL
        """
        # Build the SELECT query SQL
        sql, params = self._build_sql()

        # Use the export manager to export
        export_manager = self._get_export_manager()
        export_format = self._parse_export_format(format)

        return export_manager.to_file(
            query=sql,
            filepath=filepath,
            format=export_format,
            fields_terminated_by=fields_terminated_by,
            fields_enclosed_by=fields_enclosed_by,
            lines_terminated_by=lines_terminated_by,
            header=header,
            max_file_size=max_file_size,
            force_quote=force_quote,
        )

    def export_to_stage(
        self,
        stage_name: str,
        filename: str,
        format: str = 'csv',
        fields_terminated_by: Optional[str] = None,
        fields_enclosed_by: Optional[str] = None,
        lines_terminated_by: Optional[str] = None,
        header: bool = False,
        max_file_size: Optional[int] = None,
        force_quote: Optional[list] = None,
        compression: Optional[str] = None,
    ) -> Any:
        """
        Export query results to a stage using SELECT ... INTO STAGE (Recommended).

        This is the recommended way to export query results. It builds the SELECT query
        and exports it to an external stage (S3, local filesystem, etc.).

        Args:
            stage_name: Name of the target stage
            filename: Filename to create in the stage
            format: Export file format ('csv' or 'jsonline')
            fields_terminated_by: Field delimiter (default: ',' for CSV)
            fields_enclosed_by: Field enclosure character (default: '"' for CSV)
            lines_terminated_by: Line terminator (default: '\\n')
            header: Whether to include column headers (default: False)
            max_file_size: Maximum size per file in bytes (for splitting large exports)
            force_quote: List of column names/indices to always quote
            compression: Compression format (e.g., 'gzip', 'bzip2')

        Returns:
            ResultSet with export operation results

        Examples:
            >>> # Export users to S3 stage with compression
            >>> client.query(User).filter(User.active == True).export_to_stage(
            ...     stage_name='s3_backup',
            ...     filename='active_users.csv',
            ...     format='csv',
            ...     header=True,
            ...     compression='gzip'
            ... )

            >>> # Export aggregated sales data
            >>> from sqlalchemy import func
            >>> client.query(Sale) \\
            ...     .select(Sale.product_id, func.sum(Sale.quantity).label('total_sold')) \\
            ...     .group_by(Sale.product_id) \\
            ...     .having(func.sum(Sale.quantity) > 100) \\
            ...     .export_to_stage('data_warehouse', 'sales_summary.csv', header=True)

            >>> # Export to JSONLINE format
            >>> client.query(Event).filter(Event.timestamp > '2025-01-01').export_to_stage(
            ...     stage_name='event_archive',
            ...     filename='events_2025.jsonl',
            ...     format='jsonline'
            ... )

        Note:
            - The stage must exist before exporting
            - The stage must have appropriate write permissions
            - Use compression for large exports to save storage and transfer costs
            - This is more flexible than export_to_file() as it supports cloud storage
        """
        # Build the SELECT query SQL
        sql, params = self._build_sql()

        # Use the export manager to export
        export_manager = self._get_export_manager()
        export_format = self._parse_export_format(format)

        return export_manager.to_stage(
            query=sql,
            stage_name=stage_name,
            filename=filename,
            format=export_format,
            fields_terminated_by=fields_terminated_by,
            fields_enclosed_by=fields_enclosed_by,
            lines_terminated_by=lines_terminated_by,
            header=header,
            max_file_size=max_file_size,
            force_quote=force_quote,
            compression=compression,
        )

    def export_to(self, stage_or_filepath: str, filename: Optional[str] = None, **kwargs) -> Any:
        """
        Smart export method that automatically chooses between file or stage export.

        This is a convenience method that determines whether to export to a file or stage
        based on the arguments provided. If filename is provided, it exports to a stage.
        Otherwise, it exports to a file.

        Args:
            stage_or_filepath: Stage name (if filename provided) or file path (if filename not provided)
            filename: Optional filename for stage export. If provided, exports to stage.
            **kwargs: Additional export options (format, header, compression, etc.)

        Returns:
            ResultSet with export operation results

        Examples:
            >>> # Export to stage (recommended)
            >>> client.query(User).filter(User.age > 25).export_to(
            ...     'my_stage',
            ...     'users_over_25.csv',
            ...     format='csv',
            ...     header=True
            ... )

            >>> # Export to file
            >>> client.query(Order).filter(Order.status == 'completed').export_to(
            ...     '/tmp/completed_orders.csv',
            ...     format='csv',
            ...     header=True
            ... )

            >>> # Export with compression
            >>> client.query(Event).export_to(
            ...     'event_archive',
            ...     'events_2025.csv',
            ...     format='csv',
            ...     header=True,
            ...     compression='gzip'
            ... )

        Note:
            - If filename is provided, exports to stage (more flexible, supports cloud storage)
            - If filename is not provided, exports to local file on server
            - For better clarity, consider using export_to_stage() or export_to_file() directly
        """
        if filename:
            # Export to stage
            return self.export_to_stage(stage_or_filepath, filename, **kwargs)
        else:
            # Export to file
            return self.export_to_file(stage_or_filepath, **kwargs)


# Helper functions for ORDER BY
def desc(column: str) -> str:
    """Create descending order clause"""
    return f"{column} DESC"


def asc(column: str) -> str:
    """Create ascending order clause"""
    return f"{column} ASC"


class LogicalIn:
    """
    Helper class for creating IN conditions that can be used in filter() method.

    This class provides a way to create IN conditions with support for various
    value types including FulltextFilter objects, lists, and SQLAlchemy expressions.

    Usage
        # List of values
        query.filter(logical_in("city", ["北京", "上海", "广州"]))
        query.filter(logical_in(User.id, [1, 2, 3, 4]))

        # FulltextFilter
        query.filter(logical_in("id", boolean_match("title", "content").must("python")))

        # Subquery
        subquery = client.query(User).select(User.id).filter(User.active == True)
        query.filter(logical_in("author_id", subquery))
    """

    def __init__(self, column, values):
        self.column = column
        self.values = values

    def compile_for_query(self, model, alias):
        return self.compile(model=model, alias=alias, table=model if isinstance(model, str) else None)

    def compile(
        self, compile_kwargs=None, model=None, alias=None, table=None, _table_supplier=None, _column_compiler=None
    ):
        if hasattr(self.column, 'compile'):
            column = (
                _column_compiler(self.column) if _column_compiler else compile_expression(self.column, model, alias)
            )
        else:
            column = str(self.column)
        values = self.values
        if hasattr(values, 'query_builder'):
            source = _table_supplier() if _table_supplier is not None else None
            if source is None:
                source = sql_identifier(table or getattr(model, '__tablename__', None) or 'table', qualified=True)
            body = 'SELECT id FROM ' + source + ' WHERE ' + compile_expression(values)
        elif hasattr(values, '_build_sql'):
            sql, params = values._build_sql()
            body = substitute_parameters(sql, params)
        elif hasattr(values, 'compile'):
            body = compile_expression(values)
        elif isinstance(values, (list, tuple)):
            if not values:
                return '1=0'
            body = ','.join(sql_literal(value) for value in values)
        else:
            body = sql_literal(values)
        return f'{column} IN ({body})'


def logical_in(column, values):
    """
    Create a logical IN condition for use in filter() method.

    This function provides enhanced IN functionality that can handle various
    types of values and expressions, making it more flexible than standard
    SQLAlchemy IN operations. It automatically generates appropriate SQL
    based on the input type.

    Key Features:

    - Support for lists of values with automatic SQL generation
    - Integration with SQLAlchemy expressions
    - FulltextFilter support for complex search conditions
    - Subquery support for dynamic value sets
    - Automatic parameter binding and SQL injection prevention

    Args::

        column: Column to check against. Can be:
            - String column name: "user_id"
            - SQLAlchemy column: User.id
            - Column expression: func.upper(User.name)
        values: Values to check against. Can be:
            - List of values: [1, 2, 3, "a", "b"]
            - Single value: 42 or "test"
            - SQLAlchemy expression: func.count(User.id)
            - FulltextFilter object: boolean_match("title", "content").must("python")
            - Subquery object: client.query(User).select(User.id)
            - None: Creates "column IN (NULL)" condition

    Returns::

        LogicalIn: A logical IN condition object that can be used in filter().
                   The object automatically compiles to appropriate SQL when used.

    Examples
        # List of values - generates: WHERE city IN ('北京', '上海', '广州')
        query.filter(logical_in("city", ["北京", "上海", "广州"]))
        query.filter(logical_in(User.id, [1, 2, 3, 4]))

        # Single value - generates: WHERE id IN (42)
        query.filter(logical_in("id", 42))

        # SQLAlchemy expression - generates: WHERE id IN (SELECT COUNT(*) FROM users)
        query.filter(logical_in("id", func.count(User.id)))

        # FulltextFilter - generates: WHERE id IN (SELECT id FROM table WHERE MATCH(...))
        query.filter(logical_in(User.id, boolean_match("title", "content").must("python")))

        # Subquery - generates: WHERE user_id IN (SELECT id FROM active_users)
        active_user_ids = client.query(User).select(User.id).filter(User.active == True)
        query.filter(logical_in("user_id", active_user_ids))

        # NULL value - generates: WHERE id IN (NULL)
        query.filter(logical_in("id", None))

    Note: This function is designed to work seamlessly with MatrixOne's query
    builder and provides better integration than standard SQLAlchemy IN operations.
    """
    return LogicalIn(column, values)
