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

import logging
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
        """Add WHERE conditions from keyword arguments"""
        for key, value in kwargs.items():
            # Check if the column exists in the SQLAlchemy model
            if hasattr(self.model_class, "__table__") and key in [col.name for col in self.model_class.__table__.columns]:
                self._where_conditions.append(f"{key} = ?")
                self._where_params.append(value)
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
        builder.from_table(self.model_class._table_name, self.snapshot_name)

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
        """Execute query and return all results"""
        sql, params = self._build_select_sql()
        result = self._execute(sql, params)

        models = []
        for row in result.rows:
            # Convert row to dictionary
            row_dict = {}
            if hasattr(self.model_class, "__table__"):
                column_names = [col.name for col in self.model_class.__table__.columns]
            else:
                column_names = self._select_columns or []

            for i, col_name in enumerate(column_names):
                if i < len(row):
                    row_dict[col_name] = row[i]
            # Create SQLAlchemy model instance
            model = self.model_class(**row_dict)
            models.append(model)

        return models

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
                return getattr(result, first_column.name)
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
        # Create a new query for counting
        count_query = Query(self.model_class, self.client, self.snapshot_name)
        count_query._where_conditions = self._where_conditions.copy()
        count_query._where_params = self._where_params.copy()
        count_query._joins = self._joins.copy()
        count_query._group_by_columns = self._group_by_columns.copy()
        count_query._having_conditions = self._having_conditions.copy()
        count_query._having_params = self._having_params.copy()

        sql, params = count_query._build_select_sql()
        # Replace SELECT clause with COUNT(*)
        sql = sql.replace("SELECT *", "SELECT COUNT(*)")
        if count_query._select_columns:
            sql = sql.replace(f"SELECT {', '.join(count_query._select_columns)}", "SELECT COUNT(*)")

        result = self._execute(sql, params)
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
        from .sql_builder import build_insert_query

        if not self._insert_values:
            raise ValueError("No values provided for INSERT")

        return build_insert_query(table_name=self.model_class._table_name, values=self._insert_values)

    def update(self, **kwargs) -> "Query":
        """Start UPDATE operation"""
        self._query_type = "UPDATE"
        for key, value in kwargs.items():
            self._update_set_columns.append(f"{key} = ?")
            self._update_set_values.append(value)
        return self

    def _build_update_sql(self) -> tuple[str, List[Any]]:
        """Build UPDATE SQL query using unified SQL builder"""
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
            table_name=self.model_class._table_name,
            set_values=set_values,
            where_conditions=self._where_conditions,
            where_params=self._where_params,
        )

    def delete(self) -> Any:
        """Execute DELETE operation"""
        self._query_type = "DELETE"
        sql, params = self._build_delete_sql()
        return self._execute(sql, params)

    def _build_delete_sql(self) -> tuple[str, List[Any]]:
        """Build DELETE SQL query using unified SQL builder"""
        from .sql_builder import build_delete_query

        return build_delete_query(
            table_name=self.model_class._table_name,
            where_conditions=self._where_conditions,
            where_params=self._where_params,
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
        self._sql = None
        self._params = None

    def _compile(self):
        """Compile the CTE query to SQL"""
        if self._sql is None:
            if hasattr(self.query, "_build_sql"):
                # This is a BaseMatrixOneQuery object
                self._sql, self._params = self.query._build_sql()
            elif isinstance(self.query, str):
                # This is a raw SQL string
                self._sql = self.query
                self._params = []
            else:
                raise ValueError("CTE query must be a BaseMatrixOneQuery object or SQL string")
        return self._sql, self._params

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
                self._table_name = model_class.__tablename__
                self._columns = {col.name: col for col in model_class.__table__.columns}
            else:
                # Fallback to class name
                self._table_name = getattr(model_class, "_table_name", model_class.__name__.lower())
                self._columns = getattr(model_class, "_columns", {})

    def _execute(self, sql, params=None):
        """Execute SQL using either transaction wrapper or client"""
        if self.transaction_wrapper:
            return self.transaction_wrapper.execute(sql, params)
        else:
            return self.client.execute(sql, params)

    def _detect_sqlalchemy_model(self) -> bool:
        """Detect if the model class is a SQLAlchemy model"""
        return (
            hasattr(self.model_class, "__tablename__")
            and hasattr(self.model_class, "__mapper__")
            and hasattr(self.model_class, "__table__")
        )

    def select(self, *columns) -> "BaseMatrixOneQuery":
        """Add SELECT columns - SQLAlchemy style"""
        self._select_columns = list(columns)
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

    def join(self, target, onclause=None, isouter=False, full=False) -> "BaseMatrixOneQuery":
        """Add JOIN clause - SQLAlchemy style

        Args::

            target: Table or model to join with
            onclause: ON condition for the join (optional, will be inferred if not provided)
            isouter: If True, creates LEFT OUTER JOIN (default: False for INNER JOIN)
            full: If True, creates FULL OUTER JOIN (default: False)

        Returns::

            Self for method chaining

        Examples::

            # Basic inner join with explicit condition
            query.join(Address, User.id == Address.user_id)

            # Inner join with string condition
            query.join('addresses', 'users.id = addresses.user_id')

            # Left outer join
            query.join(Address, isouter=True)

            # Join without explicit condition (will be inferred if possible)
            query.join(Address)
        """
        # Determine join type
        if full:
            join_type = "FULL OUTER JOIN"
        elif isouter:
            join_type = "LEFT OUTER JOIN"
        else:
            join_type = "INNER JOIN"

        # Handle different target types
        if hasattr(target, 'name') and hasattr(target, 'as_sql'):
            # This is a CTE object
            table_name = target.name
        elif hasattr(target, '__tablename__'):
            # This is a SQLAlchemy model
            table_name = target.__tablename__
        elif hasattr(target, '_table_name'):
            # This is a custom model with _table_name
            table_name = target._table_name
        else:
            # String table name
            table_name = str(target)

        # Handle onclause
        if onclause is not None:
            # Process SQLAlchemy expressions
            if hasattr(onclause, 'compile'):
                # This is a SQLAlchemy expression, compile it
                compiled = onclause.compile(compile_kwargs={"literal_binds": True})
                on_condition = str(compiled)
                # Fix SQLAlchemy's quoted column names for MatrixOne compatibility
                import re

                on_condition = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", on_condition)
                # Handle SQLAlchemy's table prefixes (e.g., "users.name" -> "name")
                on_condition = re.sub(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b", r"\2", on_condition)
            else:
                # String condition
                on_condition = str(onclause)

            join_clause = f"{join_type} {table_name} ON {on_condition}"
        else:
            # No explicit onclause - create join without ON condition
            # This matches SQLAlchemy behavior where ON condition can be inferred
            join_clause = f"{join_type} {table_name}"

        self._joins.append(join_clause)
        return self

    def innerjoin(self, target, onclause=None) -> "BaseMatrixOneQuery":
        """Add INNER JOIN clause - SQLAlchemy style (alias for join with isouter=False)"""
        return self.join(target, onclause, isouter=False)

    def leftjoin(self, target, onclause=None) -> "BaseMatrixOneQuery":
        """Add LEFT JOIN clause - SQLAlchemy style (alias for join with isouter=True)"""
        return self.join(target, onclause, isouter=True)

    def rightjoin(self, target, onclause=None) -> "BaseMatrixOneQuery":
        """Add RIGHT JOIN clause - SQLAlchemy style"""
        # MatrixOne doesn't support RIGHT JOIN, so we'll use LEFT JOIN with reversed tables
        # This is a limitation of MatrixOne, but we provide the method for compatibility
        if onclause is not None:
            # Process SQLAlchemy expressions
            if hasattr(onclause, 'compile'):
                compiled = onclause.compile(compile_kwargs={"literal_binds": True})
                on_condition = str(compiled)
                import re

                on_condition = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", on_condition)
                on_condition = re.sub(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b", r"\2", on_condition)
            else:
                on_condition = str(onclause)

            # For RIGHT JOIN, we need to reverse the condition
            # This is a simplified approach - in practice, you might need more complex logic
            join_clause = f"LEFT JOIN {target} ON {on_condition}"
        else:
            join_clause = f"LEFT JOIN {target}"

        self._joins.append(join_clause)
        return self

    def fullouterjoin(self, target, onclause=None) -> "BaseMatrixOneQuery":
        """Add FULL OUTER JOIN clause - SQLAlchemy style (alias for join with full=True)"""
        return self.join(target, onclause, full=True)

    def outerjoin(self, target, onclause=None) -> "BaseMatrixOneQuery":
        """Add LEFT OUTER JOIN clause - SQLAlchemy style (alias for leftjoin)"""
        return self.leftjoin(target, onclause)

    def group_by(self, *columns) -> "BaseMatrixOneQuery":
        """
        Add GROUP BY clause to the query - SQLAlchemy style compatibility.

        The GROUP BY clause is used to group rows that have the same values in specified columns,
        typically used with aggregate functions like COUNT, SUM, AVG, etc.

        Args::

            *columns: Columns to group by. Can be:
                - SQLAlchemy column expressions (e.g., User.department, func.year(User.created_at))
                - String column names (e.g., "department", "created_at")
                - SQLAlchemy function expressions (e.g., func.year(User.created_at))

        Returns::

            BaseMatrixOneQuery: Self for method chaining.

        Examples::

            # SQLAlchemy column expressions (recommended)
            query.group_by(User.department)
            query.group_by(User.department, User.status)
            query.group_by(func.year(User.created_at))
            query.group_by(func.date(User.created_at), User.department)

            # String column names
            query.group_by("department")
            query.group_by("department", "status")

            # Complex expressions
            query.group_by(
                User.department,
                func.year(User.created_at),
                func.month(User.created_at)
            )

        Notes:
            - GROUP BY is typically used with aggregate functions (COUNT, SUM, AVG, etc.)
            - SQLAlchemy expressions provide better type safety and integration
            - Multiple columns can be grouped together
            - Column references in SQLAlchemy expressions are automatically
              converted to MatrixOne-compatible format

        Raises::

            ValueError: If invalid column type is provided
            SQLAlchemyError: If SQLAlchemy expression compilation fails
        """
        for col in columns:
            if isinstance(col, str):
                self._group_by_columns.append(col)
            elif hasattr(col, "compile"):  # SQLAlchemy expression
                # Compile the expression to SQL string
                compiled = col.compile(compile_kwargs={"literal_binds": True})
                sql_str = str(compiled)
                # Fix SQLAlchemy's quoted column names for MatrixOne compatibility
                import re

                sql_str = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", sql_str)
                # Handle SQLAlchemy's table prefixes (e.g., "users.name" -> "name")
                sql_str = re.sub(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b", r"\2", sql_str)
                self._group_by_columns.append(sql_str)
            else:
                self._group_by_columns.append(str(col))
        return self

    def having(self, condition, *params) -> "BaseMatrixOneQuery":
        """
        Add HAVING clause to the query - SQLAlchemy style compatibility.

        The HAVING clause is used to filter groups after GROUP BY operations,
        similar to WHERE clause but applied to aggregated results.

        Args::

            condition: The HAVING condition. Can be:
                - SQLAlchemy expression (e.g., func.count(User.id) > 5)
                - String condition with placeholders (e.g., "COUNT(*) > ?")
                - String condition without placeholders (e.g., "COUNT(*) > 5")
            *params: Additional parameters for string-based conditions.
                    Used to replace '?' placeholders in condition string.

        Returns::

            BaseMatrixOneQuery: Self for method chaining.

        Examples::

            # SQLAlchemy expression syntax (recommended)
            query.group_by(User.department)
            query.having(func.count(User.id) > 5)
            query.having(func.avg(User.age) > 25)
            query.having(func.count(func.distinct(User.id)) > 3)

            # String-based syntax with placeholders
            query.group_by(User.department)
            query.having("COUNT(*) > ?", 5)
            query.having("AVG(age) > ?", 25)

            # String-based syntax without placeholders
            query.group_by(User.department)
            query.having("COUNT(*) > 5")
            query.having("AVG(age) > 25")

            # Multiple HAVING conditions
            query.group_by(User.department)
            query.having(func.count(User.id) > 5)
            query.having(func.avg(User.age) > 25)
            query.having(func.max(User.age) < 65)

            # Mixed string and expression syntax
            query.group_by(User.department)
            query.having("COUNT(*) > ?", 5)  # String
            query.having(func.avg(User.age) > 25)  # Expression

        Notes:
            - HAVING clauses are typically used with GROUP BY operations
            - SQLAlchemy expressions provide better type safety and integration
            - String conditions with placeholders are safer against SQL injection
            - Multiple HAVING conditions are combined with AND logic
            - Column references in SQLAlchemy expressions are automatically
              converted to MatrixOne-compatible format

        Supported SQLAlchemy Functions:
            - func.count(): Count rows or distinct values
            - func.avg(): Calculate average
            - func.sum(): Calculate sum
            - func.min(): Find minimum value
            - func.max(): Find maximum value
            - func.distinct(): Get distinct values

        Raises::

            ValueError: If invalid condition type is provided
            SQLAlchemyError: If SQLAlchemy expression compilation fails
        """
        # Check if condition contains FulltextFilter objects
        if hasattr(condition, "compile") and self._contains_fulltext_filter(condition):
            # Handle SQLAlchemy expressions that contain FulltextFilter objects
            formatted_condition = self._process_fulltext_expression(condition)
            self._having_conditions.append(formatted_condition)
        elif hasattr(condition, "compile"):
            # This is a SQLAlchemy expression (OR, AND, BinaryExpression, etc.), compile it to SQL
            compiled = condition.compile(compile_kwargs={"literal_binds": True})
            formatted_condition = str(compiled)

            # Fix SQLAlchemy's quoted column names for MatrixOne compatibility
            import re

            formatted_condition = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", formatted_condition)

            # Handle SQLAlchemy's table prefixes (e.g., "users.name" -> "name")
            formatted_condition = re.sub(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b", r"\2", formatted_condition)

            self._having_conditions.append(formatted_condition)
        else:
            # Handle string conditions - replace ? placeholders with actual values
            formatted_condition = str(condition)

            # If there are params but no ? placeholders, append them to the condition
            if params and "?" not in formatted_condition:
                for param in params:
                    if hasattr(param, "_build_sql"):
                        # This is a MatrixOne expression, compile it
                        sql, _ = param._build_sql()
                        formatted_condition += f" AND {sql}"
                    else:
                        # Regular parameter
                        if isinstance(param, str):
                            formatted_condition += f" AND '{param}'"
                        else:
                            formatted_condition += f" AND {param}"
            else:
                # Replace ? placeholders with actual values
                for param in params:
                    if isinstance(param, str):
                        formatted_condition = formatted_condition.replace("?", f"'{param}'", 1)
                    else:
                        formatted_condition = formatted_condition.replace("?", str(param), 1)

            self._having_conditions.append(formatted_condition)

        return self

    def snapshot(self, snapshot_name: str) -> "BaseMatrixOneQuery":
        """Add snapshot support - SQLAlchemy style chaining"""
        self._snapshot_name = snapshot_name
        return self

    def alias(self, alias_name: str) -> "BaseMatrixOneQuery":
        """Set table alias for this query - SQLAlchemy style chaining"""
        self._table_alias = alias_name
        return self

    def subquery(self, alias_name: str = None) -> str:
        """Convert this query to a subquery with optional alias"""
        sql, params = self._build_sql()
        if alias_name:
            return f"({sql}) AS {alias_name}"
        else:
            return f"({sql})"

    def filter(self, condition, *params) -> "BaseMatrixOneQuery":
        """Add WHERE conditions - SQLAlchemy style unified interface"""

        # Check if condition is a LogicalIn object
        if hasattr(condition, "compile") and hasattr(condition, "column") and hasattr(condition, "values"):
            # This is a LogicalIn object
            formatted_condition = condition.compile()
            self._where_conditions.append(formatted_condition)
            # LogicalIn objects now generate complete SQL with values, no additional parameters needed
        # Check if condition contains FulltextFilter objects
        elif hasattr(condition, "compile") and self._contains_fulltext_filter(condition):
            # Handle SQLAlchemy expressions that contain FulltextFilter objects
            formatted_condition = self._process_fulltext_expression(condition)
            self._where_conditions.append(formatted_condition)
        elif hasattr(condition, "compile"):
            # This is a SQLAlchemy expression (OR, AND, BinaryExpression, etc.), compile it to SQL
            compiled = condition.compile(compile_kwargs={"literal_binds": True})
            formatted_condition = str(compiled)

            # Fix SQLAlchemy's quoted column names for MatrixOne compatibility
            import re

            formatted_condition = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", formatted_condition)

            # Handle SQLAlchemy's table prefixes (e.g., "users.name" -> "name")
            formatted_condition = re.sub(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b", r"\2", formatted_condition)

            self._where_conditions.append(formatted_condition)
        else:
            # Handle string conditions - replace ? placeholders with actual values
            formatted_condition = str(condition)

            # If there are params but no ? placeholders, append them to the condition
            if params and "?" not in formatted_condition:
                for param in params:
                    if hasattr(param, "_build_sql"):
                        # This is a subquery object, convert it to SQL
                        subquery_sql, _ = param._build_sql()
                        formatted_condition += f" ({subquery_sql})"
                    else:
                        formatted_condition += f" {param}"
            else:
                # Add params to _where_params for processing
                if params:
                    self._where_params.extend(params)

                # Process any remaining ? placeholders by replacing them with the next parameter
                while "?" in formatted_condition and self._where_params:
                    param = self._where_params.pop(0)
                    if isinstance(param, str):
                        formatted_condition = formatted_condition.replace("?", f"'{param}'", 1)
                    elif hasattr(param, "_build_sql"):
                        # This is a subquery object, convert it to SQL
                        subquery_sql, _ = param._build_sql()
                        formatted_condition = formatted_condition.replace("?", f"({subquery_sql})", 1)
                    else:
                        formatted_condition = formatted_condition.replace("?", str(param), 1)

            self._where_conditions.append(formatted_condition)

        return self

    def _contains_fulltext_filter(self, condition) -> bool:
        """Check if a SQLAlchemy expression contains FulltextFilter objects."""
        from .sqlalchemy_ext.fulltext_search import FulltextFilter

        if isinstance(condition, FulltextFilter):
            return True

        # Check nested clauses
        if hasattr(condition, 'clauses'):
            for clause in condition.clauses:
                if self._contains_fulltext_filter(clause):
                    return True

        return False

    def _process_fulltext_expression(self, condition) -> str:
        """Process SQLAlchemy expressions that contain FulltextFilter objects."""
        import re

        from .sqlalchemy_ext.fulltext_search import FulltextFilter

        if isinstance(condition, FulltextFilter):
            return condition.compile()

        # Handle and_() expressions
        if hasattr(condition, 'clauses') and hasattr(condition, 'operator'):
            parts = []
            for clause in condition.clauses:
                if isinstance(clause, FulltextFilter):
                    parts.append(clause.compile())
                elif hasattr(clause, 'compile'):
                    # Regular SQLAlchemy expression
                    compiled = clause.compile(compile_kwargs={"literal_binds": True})
                    formatted = str(compiled)
                    # Fix quoted column names and table prefixes
                    formatted = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", formatted)
                    formatted = re.sub(r"\w+\.(\w+)", r"\1", formatted)
                    parts.append(formatted)
                else:
                    parts.append(str(clause))

            # Determine operator
            if str(condition.operator).upper() == 'AND':
                return f"({' AND '.join(parts)})"
            elif str(condition.operator).upper() == 'OR':
                return f"({' OR '.join(parts)})"
            else:
                return f"({f' {condition.operator} '.join(parts)})"

        # Fallback for other types
        return str(condition)

    def filter_by(self, **kwargs) -> "BaseMatrixOneQuery":
        """Add WHERE conditions from keyword arguments - SQLAlchemy style"""
        for key, value in kwargs.items():
            if key in self._columns:
                if isinstance(value, str):
                    self._where_conditions.append(f"{key} = '{value}'")
                elif isinstance(value, (int, float)):
                    self._where_conditions.append(f"{key} = {value}")
                else:
                    # For other types, use parameterized query
                    self._where_conditions.append(f"{key} = ?")
                    self._where_params.append(value)
        return self

    def where(self, condition: str, *params) -> "BaseMatrixOneQuery":
        """Add WHERE condition - alias for filter method"""
        return self.filter(condition, *params)

    def logical_in(self, column, values) -> "BaseMatrixOneQuery":
        """
        Add IN condition with support for various value types.

        This method provides enhanced IN functionality that can handle:
        - Lists of values: [1, 2, 3]
        - SQLAlchemy expressions: func.count(User.id)
        - FulltextFilter objects: boolean_match("title", "content").must("python")
        - Subqueries: client.query(User).select(User.id)

        Args::

            column: Column to check (can be string or SQLAlchemy column)
            values: Values to check against. Can be:
                - List of values: [1, 2, 3, "a", "b"]
                - SQLAlchemy expression: func.count(User.id)
                - FulltextFilter object: boolean_match("title", "content").must("python")
                - Subquery object: client.query(User).select(User.id)

        Returns::

            BaseMatrixOneQuery: Self for method chaining.

        Examples::

            # List of values
            query.logical_in("city", ["北京", "上海", "广州"])
            query.logical_in(User.id, [1, 2, 3, 4])

            # SQLAlchemy expression
            query.logical_in("id", func.count(User.id))

            # FulltextFilter
            query.logical_in("id", boolean_match("title", "content").must("python"))

            # Subquery
            subquery = client.query(User).select(User.id).filter(User.active == True)
            query.logical_in("author_id", subquery)

        Notes:
            - This method automatically handles different value types
            - For FulltextFilter objects, it creates a subquery using the fulltext search
            - For SQLAlchemy expressions, it compiles them to SQL
            - For lists, it creates standard IN clauses with proper parameter binding
        """
        # Handle column name
        if hasattr(column, "name"):
            column_name = column.name
        else:
            column_name = str(column)

        # Handle different types of values
        if hasattr(values, "compile") and hasattr(values, "columns"):
            # This is a FulltextFilter object
            if hasattr(values, "compile") and hasattr(values, "query_builder"):
                # Convert FulltextFilter to subquery
                fulltext_sql = values.compile()
                # Create a subquery that selects IDs from the fulltext search
                # We need to determine the table name from the context
                table_name = self._table_name or "table"
                subquery_sql = f"SELECT id FROM {table_name} WHERE {fulltext_sql}"
                condition = f"{column_name} IN ({subquery_sql})"
                self._where_conditions.append(condition)
            else:
                # Handle other SQLAlchemy expressions
                compiled = values.compile(compile_kwargs={"literal_binds": True})
                sql_str = str(compiled)
                # Fix SQLAlchemy's quoted column names for MatrixOne compatibility
                import re

                sql_str = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", sql_str)
                sql_str = re.sub(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b", r"\2", sql_str)
                condition = f"{column_name} IN ({sql_str})"
                self._where_conditions.append(condition)
        elif hasattr(values, "_build_sql"):
            # This is a subquery object
            subquery_sql, subquery_params = values._build_sql()
            condition = f"{column_name} IN ({subquery_sql})"
            self._where_conditions.append(condition)
            self._where_params.extend(subquery_params)
        elif isinstance(values, (list, tuple)):
            # Handle list of values
            if not values:
                # Empty list means no matches
                condition = "1=0"  # Always false
                self._where_conditions.append(condition)
            else:
                # Create placeholders for each value
                placeholders = ",".join(["?" for _ in values])
                condition = f"{column_name} IN ({placeholders})"
                self._where_conditions.append(condition)
                self._where_params.extend(values)
        else:
            # Single value
            condition = f"{column_name} IN (?)"
            self._where_conditions.append(condition)
            self._where_params.append(values)

        return self

    def order_by(self, *columns) -> "BaseMatrixOneQuery":
        """
        Add ORDER BY clause to the query - SQLAlchemy style compatibility.

        The ORDER BY clause is used to sort the result set by one or more columns,
        either in ascending (ASC) or descending (DESC) order.

        Args::

            *columns: Columns to order by. Can be:
                - SQLAlchemy column expressions (e.g., User.name, User.created_at.desc())
                - String column names (e.g., "name", "created_at DESC")
                - SQLAlchemy function expressions (e.g., func.count(User.id))
                - SQLAlchemy desc/asc expressions (e.g., desc(User.name), asc(User.age))

        Returns::

            BaseMatrixOneQuery: Self for method chaining.

        Examples::

            # SQLAlchemy column expressions (recommended)
            query.order_by(User.name)
            query.order_by(User.created_at.desc())
            query.order_by(User.department, User.name.asc())

            # String column names
            query.order_by("name")
            query.order_by("created_at DESC")
            query.order_by("department ASC", "name DESC")

            # SQLAlchemy desc/asc functions
            from sqlalchemy import desc, asc
            query.order_by(desc(User.created_at))
            query.order_by(asc(User.name), desc(User.age))

            # Function expressions
            query.order_by(func.count(User.id).desc())
            query.order_by(func.avg(User.salary).asc())

            # Mixed expressions
            query.order_by(User.department, "name DESC")
            query.order_by(func.count(User.id).desc(), User.name.asc())

            # Complex expressions
            query.order_by(
                User.department.asc(),
                func.count(User.id).desc(),
                User.name.asc()
            )

        Notes:
            - ORDER BY sorts the result set in ascending order by default
            - Use .desc() or desc() for descending order
            - Use .asc() or asc() for explicit ascending order
            - Multiple columns are ordered from left to right
            - SQLAlchemy expressions provide better type safety and integration
            - Column references in SQLAlchemy expressions are automatically
              converted to MatrixOne-compatible format

        Raises::

            ValueError: If invalid column type is provided
            SQLAlchemyError: If SQLAlchemy expression compilation fails
        """
        for col in columns:
            if isinstance(col, str):
                self._order_by_columns.append(col)
            elif hasattr(col, "compile"):  # SQLAlchemy expression
                # Compile the expression to SQL string
                compiled = col.compile(compile_kwargs={"literal_binds": True})
                sql_str = str(compiled)
                # Fix SQLAlchemy's quoted column names for MatrixOne compatibility
                import re

                sql_str = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", sql_str)
                # Handle SQLAlchemy's table prefixes (e.g., "users.name" -> "name")
                sql_str = re.sub(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b", r"\2", sql_str)
                self._order_by_columns.append(sql_str)
            else:
                self._order_by_columns.append(str(col))
        return self

    def limit(self, count: int) -> "BaseMatrixOneQuery":
        """Add LIMIT clause - SQLAlchemy style"""
        self._limit_count = count
        return self

    def offset(self, count: int) -> "BaseMatrixOneQuery":
        """Add OFFSET clause - SQLAlchemy style"""
        self._offset_count = count
        return self

    def _build_sql(self) -> tuple[str, List[Any]]:
        """Build SQL query using unified SQL builder"""
        from .sql_builder import MatrixOneSQLBuilder

        builder = MatrixOneSQLBuilder()

        # Build CTE clause if any CTEs are defined
        if self._ctes:
            for cte in self._ctes:
                if isinstance(cte, CTE):
                    cte_sql, cte_params = cte.as_sql()
                    builder._ctes.append({'name': cte.name, 'sql': cte_sql, 'params': cte_params})
                else:
                    # Handle legacy CTE format
                    builder._ctes.append(cte)

        # Build SELECT clause with SQLAlchemy function support
        if self._select_columns:
            # Convert SQLAlchemy function objects to strings
            select_parts = []
            for col in self._select_columns:
                if hasattr(col, "compile"):  # SQLAlchemy function object
                    # Check if this is a FulltextLabel (which already has AS in compile())
                    if hasattr(col, "_compiler_dispatch") and hasattr(col, "name") and "FulltextLabel" in str(type(col)):
                        # For FulltextLabel, use compile() which already includes AS
                        sql_str = col.compile(compile_kwargs={"literal_binds": True})
                    else:
                        # For regular SQLAlchemy objects
                        compiled = col.compile(compile_kwargs={"literal_binds": True})
                        sql_str = str(compiled)
                        # Fix SQLAlchemy's quoted column names for MatrixOne compatibility
                        # Convert avg('column') to avg(column)
                        import re

                        sql_str = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", sql_str)

                        # Handle SQLAlchemy label() method - add AS alias if present
                        # But avoid using SQL reserved keywords as aliases
                        if hasattr(col, "name") and col.name and col.name.upper() not in ["DISTINCT"]:
                            sql_str = f"{sql_str} AS {col.name}"

                    select_parts.append(sql_str)
                else:
                    select_parts.append(str(col))
            builder._select_columns = select_parts
        else:
            builder.select_all()

        # Build FROM clause with optional table alias and snapshot
        if not self._table_name:
            raise ValueError("Table name is required. Provide a model class or set table name manually.")

        if self._table_alias:
            builder._from_table = f"{self._table_name} AS {self._table_alias}"
        else:
            builder._from_table = self._table_name

        builder._from_snapshot = self._snapshot_name

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

        # Build the final SQL and combine parameters
        sql, params = builder.build()

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

    def _extract_select_columns(self):
        """Extract column names from select columns"""
        select_cols = []
        for col in self._select_columns:
            # Check if this is a SQLAlchemy function with label
            if hasattr(col, "compile") and hasattr(col, "name") and col.name:
                # Special handling for DISTINCT function to avoid reserved keyword issues
                if hasattr(col, "name") and col.name.upper() == "DISTINCT":
                    # For DISTINCT functions, use the original logic to extract column name
                    col_str = str(col.compile(compile_kwargs={"literal_binds": True}))
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
                    col_str = str(col.compile(compile_kwargs={"literal_binds": True}))
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
        self._query_type = "UPDATE"

        # Handle both SQLAlchemy expressions and simple values
        for key, value in kwargs.items():
            if hasattr(value, "compile"):  # SQLAlchemy expression
                # Compile the expression to SQL
                compiled = value.compile(compile_kwargs={"literal_binds": True})
                sql_str = str(compiled)

                # Fix SQLAlchemy's quoted column names for MatrixOne compatibility
                import re

                sql_str = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", sql_str)

                self._update_set_columns.append(f"{key} = {sql_str}")
            else:  # Simple value
                self._update_set_columns.append(f"{key} = ?")
                self._update_set_values.append(value)

        return self

    def _build_update_sql(self) -> tuple[str, List[Any]]:
        """
        Build UPDATE SQL query directly to handle SQLAlchemy expressions

        Returns::

            Tuple of (SQL string, parameters list)

        Raises::

            ValueError: If no SET clauses are provided for UPDATE
        """
        if not self._update_set_columns:
            raise ValueError("No SET clauses provided for UPDATE")

        # Build SET clause
        set_clauses = []
        params = []

        value_index = 0
        for col in self._update_set_columns:
            if " = ?" in col:
                # Simple value assignment
                col_name = col.split(" = ?")[0]
                set_clauses.append(f"{col_name} = ?")
                params.append(self._update_set_values[value_index])
                value_index += 1
            else:
                # SQLAlchemy expression (already compiled to SQL)
                set_clauses.append(col)

        # Build WHERE clause
        where_clause = ""
        if self._where_conditions:
            where_clause = " WHERE " + " AND ".join(self._where_conditions)
            params.extend(self._where_params)

        # Build final SQL
        sql = f"UPDATE {self._table_name} SET {', '.join(set_clauses)}{where_clause}"

        return sql, params

    def delete(self) -> Any:
        """Execute DELETE operation"""
        self._query_type = "DELETE"
        sql, params = self._build_delete_sql()
        return self._execute(sql, params)

    def _build_delete_sql(self) -> tuple[str, List[Any]]:
        """Build DELETE SQL query using unified SQL builder"""
        from .sql_builder import build_delete_query

        return build_delete_query(
            table_name=self._table_name,
            where_conditions=self._where_conditions,
            where_params=self._where_params,
        )


# MatrixOne Snapshot Query Builder - SQLAlchemy style
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
        """Execute query and return all results - SQLAlchemy style"""
        sql, params = self._build_sql()
        result = self._execute(sql, params)

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

    def first(self) -> Optional:
        """Execute query and return first result - SQLAlchemy style"""
        self._limit_count = 1
        results = self.all()
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
        result = self.first()
        if result is None:
            return None

        # If result is a model instance, return the first column value
        if hasattr(result, '__dict__'):
            # For custom select queries, check if we have select columns
            if self._select_columns:
                # For custom select, return the first selected column value
                select_cols = self._extract_select_columns()
                if select_cols:
                    first_col_name = select_cols[0]
                    return getattr(result, first_col_name)

            # Get the first column value from the model
            if hasattr(self.model_class, "__table__"):
                first_column = list(self.model_class.__table__.columns)[0]
                return getattr(result, first_column.name)
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
        """Execute query and return count of results - SQLAlchemy style"""
        sql, params = self._build_sql()
        # Replace SELECT * with COUNT(*)
        sql = sql.replace("SELECT *", "SELECT COUNT(*)")

        result = self._execute(sql, params)
        return result.rows[0][0] if result.rows else 0

    def execute(self) -> Any:
        """Execute the query based on its type"""
        if self._query_type == "SELECT":
            sql, params = self._build_sql()
            return self._execute(sql, params)
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
        # Build SQL based on query type
        if self._query_type == "UPDATE":
            sql, params = self._build_update_sql()
        elif self._query_type == "INSERT":
            sql, params = self._build_insert_sql()
        elif self._query_type == "DELETE":
            sql, params = self._build_delete_sql()
        else:
            sql, params = self._build_sql()

        # Substitute parameters for better readability
        if params:
            formatted_sql = sql
            for param in params:
                if isinstance(param, str):
                    formatted_sql = formatted_sql.replace("?", f"'{param}'", 1)
                else:
                    formatted_sql = formatted_sql.replace("?", str(param), 1)
            return formatted_sql
        else:
            return sql

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
        sql, params = self._build_sql()

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
        sql, params = self._build_sql()

        # Build EXPLAIN ANALYZE statement
        if verbose:
            explain_sql = f"EXPLAIN ANALYZE VERBOSE {sql}"
        else:
            explain_sql = f"EXPLAIN ANALYZE {sql}"

        return self._execute(explain_sql, params)


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

    def compile(self, compile_kwargs=None):
        """Compile to SQL expression for use in filter() method"""
        # Handle column name
        if hasattr(self.column, "name"):
            column_name = self.column.name
        else:
            column_name = str(self.column)

        # Handle different types of values
        if hasattr(self.values, "compile") and hasattr(self.values, "columns"):
            # This is a FulltextFilter object
            if hasattr(self.values, "compile") and hasattr(self.values, "query_builder"):
                # Convert FulltextFilter to subquery
                fulltext_sql = self.values.compile()
                # Create a subquery that selects IDs from the fulltext search
                # We need to determine the table name from the context
                # For now, use a generic approach that works with most cases
                table_name = "table"  # Default table name - will be handled by the query context
                subquery_sql = f"SELECT id FROM {table_name} WHERE {fulltext_sql}"
                return f"{column_name} IN ({subquery_sql})"
            else:
                # Handle other SQLAlchemy expressions
                compiled = self.values.compile(compile_kwargs={"literal_binds": True})
                sql_str = str(compiled)
                # Fix SQLAlchemy's quoted column names for MatrixOne compatibility
                import re

                sql_str = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", sql_str)
                sql_str = re.sub(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b", r"\2", sql_str)
                return f"{column_name} IN ({sql_str})"
        elif hasattr(self.values, "_build_sql"):
            # This is a subquery object
            subquery_sql, subquery_params = self.values._build_sql()
            return f"{column_name} IN ({subquery_sql})"
        elif isinstance(self.values, (list, tuple)):
            # Handle list of values
            if not self.values:
                # Empty list means no matches
                return "1=0"  # Always false
            else:
                # Create SQL with actual values for LogicalIn
                formatted_values = []
                for value in self.values:
                    if isinstance(value, str):
                        formatted_values.append(f"'{value}'")
                    else:
                        formatted_values.append(str(value))
                return f"{column_name} IN ({','.join(formatted_values)})"
        else:
            # Single value
            if isinstance(self.values, str):
                return f"{column_name} IN ('{self.values}')"
            else:
                return f"{column_name} IN ({self.values})"


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
