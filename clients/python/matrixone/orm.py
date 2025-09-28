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
    """Query builder for ORM operations - SQLAlchemy style"""

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
            if hasattr(self.model_class, "__table__") and key in [
                col.name for col in self.model_class.__table__.columns
            ]:
                self._where_conditions.append(f"{key} = ?")
                self._where_params.append(value)
        return self

    def join(self, table: str, condition: str) -> "Query":
        """Add JOIN clause"""
        self._joins.append(f"JOIN {table} ON {condition}")
        return self

    def group_by(self, *columns) -> "Query":
        """Add GROUP BY clause"""
        self._group_by_columns.extend([str(col) for col in columns])
        return self

    def having(self, condition: str, *params) -> "Query":
        """Add HAVING condition"""
        self._having_conditions.append(condition)
        self._having_params.extend(params)
        return self

    def order_by(self, *columns) -> "Query":
        """Add ORDER BY clause"""
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
        result = self.client.execute(sql, params)

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

        result = self.client.execute(sql, params)
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

    def delete(self) -> "Query":
        """Start DELETE operation"""
        self._query_type = "DELETE"
        return self

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
            return self.client.execute(sql, params)
        elif self._query_type == "UPDATE":
            sql, params = self._build_update_sql()
            return self.client.execute(sql, params)
        elif self._query_type == "DELETE":
            sql, params = self._build_delete_sql()
            return self.client.execute(sql, params)
        else:
            raise ValueError(f"Unknown query type: {self._query_type}")


# Base Query Builder - SQLAlchemy style
class BaseMatrixOneQuery:
    """Base MatrixOne Query builder that contains common SQL building logic"""

    def __init__(self, model_class, client):
        self.model_class = model_class
        self.client = client
        self._snapshot_name = None
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

        # Detect if this is a SQLAlchemy model
        self._is_sqlalchemy_model = self._detect_sqlalchemy_model()

        # Get table name and columns based on model type
        if self._is_sqlalchemy_model:
            self._table_name = model_class.__tablename__
            self._columns = {col.name: col for col in model_class.__table__.columns}
        else:
            # Fallback to class name
            self._table_name = getattr(model_class, "_table_name", model_class.__name__.lower())
            self._columns = getattr(model_class, "_columns", {})

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

    def join(self, table, on=None, isouter=False) -> "BaseMatrixOneQuery":
        """Add JOIN clause - SQLAlchemy style"""
        join_type = "LEFT JOIN" if isouter else "JOIN"
        if on:
            join_clause = f"{join_type} {table} ON {on}"
        else:
            join_clause = f"{join_type} {table}"
        self._joins.append(join_clause)
        return self

    def innerjoin(self, table, on=None) -> "BaseMatrixOneQuery":
        """Add INNER JOIN clause - SQLAlchemy style"""
        if on:
            join_clause = f"INNER JOIN {table} ON {on}"
        else:
            join_clause = f"INNER JOIN {table}"
        self._joins.append(join_clause)
        return self

    def leftjoin(self, table, on=None) -> "BaseMatrixOneQuery":
        """Add LEFT JOIN clause - SQLAlchemy style"""
        if on:
            join_clause = f"LEFT JOIN {table} ON {on}"
        else:
            join_clause = f"LEFT JOIN {table}"
        self._joins.append(join_clause)
        return self

    def rightjoin(self, table, on=None) -> "BaseMatrixOneQuery":
        """Add RIGHT JOIN clause - SQLAlchemy style"""
        if on:
            join_clause = f"RIGHT JOIN {table} ON {on}"
        else:
            join_clause = f"RIGHT JOIN {table}"
        self._joins.append(join_clause)
        return self

    def fullouterjoin(self, table, on=None) -> "BaseMatrixOneQuery":
        """Add FULL OUTER JOIN clause - SQLAlchemy style"""
        if on:
            join_clause = f"FULL OUTER JOIN {table} ON {on}"
        else:
            join_clause = f"FULL OUTER JOIN {table}"
        self._joins.append(join_clause)
        return self

    def outerjoin(self, table, on=None) -> "BaseMatrixOneQuery":
        """Add LEFT OUTER JOIN clause - SQLAlchemy style (alias for leftjoin)"""
        return self.leftjoin(table, on)

    def group_by(self, *columns) -> "BaseMatrixOneQuery":
        """Add GROUP BY clause - SQLAlchemy style"""
        for col in columns:
            if isinstance(col, str):
                self._group_by_columns.append(col)
            else:
                self._group_by_columns.append(str(col))
        return self

    def having(self, condition: str, *params) -> "BaseMatrixOneQuery":
        """Add HAVING clause - SQLAlchemy style"""
        # Replace ? placeholders with actual values
        formatted_condition = condition
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

        # Check if condition contains FulltextFilter objects
        if hasattr(condition, "compile") and self._contains_fulltext_filter(condition):
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
            formatted_condition = re.sub(r"\w+\.(\w+)", r"\1", formatted_condition)

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

    def order_by(self, *columns) -> "BaseMatrixOneQuery":
        """Add ORDER BY clause - SQLAlchemy style"""
        for col in columns:
            if isinstance(col, str):
                self._order_by_columns.append(col)
            elif hasattr(col, "compile"):  # SQLAlchemy function object
                # Compile the function to SQL string
                compiled = col.compile(compile_kwargs={"literal_binds": True})
                sql_str = str(compiled)
                # Fix SQLAlchemy's quoted column names for MatrixOne compatibility
                import re

                sql_str = re.sub(r"(\w+)\('([^']+)'\)", r"\1(\2)", sql_str)
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

        # Build SELECT clause with SQLAlchemy function support
        if self._select_columns:
            # Convert SQLAlchemy function objects to strings
            select_parts = []
            for col in self._select_columns:
                if hasattr(col, "compile"):  # SQLAlchemy function object
                    # Compile the function to SQL string
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

        return builder.build()

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


# MatrixOne Snapshot Query Builder - SQLAlchemy style
class MatrixOneQuery(BaseMatrixOneQuery):
    """MatrixOne Query builder that mimics SQLAlchemy Query interface"""

    def __init__(self, model_class, client):
        super().__init__(model_class, client)

    def all(self) -> List:
        """Execute query and return all results - SQLAlchemy style"""
        sql, params = self._build_sql()
        result = self.client.execute(sql, params)

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

    def count(self) -> int:
        """Execute query and return count of results - SQLAlchemy style"""
        sql, params = self._build_sql()
        # Replace SELECT * with COUNT(*)
        sql = sql.replace("SELECT *", "SELECT COUNT(*)")

        result = self.client.execute(sql, params)
        return result.rows[0][0] if result.rows else 0


# Helper functions for ORDER BY
def desc(column: str) -> str:
    """Create descending order clause"""
    return f"{column} DESC"


def asc(column: str) -> str:
    """Create ascending order clause"""
    return f"{column} ASC"


class CTEQuery:
    """CTE (Common Table Expression) Query builder for MatrixOne"""

    def __init__(self, client):
        self.client = client
        self._ctes = []  # List of CTE definitions
        self._main_query = None  # The main query that uses CTEs

    def with_cte(self, name: str, query) -> "CTEQuery":
        """Add a CTE definition"""
        if hasattr(query, "_build_sql"):
            # This is a BaseMatrixOneQuery object
            sql, params = query._build_sql()
            self._ctes.append({"name": name, "sql": sql, "params": params})
        elif isinstance(query, str):
            # This is a raw SQL string
            self._ctes.append({"name": name, "sql": query, "params": []})
        else:
            raise ValueError("CTE query must be a BaseMatrixOneQuery object or SQL string")
        return self

    def select_from(self, *columns) -> "CTEQuery":
        """Start the main SELECT query that uses the CTEs"""
        self._main_query = {
            "type": "SELECT",
            "columns": list(columns) if columns else ["*"],
            "from_table": None,
            "joins": [],
            "where_conditions": [],
            "where_params": [],
            "group_by_columns": [],
            "having_conditions": [],
            "having_params": [],
            "order_by_columns": [],
            "limit_count": None,
            "offset_count": None,
        }
        return self

    def from_table(self, table_name: str, alias: str = None) -> "CTEQuery":
        """Set the FROM table for the main query"""
        if not self._main_query:
            raise ValueError("Must call select_from() first")
        if alias:
            self._main_query["from_table"] = f"{table_name} AS {alias}"
        else:
            self._main_query["from_table"] = table_name
        return self

    def join(self, table: str, on: str = None, join_type: str = "JOIN") -> "CTEQuery":
        """Add JOIN clause to main query"""
        if not self._main_query:
            raise ValueError("Must call select_from() first")
        if on:
            join_clause = f"{join_type} {table} ON {on}"
        else:
            join_clause = f"{join_type} {table}"
        self._main_query["joins"].append(join_clause)
        return self

    def inner_join(self, table: str, on: str = None) -> "CTEQuery":
        """Add INNER JOIN clause"""
        return self.join(table, on, "INNER JOIN")

    def left_join(self, table: str, on: str = None) -> "CTEQuery":
        """Add LEFT JOIN clause"""
        return self.join(table, on, "LEFT JOIN")

    def right_join(self, table: str, on: str = None) -> "CTEQuery":
        """Add RIGHT JOIN clause"""
        return self.join(table, on, "RIGHT JOIN")

    def full_outer_join(self, table: str, on: str = None) -> "CTEQuery":
        """Add FULL OUTER JOIN clause"""
        return self.join(table, on, "FULL OUTER JOIN")

    def where(self, condition: str, *params) -> "CTEQuery":
        """Add WHERE condition to main query"""
        if not self._main_query:
            raise ValueError("Must call select_from() first")
        self._main_query["where_conditions"].append(condition)
        self._main_query["where_params"].extend(params)
        return self

    def group_by(self, *columns) -> "CTEQuery":
        """Add GROUP BY clause to main query"""
        if not self._main_query:
            raise ValueError("Must call select_from() first")
        self._main_query["group_by_columns"].extend(columns)
        return self

    def having(self, condition: str, *params) -> "CTEQuery":
        """Add HAVING clause to main query"""
        if not self._main_query:
            raise ValueError("Must call select_from() first")
        self._main_query["having_conditions"].append(condition)
        self._main_query["having_params"].extend(params)
        return self

    def order_by(self, *columns) -> "CTEQuery":
        """Add ORDER BY clause to main query"""
        if not self._main_query:
            raise ValueError("Must call select_from() first")
        self._main_query["order_by_columns"].extend(columns)
        return self

    def limit(self, count: int) -> "CTEQuery":
        """Add LIMIT clause to main query"""
        if not self._main_query:
            raise ValueError("Must call select_from() first")
        self._main_query["limit_count"] = count
        return self

    def offset(self, count: int) -> "CTEQuery":
        """Add OFFSET clause to main query"""
        if not self._main_query:
            raise ValueError("Must call select_from() first")
        self._main_query["offset_count"] = count
        return self

    def _build_sql(self) -> tuple[str, List[Any]]:
        """Build the complete CTE SQL query using unified SQL builder"""
        if not self._ctes:
            raise ValueError("At least one CTE must be defined")
        if not self._main_query:
            raise ValueError("Main query must be defined with select_from()")

        from .sql_builder import MatrixOneSQLBuilder

        builder = MatrixOneSQLBuilder()

        # Add CTEs
        for cte in self._ctes:
            builder.with_cte(cte["name"], cte["sql"], cte["params"])

        # Build main query
        builder.select(*self._main_query["columns"])

        if self._main_query["from_table"]:
            builder.from_table(self._main_query["from_table"])

        # Add JOIN clauses
        builder._joins = self._main_query["joins"].copy()

        # Add WHERE conditions and parameters
        builder._where_conditions = self._main_query["where_conditions"].copy()
        builder._where_params = self._main_query["where_params"].copy()

        # Add GROUP BY columns
        if self._main_query["group_by_columns"]:
            builder.group_by(*self._main_query["group_by_columns"])

        # Add HAVING conditions and parameters
        builder._having_conditions = self._main_query["having_conditions"].copy()
        builder._having_params = self._main_query["having_params"].copy()

        # Add ORDER BY columns
        if self._main_query["order_by_columns"]:
            builder.order_by(*self._main_query["order_by_columns"])

        # Add LIMIT and OFFSET
        if self._main_query["limit_count"] is not None:
            builder.limit(self._main_query["limit_count"])
        if self._main_query["offset_count"] is not None:
            builder.offset(self._main_query["offset_count"])

        return builder.build()

    def execute(self) -> List:
        """Execute the CTE query and return results"""
        sql, params = self._build_sql()
        result = self.client.execute(sql, params)

        # Return raw row data as simple objects
        rows = []
        for row in result.rows:
            # Create a simple object with attributes for each column
            row_obj = type("RowData", (), {})()
            used_attr_names = set()  # Track used attribute names to handle duplicates

            for i, col_name in enumerate(self._main_query["columns"]):
                if i < len(row):
                    # Handle column names with table prefixes (e.g., "dept_stats.department_id" -> "department_id")
                    if "." in col_name:
                        attr_name = col_name.split(".")[-1]  # Take the part after the dot
                    else:
                        attr_name = col_name

                    # Replace spaces and special characters with underscores
                    attr_name = attr_name.replace(" ", "_").replace("(", "").replace(")", "")

                    # Handle duplicate attribute names by adding a suffix
                    original_attr_name = attr_name
                    counter = 2
                    while attr_name in used_attr_names:
                        attr_name = f"{original_attr_name}_{counter}"
                        counter += 1

                    used_attr_names.add(attr_name)
                    setattr(row_obj, attr_name, row[i])
            rows.append(row_obj)

        return rows

    def all(self) -> List:
        """Alias for execute() - SQLAlchemy style"""
        return self.execute()

    def first(self) -> Optional:
        """Execute query and return first result"""
        self.limit(1)
        results = self.execute()
        return results[0] if results else None

    def count(self) -> int:
        """Execute query and return count of results"""
        # Create a count query
        count_sql_parts = []
        all_params = []

        # Build WITH clause (same as original)
        with_clause = "WITH "
        cte_parts = []
        for cte in self._ctes:
            cte_parts.append(f"{cte['name']} AS ({cte['sql']})")
            all_params.extend(cte["params"])
        with_clause += ", ".join(cte_parts)
        count_sql_parts.append(with_clause)

        # Build main SELECT COUNT(*) query
        count_sql_parts.append("SELECT COUNT(*)")

        # Build FROM clause
        if self._main_query["from_table"]:
            count_sql_parts.append(f"FROM {self._main_query['from_table']}")

        # Build JOIN clauses
        if self._main_query["joins"]:
            count_sql_parts.append(" ".join(self._main_query["joins"]))

        # Build WHERE clause
        if self._main_query["where_conditions"]:
            where_clause = "WHERE " + " AND ".join(self._main_query["where_conditions"])
            count_sql_parts.append(where_clause)
            all_params.extend(self._main_query["where_params"])

        # Build GROUP BY clause
        if self._main_query["group_by_columns"]:
            group_clause = "GROUP BY " + ", ".join(self._main_query["group_by_columns"])
            count_sql_parts.append(group_clause)

        # Build HAVING clause
        if self._main_query["having_conditions"]:
            having_clause = "HAVING " + " AND ".join(self._main_query["having_conditions"])
            count_sql_parts.append(having_clause)
            all_params.extend(self._main_query["having_params"])

        count_sql = " ".join(count_sql_parts)
        result = self.client.execute(count_sql, all_params)
        return result.rows[0][0] if result.rows else 0
