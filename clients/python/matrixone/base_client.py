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
Base client functionality for MatrixOne clients
"""

# No imports needed for this module


def insert_row_values(row, columns):
    """One batch policy: first-row columns, missing NULL, no extra fields."""
    unexpected = set(row) - set(columns)
    if unexpected:
        raise ValueError('unexpected INSERT columns: ' + ', '.join(sorted(unexpected)))
    return [row.get(column) for column in columns]


class BaseMatrixOneClient:
    """
    Base class for MatrixOne clients with common SQL building logic.

    This abstract base class provides common functionality and SQL building
    logic that is shared between synchronous and asynchronous MatrixOne
    clients. It includes methods for constructing SQL statements, handling
    parameters, and managing common database operations.

    Key Features:

    - Common SQL building logic for INSERT, UPDATE, DELETE operations
    - Parameter substitution and SQL injection prevention
    - Batch operation support
    - Error handling and validation
    - Integration with MatrixOne's SQL syntax

    Supported Operations:

    - INSERT statement generation with parameter binding
    - UPDATE statement generation with WHERE conditions
    - DELETE statement generation with WHERE conditions
    - Batch INSERT operations for multiple rows
    - Parameter substitution and escaping

    Usage:

        This class is not intended to be used directly. It serves as a base
        class for Client and AsyncClient implementations, providing common
        functionality and reducing code duplication.

    Note: This is an internal base class. Use Client or AsyncClient for
    actual database operations.
    """

    def set_query_observer(self, observer=None):
        """Set a synchronous, optional callback receiving immutable QueryEvent.

        Events contain timings/outcomes, never SQL, binds or credentials.
        Missing phase measurements are None, not fabricated zero durations.
        """
        import inspect

        if observer is not None and (
            not callable(observer)
            or inspect.iscoroutinefunction(observer)
            or inspect.iscoroutinefunction(getattr(observer, '__call__', None))
        ):
            raise TypeError('query observer must be a synchronous callable or None')
        self._query_observer = observer
        return self

    def _reset_compatibility(self, overrides=None):
        from .capabilities import validate_overrides
        from .version import new_version_manager

        checked = validate_overrides(overrides)
        self._compatibility_generation = getattr(self, '_compatibility_generation', 0) + 1
        self._capability_cache = None
        self._capability_overrides = checked
        self._version_manager = new_version_manager()
        self._backend_version = None

    def _build_insert_sql(self, table_name: str, data: dict) -> str:
        """
        Build INSERT SQL statement from data dictionary.

        Args:

            table_name: Name of the table
            data: Data to insert (dict with column names as keys)

        Returns:

            SQL INSERT statement string
        """
        from ._sql_literals import sql_identifier, sql_literal

        if hasattr(table_name, '__table__'):
            from sqlalchemy import insert, inspect
            from ._query_compilation import compile_expression

            mapper = inspect(table_name).mapper
            values = {mapper.attrs[key].columns[0]: value for key, value in data.items()}
            return compile_expression(insert(mapper.local_table).values(values))

        columns = list(data)
        return "INSERT INTO %s (%s) VALUES (%s)" % (
            sql_identifier(table_name, qualified=True),
            ", ".join(sql_identifier(column) for column in columns),
            ", ".join(sql_literal(data[column]) for column in columns),
        )

    def _build_batch_insert_sql(self, table_name: str, data_list: list) -> str:
        """Build checked literal values for the legacy batch SQL interface."""
        from ._sql_literals import sql_identifier, sql_literal

        if not data_list:
            return ""
        columns = list(data_list[0])
        if hasattr(table_name, '__table__'):
            from sqlalchemy import insert, inspect
            from ._query_compilation import compile_expression

            mapper = inspect(table_name).mapper
            rows = [
                {mapper.attrs[key].columns[0]: value for key, value in zip(columns, values)}
                for values in (insert_row_values(row, columns) for row in data_list)
            ]
            return compile_expression(insert(mapper.local_table).values(rows))
        values = [
            "(" + ", ".join(sql_literal(value) for value in row) + ")"
            for row in (insert_row_values(record, columns) for record in data_list)
        ]
        return "INSERT INTO %s (%s) VALUES %s" % (
            sql_identifier(table_name, qualified=True),
            ", ".join(sql_identifier(column) for column in columns),
            ", ".join(values),
        )


class BaseMatrixOneExecutor:
    """
    Base class for MatrixOne executors with common insert/batch_insert logic.

    This abstract base class provides common execution logic for database
    operations that is shared between different executor implementations.
    It includes methods for data insertion, batch operations, and result
    handling that are common across synchronous and asynchronous executors.

    Key Features:

    - Common data insertion logic
    - Batch operation support
    - Result set handling
    - Error handling and validation
    - Integration with MatrixOne's data operations

    Supported Operations:

    - Single row insertion with parameter binding
    - Batch insertion for multiple rows
    - Result set creation and management
    - Error handling and exception management

    Usage:

        This class is not intended to be used directly. It serves as a base
        class for ClientExecutor and AsyncClientExecutor implementations,
        providing common functionality and reducing code duplication.

    Note: This is an internal base class. Use Client or AsyncClient for
    actual database operations.
    """

    def __init__(self, base_client: BaseMatrixOneClient):
        self.base_client = base_client

    def insert(self, table_name: str, data: dict):
        """
        Insert data into a table.

        Args:

            table_name: Name of the table
            data: Data to insert (dict with column names as keys)

        Returns:

            Result from execute method (implementation specific)
        """
        sql = self.base_client._build_insert_sql(table_name, data)
        return self._execute(sql)

    def batch_insert(self, table_name: str, data_list: list):
        """
        Batch insert data into a table.

        Args:

            table_name: Name of the table
            data_list: List of data dictionaries to insert

        Returns:

            Result from execute method (implementation specific)
        """
        if not data_list:
            # Return empty result based on the executor type
            return self._get_empty_result()

        sql = self.base_client._build_batch_insert_sql(table_name, data_list)
        return self._execute(sql)

    def _execute(self, sql: str):
        """Execute SQL - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement _execute method")

    def _get_empty_result(self):
        """Get empty result - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement _get_empty_result method")
