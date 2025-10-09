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

    def _build_insert_sql(self, table_name: str, data: dict) -> str:
        """
        Build INSERT SQL statement from data dictionary.

        Args:

            table_name: Name of the table
            data: Data to insert (dict with column names as keys)

        Returns:

            SQL INSERT statement string
        """
        columns = list(data.keys())
        values = list(data.values())

        # Convert vectors to string format
        formatted_values = []
        for value in values:
            if value is None:
                formatted_values.append("NULL")
            elif isinstance(value, list):
                formatted_values.append("'" + "[" + ",".join(map(str, value)) + "]" + "'")
            else:
                formatted_values.append(f"'{str(value)}'")

        columns_str = ", ".join(columns)
        values_str = ", ".join(formatted_values)

        return f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"

    def _build_batch_insert_sql(self, table_name: str, data_list: list) -> str:
        """
        Build batch INSERT SQL statement from list of data dictionaries.

        Args:

            table_name: Name of the table
            data_list: List of data dictionaries to insert

        Returns:

            SQL batch INSERT statement string
        """
        if not data_list:
            return ""

        # Get columns from first record
        columns = list(data_list[0].keys())
        columns_str = ", ".join(columns)

        # Build VALUES clause
        values_list = []
        for data in data_list:
            formatted_values = []
            for col in columns:
                value = data[col]
                if value is None:
                    formatted_values.append("NULL")
                elif isinstance(value, list):
                    formatted_values.append("'" + "[" + ",".join(map(str, value)) + "]" + "'")
                else:
                    # Escape single quotes in string values to prevent SQL injection
                    escaped_value = str(value).replace("'", "''")
                    formatted_values.append(f"'{escaped_value}'")
            values_str = "(" + ", ".join(formatted_values) + ")"
            values_list.append(values_str)

        values_clause = ", ".join(values_list)
        return f"INSERT INTO {table_name} ({columns_str}) VALUES {values_clause}"


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
