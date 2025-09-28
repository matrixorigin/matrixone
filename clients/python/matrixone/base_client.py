"""
Base client functionality for MatrixOne clients
"""

# No imports needed for this module


class BaseMatrixOneClient:
    """Base class for MatrixOne clients with common SQL building logic"""

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
            if isinstance(value, list):
                formatted_values.append("[" + ",".join(map(str, value)) + "]")
            else:
                formatted_values.append(str(value))

        columns_str = ", ".join(columns)
        values_str = ", ".join([f"'{v}'" for v in formatted_values])

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
                if isinstance(value, list):
                    formatted_values.append("[" + ",".join(map(str, value)) + "]")
                else:
                    formatted_values.append(str(value))
            values_str = "(" + ", ".join([f"'{v}'" for v in formatted_values]) + ")"
            values_list.append(values_str)

        values_clause = ", ".join(values_list)
        return f"INSERT INTO {table_name} ({columns_str}) VALUES {values_clause}"


class BaseMatrixOneExecutor:
    """Base class for MatrixOne executors with common insert/batch_insert logic"""

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
