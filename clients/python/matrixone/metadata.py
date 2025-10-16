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
MatrixOne Metadata Operations

This module provides metadata scanning capabilities for MatrixOne tables,
allowing users to analyze table statistics, column information, and data distribution.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from sqlalchemy.engine import Result


class MetadataColumn(Enum):
    """Enumeration of available metadata columns"""

    COL_NAME = "col_name"
    OBJECT_NAME = "object_name"
    IS_HIDDEN = "is_hidden"
    OBJ_LOC = "obj_loc"
    CREATE_TS = "create_ts"
    DELETE_TS = "delete_ts"
    ROWS_CNT = "rows_cnt"
    NULL_CNT = "null_cnt"
    COMPRESS_SIZE = "compress_size"
    ORIGIN_SIZE = "origin_size"
    MIN = "min"
    MAX = "max"
    SUM = "sum"


@dataclass
class MetadataRow:
    """Structured representation of a metadata scan row"""

    col_name: str
    object_name: str
    is_hidden: bool
    obj_loc: str
    create_ts: str
    delete_ts: str
    rows_cnt: int
    null_cnt: int
    compress_size: int
    origin_size: int
    min: Optional[Any] = None
    max: Optional[Any] = None
    sum: Optional[Any] = None

    @classmethod
    def from_sqlalchemy_row(cls, row) -> "MetadataRow":
        """Create MetadataRow from SQLAlchemy Row object"""
        return cls(
            col_name=row._mapping['col_name'],
            object_name=row._mapping['object_name'],
            is_hidden=row._mapping['is_hidden'] == 'true',
            obj_loc=row._mapping['obj_loc'],
            create_ts=row._mapping['create_ts'],
            delete_ts=row._mapping['delete_ts'],
            rows_cnt=row._mapping['rows_cnt'],
            null_cnt=row._mapping['null_cnt'],
            compress_size=row._mapping['compress_size'],
            origin_size=row._mapping['origin_size'],
            min=row._mapping.get('min'),
            max=row._mapping.get('max'),
            sum=row._mapping.get('sum'),
        )


class BaseMetadataManager:
    """
    Base metadata manager with shared SQL building logic.
    """

    def _build_metadata_scan_sql(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        distinct_object_name: Optional[bool] = None,
    ) -> str:
        """
        Build metadata_scan SQL query.

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name

        Returns:

            SQL query string
        """
        # Build the metadata_scan SQL query based on MatrixOne syntax
        if is_tombstone and indexname:
            # Format: "db_name.table_name.?index_name.#"
            table_ref = f"{dbname}.{tablename}.?{indexname}.#"
        elif is_tombstone:
            # Format: "db_name.table_name.#"
            table_ref = f"{dbname}.{tablename}.#"
        elif indexname:
            # Format: "db_name.table_name.?index_name"
            table_ref = f"{dbname}.{tablename}.?{indexname}"
        else:
            # Format: "db_name.table_name"
            table_ref = f"{dbname}.{tablename}"

        # Column parameter - always use '*' for metadata_scan
        column_param = "'*'"

        # Build the SQL query
        if distinct_object_name:
            return f"SELECT DISTINCT(object_name) as object_name, * FROM metadata_scan('{table_ref}', {column_param}) g"
        else:
            return f"SELECT * FROM metadata_scan('{table_ref}', {column_param}) g"

    def _process_scan_result(
        self, result: Result, columns: Optional[Union[List[Union[MetadataColumn, str]], str]] = None
    ) -> Union[Result, List[MetadataRow], List[Dict[str, Any]]]:
        """
        Process scan result based on columns parameter.

        Args:

            result: SQLAlchemy Result object
            columns: Optional list of columns to return

        Returns:

            Processed result based on columns parameter
        """
        # If columns are specified, return structured results
        if columns is not None:
            if columns == "*" or (isinstance(columns, list) and len(columns) == 0):
                # Return all columns as structured data
                return [MetadataRow.from_sqlalchemy_row(row) for row in result.fetchall()]
            else:
                # Return only specified columns as structured data
                rows = []
                for row in result.fetchall():
                    metadata_row = MetadataRow.from_sqlalchemy_row(row)
                    # Filter to only requested columns
                    filtered_row = {}
                    for col in columns:
                        col_name = col.value if isinstance(col, MetadataColumn) else col
                        if hasattr(metadata_row, col_name):
                            filtered_row[col_name] = getattr(metadata_row, col_name)
                    rows.append(filtered_row)
                return rows

        # Return raw SQLAlchemy Result
        return result

    def _format_size(self, size_bytes: int) -> str:
        """
        Format size in bytes to human-readable format.

        Args:

            size_bytes: Size in bytes

        Returns:

            Formatted size string (e.g., "1.5 MB", "500 KB")
        """
        if size_bytes == 0:
            return "0 B"

        units = ['B', 'KB', 'MB', 'GB', 'TB']
        unit_index = 0
        size = float(size_bytes)

        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1

        # Format to 2 decimal places maximum
        if size == int(size):
            return f"{int(size)} {units[unit_index]}"
        else:
            return f"{size:.2f} {units[unit_index]}".rstrip('0').rstrip('.')

    def _execute_sql(self, sql: str):
        """
        Execute SQL query. Override in subclasses to use different executors.

        Args:

            sql: SQL query to execute

        Returns:

            SQLAlchemy Result object
        """
        return self.client.execute(sql)

    async def _execute_sql_async(self, sql: str):
        """
        Execute SQL query asynchronously. Override in subclasses to use different executors.

        Args:

            sql: SQL query to execute

        Returns:

            SQLAlchemy Result object
        """
        return await self.client.execute(sql)

    def _get_table_brief_stats_logic(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        include_tombstone: bool = False,
        include_indexes: Optional[List[str]] = None,
        execute_func=None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Common logic for get_table_brief_stats. Used by both sync and async versions.

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include
            execute_func: Function to execute SQL (sync or async)

        Returns:

            Dictionary with brief statistics for table, tombstone, and indexes
        """
        result = {}

        # Get table statistics
        table_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone, indexname)
        table_result = execute_func(table_sql)
        table_rows = table_result.fetchall()

        if table_rows:
            # Aggregate table statistics
            total_objects = len(set(row._mapping['object_name'] for row in table_rows))
            total_original_size = sum(row._mapping.get('origin_size', 0) for row in table_rows)
            total_compress_size = sum(row._mapping.get('compress_size', 0) for row in table_rows)
            total_row_cnt = sum(row._mapping.get('rows_cnt', 0) for row in table_rows)
            total_null_cnt = sum(row._mapping.get('null_cnt', 0) for row in table_rows)

            result[tablename] = {
                "total_objects": total_objects,
                "original_size": self._format_size(total_original_size),
                "compress_size": self._format_size(total_compress_size),
                "row_cnt": total_row_cnt,
                "null_cnt": total_null_cnt,
            }

        # Get tombstone statistics if requested
        if include_tombstone:
            tombstone_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone=True)
            tombstone_result = execute_func(tombstone_sql)
            tombstone_rows = tombstone_result.fetchall()

            if tombstone_rows:
                total_objects = len(set(row._mapping['object_name'] for row in tombstone_rows))
                total_original_size = sum(row._mapping.get('origin_size', 0) for row in tombstone_rows)
                total_compress_size = sum(row._mapping.get('compress_size', 0) for row in tombstone_rows)
                total_row_cnt = sum(row._mapping.get('rows_cnt', 0) for row in tombstone_rows)
                total_null_cnt = sum(row._mapping.get('null_cnt', 0) for row in tombstone_rows)

                result["tombstone"] = {
                    "total_objects": total_objects,
                    "original_size": self._format_size(total_original_size),
                    "compress_size": self._format_size(total_compress_size),
                    "row_cnt": total_row_cnt,
                    "null_cnt": total_null_cnt,
                }

        # Get index statistics if requested
        if include_indexes:
            for index_name in include_indexes:
                index_sql = self._build_metadata_scan_sql(dbname, tablename, indexname=index_name)
                index_result = execute_func(index_sql)
                index_rows = index_result.fetchall()

                if index_rows:
                    total_objects = len(set(row._mapping['object_name'] for row in index_rows))
                    total_original_size = sum(row._mapping.get('origin_size', 0) for row in index_rows)
                    total_compress_size = sum(row._mapping.get('compress_size', 0) for row in index_rows)
                    total_row_cnt = sum(row._mapping.get('rows_cnt', 0) for row in index_rows)
                    total_null_cnt = sum(row._mapping.get('null_cnt', 0) for row in index_rows)

                    result[index_name] = {
                        "total_objects": total_objects,
                        "original_size": self._format_size(total_original_size),
                        "compress_size": self._format_size(total_compress_size),
                        "row_cnt": total_row_cnt,
                        "null_cnt": total_null_cnt,
                    }

        return result

    async def _get_table_brief_stats_logic_async(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        include_tombstone: bool = False,
        include_indexes: Optional[List[str]] = None,
        execute_func=None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Common async logic for get_table_brief_stats. Used by async versions.

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include
            execute_func: Async function to execute SQL

        Returns:

            Dictionary with brief statistics for table, tombstone, and indexes
        """
        result = {}

        # Get table statistics
        table_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone, indexname)
        table_result = await execute_func(table_sql)
        table_rows = table_result.fetchall()

        if table_rows:
            # Aggregate table statistics
            total_objects = len(set(row._mapping['object_name'] for row in table_rows))
            total_original_size = sum(row._mapping.get('origin_size', 0) for row in table_rows)
            total_compress_size = sum(row._mapping.get('compress_size', 0) for row in table_rows)
            total_row_cnt = sum(row._mapping.get('rows_cnt', 0) for row in table_rows)
            total_null_cnt = sum(row._mapping.get('null_cnt', 0) for row in table_rows)

            result[tablename] = {
                "total_objects": total_objects,
                "original_size": self._format_size(total_original_size),
                "compress_size": self._format_size(total_compress_size),
                "row_cnt": total_row_cnt,
                "null_cnt": total_null_cnt,
            }

        # Get tombstone statistics if requested
        if include_tombstone:
            tombstone_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone=True)
            tombstone_result = await execute_func(tombstone_sql)
            tombstone_rows = tombstone_result.fetchall()

            if tombstone_rows:
                total_objects = len(set(row._mapping['object_name'] for row in tombstone_rows))
                total_original_size = sum(row._mapping.get('origin_size', 0) for row in tombstone_rows)
                total_compress_size = sum(row._mapping.get('compress_size', 0) for row in tombstone_rows)
                total_row_cnt = sum(row._mapping.get('rows_cnt', 0) for row in tombstone_rows)
                total_null_cnt = sum(row._mapping.get('null_cnt', 0) for row in tombstone_rows)

                result["tombstone"] = {
                    "total_objects": total_objects,
                    "original_size": self._format_size(total_original_size),
                    "compress_size": self._format_size(total_compress_size),
                    "row_cnt": total_row_cnt,
                    "null_cnt": total_null_cnt,
                }

        # Get index statistics if requested
        if include_indexes:
            for index_name in include_indexes:
                index_sql = self._build_metadata_scan_sql(dbname, tablename, indexname=index_name)
                index_result = await execute_func(index_sql)
                index_rows = index_result.fetchall()

                if index_rows:
                    total_objects = len(set(row._mapping['object_name'] for row in index_rows))
                    total_original_size = sum(row._mapping.get('origin_size', 0) for row in index_rows)
                    total_compress_size = sum(row._mapping.get('compress_size', 0) for row in index_rows)
                    total_row_cnt = sum(row._mapping.get('rows_cnt', 0) for row in index_rows)
                    total_null_cnt = sum(row._mapping.get('null_cnt', 0) for row in index_rows)

                    result[index_name] = {
                        "total_objects": total_objects,
                        "original_size": self._format_size(total_original_size),
                        "compress_size": self._format_size(total_compress_size),
                        "row_cnt": total_row_cnt,
                        "null_cnt": total_null_cnt,
                    }

        return result

    async def _get_table_detail_stats_logic_async(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        include_tombstone: bool = False,
        include_indexes: Optional[List[str]] = None,
        execute_func=None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Common async logic for get_table_detail_stats. Used by async versions.

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include
            execute_func: Async function to execute SQL

        Returns:

            Dictionary with detailed statistics for table, tombstone, and indexes
        """
        result = {}

        # Get table statistics
        table_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone, indexname)
        table_result = await execute_func(table_sql)
        table_rows = table_result.fetchall()

        if table_rows:
            # Convert to detailed format
            table_details = []
            for row in table_rows:
                table_details.append(
                    {
                        "object_name": row._mapping['object_name'],
                        "create_ts": row._mapping['create_ts'],
                        "delete_ts": row._mapping['delete_ts'],
                        "row_cnt": row._mapping.get('rows_cnt', 0),
                        "null_cnt": row._mapping.get('null_cnt', 0),
                        "original_size": self._format_size(row._mapping.get('origin_size', 0)),
                        "compress_size": self._format_size(row._mapping.get('compress_size', 0)),
                    }
                )
            result[tablename] = table_details

        # Get tombstone statistics if requested
        if include_tombstone:
            tombstone_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone=True)
            tombstone_result = await execute_func(tombstone_sql)
            tombstone_rows = tombstone_result.fetchall()

            if tombstone_rows:
                tombstone_details = []
                for row in tombstone_rows:
                    tombstone_details.append(
                        {
                            "object_name": row._mapping['object_name'],
                            "create_ts": row._mapping['create_ts'],
                            "delete_ts": row._mapping['delete_ts'],
                            "row_cnt": row._mapping.get('rows_cnt', 0),
                            "null_cnt": row._mapping.get('null_cnt', 0),
                            "original_size": self._format_size(row._mapping.get('origin_size', 0)),
                            "compress_size": self._format_size(row._mapping.get('compress_size', 0)),
                        }
                    )
                result["tombstone"] = tombstone_details

        # Get index statistics if requested
        if include_indexes:
            for index_name in include_indexes:
                index_sql = self._build_metadata_scan_sql(dbname, tablename, indexname=index_name)
                index_result = await execute_func(index_sql)
                index_rows = index_result.fetchall()

                if index_rows:
                    index_details = []
                    for row in index_rows:
                        index_details.append(
                            {
                                "object_name": row._mapping['object_name'],
                                "create_ts": row._mapping['create_ts'],
                                "delete_ts": row._mapping['delete_ts'],
                                "row_cnt": row._mapping.get('rows_cnt', 0),
                                "null_cnt": row._mapping.get('null_cnt', 0),
                                "original_size": self._format_size(row._mapping.get('origin_size', 0)),
                                "compress_size": self._format_size(row._mapping.get('compress_size', 0)),
                            }
                        )
                    result[index_name] = index_details

        return result

    def get_table_brief_stats(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        include_tombstone: bool = False,
        include_indexes: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get brief statistics for a table, tombstone, and indexes.

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include

        Returns:

            Dictionary with brief statistics for table, tombstone, and indexes
        """
        return self._get_table_brief_stats_logic(
            dbname, tablename, is_tombstone, indexname, include_tombstone, include_indexes, self._execute_sql
        )

    def _get_table_detail_stats_logic(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        include_tombstone: bool = False,
        include_indexes: Optional[List[str]] = None,
        execute_func=None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Common logic for get_table_detail_stats. Used by both sync and async versions.

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include
            execute_func: Function to execute SQL (sync or async)

        Returns:

            Dictionary with detailed statistics for table, tombstone, and indexes
        """
        result = {}

        # Get table statistics
        table_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone, indexname)
        table_result = execute_func(table_sql)
        table_rows = table_result.fetchall()

        if table_rows:
            # Convert to detailed format
            table_details = []
            for row in table_rows:
                table_details.append(
                    {
                        "object_name": row._mapping['object_name'],
                        "create_ts": row._mapping['create_ts'],
                        "delete_ts": row._mapping['delete_ts'],
                        "row_cnt": row._mapping.get('rows_cnt', 0),
                        "null_cnt": row._mapping.get('null_cnt', 0),
                        "original_size": self._format_size(row._mapping.get('origin_size', 0)),
                        "compress_size": self._format_size(row._mapping.get('compress_size', 0)),
                    }
                )
            result[tablename] = table_details

        # Get tombstone statistics if requested
        if include_tombstone:
            tombstone_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone=True)
            tombstone_result = execute_func(tombstone_sql)
            tombstone_rows = tombstone_result.fetchall()

            if tombstone_rows:
                tombstone_details = []
                for row in tombstone_rows:
                    tombstone_details.append(
                        {
                            "object_name": row._mapping['object_name'],
                            "create_ts": row._mapping['create_ts'],
                            "delete_ts": row._mapping['delete_ts'],
                            "row_cnt": row._mapping.get('rows_cnt', 0),
                            "null_cnt": row._mapping.get('null_cnt', 0),
                            "original_size": self._format_size(row._mapping.get('origin_size', 0)),
                            "compress_size": self._format_size(row._mapping.get('compress_size', 0)),
                        }
                    )
                result["tombstone"] = tombstone_details

        # Get index statistics if requested
        if include_indexes:
            for index_name in include_indexes:
                index_sql = self._build_metadata_scan_sql(dbname, tablename, indexname=index_name)
                index_result = execute_func(index_sql)
                index_rows = index_result.fetchall()

                if index_rows:
                    index_details = []
                    for row in index_rows:
                        index_details.append(
                            {
                                "object_name": row._mapping['object_name'],
                                "create_ts": row._mapping['create_ts'],
                                "delete_ts": row._mapping['delete_ts'],
                                "row_cnt": row._mapping.get('rows_cnt', 0),
                                "null_cnt": row._mapping.get('null_cnt', 0),
                                "original_size": self._format_size(row._mapping.get('origin_size', 0)),
                                "compress_size": self._format_size(row._mapping.get('compress_size', 0)),
                            }
                        )
                    result[index_name] = index_details

        return result

    def get_table_detail_stats(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        include_tombstone: bool = False,
        include_indexes: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get detailed statistics for a table, tombstone, and indexes.

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include

        Returns:

            Dictionary with detailed statistics for table, tombstone, and indexes
        """
        return self._get_table_detail_stats_logic(
            dbname, tablename, is_tombstone, indexname, include_tombstone, include_indexes, self._execute_sql
        )


class MetadataManager(BaseMetadataManager):
    """
    Metadata manager for MatrixOne table metadata operations.

    Provides methods to scan table metadata including column statistics,
    row counts, null counts, and data distribution information.
    """

    def __init__(self, client):
        """
        Initialize metadata manager.

        Args:

            client: MatrixOne client instance (Client or AsyncClient)
        """
        self.client = client

    def scan(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        columns: Optional[Union[List[Union[MetadataColumn, str]], str]] = None,
        distinct_object_name: Optional[bool] = None,
    ) -> Union[Result, List[MetadataRow]]:
        """
        Scan table metadata using metadata_scan function.

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name
            columns: Optional list of columns to return. Can be MetadataColumn enum values or strings.
                    If None, returns all columns as SQLAlchemy Result. If specified, returns List[MetadataRow].
            distinct_object_name: Optional flag to return distinct object names only.

        Returns:

            If columns is None: SQLAlchemy Result object containing metadata scan results
            If columns is specified: List[MetadataRow] containing structured metadata

        Example:

            ```python
            # Scan all columns of a table (returns SQLAlchemy Result)
            result = client.metadata.scan("test_db", "users")

            # Scan specific column
            result = client.metadata.scan("test_db", "users", indexname="id")

            # Scan with tombstone filter
            result = client.metadata.scan("test_db", "users", is_tombstone=False)

            # Scan tombstone objects
            result = client.metadata.scan("test_db", "users", is_tombstone=True)

            # Scan specific index
            result = client.metadata.scan("test_db", "users", indexname="idx_name")

            # Get structured results with specific columns
            rows = client.metadata.scan("test_db", "users",
                                      columns=[MetadataColumn.COL_NAME, MetadataColumn.ROWS_CNT])
            for row in rows:
                print(f"Column: {row.col_name}, Rows: {row.rows_cnt}")

            # Get all structured results
            rows = client.metadata.scan("test_db", "users", columns="*")
            ```
        """
        # Build SQL query
        sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone, indexname, distinct_object_name)

        # Execute the query
        result = self._execute_sql(sql)

        # Process result based on columns parameter
        return self._process_scan_result(result, columns)


class TransactionMetadataManager(BaseMetadataManager):
    """
    Transaction metadata manager for MatrixOne table metadata operations within transactions.

    Provides methods to scan table metadata including column statistics,
    row counts, null counts, and data distribution information within a transaction context.
    """

    def __init__(self, client, transaction_wrapper):
        """
        Initialize transaction metadata manager.

        Args:

            client: MatrixOne client instance
            transaction_wrapper: Transaction wrapper instance
        """
        self.client = client
        self.transaction_wrapper = transaction_wrapper

    def _execute_sql(self, sql: str):
        """
        Execute SQL query using transaction wrapper.

        Args:

            sql: SQL query to execute

        Returns:

            SQLAlchemy Result object
        """
        return self.transaction_wrapper.execute(sql)

    def scan(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        columns: Optional[Union[List[Union[MetadataColumn, str]], str]] = None,
        distinct_object_name: Optional[bool] = None,
    ) -> Union[Result, List[MetadataRow]]:
        """
        Scan table metadata using metadata_scan function within transaction.

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name

        Returns:

            SQLAlchemy Result object containing metadata scan results
        """
        # Build SQL query
        sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone, indexname, distinct_object_name)

        # Execute the query within transaction
        result = self.transaction_wrapper.execute(sql)

        # Process result based on columns parameter
        return self._process_scan_result(result, columns)
