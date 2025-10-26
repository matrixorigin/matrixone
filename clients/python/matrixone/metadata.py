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
    Uses executor pattern to support both Client and Session contexts.
    """

    def __init__(self, client, executor=None):
        """
        Initialize base metadata manager.

        Args:
            client: MatrixOne client instance
            executor: Optional executor (Session). If None, uses client directly
        """
        self.client = client
        self.executor = executor if executor is not None else client

    def _get_executor(self):
        """Get the executor for SQL execution"""
        return self.executor

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
            is_tombstone: Optional tombstone flag (True or False)
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

    def _prepare_brief_stats(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        include_tombstone: bool = False,
        include_indexes: Optional[List[str]] = None,
    ) -> List[tuple]:
        """
        Prepare SQL queries for brief statistics collection.

        Returns:
            List of (query_key, sql) tuples
        """
        queries = []

        # Main table query
        table_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone, indexname)
        queries.append((tablename, table_sql))

        # Tombstone query
        if include_tombstone:
            tombstone_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone=True)
            queries.append(("tombstone", tombstone_sql))

        # Index queries
        if include_indexes:
            for index_name in include_indexes:
                index_sql = self._build_metadata_scan_sql(dbname, tablename, indexname=index_name)
                queries.append((index_name, index_sql))

        return queries

    def _process_brief_stats_result(self, rows) -> Dict[str, Any]:
        """
        Process rows into brief statistics.

        Args:
            rows: Fetched rows from query result

        Returns:
            Dictionary with aggregated statistics
        """
        if not rows:
            return {}

        total_objects = len(set(row._mapping['object_name'] for row in rows))
        total_original_size = sum(row._mapping.get('origin_size', 0) for row in rows)
        total_compress_size = sum(row._mapping.get('compress_size', 0) for row in rows)
        total_row_cnt = sum(row._mapping.get('rows_cnt', 0) for row in rows)
        total_null_cnt = sum(row._mapping.get('null_cnt', 0) for row in rows)

        return {
            "total_objects": total_objects,
            "original_size": self._format_size(total_original_size),
            "compress_size": self._format_size(total_compress_size),
            "row_cnt": total_row_cnt,
            "null_cnt": total_null_cnt,
        }

    def _prepare_detail_stats(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        include_tombstone: bool = False,
        include_indexes: Optional[List[str]] = None,
    ) -> List[tuple]:
        """
        Prepare SQL queries for detailed statistics collection.

        Returns:
            List of (query_key, sql) tuples
        """
        queries = []

        # Main table query
        table_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone, indexname)
        queries.append((tablename, table_sql))

        # Tombstone query
        if include_tombstone:
            tombstone_sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone=True)
            queries.append(("tombstone", tombstone_sql))

        # Index queries
        if include_indexes:
            for index_name in include_indexes:
                index_sql = self._build_metadata_scan_sql(dbname, tablename, indexname=index_name)
                queries.append((index_name, index_sql))

        return queries

    def _process_detail_stats_result(self, rows) -> List[Dict[str, Any]]:
        """
        Process rows into detailed statistics.

        Args:
            rows: Fetched rows from query result

        Returns:
            List of detailed statistics dictionaries
        """
        details = []
        for row in rows:
            details.append(
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
        return details


class MetadataManager(BaseMetadataManager):
    """
    Synchronous metadata manager for MatrixOne table metadata operations.

    This class provides comprehensive metadata scanning capabilities for analyzing
    table statistics, column information, data distribution, and storage details.
    It enables deep introspection of table structure and performance characteristics.

    Key Features:

    - **Metadata scanning**: Access detailed table and column metadata
    - **Column statistics**: Row counts, null counts, min/max values, sums
    - **Storage analysis**: Compression ratios, object sizes, data distribution
    - **Index metadata**: Scan index-specific metadata
    - **Tombstone inspection**: Analyze deleted data objects
    - **Performance insights**: Identify storage hotspots and optimization opportunities
    - **Transaction-aware**: Full integration with transaction contexts

    Executor Pattern:

    - If executor is None, uses self.client.execute (default client-level executor)
    - If executor is provided (e.g., session), uses executor.execute (transaction-aware)
    - All operations can participate in transactions when used via session

    Available Metadata Columns:

    - **COL_NAME**: Column name
    - **OBJECT_NAME**: Object/block name in storage
    - **IS_HIDDEN**: Whether object is hidden
    - **OBJ_LOC**: Object storage location
    - **CREATE_TS**: Creation timestamp
    - **DELETE_TS**: Deletion timestamp (for tombstones)
    - **ROWS_CNT**: Number of rows in the object
    - **NULL_CNT**: Number of null values
    - **COMPRESS_SIZE**: Compressed storage size
    - **ORIGIN_SIZE**: Original uncompressed size
    - **MIN**: Minimum value in column
    - **MAX**: Maximum value in column
    - **SUM**: Sum of values (for numeric columns)

    Usage Examples::

        from matrixone import Client
        from matrixone.metadata import MetadataColumn

        client = Client(host='localhost', port=6001, user='root', password='111', database='test')

        # Basic metadata scan (returns SQLAlchemy Result)
        result = client.metadata.scan('test_db', 'users')
        for row in result:
            print(f"Column: {row.col_name}, Rows: {row.rows_cnt}")

        # Scan specific columns with structured output
        rows = client.metadata.scan(
            'test_db', 'users',
            columns=[MetadataColumn.COL_NAME, MetadataColumn.ROWS_CNT, MetadataColumn.NULL_CNT]
        )
        for row in rows:
            print(f"{row['col_name']}: {row['rows_cnt']} rows, {row['null_cnt']} nulls")

        # Get all structured metadata
        metadata_rows = client.metadata.scan('test_db', 'users', columns='*')
        for row in metadata_rows:
            print(f"{row.col_name}: {row.rows_cnt} rows")

        # Scan index metadata
        index_result = client.metadata.scan(
            'test_db', 'users',
            indexname='idx_email'
        )

        # Scan tombstone (deleted) objects
        tombstone_result = client.metadata.scan(
            'test_db', 'users',
            is_tombstone=True
        )

        # Get table statistics summary
        stats = client.metadata.get_table_stats('test_db', 'users')
        print(f"Total rows: {stats['total_rows']}")
        print(f"Total size: {stats['total_size']}")
        print(f"Compression ratio: {stats['compression_ratio']}")

        # Get detailed statistics with indexes and tombstones
        detailed_stats = client.metadata.get_table_stats(
            'test_db', 'users',
            include_indexes=['idx_email', 'idx_name'],
            include_tombstone=True
        )
        print(f"Table data: {detailed_stats['table']}")
        print(f"Tombstone data: {detailed_stats['tombstone']}")
        for idx in detailed_stats['indexes']:
            print(f"Index {idx['name']}: {idx['size']}")

        # Using within a transaction
        with client.session() as session:
            # Scan metadata within transaction context
            result = session.metadata.scan('test_db', 'users')

    Use Cases:

    - **Performance analysis**: Identify tables with high null counts or poor compression
    - **Storage optimization**: Analyze object distribution and compression ratios
    - **Data quality**: Check for null values and data distribution
    - **Index analysis**: Evaluate index size and effectiveness
    - **Tombstone management**: Monitor deleted data objects
    - **Capacity planning**: Track table growth and storage usage

    See Also:

        - Client.create_table: For creating tables
        - VectorManager: For vector-specific metadata operations
        - LoadDataManager: For bulk data loading
    """

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
            is_tombstone: Optional tombstone flag (True or False)
            indexname: Optional index name
            columns: Optional list of columns to return. Can be MetadataColumn enum values or strings.
                If None, returns all columns as SQLAlchemy Result. If specified, returns List of MetadataRow.
            distinct_object_name: Optional flag to return distinct object names only.

        Returns:
            - If columns is None: SQLAlchemy Result object
            - If columns is specified: List of MetadataRow

        Example::

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
            from matrixone.metadata import MetadataColumn
            rows = client.metadata.scan("test_db", "users",
                                         columns=[MetadataColumn.COL_NAME,
                                                  MetadataColumn.ROWS_CNT])
            for row in rows:
                print(f"Column: {row.col_name}, Rows: {row.rows_cnt}")

            # Get all structured results
            rows = client.metadata.scan("test_db", "users", columns="*")
        """
        # Build SQL query
        sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone, indexname, distinct_object_name)

        # Execute the query using executor
        result = self._get_executor().execute(sql)

        # Process result based on columns parameter
        return self._process_scan_result(result, columns)

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
            is_tombstone: Optional tombstone flag (True or False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include

        Returns:

            Dictionary with brief statistics for table, tombstone, and indexes
        """
        queries = self._prepare_brief_stats(dbname, tablename, is_tombstone, indexname, include_tombstone, include_indexes)

        result = {}
        for key, sql in queries:
            query_result = self._get_executor().execute(sql)
            rows = query_result.fetchall()
            stats = self._process_brief_stats_result(rows)
            if stats:
                result[key] = stats

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
            is_tombstone: Optional tombstone flag (True or False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include

        Returns:

            Dictionary with detailed statistics for table, tombstone, and indexes
        """
        queries = self._prepare_detail_stats(dbname, tablename, is_tombstone, indexname, include_tombstone, include_indexes)

        result = {}
        for key, sql in queries:
            query_result = self._get_executor().execute(sql)
            rows = query_result.fetchall()
            details = self._process_detail_stats_result(rows)
            if details:
                result[key] = details

        return result


class AsyncMetadataManager(BaseMetadataManager):
    """
    Asynchronous metadata manager for MatrixOne table metadata operations.

    Provides the same comprehensive metadata scanning functionality as MetadataManager
    but with full async/await support for non-blocking I/O operations. Ideal for
    high-concurrency applications requiring metadata analysis.

    Key Features:

    - **Non-blocking operations**: All metadata operations use async/await
    - **Async metadata scanning**: Asynchronously access table and column metadata
    - **Concurrent analysis**: Scan multiple tables concurrently
    - **Async statistics**: Get table statistics without blocking
    - **Transaction-aware**: Full integration with async transaction contexts
    - **Executor pattern**: Works with both async client and async session

    Executor Pattern:

    - If executor is None, uses self.client.execute (default async client-level executor)
    - If executor is provided (e.g., async session), uses executor.execute (async transaction-aware)
    - All operations are non-blocking and use async/await
    - Enables concurrent metadata operations

    Usage Examples::

        from matrixone import AsyncClient
        from matrixone.metadata import MetadataColumn
        import asyncio

        async def main():
            client = AsyncClient()
            await client.connect(host='localhost', port=6001, user='root', password='111', database='test')

            # Basic async metadata scan
            result = await client.metadata.scan('test_db', 'users')
            async for row in result:
                print(f"Column: {row.col_name}, Rows: {row.rows_cnt}")

            # Scan specific columns with structured output
            rows = await client.metadata.scan(
                'test_db', 'users',
                columns=[MetadataColumn.COL_NAME, MetadataColumn.ROWS_CNT]
            )
            for row in rows:
                print(f"{row['col_name']}: {row['rows_cnt']} rows")

            # Get async table statistics
            stats = await client.metadata.get_table_stats('test_db', 'users')
            print(f"Total rows: {stats['total_rows']}")
            print(f"Compression ratio: {stats['compression_ratio']}")

            # Concurrent metadata scanning of multiple tables
            results = await asyncio.gather(
                client.metadata.scan('test_db', 'users'),
                client.metadata.scan('test_db', 'orders'),
                client.metadata.scan('test_db', 'products')
            )

            # Concurrent statistics for multiple tables
            stats_list = await asyncio.gather(
                client.metadata.get_table_stats('test_db', 'users'),
                client.metadata.get_table_stats('test_db', 'orders'),
                client.metadata.get_table_stats('test_db', 'products')
            )
            for table_stats in stats_list:
                print(f"Table: {table_stats['total_rows']} rows")

            # Scan index metadata asynchronously
            index_result = await client.metadata.scan(
                'test_db', 'users',
                indexname='idx_email'
            )

            # Using within async transaction
            async with client.session() as session:
                result = await session.metadata.scan('test_db', 'users')
                stats = await session.metadata.get_table_stats('test_db', 'orders')

            await client.disconnect()

        asyncio.run(main())

    Use Cases:

    - **Async performance monitoring**: Non-blocking metadata collection
    - **Concurrent analysis**: Analyze multiple tables simultaneously
    - **Real-time dashboards**: Update table statistics without blocking
    - **High-throughput applications**: Metadata operations in async web servers
    - **Batch metadata collection**: Gather stats from many tables efficiently

    See Also:

        - AsyncClient: For async database operations
        - AsyncSession: For async transaction management
        - MetadataManager: For synchronous metadata operations
    """

    async def scan(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        columns: Optional[List[Union[MetadataColumn, str]]] = None,
        distinct_object_name: Optional[bool] = None,
    ) -> Union[Result, List[MetadataRow]]:
        """
        Scan table metadata using metadata_scan function (async).

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True or False)
            indexname: Optional index name
            columns: Optional list of columns to return

        Returns:
            SQLAlchemy Result object

        Example::

            # Scan all columns of a table
            result = await client.metadata.scan("test_db", "users")

            # Scan specific column
            result = await client.metadata.scan("test_db", "users", indexname="id")

            # Scan with tombstone filter
            result = await client.metadata.scan("test_db", "users", is_tombstone=False)

            # Scan tombstone objects
            result = await client.metadata.scan("test_db", "users", is_tombstone=True)

            # Scan specific index
            result = await client.metadata.scan("test_db", "users", indexname="idx_name")
        """
        # Build SQL query
        sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone, indexname, distinct_object_name)

        # Execute the query using executor
        result = await self._get_executor().execute(sql)

        # Process result based on columns parameter
        return self._process_scan_result(result, columns)

    async def get_table_brief_stats(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        include_tombstone: bool = False,
        include_indexes: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get brief statistics for a table, tombstone, and indexes (async).

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True or False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include

        Returns:

            Dictionary with brief statistics for table, tombstone, and indexes
        """
        queries = self._prepare_brief_stats(dbname, tablename, is_tombstone, indexname, include_tombstone, include_indexes)

        result = {}
        for key, sql in queries:
            query_result = await self._get_executor().execute(sql)
            rows = query_result.fetchall()
            stats = self._process_brief_stats_result(rows)
            if stats:
                result[key] = stats

        return result

    async def get_table_detail_stats(
        self,
        dbname: str,
        tablename: str,
        is_tombstone: Optional[bool] = None,
        indexname: Optional[str] = None,
        include_tombstone: bool = False,
        include_indexes: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get detailed statistics for a table, tombstone, and indexes (async).

        Args:

            dbname: Database name
            tablename: Table name
            is_tombstone: Optional tombstone flag (True or False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include

        Returns:

            Dictionary with detailed statistics for table, tombstone, and indexes
        """
        queries = self._prepare_detail_stats(dbname, tablename, is_tombstone, indexname, include_tombstone, include_indexes)

        result = {}
        for key, sql in queries:
            query_result = await self._get_executor().execute(sql)
            rows = query_result.fetchall()
            details = self._process_detail_stats_result(rows)
            if details:
                result[key] = details

        return result
