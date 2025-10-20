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
MatrixOne Async Metadata Operations

This module provides async metadata scanning capabilities for MatrixOne tables,
allowing users to analyze table statistics, column information, and data distribution.
"""

from typing import Any, Dict, List, Optional, Union

from sqlalchemy.engine import Result

from .metadata import BaseMetadataManager, MetadataColumn, MetadataRow


class AsyncMetadataManager(BaseMetadataManager):
    """
    Async metadata manager for MatrixOne table metadata operations.

    Provides async methods to scan table metadata including column statistics,
    row counts, null counts, and data distribution information.
    """

    def __init__(self, client):
        """
        Initialize async metadata manager.

        Args:

            client: MatrixOne AsyncClient instance
        """
        self.client = client

    async def _execute_sql(self, sql: str):
        """
        Execute SQL query asynchronously.

        Args:

            sql: SQL query to execute

        Returns:

            SQLAlchemy Result object
        """
        return await self.client.execute(sql)

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
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name

        Returns:

            SQLAlchemy Result object containing metadata scan results

        Example:

            ```python
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
            ```
        """
        # Build SQL query
        sql = self._build_metadata_scan_sql(dbname, tablename, is_tombstone, indexname, distinct_object_name)

        # Execute the query
        result = await self._execute_sql(sql)

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
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include

        Returns:

            Dictionary with brief statistics for table, tombstone, and indexes
        """

        # Create a wrapper function that awaits the async execution
        async def async_execute_func(sql: str):
            return await self._execute_sql(sql)

        return await self._get_table_brief_stats_logic_async(
            dbname, tablename, is_tombstone, indexname, include_tombstone, include_indexes, async_execute_func
        )

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
            is_tombstone: Optional tombstone flag (True/False)
            indexname: Optional index name
            include_tombstone: Whether to include tombstone statistics
            include_indexes: List of index names to include

        Returns:

            Dictionary with detailed statistics for table, tombstone, and indexes
        """

        # Create a wrapper function that awaits the async execution
        async def async_execute_func(sql: str):
            return await self._execute_sql(sql)

        return await self._get_table_detail_stats_logic_async(
            dbname, tablename, is_tombstone, indexname, include_tombstone, include_indexes, async_execute_func
        )


class AsyncTransactionMetadataManager(BaseMetadataManager):
    """
    Async transaction metadata manager for MatrixOne table metadata operations within transactions.

    Provides async methods to scan table metadata including column statistics,
    row counts, null counts, and data distribution information within a transaction context.
    """

    def __init__(self, client, transaction_wrapper):
        """
        Initialize async transaction metadata manager.

        Args:

            client: MatrixOne AsyncClient instance
            transaction_wrapper: Async transaction wrapper instance
        """
        self.client = client
        self.transaction_wrapper = transaction_wrapper

    async def _execute_sql(self, sql: str):
        """
        Execute SQL query using async transaction wrapper.

        Args:

            sql: SQL query to execute

        Returns:

            SQLAlchemy Result object
        """
        return await self.transaction_wrapper.execute(sql)

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
        Scan table metadata using metadata_scan function within transaction (async).

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
        result = await self._execute_sql(sql)

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
        Get brief statistics for a table, tombstone, and indexes within transaction (async).

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

        # Create a wrapper function that awaits the async execution
        async def async_execute_func(sql: str):
            return await self._execute_sql(sql)

        return await self._get_table_brief_stats_logic_async(
            dbname, tablename, is_tombstone, indexname, include_tombstone, include_indexes, async_execute_func
        )

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
        Get detailed statistics for a table, tombstone, and indexes within transaction (async).

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

        # Create a wrapper function that awaits the async execution
        async def async_execute_func(sql: str):
            return await self._execute_sql(sql)

        return await self._get_table_detail_stats_logic_async(
            dbname, tablename, is_tombstone, indexname, include_tombstone, include_indexes, async_execute_func
        )
