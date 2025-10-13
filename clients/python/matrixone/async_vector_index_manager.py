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
Async vector index manager for MatrixOne async client.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

from sqlalchemy import text

if TYPE_CHECKING:
    from .sqlalchemy_ext import VectorOpType


async def _exec_sql_safe_async(connection, sql: str):
    """Execute SQL safely for async connections, bypassing bind parameter parsing when possible."""
    if hasattr(connection, 'exec_driver_sql'):
        # Escape % to %% for pymysql's format string handling
        escaped_sql = sql.replace('%', '%%')
        return await connection.exec_driver_sql(escaped_sql)
    else:
        return await connection.execute(text(sql))


class AsyncVectorManager:
    """
    Unified async vector manager for MatrixOne vector operations and chain operations.

    This class provides comprehensive asynchronous vector functionality including vector table
    creation, vector indexing, vector data operations, and vector similarity search.
    It supports both IVF (Inverted File) and HNSW (Hierarchical Navigable Small World)
    indexing algorithms for efficient vector similarity search.

    Key Features:

    - Async vector table creation with configurable dimensions and precision
    - Async vector index creation and management (IVF, HNSW)
    - Async vector data insertion and batch operations
    - Async vector similarity search with multiple distance metrics
    - Async vector range search for distance-based filtering
    - Integration with MatrixOne's vector capabilities
    - Support for both f32 and f64 vector precision

    Supported Index Types:
    - IVF (Inverted File): Good for large datasets, requires training
    - HNSW: Good for high-dimensional vectors, no training required

    Supported Distance Metrics:
    - L2 (Euclidean) distance: Standard Euclidean distance
    - Cosine similarity: Cosine of the angle between vectors
    - Inner product: Dot product of vectors

    Usage Examples:

        # Initialize async vector manager
        vector_ops = client.vector_ops

        # Create vector table
        await vector_ops.create_table("documents", {
            "id": "int primary key",
            "content": "text",
            "embedding": "vecf32(384)"
        })

        # Create vector index
        await vector_ops.create_ivf("documents", "idx_embedding", "embedding", lists=100)

        # Vector similarity search
        results = await vector_ops.similarity_search(
            table_name="documents",
            vector_column="embedding",
            query_vector=[0.1, 0.2, 0.3, ...],  # 384-dimensional vector
            limit=10,
            distance_type="l2"
        )
    """

    def __init__(self, client):
        self.client = client

    async def create_ivf(
        self,
        table_name: str,
        name: str,
        column: str,
        lists: int = 100,
        op_type: VectorOpType = None,
    ) -> "AsyncVectorManager":
        """
        Create an IVFFLAT vector index using chain operations.

        Args:

            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            lists: Number of lists for IVFFLAT (default: 100)
            op_type: Vector operation type (VectorOpType enum, default: VectorOpType.VECTOR_L2_OPS)

        Returns:

            AsyncVectorManager: Self for chaining
        """
        from .sqlalchemy_ext import IVFVectorIndex, VectorOpType

        # Use default if not provided
        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        try:
            index = IVFVectorIndex(name, column, lists, op_type)
            sql = index.create_sql(table_name)

            async with self.client._engine.begin() as conn:
                # Enable IVF indexing in the same connection
                await _exec_sql_safe_async(conn, "SET experimental_ivf_index = 1")
                await _exec_sql_safe_async(conn, "SET probe_limit = 1")
                await _exec_sql_safe_async(conn, sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create IVFFLAT vector index {name} on table {table_name}: {e}")

    async def create_hnsw(
        self,
        table_name: str,
        name: str,
        column: str,
        m: int = 16,
        ef_construction: int = 200,
        ef_search: int = 50,
        op_type: VectorOpType = None,
    ) -> "AsyncVectorManager":
        """
        Create an HNSW vector index using chain operations.

        Args:

            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            m: Number of bi-directional links for HNSW (default: 16)
            ef_construction: Size of dynamic candidate list for HNSW construction (default: 200)
            ef_search: Size of dynamic candidate list for HNSW search (default: 50)
            op_type: Vector operation type (VectorOpType enum, default: VectorOpType.VECTOR_L2_OPS)

        Returns:

            AsyncVectorManager: Self for chaining
        """
        from .sqlalchemy_ext import HnswVectorIndex, VectorOpType

        # Use default if not provided
        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        try:
            index = HnswVectorIndex(name, column, m, ef_construction, ef_search, op_type)
            sql = index.create_sql(table_name)

            async with self.client._engine.begin() as conn:
                # Enable HNSW indexing in the same connection
                await _exec_sql_safe_async(conn, "SET experimental_hnsw_index = 1")
                await _exec_sql_safe_async(conn, sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create HNSW vector index {name} on table {table_name}: {e}")

    async def drop(self, table_name: str, name: str) -> "AsyncVectorManager":
        """
        Drop a vector index using chain operations.

        Args:

            table_name: Name of the table
            name: Name of the index

        Returns:

            AsyncVectorManager: Self for chaining
        """
        try:
            async with self.client._engine.begin() as conn:
                await _exec_sql_safe_async(conn, f"DROP INDEX IF EXISTS {name} ON {table_name}")
            return self
        except Exception as e:
            raise Exception(f"Failed to drop vector index {name} from table {table_name}: {e}")

    async def enable_ivf(self, probe_limit: int = 1) -> "AsyncVectorManager":
        """
        Enable IVF indexing with probe limit.

        Args:

            probe_limit: Probe limit for IVF indexing

        Returns:

            AsyncVectorManager: Self for chaining
        """
        try:
            await self.client.execute("SET experimental_ivf_index = 1")
            await self.client.execute(f"SET probe_limit = {probe_limit}")
            return self
        except Exception as e:
            raise Exception(f"Failed to enable IVF indexing: {e}")

    async def disable_ivf(self) -> "AsyncVectorManager":
        """
        Disable IVF indexing.

        Returns:

            AsyncVectorManager: Self for chaining
        """
        try:
            await self.client.execute("SET experimental_ivf_index = 0")
            return self
        except Exception as e:
            raise Exception(f"Failed to disable IVF indexing: {e}")

    async def enable_hnsw(self) -> "AsyncVectorManager":
        """
        Enable HNSW indexing.

        Returns:

            AsyncVectorManager: Self for chaining
        """
        try:
            await self.client.execute("SET experimental_hnsw_index = 1")
            return self
        except Exception as e:
            raise Exception(f"Failed to enable HNSW indexing: {e}")

    async def disable_hnsw(self) -> "AsyncVectorManager":
        """
        Disable HNSW indexing.

        Returns:

            AsyncVectorManager: Self for chaining
        """
        try:
            await self.client.execute("SET experimental_hnsw_index = 0")
            return self
        except Exception as e:
            raise Exception(f"Failed to disable HNSW indexing: {e}")

    # Data operations
    async def insert(self, table_name_or_model, data: dict) -> "AsyncVectorManager":
        """
        Insert vector data using chain operations asynchronously.

        Args:

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data: Data to insert (dict with column names as keys)

        Returns:

            AsyncVectorManager: Self for chaining
        """
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        await self.client.insert(table_name, data)
        return self

    async def insert_in_transaction(self, table_name_or_model, data: dict, connection) -> "AsyncVectorManager":
        """
        Insert vector data within an existing SQLAlchemy transaction asynchronously.

        Args:

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data: Data to insert (dict with column names as keys)
            connection: SQLAlchemy connection object (required for transaction support)

        Returns:

            AsyncVectorManager: Self for chaining

        Raises:

            ValueError: If connection is not provided
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        # Build INSERT statement
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

        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        await self.client._execute_with_logging(connection, sql, context="Vector insert")

        return self

    async def batch_insert(self, table_name_or_model, data_list: list) -> "AsyncVectorManager":
        """
        Batch insert vector data using chain operations asynchronously.

        Args:

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data_list: List of data dictionaries to insert

        Returns:

            AsyncVectorManager: Self for chaining
        """
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        await self.client.batch_insert(table_name, data_list)
        return self

    async def similarity_search(
        self,
        table_name: str,
        vector_column: str,
        query_vector: list,
        limit: int = 10,
        distance_type: str = "l2",
        select_columns: list = None,
        where_conditions: list = None,
        where_params: list = None,
        connection=None,
    ) -> list:
        """
        Perform similarity search using chain operations.

        Args:

            table_name: Name of the table
            vector_column: Name of the vector column
            query_vector: Query vector as list
            limit: Number of results to return
            distance_type: Type of distance calculation (l2, cosine, inner_product)
            select_columns: List of columns to select (None means all columns)
            where_conditions: List of WHERE conditions
            where_params: List of parameters for WHERE conditions
            connection: Optional existing database connection (for transaction support)

        Returns:

            List of search results
        """
        from .sql_builder import DistanceFunction, build_vector_similarity_query

        # Convert distance type to enum
        if distance_type == "l2":
            distance_func = DistanceFunction.L2  # Use L2 for consistency with ORM l2_distance()
        elif distance_type == "l2_sq":
            distance_func = DistanceFunction.L2_SQ  # Explicitly use squared distance if needed
        elif distance_type == "cosine":
            distance_func = DistanceFunction.COSINE
        elif distance_type == "inner_product":
            distance_func = DistanceFunction.INNER_PRODUCT
        else:
            raise ValueError(f"Unsupported distance type: {distance_type}")

        # Build query using unified SQL builder
        sql = build_vector_similarity_query(
            table_name=table_name,
            vector_column=vector_column,
            query_vector=query_vector,
            distance_func=distance_func,
            limit=limit,
            select_columns=select_columns,
            where_conditions=where_conditions,
            where_params=where_params,
        )

        if connection is not None:
            # Use existing connection (for transaction support)
            result = await self.client._execute_with_logging(connection, sql, context="Async vector similarity search")
            return result.fetchall()
        else:
            # Create new connection
            async with self.client._engine.begin() as conn:
                result = await self.client._execute_with_logging(conn, sql, context="Async vector similarity search")
                return result.fetchall()

    async def range_search(
        self,
        table_name: str,
        vector_column: str,
        query_vector: list,
        max_distance: float,
        distance_type: str = "l2",
        select_columns: list = None,
        connection=None,
    ) -> list:
        """
        Perform range search using chain operations.

        Args:

            table_name: Name of the table
            vector_column: Name of the vector column
            query_vector: Query vector as list
            max_distance: Maximum distance threshold
            distance_type: Type of distance calculation
            select_columns: List of columns to select (None means all columns)
            connection: Optional existing database connection (for transaction support)

        Returns:

            List of search results within range
        """
        # Convert vector to string format
        vector_str = "[" + ",".join(map(str, query_vector)) + "]"

        # Build distance function based on type
        if distance_type == "l2":
            distance_func = "l2_distance"
        elif distance_type == "cosine":
            distance_func = "cosine_distance"
        elif distance_type == "inner_product":
            distance_func = "inner_product"
        else:
            raise ValueError(f"Unsupported distance type: {distance_type}")

        # Build SELECT clause
        if select_columns is None:
            select_clause = "*"
        else:
            # Ensure vector_column is included for distance calculation
            columns_to_select = list(select_columns)
            if vector_column not in columns_to_select:
                columns_to_select.append(vector_column)
            select_clause = ", ".join(columns_to_select)

        # Build query
        sql = f"""
        SELECT {select_clause}, {distance_func}({vector_column}, '{vector_str}') as distance
        FROM {table_name}
        WHERE {distance_func}({vector_column}, '{vector_str}') <= {max_distance}
        ORDER BY distance
        """

        if connection is not None:
            # Use existing connection (for transaction support)
            result = await self.client._execute_with_logging(connection, sql, context="Async vector range search")
            return result.fetchall()
        else:
            # Create new connection
            async with self.client._engine.begin() as conn:
                result = await self.client._execute_with_logging(conn, sql, context="Async vector range search")
                return result.fetchall()

    async def get_ivf_stats(self, table_name_or_model, column_name: str = None) -> Dict[str, Any]:
        """
        Get IVF index statistics for a table.

        Args:

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            column_name: Name of the vector column (optional, will be inferred if not provided)

        Returns:

            Dict containing IVF index statistics including:
            - index_tables: Dictionary mapping table types to table names
            - distribution: Dictionary containing bucket distribution data
            - database: Database name
            - table_name: Table name
            - column_name: Vector column name

        Raises:

            Exception: If IVF index is not found or if there are errors retrieving stats

        Examples:

            # Get stats for a table with vector column
            stats = await client.vector_ops.get_ivf_stats("my_table", "embedding")
            print(f"Index tables: {stats['index_tables']}")
            print(f"Distribution: {stats['distribution']}")

            # Get stats using model class
            stats = await client.vector_ops.get_ivf_stats(MyModel, "vector_col")
        """
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        # Get database name from connection params
        database = self.client._connection_params.get('database')
        if not database:
            raise Exception("No database connection found. Please connect to a database first.")

        # If column_name is not provided, try to infer it
        if not column_name:
            # Query the table schema to find vector columns
            async with self.client._engine.begin() as conn:
                schema_sql = (
                    f"SELECT column_name, data_type "
                    f"FROM information_schema.columns "
                    f"WHERE table_schema = '{database}' "
                    f"AND table_name = '{table_name}' "
                    f"AND (data_type LIKE '%VEC%' OR data_type LIKE '%vec%')"
                )
                result = await self.client._execute_with_logging(conn, schema_sql, context="Auto-detect vector column")
                vector_columns = result.fetchall()

                if not vector_columns:
                    raise Exception(f"No vector columns found in table {table_name}")
                elif len(vector_columns) == 1:
                    column_name = vector_columns[0][0]
                else:
                    # Multiple vector columns found, raise error asking user to specify
                    column_names = [col[0] for col in vector_columns]
                    raise Exception(
                        f"Multiple vector columns found in table {table_name}: {column_names}. "
                        f"Please specify the column_name parameter."
                    )

        # Get IVF index table names
        async with self.client._engine.begin() as conn:
            index_tables = await self._get_ivf_index_table_names(database, table_name, column_name, conn)

            if not index_tables:
                raise Exception(f"No IVF index found for table {table_name}, column {column_name}")

            # Get the entries table name for distribution analysis
            entries_table = index_tables.get('entries')
            if not entries_table:
                raise Exception("No entries table found in IVF index")

            # Get bucket distribution
            distribution = await self._get_ivf_buckets_distribution(database, entries_table, conn)

        return {
            'index_tables': index_tables,
            'distribution': distribution,
            'database': database,
            'table_name': table_name,
            'column_name': column_name,
        }

    async def _get_ivf_index_table_names(
        self,
        database: str,
        table_name: str,
        column_name: str,
        connection,
    ) -> Dict[str, str]:
        """
        Get the table names of the IVF index tables.
        """
        sql = (
            f"SELECT i.algo_table_type, i.index_table_name "
            f"FROM `mo_catalog`.`mo_indexes` AS i "
            f"JOIN `mo_catalog`.`mo_tables` AS t ON i.table_id = t.rel_id "
            f"AND i.column_name = '{column_name}' "
            f"AND t.relname = '{table_name}' "
            f"AND t.reldatabase = '{database}' "
            f"AND i.algo='ivfflat'"
        )
        result = await self.client._execute_with_logging(connection, sql, context="Get IVF index table names")
        return {row[0]: row[1] for row in result}

    async def _get_ivf_buckets_distribution(
        self,
        database: str,
        table_name: str,
        connection,
    ) -> Dict[str, List[int]]:
        """
        Get the buckets distribution of the IVF index tables.
        """
        sql = (
            f"SELECT "
            f"  COUNT(*) AS centroid_count, "
            f"  __mo_index_centroid_fk_id AS centroid_id, "
            f"  __mo_index_centroid_fk_version AS centroid_version "
            f"FROM `{database}`.`{table_name}` "
            f"GROUP BY `__mo_index_centroid_fk_id`, `__mo_index_centroid_fk_version`"
        )
        result = await self.client._execute_with_logging(connection, sql, context="Get IVF buckets distribution")
        rows = result.fetchall()
        return {
            "centroid_count": [row[0] for row in rows],
            "centroid_id": [row[1] for row in rows],
            "centroid_version": [row[2] for row in rows],
        }
