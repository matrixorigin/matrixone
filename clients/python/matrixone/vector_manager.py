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
Vector Manager for MatrixOne Vector Operations.

This module provides VectorManager and AsyncVectorManager classes for
synchronous and asynchronous vector operations in MatrixOne.
"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Dict, List, Union

if TYPE_CHECKING:
    from .sqlalchemy_ext import VectorOpType


def _extract_table_name(table_name_or_model: Union[str, type]) -> str:
    """
    Extract table name from string or SQLAlchemy Model class.

    Args:
        table_name_or_model: Table name string or SQLAlchemy Model class

    Returns:
        Table name string
    """
    if isinstance(table_name_or_model, str):
        return table_name_or_model
    # Try to extract __tablename__ from Model class
    if hasattr(table_name_or_model, '__tablename__'):
        return table_name_or_model.__tablename__
    # If neither, assume it's a string-like object
    return str(table_name_or_model)


class VectorManager:
    """
    Unified vector manager for MatrixOne vector operations.

    Supports two usage modes:
    1. Client mode (executor=None): Each operation is an independent auto-commit transaction
    2. Session mode (executor=Session): Operations execute within the session's transaction context

    Key Features:
    - Vector index creation and management (IVF, HNSW)
    - Vector data insertion and batch operations
    - Vector similarity search with multiple distance metrics
    - Vector range search for distance-based filtering

    Supported Index Types:
    - IVF (Inverted File): Good for large datasets, requires training
    - HNSW: Good for high-dimensional vectors, no training required

    Supported Distance Metrics:
    - L2 (Euclidean) distance
    - Cosine similarity
    - Inner product
    """

    def __init__(self, client, executor=None):
        """
        Initialize VectorManager.

        Args:
            client: MatrixOne client instance
            executor: Optional executor (Session). If None, client is used.
        """
        self.client = client
        self.executor = executor if executor is not None else client

    def create_ivf(
        self,
        table_name: Union[str, type],
        name: str,
        column: str,
        lists: int = 100,
        op_type: "VectorOpType" = None,
    ) -> "VectorManager":
        """
        Create an IVFFLAT vector index.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            name: Name of the index
            column: Vector column to index
            lists: Number of lists for IVFFLAT (default: 100)
            op_type: Vector operation type (default: VectorOpType.VECTOR_L2_OPS)

        Returns:
            Self for chaining
        """
        from .sqlalchemy_ext import IVFVectorIndex, VectorOpType

        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        try:
            table_name = _extract_table_name(table_name)
            index = IVFVectorIndex(name, column, lists, op_type)
            sql = index.create_sql(table_name)

            # Execute as auto-commit transaction or within session context
            self.executor.execute("SET experimental_ivf_index = 1")
            self.executor.execute("SET probe_limit = 1")
            self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create IVFFLAT vector index {name} on table {table_name}: {e}")

    def create_hnsw(
        self,
        table_name: Union[str, type],
        name: str,
        column: str,
        m: int = 16,
        ef_construction: int = 200,
        ef_search: int = 50,
        op_type: "VectorOpType" = None,
    ) -> "VectorManager":
        """
        Create an HNSW vector index.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            name: Name of the index
            column: Vector column to index
            m: Number of bi-directional links for HNSW (default: 16)
            ef_construction: Size of dynamic candidate list for construction (default: 200)
            ef_search: Size of dynamic candidate list for search (default: 50)
            op_type: Vector operation type (default: VectorOpType.VECTOR_L2_OPS)

        Returns:
            Self for chaining
        """
        from .sqlalchemy_ext import HnswVectorIndex, VectorOpType

        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        try:
            table_name = _extract_table_name(table_name)
            index = HnswVectorIndex(name, column, m, ef_construction, ef_search, op_type)
            sql = index.create_sql(table_name)

            self.executor.execute("SET experimental_hnsw_index = 1")
            self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create HNSW vector index {name} on table {table_name}: {e}")

    def drop(self, table_name: Union[str, type], name: str) -> "VectorManager":
        """
        Drop a vector index.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            name: Name of the index

        Returns:
            Self for chaining
        """
        try:
            table_name = _extract_table_name(table_name)
            self.executor.execute(f"DROP INDEX IF EXISTS {name} ON {table_name}")
            return self
        except Exception as e:
            raise Exception(f"Failed to drop vector index {name} from table {table_name}: {e}")

    def enable_ivf(self, probe_limit: int = 1) -> "VectorManager":
        """
        Enable IVF indexing with probe limit.

        Args:
            probe_limit: Probe limit for IVF indexing

        Returns:
            Self for chaining
        """
        try:
            self.executor.execute("SET experimental_ivf_index = 1")
            self.executor.execute(f"SET probe_limit = {probe_limit}")
            return self
        except Exception as e:
            raise Exception(f"Failed to enable IVF indexing: {e}")

    def disable_ivf(self) -> "VectorManager":
        """
        Disable IVF indexing.

        Returns:
            Self for chaining
        """
        try:
            self.executor.execute("SET experimental_ivf_index = 0")
            return self
        except Exception as e:
            raise Exception(f"Failed to disable IVF indexing: {e}")

    def enable_hnsw(self) -> "VectorManager":
        """
        Enable HNSW indexing.

        Returns:
            Self for chaining
        """
        try:
            self.executor.execute("SET experimental_hnsw_index = 1")
            return self
        except Exception as e:
            raise Exception(f"Failed to enable HNSW indexing: {e}")

    def disable_hnsw(self) -> "VectorManager":
        """
        Disable HNSW indexing.

        Returns:
            Self for chaining
        """
        try:
            self.executor.execute("SET experimental_hnsw_index = 0")
            return self
        except Exception as e:
            raise Exception(f"Failed to disable HNSW indexing: {e}")

    def insert(self, table_name: Union[str, type], data: Dict[str, Any]) -> "VectorManager":
        """
        Insert a single vector record.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            data: Dictionary of column names and values

        Returns:
            Self for chaining
        """
        try:
            table_name = _extract_table_name(table_name)
            columns = ", ".join(data.keys())
            # Format values directly for MatrixOne
            values = []
            for key in data.keys():
                val = data[key]
                if isinstance(val, (list, tuple)):
                    # Vector type: format as '[v1,v2,v3,...]' with quotes
                    values.append("'[" + ",".join(str(v) for v in val) + "]'")
                elif isinstance(val, str):
                    values.append(f"'{val}'")
                else:
                    values.append(str(val))
            values_str = ", ".join(values)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values_str})"

            self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to insert vector data into table {table_name}: {e}")

    def batch_insert(self, table_name: Union[str, type], data: List[Dict[str, Any]]) -> "VectorManager":
        """
        Insert multiple vector records in batch.

        Args:
            table_name: Name of the table
            data: List of dictionaries containing column names and values

        Returns:
            Self for chaining
        """
        if not data:
            return self

        try:
            for record in data:
                self.insert(table_name, record)
            return self
        except Exception as e:
            raise Exception(f"Failed to batch insert vector data into table {table_name}: {e}")

    def similarity_search(
        self,
        table_name: Union[str, type],
        vector_column: str,
        query_vector: List[float],
        limit: int = 10,
        select_columns: List[str] = None,
        where_clause: str = None,
        distance_type: str = "l2",
    ) -> List[Dict[str, Any]]:
        """
        Perform vector similarity search.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            vector_column: Name of the vector column
            query_vector: Query vector
            limit: Maximum number of results (default: 10)
            select_columns: Columns to return (default: all)
            where_clause: Optional WHERE clause filter
            distance_type: Distance metric ("l2", "cosine", "inner_product") (default: "l2")

        Returns:
            List of matching records with similarity scores
        """
        try:
            table_name = _extract_table_name(table_name)
            columns = ", ".join(select_columns) if select_columns else "*"
            vector_str = "[" + ",".join(str(v) for v in query_vector) + "]"

            # Select distance function based on distance_type
            if distance_type == "l2":
                distance_func = "l2_distance"
            elif distance_type == "cosine":
                distance_func = "cosine_distance"
            elif distance_type == "inner_product":
                distance_func = "inner_product"
            else:
                distance_func = "l2_distance"  # Default to l2

            sql = f"SELECT {columns}, " f"{distance_func}({vector_column}, '{vector_str}') as distance " f"FROM {table_name}"

            if where_clause:
                sql += f" WHERE {where_clause}"

            sql += f" ORDER BY distance LIMIT {limit}"

            result = self.executor.execute(sql)
            return [dict(row._mapping) for row in result]
        except Exception as e:
            raise Exception(f"Failed to perform similarity search on table {table_name}: {e}")

    def range_search(
        self,
        table_name: Union[str, type],
        vector_column: str,
        query_vector: List[float],
        max_distance: float,
        select_columns: List[str] = None,
        where_clause: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Perform vector range search within a maximum distance.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            vector_column: Name of the vector column
            query_vector: Query vector
            max_distance: Maximum distance threshold
            select_columns: Columns to return (default: all)
            where_clause: Optional WHERE clause filter

        Returns:
            List of matching records within distance threshold
        """
        try:
            table_name = _extract_table_name(table_name)
            columns = ", ".join(select_columns) if select_columns else "*"
            vector_str = "[" + ",".join(str(v) for v in query_vector) + "]"

            sql = (
                f"SELECT {columns}, "
                f"l2_distance({vector_column}, '{vector_str}') as distance "
                f"FROM {table_name} "
                f"WHERE l2_distance({vector_column}, '{vector_str}') <= {max_distance}"
            )

            if where_clause:
                sql += f" AND ({where_clause})"

            result = self.executor.execute(sql)
            return [dict(row._mapping) for row in result]
        except Exception as e:
            raise Exception(f"Failed to perform range search on table {table_name}: {e}")

    def get_ivf_stats(
        self,
        table_name: Union[str, type],
        column_name: str = None,
        database: str = None,
    ) -> Dict[str, Any]:
        """
        Get IVF index statistics.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            column_name: Optional vector column name (auto-inferred if not provided)
            database: Optional database name (uses current if not provided)

        Returns:
            Dictionary containing index statistics and distribution
        """
        table_name = _extract_table_name(table_name)

        if database is None:
            # Get database from client connection string or default database
            database = getattr(self.client, 'database', 'test')

        # Auto-infer column name if not provided
        if not column_name:
            schema_sql = (
                f"SELECT column_name, data_type "
                f"FROM information_schema.columns "
                f"WHERE table_schema = '{database}' "
                f"AND table_name = '{table_name}' "
                f"AND (data_type LIKE '%VEC%' OR data_type LIKE '%vec%')"
            )
            result = self.executor.execute(schema_sql)
            vector_columns = result.fetchall()

            if not vector_columns:
                raise Exception(f"No vector columns found in table {table_name}")
            elif len(vector_columns) == 1:
                column_name = vector_columns[0][0]
            else:
                column_names = [col[0] for col in vector_columns]
                raise Exception(
                    f"Multiple vector columns found in table {table_name}: {column_names}. "
                    f"Please specify the column_name parameter."
                )

        # Get IVF index table names
        index_sql = (
            f"SELECT i.algo_table_type, i.index_table_name "
            f"FROM `mo_catalog`.`mo_indexes` AS i "
            f"JOIN `mo_catalog`.`mo_tables` AS t ON i.table_id = t.rel_id "
            f"AND i.column_name = '{column_name}' "
            f"AND t.relname = '{table_name}' "
            f"AND t.reldatabase = '{database}' "
            f"AND i.algo='ivfflat'"
        )
        result = self.executor.execute(index_sql)
        index_tables = {row[0]: row[1] for row in result}

        if not index_tables:
            raise Exception(f"No IVF index found for table {table_name}, column {column_name}")

        # Get the entries table name for distribution analysis
        entries_table = index_tables.get('entries')
        if not entries_table:
            raise Exception("No entries table found in IVF index")

        # Get bucket distribution
        dist_sql = (
            f"SELECT "
            f"  COUNT(*) AS centroid_count, "
            f"  __mo_index_centroid_fk_id AS centroid_id, "
            f"  __mo_index_centroid_fk_version AS centroid_version "
            f"FROM `{database}`.`{entries_table}` "
            f"GROUP BY `__mo_index_centroid_fk_id`, `__mo_index_centroid_fk_version`"
        )
        result = self.executor.execute(dist_sql)
        rows = result.fetchall()
        distribution = {
            "centroid_count": [row[0] for row in rows],
            "centroid_id": [row[1] for row in rows],
            "centroid_version": [row[2] for row in rows],
        }

        return {
            'index_tables': index_tables,
            'distribution': distribution,
            'database': database,
            'table_name': table_name,
            'column_name': column_name,
        }


class AsyncVectorManager:
    """
    Unified async vector manager for MatrixOne vector operations.

    Supports two usage modes:
    1. AsyncClient mode (executor=None): Each operation is an independent auto-commit transaction
    2. AsyncSession mode (executor=AsyncSession): Operations execute within the session's transaction context

    Key Features:
    - Async vector index creation and management (IVF, HNSW)
    - Async vector data insertion and batch operations
    - Async vector similarity search with multiple distance metrics
    - Async vector range search for distance-based filtering

    Supported Index Types:
    - IVF (Inverted File): Good for large datasets, requires training
    - HNSW: Good for high-dimensional vectors, no training required

    Supported Distance Metrics:
    - L2 (Euclidean) distance
    - Cosine similarity
    - Inner product
    """

    def __init__(self, client, executor=None):
        """
        Initialize AsyncVectorManager.

        Args:
            client: MatrixOne async client instance
            executor: Optional executor (AsyncSession). If None, client is used.
        """
        self.client = client
        self.executor = executor if executor is not None else client

    async def create_ivf(
        self,
        table_name: Union[str, type],
        name: str,
        column: str,
        lists: int = 100,
        op_type: "VectorOpType" = None,
    ) -> "AsyncVectorManager":
        """
        Create an IVFFLAT vector index.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            name: Name of the index
            column: Vector column to index
            lists: Number of lists for IVFFLAT (default: 100)
            op_type: Vector operation type (default: VectorOpType.VECTOR_L2_OPS)

        Returns:
            Self for chaining
        """
        from .sqlalchemy_ext import IVFVectorIndex, VectorOpType

        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        try:
            table_name = _extract_table_name(table_name)
            index = IVFVectorIndex(name, column, lists, op_type)
            sql = index.create_sql(table_name)

            # Execute as auto-commit transaction or within session context
            await self.executor.execute("SET experimental_ivf_index = 1")
            await self.executor.execute("SET probe_limit = 1")
            await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create IVFFLAT vector index {name} on table {table_name}: {e}")

    async def create_hnsw(
        self,
        table_name: Union[str, type],
        name: str,
        column: str,
        m: int = 16,
        ef_construction: int = 200,
        ef_search: int = 50,
        op_type: "VectorOpType" = None,
    ) -> "AsyncVectorManager":
        """
        Create an HNSW vector index.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            name: Name of the index
            column: Vector column to index
            m: Number of bi-directional links for HNSW (default: 16)
            ef_construction: Size of dynamic candidate list for construction (default: 200)
            ef_search: Size of dynamic candidate list for search (default: 50)
            op_type: Vector operation type (default: VectorOpType.VECTOR_L2_OPS)

        Returns:
            Self for chaining
        """
        from .sqlalchemy_ext import HnswVectorIndex, VectorOpType

        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        try:
            table_name = _extract_table_name(table_name)
            index = HnswVectorIndex(name, column, m, ef_construction, ef_search, op_type)
            sql = index.create_sql(table_name)

            await self.executor.execute("SET experimental_hnsw_index = 1")
            await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create HNSW vector index {name} on table {table_name}: {e}")

    async def drop(self, table_name: Union[str, type], name: str) -> "AsyncVectorManager":
        """
        Drop a vector index.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            name: Name of the index

        Returns:
            Self for chaining
        """
        try:
            table_name = _extract_table_name(table_name)
            await self.executor.execute(f"DROP INDEX IF EXISTS {name} ON {table_name}")
            return self
        except Exception as e:
            raise Exception(f"Failed to drop vector index {name} from table {table_name}: {e}")

    async def enable_ivf(self, probe_limit: int = 1) -> "AsyncVectorManager":
        """
        Enable IVF indexing with probe limit.

        Args:
            probe_limit: Probe limit for IVF indexing

        Returns:
            Self for chaining
        """
        try:
            await self.executor.execute("SET experimental_ivf_index = 1")
            await self.executor.execute(f"SET probe_limit = {probe_limit}")
            return self
        except Exception as e:
            raise Exception(f"Failed to enable IVF indexing: {e}")

    async def disable_ivf(self) -> "AsyncVectorManager":
        """
        Disable IVF indexing.

        Returns:
            Self for chaining
        """
        try:
            await self.executor.execute("SET experimental_ivf_index = 0")
            return self
        except Exception as e:
            raise Exception(f"Failed to disable IVF indexing: {e}")

    async def enable_hnsw(self) -> "AsyncVectorManager":
        """
        Enable HNSW indexing.

        Returns:
            Self for chaining
        """
        try:
            await self.executor.execute("SET experimental_hnsw_index = 1")
            return self
        except Exception as e:
            raise Exception(f"Failed to enable HNSW indexing: {e}")

    async def disable_hnsw(self) -> "AsyncVectorManager":
        """
        Disable HNSW indexing.

        Returns:
            Self for chaining
        """
        try:
            await self.executor.execute("SET experimental_hnsw_index = 0")
            return self
        except Exception as e:
            raise Exception(f"Failed to disable HNSW indexing: {e}")

    async def insert(self, table_name: Union[str, type], data: Dict[str, Any]) -> "AsyncVectorManager":
        """
        Insert a single vector record.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            data: Dictionary of column names and values

        Returns:
            Self for chaining
        """
        try:
            table_name = _extract_table_name(table_name)
            columns = ", ".join(data.keys())
            # Format values directly for MatrixOne
            values = []
            for key in data.keys():
                val = data[key]
                if isinstance(val, (list, tuple)):
                    # Vector type: format as '[v1,v2,v3,...]' with quotes
                    values.append("'[" + ",".join(str(v) for v in val) + "]'")
                elif isinstance(val, str):
                    values.append(f"'{val}'")
                else:
                    values.append(str(val))
            values_str = ", ".join(values)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values_str})"

            await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to insert vector data into table {table_name}: {e}")

    async def batch_insert(self, table_name: Union[str, type], data: List[Dict[str, Any]]) -> "AsyncVectorManager":
        """
        Insert multiple vector records in batch.

        Args:
            table_name: Name of the table
            data: List of dictionaries containing column names and values

        Returns:
            Self for chaining
        """
        if not data:
            return self

        try:
            for record in data:
                await self.insert(table_name, record)
            return self
        except Exception as e:
            raise Exception(f"Failed to batch insert vector data into table {table_name}: {e}")

    async def similarity_search(
        self,
        table_name: Union[str, type],
        vector_column: str,
        query_vector: List[float],
        limit: int = 10,
        select_columns: List[str] = None,
        where_clause: str = None,
        distance_type: str = "l2",
    ) -> List[Dict[str, Any]]:
        """
        Perform vector similarity search.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            vector_column: Name of the vector column
            query_vector: Query vector
            limit: Maximum number of results (default: 10)
            select_columns: Columns to return (default: all)
            where_clause: Optional WHERE clause filter
            distance_type: Distance metric ("l2", "cosine", "inner_product") (default: "l2")

        Returns:
            List of matching records with similarity scores
        """
        try:
            table_name = _extract_table_name(table_name)
            columns = ", ".join(select_columns) if select_columns else "*"
            vector_str = "[" + ",".join(str(v) for v in query_vector) + "]"

            # Select distance function based on distance_type
            if distance_type == "l2":
                distance_func = "l2_distance"
            elif distance_type == "cosine":
                distance_func = "cosine_distance"
            elif distance_type == "inner_product":
                distance_func = "inner_product"
            else:
                distance_func = "l2_distance"  # Default to l2

            sql = f"SELECT {columns}, " f"{distance_func}({vector_column}, '{vector_str}') as distance " f"FROM {table_name}"

            if where_clause:
                sql += f" WHERE {where_clause}"

            sql += f" ORDER BY distance LIMIT {limit}"

            result = await self.executor.execute(sql)
            return [dict(row._mapping) for row in result]
        except Exception as e:
            raise Exception(f"Failed to perform similarity search on table {table_name}: {e}")

    async def range_search(
        self,
        table_name: Union[str, type],
        vector_column: str,
        query_vector: List[float],
        max_distance: float,
        select_columns: List[str] = None,
        where_clause: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Perform vector range search within a maximum distance.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            vector_column: Name of the vector column
            query_vector: Query vector
            max_distance: Maximum distance threshold
            select_columns: Columns to return (default: all)
            where_clause: Optional WHERE clause filter

        Returns:
            List of matching records within distance threshold
        """
        try:
            table_name = _extract_table_name(table_name)
            columns = ", ".join(select_columns) if select_columns else "*"
            vector_str = "[" + ",".join(str(v) for v in query_vector) + "]"

            sql = (
                f"SELECT {columns}, "
                f"l2_distance({vector_column}, '{vector_str}') as distance "
                f"FROM {table_name} "
                f"WHERE l2_distance({vector_column}, '{vector_str}') <= {max_distance}"
            )

            if where_clause:
                sql += f" AND ({where_clause})"

            result = await self.executor.execute(sql)
            return [dict(row._mapping) for row in result]
        except Exception as e:
            raise Exception(f"Failed to perform range search on table {table_name}: {e}")

    async def get_ivf_stats(
        self,
        table_name: Union[str, type],
        column_name: str = None,
        database: str = None,
    ) -> Dict[str, Any]:
        """
        Get IVF index statistics.

        Args:
            table_name: Name of the table (string) or SQLAlchemy Model class
            column_name: Optional vector column name (auto-inferred if not provided)
            database: Optional database name (uses current if not provided)

        Returns:
            Dictionary containing index statistics and distribution
        """
        table_name = _extract_table_name(table_name)

        if database is None:
            # Get database from client connection string or default database
            database = getattr(self.client, 'database', 'test')

        # Auto-infer column name if not provided
        if not column_name:
            schema_sql = (
                f"SELECT column_name, data_type "
                f"FROM information_schema.columns "
                f"WHERE table_schema = '{database}' "
                f"AND table_name = '{table_name}' "
                f"AND (data_type LIKE '%VEC%' OR data_type LIKE '%vec%')"
            )
            result = await self.executor.execute(schema_sql)
            vector_columns = result.fetchall()

            if not vector_columns:
                raise Exception(f"No vector columns found in table {table_name}")
            elif len(vector_columns) == 1:
                column_name = vector_columns[0][0]
            else:
                column_names = [col[0] for col in vector_columns]
                raise Exception(
                    f"Multiple vector columns found in table {table_name}: {column_names}. "
                    f"Please specify the column_name parameter."
                )

        # Get IVF index table names
        index_sql = (
            f"SELECT i.algo_table_type, i.index_table_name "
            f"FROM `mo_catalog`.`mo_indexes` AS i "
            f"JOIN `mo_catalog`.`mo_tables` AS t ON i.table_id = t.rel_id "
            f"AND i.column_name = '{column_name}' "
            f"AND t.relname = '{table_name}' "
            f"AND t.reldatabase = '{database}' "
            f"AND i.algo='ivfflat'"
        )
        result = await self.executor.execute(index_sql)
        index_tables = {row[0]: row[1] for row in result}

        if not index_tables:
            raise Exception(f"No IVF index found for table {table_name}, column {column_name}")

        # Get the entries table name for distribution analysis
        entries_table = index_tables.get('entries')
        if not entries_table:
            raise Exception("No entries table found in IVF index")

        # Get bucket distribution
        dist_sql = (
            f"SELECT "
            f"  COUNT(*) AS centroid_count, "
            f"  __mo_index_centroid_fk_id AS centroid_id, "
            f"  __mo_index_centroid_fk_version AS centroid_version "
            f"FROM `{database}`.`{entries_table}` "
            f"GROUP BY `__mo_index_centroid_fk_id`, `__mo_index_centroid_fk_version`"
        )
        result = await self.executor.execute(dist_sql)
        rows = result.fetchall()
        distribution = {
            "centroid_count": [row[0] for row in rows],
            "centroid_id": [row[1] for row in rows],
            "centroid_version": [row[2] for row in rows],
        }

        return {
            'index_tables': index_tables,
            'distribution': distribution,
            'database': database,
            'table_name': table_name,
            'column_name': column_name,
        }
