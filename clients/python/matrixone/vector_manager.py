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
Optimized Vector Manager for MatrixOne Vector Operations.

This module provides VectorManager and AsyncVectorManager classes with
shared SQL generation logic to eliminate code duplication.
"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Dict, List, Union

if TYPE_CHECKING:
    from .sqlalchemy_ext import VectorOpType


def _extract_table_name(table_name_or_model: Union[str, type]) -> str:
    """Extract table name from string or SQLAlchemy Model class."""
    if isinstance(table_name_or_model, str):
        return table_name_or_model
    if hasattr(table_name_or_model, '__tablename__'):
        return table_name_or_model.__tablename__
    return str(table_name_or_model)


class _VectorManagerBase:
    """
    Base class with shared SQL generation logic.

    All SQL building logic is centralized here, eliminating duplication
    between sync and async implementations.
    """

    @staticmethod
    def _build_create_ivf_sql(table_name: str, name: str, column: str, lists: int, op_type: "VectorOpType") -> tuple:
        """Build SQL for creating IVF index."""
        from .sqlalchemy_ext import IVFVectorIndex, VectorOpType

        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        index = IVFVectorIndex(name, column, lists, op_type)
        sql = index.create_sql(table_name)
        setup_sqls = ["SET experimental_ivf_index = 1", "SET probe_limit = 1"]
        return setup_sqls, sql

    @staticmethod
    def _build_create_hnsw_sql(
        table_name: str, name: str, column: str, m: int, ef_construction: int, ef_search: int, op_type: "VectorOpType"
    ) -> str:
        """Build SQL for creating HNSW index."""
        from .sqlalchemy_ext import HnswVectorIndex, VectorOpType

        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        index = HnswVectorIndex(name, column, m=m, ef_construction=ef_construction, ef_search=ef_search, op_type=op_type)
        return index.create_sql(table_name)

    @staticmethod
    def _build_drop_sql(table_name: str, name: str) -> str:
        """Build SQL for dropping index."""
        return f"DROP INDEX {name} ON {table_name}"

    @staticmethod
    def _build_enable_ivf_sql(probe_limit: int) -> list:
        """Build SQL for enabling IVF."""
        return ["SET experimental_ivf_index = 1", f"SET probe_limit = {probe_limit}"]

    @staticmethod
    def _build_disable_ivf_sql() -> str:
        """Build SQL for disabling IVF."""
        return "SET experimental_ivf_index = 0"

    @staticmethod
    def _build_enable_hnsw_sql() -> str:
        """Build SQL for enabling HNSW."""
        return "SET experimental_hnsw_index = 1"

    @staticmethod
    def _build_disable_hnsw_sql() -> str:
        """Build SQL for disabling HNSW."""
        return "SET experimental_hnsw_index = 0"

    @staticmethod
    def _build_insert_sql(table_name: str, data: Dict[str, Any]) -> str:
        """Build SQL for inserting vector data."""
        columns = ", ".join(data.keys())
        values = []
        for v in data.values():
            if isinstance(v, list):
                vector_str = "[" + ",".join(str(x) for x in v) + "]"
                values.append(f"'{vector_str}'")
            elif isinstance(v, str):
                values.append(f"'{v}'")
            else:
                values.append(str(v))
        values_str = ", ".join(values)
        return f"INSERT INTO {table_name} ({columns}) VALUES ({values_str})"

    @staticmethod
    def _build_similarity_search_sql(
        table_name: str,
        vector_column: str,
        query_vector: List[float],
        limit: int,
        select_columns: List[str],
        where_clause: str,
        distance_type: str,
    ) -> str:
        """Build SQL for similarity search."""
        columns = ", ".join(select_columns) if select_columns else "*"
        vector_str = "[" + ",".join(str(v) for v in query_vector) + "]"

        # Select distance function
        if distance_type == "l2":
            distance_func = "l2_distance"
        elif distance_type == "cosine":
            distance_func = "cosine_distance"
        elif distance_type == "inner_product":
            distance_func = "inner_product"
        else:
            distance_func = "l2_distance"

        sql = f"SELECT {columns}, {distance_func}({vector_column}, '{vector_str}') as distance FROM {table_name}"

        if where_clause:
            sql += f" WHERE {where_clause}"

        sql += f" ORDER BY distance LIMIT {limit}"
        return sql

    @staticmethod
    def _build_range_search_sql(
        table_name: str,
        vector_column: str,
        query_vector: List[float],
        max_distance: float,
        select_columns: List[str],
        where_clause: str,
    ) -> str:
        """Build SQL for range search."""
        columns = ", ".join(select_columns) if select_columns else "*"
        vector_str = "[" + ",".join(str(v) for v in query_vector) + "]"

        sql = f"SELECT {columns}, l2_distance({vector_column}, '{vector_str}') as distance FROM {table_name}"

        distance_condition = f"l2_distance({vector_column}, '{vector_str}') <= {max_distance}"

        if where_clause:
            sql += f" WHERE {where_clause} AND {distance_condition}"
        else:
            sql += f" WHERE {distance_condition}"

        sql += " ORDER BY distance"
        return sql

    @staticmethod
    def _build_column_inference_sql(table_name: str, database: str) -> str:
        """Build SQL for inferring vector column name."""
        return (
            f"SELECT column_name, data_type "
            f"FROM information_schema.columns "
            f"WHERE table_schema = '{database}' "
            f"AND table_name = '{table_name}' "
            f"AND (data_type LIKE '%VEC%' OR data_type LIKE '%vec%')"
        )

    @staticmethod
    def _build_index_tables_sql(table_name: str, column_name: str, database: str) -> str:
        """Build SQL for getting IVF index table names."""
        return (
            f"SELECT i.algo_table_type, i.index_table_name "
            f"FROM `mo_catalog`.`mo_indexes` AS i "
            f"JOIN `mo_catalog`.`mo_tables` AS t ON i.table_id = t.rel_id "
            f"AND i.column_name = '{column_name}' "
            f"AND t.relname = '{table_name}' "
            f"AND t.reldatabase = '{database}' "
            f"AND i.algo='ivfflat'"
        )

    @staticmethod
    def _build_distribution_sql(database: str, entries_table: str) -> str:
        """Build SQL for getting bucket distribution."""
        return (
            f"SELECT "
            f"  COUNT(*) AS centroid_count, "
            f"  __mo_index_centroid_fk_id AS centroid_id, "
            f"  __mo_index_centroid_fk_version AS centroid_version "
            f"FROM `{database}`.`{entries_table}` "
            f"GROUP BY `__mo_index_centroid_fk_id`, `__mo_index_centroid_fk_version`"
        )


class VectorManager(_VectorManagerBase):
    """
    Synchronous vector manager for MatrixOne vector operations.

    Supports two usage modes:
    1. Client mode: Each operation is an independent auto-commit transaction
    2. Session mode: Operations execute within the session's transaction context
    """

    def __init__(self, client, executor=None):
        """Initialize VectorManager."""
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
        """Create an IVFFLAT vector index."""
        try:
            table_name = _extract_table_name(table_name)
            setup_sqls, create_sql = self._build_create_ivf_sql(table_name, name, column, lists, op_type)

            for sql in setup_sqls:
                self.executor.execute(sql)
            self.executor.execute(create_sql)
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
        """Create an HNSW vector index."""
        try:
            table_name = _extract_table_name(table_name)
            sql = self._build_create_hnsw_sql(table_name, name, column, m, ef_construction, ef_search, op_type)
            self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create HNSW vector index {name} on table {table_name}: {e}")

    def drop(self, table_name: Union[str, type], name: str) -> "VectorManager":
        """Drop a vector index."""
        try:
            table_name = _extract_table_name(table_name)
            sql = self._build_drop_sql(table_name, name)
            self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to drop vector index {name} on table {table_name}: {e}")

    def enable_ivf(self, probe_limit: int = 1) -> "VectorManager":
        """Enable IVF indexing."""
        try:
            sqls = self._build_enable_ivf_sql(probe_limit)
            for sql in sqls:
                self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"IVF indexing is not supported: {e}")

    def disable_ivf(self) -> "VectorManager":
        """Disable IVF indexing."""
        try:
            sql = self._build_disable_ivf_sql()
            self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to disable IVF indexing: {e}")

    def enable_hnsw(self) -> "VectorManager":
        """Enable HNSW indexing."""
        try:
            sql = self._build_enable_hnsw_sql()
            self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to enable HNSW indexing: {e}")

    def disable_hnsw(self) -> "VectorManager":
        """Disable HNSW indexing."""
        try:
            sql = self._build_disable_hnsw_sql()
            self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to disable HNSW indexing: {e}")

    def insert(self, table_name: Union[str, type], data: Dict[str, Any]) -> "VectorManager":
        """Insert vector data into table."""
        try:
            table_name = _extract_table_name(table_name)
            sql = self._build_insert_sql(table_name, data)
            self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to insert vector data into table {table_name}: {e}")

    def batch_insert(self, table_name: Union[str, type], data_list: List[Dict[str, Any]]) -> "VectorManager":
        """Batch insert vector data."""
        try:
            for record in data_list:
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
        _log_mode: str = None,
    ) -> List[Dict[str, Any]]:
        """Perform vector similarity search."""
        try:
            table_name = _extract_table_name(table_name)
            sql = self._build_similarity_search_sql(
                table_name, vector_column, query_vector, limit, select_columns, where_clause, distance_type
            )
            result = self.executor.execute(sql, _log_mode=_log_mode)
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
        _log_mode: str = None,
    ) -> List[Dict[str, Any]]:
        """Perform vector range search."""
        try:
            table_name = _extract_table_name(table_name)
            sql = self._build_range_search_sql(
                table_name, vector_column, query_vector, max_distance, select_columns, where_clause
            )
            result = self.executor.execute(sql, _log_mode=_log_mode)
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
            database = getattr(self.client, 'database', 'test')

        # Auto-infer column name if not provided
        if not column_name:
            schema_sql = self._build_column_inference_sql(table_name, database)
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
        index_sql = self._build_index_tables_sql(table_name, column_name, database)
        result = self.executor.execute(index_sql)
        index_tables = {row[0]: row[1] for row in result}

        if not index_tables:
            raise Exception(f"No IVF index found for table {table_name}, column {column_name}")

        # Get the entries table name for distribution analysis
        entries_table = index_tables.get('entries')
        if not entries_table:
            raise Exception("No entries table found in IVF index")

        # Get bucket distribution
        dist_sql = self._build_distribution_sql(database, entries_table)
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


class AsyncVectorManager(_VectorManagerBase):
    """
    Asynchronous vector manager for MatrixOne vector operations.

    Supports two usage modes:
    1. AsyncClient mode: Each operation is an independent auto-commit transaction
    2. AsyncSession mode: Operations execute within the session's transaction context
    """

    def __init__(self, client, executor=None):
        """Initialize AsyncVectorManager."""
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
        """Create an IVFFLAT vector index."""
        try:
            table_name = _extract_table_name(table_name)
            setup_sqls, create_sql = self._build_create_ivf_sql(table_name, name, column, lists, op_type)

            for sql in setup_sqls:
                await self.executor.execute(sql)
            await self.executor.execute(create_sql)
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
        """Create an HNSW vector index."""
        try:
            table_name = _extract_table_name(table_name)
            sql = self._build_create_hnsw_sql(table_name, name, column, m, ef_construction, ef_search, op_type)
            await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create HNSW vector index {name} on table {table_name}: {e}")

    async def drop(self, table_name: Union[str, type], name: str) -> "AsyncVectorManager":
        """Drop a vector index."""
        try:
            table_name = _extract_table_name(table_name)
            sql = self._build_drop_sql(table_name, name)
            await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to drop vector index {name} on table {table_name}: {e}")

    async def enable_ivf(self, probe_limit: int = 1) -> "AsyncVectorManager":
        """Enable IVF indexing."""
        try:
            sqls = self._build_enable_ivf_sql(probe_limit)
            for sql in sqls:
                await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"IVF indexing is not supported: {e}")

    async def disable_ivf(self) -> "AsyncVectorManager":
        """Disable IVF indexing."""
        try:
            sql = self._build_disable_ivf_sql()
            await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to disable IVF indexing: {e}")

    async def enable_hnsw(self) -> "AsyncVectorManager":
        """Enable HNSW indexing."""
        try:
            sql = self._build_enable_hnsw_sql()
            await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to enable HNSW indexing: {e}")

    async def disable_hnsw(self) -> "AsyncVectorManager":
        """Disable HNSW indexing."""
        try:
            sql = self._build_disable_hnsw_sql()
            await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to disable HNSW indexing: {e}")

    async def insert(self, table_name: Union[str, type], data: Dict[str, Any]) -> "AsyncVectorManager":
        """Insert vector data into table."""
        try:
            table_name = _extract_table_name(table_name)
            sql = self._build_insert_sql(table_name, data)
            await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to insert vector data into table {table_name}: {e}")

    async def batch_insert(self, table_name: Union[str, type], data_list: List[Dict[str, Any]]) -> "AsyncVectorManager":
        """Batch insert vector data."""
        try:
            for record in data_list:
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
        _log_mode: str = None,
    ) -> List[Dict[str, Any]]:
        """Perform vector similarity search."""
        try:
            table_name = _extract_table_name(table_name)
            sql = self._build_similarity_search_sql(
                table_name, vector_column, query_vector, limit, select_columns, where_clause, distance_type
            )
            result = await self.executor.execute(sql, _log_mode=_log_mode)
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
        _log_mode: str = None,
    ) -> List[Dict[str, Any]]:
        """Perform vector range search."""
        try:
            table_name = _extract_table_name(table_name)
            sql = self._build_range_search_sql(
                table_name, vector_column, query_vector, max_distance, select_columns, where_clause
            )
            result = await self.executor.execute(sql, _log_mode=_log_mode)
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
            database = getattr(self.client, 'database', 'test')

        # Auto-infer column name if not provided
        if not column_name:
            schema_sql = self._build_column_inference_sql(table_name, database)
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
        index_sql = self._build_index_tables_sql(table_name, column_name, database)
        result = await self.executor.execute(index_sql)
        index_tables = {row[0]: row[1] for row in result}

        if not index_tables:
            raise Exception(f"No IVF index found for table {table_name}, column {column_name}")

        # Get the entries table name for distribution analysis
        entries_table = index_tables.get('entries')
        if not entries_table:
            raise Exception("No entries table found in IVF index")

        # Get bucket distribution
        dist_sql = self._build_distribution_sql(database, entries_table)
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
