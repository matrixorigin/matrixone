"""
Async vector index manager for MatrixOne async client.
"""

from sqlalchemy import text


class AsyncVectorIndexManager:
    """Async vector index manager for client chain operations"""

    def __init__(self, client):
        self.client = client

    async def create_ivf(
        self,
        table_name: str,
        name: str,
        column: str,
        lists: int = 100,
        op_type: str = "vector_l2_ops",
    ) -> "AsyncVectorIndexManager":
        """
        Create an IVFFLAT vector index using chain operations.

        Args:
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            lists: Number of lists for IVFFLAT (default: 100)
            op_type: Vector operation type (default: vector_l2_ops)

        Returns:
            AsyncVectorIndexManager: Self for chaining
        """
        from .sqlalchemy_ext import IVFVectorIndex, VectorOpType

        # Convert string parameters to enum values
        if op_type == "vector_l2_ops":
            op_type = VectorOpType.VECTOR_L2_OPS

        try:
            index = IVFVectorIndex(name, column, lists, op_type)
            sql = index.create_sql(table_name)

            async with self.client._engine.begin() as conn:
                # Enable IVF indexing
                await conn.execute(text("SET experimental_ivf_index = 1"))
                await conn.execute(text("SET probe_limit = 1"))
                await conn.execute(text(sql))
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
        op_type: str = "vector_l2_ops",
    ) -> "AsyncVectorIndexManager":
        """
        Create an HNSW vector index using chain operations.

        Args:
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            m: Number of bi-directional links for HNSW (default: 16)
            ef_construction: Size of dynamic candidate list for HNSW construction (default: 200)
            ef_search: Size of dynamic candidate list for HNSW search (default: 50)
            op_type: Vector operation type (default: vector_l2_ops)

        Returns:
            AsyncVectorIndexManager: Self for chaining
        """
        from .sqlalchemy_ext import HnswVectorIndex, VectorOpType

        # Convert string parameters to enum values
        if op_type == "vector_l2_ops":
            op_type = VectorOpType.VECTOR_L2_OPS

        try:
            index = HnswVectorIndex(name, column, m, ef_construction, ef_search, op_type)
            sql = index.create_sql(table_name)

            async with self.client._engine.begin() as conn:
                # Enable HNSW indexing
                await conn.execute(text("SET experimental_hnsw_index = 1"))
                await conn.execute(text(sql))
            return self
        except Exception as e:
            raise Exception(f"Failed to create HNSW vector index {name} on table {table_name}: {e}")

    async def drop(self, table_name: str, name: str) -> "AsyncVectorIndexManager":
        """
        Drop a vector index using chain operations.

        Args:
            table_name: Name of the table
            name: Name of the index

        Returns:
            AsyncVectorIndexManager: Self for chaining
        """
        try:
            async with self.client._engine.begin() as conn:
                await conn.execute(text(f"DROP INDEX IF EXISTS {name} ON {table_name}"))
            return self
        except Exception as e:
            raise Exception(f"Failed to drop vector index {name} from table {table_name}: {e}")

    async def enable_ivf(self, probe_limit: int = 1) -> "AsyncVectorIndexManager":
        """
        Enable IVF indexing with probe limit.

        Args:
            probe_limit: Probe limit for IVF indexing

        Returns:
            AsyncVectorIndexManager: Self for chaining
        """
        try:
            await self.client.execute("SET experimental_ivf_index = 1")
            await self.client.execute(f"SET probe_limit = {probe_limit}")
            return self
        except Exception as e:
            raise Exception(f"Failed to enable IVF indexing: {e}")

    async def disable_ivf(self) -> "AsyncVectorIndexManager":
        """
        Disable IVF indexing.

        Returns:
            AsyncVectorIndexManager: Self for chaining
        """
        try:
            await self.client.execute("SET experimental_ivf_index = 0")
            return self
        except Exception as e:
            raise Exception(f"Failed to disable IVF indexing: {e}")

    async def enable_hnsw(self) -> "AsyncVectorIndexManager":
        """
        Enable HNSW indexing.

        Returns:
            AsyncVectorIndexManager: Self for chaining
        """
        try:
            await self.client.execute("SET experimental_hnsw_index = 1")
            return self
        except Exception as e:
            raise Exception(f"Failed to enable HNSW indexing: {e}")

    async def disable_hnsw(self) -> "AsyncVectorIndexManager":
        """
        Disable HNSW indexing.

        Returns:
            AsyncVectorIndexManager: Self for chaining
        """
        try:
            await self.client.execute("SET experimental_hnsw_index = 0")
            return self
        except Exception as e:
            raise Exception(f"Failed to disable HNSW indexing: {e}")
