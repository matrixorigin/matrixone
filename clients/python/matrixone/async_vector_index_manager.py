"""
Async vector index manager for MatrixOne async client.
"""

from sqlalchemy import text


class AsyncVectorManager:
    """Unified async vector manager for client chain operations - handles both index and data operations"""

    def __init__(self, client):
        self.client = client

    async def create_ivf(
        self,
        table_name: str,
        name: str,
        column: str,
        lists: int = 100,
        op_type: str = "vector_l2_ops",
    ) -> "AsyncVectorManager":
        """
        Create an IVFFLAT vector index using chain operations.

        Args:
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            lists: Number of lists for IVFFLAT (default: 100)
            op_type: Vector operation type (default: vector_l2_ops)

        Returns:
            AsyncVectorManager: Self for chaining
        """
        from .sqlalchemy_ext import IVFVectorIndex, VectorOpType

        # Convert string parameters to enum values
        if op_type == "vector_l2_ops":
            op_type = VectorOpType.VECTOR_L2_OPS

        try:
            index = IVFVectorIndex(name, column, lists, op_type)
            sql = index.create_sql(table_name)

            async with self.client._engine.begin() as conn:
                # Enable IVF indexing in the same connection
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
            op_type: Vector operation type (default: vector_l2_ops)

        Returns:
            AsyncVectorManager: Self for chaining
        """
        from .sqlalchemy_ext import HnswVectorIndex, VectorOpType

        # Convert string parameters to enum values
        if op_type == "vector_l2_ops":
            op_type = VectorOpType.VECTOR_L2_OPS

        try:
            index = HnswVectorIndex(name, column, m, ef_construction, ef_search, op_type)
            sql = index.create_sql(table_name)

            async with self.client._engine.begin() as conn:
                # Enable HNSW indexing in the same connection
                await conn.execute(text("SET experimental_hnsw_index = 1"))
                await conn.execute(text(sql))
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
                await conn.execute(text(f"DROP INDEX IF EXISTS {name} ON {table_name}"))
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
    async def insert(self, table_name: str, data: dict) -> "AsyncVectorManager":
        """
        Insert vector data using chain operations asynchronously.

        Args:
            table_name: Name of the table
            data: Data to insert (dict with column names as keys)

        Returns:
            AsyncVectorManager: Self for chaining
        """
        await self.client.insert(table_name, data)
        return self

    async def insert_in_transaction(self, table_name: str, data: dict, connection) -> "AsyncVectorManager":
        """
        Insert vector data within an existing SQLAlchemy transaction asynchronously.

        Args:
            table_name: Name of the table
            data: Data to insert (dict with column names as keys)
            connection: SQLAlchemy connection object (required for transaction support)

        Returns:
            AsyncVectorManager: Self for chaining

        Raises:
            ValueError: If connection is not provided
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        # Use client's insert method but execute within transaction
        from sqlalchemy import text

        # Build INSERT statement
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

        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        await connection.execute(text(sql))

        return self

    async def batch_insert(self, table_name: str, data_list: list) -> "AsyncVectorManager":
        """
        Batch insert vector data using chain operations asynchronously.

        Args:
            table_name: Name of the table
            data_list: List of data dictionaries to insert

        Returns:
            AsyncVectorManager: Self for chaining
        """
        await self.client.batch_insert(table_name, data_list)
        return self
