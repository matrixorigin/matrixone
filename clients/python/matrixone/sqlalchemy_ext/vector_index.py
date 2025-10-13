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
Vector index support for SQLAlchemy integration with MatrixOne.
"""

from typing import List, Optional, Union

from sqlalchemy import Column, Index, text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql.ddl import CreateIndex as SQLAlchemyCreateIndex


def _exec_sql_safe(connection, sql: str):
    """
    Execute SQL safely, bypassing SQLAlchemy's bind parameter parsing.

    This prevents JSON strings like {"a":1} from being incorrectly parsed as :1 bind params.
    Uses exec_driver_sql() when available, falls back to text() for testing/compatibility.
    """
    if hasattr(connection, 'exec_driver_sql'):
        # Escape % to %% for pymysql's format string handling
        escaped_sql = sql.replace('%', '%%')
        return connection.exec_driver_sql(escaped_sql)
    else:
        # Fallback for testing or older SQLAlchemy versions
        return connection.execute(text(sql))


class VectorIndexType:
    """Enum-like class for vector index types."""

    IVFFLAT = "ivfflat"
    HNSW = "hnsw"  # Future support


class VectorOpType:
    """Enum-like class for vector operation types."""

    VECTOR_L2_OPS = "vector_l2_ops"
    VECTOR_IP_OPS = "vector_ip_ops"
    VECTOR_COSINE_OPS = "vector_cosine_ops"


class IVFVectorIndex(Index):
    """
    SQLAlchemy Index for IVFFLAT vector columns with MatrixOne-specific syntax.

    Specialized class for IVFFLAT vector indexes with type safety and clear API.

    Usage Examples

    1. Class Methods (Recommended for one-time operations):

        # Create index using class method
        success = IVFVectorIndex.create_index(
            engine=engine,
            table_name='my_table',
            name='idx_embedding',
            column='embedding',
            lists=100,
            op_type=VectorOpType.VECTOR_L2_OPS
        )

        # Drop index using class method
        success = IVFVectorIndex.drop_index(
            engine=engine,
            table_name='my_table',
            name='idx_embedding'
        )

        # Create index within existing transaction
        with engine.begin() as conn:
            success = IVFVectorIndex.create_index_in_transaction(
                connection=conn,
                table_name='my_table',
                name='idx_embedding',
                column='embedding',
                lists=100
            )

        # Drop index within existing transaction
        with engine.begin() as conn:
            success = IVFVectorIndex.drop_index_in_transaction(
                connection=conn,
                table_name='my_table',
                name='idx_embedding'
            )

    2. Instance Methods (Useful for reusable index configurations):

        # Create index object
        index = IVFVectorIndex('idx_embedding', 'embedding', lists=100)

        # Create index using instance method
        success = index.create(engine, 'my_table')

        # Drop index using instance method
        success = index.drop(engine, 'my_table')

        # Create index within existing transaction
        with engine.begin() as conn:
            success = index.create_in_transaction(conn, 'my_table')

        # Drop index within existing transaction
        with engine.begin() as conn:
            success = index.drop_in_transaction(conn, 'my_table')

    3. SQLAlchemy ORM Integration:

        # In table definition
        class Document(Base):
            __tablename__ = 'documents'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(128, "f32")

            # Note: For ORM integration, create table first, then create index separately
            # __table_args__ = (IVFVectorIndex('idx_embedding', 'embedding', lists=100),)

        # Create table first
        Base.metadata.create_all(engine)

        # Then create index separately
        IVFVectorIndex.create_index(engine, 'documents', 'idx_embedding', 'embedding', lists=100)

    4. Client Chain Operations:

        # Using client.vector_index.create_ivf() method
        client.vector_index.create_ivf('my_table', 'idx_embedding', 'embedding', lists=100)

        # Using client.vector_index.create_ivf_in_transaction() method
        with client.transaction() as tx:
            client.vector_index.create_ivf_in_transaction(
                'my_table', 'idx_embedding', 'embedding', tx.connection, lists=100
            )

    Parameters:
        name (str): Index name
        column (Union[str, Column]): Vector column to index
        lists (int): Number of lists for IVFFLAT (default: 100)
        op_type (str): Vector operation type (default: vector_l2_ops)
        **kwargs: Additional index parameters

    Note:

        - MatrixOne supports only ONE index per vector column
        - Enable IVF indexing before creating IVFFLAT indexes: SET experimental_ivf_index = 1
        - Set probe limit for search: SET probe_limit = 1
    """

    def __init__(
        self,
        name: str,
        column: Union[str, Column],
        lists: int = 100,
        op_type: str = VectorOpType.VECTOR_L2_OPS,
        **kwargs,
    ):
        """
        Initialize IVFVectorIndex.

        Args::

            name: Index name
            column: Vector column to index
            lists: Number of lists for IVFFLAT (default: 100)
            op_type: Vector operation type (default: vector_l2_ops)
            **kwargs: Additional index parameters
        """
        self.index_type = VectorIndexType.IVFFLAT
        self.lists = lists
        self.op_type = op_type

        # Store column name for later use
        self._column_name = str(column) if not isinstance(column, str) else column

        # Call parent constructor first
        super().__init__(name, column, **kwargs)

        # Set dialect options after initialization to bind to matrixone dialect
        self.dialect_options["matrixone"] = {"length": None, "using": None}
        # Also provide mysql fallback for compatibility
        self.dialect_options["mysql"] = {"length": None, "using": None, "with_parser": None}

    def _create_index_sql(self, table_name: str) -> str:
        """Generate the CREATE INDEX SQL for IVFFLAT vector index."""
        column_name = self._column_name
        sql_parts = [f"CREATE INDEX {self.name} USING {self.index_type} ON {table_name}({column_name})"]
        sql_parts.append(f"LISTS {self.lists}")
        sql_parts.append(f"op_type '{self.op_type}'")
        return " ".join(sql_parts)

    def create_sql(self, table_name: str) -> str:
        """Generate CREATE INDEX SQL for the given table name."""
        return self._create_index_sql(table_name)

    def drop_sql(self, table_name: str) -> str:
        """Generate DROP INDEX SQL for the given table name."""
        return f"DROP INDEX {self.name} ON {table_name}"

    @classmethod
    def create_index(
        cls,
        engine,
        table_name: str,
        name: str,
        column: Union[str, Column],
        lists: int = 100,
        op_type: str = VectorOpType.VECTOR_L2_OPS,
        **kwargs,
    ) -> bool:
        """
        Create an IVFFLAT vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            lists: Number of lists for IVFFLAT (default: 100)
            op_type: Vector operation type (default: vector_l2_ops)
            **kwargs: Additional index parameters

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            index = cls(name, column, lists, op_type, **kwargs)
            sql = index.create_sql(table_name)

            with engine.begin() as conn:
                # Enable IVF indexing
                _exec_sql_safe(conn, "SET experimental_ivf_index = 1")
                _exec_sql_safe(conn, "SET probe_limit = 1")

                # Execute CREATE INDEX
                _exec_sql_safe(conn, sql)

                # Try to log using the connection's info attribute if available
                try:
                    from ..client import logger

                    logger.info(
                        f"✓ | CREATE INDEX {name} USING ivfflat ON {table_name}({column}) "
                        f"LISTS {lists} op_type '{op_type}'"
                    )
                except Exception:
                    pass  # Silently skip logging if not available (for tests)

            return True
        except Exception as e:
            print(f"Failed to create IVFFLAT vector index: {e}")
            return False

    @classmethod
    def create_index_in_transaction(
        cls,
        connection,
        table_name: str,
        name: str,
        column: Union[str, Column],
        lists: int = 100,
        op_type: str = VectorOpType.VECTOR_L2_OPS,
        **kwargs,
    ) -> bool:
        """
        Create an IVFFLAT vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection object
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            lists: Number of lists for IVFFLAT (default: 100)
            op_type: Vector operation type (default: vector_l2_ops)
            **kwargs: Additional index parameters

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            index = cls(name, column, lists, op_type, **kwargs)
            sql = index.create_sql(table_name)

            # Enable IVF indexing
            _exec_sql_safe(connection, "SET experimental_ivf_index = 1")
            _exec_sql_safe(connection, "SET probe_limit = 1")
            _exec_sql_safe(connection, sql)
            return True
        except Exception as e:
            print(f"Failed to create IVFFLAT vector index in transaction: {e}")
            return False

    @classmethod
    def drop_index(cls, engine, table_name: str, name: str) -> bool:
        """
        Drop an IVFFLAT vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table
            name: Name of the index to drop

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = f"DROP INDEX {name} ON {table_name}"
            with engine.begin() as conn:
                _exec_sql_safe(conn, sql)
            return True
        except Exception as e:
            print(f"Failed to drop IVFFLAT vector index: {e}")
            return False

    @classmethod
    def drop_index_in_transaction(cls, connection, table_name: str, name: str) -> bool:
        """
        Drop an IVFFLAT vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection object
            table_name: Name of the table
            name: Name of the index to drop

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = f"DROP INDEX {name} ON {table_name}"
            _exec_sql_safe(connection, sql)
            return True
        except Exception as e:
            print(f"Failed to drop IVFFLAT vector index in transaction: {e}")
            return False

    def create(self, engine, table_name: str) -> bool:
        """
        Create this IVFFLAT vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = self.create_sql(table_name)

            with engine.begin() as conn:
                # Enable IVF indexing
                _exec_sql_safe(conn, "SET experimental_ivf_index = 1")
                _exec_sql_safe(conn, "SET probe_limit = 1")
                _exec_sql_safe(conn, sql)
            return True
        except Exception as e:
            print(f"Failed to create IVFFLAT vector index: {e}")
            return False

    def drop(self, engine, table_name: str) -> bool:
        """
        Drop this IVFFLAT vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = self.drop_sql(table_name)
            with engine.begin() as conn:
                _exec_sql_safe(conn, sql)
            return True
        except Exception as e:
            print(f"Failed to drop IVFFLAT vector index: {e}")
            return False

    def create_in_transaction(self, connection, table_name: str) -> bool:
        """
        Create this IVFFLAT vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection object
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = self.create_sql(table_name)

            # Enable IVF indexing
            _exec_sql_safe(connection, "SET experimental_ivf_index = 1")
            _exec_sql_safe(connection, "SET probe_limit = 1")
            _exec_sql_safe(connection, sql)
            return True
        except Exception as e:
            print(f"Failed to create IVFFLAT vector index in transaction: {e}")
            return False

    def drop_in_transaction(self, connection, table_name: str) -> bool:
        """
        Drop this IVFFLAT vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection object
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = self.drop_sql(table_name)
            _exec_sql_safe(connection, sql)
            return True
        except Exception as e:
            print(f"Failed to drop IVFFLAT vector index in transaction: {e}")
            return False


class HnswVectorIndex(Index):
    """
    SQLAlchemy Index for HNSW vector columns with MatrixOne-specific syntax.

    Specialized class for HNSW vector indexes with type safety and clear API.

    Usage Examples

    1. Class Methods (Recommended for one-time operations):

        # Create index using class method
        success = HnswVectorIndex.create_index(
            engine=engine,
            table_name='my_table',
            name='idx_embedding',
            column='embedding',
            m=16,
            ef_construction=200,
            ef_search=50,
            op_type=VectorOpType.VECTOR_L2_OPS
        )

        # Drop index using class method
        success = HnswVectorIndex.drop_index(
            engine=engine,
            table_name='my_table',
            name='idx_embedding'
        )

        # Create index within existing transaction
        with engine.begin() as conn:
            success = HnswVectorIndex.create_index_in_transaction(
                connection=conn,
                table_name='my_table',
                name='idx_embedding',
                column='embedding',
                m=16,
                ef_construction=200,
                ef_search=50
            )

        # Drop index within existing transaction
        with engine.begin() as conn:
            success = HnswVectorIndex.drop_index_in_transaction(
                connection=conn,
                table_name='my_table',
                name='idx_embedding'
            )

    2. Instance Methods (Useful for reusable index configurations):

        # Create index object
        index = HnswVectorIndex('idx_embedding', 'embedding', m=16, ef_construction=200, ef_search=50)

        # Create index using instance method
        success = index.create(engine, 'my_table')

        # Drop index using instance method
        success = index.drop(engine, 'my_table')

        # Create index within existing transaction
        with engine.begin() as conn:
            success = index.create_in_transaction(conn, 'my_table')

        # Drop index within existing transaction
        with engine.begin() as conn:
            success = index.drop_in_transaction(conn, 'my_table')

    3. SQLAlchemy ORM Integration:

        # In table definition (requires BigInteger primary key for HNSW)
        class Document(Base):
            __tablename__ = 'documents'
            id = Column(BigInteger, primary_key=True)  # BigInteger required for HNSW
            embedding = create_vector_column(128, "f32")

            # Note: For ORM integration, create table first, then create index separately
            # __table_args__ = (HnswVectorIndex('idx_embedding', 'embedding', m=16),)

        # Create table first
        Base.metadata.create_all(engine)

        # Then create index separately
        HnswVectorIndex.create_index(engine, 'documents', 'idx_embedding', 'embedding', m=16)

    4. Client Chain Operations:

        # Using client.vector_index.create_hnsw() method
        client.vector_index.create_hnsw('my_table', 'idx_embedding', 'embedding', m=16, ef_construction=200)

        # Using client.vector_index.create_hnsw_in_transaction() method
        with client.transaction() as tx:
            client.vector_index.create_hnsw_in_transaction(
                'my_table', 'idx_embedding', 'embedding', tx.connection, m=16
            )

    Parameters:
        name (str): Index name
        column (Union[str, Column]): Vector column to index
        m (int): Number of bi-directional links for HNSW (default: 16)
        ef_construction (int): Size of dynamic candidate list for HNSW construction (default: 200)
        ef_search (int): Size of dynamic candidate list for HNSW search (default: 50)
        op_type (str): Vector operation type (default: vector_l2_ops)
        **kwargs: Additional index parameters

    Note:

        - MatrixOne supports only ONE index per vector column
        - Enable HNSW indexing before creating HNSW indexes: SET experimental_hnsw_index = 1
        - HNSW indexes require BigInteger primary key in the table
        - Higher M values provide better recall but slower construction
        - Higher ef_construction provides better index quality but slower construction
        - Higher ef_search provides better recall but slower search
    """

    def __init__(
        self,
        name: str,
        column: Union[str, Column],
        m: int = 16,
        ef_construction: int = 200,
        ef_search: int = 50,
        op_type: str = VectorOpType.VECTOR_L2_OPS,
        **kwargs,
    ):
        """
        Initialize HnswVectorIndex.

        Args::

            name: Index name
            column: Vector column to index
            m: Number of bi-directional links for HNSW (default: 16)
            ef_construction: Size of dynamic candidate list for HNSW construction (default: 200)
            ef_search: Size of dynamic candidate list for HNSW search (default: 50)
            op_type: Vector operation type (default: vector_l2_ops)
            **kwargs: Additional index parameters
        """
        self.index_type = VectorIndexType.HNSW
        self.m = m
        self.ef_construction = ef_construction
        self.ef_search = ef_search
        self.op_type = op_type

        # Store column name for later use
        self._column_name = str(column) if not isinstance(column, str) else column

        # Call parent constructor first
        super().__init__(name, column, **kwargs)

        # Set dialect options after initialization to bind to matrixone dialect
        self.dialect_options["matrixone"] = {"length": None, "using": None}
        # Also provide mysql fallback for compatibility
        self.dialect_options["mysql"] = {"length": None, "using": None, "with_parser": None}

    def _create_index_sql(self, table_name: str) -> str:
        """Generate the CREATE INDEX SQL for HNSW vector index."""
        column_name = self._column_name
        sql_parts = [f"CREATE INDEX {self.name} USING {self.index_type} ON {table_name}({column_name})"]
        sql_parts.append(f"M {self.m}")
        sql_parts.append(f"EF_CONSTRUCTION {self.ef_construction}")
        sql_parts.append(f"EF_SEARCH {self.ef_search}")
        sql_parts.append(f"op_type '{self.op_type}'")
        return " ".join(sql_parts)

    def create_sql(self, table_name: str) -> str:
        """Generate CREATE INDEX SQL for the given table name."""
        return self._create_index_sql(table_name)

    def drop_sql(self, table_name: str) -> str:
        """Generate DROP INDEX SQL for the given table name."""
        return f"DROP INDEX {self.name} ON {table_name}"

    @classmethod
    def create_index(
        cls,
        engine,
        table_name: str,
        name: str,
        column: Union[str, Column],
        m: int = 16,
        ef_construction: int = 200,
        ef_search: int = 50,
        op_type: str = VectorOpType.VECTOR_L2_OPS,
        **kwargs,
    ) -> bool:
        """
        Create an HNSW vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            m: Number of bi-directional links for HNSW (default: 16)
            ef_construction: Size of dynamic candidate list for HNSW construction (default: 200)
            ef_search: Size of dynamic candidate list for HNSW search (default: 50)
            op_type: Vector operation type (default: vector_l2_ops)
            **kwargs: Additional index parameters

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            index = cls(name, column, m, ef_construction, ef_search, op_type, **kwargs)
            sql = index.create_sql(table_name)

            with engine.begin() as conn:
                # Enable HNSW indexing
                _exec_sql_safe(conn, "SET experimental_hnsw_index = 1")

                # Execute CREATE INDEX
                _exec_sql_safe(conn, sql)

                # Try to log using the connection's info attribute if available
                try:
                    from ..client import logger

                    logger.info(
                        f"✓ | CREATE INDEX {name} USING hnsw ON {table_name}({column}) "
                        f"M {m} EF_CONSTRUCTION {ef_construction} EF_SEARCH {ef_search} op_type '{op_type}'"
                    )
                except Exception:
                    pass  # Silently skip logging if not available (for tests)

            return True
        except Exception as e:
            print(f"Failed to create HNSW vector index: {e}")
            return False

    @classmethod
    def create_index_in_transaction(
        cls,
        connection,
        table_name: str,
        name: str,
        column: Union[str, Column],
        m: int = 16,
        ef_construction: int = 200,
        ef_search: int = 50,
        op_type: str = VectorOpType.VECTOR_L2_OPS,
        **kwargs,
    ) -> bool:
        """
        Create an HNSW vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection object
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            m: Number of bi-directional links for HNSW (default: 16)
            ef_construction: Size of dynamic candidate list for HNSW construction (default: 200)
            ef_search: Size of dynamic candidate list for HNSW search (default: 50)
            op_type: Vector operation type (default: vector_l2_ops)
            **kwargs: Additional index parameters

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            index = cls(name, column, m, ef_construction, ef_search, op_type, **kwargs)
            sql = index.create_sql(table_name)

            # Enable HNSW indexing
            _exec_sql_safe(connection, "SET experimental_hnsw_index = 1")
            _exec_sql_safe(connection, sql)
            return True
        except Exception as e:
            print(f"Failed to create HNSW vector index in transaction: {e}")
            return False

    @classmethod
    def drop_index(cls, engine, table_name: str, name: str) -> bool:
        """
        Drop an HNSW vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table
            name: Name of the index to drop

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = f"DROP INDEX {name} ON {table_name}"
            with engine.begin() as conn:
                _exec_sql_safe(conn, sql)
            return True
        except Exception as e:
            print(f"Failed to drop HNSW vector index: {e}")
            return False

    @classmethod
    def drop_index_in_transaction(cls, connection, table_name: str, name: str) -> bool:
        """
        Drop an HNSW vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection object
            table_name: Name of the table
            name: Name of the index to drop

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = f"DROP INDEX {name} ON {table_name}"
            _exec_sql_safe(connection, sql)
            return True
        except Exception as e:
            print(f"Failed to drop HNSW vector index in transaction: {e}")
            return False

    def create(self, engine, table_name: str) -> bool:
        """
        Create this HNSW vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = self.create_sql(table_name)

            with engine.begin() as conn:
                # Enable HNSW indexing
                _exec_sql_safe(conn, "SET experimental_hnsw_index = 1")
                _exec_sql_safe(conn, sql)
            return True
        except Exception as e:
            print(f"Failed to create HNSW vector index: {e}")
            return False

    def drop(self, engine, table_name: str) -> bool:
        """
        Drop this HNSW vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = self.drop_sql(table_name)
            with engine.begin() as conn:
                _exec_sql_safe(conn, sql)
            return True
        except Exception as e:
            print(f"Failed to drop HNSW vector index: {e}")
            return False

    def create_in_transaction(self, connection, table_name: str) -> bool:
        """
        Create this HNSW vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection object
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = self.create_sql(table_name)

            # Enable HNSW indexing
            _exec_sql_safe(connection, "SET experimental_hnsw_index = 1")
            _exec_sql_safe(connection, sql)
            return True
        except Exception as e:
            print(f"Failed to create HNSW vector index in transaction: {e}")
            return False

    def drop_in_transaction(self, connection, table_name: str) -> bool:
        """
        Drop this HNSW vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection object
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = self.drop_sql(table_name)
            _exec_sql_safe(connection, sql)
            return True
        except Exception as e:
            print(f"Failed to drop HNSW vector index in transaction: {e}")
            return False


class VectorIndex(Index):
    """
    SQLAlchemy Index for vector columns with MatrixOne-specific syntax.

    This class provides a generic interface for creating vector indexes with various
    algorithms and operation types. It supports both IVF (Inverted File) and HNSW
    (Hierarchical Navigable Small World) indexing algorithms.

    Key Features:

    - Support for multiple vector indexing algorithms (IVF, HNSW)
    - Configurable operation types (L2 distance, cosine similarity, inner product)
    - Automatic SQL generation for index creation and management
    - Integration with MatrixOne's vector search capabilities
    - Support for both class methods and instance methods

    Supported Index Types:
    - IVF (Inverted File): Good for large datasets, requires training
    - HNSW: Good for high-dimensional vectors, no training required

    Supported Operation Types:
    - VECTOR_L2_OPS: L2 (Euclidean) distance
    - VECTOR_COSINE_OPS: Cosine similarity
    - VECTOR_INNER_PRODUCT_OPS: Inner product similarity

    Usage Examples::

        # Create IVF index
        index = VectorIndex(
            name='vec_idx_ivf',
            column='embedding',
            index_type=VectorIndexType.IVFFLAT,
            lists=100,
            op_type=VectorOpType.VECTOR_L2_OPS
        )

        # Create HNSW index
        index = VectorIndex(
            name='vec_idx_hnsw',
            column='embedding',
            index_type=VectorIndexType.HNSW,
            m=16,
            ef_construction=200,
            op_type=VectorOpType.VECTOR_COSINE_OPS
        )

    Note: This is the legacy generic class. For better type safety and specific
    algorithm features, consider using IVFVectorIndex or HnswVectorIndex instead.
    """

    def __init__(
        self,
        name: str,
        column: Union[str, Column],
        index_type: str = VectorIndexType.IVFFLAT,
        lists: Optional[int] = None,
        op_type: str = VectorOpType.VECTOR_L2_OPS,
        # HNSW parameters
        m: Optional[int] = None,
        ef_construction: Optional[int] = None,
        ef_search: Optional[int] = None,
        **kwargs,
    ):
        """
        Initialize VectorIndex.

        Args::

            name: Index name
            column: Vector column to index
            index_type: Type of vector index (ivfflat, hnsw, etc.)
            lists: Number of lists for IVFFLAT (optional)
            op_type: Vector operation type
            m: Number of bi-directional links for HNSW (optional)
            ef_construction: Size of dynamic candidate list for HNSW construction (optional)
            ef_search: Size of dynamic candidate list for HNSW search (optional)
            **kwargs: Additional index parameters
        """
        self.index_type = index_type
        self.lists = lists
        self.op_type = op_type
        # HNSW parameters
        self.m = m
        self.ef_construction = ef_construction
        self.ef_search = ef_search

        # Store column name for later use
        self._column_name = str(column) if not isinstance(column, str) else column

        # Call parent constructor first
        super().__init__(name, column, **kwargs)

        # Set dialect options after initialization to bind to matrixone dialect
        self.dialect_options["matrixone"] = {"length": None, "using": None}
        # Also provide mysql fallback for compatibility
        self.dialect_options["mysql"] = {"length": None, "using": None, "with_parser": None}

    def _create_index_sql(self, table_name: str) -> str:
        """Generate the CREATE INDEX SQL for vector index."""
        # For simplicity, we'll use the column name passed during initialization
        # This should be stored as a string in most cases
        column_name = self._column_name

        sql_parts = [f"CREATE INDEX {self.name} USING {self.index_type} ON {table_name}({column_name})"]

        # Add parameters based on index type
        if self.index_type == VectorIndexType.IVFFLAT and self.lists is not None:
            sql_parts.append(f"lists = {self.lists}")
        elif self.index_type == VectorIndexType.HNSW:
            # Add HNSW parameters
            if self.m is not None:
                sql_parts.append(f"M {self.m}")
            if self.ef_construction is not None:
                sql_parts.append(f"EF_CONSTRUCTION {self.ef_construction}")
            if self.ef_search is not None:
                sql_parts.append(f"EF_SEARCH {self.ef_search}")

        # Add operation type
        sql_parts.append(f"op_type '{self.op_type}'")

        return " ".join(sql_parts)

    def create_sql(self, table_name: str) -> str:
        """Generate CREATE INDEX SQL for the given table name."""
        return self._create_index_sql(table_name)

    def drop_sql(self, table_name: str) -> str:
        """Generate DROP INDEX SQL for the given table name."""
        return f"DROP INDEX {self.name} ON {table_name}"

    @classmethod
    def create_index(
        cls,
        engine,
        table_name: str,
        name: str,
        column: Union[str, Column],
        index_type: str = VectorIndexType.IVFFLAT,
        lists: Optional[int] = None,
        op_type: str = VectorOpType.VECTOR_L2_OPS,
        # HNSW parameters
        m: Optional[int] = None,
        ef_construction: Optional[int] = None,
        ef_search: Optional[int] = None,
        **kwargs,
    ) -> bool:
        """
        Create a vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            index_type: Type of vector index (ivfflat, hnsw, etc.)
            lists: Number of lists for IVFFLAT (optional)
            op_type: Vector operation type
            m: Number of bi-directional links for HNSW (optional)
            ef_construction: Size of dynamic candidate list for HNSW construction (optional)
            ef_search: Size of dynamic candidate list for HNSW search (optional)
            **kwargs: Additional index parameters

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            index = cls(name, column, index_type, lists, op_type, m, ef_construction, ef_search, **kwargs)
            sql = index.create_sql(table_name)

            with engine.begin() as conn:
                # Enable appropriate indexing in the same connection
                if index_type == VectorIndexType.IVFFLAT:
                    _exec_sql_safe(conn, "SET experimental_ivf_index = 1")
                    _exec_sql_safe(conn, "SET probe_limit = 1")
                elif index_type == VectorIndexType.HNSW:
                    _exec_sql_safe(conn, "SET experimental_hnsw_index = 1")

                _exec_sql_safe(conn, sql)
            return True
        except Exception as e:
            print(f"Failed to create vector index: {e}")
            return False

    @classmethod
    def drop_index(cls, engine, table_name: str, name: str) -> bool:
        """
        Drop a vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table
            name: Name of the index to drop

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = f"DROP INDEX {name} ON {table_name}"
            with engine.begin() as conn:
                _exec_sql_safe(conn, sql)
            return True
        except Exception as e:
            print(f"Failed to drop vector index: {e}")
            return False

    def create(self, engine, table_name: str) -> bool:
        """
        Create this vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        return self.__class__.create_index(
            engine,
            table_name,
            self.name,
            self._column_name,
            self.index_type,
            self.lists,
            self.op_type,
            self.m,
            self.ef_construction,
            self.ef_search,
        )

    def drop(self, engine, table_name: str) -> bool:
        """
        Drop this vector index using ORM-style method.

        Args::

            engine: SQLAlchemy engine
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        return self.__class__.drop_index(engine, table_name, self.name)

    @classmethod
    def create_index_in_transaction(
        cls,
        connection,
        table_name: str,
        name: str,
        column: Union[str, Column],
        index_type: str = VectorIndexType.IVFFLAT,
        lists: Optional[int] = None,
        op_type: str = VectorOpType.VECTOR_L2_OPS,
        # HNSW parameters
        m: Optional[int] = None,
        ef_construction: Optional[int] = None,
        ef_search: Optional[int] = None,
        **kwargs,
    ) -> bool:
        """
        Create a vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection (within a transaction)
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            index_type: Type of vector index (ivfflat, hnsw, etc.)
            lists: Number of lists for IVFFLAT (optional)
            op_type: Vector operation type
            m: Number of bi-directional links for HNSW (optional)
            ef_construction: Size of dynamic candidate list for HNSW construction (optional)
            ef_search: Size of dynamic candidate list for HNSW search (optional)
            **kwargs: Additional index parameters

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            index = cls(name, column, index_type, lists, op_type, m, ef_construction, ef_search, **kwargs)
            sql = index.create_sql(table_name)

            # Note: Indexing should be enabled before calling this method
            # The SET statements are removed to avoid interfering with transaction rollback

            _exec_sql_safe(connection, sql)
            return True
        except Exception as e:
            print(f"Failed to create vector index in transaction: {e}")
            # Re-raise the exception to ensure transaction rollback
            raise

    @classmethod
    def drop_index_in_transaction(cls, connection, table_name: str, name: str) -> bool:
        """
        Drop a vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection (within a transaction)
            table_name: Name of the table
            name: Name of the index to drop

        Returns::

            bool: True if successful, False otherwise
        """
        try:
            sql = f"DROP INDEX {name} ON {table_name}"
            _exec_sql_safe(connection, sql)
            return True
        except Exception as e:
            print(f"Failed to drop vector index in transaction: {e}")
            return False

    def create_in_transaction(self, connection, table_name: str) -> bool:
        """
        Create this vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection (within a transaction)
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        return self.__class__.create_index_in_transaction(
            connection,
            table_name,
            self.name,
            self._column_name,
            self.index_type,
            self.lists,
            self.op_type,
            self.m,
            self.ef_construction,
            self.ef_search,
        )

    def drop_in_transaction(self, connection, table_name: str) -> bool:
        """
        Drop this vector index within an existing transaction.

        Args::

            connection: SQLAlchemy connection (within a transaction)
            table_name: Name of the table

        Returns::

            bool: True if successful, False otherwise
        """
        return self.__class__.drop_index_in_transaction(connection, table_name, self.name)


class CreateVectorIndex(DDLElement):
    """DDL element for creating vector indexes."""

    def __init__(self, index: VectorIndex, if_not_exists: bool = False):
        self.index = index
        self.if_not_exists = if_not_exists


@compiles(CreateVectorIndex)
def compile_create_vector_index(element: CreateVectorIndex, compiler, **kw):
    """Compile CREATE VECTOR INDEX statement."""
    index = element.index

    # Use the stored column name
    column_name = index._column_name

    sql_parts = ["CREATE INDEX"]

    if element.if_not_exists:
        sql_parts.append("IF NOT EXISTS")

    sql_parts.append(f"{index.name} USING {index.index_type} ON {index.table.name}({column_name})")

    # Add parameters based on index type
    if index.index_type == VectorIndexType.IVFFLAT and index.lists is not None:
        sql_parts.append(f"lists = {index.lists}")
    elif index.index_type == VectorIndexType.HNSW:
        # Add HNSW parameters
        if index.m is not None:
            sql_parts.append(f"M {index.m}")
        if index.ef_construction is not None:
            sql_parts.append(f"EF_CONSTRUCTION {index.ef_construction}")
        if index.ef_search is not None:
            sql_parts.append(f"EF_SEARCH {index.ef_search}")

    # Add operation type
    sql_parts.append(f"op_type '{index.op_type}'")

    return " ".join(sql_parts)


@compiles(SQLAlchemyCreateIndex, "matrixone")
def compile_create_vector_index_matrixone(element: SQLAlchemyCreateIndex, compiler, **kw):
    """Compile CREATE INDEX for VectorIndex on MatrixOne dialect."""
    index = element.element

    # Check if this is a VectorIndex
    if isinstance(index, VectorIndex):
        # Use the stored column name
        column_name = index._column_name

        sql_parts = ["CREATE INDEX"]

        if element.if_not_exists:
            sql_parts.append("IF NOT EXISTS")

        sql_parts.append(f"{index.name} USING {index.index_type} ON {index.table.name}({column_name})")

        # Add parameters based on index type
        if index.index_type == VectorIndexType.IVFFLAT and index.lists is not None:
            sql_parts.append(f"lists = {index.lists}")
        elif index.index_type == VectorIndexType.HNSW:
            # Add HNSW parameters
            if index.m is not None:
                sql_parts.append(f"M {index.m}")
            if index.ef_construction is not None:
                sql_parts.append(f"EF_CONSTRUCTION {index.ef_construction}")
            if index.ef_search is not None:
                sql_parts.append(f"EF_SEARCH {index.ef_search}")

        # Add operation type
        sql_parts.append(f"op_type '{index.op_type}'")

        return " ".join(sql_parts)
    else:
        # Fall back to default compilation
        return compiler.visit_create_index(element, **kw)


@compiles(SQLAlchemyCreateIndex, "mysql")
def compile_create_vector_index_mysql(element: SQLAlchemyCreateIndex, compiler, **kw):
    """Compile CREATE INDEX for VectorIndex on MySQL dialect."""
    index = element.element

    # Check if this is a VectorIndex
    if isinstance(index, VectorIndex):
        # Use the stored column name
        column_name = index._column_name

        sql_parts = ["CREATE INDEX"]

        if element.if_not_exists:
            sql_parts.append("IF NOT EXISTS")

        sql_parts.append(f"{index.name} USING {index.index_type} ON {index.table.name}({column_name})")

        # Add parameters based on index type
        if index.index_type == VectorIndexType.IVFFLAT and index.lists is not None:
            sql_parts.append(f"lists = {index.lists}")
        elif index.index_type == VectorIndexType.HNSW:
            # Add HNSW parameters
            if index.m is not None:
                sql_parts.append(f"M {index.m}")
            if index.ef_construction is not None:
                sql_parts.append(f"EF_CONSTRUCTION {index.ef_construction}")
            if index.ef_search is not None:
                sql_parts.append(f"EF_SEARCH {index.ef_search}")

        # Add operation type
        sql_parts.append(f"op_type '{index.op_type}'")

        return " ".join(sql_parts)
    else:
        # Fall back to default MySQL index compilation
        return compiler.visit_create_index(element, **kw)


def create_vector_index(
    name: str,
    column: Union[str, Column],
    index_type: str = VectorIndexType.IVFFLAT,
    lists: Optional[int] = None,
    op_type: str = VectorOpType.VECTOR_L2_OPS,
    # HNSW parameters
    m: Optional[int] = None,
    ef_construction: Optional[int] = None,
    ef_search: Optional[int] = None,
    **kwargs,
) -> VectorIndex:
    """
    Create a vector index.

    Args::

        name: Index name
        column: Vector column to index
        index_type: Type of vector index (ivfflat, hnsw, etc.)
        lists: Number of lists for IVFFLAT (optional)
        op_type: Vector operation type
        m: Number of bi-directional links for HNSW (optional)
        ef_construction: Size of dynamic candidate list for HNSW construction (optional)
        ef_search: Size of dynamic candidate list for HNSW search (optional)
        **kwargs: Additional index parameters

    Returns::

        VectorIndex instance

    Example
        # Create IVFFLAT index with 256 lists
        idx = create_vector_index(
        "idx_vector_l2",
        "embedding",
        index_type="ivfflat",
        lists=256,
        op_type="vector_l2_ops"
        )

        # Create HNSW index with custom parameters
        idx = create_vector_index(
        "idx_vector_hnsw",
        "embedding",
        index_type="hnsw",
        m=48,
        ef_construction=64,
        ef_search=64,
        op_type="vector_l2_ops"
        )
    """
    return VectorIndex(
        name=name,
        column=column,
        index_type=index_type,
        lists=lists,
        op_type=op_type,
        m=m,
        ef_construction=ef_construction,
        ef_search=ef_search,
        **kwargs,
    )


def create_ivfflat_index(
    name: str,
    column: Union[str, Column],
    lists: int = 256,
    op_type: str = VectorOpType.VECTOR_L2_OPS,
    **kwargs,
) -> VectorIndex:
    """
    Create an IVFFLAT vector index.

    Args::

        name: Index name
        column: Vector column to index
        lists: Number of lists (default: 256)
        op_type: Vector operation type (default: vector_l2_ops)
        **kwargs: Additional index parameters

    Returns::

        VectorIndex instance

    Example
        # Create IVFFLAT index with 256 lists for L2 distance
        idx = create_ivfflat_index("idx_embedding_l2", "embedding", lists=256)

        # Create IVFFLAT index with 128 lists for cosine similarity
        idx = create_ivfflat_index(
        "idx_embedding_cosine",
        "embedding",
        lists=128,
        op_type="vector_cosine_ops"
        )
    """
    return create_vector_index(
        name=name,
        column=column,
        index_type=VectorIndexType.IVFFLAT,
        lists=lists,
        op_type=op_type,
        **kwargs,
    )


def create_hnsw_index(
    name: str,
    column: Union[str, Column],
    m: int = 48,
    ef_construction: int = 64,
    ef_search: int = 64,
    op_type: str = VectorOpType.VECTOR_L2_OPS,
    **kwargs,
) -> VectorIndex:
    """
    Create an HNSW vector index.

    Args::

        name: Index name
        column: Vector column to index
        m: Number of bi-directional links (default: 48)
        ef_construction: Size of dynamic candidate list for construction (default: 64)
        ef_search: Size of dynamic candidate list for search (default: 64)
        op_type: Vector operation type (default: vector_l2_ops)
        **kwargs: Additional index parameters

    Returns::

        VectorIndex instance

    Example
        # Create HNSW index with default parameters
        idx = create_hnsw_index("idx_embedding_hnsw", "embedding")

        # Create HNSW index with custom parameters
        idx = create_hnsw_index(
        "idx_embedding_hnsw_custom",
        "embedding",
        m=32,
        ef_construction=128,
        ef_search=128,
        op_type="vector_cosine_ops"
        )
    """
    return create_vector_index(
        name=name,
        column=column,
        index_type=VectorIndexType.HNSW,
        m=m,
        ef_construction=ef_construction,
        ef_search=ef_search,
        op_type=op_type,
        **kwargs,
    )


class VectorIndexBuilder:
    """
    Builder class for creating vector indexes with different configurations.
    """

    def __init__(self, column: Union[str, Column]):
        """
        Initialize VectorIndexBuilder.

        Args::

            column: Vector column to index
        """
        self.column = column
        self._indexes = []

    def ivfflat(
        self, name: str, lists: int = 256, op_type: str = VectorOpType.VECTOR_L2_OPS, **kwargs
    ) -> "VectorIndexBuilder":
        """
        Add an IVFFLAT index.

        Args::

            name: Index name
            lists: Number of lists
            op_type: Vector operation type
            **kwargs: Additional parameters

        Returns::

            Self for method chaining
        """
        index = create_ivfflat_index(name, self.column, lists, op_type, **kwargs)
        self._indexes.append(index)
        return self

    def l2_index(self, name: str, lists: int = 256, **kwargs) -> "VectorIndexBuilder":
        """
        Add an L2 distance index.

        Args::

            name: Index name
            lists: Number of lists for IVFFLAT
            **kwargs: Additional parameters

        Returns::

            Self for method chaining
        """
        return self.ivfflat(name, lists, VectorOpType.VECTOR_L2_OPS, **kwargs)

    def cosine_index(self, name: str, lists: int = 256, **kwargs) -> "VectorIndexBuilder":
        """
        Add a cosine similarity index.

        Args::

            name: Index name
            lists: Number of lists for IVFFLAT
            **kwargs: Additional parameters

        Returns::

            Self for method chaining
        """
        return self.ivfflat(name, lists, VectorOpType.VECTOR_COSINE_OPS, **kwargs)

    def ip_index(self, name: str, lists: int = 256, **kwargs) -> "VectorIndexBuilder":
        """
        Add an inner product index.

        Args::

            name: Index name
            lists: Number of lists for IVFFLAT
            **kwargs: Additional parameters

        Returns::

            Self for method chaining
        """
        return self.ivfflat(name, lists, VectorOpType.VECTOR_IP_OPS, **kwargs)

    def hnsw(
        self,
        name: str,
        m: int = 48,
        ef_construction: int = 64,
        ef_search: int = 64,
        op_type: str = VectorOpType.VECTOR_L2_OPS,
        **kwargs,
    ) -> "VectorIndexBuilder":
        """
        Add an HNSW index.

        Args::

            name: Index name
            m: Number of bi-directional links
            ef_construction: Size of dynamic candidate list for construction
            ef_search: Size of dynamic candidate list for search
            op_type: Vector operation type
            **kwargs: Additional parameters

        Returns::

            Self for method chaining
        """
        index = create_hnsw_index(name, self.column, m, ef_construction, ef_search, op_type, **kwargs)
        self._indexes.append(index)
        return self

    def hnsw_l2_index(
        self,
        name: str,
        m: int = 48,
        ef_construction: int = 64,
        ef_search: int = 64,
        **kwargs,
    ) -> "VectorIndexBuilder":
        """
        Add an HNSW L2 distance index.

        Args::

            name: Index name
            m: Number of bi-directional links
            ef_construction: Size of dynamic candidate list for construction
            ef_search: Size of dynamic candidate list for search
            **kwargs: Additional parameters

        Returns::

            Self for method chaining
        """
        return self.hnsw(name, m, ef_construction, ef_search, VectorOpType.VECTOR_L2_OPS, **kwargs)

    def hnsw_cosine_index(
        self,
        name: str,
        m: int = 48,
        ef_construction: int = 64,
        ef_search: int = 64,
        **kwargs,
    ) -> "VectorIndexBuilder":
        """
        Add an HNSW cosine similarity index.

        Args::

            name: Index name
            m: Number of bi-directional links
            ef_construction: Size of dynamic candidate list for construction
            ef_search: Size of dynamic candidate list for search
            **kwargs: Additional parameters

        Returns::

            Self for method chaining
        """
        return self.hnsw(name, m, ef_construction, ef_search, VectorOpType.VECTOR_COSINE_OPS, **kwargs)

    def hnsw_ip_index(
        self,
        name: str,
        m: int = 48,
        ef_construction: int = 64,
        ef_search: int = 64,
        **kwargs,
    ) -> "VectorIndexBuilder":
        """
        Add an HNSW inner product index.

        Args::

            name: Index name
            m: Number of bi-directional links
            ef_construction: Size of dynamic candidate list for construction
            ef_search: Size of dynamic candidate list for search
            **kwargs: Additional parameters

        Returns::

            Self for method chaining
        """
        return self.hnsw(name, m, ef_construction, ef_search, VectorOpType.VECTOR_IP_OPS, **kwargs)

    def build(self) -> List[VectorIndex]:
        """
        Build and return the list of vector indexes.

        Returns::

            List of VectorIndex instances
        """
        return self._indexes.copy()

    def add_to_table(self, table) -> "VectorIndexBuilder":
        """
        Add indexes to a table.

        Args::

            table: SQLAlchemy Table instance

        Returns::

            Self for method chaining
        """
        for index in self._indexes:
            index.table = table
            table.indexes.add(index)
        return self


def vector_index_builder(column: Union[str, Column]) -> VectorIndexBuilder:
    """
    Create a VectorIndexBuilder for a column.

    Args::

        column: Vector column to index

    Returns::

        VectorIndexBuilder instance

    Example
        # Create multiple indexes for a vector column
        indexes = vector_index_builder("embedding") \
        .l2_index("idx_l2", lists=256) \
        .cosine_index("idx_cosine", lists=128) \
        .build()
    """
    return VectorIndexBuilder(column)
