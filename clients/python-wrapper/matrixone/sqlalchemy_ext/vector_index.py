"""
Vector index support for SQLAlchemy integration with MatrixOne.
"""

from typing import List, Optional, Union

from sqlalchemy import Column, Index, text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql.ddl import CreateIndex as SQLAlchemyCreateIndex


class VectorIndexType:
    """Enum-like class for vector index types."""

    IVFFLAT = "ivfflat"
    HNSW = "hnsw"  # Future support


class VectorOpType:
    """Enum-like class for vector operation types."""

    VECTOR_L2_OPS = "vector_l2_ops"
    VECTOR_IP_OPS = "vector_ip_ops"
    VECTOR_COSINE_OPS = "vector_cosine_ops"


class VectorIndex(Index):
    """
    SQLAlchemy Index for vector columns with MatrixOne-specific syntax.

    Supports creating vector indexes with various algorithms and operation types.
    """

    def __init__(
        self,
        name: str,
        column: Union[str, Column],
        index_type: str = VectorIndexType.IVFFLAT,
        lists: Optional[int] = None,
        op_type: str = VectorOpType.VECTOR_L2_OPS,
        **kwargs,
    ):
        """
        Initialize VectorIndex.

        Args:
            name: Index name
            column: Vector column to index
            index_type: Type of vector index (ivfflat, hnsw, etc.)
            lists: Number of lists for IVFFLAT (optional)
            op_type: Vector operation type
            **kwargs: Additional index parameters
        """
        self.index_type = index_type
        self.lists = lists
        self.op_type = op_type

        # Store column name for later use
        self._column_name = str(column) if not isinstance(column, str) else column

        # Call parent constructor first
        super().__init__(name, column, **kwargs)

        # Set dialect options after initialization to bind to matrixone dialect
        self.dialect_options["matrixone"] = {"length": None, "using": None}
        # Also provide mysql fallback for compatibility
        self.dialect_options["mysql"] = {"length": None, "using": None}

    def _create_index_sql(self, table_name: str) -> str:
        """Generate the CREATE INDEX SQL for vector index."""
        # For simplicity, we'll use the column name passed during initialization
        # This should be stored as a string in most cases
        column_name = self._column_name

        sql_parts = [f"CREATE INDEX {self.name} USING {self.index_type} ON {table_name}({column_name})"]

        # Add lists parameter for IVFFLAT
        if self.index_type == VectorIndexType.IVFFLAT and self.lists is not None:
            sql_parts.append(f"lists = {self.lists}")

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
        **kwargs,
    ) -> bool:
        """
        Create a vector index using ORM-style method.

        Args:
            engine: SQLAlchemy engine
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            index_type: Type of vector index (ivfflat, hnsw, etc.)
            lists: Number of lists for IVFFLAT (optional)
            op_type: Vector operation type
            **kwargs: Additional index parameters

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            index = cls(name, column, index_type, lists, op_type, **kwargs)
            sql = index.create_sql(table_name)

            with engine.begin() as conn:
                conn.execute(text(sql))
            return True
        except Exception as e:
            print(f"Failed to create vector index: {e}")
            return False

    @classmethod
    def drop_index(cls, engine, table_name: str, name: str) -> bool:
        """
        Drop a vector index using ORM-style method.

        Args:
            engine: SQLAlchemy engine
            table_name: Name of the table
            name: Name of the index to drop

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            sql = f"DROP INDEX {name} ON {table_name}"
            with engine.begin() as conn:
                conn.execute(text(sql))
            return True
        except Exception as e:
            print(f"Failed to drop vector index: {e}")
            return False

    def create(self, engine, table_name: str) -> bool:
        """
        Create this vector index using ORM-style method.

        Args:
            engine: SQLAlchemy engine
            table_name: Name of the table

        Returns:
            bool: True if successful, False otherwise
        """
        return self.__class__.create_index(
            engine, table_name, self.name, self._column_name, self.index_type, self.lists, self.op_type
        )

    def drop(self, engine, table_name: str) -> bool:
        """
        Drop this vector index using ORM-style method.

        Args:
            engine: SQLAlchemy engine
            table_name: Name of the table

        Returns:
            bool: True if successful, False otherwise
        """
        return self.__class__.drop_index(engine, table_name, self.name)


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

    # Add lists parameter for IVFFLAT
    if index.index_type == VectorIndexType.IVFFLAT and index.lists is not None:
        sql_parts.append(f"lists = {index.lists}")

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

        # Add lists parameter for IVFFLAT
        if index.index_type == VectorIndexType.IVFFLAT and index.lists is not None:
            sql_parts.append(f"lists = {index.lists}")

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

        # Add lists parameter for IVFFLAT
        if index.index_type == VectorIndexType.IVFFLAT and index.lists is not None:
            sql_parts.append(f"lists = {index.lists}")

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
    **kwargs,
) -> VectorIndex:
    """
    Create a vector index.

    Args:
        name: Index name
        column: Vector column to index
        index_type: Type of vector index (ivfflat, hnsw, etc.)
        lists: Number of lists for IVFFLAT (optional)
        op_type: Vector operation type
        **kwargs: Additional index parameters

    Returns:
        VectorIndex instance

    Example:
        # Create IVFFLAT index with 256 lists
        idx = create_vector_index(
            "idx_vector_l2",
            "embedding",
            index_type="ivfflat",
            lists=256,
            op_type="vector_l2_ops"
        )
    """
    return VectorIndex(name=name, column=column, index_type=index_type, lists=lists, op_type=op_type, **kwargs)


def create_ivfflat_index(
    name: str, column: Union[str, Column], lists: int = 256, op_type: str = VectorOpType.VECTOR_L2_OPS, **kwargs
) -> VectorIndex:
    """
    Create an IVFFLAT vector index.

    Args:
        name: Index name
        column: Vector column to index
        lists: Number of lists (default: 256)
        op_type: Vector operation type (default: vector_l2_ops)
        **kwargs: Additional index parameters

    Returns:
        VectorIndex instance

    Example:
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
        name=name, column=column, index_type=VectorIndexType.IVFFLAT, lists=lists, op_type=op_type, **kwargs
    )


class VectorIndexBuilder:
    """
    Builder class for creating vector indexes with different configurations.
    """

    def __init__(self, column: Union[str, Column]):
        """
        Initialize VectorIndexBuilder.

        Args:
            column: Vector column to index
        """
        self.column = column
        self._indexes = []

    def ivfflat(
        self, name: str, lists: int = 256, op_type: str = VectorOpType.VECTOR_L2_OPS, **kwargs
    ) -> "VectorIndexBuilder":
        """
        Add an IVFFLAT index.

        Args:
            name: Index name
            lists: Number of lists
            op_type: Vector operation type
            **kwargs: Additional parameters

        Returns:
            Self for method chaining
        """
        index = create_ivfflat_index(name, self.column, lists, op_type, **kwargs)
        self._indexes.append(index)
        return self

    def l2_index(self, name: str, lists: int = 256, **kwargs) -> "VectorIndexBuilder":
        """
        Add an L2 distance index.

        Args:
            name: Index name
            lists: Number of lists for IVFFLAT
            **kwargs: Additional parameters

        Returns:
            Self for method chaining
        """
        return self.ivfflat(name, lists, VectorOpType.VECTOR_L2_OPS, **kwargs)

    def cosine_index(self, name: str, lists: int = 256, **kwargs) -> "VectorIndexBuilder":
        """
        Add a cosine similarity index.

        Args:
            name: Index name
            lists: Number of lists for IVFFLAT
            **kwargs: Additional parameters

        Returns:
            Self for method chaining
        """
        return self.ivfflat(name, lists, VectorOpType.VECTOR_COSINE_OPS, **kwargs)

    def ip_index(self, name: str, lists: int = 256, **kwargs) -> "VectorIndexBuilder":
        """
        Add an inner product index.

        Args:
            name: Index name
            lists: Number of lists for IVFFLAT
            **kwargs: Additional parameters

        Returns:
            Self for method chaining
        """
        return self.ivfflat(name, lists, VectorOpType.VECTOR_IP_OPS, **kwargs)

    def build(self) -> List[VectorIndex]:
        """
        Build and return the list of vector indexes.

        Returns:
            List of VectorIndex instances
        """
        return self._indexes.copy()

    def add_to_table(self, table) -> "VectorIndexBuilder":
        """
        Add indexes to a table.

        Args:
            table: SQLAlchemy Table instance

        Returns:
            Self for method chaining
        """
        for index in self._indexes:
            index.table = table
            table.indexes.add(index)
        return self


def vector_index_builder(column: Union[str, Column]) -> VectorIndexBuilder:
    """
    Create a VectorIndexBuilder for a column.

    Args:
        column: Vector column to index

    Returns:
        VectorIndexBuilder instance

    Example:
        # Create multiple indexes for a vector column
        indexes = vector_index_builder("embedding") \
            .l2_index("idx_l2", lists=256) \
            .cosine_index("idx_cosine", lists=128) \
            .build()
    """
    return VectorIndexBuilder(column)
