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
Vector type for SQLAlchemy integration with MatrixOne.
"""

from typing import Any, List, Optional, Union

from sqlalchemy import Column, Text, TypeDecorator, func
from sqlalchemy.dialects import mysql
from sqlalchemy.types import UserDefinedType


class VectorPrecision:
    """Enum-like class for vector precision types."""

    F32 = "f32"
    F64 = "f64"


class VectorType(UserDefinedType):
    """
    SQLAlchemy type for MatrixOne vector columns.

    This type represents vector data in MatrixOne database and provides
    proper serialization/deserialization for SQLAlchemy operations.
    It supports both vecf32 and vecf64 precision types with configurable dimensions.

    Key Features:

    - Support for both 32-bit (vecf32) and 64-bit (vecf64) vector precision
    - Configurable vector dimensions
    - Automatic serialization/deserialization of vector data
    - Integration with MatrixOne's vector indexing and search capabilities
    - Support for vector similarity operations

    Usage
        # Define vector columns in SQLAlchemy models
        class Document(Base):
        __tablename__ = 'documents'
        id = Column(Integer, primary_key=True)
        content = Column(Text)
        embedding = Column(VectorType(384, VectorPrecision.F32))  # 384-dim f32 vector
        embedding_64 = Column(VectorType(512, VectorPrecision.F64))  # 512-dim f64 vector

        # Use in table creation
        client.create_table_orm('documents',
        Column('id', Integer, primary_key=True),
        Column('content', Text),
        Column('embedding', VectorType(384, VectorPrecision.F32))
        )

    Supported Operations:

    - Vector similarity search using distance functions
    - Vector indexing with HNSW and IVF algorithms
    - Vector arithmetic operations
    - Integration with fulltext search capabilities

    Note: Vector dimensions and precision must match the requirements of your
    vector indexing strategy and embedding model.
    """

    __visit_name__ = "VECTOR"

    def __init__(self, dimension: Optional[int] = None, precision: str = VectorPrecision.F32):
        """
        Initialize VectorType.

        Args::

            dimension: Vector dimension (optional)
            precision: Vector precision - VectorPrecision.F32 for vecf32, VectorPrecision.F64 for vecf64
        """
        self.dimension = dimension
        self.precision = precision

    def get_col_spec(self, **kw: Any) -> str:
        """Return the column specification for this type."""
        if self.dimension is not None:
            return f"vec{self.precision}({self.dimension})"
        else:
            return f"vec{self.precision}"

    def bind_processor(self, dialect):
        """Return a conversion function for processing bind values."""

        def process(value):
            if value is None:
                return None
            if isinstance(value, str):
                return value
            if isinstance(value, list):
                # Convert list to MatrixOne vector format
                return "[" + ",".join(map(str, value)) + "]"
            return str(value)

        return process

    def process_bind_param(self, value, dialect):
        """Process the value before binding to the database."""
        if value is None:
            return None
        if isinstance(value, list):
            # Convert list to MatrixOne vector format
            return "[" + ",".join(map(str, value)) + "]"
        if isinstance(value, str):
            return value
        return str(value)

    def result_processor(self, dialect, coltype):
        """Return a conversion function for processing result values."""

        def process(value):
            if value is None:
                return None
            if isinstance(value, str):
                # Parse MatrixOne vector format back to list
                try:
                    # Remove brackets and split by comma
                    clean_value = value.strip("[]")
                    if clean_value:
                        return [float(x.strip()) for x in clean_value.split(",")]
                    else:
                        return []
                except (ValueError, AttributeError):
                    return value
            return value

        return process

    def __str__(self):
        """Return the column specification for this type."""
        return self.get_col_spec()

    def __repr__(self):
        if self.dimension:
            return f"VectorType(dimension={self.dimension}, precision='{self.precision}')"
        else:
            return f"VectorType(precision='{self.precision}')"


class Vectorf32(TypeDecorator):
    """Convenience class for 32-bit float vectors using TypeDecorator."""

    impl = Text
    cache_ok = True

    def __init__(self, dimension: Optional[int] = None):
        self.dimension = dimension
        self.precision = VectorPrecision.F32
        super().__init__()

    def get_col_spec(self, **kw):
        """Return the column specification for this type."""
        if self.dimension is not None:
            return f"vecf32({self.dimension})"
        else:
            return "vecf32"

    def load_dialect_impl(self, dialect):
        """Return the appropriate type for the given dialect."""
        # For SQL generation, return our custom type
        if hasattr(dialect, "name") and dialect.name == "matrixone":
            return VectorType(dimension=self.dimension, precision=VectorPrecision.F32)
        return self.impl

    def process_bind_param(self, value, dialect):
        """Process the value before binding to the database."""
        if value is None:
            return None
        if isinstance(value, list):
            # Convert list to MatrixOne vector format
            return "[" + ",".join(map(str, value)) + "]"
        if isinstance(value, str):
            return value
        return str(value)

    def process_result_value(self, value, dialect):
        """Process the value after retrieving from the database."""
        if value is None:
            return None
        if isinstance(value, str):
            try:
                clean_value = value.strip("[]")
                if clean_value:
                    return [float(x.strip()) for x in clean_value.split(",")]
                else:
                    return []
            except (ValueError, AttributeError):
                return value
        return value

    def bind_processor(self, dialect):
        """Return a conversion function for processing bind values."""

        def process(value):
            return self.process_bind_param(value, dialect)

        return process

    def result_processor(self, dialect, coltype):
        """Return a conversion function for processing result values."""

        def process(value):
            return self.process_result_value(value, dialect)

        return process

    def __str__(self):
        """Return the column specification for this type."""
        return self.get_col_spec()

    def __repr__(self):
        if self.dimension:
            return f"Vectorf32(dimension={self.dimension})"
        else:
            return "Vectorf32()"


class Vectorf64(TypeDecorator):
    """Convenience class for 64-bit float vectors using TypeDecorator."""

    impl = Text
    cache_ok = True

    def __init__(self, dimension: Optional[int] = None):
        self.dimension = dimension
        self.precision = VectorPrecision.F64
        super().__init__()

    def get_col_spec(self, **kw):
        """Return the column specification for this type."""
        if self.dimension is not None:
            return f"vecf64({self.dimension})"
        else:
            return "vecf64"

    def load_dialect_impl(self, dialect):
        """Return the appropriate type for the given dialect."""
        # For SQL generation, return our custom type
        if hasattr(dialect, "name") and dialect.name == "matrixone":
            return VectorType(dimension=self.dimension, precision=VectorPrecision.F64)
        return self.impl

    def process_bind_param(self, value, dialect):
        """Process the value before binding to the database."""
        if value is None:
            return None
        if isinstance(value, list):
            # Convert list to MatrixOne vector format
            return "[" + ",".join(map(str, value)) + "]"
        if isinstance(value, str):
            return value
        return str(value)

    def process_result_value(self, value, dialect):
        """Process the value after retrieving from the database."""
        if value is None:
            return None
        if isinstance(value, str):
            try:
                clean_value = value.strip("[]")
                if clean_value:
                    return [float(x.strip()) for x in clean_value.split(",")]
                else:
                    return []
            except (ValueError, AttributeError):
                return value
        return value

    def bind_processor(self, dialect):
        """Return a conversion function for processing bind values."""

        def process(value):
            return self.process_bind_param(value, dialect)

        return process

    def result_processor(self, dialect, coltype):
        """Return a conversion function for processing result values."""

        def process(value):
            return self.process_result_value(value, dialect)

        return process

    def __str__(self):
        """Return the column specification for this type."""
        return self.get_col_spec()

    def __repr__(self):
        if self.dimension:
            return f"Vectorf64(dimension={self.dimension})"
        else:
            return "Vectorf64()"


class VectorTypeDecorator(TypeDecorator):
    """
    A decorator that allows VectorType to work seamlessly with SQLAlchemy.

    This provides better integration with SQLAlchemy's type system and
    allows for more flexible usage in models.
    """

    impl = Text  # Use TEXT type for large vector storage
    cache_ok = True

    def __init__(self, dimension: Optional[int] = None, precision: str = VectorPrecision.F32, **kwargs):
        """
        Initialize VectorTypeDecorator.

        Args::

            dimension: Vector dimension
            precision: Vector precision
            **kwargs: Additional arguments passed to TypeDecorator
        """
        self.dimension = dimension
        self.precision = precision
        super().__init__(**kwargs)

    def load_dialect_impl(self, dialect):
        """Return the appropriate type for the given dialect."""
        if dialect.name == "mysql":
            return mysql.TEXT  # Use TEXT for large vector data
        return self.impl

    def process_bind_param(self, value, dialect):
        """Process the value before binding to the database."""
        if value is None:
            return None
        if isinstance(value, list):
            # Convert list to MatrixOne vector format
            return "[" + ",".join(map(str, value)) + "]"
        if isinstance(value, str):
            return value
        return str(value)

    def process_result_value(self, value, dialect):
        """Process the value after retrieving from the database."""
        if value is None:
            return None
        if isinstance(value, str):
            # Parse MatrixOne vector format back to list
            try:
                # Remove brackets and split by comma
                clean_value = value.strip("[]")
                if clean_value:
                    return [float(x.strip()) for x in clean_value.split(",")]
                else:
                    return []
            except (ValueError, AttributeError):
                return value
        return value

    def __repr__(self):
        if self.dimension:
            return f"VectorTypeDecorator(dimension={self.dimension}, precision='{self.precision}')"
        else:
            return f"VectorTypeDecorator(precision='{self.precision}')"


class VectorColumn(Column):
    """
    Extended Column class with vector distance functions.

    Provides convenient methods for vector similarity operations.
    """

    inherit_cache = True

    def l2_distance(self, other: Union[List[float], str, Column]) -> func:
        """
            Calculate L2 (Euclidean) distance between vectors.

            Args::

                other: Target vector as list, string, or column

            Returns::

                SQLAlchemy function expression

            Example

        query = session.query(Document).filter(
                    Document.embedding.l2_distance([1, 2, 3]) < 0.5
                )
        """
        from sqlalchemy import literal

        if isinstance(other, list):
            # Convert list to MatrixOne vector format and use literal() to preserve formatting
            vector_str = "[" + ",".join(map(str, other)) + "]"
            return func.l2_distance(self, literal(vector_str))
        elif isinstance(other, str):
            return func.l2_distance(self, literal(other))
        else:
            return func.l2_distance(self, other)

    def l2_distance_sq(self, other: Union[List[float], str, Column]) -> func:
        """
            Calculate squared L2 distance between vectors.

            Args::

                other: Target vector as list, string, or column

            Returns::

                SQLAlchemy function expression

            Example

        query = session.query(Document).order_by(
                    Document.embedding.l2_distance_sq([1, 2, 3])
                )
        """
        from sqlalchemy import literal

        if isinstance(other, list):
            vector_str = "[" + ",".join(map(str, other)) + "]"
            return func.l2_distance_sq(self, literal(vector_str))
        elif isinstance(other, str):
            return func.l2_distance_sq(self, literal(other))
        else:
            return func.l2_distance_sq(self, other)

    def cosine_distance(self, other: Union[List[float], str, Column]) -> func:
        """
            Calculate cosine distance between vectors.

            Args::

                other: Target vector as list, string, or column

            Returns::

                SQLAlchemy function expression

            Example

        query = session.query(Document).filter(
                    Document.embedding.cosine_distance([1, 2, 3]) < 0.1
                )
        """
        from sqlalchemy import literal

        if isinstance(other, list):
            vector_str = "[" + ",".join(map(str, other)) + "]"
            return func.cosine_distance(self, literal(vector_str))
        elif isinstance(other, str):
            return func.cosine_distance(self, literal(other))
        else:
            return func.cosine_distance(self, other)

    def negative_inner_product(self, other: Union[List[float], str, Column]) -> func:
        """
            Calculate negative inner product between vectors.
            Note: This is implemented as -inner_product() since MatrixOne doesn't have native support.

            Args::

                other: Target vector as list, string, or column

            Returns::

                SQLAlchemy function expression

            Example

        query = session.query(Document).order_by(
                    Document.embedding.negative_inner_product([1, 2, 3])
                )
        """
        if isinstance(other, list):
            vector_str = "[" + ",".join(map(str, other)) + "]"
            return -func.inner_product(self, vector_str)
        elif isinstance(other, str):
            return -func.inner_product(self, other)
        else:
            return -func.inner_product(self, other)

    def inner_product(self, other: Union[List[float], str, Column]) -> func:
        """
            Calculate inner product (dot product) between vectors.

            Args::

                other: Target vector as list, string, or column

            Returns::

                SQLAlchemy function expression

            Example

        query = session.query(Document).order_by(
                    Document.embedding.inner_product([1, 2, 3]).desc()
                )
        """
        from sqlalchemy import literal

        if isinstance(other, list):
            vector_str = "[" + ",".join(map(str, other)) + "]"
            return func.inner_product(self, literal(vector_str))
        elif isinstance(other, str):
            return func.inner_product(self, literal(other))
        else:
            return func.inner_product(self, other)

    def similarity_search(
        self,
        other: Union[List[float], str, Column],
        distance_type: str = "l2",
        max_distance: Optional[float] = None,
    ) -> func:
        """
            Create a similarity search expression with optional distance filtering.

            Args::

                other: Target vector as list, string, or column
                distance_type: Type of distance calculation ("l2", "cosine", "inner_product")
                max_distance: Optional maximum distance threshold

            Returns::

                SQLAlchemy function expression for distance calculation

            Example

        # For ordering by similarity
                query = session.query(Document).order_by(
                    Document.embedding.similarity_search([1, 2, 3])
                )

                # For filtering by distance
                query = session.query(Document).filter(
                    Document.embedding.similarity_search([1, 2, 3], max_distance=1.0) < 1.0
                )
        """
        if distance_type == "l2":
            distance_expr = self.l2_distance(other)
        elif distance_type == "cosine":
            distance_expr = self.cosine_distance(other)
        elif distance_type == "inner_product":
            distance_expr = self.inner_product(other)
        else:
            raise ValueError(f"Unsupported distance type: {distance_type}")

        return distance_expr

    def within_distance(
        self, other: Union[List[float], str, Column], max_distance: float, distance_type: str = "l2"
    ) -> func:
        """
            Create a distance threshold filter expression.

            Args::

                other: Target vector as list, string, or column
                max_distance: Maximum distance threshold
                distance_type: Type of distance calculation ("l2", "cosine", "inner_product")

            Returns::

                SQLAlchemy boolean expression

            Example

        query = session.query(Document).filter(
                    Document.embedding.within_distance([1, 2, 3], max_distance=1.0)
                )
        """
        if distance_type == "l2":
            return self.l2_distance(other) < max_distance
        elif distance_type == "cosine":
            return self.cosine_distance(other) < max_distance
        elif distance_type == "inner_product":
            # For inner product, higher values are more similar
            return self.inner_product(other) > max_distance
        else:
            raise ValueError(f"Unsupported distance type: {distance_type}")

    def most_similar(self, other: Union[List[float], str, Column], distance_type: str = "l2", limit: int = 10) -> func:
        """
            Create an expression for finding most similar vectors.

            Args::

                other: Target vector as list, string, or column
                distance_type: Type of distance calculation ("l2", "cosine", "inner_product")
                limit: Number of results to return

            Returns::

                SQLAlchemy function expression for ordering

            Example

        query = session.query(Document).order_by(
                    Document.embedding.most_similar([1, 2, 3])
                ).limit(10)
        """
        if distance_type == "l2":
            return self.l2_distance(other)
        elif distance_type == "cosine":
            return self.cosine_distance(other)
        elif distance_type == "inner_product":
            # For inner product, we want descending order (higher is better)
            return self.inner_product(other).desc()
        else:
            raise ValueError(f"Unsupported distance type: {distance_type}")


# Independent distance functions for more flexible API
def l2_distance(column: Column, other: Union[List[float], str, Column]) -> func:
    """
    Calculate L2 (Euclidean) distance between vectors.

    This is an independent function that can be used with any column,
    providing more flexibility than the VectorColumn methods.

    Args::

        column: Vector column to calculate distance from
        other: Target vector as list, string, or column

    Returns::

        SQLAlchemy function expression

    Example
        from matrixone.sqlalchemy_ext import l2_distance

        # With list vector
        result = session.query(Document).filter(
        l2_distance(Document.embedding, [1, 2, 3]) < 0.5
        )

        # With string vector
        result = session.query(Document).filter(
        l2_distance(Document.embedding, "[1,2,3]") < 0.5
        )

        # With another column
        result = session.query(Document).filter(
        l2_distance(Document.embedding, Document.query_vector) < 0.5
        )
    """
    if isinstance(other, list):
        # Convert list to MatrixOne vector format
        vector_str = "[" + ",".join(map(str, other)) + "]"
        return func.l2_distance(column, vector_str)
    elif isinstance(other, str):
        return func.l2_distance(column, other)
    else:
        return func.l2_distance(column, other)


def l2_distance_sq(column: Column, other: Union[List[float], str, Column]) -> func:
    """
    Calculate squared L2 distance between vectors.

    Args::

        column: Vector column to calculate distance from
        other: Target vector as list, string, or column

    Returns::

        SQLAlchemy function expression

    Example
        from matrixone.sqlalchemy_ext import l2_distance_sq

        result = session.query(Document).order_by(
        l2_distance_sq(Document.embedding, [1, 2, 3])
        )
    """
    if isinstance(other, list):
        vector_str = "[" + ",".join(map(str, other)) + "]"
        return func.l2_distance_sq(column, vector_str)
    elif isinstance(other, str):
        return func.l2_distance_sq(column, other)
    else:
        return func.l2_distance_sq(column, other)


def cosine_distance(column: Column, other: Union[List[float], str, Column]) -> func:
    """
    Calculate cosine distance between vectors.

    Args::

        column: Vector column to calculate distance from
        other: Target vector as list, string, or column

    Returns::

        SQLAlchemy function expression

    Example
        from matrixone.sqlalchemy_ext import cosine_distance

        result = session.query(Document).filter(
        cosine_distance(Document.embedding, [1, 2, 3]) < 0.1
        )
    """
    if isinstance(other, list):
        vector_str = "[" + ",".join(map(str, other)) + "]"
        return func.cosine_distance(column, vector_str)
    elif isinstance(other, str):
        return func.cosine_distance(column, other)
    else:
        return func.cosine_distance(column, other)


def inner_product(column: Column, other: Union[List[float], str, Column]) -> func:
    """
    Calculate inner product (dot product) between vectors.

    Args::

        column: Vector column to calculate distance from
        other: Target vector as list, string, or column

    Returns::

        SQLAlchemy function expression

    Example
        from matrixone.sqlalchemy_ext import inner_product

        result = session.query(Document).order_by(
        inner_product(Document.embedding, [1, 2, 3]).desc()
        )
    """
    if isinstance(other, list):
        vector_str = "[" + ",".join(map(str, other)) + "]"
        return func.inner_product(column, vector_str)
    elif isinstance(other, str):
        return func.inner_product(column, other)
    else:
        return func.inner_product(column, other)


def negative_inner_product(column: Column, other: Union[List[float], str, Column]) -> func:
    """
    Calculate negative inner product between vectors.

    Note: This is implemented as -inner_product() since MatrixOne doesn't have native support.

    Args::

        column: Vector column to calculate distance from
        other: Target vector as list, string, or column

    Returns::

        SQLAlchemy function expression

    Example
        from matrixone.sqlalchemy_ext import negative_inner_product

        result = session.query(Document).order_by(
        negative_inner_product(Document.embedding, [1, 2, 3])
        )
    """
    if isinstance(other, list):
        vector_str = "[" + ",".join(map(str, other)) + "]"
        return -func.inner_product(column, vector_str)
    elif isinstance(other, str):
        return -func.inner_product(column, other)
    else:
        return -func.inner_product(column, other)


# Convenience functions for vector operations
def create_vector_column(dimension: int, precision: str = VectorPrecision.F32, **kwargs) -> VectorColumn:
    """
    Create a vector column with distance function support.

    Args::

        dimension: Vector dimension
        precision: Vector precision (VectorPrecision.F32 or VectorPrecision.F64)
        **kwargs: Additional column arguments

    Returns::

        VectorColumn instance

    Example
        class Document(Base):
        id = Column(Integer, primary_key=True)
        embedding = create_vector_column(128, precision=VectorPrecision.F32)
        description = Column(String(500))
    """
    if precision == VectorPrecision.F32:
        return VectorColumn(Vectorf32(dimension=dimension), **kwargs)
    elif precision == VectorPrecision.F64:
        return VectorColumn(Vectorf64(dimension=dimension), **kwargs)
    else:
        raise ValueError(f"Precision must be '{VectorPrecision.F32}' or '{VectorPrecision.F64}'")


def vector_similarity_search(
    column: Column,
    query_vector: Union[List[float], str, Column],
    distance_type: str = "l2",
    max_distance: Optional[float] = None,
) -> func:
    """
    Create a similarity search expression with optional distance filtering.

    This is a convenience function that combines distance calculation with filtering.

    Args::

        column: Vector column to search in
        query_vector: Query vector as list, string, or column
        distance_type: Type of distance calculation ("l2", "cosine", "inner_product")
        max_distance: Optional maximum distance threshold

    Returns::

        SQLAlchemy function expression for distance calculation

    Example
        from matrixone.sqlalchemy_ext import vector_similarity_search

        # For ordering by similarity
        result = session.query(Document).order_by(
        vector_similarity_search(Document.embedding, [1, 2, 3])
        )

        # For filtering by distance
        result = session.query(Document).filter(
        vector_similarity_search(Document.embedding, [1, 2, 3], max_distance=1.0) < 1.0
        )
    """
    if distance_type == "l2":
        distance_expr = l2_distance(column, query_vector)
    elif distance_type == "cosine":
        distance_expr = cosine_distance(column, query_vector)
    elif distance_type == "inner_product":
        distance_expr = inner_product(column, query_vector)
    else:
        raise ValueError(f"Unsupported distance type: {distance_type}")

    return distance_expr


def within_distance(
    column: Column,
    query_vector: Union[List[float], str, Column],
    max_distance: float,
    distance_type: str = "l2",
) -> func:
    """
    Create a distance threshold filter expression.

    Args::

        column: Vector column to filter
        query_vector: Query vector as list, string, or column
        max_distance: Maximum distance threshold
        distance_type: Type of distance calculation ("l2", "cosine", "inner_product")

    Returns::

        SQLAlchemy boolean expression

    Example
        from matrixone.sqlalchemy_ext import within_distance

        result = session.query(Document).filter(
        within_distance(Document.embedding, [1, 2, 3], max_distance=1.0)
        )
    """
    if distance_type == "l2":
        return l2_distance(column, query_vector) < max_distance
    elif distance_type == "cosine":
        return cosine_distance(column, query_vector) < max_distance
    elif distance_type == "inner_product":
        # For inner product, higher values are more similar
        return inner_product(column, query_vector) > max_distance
    else:
        raise ValueError(f"Unsupported distance type: {distance_type}")


def most_similar(column: Column, query_vector: Union[List[float], str, Column], distance_type: str = "l2") -> func:
    """
    Create an expression for finding most similar vectors.

    Args::

        column: Vector column to search in
        query_vector: Query vector as list, string, or column
        distance_type: Type of distance calculation ("l2", "cosine", "inner_product")

    Returns::

        SQLAlchemy function expression for ordering

    Example
        from matrixone.sqlalchemy_ext import most_similar

        result = session.query(Document).order_by(
        most_similar(Document.embedding, [1, 2, 3])
        ).limit(10)
    """
    if distance_type == "l2":
        return l2_distance(column, query_vector)
    elif distance_type == "cosine":
        return cosine_distance(column, query_vector)
    elif distance_type == "inner_product":
        # For inner product, we want descending order (higher is better)
        return inner_product(column, query_vector).desc()
    else:
        raise ValueError(f"Unsupported distance type: {distance_type}")


def vector_distance_functions():
    """
    Return available vector distance functions.

    Returns::

        List of function names
    """
    return ["l2_distance", "l2_distance_sq", "cosine_distance"]
