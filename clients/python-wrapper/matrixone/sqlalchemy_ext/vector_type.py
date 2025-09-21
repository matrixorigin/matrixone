"""
Vector type for SQLAlchemy integration with MatrixOne.
"""

from typing import Any, Optional

from sqlalchemy import Text, TypeDecorator
from sqlalchemy.dialects import mysql
from sqlalchemy.types import UserDefinedType


class VectorType(UserDefinedType):
    """
    SQLAlchemy type for MatrixOne vector columns.

    This type represents vector data in MatrixOne database and provides
    proper serialization/deserialization for SQLAlchemy operations.
    """

    __visit_name__ = "VECTOR"

    def __init__(self, dimension: Optional[int] = None, precision: str = "f32"):
        """
        Initialize VectorType.

        Args:
            dimension: Vector dimension (optional)
            precision: Vector precision - "f32" for vecf32, "f64" for vecf64
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
        self.precision = "f32"
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
            return VectorType(dimension=self.dimension, precision="f32")
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
        self.precision = "f64"
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
            return VectorType(dimension=self.dimension, precision="f64")
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

    def __init__(self, dimension: Optional[int] = None, precision: str = "f32", **kwargs):
        """
        Initialize VectorTypeDecorator.

        Args:
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
