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
Table builder utilities for MatrixOne vector tables.
"""

from typing import List, Optional, Union

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Table,
    Text,
    Time,
)
from sqlalchemy.dialects.mysql import BLOB, JSON, LONGBLOB, MEDIUMBLOB, TINYBLOB, TINYINT, VARBINARY

from .vector_type import Vectorf32, Vectorf64, VectorPrecision, VectorType


class VectorTableBuilder:
    """
    Builder class for creating MatrixOne vector tables with SQLAlchemy.

    This class provides a fluent interface for building vector tables with
    proper column definitions, indexes, and constraints. It's designed to
    work seamlessly with MatrixOne's vector capabilities and SQLAlchemy.

    Key Features:

    - Fluent method chaining for table definition
    - Support for all MatrixOne column types including vectors
    - Automatic vector index creation
    - Constraint and foreign key support
    - Integration with SQLAlchemy metadata

    Supported Column Types:
    - Standard types: Integer, String, Text, DateTime, etc.
    - Vector types: Vectorf32, Vectorf64 with configurable dimensions
    - MatrixOne-specific types: JSON, BLOB variants

    Usage Examples

    .. code-block:: python

        # Create a simple vector table
        builder = VectorTableBuilder('documents')
        table = (builder
                .add_int_column('id', primary_key=True)
                .add_string_column('title', length=255)
                .add_text_column('content')
                .add_vector_column('embedding', Vectorf32(384))
                .build())

        # Create a complex table with indexes
        builder = VectorTableBuilder('products')
        table = (builder
                .add_bigint_column('id', primary_key=True)
                .add_string_column('name', length=100)
                .add_numeric_column('price', precision=10, scale=2)
                .add_vector_column('features', Vectorf64(512))
                .add_vector_index('idx_features', 'features', 'ivfflat', lists=100)
                .build())

    Note: This builder is primarily used internally by the Client's table
    creation methods, but can be used directly for advanced use cases.
    """

    def __init__(self, table_name: str, metadata: MetaData = None):
        """
        Initialize the table builder.

        Args:

            table_name: Name of the table to create
            metadata: SQLAlchemy metadata object
        """
        self.table_name = table_name
        self.metadata = metadata or MetaData()
        self.columns = []
        self.indexes = []
        self.constraints = []

    def add_column(self, name: str, type_, **kwargs):
        """Add a column to the table."""
        column = Column(name, type_, **kwargs)
        self.columns.append(column)
        return self

    def add_int_column(self, name: str, primary_key: bool = False, **kwargs):
        """Add an integer column."""
        if primary_key:
            kwargs["primary_key"] = True
        return self.add_column(name, Integer, **kwargs)

    def add_bigint_column(self, name: str, primary_key: bool = False, **kwargs):
        """Add a bigint column."""
        if primary_key:
            kwargs["primary_key"] = True
        return self.add_column(name, BigInteger, **kwargs)

    def add_string_column(self, name: str, length: int = 255, **kwargs):
        """Add a string column."""
        return self.add_column(name, String(length), **kwargs)

    def add_text_column(self, name: str, **kwargs):
        """Add a text column."""
        return self.add_column(name, Text, **kwargs)

    def add_json_column(self, name: str, **kwargs):
        """Add a JSON column."""
        return self.add_column(name, JSON, **kwargs)

    def add_vector_column(self, name: str, dimension: int, precision: str = VectorPrecision.F32, **kwargs):
        """Add a vector column."""
        if precision == VectorPrecision.F32:
            vector_type = Vectorf32(dimension=dimension)
        elif precision == VectorPrecision.F64:
            vector_type = Vectorf64(dimension=dimension)
        else:
            vector_type = VectorType(dimension=dimension, precision=precision)

        return self.add_column(name, vector_type, **kwargs)

    def add_vecf32_column(self, name: str, dimension: int, **kwargs):
        """Add a vecf32 column."""
        return self.add_vector_column(name, dimension, VectorPrecision.F32, **kwargs)

    def add_vecf64_column(self, name: str, dimension: int, **kwargs):
        """Add a vecf64 column."""
        return self.add_vector_column(name, dimension, VectorPrecision.F64, **kwargs)

    def add_smallint_column(self, name: str, primary_key: bool = False, **kwargs):
        """Add a smallint column."""
        if primary_key:
            kwargs["primary_key"] = True
        return self.add_column(name, SmallInteger, **kwargs)

    def add_tinyint_column(self, name: str, primary_key: bool = False, **kwargs):
        """Add a tinyint column."""
        if primary_key:
            kwargs["primary_key"] = True
        return self.add_column(name, TINYINT, **kwargs)

    def add_numeric_column(
        self, name: str, column_type: str, precision: Optional[int] = None, scale: Optional[int] = None, **kwargs
    ):
        """Add a numeric column (float, double, decimal, numeric)."""
        if column_type in ("float", "double"):
            from sqlalchemy import Float

            return self.add_column(name, Float(precision=precision), **kwargs)
        elif column_type in ("decimal", "numeric"):
            return self.add_column(name, Numeric(precision=precision, scale=scale), **kwargs)
        else:
            raise ValueError(f"Unsupported numeric type: {column_type}")

    def add_datetime_column(self, name: str, column_type: str, **kwargs):
        """Add a datetime column (date, datetime, timestamp, time, year)."""
        if column_type == "date":
            return self.add_column(name, Date, **kwargs)
        elif column_type == "datetime":
            return self.add_column(name, DateTime, **kwargs)
        elif column_type == "timestamp":
            return self.add_column(name, TIMESTAMP, **kwargs)
        elif column_type == "time":
            return self.add_column(name, Time, **kwargs)
        elif column_type == "year":
            # MySQL YEAR type
            from sqlalchemy.dialects.mysql import YEAR

            return self.add_column(name, YEAR, **kwargs)
        else:
            raise ValueError(f"Unsupported datetime type: {column_type}")

    def add_boolean_column(self, name: str, **kwargs):
        """Add a boolean column."""
        return self.add_column(name, Boolean, **kwargs)

    def add_binary_column(self, name: str, column_type: str, **kwargs):
        """Add a binary column (blob, longblob, mediumblob, tinyblob, binary, varbinary)."""
        if column_type == "blob":
            return self.add_column(name, BLOB, **kwargs)
        elif column_type == "longblob":
            return self.add_column(name, LONGBLOB, **kwargs)
        elif column_type == "mediumblob":
            return self.add_column(name, MEDIUMBLOB, **kwargs)
        elif column_type == "tinyblob":
            return self.add_column(name, TINYBLOB, **kwargs)
        elif column_type == "binary":
            from sqlalchemy import Binary

            return self.add_column(name, Binary, **kwargs)
        elif column_type == "varbinary":
            return self.add_column(name, VARBINARY, **kwargs)
        else:
            raise ValueError(f"Unsupported binary type: {column_type}")

    def add_enum_column(self, name: str, column_type: str, values: list, **kwargs):
        """Add an enum or set column."""
        if column_type == "enum":
            from sqlalchemy import Enum

            return self.add_column(name, Enum(*values), **kwargs)
        elif column_type == "set":
            from sqlalchemy.dialects.mysql import SET

            return self.add_column(name, SET(*values), **kwargs)
        else:
            raise ValueError(f"Unsupported enum type: {column_type}")

    def add_index(self, columns: Union[str, List[str]], name: Optional[str] = None, **kwargs):
        """Add an index to the table."""
        if isinstance(columns, str):
            columns = [columns]

        index_name = name or f"idx_{self.table_name}_{'_'.join(columns)}"
        index = Index(index_name, *columns, **kwargs)
        self.indexes.append(index)
        return self

    def add_primary_key(self, columns: Union[str, List[str]]):
        """Add a primary key constraint."""
        if isinstance(columns, str):
            columns = [columns]
        constraint = PrimaryKeyConstraint(*columns)
        self.constraints.append(constraint)
        return self

    def add_foreign_key(
        self,
        columns: Union[str, List[str]],
        ref_table: str,
        ref_columns: Union[str, List[str]],
        name: Optional[str] = None,
    ):
        """Add a foreign key constraint."""
        if isinstance(columns, str):
            columns = [columns]
        if isinstance(ref_columns, str):
            ref_columns = [ref_columns]

        constraint_name = name or f"fk_{self.table_name}_{'_'.join(columns)}"
        constraint = ForeignKeyConstraint(columns, [f"{ref_table}.{col}" for col in ref_columns], name=constraint_name)
        self.constraints.append(constraint)
        return self

    def build(self) -> Table:
        """Build and return the SQLAlchemy Table object."""
        return Table(self.table_name, self.metadata, *self.columns, *self.indexes, *self.constraints)


def create_vector_table(table_name: str, metadata: MetaData = None) -> VectorTableBuilder:
    """
    Create a new vector table builder.

    Args:

        table_name: Name of the table
        metadata: SQLAlchemy metadata object

    Returns:

        VectorTableBuilder instance
    """
    return VectorTableBuilder(table_name, metadata)


# Convenience functions for common table patterns
def create_vector_index_table(table_name: str, metadata: MetaData = None) -> VectorTableBuilder:
    """
    Create a table builder for vector index tables.

    Example: create table vector_index_07(a int primary key, b vecf32(128), c int, key c_k(c))
    """
    builder = VectorTableBuilder(table_name, metadata)

    # Add common columns
    builder.add_int_column("a", primary_key=True)
    builder.add_vecf32_column("b", dimension=128)
    builder.add_int_column("c")

    # Add index on column c
    builder.add_index("c", name="c_k")

    return builder


def create_document_vector_table(table_name: str, metadata: MetaData = None, vector_dim: int = 384) -> VectorTableBuilder:
    """
    Create a table builder for document vector storage.
    """
    builder = VectorTableBuilder(table_name, metadata)

    builder.add_int_column("id", primary_key=True, autoincrement=True)
    builder.add_string_column("document_id", length=100, nullable=False, unique=True)
    builder.add_string_column("title", length=255)
    builder.add_text_column("content")
    builder.add_vecf32_column("embedding", dimension=vector_dim, nullable=False)
    builder.add_json_column("metadata")

    # Add indexes
    builder.add_index("document_id")
    builder.add_index("title")

    return builder


def create_product_vector_table(table_name: str, metadata: MetaData = None, vector_dim: int = 512) -> VectorTableBuilder:
    """
    Create a table builder for product vector storage.
    """
    builder = VectorTableBuilder(table_name, metadata)

    builder.add_int_column("id", primary_key=True, autoincrement=True)
    builder.add_string_column("product_id", length=50, nullable=False, unique=True)
    builder.add_string_column("name", length=200)
    builder.add_text_column("description")
    builder.add_vecf32_column("embedding", dimension=vector_dim, nullable=False)
    builder.add_string_column("category", length=100)
    builder.add_string_column("price", length=20)

    # Add indexes
    builder.add_index("product_id")
    builder.add_index("category")

    return builder
