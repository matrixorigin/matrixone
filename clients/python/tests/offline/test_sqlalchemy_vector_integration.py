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
Offline tests for SQLAlchemy integration with MatrixOne vector types.
"""

import pytest
import sys
from unittest.mock import Mock
from sqlalchemy import MetaData, Table, Column, Integer, String, Index
from sqlalchemy.schema import CreateTable
from matrixone.sqlalchemy_ext import (
    VectorType,
    Vectorf32,
    Vectorf64,
    VectorTypeDecorator,
    MatrixOneDialect,
    VectorTableBuilder,
)

pytestmark = pytest.mark.vector

# No longer needed - global mocks have been fixed


class TestSQLAlchemyVectorIntegration:
    """Test SQLAlchemy vector integration functionality."""

    def test_vector_type_in_sqlalchemy_table(self):
        """Test using VectorType in SQLAlchemy Table definition."""
        metadata = MetaData()

        # Create table with vector columns
        table = Table(
            'vector_test',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            Column('embedding_32', Vectorf32(dimension=128)),
            Column('embedding_64', Vectorf64(dimension=256)),
        )

        # Verify table structure
        assert table.name == 'vector_test'
        assert len(table.columns) == 4

        # Check column types
        column_types = {col.name: type(col.type) for col in table.columns}
        assert column_types['id'] == Integer
        assert column_types['name'] == String
        assert column_types['embedding_32'] == Vectorf32
        assert column_types['embedding_64'] == Vectorf64

    def test_sql_generation_with_vector_types(self):
        """Test SQL generation with vector types."""
        metadata = MetaData()

        table = Table(
            'sql_test',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('vector', Vectorf32(dimension=128)),
        )

        # Generate CREATE TABLE SQL using MatrixOne dialect
        from matrixone.sqlalchemy_ext import MatrixOneDialect

        create_sql = str(CreateTable(table).compile(dialect=MatrixOneDialect(), compile_kwargs={"literal_binds": True}))

        # Verify SQL contains vector type
        assert "CREATE TABLE sql_test" in create_sql
        assert "vector vecf32(128)" in create_sql
        assert "PRIMARY KEY (id)" in create_sql

    def test_vector_type_decorator_in_table(self):
        """Test VectorTypeDecorator in table definition."""
        metadata = MetaData()

        table = Table(
            'decorator_test',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('vector', VectorTypeDecorator(dimension=64, precision="f32")),
        )

        # Verify table structure
        assert len(table.columns) == 2
        vector_col = table.columns['vector']
        assert isinstance(vector_col.type, VectorTypeDecorator)
        assert vector_col.type.dimension == 64
        assert vector_col.type.precision == "f32"

    def test_matrixone_dialect_type_handling(self):
        """Test MatrixOneDialect type handling."""
        dialect = MatrixOneDialect()

        # Test vector type creation from strings
        test_cases = [
            ("vecf32(128)", "f32", 128),
            ("VECF32(256)", "f32", 256),
            ("vecf64(512)", "f64", 512),
            ("VECF64(1024)", "f64", 1024),
        ]

        for type_str, precision, expected_dim in test_cases:
            vector_type = dialect._create_vector_type(precision, type_str)
            assert vector_type.precision == precision
            assert vector_type.dimension == expected_dim

    def test_vector_table_builder_integration(self):
        """Test VectorTableBuilder integration with SQLAlchemy."""
        metadata = MetaData()

        # Create table using builder
        builder = VectorTableBuilder("builder_test", metadata)
        builder.add_int_column("id", primary_key=True)
        builder.add_string_column("name", length=100)
        builder.add_vecf32_column("embedding", dimension=128)
        builder.add_index("name")

        table = builder.build()

        # Verify table structure
        assert table.name == "builder_test"
        assert len(table.columns) == 3
        assert len(table.indexes) == 1

        # Check column types
        for col in table.columns:
            if col.name == "embedding":
                assert str(col.type) == "vecf32(128)"

    def test_vector_type_serialization(self):
        """Test vector type data serialization."""
        # Test Vectorf32 serialization
        vec32 = Vectorf32(dimension=128)
        bind_processor = vec32.bind_processor(None)

        # Test list to string conversion
        vector_list = [1.0, 2.0, 3.0]
        result = bind_processor(vector_list)
        assert result == "[1.0,2.0,3.0]"

        # Test string passthrough
        vector_string = "[4.0,5.0,6.0]"
        result = bind_processor(vector_string)
        assert result == vector_string

    def test_vector_type_deserialization(self):
        """Test vector type data deserialization."""
        # Test Vectorf64 deserialization
        vec64 = Vectorf64(dimension=256)
        result_processor = vec64.result_processor(None, None)

        # Test string to list conversion
        vector_string = "[1.0,2.0,3.0]"
        result = result_processor(vector_string)
        assert result == [1.0, 2.0, 3.0]

        # Test None handling
        result = result_processor(None)
        assert result is None

    def test_complex_table_with_multiple_vector_columns(self):
        """Test complex table with multiple vector columns."""
        metadata = MetaData()

        table = Table(
            'complex_vector_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('title', String(200)),
            Column('content_embedding', Vectorf32(dimension=384)),
            Column('title_embedding', Vectorf32(dimension=128)),
            Column('metadata_embedding', Vectorf64(dimension=512)),
        )

        # Verify table structure
        assert table.name == 'complex_vector_table'
        assert len(table.columns) == 5

        # Check vector columns
        vector_columns = [
            col for col in table.columns if isinstance(col.type, (VectorType, VectorTypeDecorator, Vectorf32, Vectorf64))
        ]
        assert len(vector_columns) == 3

        # Verify dimensions
        dimensions = {col.name: col.type.dimension for col in vector_columns}
        assert dimensions['content_embedding'] == 384
        assert dimensions['title_embedding'] == 128
        assert dimensions['metadata_embedding'] == 512

    def test_table_with_indexes_and_vector_columns(self):
        """Test table with indexes and vector columns."""
        metadata = MetaData()

        table = Table(
            'indexed_vector_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('category', String(50)),
            Column('embedding', Vectorf32(dimension=128)),
            Column('score', Integer),
        )

        # Add indexes
        Index('idx_category', table.c.category)
        Index('idx_score', table.c.score)

        # Verify table structure
        assert len(table.indexes) == 2

        # Generate SQL to verify structure using MatrixOne dialect
        from matrixone.sqlalchemy_ext import MatrixOneDialect

        create_sql = str(CreateTable(table).compile(dialect=MatrixOneDialect(), compile_kwargs={"literal_binds": True}))

        # Check SQL contains expected elements
        assert "CREATE TABLE indexed_vector_table" in create_sql
        assert "embedding vecf32(128)" in create_sql
        # Note: Index creation is separate from table creation in SQLAlchemy
        # The indexes are defined but not included in the CREATE TABLE statement

    def test_vector_type_comparison(self):
        """Test vector type comparison."""
        vec1 = Vectorf32(dimension=128)
        vec2 = Vectorf32(dimension=128)
        vec3 = Vectorf32(dimension=256)
        vec4 = Vectorf64(dimension=128)

        # Test equality by comparing properties
        assert vec1.dimension == vec2.dimension
        assert vec1.precision == vec2.precision

        # Different dimension should not be equal
        assert vec1.dimension != vec3.dimension

        # Different precision should not be equal
        assert vec1.precision != vec4.precision

    def test_vector_type_edge_cases(self):
        """Test edge cases for vector types."""
        # Test very large dimensions
        large_vec = Vectorf32(dimension=65535)
        assert large_vec.dimension == 65535

        # Test dimension 1
        small_vec = Vectorf32(dimension=1)
        assert small_vec.dimension == 1

        # Test without dimension
        no_dim_vec = VectorType(precision="f32")
        assert no_dim_vec.dimension is None
        assert no_dim_vec.get_col_spec() == "vecf32"
