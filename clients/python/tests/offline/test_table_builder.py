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
Offline tests for VectorTableBuilder functionality.
"""

import pytest
import sys
from unittest.mock import Mock
from sqlalchemy import MetaData
from sqlalchemy.schema import CreateTable
from matrixone.sqlalchemy_ext import (
    VectorTableBuilder,
    create_vector_table,
    create_vector_index_table,
    Vectorf32,
    Vectorf64,
)

pytestmark = pytest.mark.vector

# No longer needed - global mocks have been fixed


class TestVectorTableBuilder:
    """Test VectorTableBuilder functionality."""

    def test_create_vector_index_table(self):
        """Test creating the exact table from the example."""
        metadata = MetaData()
        builder = create_vector_index_table("vector_index_07", metadata)
        table = builder.build()

        assert table.name == "vector_index_07"
        assert len(table.columns) == 3

        # Check columns
        column_names = [col.name for col in table.columns]
        assert "a" in column_names
        assert "b" in column_names
        assert "c" in column_names

        # Check column types
        for col in table.columns:
            if col.name == "a":
                assert col.primary_key
                assert str(col.type) == "INTEGER"
            elif col.name == "b":
                assert str(col.type) == "vecf32(128)"
            elif col.name == "c":
                assert str(col.type) == "INTEGER"

        # Check indexes
        assert len(table.indexes) == 1
        index = list(table.indexes)[0]
        assert index.name == "c_k"
        assert len(index.columns) == 1
        assert index.columns[0].name == "c"

    def test_manual_table_builder(self):
        """Test manual table building."""
        metadata = MetaData()
        builder = VectorTableBuilder("test_table", metadata)

        builder.add_int_column("id", primary_key=True)
        builder.add_string_column("name", length=100)
        builder.add_vecf32_column("embedding", dimension=128)
        builder.add_index("name")

        table = builder.build()

        assert table.name == "test_table"
        assert len(table.columns) == 3
        assert len(table.indexes) == 1

        # Check column types
        for col in table.columns:
            if col.name == "id":
                assert col.primary_key
                assert str(col.type) == "INTEGER"
            elif col.name == "name":
                assert str(col.type) == "VARCHAR(100)"
            elif col.name == "embedding":
                assert str(col.type) == "vecf32(128)"

    def test_vector_column_types(self):
        """Test different vector column types."""
        metadata = MetaData()
        builder = VectorTableBuilder("vector_types_test", metadata)

        builder.add_vecf32_column("vec32", dimension=128)
        builder.add_vecf64_column("vec64", dimension=256)
        builder.add_int_column("id", primary_key=True)

        table = builder.build()

        # Check vector column types
        for col in table.columns:
            if col.name == "vec32":
                assert str(col.type) == "vecf32(128)"
            elif col.name == "vec64":
                assert str(col.type) == "vecf64(256)"

    def test_multiple_indexes(self):
        """Test table with multiple indexes."""
        metadata = MetaData()
        builder = VectorTableBuilder("multi_index_table", metadata)

        builder.add_int_column("id", primary_key=True)
        builder.add_string_column("name", length=100)
        builder.add_string_column("category", length=50)
        builder.add_int_column("version")
        builder.add_vecf32_column("embedding", dimension=128)

        # Add multiple indexes
        builder.add_index("name")
        builder.add_index("category")
        builder.add_index(["category", "version"])

        table = builder.build()

        assert len(table.indexes) == 3

        index_names = [idx.name for idx in table.indexes]
        assert any("name" in name for name in index_names)
        assert any("category" in name for name in index_names)
        assert any("category" in name and "version" in name for name in index_names)

    def test_sql_generation(self):
        """Test SQL generation for vector tables."""
        metadata = MetaData()
        builder = create_vector_index_table("sql_test_table", metadata)
        table = builder.build()

        # Generate CREATE TABLE SQL using MatrixOne dialect
        from matrixone.sqlalchemy_ext import MatrixOneDialect

        create_sql = str(CreateTable(table).compile(dialect=MatrixOneDialect(), compile_kwargs={"literal_binds": True}))

        # Check that SQL contains expected elements
        assert "CREATE TABLE sql_test_table" in create_sql
        assert "a INTEGER NOT NULL" in create_sql
        assert "b vecf32(128)" in create_sql
        assert "c INTEGER" in create_sql
        assert "PRIMARY KEY (a)" in create_sql

    def test_empty_table_builder(self):
        """Test creating an empty table builder."""
        metadata = MetaData()
        builder = VectorTableBuilder("empty_table", metadata)
        table = builder.build()

        assert table.name == "empty_table"
        assert len(table.columns) == 0
        assert len(table.indexes) == 0

    def test_table_builder_fluent_interface(self):
        """Test the fluent interface of table builder."""
        metadata = MetaData()

        # Chain method calls
        table = (
            VectorTableBuilder("fluent_table", metadata)
            .add_int_column("id", primary_key=True)
            .add_string_column("name", length=100)
            .add_vecf32_column("embedding", dimension=128)
            .add_index("name")
            .build()
        )

        assert table.name == "fluent_table"
        assert len(table.columns) == 3
        assert len(table.indexes) == 1

    def test_create_vector_table_convenience(self):
        """Test create_vector_table convenience function."""
        metadata = MetaData()
        builder = create_vector_table("convenience_table", metadata)

        # Should return a builder, not a table
        assert isinstance(builder, VectorTableBuilder)
        assert builder.table_name == "convenience_table"

        # Build the table
        table = builder.add_int_column("id", primary_key=True).build()
        assert table.name == "convenience_table"
