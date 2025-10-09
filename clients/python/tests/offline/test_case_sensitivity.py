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
Tests for case sensitivity handling in MatrixOne SQLAlchemy extensions.
"""

import pytest
import sys
from unittest.mock import Mock
from matrixone.sqlalchemy_ext import MatrixOneDialect, VectorType, Vectorf32, Vectorf64

pytestmark = pytest.mark.vector

# No longer needed - global mocks have been fixed


class TestCaseSensitivity:
    """Test case sensitivity handling."""

    def test_vector_type_case_insensitive_parsing(self):
        """Test that vector types are parsed case-insensitively."""
        dialect = MatrixOneDialect()

        # Test various case combinations
        test_cases = [
            ("vecf32(128)", "f32", 128),
            ("VECF32(128)", "f32", 128),
            ("VecF32(128)", "f32", 128),
            ("VECf32(128)", "f32", 128),
            ("vecf64(256)", "f64", 256),
            ("VECF64(256)", "f64", 256),
            ("VecF64(256)", "f64", 256),
            ("VECf64(256)", "f64", 256),
        ]

        for type_str, expected_precision, expected_dimension in test_cases:
            vector_type = dialect._create_vector_type(expected_precision, type_str)

            assert vector_type.precision == expected_precision
            assert vector_type.dimension == expected_dimension

    def test_vector_type_no_dimension(self):
        """Test vector types without dimension."""
        dialect = MatrixOneDialect()

        test_cases = [
            ("vecf32", "f32"),
            ("VECF32", "f32"),
            ("VecF32", "f32"),
            ("vecf64", "f64"),
            ("VECF64", "f64"),
            ("VecF64", "f64"),
        ]

        for type_str, expected_precision in test_cases:
            vector_type = dialect._create_vector_type(expected_precision, type_str)

            assert vector_type.precision == expected_precision
            assert vector_type.dimension is None

    def test_vector_type_invalid_cases(self):
        """Test handling of invalid vector type strings."""
        dialect = MatrixOneDialect()

        # These should not raise exceptions but return default types
        invalid_cases = [
            "vecf32()",  # Empty parentheses
            "vecf32(abc)",  # Non-numeric dimension
            "vecf32(128",  # Missing closing parenthesis
            "vecf32(128,64)",  # Multiple parameters
            "unknown_type",  # Unknown type
        ]

        for invalid_type in invalid_cases:
            # Should not raise exception
            vector_type = dialect._create_vector_type("f32", invalid_type)
            assert vector_type.precision == "f32"
            assert vector_type.dimension is None

    def test_vector_type_edge_cases(self):
        """Test edge cases for vector type parsing."""
        dialect = MatrixOneDialect()

        # Test with whitespace
        vector_type = dialect._create_vector_type("f32", "  vecf32(128)  ")
        assert vector_type.dimension == 128

        # Test with mixed case and whitespace
        vector_type = dialect._create_vector_type("f64", "  VECF64(256)  ")
        assert vector_type.dimension == 256

        # Test with very large dimensions
        vector_type = dialect._create_vector_type("f32", "vecf32(65535)")
        assert vector_type.dimension == 65535

        # Test with small dimensions
        vector_type = dialect._create_vector_type("f32", "vecf32(1)")
        assert vector_type.dimension == 1

    def test_vector_type_compiler_case_handling(self):
        """Test that the compiler handles case properly."""
        from matrixone.sqlalchemy_ext.dialect import MatrixOneCompiler

        # Create a compiler instance
        compiler = MatrixOneCompiler(MatrixOneDialect(), None)

        # Test vector type compilation
        vector_type = Vectorf32(dimension=128)
        result = compiler.visit_user_defined_type(vector_type)
        assert result == "vecf32(128)"

        vector_type = Vectorf64(dimension=256)
        result = compiler.visit_user_defined_type(vector_type)
        assert result == "vecf64(256)"

    def test_column_processing_case_insensitive(self):
        """Test that column processing is case-insensitive."""
        dialect = MatrixOneDialect()

        # Mock column data
        columns = [
            {'name': 'col1', 'type': 'vecf32(128)'},
            {'name': 'col2', 'type': 'VECF32(256)'},
            {'name': 'col3', 'type': 'VecF32(512)'},
            {'name': 'col4', 'type': 'VECF64(1024)'},
            {'name': 'col5', 'type': 'vecf64(2048)'},
            {'name': 'col6', 'type': 'INTEGER'},  # Non-vector type
        ]

        # Process columns (simulating get_columns behavior)
        for column in columns:
            if isinstance(column['type'], str):
                type_str_lower = column['type'].lower()
                if type_str_lower.startswith('vecf32'):
                    column['type'] = dialect._create_vector_type('f32', column['type'])
                elif type_str_lower.startswith('vecf64'):
                    column['type'] = dialect._create_vector_type('f64', column['type'])

        # Verify results
        assert isinstance(columns[0]['type'], VectorType)
        assert columns[0]['type'].dimension == 128
        assert columns[0]['type'].precision == "f32"

        assert isinstance(columns[1]['type'], VectorType)
        assert columns[1]['type'].dimension == 256
        assert columns[1]['type'].precision == "f32"

        assert isinstance(columns[2]['type'], VectorType)
        assert columns[2]['type'].dimension == 512
        assert columns[2]['type'].precision == "f32"

        assert isinstance(columns[3]['type'], VectorType)
        assert columns[3]['type'].dimension == 1024
        assert columns[3]['type'].precision == "f64"

        assert isinstance(columns[4]['type'], VectorType)
        assert columns[4]['type'].dimension == 2048
        assert columns[4]['type'].precision == "f64"

        # Non-vector type should remain unchanged
        assert columns[5]['type'] == 'INTEGER'

    def test_sql_generation_case_consistency(self):
        """Test that SQL generation produces consistent case."""
        from sqlalchemy import MetaData
        from sqlalchemy.schema import CreateTable
        from matrixone.sqlalchemy_ext import VectorTableBuilder

        metadata = MetaData()

        # Create table with vector columns
        builder = VectorTableBuilder("case_test_table", metadata)
        builder.add_int_column("id", primary_key=True)
        builder.add_vecf32_column("embedding_32", dimension=128)
        builder.add_vecf64_column("embedding_64", dimension=256)

        table = builder.build()

        # Generate SQL using MatrixOne dialect
        from matrixone.sqlalchemy_ext import MatrixOneDialect

        create_sql = str(CreateTable(table).compile(dialect=MatrixOneDialect(), compile_kwargs={"literal_binds": True}))

        # Check that vector types are in lowercase
        assert "vecf32(128)" in create_sql
        assert "vecf64(256)" in create_sql
        assert "VECF32" not in create_sql
        assert "VECF64" not in create_sql

    def test_real_world_case_scenarios(self):
        """Test real-world case sensitivity scenarios."""
        dialect = MatrixOneDialect()

        # Simulate what might come from MatrixOne database introspection
        real_world_types = [
            "vecf32(384)",  # Standard lowercase
            "VECF32(768)",  # All uppercase (some databases)
            "VecF32(128)",  # Mixed case (some tools)
            "vecF64(1024)",  # Mixed case with different pattern
            "VECf64(512)",  # Another mixed case pattern
        ]

        for type_str in real_world_types:
            # Extract precision from type string
            if 'f32' in type_str.lower():
                precision = 'f32'
            else:
                precision = 'f64'

            vector_type = dialect._create_vector_type(precision, type_str)

            # Should successfully create vector type regardless of case
            assert isinstance(vector_type, VectorType)
            assert vector_type.precision == precision
            assert vector_type.dimension is not None
