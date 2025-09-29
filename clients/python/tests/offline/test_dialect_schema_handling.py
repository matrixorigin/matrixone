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
Offline tests for MatrixOne dialect schema handling.
Tests the has_table method and type compiler functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError

from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect, MatrixOneTypeCompiler


class TestMatrixOneDialectSchemaHandling:
    """Test MatrixOne dialect schema handling functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = MatrixOneDialect()
        self.mock_connection = Mock()
        self.mock_connection.connection = Mock()

    def test_has_table_with_none_schema(self):
        """Test has_table method with schema=None (the main issue we're fixing)."""
        # Mock connection with database name in DSN
        self.mock_connection.connection.dsn = "host=localhost port=6001 dbname=test user=root"
        # Mock get_dsn_parameters to return None so it falls back to DSN parsing
        self.mock_connection.connection.get_dsn_parameters.return_value = {}

        # Mock the super().has_table call to return True
        with patch('sqlalchemy.dialects.mysql.base.MySQLDialect.has_table') as mock_parent_has_table:
            mock_parent_has_table.return_value = True

            # Test the method
            result = self.dialect.has_table(self.mock_connection, "test_table", schema=None)

            # Verify that schema was set to "test" (from DSN)
            mock_parent_has_table.assert_called_once_with(self.mock_connection, "test_table", "test", **{})
            assert result is True

    def test_has_table_with_explicit_schema(self):
        """Test has_table method with explicit schema."""
        # Mock the super().has_table call
        with patch('sqlalchemy.dialects.mysql.base.MySQLDialect.has_table') as mock_parent_has_table:
            mock_parent_has_table.return_value = False

            # Test with explicit schema
            result = self.dialect.has_table(self.mock_connection, "test_table", schema="mydb")

            # Verify that the explicit schema was used
            mock_parent_has_table.assert_called_once_with(self.mock_connection, "test_table", "mydb", **{})
            assert result is False

    def test_has_table_fallback_to_default_schema(self):
        """Test has_table method fallback to default schema when DSN parsing fails."""
        # Mock connection without DSN or with invalid DSN
        self.mock_connection.connection.dsn = "invalid_dsn"
        # Mock get_dsn_parameters to return None so it falls back to DSN parsing
        self.mock_connection.connection.get_dsn_parameters.return_value = {}

        with patch('sqlalchemy.dialects.mysql.base.MySQLDialect.has_table') as mock_parent_has_table:
            mock_parent_has_table.return_value = True

            # Test the method
            result = self.dialect.has_table(self.mock_connection, "test_table", schema=None)

            # Verify that fallback schema "test" was used
            mock_parent_has_table.assert_called_once_with(self.mock_connection, "test_table", "test", **{})
            assert result is True

    def test_has_table_with_dsn_parameters(self):
        """Test has_table method with DSN parameters."""
        # Mock connection with get_dsn_parameters method
        self.mock_connection.connection.get_dsn_parameters.return_value = {
            'dbname': 'mydatabase',
            'user': 'root',
            'host': 'localhost',
        }

        with patch('sqlalchemy.dialects.mysql.base.MySQLDialect.has_table') as mock_parent_has_table:
            mock_parent_has_table.return_value = True

            # Test the method
            result = self.dialect.has_table(self.mock_connection, "test_table", schema=None)

            # Verify that database name from DSN parameters was used
            mock_parent_has_table.assert_called_once_with(self.mock_connection, "test_table", "mydatabase", **{})
            assert result is True

    def test_has_table_exception_handling(self):
        """Test has_table method exception handling."""
        # Mock connection that raises exception
        self.mock_connection.connection.dsn = "invalid_dsn"
        self.mock_connection.connection.get_dsn_parameters.side_effect = Exception("Connection error")

        with patch('sqlalchemy.dialects.mysql.base.MySQLDialect.has_table') as mock_parent_has_table:
            mock_parent_has_table.return_value = True

            # Test the method
            result = self.dialect.has_table(self.mock_connection, "test_table", schema=None)

            # Verify that fallback schema "test" was used after exception
            mock_parent_has_table.assert_called_once_with(self.mock_connection, "test_table", "test", **{})
            assert result is True


class TestMatrixOneTypeCompiler:
    """Test MatrixOne type compiler functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = MatrixOneDialect()
        self.type_compiler = MatrixOneTypeCompiler(self.dialect)

    def test_visit_vector_type(self):
        """Test visit_VECTOR method."""
        # Mock vector type
        mock_vector_type = Mock()
        mock_vector_type.get_col_spec.return_value = "VECTOR(128)"

        result = self.type_compiler.visit_VECTOR(mock_vector_type)
        assert result == "VECTOR(128)"

    def test_visit_vector_type_without_get_col_spec(self):
        """Test visit_VECTOR method without get_col_spec."""
        # Mock vector type without get_col_spec
        mock_vector_type = Mock()
        del mock_vector_type.get_col_spec

        result = self.type_compiler.visit_VECTOR(mock_vector_type)
        assert result == "VECTOR"

    def test_visit_integer_type(self):
        """Test visit_integer method delegation to MySQL compiler."""
        from sqlalchemy import Integer

        # Test the method directly - it should work without mocking
        result = self.type_compiler.visit_integer(Integer())

        # Verify it returns a valid SQL type
        assert result == "INTEGER"

    def test_visit_string_type(self):
        """Test visit_string method delegation to MySQL compiler."""
        from sqlalchemy import String

        # Test the method directly - it should work without mocking
        result = self.type_compiler.visit_string(String(255))

        # Verify it returns a valid SQL type
        assert result == "VARCHAR(255)"

    def test_visit_text_type(self):
        """Test visit_text method delegation to MySQL compiler."""
        from sqlalchemy import Text

        # Test the method directly - it should work without mocking
        result = self.type_compiler.visit_text(Text())

        # Verify it returns a valid SQL type
        assert result == "TEXT"

    def test_visit_boolean_type(self):
        """Test visit_boolean method delegation to MySQL compiler."""
        from sqlalchemy import Boolean

        # Test the method directly - it should work without mocking
        result = self.type_compiler.visit_boolean(Boolean())

        # Verify it returns a valid SQL type
        assert result == "BOOL"

    def test_visit_float_type(self):
        """Test visit_float method delegation to MySQL compiler."""
        from sqlalchemy import Float

        # Test the method directly - it should work without mocking
        result = self.type_compiler.visit_float(Float())

        # Verify it returns a valid SQL type
        assert result == "FLOAT"

    def test_visit_numeric_type(self):
        """Test visit_numeric method delegation to MySQL compiler."""
        from sqlalchemy import Numeric
        import sqlalchemy

        # Test the method directly - it should work without mocking
        result = self.type_compiler.visit_numeric(Numeric(10, 2))

        # Verify it returns a valid SQL type
        # SQLAlchemy 2.0.x delegates to MySQL and returns NUMERIC
        # SQLAlchemy 1.4.x has explicit implementation that returns DECIMAL
        if sqlalchemy.__version__.startswith('2.'):
            expected = "NUMERIC(10, 2)"
        else:
            expected = "DECIMAL(10, 2)"

        assert result == expected

    def test_visit_date_type(self):
        """Test visit_date method delegation to MySQL compiler."""
        from sqlalchemy import Date

        # Test the method directly - it should work without mocking
        result = self.type_compiler.visit_date(Date())

        # Verify it returns a valid SQL type
        assert result == "DATE"

    def test_visit_datetime_type(self):
        """Test visit_datetime method delegation to MySQL compiler."""
        from sqlalchemy import DateTime

        # Test the method directly - it should work without mocking
        result = self.type_compiler.visit_datetime(DateTime())

        # Verify it returns a valid SQL type
        assert result == "DATETIME"

    def test_visit_time_type(self):
        """Test visit_time method delegation to MySQL compiler."""
        from sqlalchemy import Time

        # Test the method directly - it should work without mocking
        result = self.type_compiler.visit_time(Time())

        # Verify it returns a valid SQL type
        assert result == "TIME"

    def test_visit_timestamp_type(self):
        """Test visit_timestamp method delegation to MySQL compiler."""
        from sqlalchemy import TIMESTAMP

        # Test the method directly - it should work without mocking
        result = self.type_compiler.visit_timestamp(TIMESTAMP())

        # Verify it returns a valid SQL type
        assert result == "TIMESTAMP"

    def test_visit_decimal_type(self):
        """Test visit_DECIMAL method for SQLAlchemy 2.0 compatibility."""
        from sqlalchemy import DECIMAL

        # Test DECIMAL with precision and scale
        decimal_type = DECIMAL(10, 2)
        result = self.type_compiler.visit_DECIMAL(decimal_type)
        assert result == "DECIMAL(10, 2)"

        # Test DECIMAL without precision and scale
        decimal_type = DECIMAL()
        result = self.type_compiler.visit_DECIMAL(decimal_type)
        assert result == "DECIMAL"

    def test_visit_numeric_type_new(self):
        """Test visit_NUMERIC method for SQLAlchemy 2.0 compatibility."""
        from sqlalchemy import NUMERIC

        # Test NUMERIC with precision and scale
        numeric_type = NUMERIC(15, 3)
        result = self.type_compiler.visit_NUMERIC(numeric_type)
        assert result == "DECIMAL(15, 3)"

        # Test NUMERIC without precision and scale
        numeric_type = NUMERIC()
        result = self.type_compiler.visit_NUMERIC(numeric_type)
        assert result == "DECIMAL"

    def test_visit_bigint_type(self):
        """Test visit_BIGINT method for SQLAlchemy 2.0 compatibility."""
        from sqlalchemy import BigInteger

        # Test BIGINT type
        bigint_type = BigInteger()
        result = self.type_compiler.visit_BIGINT(bigint_type)
        assert result == "BIGINT"

    def test_visit_big_integer_type(self):
        """Test visit_big_integer method for SQLAlchemy 2.0 compatibility."""
        from sqlalchemy import BigInteger

        # Test BigInteger type
        bigint_type = BigInteger()
        result = self.type_compiler.visit_big_integer(bigint_type)
        assert result == "BIGINT"

    def test_visit_timestamp_type_new(self):
        """Test visit_TIMESTAMP method for SQLAlchemy 2.0 compatibility."""
        from sqlalchemy import TIMESTAMP

        # Test TIMESTAMP type
        timestamp_type = TIMESTAMP()
        result = self.type_compiler.visit_TIMESTAMP(timestamp_type)
        assert result == "TIMESTAMP"

    # Additional comprehensive type compatibility tests
    def test_visit_small_integer_type(self):
        """Test visit_small_integer method delegation to MySQL compiler."""
        from sqlalchemy import SmallInteger

        result = self.type_compiler.visit_small_integer(SmallInteger())
        assert result == "SMALLINT"

    def test_visit_big_integer_type(self):
        """Test visit_big_integer method delegation to MySQL compiler."""
        from sqlalchemy import BigInteger

        result = self.type_compiler.visit_big_integer(BigInteger())
        assert result == "BIGINT"

    def test_visit_char_type(self):
        """Test visit_CHAR method delegation to MySQL compiler."""
        from sqlalchemy import CHAR

        result = self.type_compiler.visit_CHAR(CHAR(10))
        assert result == "CHAR(10)"

    def test_visit_varchar_type(self):
        """Test visit_VARCHAR method delegation to MySQL compiler."""
        from sqlalchemy import VARCHAR

        result = self.type_compiler.visit_VARCHAR(VARCHAR(255))
        assert result == "VARCHAR(255)"

    def test_visit_unicode_type(self):
        """Test visit_unicode method delegation to MySQL compiler."""
        from sqlalchemy import Unicode

        result = self.type_compiler.visit_unicode(Unicode(100))
        assert result == "VARCHAR(100)"

    def test_visit_unicode_text_type(self):
        """Test visit_unicode_text method delegation to MySQL compiler."""
        from sqlalchemy import UnicodeText

        result = self.type_compiler.visit_unicode_text(UnicodeText())
        assert result == "TEXT"

    def test_visit_large_binary_type(self):
        """Test visit_large_binary method delegation to MySQL compiler."""
        from sqlalchemy import LargeBinary

        result = self.type_compiler.visit_large_binary(LargeBinary())
        assert result == "BLOB"  # MySQL returns BLOB for LargeBinary

    def test_visit_binary_type(self):
        """Test visit_binary method delegation to MySQL compiler."""
        from sqlalchemy import BINARY

        result = self.type_compiler.visit_binary(BINARY(100))
        assert result == "BINARY(100)"

    def test_visit_blob_type(self):
        """Test visit_BLOB method delegation to MySQL compiler."""
        from sqlalchemy import BLOB

        result = self.type_compiler.visit_BLOB(BLOB())
        assert result == "BLOB"

    def test_visit_enum_type(self):
        """Test visit_enum method delegation to MySQL compiler."""
        from sqlalchemy import Enum

        result = self.type_compiler.visit_enum(Enum('A', 'B', 'C'))
        assert result == "ENUM('A','B','C')"

    def test_visit_pickle_type(self):
        """Test visit_pickle method delegation to MySQL compiler."""
        from sqlalchemy import PickleType

        # PickleType is handled by the parent MySQL compiler
        # We need to test it through the process method
        result = self.type_compiler.process(PickleType())
        assert result == "BLOB"

    def test_visit_interval_type(self):
        """Test visit_interval method delegation to MySQL compiler."""
        from sqlalchemy import Interval

        # Interval is handled by the parent MySQL compiler
        # We need to test it through the process method
        result = self.type_compiler.process(Interval())
        assert result == "DATETIME"  # MySQL returns DATETIME for Interval

    def test_visit_uuid_type(self):
        """Test visit_uuid method delegation to MySQL compiler."""
        try:
            from sqlalchemy import UUID

            result = self.type_compiler.visit_uuid(UUID())
            assert result == "CHAR(32)"  # MySQL returns CHAR(32) for UUID
        except ImportError:
            # UUID type not available in this SQLAlchemy version
            pass

    def test_visit_json_type(self):
        """Test visit_JSON method delegation to MySQL compiler."""
        try:
            from sqlalchemy import JSON

            result = self.type_compiler.visit_JSON(JSON())
            assert result == "JSON"
        except ImportError:
            # JSON type not available in this SQLAlchemy version
            pytest.skip("JSON type not available in this SQLAlchemy version")

    def test_visit_arbitrary_type(self):
        """Test visit_arbitrary_type method delegation to MySQL compiler."""
        from sqlalchemy import TypeDecorator, String

        class CustomType(TypeDecorator):
            impl = String(255)  # Provide length to avoid VARCHAR length error

        # Test through process method since visit_arbitrary_type may not exist
        result = self.type_compiler.process(CustomType())
        assert result == "VARCHAR(255)"

    def test_visit_type_decorator_type(self):
        """Test visit_type_decorator method delegation to MySQL compiler."""
        from sqlalchemy import TypeDecorator, String

        class CustomType(TypeDecorator):
            impl = String(255)  # Provide length to avoid VARCHAR length error

        result = self.type_compiler.visit_type_decorator(CustomType())
        assert result == "VARCHAR(255)"

    def test_visit_user_defined_type(self):
        """Test visit_user_defined_type method delegation to MySQL compiler."""
        from sqlalchemy import TypeDecorator, String

        class CustomType(TypeDecorator):
            impl = String(255)  # Provide length to avoid VARCHAR length error

        # Test through process method since visit_user_defined_type may not exist
        result = self.type_compiler.process(CustomType())
        assert result == "VARCHAR(255)"

    # Test type compatibility with different SQLAlchemy versions
    def test_type_compatibility_matrix(self):
        """Test type compatibility across different SQLAlchemy versions."""
        import sqlalchemy
        from sqlalchemy import Integer, String, Numeric, DateTime

        # Test basic types that should work consistently
        test_cases = [
            (Integer(), "INTEGER"),
            (String(255), "VARCHAR(255)"),
            (DateTime(), "DATETIME"),
        ]

        for sql_type, expected in test_cases:
            # Get the appropriate visit method name
            type_name = sql_type.__class__.__name__.lower()
            visit_method = getattr(self.type_compiler, f"visit_{type_name}", None)

            if visit_method:
                result = visit_method(sql_type)
                assert result == expected, f"Type {type_name} failed: expected {expected}, got {result}"

    def test_numeric_precision_scale_compatibility(self):
        """Test numeric types with different precision and scale values."""
        import sqlalchemy
        from sqlalchemy import Numeric, DECIMAL, NUMERIC

        # Test cases with different precision and scale
        test_cases = [
            (
                Numeric(5, 2),
                "DECIMAL(5, 2)" if sqlalchemy.__version__.startswith('1.') else "NUMERIC(5, 2)",
            ),
            (
                Numeric(10, 0),
                "DECIMAL(10, 0)" if sqlalchemy.__version__.startswith('1.') else "NUMERIC(10, 0)",
            ),
            (
                Numeric(18, 4),
                "DECIMAL(18, 4)" if sqlalchemy.__version__.startswith('1.') else "NUMERIC(18, 4)",
            ),
        ]

        for sql_type, expected in test_cases:
            result = self.type_compiler.visit_numeric(sql_type)
            assert result == expected, f"Numeric type failed: expected {expected}, got {result}"

    def test_string_length_compatibility(self):
        """Test string types with different length specifications."""
        from sqlalchemy import String, VARCHAR, CHAR

        test_cases = [
            (String(1), "VARCHAR(1)"),
            (String(255), "VARCHAR(255)"),
            (String(65535), "VARCHAR(65535)"),
            (VARCHAR(100), "VARCHAR(100)"),
            (CHAR(10), "CHAR(10)"),
        ]

        for sql_type, expected in test_cases:
            # Use the process method which handles all types correctly
            result = self.type_compiler.process(sql_type)
            assert result == expected, f"String type {sql_type.__class__.__name__} failed: expected {expected}, got {result}"

    def test_datetime_precision_compatibility(self):
        """Test datetime types with different precision specifications."""
        from sqlalchemy import DateTime, TIMESTAMP, Time

        test_cases = [
            (DateTime(), "DATETIME"),
            (TIMESTAMP(), "TIMESTAMP"),
            (Time(), "TIME"),
        ]

        for sql_type, expected in test_cases:
            type_name = sql_type.__class__.__name__.lower()
            visit_method = getattr(self.type_compiler, f"visit_{type_name}")
            result = visit_method(sql_type)
            assert result == expected, f"DateTime type {type_name} failed: expected {expected}, got {result}"


class TestMatrixOneDialectIntegration:
    """Test MatrixOne dialect integration with SQLAlchemy components."""

    def test_dialect_registration(self):
        """Test that MatrixOne dialect is properly registered."""
        # Test that the dialect has the correct name
        assert MatrixOneDialect.name == "matrixone"

        # Test that the dialect has the required components
        assert hasattr(MatrixOneDialect, 'statement_compiler')
        assert hasattr(MatrixOneDialect, 'type_compiler')
        assert MatrixOneDialect.statement_compiler is not None
        assert MatrixOneDialect.type_compiler is not None

    def test_dialect_initialization(self):
        """Test MatrixOne dialect initialization."""
        dialect = MatrixOneDialect()

        # Test that the dialect initializes correctly
        assert dialect.name == "matrixone"
        assert hasattr(dialect, '_connection_charset')
        assert dialect._connection_charset == "utf8mb4"
        assert dialect.supports_statement_cache is True

    def test_dialect_error_extraction(self):
        """Test error code extraction from MatrixOne exceptions."""
        dialect = MatrixOneDialect()

        # Test with integer error code
        mock_exception = Mock()
        mock_exception.args = (1062, "Duplicate entry")
        error_code = dialect._extract_error_code(mock_exception)
        assert error_code == 1062

        # Test with non-integer error code
        mock_exception.args = ("Error message",)
        error_code = dialect._extract_error_code(mock_exception)
        assert error_code is None

        # Test with empty args
        mock_exception.args = ()
        error_code = dialect._extract_error_code(mock_exception)
        assert error_code is None

    def test_dialect_table_names(self):
        """Test get_table_names method."""
        dialect = MatrixOneDialect()
        mock_connection = Mock()

        # Mock the parent method
        with patch('sqlalchemy.dialects.mysql.base.MySQLDialect.get_table_names') as mock_parent_get_table_names:
            mock_parent_get_table_names.return_value = ["table1", "table2"]

            # Test the method
            result = dialect.get_table_names(mock_connection, schema="test")

            # Verify that the parent method was called
            mock_parent_get_table_names.assert_called_once_with(mock_connection, "test", **{})
            assert result == ["table1", "table2"]

    def test_dialect_columns(self):
        """Test get_columns method."""
        dialect = MatrixOneDialect()
        mock_connection = Mock()

        # Mock the parent method
        with patch('sqlalchemy.dialects.mysql.base.MySQLDialect.get_columns') as mock_parent_get_columns:
            mock_parent_get_columns.return_value = [{"name": "id", "type": "INT"}]

            # Test the method
            result = dialect.get_columns(mock_connection, "test_table", schema="test")

            # Verify that the parent method was called
            mock_parent_get_columns.assert_called_once_with(mock_connection, "test_table", "test", **{})
            assert result == [{"name": "id", "type": "INT"}]
