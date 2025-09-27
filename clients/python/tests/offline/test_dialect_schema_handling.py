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
            mock_parent_has_table.assert_called_once_with(
                self.mock_connection, "test_table", "test", **{}
            )
            assert result is True

    def test_has_table_with_explicit_schema(self):
        """Test has_table method with explicit schema."""
        # Mock the super().has_table call
        with patch('sqlalchemy.dialects.mysql.base.MySQLDialect.has_table') as mock_parent_has_table:
            mock_parent_has_table.return_value = False
            
            # Test with explicit schema
            result = self.dialect.has_table(self.mock_connection, "test_table", schema="mydb")
            
            # Verify that the explicit schema was used
            mock_parent_has_table.assert_called_once_with(
                self.mock_connection, "test_table", "mydb", **{}
            )
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
            mock_parent_has_table.assert_called_once_with(
                self.mock_connection, "test_table", "test", **{}
            )
            assert result is True

    def test_has_table_with_dsn_parameters(self):
        """Test has_table method with DSN parameters."""
        # Mock connection with get_dsn_parameters method
        self.mock_connection.connection.get_dsn_parameters.return_value = {
            'dbname': 'mydatabase',
            'user': 'root',
            'host': 'localhost'
        }
        
        with patch('sqlalchemy.dialects.mysql.base.MySQLDialect.has_table') as mock_parent_has_table:
            mock_parent_has_table.return_value = True
            
            # Test the method
            result = self.dialect.has_table(self.mock_connection, "test_table", schema=None)
            
            # Verify that database name from DSN parameters was used
            mock_parent_has_table.assert_called_once_with(
                self.mock_connection, "test_table", "mydatabase", **{}
            )
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
            mock_parent_has_table.assert_called_once_with(
                self.mock_connection, "test_table", "test", **{}
            )
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
        
        # Test the method directly - it should work without mocking
        result = self.type_compiler.visit_numeric(Numeric(10, 2))
        
        # Verify it returns a valid SQL type
        assert result == "NUMERIC(10, 2)"

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
