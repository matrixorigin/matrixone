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
Online tests for MatrixOne dialect schema handling.
Tests the has_table method and type compiler functionality with real database connections.
"""

import pytest
import sqlalchemy
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Text,
    Boolean,
    Float,
    Numeric,
    Date,
    DateTime,
    Time,
    TIMESTAMP,
    text,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError

from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect
from .test_config import online_config

# SQLAlchemy version compatibility
SA_VERSION = tuple(map(int, sqlalchemy.__version__.split(".")[:2]))


def execute_sql(connection, sql):
    """Execute SQL in a version-compatible way."""
    # For SQLAlchemy 2.0 compatibility, use explicit transaction management
    if SA_VERSION >= (2, 0):
        # SQLAlchemy 2.0+ style
        return connection.execute(text(sql))
    else:
        # SQLAlchemy 1.4.x style - use begin() for explicit transaction
        if hasattr(connection, 'in_transaction') and not connection.in_transaction():
            with connection.begin():
                return connection.execute(text(sql))
        else:
            return connection.execute(text(sql))


class TestMatrixOneDialectSchemaHandlingOnline:
    """Test MatrixOne dialect schema handling with real database connections."""

    @pytest.fixture(scope="class")
    def engine(self):
        """Create engine for testing."""
        # Use configurable connection parameters
        host, port, user, password, database = online_config.get_connection_params()
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        # Replace the dialect with our MatrixOne dialect but preserve dbapi
        original_dbapi = engine.dialect.dbapi
        engine.dialect = MatrixOneDialect()
        engine.dialect.dbapi = original_dbapi
        return engine

    @pytest.fixture(scope="class")
    def metadata(self):
        """Create metadata for testing."""
        return MetaData()

    def test_has_table_with_none_schema_online(self, engine, metadata):
        """Test has_table method with schema=None using real database connection."""
        # Create a test table
        test_table = Table(
            'test_schema_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
        )

        # Create the table
        test_table.create(engine, checkfirst=True)

        try:
            # Test has_table with schema=None (this was the main issue)
            dialect = engine.dialect
            with engine.connect() as connection:
                # This should not raise AssertionError
                result = dialect.has_table(connection, 'test_schema_table', schema=None)
                assert result is True

                # Test with non-existent table
                result = dialect.has_table(connection, 'non_existent_table', schema=None)
                assert result is False

        finally:
            # Clean up
            test_table.drop(engine, checkfirst=True)

    def test_has_table_with_explicit_schema_online(self, engine, metadata):
        """Test has_table method with explicit schema using real database connection."""
        # Create a test table
        test_table = Table(
            'test_explicit_schema_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
        )

        # Create the table
        test_table.create(engine, checkfirst=True)

        try:
            # Get database name from connection
            with engine.connect() as connection:
                # Use version-compatible API
                result = connection.execute(text("SELECT DATABASE()"))
                current_db = result.scalar()

                # Test has_table with explicit schema
                dialect = engine.dialect
                result = dialect.has_table(connection, 'test_explicit_schema_table', schema=current_db)
                assert result is True

                # Test with non-existent table
                result = dialect.has_table(connection, 'non_existent_table', schema=current_db)
                assert result is False

        finally:
            # Clean up
            test_table.drop(engine, checkfirst=True)

    def test_type_compiler_with_standard_types_online(self, engine, metadata):
        """Test type compiler with standard SQLAlchemy types using real database connection."""
        # Create a test table with various standard types
        test_table = Table(
            'test_type_compiler_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            Column('description', Text),
            Column('is_active', Boolean),
            Column('price', Float),
            Column('quantity', Numeric(10, 2)),
            Column('created_date', Date),
            Column('created_at', DateTime),
            Column('updated_at', Time),
            Column('timestamp', TIMESTAMP),
        )

        try:
            # Create the table - this should not raise UnsupportedCompilationError
            test_table.create(engine, checkfirst=True)

            # Verify the table was created
            with engine.connect() as connection:
                dialect = engine.dialect
                result = dialect.has_table(connection, 'test_type_compiler_table', schema=None)
                assert result is True

        finally:
            # Clean up
            test_table.drop(engine, checkfirst=True)

    def test_create_all_with_declarative_base_online(self, engine):
        """Test create_all with declarative_base (the original failing scenario)."""
        # Create declarative base
        Base = declarative_base()

        # Define a test model
        class TestModel(Base):
            __tablename__ = 'test_declarative_model'

            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            description = Column(Text)

        try:
            # This should not raise AssertionError or UnsupportedCompilationError
            Base.metadata.create_all(engine, checkfirst=True)

            # Verify the table was created
            with engine.connect() as connection:
                dialect = engine.dialect
                result = dialect.has_table(connection, 'test_declarative_model', schema=None)
                assert result is True

        finally:
            # Clean up
            Base.metadata.drop_all(engine, checkfirst=True)

    def test_create_all_with_vector_types_online(self, engine):
        """Test create_all with vector types (if available)."""
        try:
            from matrixone.sqlalchemy_ext import Vectorf32, create_vector_column

            # Create declarative base
            Base = declarative_base()

            # Define a test model with vector types
            class VectorTestModel(Base):
                __tablename__ = 'test_vector_model'

                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                embedding = create_vector_column(128, "f32")

            # This should not raise any errors
            Base.metadata.create_all(engine, checkfirst=True)

            # Verify the table was created
            with engine.connect() as connection:
                dialect = engine.dialect
                result = dialect.has_table(connection, 'test_vector_model', schema=None)
                assert result is True

        except ImportError:
            # Skip if vector types are not available
            pytest.skip("Vector types not available")
        finally:
            # Clean up
            try:
                Base.metadata.drop_all(engine, checkfirst=True)
            except:
                pass

    def test_dialect_connection_handling_online(self, engine):
        """Test dialect connection handling with real database."""
        dialect = engine.dialect

        with engine.connect() as connection:
            # Test that the dialect can handle the connection
            assert hasattr(dialect, 'has_table')
            assert hasattr(dialect, 'get_table_names')
            assert hasattr(dialect, 'get_columns')

            # Test basic dialect functionality
            tables = dialect.get_table_names(connection, schema="test")
            assert isinstance(tables, list)

            # Test that we can get current database name
            # Use version-compatible API
            result = execute_sql(connection, "SELECT DATABASE()")
            current_db = result.scalar()
            assert current_db is not None

    def test_dialect_error_handling_online(self, engine):
        """Test dialect error handling with real database."""
        dialect = engine.dialect

        with engine.connect() as connection:
            # Test error code extraction
            try:
                # Try to create a table that already exists (should raise an error)
                # Use version-compatible API
                execute_sql(connection, "CREATE TABLE test_error_handling (id INT PRIMARY KEY)")
                execute_sql(connection, "CREATE TABLE test_error_handling (id INT PRIMARY KEY)")
                # connection.execute(text("CREATE TABLE test_error_handling (id INT PRIMARY KEY)"))
            except Exception as e:
                # Test error code extraction
                error_code = dialect._extract_error_code(e)
                # The error code should be extracted if it's an integer
                if error_code is not None:
                    assert isinstance(error_code, int)

            # Clean up
            try:
                execute_sql(connection, "DROP TABLE IF EXISTS test_error_handling")
            except:
                pass

    def test_dialect_type_compiler_registration_online(self, engine):
        """Test that type compiler is properly registered with the dialect."""
        dialect = engine.dialect

        # Test that the type compiler is properly registered
        assert hasattr(dialect, 'type_compiler')
        assert dialect.type_compiler is not None

        # Test that we can create a type compiler instance
        # In SQLAlchemy 1.4.x, type_compiler is a class, not an instance
        if hasattr(dialect.type_compiler, '__call__'):
            type_compiler = dialect.type_compiler(dialect)
        else:
            type_compiler = dialect.type_compiler
        assert type_compiler is not None

        # Test that the type compiler has the required methods
        assert hasattr(type_compiler, 'visit_integer')
        assert hasattr(type_compiler, 'visit_string')
        assert hasattr(type_compiler, 'visit_text')
        assert hasattr(type_compiler, 'visit_boolean')
        assert hasattr(type_compiler, 'visit_float')
        assert hasattr(type_compiler, 'visit_numeric')
        assert hasattr(type_compiler, 'visit_date')
        assert hasattr(type_compiler, 'visit_datetime')
        assert hasattr(type_compiler, 'visit_time')
        assert hasattr(type_compiler, 'visit_timestamp')

    def test_dialect_statement_compiler_registration_online(self, engine):
        """Test that statement compiler is properly registered with the dialect."""
        dialect = engine.dialect

        # Test that the statement compiler is properly registered
        assert hasattr(dialect, 'statement_compiler')
        assert dialect.statement_compiler is not None

        # Test that we can create a statement compiler instance
        with engine.connect() as connection:
            # Create a simple statement to test the compiler
            # Use SQLAlchemy 1.4 compatible API
            stmt = text("SELECT 1")
            statement_compiler = dialect.statement_compiler(dialect, stmt)
            assert statement_compiler is not None

            # Test that the statement compiler has the required methods
            assert hasattr(statement_compiler, 'visit_user_defined_type')
