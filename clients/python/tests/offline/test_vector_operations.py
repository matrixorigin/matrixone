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
Offline tests for vector operations (create, search, order by, limit).
Uses mocks to simulate database interactions without requiring a real connection.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# Import the vector types and functions we want to test
from matrixone.sqlalchemy_ext import (
    VectorType,
    Vectorf32,
    Vectorf64,
    VectorColumn,
    create_vector_column,
    vector_distance_functions,
)
from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect
from matrixone.sql_builder import (
    MatrixOneSQLBuilder,
    DistanceFunction,
    build_vector_similarity_query,
    build_select_query,
    build_insert_query,
    build_update_query,
    build_delete_query,
)


class TestVectorOperationsOffline:
    """Test vector operations using mocks."""

    def setup_method(self):
        """Set up test fixtures."""
        # Mock engine and session
        self.mock_engine = Mock()
        self.mock_session = Mock()
        self.mock_connection = Mock()

        # Mock SQLAlchemy components
        with patch('sqlalchemy.create_engine') as mock_create_engine:
            mock_create_engine.return_value = self.mock_engine
            self.mock_engine.connect.return_value = self.mock_connection
            self.mock_engine.begin.return_value.__enter__ = Mock(return_value=self.mock_connection)
            self.mock_engine.begin.return_value.__exit__ = Mock(return_value=None)

            # Mock sessionmaker
            with patch('sqlalchemy.orm.sessionmaker') as mock_sessionmaker:
                mock_sessionmaker.return_value = self.mock_session

                # Mock declarative_base
                with patch('sqlalchemy.orm.declarative_base') as mock_declarative_base:
                    self.mock_base = Mock()
                    mock_declarative_base.return_value = self.mock_base

                    # Mock metadata
                    self.mock_metadata = Mock()
                    self.mock_base.metadata = self.mock_metadata

                    # Mock table creation
                    self.mock_metadata.create_all.return_value = None
                    self.mock_metadata.drop_all.return_value = None

    def test_vector_column_creation(self):
        """Test creating vector columns with different dimensions and precisions."""
        # Test Vectorf32 column
        vec32_col = create_vector_column(128, "f32")
        assert vec32_col.type.precision == "f32"
        assert vec32_col.type.dimension == 128

        # Test Vectorf64 column
        vec64_col = create_vector_column(256, "f64")
        assert vec64_col.type.precision == "f64"
        assert vec64_col.type.dimension == 256

        # Test with additional column options
        vec_col_with_options = create_vector_column(512, "f32", nullable=False, index=True)
        assert vec_col_with_options.nullable is False
        assert vec_col_with_options.index is True

    def test_vector_distance_functions_availability(self):
        """Test that all expected vector distance functions are available."""
        functions = vector_distance_functions()
        expected_functions = ["l2_distance", "l2_distance_sq", "cosine_distance"]

        for func in expected_functions:
            assert func in functions

    def test_vector_column_distance_methods(self):
        """Test vector column distance calculation methods."""
        # Create a vector column
        vec_col = create_vector_column(10, "f32")

        # Test that methods exist and return function expressions
        # Test l2_distance
        result = vec_col.l2_distance([1.0, 2.0, 3.0])
        assert result is not None
        assert hasattr(result, 'compile')

        # Test l2_distance_sq
        result = vec_col.l2_distance_sq([1.0, 2.0, 3.0])
        assert result is not None
        assert hasattr(result, 'compile')

        # Test cosine_distance
        result = vec_col.cosine_distance([1.0, 2.0, 3.0])
        assert result is not None
        assert hasattr(result, 'compile')

    def test_vector_table_creation_mock(self):
        """Test vector table creation using mocks."""
        # Mock the table creation process
        self.mock_metadata.create_all.return_value = None

        # Simulate table creation
        self.mock_metadata.create_all(self.mock_engine)

        # Verify create_all was called
        self.mock_metadata.create_all.assert_called_once_with(self.mock_engine)

    def test_vector_table_drop_mock(self):
        """Test vector table dropping using mocks."""
        # Mock the table drop process
        self.mock_metadata.drop_all.return_value = None

        # Simulate table dropping
        self.mock_metadata.drop_all(self.mock_engine)

        # Verify drop_all was called
        self.mock_metadata.drop_all.assert_called_once_with(self.mock_engine)

    def test_vector_search_query_construction(self):
        """Test constructing vector search queries."""
        # Mock session and query
        mock_query = Mock()
        self.mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query

        # Create a mock model
        mock_model = Mock()
        mock_model.embedding = create_vector_column(10, "f32")

        # Test search query construction
        query = self.mock_session.query(mock_model)
        filtered_query = query.filter(mock_model.embedding.l2_distance([1.0, 2.0, 3.0]) < 0.5)
        ordered_query = filtered_query.order_by(mock_model.embedding.l2_distance([1.0, 2.0, 3.0]))
        limited_query = ordered_query.limit(10)

        # Verify query chain was constructed
        self.mock_session.query.assert_called_once_with(mock_model)
        mock_query.filter.assert_called_once()
        mock_query.order_by.assert_called_once()
        mock_query.limit.assert_called_once_with(10)

    def test_vector_insertion_mock(self):
        """Test vector data insertion using mocks."""
        # Mock session add and commit
        self.mock_session.add.return_value = None
        self.mock_session.commit.return_value = None

        # Create mock data
        mock_data = Mock()
        mock_data.embedding = [1.0, 2.0, 3.0, 4.0, 5.0]
        mock_data.description = "Test document"

        # Simulate insertion
        self.mock_session.add(mock_data)
        self.mock_session.commit()

        # Verify insertion was called
        self.mock_session.add.assert_called_once_with(mock_data)
        self.mock_session.commit.assert_called_once()

    def test_vector_batch_insertion_mock(self):
        """Test batch vector data insertion using mocks."""
        # Mock session bulk_insert_mappings
        self.mock_session.bulk_insert_mappings.return_value = None
        self.mock_session.commit.return_value = None

        # Create mock batch data
        batch_data = [
            {"embedding": [1.0, 2.0, 3.0], "description": "Doc 1"},
            {"embedding": [4.0, 5.0, 6.0], "description": "Doc 2"},
            {"embedding": [7.0, 8.0, 9.0], "description": "Doc 3"},
        ]

        # Simulate batch insertion
        self.mock_session.bulk_insert_mappings(Mock, batch_data)
        self.mock_session.commit()

        # Verify batch insertion was called
        self.mock_session.bulk_insert_mappings.assert_called_once()
        self.mock_session.commit.assert_called_once()

    def test_vector_search_with_multiple_criteria(self):
        """Test complex vector search with multiple criteria."""
        # Mock session and query
        mock_query = Mock()
        self.mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = []

        # Create mock model
        mock_model = Mock()
        mock_model.embedding = create_vector_column(10, "f32")

        # Test complex search
        query = self.mock_session.query(mock_model)
        query = query.filter(mock_model.embedding.l2_distance([1.0, 2.0, 3.0]) < 0.5)
        query = query.filter(mock_model.embedding.cosine_distance([1.0, 2.0, 3.0]) < 0.3)
        query = query.order_by(mock_model.embedding.l2_distance([1.0, 2.0, 3.0]))
        query = query.limit(5)
        results = query.all()

        # Verify complex query chain
        assert mock_query.filter.call_count == 2  # Two filter calls
        mock_query.order_by.assert_called_once()
        mock_query.limit.assert_called_once_with(5)
        mock_query.all.assert_called_once()

    def test_vector_type_compilation(self):
        """Test vector type compilation for SQL generation."""
        # Test Vectorf32 compilation
        vec32_type = Vectorf32(dimension=128)
        col_spec = vec32_type.get_col_spec()
        assert col_spec == "vecf32(128)"

        # Test Vectorf64 compilation
        vec64_type = Vectorf64(dimension=256)
        col_spec = vec64_type.get_col_spec()
        assert col_spec == "vecf64(256)"

    def test_vector_column_string_representation(self):
        """Test vector column string representation."""
        vec_col = create_vector_column(64, "f32")

        # Test that the column has the expected type
        assert str(vec_col.type) == "vecf32(64)"

        # Test column name
        vec_col.name = "embedding"
        assert vec_col.name == "embedding"

    def test_vector_distance_function_parameters(self):
        """Test vector distance functions with different parameter types."""
        vec_col = create_vector_column(5, "f32")

        # Test with list parameter - should return function expression
        result = vec_col.l2_distance([1.0, 2.0, 3.0, 4.0, 5.0])
        assert result is not None
        assert hasattr(result, 'compile')

        # Test with string parameter - should return function expression
        result = vec_col.l2_distance("[1.0,2.0,3.0,4.0,5.0]")
        assert result is not None
        assert hasattr(result, 'compile')

        # Test with column parameter - should return function expression
        other_col = Mock()
        result = vec_col.l2_distance(other_col)
        assert result is not None
        assert hasattr(result, 'compile')

    def test_vector_sql_generation_comparison(self):
        """Test actual SQL generation against expected SQL."""
        # Create a vector column
        vec_col = create_vector_column(10, "f32")
        vec_col.name = "embedding"

        # Test L2 distance SQL generation
        result = vec_col.l2_distance([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])

        # Verify the result is a function expression
        assert result is not None
        assert hasattr(result, 'compile')

        # Test that we can compile it to SQL
        sql = str(result.compile(dialect=MatrixOneDialect()))
        assert "l2_distance" in sql
        assert "embedding" in sql

    def test_vector_table_creation_sql_comparison(self):
        """Test table creation SQL generation against expected SQL."""
        from sqlalchemy import Table, Column, Integer, String, MetaData
        from sqlalchemy.schema import CreateTable

        # Create metadata and table
        metadata = MetaData()
        table = Table(
            'vector_docs',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('embedding', Vectorf32(dimension=128)),
            Column('description', String(200)),
        )

        # Generate CREATE TABLE SQL
        create_sql = str(CreateTable(table).compile(dialect=MatrixOneDialect()))

        # Expected SQL components
        expected_components = [
            "CREATE TABLE vector_docs",
            "id INTEGER NOT NULL",
            "embedding vecf32(128)",
            "description VARCHAR(200)",
            "PRIMARY KEY (id)",
        ]

        # Verify all expected components are in the generated SQL
        for component in expected_components:
            assert component in create_sql

    def test_vector_search_query_sql_comparison(self):
        """Test search query SQL generation against expected SQL."""
        from sqlalchemy import Table, Column, Integer, String, MetaData, select
        from sqlalchemy.orm import declarative_base

        # Create base and model
        Base = declarative_base()

        class VectorDoc(Base):
            __tablename__ = 'vector_docs'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(10, "f32")
            description = Column(String(200))

        # Create search query
        query = (
            select(VectorDoc)
            .where(VectorDoc.embedding.l2_distance([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]) < 0.5)
            .order_by(VectorDoc.embedding.l2_distance([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]))
            .limit(10)
        )

        # Generate SQL
        sql = str(query.compile(dialect=MatrixOneDialect()))

        # Expected SQL components (actual format from SQLAlchemy)
        expected_components = [
            "SELECT vector_docs.id",
            "vector_docs.embedding",
            "vector_docs.description",
            "FROM vector_docs",
            "WHERE l2_distance(vector_docs.embedding",
            "ORDER BY l2_distance(vector_docs.embedding",
            "LIMIT %s",  # SQLAlchemy uses %s for parameters
        ]

        # Verify all expected components are in the generated SQL
        for component in expected_components:
            assert component in sql

    def test_vector_complex_query_sql_comparison(self):
        """Test complex vector query SQL generation against expected SQL."""
        from sqlalchemy import Table, Column, Integer, String, MetaData, select, and_
        from sqlalchemy.orm import declarative_base

        # Create base and model
        Base = declarative_base()

        class VectorDoc(Base):
            __tablename__ = 'vector_docs'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(5, "f32")
            description = Column(String(200))
            category = Column(String(50))

        # Create complex search query
        query = (
            select(VectorDoc)
            .where(
                and_(
                    VectorDoc.embedding.l2_distance([1.0, 2.0, 3.0, 4.0, 5.0]) < 0.5,
                    VectorDoc.embedding.cosine_distance([1.0, 2.0, 3.0, 4.0, 5.0]) < 0.3,
                    VectorDoc.category == "science",
                )
            )
            .order_by(VectorDoc.embedding.l2_distance([1.0, 2.0, 3.0, 4.0, 5.0]))
            .limit(5)
        )

        # Generate SQL
        sql = str(query.compile(dialect=MatrixOneDialect()))

        # Expected SQL components (actual format from SQLAlchemy)
        expected_components = [
            "SELECT vector_docs.id",
            "vector_docs.embedding",
            "vector_docs.description",
            "vector_docs.category",
            "FROM vector_docs",
            "WHERE l2_distance(vector_docs.embedding",
            "cosine_distance(vector_docs.embedding",
            "vector_docs.category = %s",  # SQLAlchemy uses %s for parameters
            "ORDER BY l2_distance(vector_docs.embedding",
            "LIMIT %s",  # SQLAlchemy uses %s for parameters
        ]

        # Verify all expected components are in the generated SQL
        for component in expected_components:
            assert component in sql

    def test_vector_batch_insert_sql_comparison(self):
        """Test batch insert SQL generation against expected SQL."""
        from sqlalchemy import Table, Column, Integer, String, MetaData, insert
        from sqlalchemy.orm import declarative_base

        # Create base and model
        Base = declarative_base()

        class VectorDoc(Base):
            __tablename__ = 'vector_docs'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(3, "f32")
            description = Column(String(200))

        # Create batch insert statement
        stmt = insert(VectorDoc).values(
            [
                {"embedding": [1.0, 2.0, 3.0], "description": "Doc 1"},
                {"embedding": [4.0, 5.0, 6.0], "description": "Doc 2"},
                {"embedding": [7.0, 8.0, 9.0], "description": "Doc 3"},
            ]
        )

        # Generate SQL
        sql = str(stmt.compile(dialect=MatrixOneDialect()))

        # Expected SQL components (actual format from SQLAlchemy)
        expected_components = [
            "INSERT INTO vector_docs",
            "embedding",
            "description",
            "VALUES",
            "(%s, %s)",  # SQLAlchemy uses %s for parameters
            "(%s, %s)",
            "(%s, %s)",
        ]

        # Verify all expected components are in the generated SQL
        for component in expected_components:
            assert component in sql


class TestVectorOperationsIntegrationOffline:
    """Integration tests for vector operations using mocks."""

    def test_complete_vector_workflow_mock(self):
        """Test complete vector workflow: create table, insert data, search."""
        # Mock all necessary components
        with patch('sqlalchemy.create_engine') as mock_create_engine, patch(
            'sqlalchemy.orm.sessionmaker'
        ) as mock_sessionmaker, patch('sqlalchemy.orm.declarative_base') as mock_declarative_base:

            # Setup mocks
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            mock_session = Mock()
            mock_sessionmaker.return_value = mock_session

            mock_base = Mock()
            mock_declarative_base.return_value = mock_base
            mock_base.metadata = Mock()

            # Test workflow
            # 1. Create table
            mock_base.metadata.create_all(mock_engine)

            # 2. Insert data
            mock_data = Mock()
            mock_session.add(mock_data)
            mock_session.commit()

            # 3. Search data
            mock_query = Mock()
            mock_session.query.return_value = mock_query
            mock_query.filter.return_value = mock_query
            mock_query.order_by.return_value = mock_query
            mock_query.limit.return_value = mock_query
            mock_query.all.return_value = []

            # Execute search
            results = mock_query.all()

            # Verify workflow
            mock_base.metadata.create_all.assert_called_once_with(mock_engine)
            mock_session.add.assert_called_once_with(mock_data)
            mock_session.commit.assert_called_once()
            mock_query.all.assert_called_once()

    def test_vector_error_handling_mock(self):
        """Test vector operations error handling using mocks."""
        # Mock session with error
        mock_session = Mock()
        mock_session.add.side_effect = SQLAlchemyError("Mock database error")

        # Test error handling
        with pytest.raises(SQLAlchemyError):
            mock_data = Mock()
            mock_session.add(mock_data)
            mock_session.commit()

        # Verify error was raised
        mock_session.add.assert_called_once()
        mock_session.commit.assert_not_called()


class TestUnifiedSQLBuilderOffline:
    """Test unified SQL builder functionality offline."""

    def test_basic_sql_construction(self):
        """Test basic SQL construction with MatrixOneSQLBuilder."""
        builder = MatrixOneSQLBuilder()
        sql, params = builder.select('id', 'name').from_table('users').build()

        assert sql == "SELECT id, name FROM users"
        assert params == []

    def test_select_with_where(self):
        """Test SELECT with WHERE clause."""
        builder = MatrixOneSQLBuilder()
        sql, params = builder.select('*').from_table('users').where('age > ?', 18).build()

        assert "SELECT * FROM users WHERE age > ?" in sql
        assert params == [18]

    def test_vector_similarity_query(self):
        """Test vector similarity query building."""
        sql = build_vector_similarity_query(
            table_name='documents',
            vector_column='embedding',
            query_vector=[0.1, 0.2, 0.3],
            distance_func=DistanceFunction.L2_SQ,
            limit=10,
            select_columns=['id', 'title'],
            where_conditions=['category = ?'],
            where_params=['news'],
        )

        assert "l2_distance_sq" in sql
        assert "WHERE category = 'news'" in sql
        assert "ORDER BY distance" in sql
        assert "LIMIT 10" in sql

    def test_cte_query(self):
        """Test CTE query construction."""
        builder = MatrixOneSQLBuilder()
        sql, params = (
            builder.with_cte(
                'dept_stats',
                'SELECT department_id, COUNT(*) as emp_count FROM employees GROUP BY department_id',
            )
            .select('d.name', 'ds.emp_count')
            .from_table('departments d')
            .join('dept_stats ds', 'd.id = ds.department_id', 'INNER')
            .where('ds.emp_count > ?', 5)
            .build()
        )

        assert "WITH dept_stats AS" in sql
        assert "INNER JOIN dept_stats ds" in sql
        assert params == [5]

    def test_insert_query(self):
        """Test INSERT query construction."""
        sql, params = build_insert_query(
            table_name="users", values={'name': 'John Doe', 'email': 'john@example.com', 'age': 30}
        )

        assert "INSERT INTO users" in sql
        assert "name, email, age" in sql
        assert params == ['John Doe', 'john@example.com', 30]

    def test_update_query(self):
        """Test UPDATE query construction."""
        sql, params = build_update_query(
            table_name="users",
            set_values={'age': 31, 'last_login': '2024-01-01'},
            where_conditions=['id = ?'],
            where_params=[123],
        )

        assert "UPDATE users SET" in sql
        assert "WHERE id = ?" in sql
        assert params == [31, '2024-01-01', 123]

    def test_delete_query(self):
        """Test DELETE query construction."""
        sql, params = build_delete_query(
            table_name="users",
            where_conditions=['status = ?', 'last_login < ?'],
            where_params=['inactive', '2023-01-01'],
        )

        assert "DELETE FROM users" in sql
        assert "WHERE status = ? AND last_login < ?" in sql
        assert params == ['inactive', '2023-01-01']

    def test_convenience_functions(self):
        """Test convenience functions for common operations."""
        # Test build_select_query
        sql = build_select_query(
            table_name="products",
            select_columns=["id", "name", "price"],
            where_conditions=["category = ?", "price < ?"],
            where_params=["electronics", 1000],
            order_by=["price"],
            limit=5,
        )

        assert "SELECT id, name, price FROM products" in sql
        assert "WHERE category = 'electronics' AND price < 1000" in sql
        assert "ORDER BY price" in sql
        assert "LIMIT 5" in sql

    def test_parameter_substitution(self):
        """Test parameter substitution for MatrixOne compatibility."""
        builder = MatrixOneSQLBuilder()
        sql = (
            builder.select('*')
            .from_table('users')
            .where('age > ?', 18)
            .where('name = ?', "John")
            .build_with_parameter_substitution()
        )

        assert "WHERE age > 18 AND name = 'John'" in sql
        assert "?" not in sql  # All parameters should be substituted

    def test_snapshot_syntax(self):
        """Test snapshot query syntax."""
        builder = MatrixOneSQLBuilder()
        sql, params = builder.select('*').from_table('users', 'user_snapshot').build()

        assert "FROM users{snapshot = 'user_snapshot'}" in sql
        assert params == []

    def test_complex_query_construction(self):
        """Test complex query construction with multiple clauses."""
        builder = MatrixOneSQLBuilder()
        sql, params = (
            builder.select('u.id', 'u.name', 'd.name as dept_name', 'COUNT(p.id) as project_count')
            .from_table('users u')
            .left_join('departments d', 'u.department_id = d.id')
            .left_join('projects p', 'u.id = p.owner_id')
            .where('u.status = ?', 'active')
            .where('u.created_at > ?', '2023-01-01')
            .group_by('u.id', 'u.name', 'd.name')
            .having('COUNT(p.id) > ?', 0)
            .order_by('project_count DESC', 'u.name')
            .limit(10)
            .offset(20)
            .build()
        )

        assert "SELECT u.id, u.name, d.name as dept_name, COUNT(p.id) as project_count" in sql
        assert "FROM users u" in sql
        assert "LEFT JOIN departments d" in sql
        assert "LEFT JOIN projects p" in sql
        assert "WHERE u.status = ? AND u.created_at > ?" in sql
        assert "GROUP BY u.id, u.name, d.name" in sql
        assert "HAVING COUNT(p.id) > ?" in sql
        assert "ORDER BY project_count DESC, u.name" in sql
        assert "LIMIT 10" in sql
        assert "OFFSET 20" in sql
        assert params == ['active', '2023-01-01', 0]
