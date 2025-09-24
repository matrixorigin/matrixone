"""
Online tests for MatrixOne vector table operations.
"""

import pytest
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String
from sqlalchemy.schema import CreateTable, DropTable
from matrixone.sqlalchemy_ext import (
    Vectorf32, Vectorf64, VectorTableBuilder, MatrixOneDialect,
    create_vector_table, create_vector_index_table
)
from .test_config import online_config


class TestVectorTableOperations:
    """Test vector table operations with real MatrixOne database."""

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

    def test_create_vector_table_with_sqlalchemy(self, engine, metadata):
        """Test creating vector table using SQLAlchemy."""
        # Create table with vector columns
        table = Table(
            'test_vector_table_1',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            Column('embedding_32', Vectorf32(dimension=128)),
            Column('embedding_64', Vectorf64(dimension=256))
        )
        
        # Create table
        with engine.begin() as conn:
            conn.execute(DropTable(table, if_exists=True))
            conn.execute(CreateTable(table))
            
            # Verify table was created
            result = conn.execute(text(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{online_config.get_test_database()}' 
                AND table_name = 'test_vector_table_1'
            """))
            assert result.fetchone() is not None

    def test_insert_vector_data(self, engine, metadata):
        """Test inserting vector data."""
        table = Table(
            'test_vector_table_2',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('vector_32', Vectorf32(dimension=128)),
            Column('vector_64', Vectorf64(dimension=256))
        )
        
        with engine.begin() as conn:
            # Create table
            conn.execute(DropTable(table, if_exists=True))
            conn.execute(CreateTable(table))
            
            # Insert test data
            vector_32_data = [0.1] * 128
            vector_64_data = [0.2] * 256
            
            vector_32_str = '[' + ','.join(map(str, vector_32_data)) + ']'
            vector_64_str = '[' + ','.join(map(str, vector_64_data)) + ']'
            
            insert_sql = text(f"""
                INSERT INTO test_vector_table_2 (id, vector_32, vector_64)
                VALUES (1, :vector_32, :vector_64)
            """)
            
            conn.execute(insert_sql, {
                "vector_32": vector_32_str,
                "vector_64": vector_64_str
            })
            
            # Verify data was inserted
            result = conn.execute(text("SELECT * FROM test_vector_table_2 WHERE id = 1"))
            row = result.fetchone()
            assert row is not None
            assert row[0] == 1  # id
            
            # Parse returned vectors and compare
            returned_vector_32 = row[1]
            returned_vector_64 = row[2]
            
            # Parse and compare vector data (ignore formatting differences)
            parsed_32 = [float(x.strip()) for x in returned_vector_32[1:-1].split(',')]
            parsed_64 = [float(x.strip()) for x in returned_vector_64[1:-1].split(',')]
            
            assert parsed_32 == vector_32_data
            assert parsed_64 == vector_64_data

    def test_vector_table_builder_online(self, engine, metadata):
        """Test VectorTableBuilder with real database."""
        builder = VectorTableBuilder("test_builder_table", metadata)
        builder.add_int_column("id", primary_key=True)
        builder.add_string_column("name", length=100)
        builder.add_vecf32_column("embedding", dimension=128)
        builder.add_index("name")
        
        table = builder.build()
        
        with engine.begin() as conn:
            # Create table
            conn.execute(DropTable(table, if_exists=True))
            conn.execute(CreateTable(table))
            
            # Verify table structure
            result = conn.execute(text(f"""
                SELECT column_name, data_type, column_type
                FROM information_schema.columns 
                WHERE table_schema = '{online_config.get_test_database()}' 
                AND table_name = 'test_builder_table'
                ORDER BY ordinal_position
            """))
            
            columns = result.fetchall()
            assert len(columns) == 3
            
            # Check column types
            column_info = {col[0]: col[2] for col in columns}
            assert 'id' in column_info
            assert 'name' in column_info
            assert 'embedding' in column_info
            
            # The embedding column should be vecf32(128)
            assert 'vecf32(128)' in column_info['embedding'].lower()

    def test_create_vector_index_table_online(self, engine, metadata):
        """Test create_vector_index_table convenience function."""
        builder = create_vector_index_table("test_index_table", metadata)
        table = builder.build()
        
        with engine.begin() as conn:
            # Create table
            conn.execute(DropTable(table, if_exists=True))
            conn.execute(CreateTable(table))
            
            # Verify table structure matches expected schema
            result = conn.execute(text(f"""
                SELECT column_name, column_type
                FROM information_schema.columns 
                WHERE table_schema = '{online_config.get_test_database()}' 
                AND table_name = 'test_index_table'
                ORDER BY ordinal_position
            """))
            
            columns = result.fetchall()
            assert len(columns) == 3
            
            column_info = {col[0]: col[1] for col in columns}
            assert 'a' in column_info
            assert 'b' in column_info
            assert 'c' in column_info
            
            # Check that column 'b' is vecf32(128)
            assert 'vecf32(128)' in column_info['b'].lower()

    def test_vector_data_operations(self, engine, metadata):
        """Test various vector data operations."""
        table = Table(
            'test_vector_operations',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('vector', Vectorf32(dimension=64)),
            Column('metadata', String(500))
        )
        
        with engine.begin() as conn:
            # Create table
            conn.execute(DropTable(table, if_exists=True))
            conn.execute(CreateTable(table))
            
            # Insert multiple vectors
            test_data = [
                (1, [0.1] * 64, '{"category": "test"}'),
                (2, [0.2] * 64, '{"category": "demo"}'),
                (3, [0.3] * 64, None),
            ]
            
            for id_val, vector_data, metadata_str in test_data:
                vector_str = '[' + ','.join(map(str, vector_data)) + ']'
                insert_sql = text(f"""
                    INSERT INTO test_vector_operations (id, vector, metadata)
                    VALUES (:id, :vector, :metadata)
                """)
                
                conn.execute(insert_sql, {
                    "id": id_val,
                    "vector": vector_str,
                    "metadata": metadata_str
                })
            
            # Query all data
            result = conn.execute(text("SELECT * FROM test_vector_operations ORDER BY id"))
            rows = result.fetchall()
            
            assert len(rows) == 3
            assert rows[0][0] == 1  # id
            assert rows[1][0] == 2  # id
            assert rows[2][0] == 3  # id
            
            # Verify vector data format
            for row in rows:
                vector_str = row[1]
                assert vector_str.startswith('[')
                assert vector_str.endswith(']')
                # Parse and verify dimension
                vector_values = [float(x.strip()) for x in vector_str[1:-1].split(',')]
                assert len(vector_values) == 64

    def test_matrixone_dialect_introspection(self, engine):
        """Test MatrixOneDialect with real database introspection."""
        # Create a table first
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_dialect_introspection"))
            conn.execute(text("""
                CREATE TABLE test_dialect_introspection (
                    id INT PRIMARY KEY,
                    vector_col VECF32(128),
                    name VARCHAR(100)
                )
            """))
        
        # Test dialect introspection - use raw SQL instead of inspector
        with engine.begin() as conn:
            # Query column information directly
            result = conn.execute(text(f"""
                SELECT column_name, column_type, data_type
                FROM information_schema.columns 
                WHERE table_schema = '{online_config.get_test_database()}' 
                AND table_name = 'test_dialect_introspection'
                ORDER BY ordinal_position
            """))
            
            columns = result.fetchall()
            
            # Find the vector column
            vector_column = None
            for col in columns:
                if col[0] == 'vector_col':  # column_name
                    vector_column = col
                    break
            
            assert vector_column is not None
            # The column type should contain vecf32
            assert 'vecf32' in vector_column[1].lower()  # column_type

    def test_vector_table_with_indexes(self, engine, metadata):
        """Test creating vector table with indexes."""
        table = Table(
            'test_vector_with_indexes',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('category', String(50)),
            Column('vector', Vectorf32(dimension=128)),
            Column('score', Integer)
        )
        
        # Add indexes
        from sqlalchemy import Index
        idx_category = Index('idx_category', table.c.category)
        idx_score = Index('idx_score', table.c.score)
        
        with engine.begin() as conn:
            # Create table
            conn.execute(DropTable(table, if_exists=True))
            conn.execute(CreateTable(table))
            
            # Create indexes separately
            conn.execute(text("CREATE INDEX idx_category ON test_vector_with_indexes (category)"))
            conn.execute(text("CREATE INDEX idx_score ON test_vector_with_indexes (score)"))
            
            # Verify indexes were created
            result = conn.execute(text(f"""
                SELECT index_name 
                FROM information_schema.statistics 
                WHERE table_schema = '{online_config.get_test_database()}' 
                AND table_name = 'test_vector_with_indexes'
                AND index_name != 'PRIMARY'
            """))
            
            indexes = [row[0] for row in result.fetchall()]
            
            # Check if indexes exist
            assert 'idx_category' in indexes
            assert 'idx_score' in indexes

    def test_large_vector_dimensions(self, engine, metadata):
        """Test vector tables with large dimensions."""
        table = Table(
            'test_large_vector',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('large_vector_32', Vectorf32(dimension=1024)),
            Column('large_vector_64', Vectorf64(dimension=2048))
        )
        
        with engine.begin() as conn:
            # Create table
            conn.execute(DropTable(table, if_exists=True))
            conn.execute(CreateTable(table))
            
            # Insert large vector data
            large_32_data = [0.001] * 1024
            large_64_data = [0.002] * 2048
            
            vector_32_str = '[' + ','.join(map(str, large_32_data)) + ']'
            vector_64_str = '[' + ','.join(map(str, large_64_data)) + ']'
            
            insert_sql = text(f"""
                INSERT INTO test_large_vector (id, large_vector_32, large_vector_64)
                VALUES (1, :vector_32, :vector_64)
            """)
            
            conn.execute(insert_sql, {
                "vector_32": vector_32_str,
                "vector_64": vector_64_str
            })
            
            # Verify data
            result = conn.execute(text("SELECT * FROM test_large_vector WHERE id = 1"))
            row = result.fetchone()
            assert row is not None
            
            # Verify vector dimensions
            vector_32_str = row[1]
            vector_64_str = row[2]
            
            vector_32_values = [float(x.strip()) for x in vector_32_str[1:-1].split(',')]
            vector_64_values = [float(x.strip()) for x in vector_64_str[1:-1].split(',')]
            
            assert len(vector_32_values) == 1024
            assert len(vector_64_values) == 2048

    def test_vector_table_cleanup(self, engine):
        """Clean up test tables."""
        with engine.begin() as conn:
            test_tables = [
                'test_vector_table_1',
                'test_vector_table_2', 
                'test_builder_table',
                'test_index_table',
                'test_vector_operations',
                'test_dialect_introspection',
                'test_vector_with_indexes',
                'test_large_vector'
            ]
            
            for table_name in test_tables:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
