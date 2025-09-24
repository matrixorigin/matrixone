"""
Online tests for SQLAlchemy vector extensions with MatrixOne.
"""

import pytest
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Index
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.orm import declarative_base, sessionmaker
from matrixone.sqlalchemy_ext import (
    VectorType, Vectorf32, Vectorf64, VectorTypeDecorator,
    MatrixOneDialect
)
from .test_config import online_config


class TestSQLAlchemyVectorExtensions:
    """Test SQLAlchemy vector extensions with real MatrixOne database."""

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

    def test_vector_type_serialization_deserialization(self, engine, metadata):
        """Test vector type data serialization and deserialization."""
        table = Table(
            'test_vector_serialization',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('vector_32', Vectorf32(dimension=128)),
            Column('vector_64', Vectorf64(dimension=256))
        )
        
        with engine.begin() as conn:
            # Create table
            conn.execute(DropTable(table, if_exists=True))
            conn.execute(CreateTable(table))
            
            # Test vector type processors
            vec32 = Vectorf32(dimension=128)
            vec64 = Vectorf64(dimension=256)
            
            # Test bind processors
            bind_processor_32 = vec32.bind_processor(None)
            bind_processor_64 = vec64.bind_processor(None)
            
            # Test list to string conversion
            test_vector_32 = [0.1, 0.2, 0.3] + [0.0] * 125  # 128 dimensions
            test_vector_64 = [0.4, 0.5, 0.6] + [0.0] * 253  # 256 dimensions
            
            vector_32_str = bind_processor_32(test_vector_32)
            vector_64_str = bind_processor_64(test_vector_64)
            
            assert vector_32_str.startswith('[')
            assert vector_32_str.endswith(']')
            assert vector_64_str.startswith('[')
            assert vector_64_str.endswith(']')
            
            # Insert data using processors
            insert_sql = text(f"""
                INSERT INTO test_vector_serialization (id, vector_32, vector_64)
                VALUES (1, :vector_32, :vector_64)
            """)
            
            conn.execute(insert_sql, {
                "vector_32": vector_32_str,
                "vector_64": vector_64_str
            })
            
            # Test result processors
            result_processor_32 = vec32.result_processor(None, None)
            result_processor_64 = vec64.result_processor(None, None)
            
            # Query and process results
            result = conn.execute(text("SELECT vector_32, vector_64 FROM test_vector_serialization WHERE id = 1"))
            row = result.fetchone()
            
            processed_32 = result_processor_32(row[0])
            processed_64 = result_processor_64(row[1])
            
            assert isinstance(processed_32, list)
            assert isinstance(processed_64, list)
            assert len(processed_32) == 128
            assert len(processed_64) == 256

    def test_vector_type_decorator_online(self, engine, metadata):
        """Test VectorTypeDecorator with real database."""
        table = Table(
            'test_vector_decorator',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('vector', VectorTypeDecorator(dimension=64, precision="f32"))
        )
        
        with engine.begin() as conn:
            # Create table
            conn.execute(DropTable(table, if_exists=True))
            conn.execute(CreateTable(table))
            
            # Test decorator functionality
            decorator = VectorTypeDecorator(dimension=64, precision="f32")
            
            # Test bind parameter processing
            test_vector = [0.1] * 64
            bind_result = decorator.process_bind_param(test_vector, None)
            assert bind_result.startswith('[')
            assert bind_result.endswith(']')
            
            # Insert data
            insert_sql = text(f"""
                INSERT INTO test_vector_decorator (id, vector)
                VALUES (1, :vector)
            """)
            
            conn.execute(insert_sql, {"vector": bind_result})
            
            # Test result value processing
            result = conn.execute(text("SELECT vector FROM test_vector_decorator WHERE id = 1"))
            row = result.fetchone()
            
            processed_result = decorator.process_result_value(row[0], None)
            assert isinstance(processed_result, list)
            assert len(processed_result) == 64

    def test_matrixone_dialect_with_real_db(self, engine):
        """Test MatrixOneDialect with real database operations."""
        dialect = MatrixOneDialect()
        
        # Test vector type creation from various string formats
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

    def test_vector_type_compilation(self, engine):
        """Test vector type compilation."""
        # Test vector type compilation directly
        vector_32 = Vectorf32(dimension=128)
        vector_64 = Vectorf64(dimension=256)
        
        # Test get_col_spec method
        spec_32 = vector_32.get_col_spec()
        spec_64 = vector_64.get_col_spec()
        
        assert spec_32 == "vecf32(128)"
        assert spec_64 == "vecf64(256)"

    def test_declarative_base_with_vector_types(self, engine):
        """Test using vector types with SQLAlchemy declarative base."""
        Base = declarative_base()
        
        class VectorModel(Base):
            __tablename__ = 'test_declarative_vector'
            
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            embedding_32 = Column(Vectorf32(dimension=128))
            embedding_64 = Column(Vectorf64(dimension=256))
        
        # Create tables
        Base.metadata.create_all(engine)
        
        try:
            # Test insertion using ORM
            Session = sessionmaker(bind=engine)
            session = Session()
            
            # Create test data
            test_model = VectorModel(
                name="test_item",
                embedding_32=[0.1] * 128,
                embedding_64=[0.2] * 256
            )
            
            session.add(test_model)
            session.commit()
            
            # Query back
            retrieved = session.query(VectorModel).filter_by(name="test_item").first()
            assert retrieved is not None
            assert retrieved.name == "test_item"
            assert isinstance(retrieved.embedding_32, list)
            assert isinstance(retrieved.embedding_64, list)
            assert len(retrieved.embedding_32) == 128
            assert len(retrieved.embedding_64) == 256
            
            session.close()
            
        finally:
            # Clean up
            Base.metadata.drop_all(engine)

    def test_vector_type_edge_cases_online(self, engine, metadata):
        """Test edge cases with vector types in real database."""
        table = Table(
            'test_vector_edge_cases',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('small_vector', Vectorf32(dimension=1)),
            Column('medium_vector', Vectorf32(dimension=1000)),  # Use manageable size
            Column('variable_vector', Vectorf32(dimension=3))  # Use fixed dimension instead
        )
        
        with engine.begin() as conn:
            # Create table
            conn.execute(DropTable(table, if_exists=True))
            conn.execute(CreateTable(table))
            
            # Test small dimension vector
            small_vector_data = [1.0]
            small_vector_str = '[' + ','.join(map(str, small_vector_data)) + ']'
            
            # Test medium dimension vector
            medium_vector_data = [0.001] * 1000
            medium_vector_str = '[' + ','.join(map(str, medium_vector_data)) + ']'
            
            # Test variable dimension vector
            variable_vector_data = [0.5, 0.6, 0.7]
            variable_vector_str = '[' + ','.join(map(str, variable_vector_data)) + ']'
            
            insert_sql = text(f"""
                INSERT INTO test_vector_edge_cases (id, small_vector, medium_vector, variable_vector)
                VALUES (1, :small_vector, :medium_vector, :variable_vector)
            """)
            
            conn.execute(insert_sql, {
                "small_vector": small_vector_str,
                "medium_vector": medium_vector_str,
                "variable_vector": variable_vector_str
            })
            
            # Verify data
            result = conn.execute(text("SELECT * FROM test_vector_edge_cases WHERE id = 1"))
            row = result.fetchone()
            assert row is not None
            
            # Parse and verify dimensions
            small_parsed = [float(x.strip()) for x in row[1][1:-1].split(',')]
            medium_parsed = [float(x.strip()) for x in row[2][1:-1].split(',')]
            variable_parsed = [float(x.strip()) for x in row[3][1:-1].split(',')]
            
            assert len(small_parsed) == 1
            assert len(medium_parsed) == 1000
            assert len(variable_parsed) == 3

    def test_vector_table_with_complex_schema(self, engine, metadata):
        """Test vector table with complex schema including indexes and constraints."""
        table = Table(
            'test_complex_vector_schema',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('document_id', String(100), nullable=False),
            Column('category', String(50)),
            Column('content_embedding', Vectorf32(dimension=384)),
            Column('title_embedding', Vectorf32(dimension=128)),
            Column('metadata_embedding', Vectorf64(dimension=512)),
            Column('created_at', String(50)),
            Column('updated_at', String(50))
        )
        
        # Add multiple indexes
        Index('idx_document_id', table.c.document_id)
        Index('idx_category', table.c.category)
        Index('idx_created_at', table.c.created_at)
        
        with engine.begin() as conn:
            # Create table
            conn.execute(DropTable(table, if_exists=True))
            conn.execute(CreateTable(table))
            
            # Create indexes separately
            conn.execute(text("CREATE INDEX idx_document_id ON test_complex_vector_schema (document_id)"))
            conn.execute(text("CREATE INDEX idx_category ON test_complex_vector_schema (category)"))
            conn.execute(text("CREATE INDEX idx_created_at ON test_complex_vector_schema (created_at)"))
            
            # Insert complex data
            test_data = {
                "id": 1,
                "document_id": "doc_001",
                "category": "technology",
                "content_embedding": [0.1] * 384,
                "title_embedding": [0.2] * 128,
                "metadata_embedding": [0.3] * 512,
                "created_at": "2024-01-01",
                "updated_at": "2024-01-02"
            }
            
            # Convert vector data to strings
            content_str = '[' + ','.join(map(str, test_data["content_embedding"])) + ']'
            title_str = '[' + ','.join(map(str, test_data["title_embedding"])) + ']'
            metadata_str = '[' + ','.join(map(str, test_data["metadata_embedding"])) + ']'
            
            insert_sql = text(f"""
                INSERT INTO test_complex_vector_schema 
                (id, document_id, category, content_embedding, title_embedding, 
                 metadata_embedding, created_at, updated_at)
                VALUES (:id, :document_id, :category, :content_embedding, 
                        :title_embedding, :metadata_embedding, :created_at, :updated_at)
            """)
            
            conn.execute(insert_sql, {
                "id": test_data["id"],
                "document_id": test_data["document_id"],
                "category": test_data["category"],
                "content_embedding": content_str,
                "title_embedding": title_str,
                "metadata_embedding": metadata_str,
                "created_at": test_data["created_at"],
                "updated_at": test_data["updated_at"]
            })
            
            # Verify data and indexes
            result = conn.execute(text("SELECT * FROM test_complex_vector_schema WHERE id = 1"))
            row = result.fetchone()
            assert row is not None
            assert row[1] == "doc_001"
            assert row[2] == "technology"
            
            # Verify indexes exist
            index_result = conn.execute(text(f"""
                SELECT index_name 
                FROM information_schema.statistics 
                WHERE table_schema = '{online_config.get_test_database()}' 
                AND table_name = 'test_complex_vector_schema'
                AND index_name != 'PRIMARY'
            """))
            
            indexes = [row[0] for row in index_result.fetchall()]
            assert 'idx_document_id' in indexes
            assert 'idx_category' in indexes
            assert 'idx_created_at' in indexes

    def test_cleanup_test_tables(self, engine):
        """Clean up all test tables."""
        with engine.begin() as conn:
            test_tables = [
                'test_vector_serialization',
                'test_vector_decorator',
                'test_declarative_vector',
                'test_vector_edge_cases',
                'test_complex_vector_schema'
            ]
            
            for table_name in test_tables:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
