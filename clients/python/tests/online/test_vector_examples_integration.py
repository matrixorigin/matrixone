"""
Online tests for vector examples integration.
Tests vector operations based on examples but with proper test structure.
"""

import pytest
import sys
import os
from sqlalchemy import create_engine, text, select, and_, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from matrixone import Client
from matrixone.sqlalchemy_ext import (
    VectorType, Vectorf32, Vectorf64, VectorColumn, 
    create_vector_column, vector_distance_functions
)
from matrixone.config import get_connection_params
from tests.online.test_config import OnlineTestConfig


class TestVectorExamplesIntegration:
    """Test vector operations based on examples with proper test structure"""
    
    @pytest.fixture(scope="class")
    def client(self):
        """Create and connect MatrixOne client."""
        config = OnlineTestConfig()
        client = Client()
        client.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database
        )
        yield client
        if hasattr(client, 'disconnect'):
            client.disconnect()
    
    @pytest.fixture(scope="class")
    def engine(self, client):
        """Get SQLAlchemy engine from client."""
        return client.get_sqlalchemy_engine()
    
    @pytest.fixture(scope="class")
    def Base(self):
        """Create declarative base for each test class."""
        return declarative_base()
    
    @pytest.fixture(scope="class")
    def Session(self, engine):
        """Create session maker."""
        return sessionmaker(bind=engine)
    
    def setup_method(self):
        """Set up test data and clean up before each test."""
        pass
    
    def teardown_method(self):
        """Clean up after each test."""
        pass

    def test_vector_distance_functions_basic(self, engine):
        """Test basic vector distance functions"""
        
        # Clean up any existing table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_distance_basic_test"))
        
        # Create table with vector column
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE vector_distance_basic_test (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(100),
                    embedding vecf32(128)
                )
            """))
        
        # Insert sample data
        sample_data = [
            ("Document 1", [0.1] * 128),
            ("Document 2", [0.2] * 128),
            ("Document 3", [0.3] * 128),
        ]
        
        with engine.begin() as conn:
            for name, embedding in sample_data:
                embedding_str = "[" + ",".join(map(str, embedding)) + "]"
                conn.execute(text(f"""
                    INSERT INTO vector_distance_basic_test (name, embedding) 
                    VALUES ('{name}', '{embedding_str}')
                """))
        
        # Test L2 distance
        query_vector = [0.15] * 128
        query_str = "[" + ",".join(map(str, query_vector)) + "]"
        
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT name, l2_distance(embedding, '{query_str}') as distance
                FROM vector_distance_basic_test
                ORDER BY l2_distance(embedding, '{query_str}')
                LIMIT 3
            """))
            rows = result.fetchall()
        
        assert len(rows) == 3
        # Document 2 should be closest (distance should be smallest)
        assert rows[0][0] == "Document 2"
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE vector_distance_basic_test"))

    def test_vector_index_creation_basic(self, client, engine):
        """Test basic vector index creation"""
        
        # Clean up any existing table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_index_basic_test"))
        
        # Create table
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE vector_index_basic_test (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    title VARCHAR(200),
                    embedding vecf32(128)
                )
            """))
        
        # Insert sample data
        sample_data = [
            ("ML Guide", [0.1] * 128),
            ("AI Tutorial", [0.2] * 128),
            ("Data Science", [0.3] * 128),
        ]
        
        with engine.begin() as conn:
            for title, embedding in sample_data:
                embedding_str = "[" + ",".join(map(str, embedding)) + "]"
                conn.execute(text(f"""
                    INSERT INTO vector_index_basic_test (title, embedding) 
                    VALUES ('{title}', '{embedding_str}')
                """))
        
        # Create IVFFLAT index
        client.vector_index.create_ivf(
            table_name="vector_index_basic_test",
            name="idx_embedding_basic",
            column="embedding",
            lists=32,
            op_type="vector_l2_ops"
        )
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_index_basic_test"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            assert "idx_embedding_basic" in indexes
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE vector_index_basic_test"))

    def test_hnsw_index_creation_basic(self, client, engine):
        """Test basic HNSW index creation"""
        
        # Clean up any existing table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_hnsw_basic_test"))
        
        # Create table
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE vector_hnsw_basic_test (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    title VARCHAR(200),
                    embedding vecf32(128)
                )
            """))
        
        # Insert sample data
        sample_data = [
            ("HNSW Doc 1", [0.1, 0.2, 0.3] + [0.0] * 125),
            ("HNSW Doc 2", [0.2, 0.3, 0.4] + [0.0] * 125),
            ("HNSW Doc 3", [0.3, 0.4, 0.5] + [0.0] * 125),
        ]
        
        with engine.begin() as conn:
            for title, embedding in sample_data:
                embedding_str = "[" + ",".join(map(str, embedding)) + "]"
                conn.execute(text(f"""
                    INSERT INTO vector_hnsw_basic_test (title, embedding) 
                    VALUES ('{title}', '{embedding_str}')
                """))
        
        # Create HNSW index
        client.vector_index.create_hnsw(
            table_name="vector_hnsw_basic_test",
            name="idx_hnsw_basic",
            column="embedding",
            m=16,
            ef_construction=200,
            ef_search=50,
            op_type="vector_l2_ops"
        )
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_hnsw_basic_test"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            assert "idx_hnsw_basic" in indexes
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE vector_hnsw_basic_test"))

    def test_vector_distance_functions_comprehensive(self, engine):
        """Test comprehensive vector distance functions (example_12)"""
        
        # Clean up any existing table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_distance_comprehensive_test"))
        
        # Create table with vector column
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE vector_distance_comprehensive_test (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(100),
                    embedding vecf32(128)
                )
            """))
        
        # Insert sample data with clearly distinct values
        sample_data = [
            ("Document 1", [0.1] * 128),
            ("Document 2", [0.4] * 128),
            ("Document 3", [0.7] * 128),
            ("Document 4", [0.9] * 128),
            ("Document 5", [1.0] * 128)
        ]
        
        with engine.begin() as conn:
            for name, embedding in sample_data:
                embedding_str = "[" + ",".join(map(str, embedding)) + "]"
                conn.execute(text(f"""
                    INSERT INTO vector_distance_comprehensive_test (name, embedding) 
                    VALUES ('{name}', '{embedding_str}')
                """))
        
        # Test all distance functions
        query_vector = [0.2] * 128  # Clearly closer to Document 1 (0.1) than Document 2 (0.4)
        query_str = "[" + ",".join(map(str, query_vector)) + "]"
        
        # Test L2 distance
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT name, l2_distance(embedding, '{query_str}') as distance
                FROM vector_distance_comprehensive_test
                ORDER BY l2_distance(embedding, '{query_str}')
                LIMIT 3
            """))
            rows = result.fetchall()
        
        assert len(rows) == 3
        assert rows[0][0] == "Document 1"  # Should be closest (0.2 is much closer to 0.1 than 0.4)
        
        # Test cosine distance
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT name, cosine_distance(embedding, '{query_str}') as distance
                FROM vector_distance_comprehensive_test
                ORDER BY cosine_distance(embedding, '{query_str}')
                LIMIT 3
            """))
            rows = result.fetchall()
        
        assert len(rows) == 3
        
        # Test inner product
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT name, inner_product(embedding, '{query_str}') as similarity
                FROM vector_distance_comprehensive_test
                ORDER BY inner_product(embedding, '{query_str}') DESC
                LIMIT 3
            """))
            rows = result.fetchall()
        
        assert len(rows) == 3
        
        # Test L2 distance squared (alternative to negative inner product)
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT name, l2_distance_sq(embedding, '{query_str}') as distance_sq
                FROM vector_distance_comprehensive_test
                ORDER BY l2_distance_sq(embedding, '{query_str}')
                LIMIT 3
            """))
            rows = result.fetchall()
        
        assert len(rows) == 3
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE vector_distance_comprehensive_test"))

    def test_vector_index_operations_comprehensive(self, client, engine):
        """Test comprehensive vector index operations (example_13)"""
        
        # Clean up any existing table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_index_comprehensive_test"))
        
        # Create table
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE vector_index_comprehensive_test (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    title VARCHAR(200),
                    content TEXT,
                    embedding vecf32(128)
                )
            """))
        
        # Insert sample data
        sample_data = [
            ("Machine Learning Basics", "Introduction to ML concepts", [0.1] * 128),
            ("Deep Learning Guide", "Neural networks and deep learning", [0.2] * 128),
            ("Data Science Handbook", "Comprehensive data science guide", [0.3] * 128),
        ]
        
        with engine.begin() as conn:
            for title, content, embedding in sample_data:
                embedding_str = "[" + ",".join(map(str, embedding)) + "]"
                conn.execute(text(f"""
                    INSERT INTO vector_index_comprehensive_test (title, content, embedding) 
                    VALUES ('{title}', '{content}', '{embedding_str}')
                """))
        
        # Create IVFFLAT index
        client.vector_index.create_ivf(
            table_name="vector_index_comprehensive_test",
            name="idx_embedding_comprehensive",
            column="embedding",
            lists=32,
            op_type="vector_l2_ops"
        )
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_index_comprehensive_test"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            assert "idx_embedding_comprehensive" in indexes
        
        # Test vector search with index
        query_vector = [0.15] * 128
        query_str = "[" + ",".join(map(str, query_vector)) + "]"
        
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT title, l2_distance(embedding, '{query_str}') as distance
                FROM vector_index_comprehensive_test
                ORDER BY l2_distance(embedding, '{query_str}')
                LIMIT 2
            """))
            rows = result.fetchall()
        
        assert len(rows) == 2
        
        # Test index drop and recreate
        client.vector_index.drop("vector_index_comprehensive_test", "idx_embedding_comprehensive")
        
        # Verify index is dropped
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_index_comprehensive_test"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            assert "idx_embedding_comprehensive" not in indexes
        
        # Recreate index
        client.vector_index.create_ivf(
            table_name="vector_index_comprehensive_test",
            name="idx_embedding_comprehensive",
            column="embedding",
            lists=16,
            op_type="vector_l2_ops"
        )
        
        # Verify index is recreated
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_index_comprehensive_test"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            assert "idx_embedding_comprehensive" in indexes
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE vector_index_comprehensive_test"))

    def test_vector_index_orm_operations(self, client, engine, Base, Session):
        """Test vector index ORM operations (example_14)"""
        
        # Clean up any existing table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_orm_test"))
        
        # Create ORM model with vector index
        class VectorDocument(Base):
            __tablename__ = 'vector_orm_test'
            
            id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(200))
            content = Column(String(1000))
            embedding = create_vector_column(128, "f32")
        
        # Create table
        Base.metadata.create_all(engine)
        
        # Create session
        session = Session()
        
        try:
            # Insert data using ORM
            doc1 = VectorDocument(
                title="Machine Learning Guide",
                content="Comprehensive guide to ML",
                embedding=[0.1] * 128
            )
            doc2 = VectorDocument(
                title="Deep Learning Tutorial",
                content="Neural networks tutorial",
                embedding=[0.2] * 128
            )
            
            session.add(doc1)
            session.add(doc2)
            session.commit()
            
            # Query data using ORM
            documents = session.query(VectorDocument).all()
            assert len(documents) == 2
            assert documents[0].title == "Machine Learning Guide"
            assert documents[1].title == "Deep Learning Tutorial"
            
        finally:
            session.close()
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE vector_orm_test"))

    def test_vector_index_client_chain_operations(self, client, engine):
        """Test vector index client chain operations (example_15)"""
        
        # Clean up any existing table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_chain_test"))
        
        # Create table
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE vector_chain_test (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    title VARCHAR(200),
                    embedding vecf32(128)
                )
            """))
        
        # Insert sample data
        sample_data = [
            ("Document 1", [0.1] * 128),
            ("Document 2", [0.2] * 128),
            ("Document 3", [0.3] * 128),
        ]
        
        with engine.begin() as conn:
            for title, embedding in sample_data:
                embedding_str = "[" + ",".join(map(str, embedding)) + "]"
                conn.execute(text(f"""
                    INSERT INTO vector_chain_test (title, embedding) 
                    VALUES ('{title}', '{embedding_str}')
                """))
        
        # Test basic chain operation: enable_ivf().create_ivf()
        client.vector_index.enable_ivf(probe_limit=1).create_ivf(
            table_name="vector_chain_test",
            name="idx_embedding_chain",
            column="embedding",
            lists=32,
            op_type="vector_l2_ops"
        )
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_chain_test"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            assert "idx_embedding_chain" in indexes
        
        # Test chain operation: drop()
        client.vector_index.drop("vector_chain_test", "idx_embedding_chain")
        
        # Verify index was dropped
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_chain_test"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            assert "idx_embedding_chain" not in indexes
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE vector_chain_test"))

    def test_vector_comprehensive_operations(self, client, engine):
        """Test comprehensive vector operations (example_16)"""
        
        # Clean up any existing table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_comprehensive_test"))
        
        # Create table
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE vector_comprehensive_test (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    title VARCHAR(200),
                    content TEXT,
                    category VARCHAR(50),
                    embedding vecf32(128)
                )
            """))
        
        # Insert sample data
        sample_data = [
            ("Machine Learning Guide", "Comprehensive ML guide", "technology", [0.1, 0.2, 0.3] + [0.0] * 125),
            ("Data Science Handbook", "Data science techniques", "science", [0.2, 0.3, 0.4] + [0.0] * 125),
            ("AI Research Paper", "Latest AI research", "research", [0.3, 0.4, 0.5] + [0.0] * 125),
            ("Database Optimization", "DB performance tips", "technology", [0.4, 0.5, 0.6] + [0.0] * 125),
        ]
        
        with engine.begin() as conn:
            for title, content, category, embedding in sample_data:
                embedding_str = "[" + ",".join(map(str, embedding)) + "]"
                conn.execute(text(f"""
                    INSERT INTO vector_comprehensive_test (title, content, category, embedding) 
                    VALUES ('{title}', '{content}', '{category}', '{embedding_str}')
                """))
        
        # Create vector index
        client.vector_index.create_ivf(
            table_name="vector_comprehensive_test",
            name="idx_embedding_comprehensive",
            column="embedding",
            lists=32,
            op_type="vector_l2_ops"
        )
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_comprehensive_test"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            assert "idx_embedding_comprehensive" in indexes
        
        # Test vector query operations
        query_vector = [0.25, 0.35, 0.45] + [0.0] * 125
        query_str = "[" + ",".join(map(str, query_vector)) + "]"
        
        # Test different distance functions
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT title, l2_distance(embedding, '{query_str}') as distance
                FROM vector_comprehensive_test
                ORDER BY l2_distance(embedding, '{query_str}')
                LIMIT 3
            """))
            rows = result.fetchall()
        
        assert len(rows) == 3
        
        # Test filtered search
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT title, l2_distance(embedding, '{query_str}') as distance
                FROM vector_comprehensive_test
                WHERE category = 'technology'
                ORDER BY l2_distance(embedding, '{query_str}')
            """))
            rows = result.fetchall()
        
        assert len(rows) == 2
        assert all("Machine Learning" in row[0] or "Database" in row[0] for row in rows)
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE vector_comprehensive_test"))

    def test_hnsw_comprehensive_operations(self, client, engine):
        """Test comprehensive HNSW operations (example_17)"""
        
        # Clean up any existing table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_hnsw_comprehensive_test"))
        
        # Create table
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE vector_hnsw_comprehensive_test (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    title VARCHAR(200),
                    category VARCHAR(50),
                    embedding vecf32(128)
                )
            """))
        
        # Insert sample data
        sample_data = [
            ("Document 1", "tech", [0.1, 0.2, 0.3] + [0.0] * 125),
            ("Document 2", "science", [0.2, 0.3, 0.4] + [0.0] * 125),
            ("Document 3", "art", [0.3, 0.4, 0.5] + [0.0] * 125),
        ]
        
        with engine.begin() as conn:
            for title, category, embedding in sample_data:
                embedding_str = "[" + ",".join(map(str, embedding)) + "]"
                conn.execute(text(f"""
                    INSERT INTO vector_hnsw_comprehensive_test (title, category, embedding) 
                    VALUES ('{title}', '{category}', '{embedding_str}')
                """))
        
        # Create HNSW index
        client.vector_index.create_hnsw(
            table_name="vector_hnsw_comprehensive_test",
            name="idx_hnsw_comprehensive",
            column="embedding",
            m=16,
            ef_construction=200,
            ef_search=50,
            op_type="vector_l2_ops"
        )
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_hnsw_comprehensive_test"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            assert "idx_hnsw_comprehensive" in indexes
        
        # Test vector search with HNSW index
        query_vector = [0.15, 0.25, 0.35] + [0.0] * 125
        query_str = "[" + ",".join(map(str, query_vector)) + "]"
        
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                SELECT title, l2_distance(embedding, '{query_str}') as distance
                FROM vector_hnsw_comprehensive_test
                ORDER BY l2_distance(embedding, '{query_str}')
                LIMIT 3
            """))
            rows = result.fetchall()
        
        assert len(rows) == 3
        
        # Test different HNSW configurations
        configurations = [
            {
                "name": "hnsw_fast",
                "table": "vector_hnsw_fast_test",
                "m": 16,
                "ef_construction": 32,
                "ef_search": 32
            },
            {
                "name": "hnsw_balanced",
                "table": "vector_hnsw_balanced_test",
                "m": 48,
                "ef_construction": 64,
                "ef_search": 64
            }
        ]
        
        for config in configurations:
            # Create table
            with engine.begin() as conn:
                conn.execute(text(f"""
                    CREATE TABLE {config['table']} (
                        id BIGINT PRIMARY KEY AUTO_INCREMENT,
                        title VARCHAR(200),
                        embedding vecf32(128)
                    )
                """))
            
            # Insert test data
            embedding = [0.1] * 128
            embedding_str = "[" + ",".join(map(str, embedding)) + "]"
            with engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO {config['table']} (title, embedding) 
                    VALUES ('Test Document', '{embedding_str}')
                """))
            
            # Create HNSW index with specific configuration
            client.vector_index.create_hnsw(
                table_name=config["table"],
                name="idx_hnsw",
                column="embedding",
                m=config["m"],
                ef_construction=config["ef_construction"],
                ef_search=config["ef_search"],
                op_type="vector_l2_ops"
            )
            
            # Verify index was created
            with engine.begin() as conn:
                result = conn.execute(text(f"SHOW INDEX FROM {config['table']}"))
                rows = result.fetchall()
                indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
                assert "idx_hnsw" in indexes
            
            # Clean up
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE {config['table']}"))
        
        # Clean up main table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE vector_hnsw_comprehensive_test"))
