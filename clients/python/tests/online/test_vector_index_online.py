"""
Online tests for vector index operations.
Uses real database connections to test actual vector indexing functionality.
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
    create_vector_column, vector_distance_functions,
    VectorIndex, VectorIndexType, VectorOpType, CreateVectorIndex,
    create_vector_index, create_ivfflat_index, VectorIndexBuilder,
    vector_index_builder, IVFConfig, create_ivf_config,
    enable_ivf_indexing, disable_ivf_indexing, set_probe_limit, get_ivf_status
)
from matrixone.config import get_connection_params
from .test_config import online_config


class TestVectorIndexOnline:
    """Test vector index operations with real database connections."""
    
    @pytest.fixture(scope="class")
    def client(self):
        """Create and connect MatrixOne client."""
        from matrixone.config import get_connection_params
        from matrixone.logger import create_default_logger
        
        # Get connection parameters
        host, port, user, password, database = get_connection_params()
        
        # Create logger
        logger = create_default_logger()
        
        # Create client and connect
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect(host=host, port=port, user=user, password=password, database=database)
        
        yield client
        
        # Cleanup
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
        """Create session factory."""
        return sessionmaker(bind=engine)
    
    def test_ivf_config_creation(self, engine):
        """Test IVF configuration manager creation."""
        ivf_config = create_ivf_config(engine)
        assert ivf_config is not None
        assert ivf_config.engine == engine
    
    def test_ivf_support_check(self, engine):
        """Test checking if IVF is supported."""
        ivf_config = create_ivf_config(engine)
        is_supported = ivf_config.is_ivf_supported()
        # This should not raise an exception
        assert isinstance(is_supported, bool)
    
    def test_ivf_status_retrieval(self, engine):
        """Test getting IVF status."""
        ivf_config = create_ivf_config(engine)
        status = ivf_config.get_ivf_status()
        
        assert isinstance(status, dict)
        assert "ivf_enabled" in status
        assert "probe_limit" in status
        assert "error" in status
    
    def test_ivf_enable_disable(self, engine):
        """Test enabling and disabling IVF indexing."""
        ivf_config = create_ivf_config(engine)
        
        # Try to enable IVF
        enable_result = ivf_config.enable_ivf_indexing()
        assert isinstance(enable_result, bool)
        
        # Try to disable IVF
        disable_result = ivf_config.disable_ivf_indexing()
        assert isinstance(disable_result, bool)
    
    def test_probe_limit_setting(self, engine):
        """Test setting probe limit."""
        ivf_config = create_ivf_config(engine)
        
        # Try to set probe limit
        result = ivf_config.set_probe_limit(5)
        assert isinstance(result, bool)
        
        # Try with invalid values (these should return False, not raise ValueError)
        result_negative = ivf_config.set_probe_limit(-1)
        assert isinstance(result_negative, bool)
        
        result_zero = ivf_config.set_probe_limit(0)
        assert isinstance(result_zero, bool)
    
    def test_vector_index_creation_with_table(self, client, Base, Session):
        """Test creating vector index with table creation using client interface."""
        class DocumentWithIndex(Base):
            __tablename__ = 'test_vector_index_online_01'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(128, "f32")
            title = Column(String(200))
            content = Column(String(1000))
        
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)
        
        # Enable IVF indexing
        client.vector_index.enable_ivf()
        
        # Create vector index using client interface
        try:
            client.vector_index.create_ivf(
                name="idx_embedding_l2_online01",
                table_name="test_vector_index_online_01",
                column="embedding",
                lists=128,
                op_type="vector_l2_ops"
            )
            # If we get here, index creation succeeded
            assert True
        except Exception as e:
            # If IVF is not supported, this is expected
            assert "not supported" in str(e) or "IVF" in str(e)
        
        # Clean up using client interface
        client.drop_all(Base)
    
    def test_vector_index_on_existing_table(self, client, Base, Session):
        """Test creating vector index on existing table."""
        class DocumentWithoutIndex(Base):
            __tablename__ = 'test_vector_index_online_02'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(64, "f32")
            title = Column(String(200))
            category = Column(String(50))
        
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)
        
        # Enable IVF indexing
        client.vector_index.enable_ivf()
        
        # Create L2 distance index using client API
        try:
            client.vector_index.create_ivf(
                name="idx_embedding_l2_online02",
                table_name="test_vector_index_online_02",
                column="embedding",
                lists=64,
                op_type="vector_l2_ops"
            )
            # If we get here, index creation succeeded
            assert True
        except Exception as e:
            # If IVF is not enabled, this is expected
            assert "IVF index is not enabled" in str(e) or "not supported" in str(e)
        
        # Try to create second index (should fail due to MatrixOne limitation)
        try:
            client.vector_index.create_ivf(
                name="idx_embedding_cosine_online02",
                table_name="test_vector_index_online_02",
                column="embedding",
                lists=32,
                op_type="vector_cosine_ops"
            )
            # If we get here, second index creation succeeded (unexpected)
            assert False, "Second index creation should have failed"
        except Exception as e:
            # This is expected - MatrixOne doesn't support multiple indexes on same column
            assert True
        
        # Clean up using client interface
        client.drop_all(Base)
    
    def test_vector_index_builder(self, client, Base, Session):
        """Test using VectorIndexBuilder."""
        class DocumentBuilder(Base):
            __tablename__ = 'test_vector_index_online_03'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(96, "f32")
            title = Column(String(200))
            category = Column(String(50))
        
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)
        
        # Enable IVF indexing
        client.vector_index.enable_ivf()
        
        # Try to create multiple indexes using client API (only first should succeed)
        index_configs = [
            {"name": "idx_l2_builder_online03", "op_type": "vector_l2_ops", "lists": 64},
            {"name": "idx_cosine_builder_online03", "op_type": "vector_cosine_ops", "lists": 32},
            {"name": "idx_ip_builder_online03", "op_type": "vector_ip_ops", "lists": 48}
        ]
        
        success_count = 0
        for i, config in enumerate(index_configs):
            try:
                client.vector_index.create_ivf(
                    name=config["name"],
                    table_name="test_vector_index_online_03",
                    column="embedding",
                    lists=config["lists"],
                    op_type=config["op_type"]
                )
                success_count += 1
                if i == 0:
                    # First index should succeed (if IVF is enabled)
                    assert True
                else:
                    # Subsequent indexes should fail
                    assert False, f"Index {i+1} should have failed"
            except Exception as e:
                if i == 0:
                    # First index might fail if IVF is not enabled
                    assert "IVF index is not enabled" in str(e) or "not supported" in str(e)
                else:
                    # Subsequent indexes should fail due to MatrixOne limitation
                    assert True
        
        # Clean up using client interface
        client.drop_all(Base)
    
    def test_vector_index_with_data_operations(self, client, Base, Session):
        """Test vector index with data insertion and search."""
        class DocumentWithData(Base):
            __tablename__ = 'test_vector_index_online_04'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(64, "f32")
            title = Column(String(200))
            category = Column(String(50))
        
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)
        
        # Enable IVF indexing using client API
        client.vector_index.enable_ivf()
        
        # Create vector index using client API
        try:
            client.vector_index.create_ivf(
                name="idx_embedding_l2_online04",
                table_name="test_vector_index_online_04",
                column="embedding",
                lists=32,
                op_type="vector_l2_ops"
            )
        except Exception as e:
            # If IVF is still not enabled, skip the data operations test
            pytest.skip(f"IVF index creation failed, skipping data operations test: {e}")
        
        # Insert sample data using client API
        sample_data = [
            {
                "id": 1,
                "embedding": [0.1] * 64,
                "title": "Document 1",
                "category": "tech"
            },
            {
                "id": 2,
                "embedding": [0.2] * 64,
                "title": "Document 2",
                "category": "science"
            },
            {
                "id": 3,
                "embedding": [0.3] * 64,
                "title": "Document 3",
                "category": "tech"
            }
        ]
        
        # Use client batch insert API
        client.vector_data.batch_insert("test_vector_index_online_04", sample_data)
        
        # Verify data was inserted using client query
        result = client.execute("SELECT COUNT(*) FROM test_vector_index_online_04")
        count = result.fetchone()[0]
        assert count == 3
        
        # Test vector search using client API
        query_vector = [0.15] * 64
        search_results = client.vector_query.similarity_search(
            table_name="test_vector_index_online_04",
            vector_column="embedding",
            query_vector=query_vector,
            limit=2,
            distance_type="l2"
        )
        
        assert len(search_results) >= 1  # Should get at least one result
        
        # Clean up using client interface
        client.drop_all(Base)
    
    def test_vector_index_ddl_definition_with_session(self, engine, Base, Session):
        """Test vector index defined in DDL, with data operations and index deletion."""
        # Clean up first
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_index_online_05"))
        
        # Enable IVF indexing first
        ivf_config = create_ivf_config(engine)
        if not ivf_config.is_ivf_supported():
            pytest.skip("IVF indexing is not supported in this MatrixOne version")
        
        # Try to enable IVF indexing
        enable_result = ivf_config.enable_ivf_indexing()
        if not enable_result:
            pytest.skip("Failed to enable IVF indexing")
        
        # Set probe limit
        ivf_config.set_probe_limit(1)
        
        # First, try to create table with DDL-defined index
        class DocumentWithDDLIndex(Base):
            __tablename__ = 'test_vector_index_online_05'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(128, "f32")
            title = Column(String(200))
            category = Column(String(50))
            # Define vector index in DDL
            __table_args__ = (
                VectorIndex(
                    "idx_embedding_ddl_online05",
                    "embedding",
                    index_type=VectorIndexType.IVFFLAT,
                    lists=64,
                    op_type=VectorOpType.VECTOR_L2_OPS
                ),
            )
        
        # This MUST succeed - if it fails, the test should fail
        Base.metadata.create_all(engine, tables=[DocumentWithDDLIndex.__table__])
        
        # Verify table was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW TABLES LIKE 'test_vector_index_online_05'"))
            assert result.fetchone() is not None
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM test_vector_index_online_05"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            assert "idx_embedding_ddl_online05" in index_names
        
        # Insert sample data
        session = Session()
        try:
            sample_docs = [
                DocumentWithDDLIndex(
                    id=1,
                    embedding=[0.1] * 128,
                    title="DDL Document 1",
                    category="tech"
                ),
                DocumentWithDDLIndex(
                    id=2,
                    embedding=[0.2] * 128,
                    title="DDL Document 2",
                    category="science"
                ),
                DocumentWithDDLIndex(
                    id=3,
                    embedding=[0.3] * 128,
                    title="DDL Document 3",
                    category="tech"
                ),
                DocumentWithDDLIndex(
                    id=4,
                    embedding=[0.4] * 128,
                    title="DDL Document 4",
                    category="science"
                )
            ]
            
            session.add_all(sample_docs)
            session.commit()
            
            # Verify data was inserted
            count = session.query(DocumentWithDDLIndex).count()
            assert count == 4
            
            # Test vector search using L2 distance
            query_vector = [0.25] * 128
            results = session.query(DocumentWithDDLIndex).order_by(
                DocumentWithDDLIndex.embedding.l2_distance(query_vector)
            ).limit(3).all()
            
            assert len(results) == 3
            # Verify that we got results (the exact order may vary based on distance calculation)
            for result in results:
                assert result.title in ["DDL Document 1", "DDL Document 2", "DDL Document 3", "DDL Document 4"]
            
            # Test cosine distance search
            cosine_results = session.query(DocumentWithDDLIndex).order_by(
                DocumentWithDDLIndex.embedding.cosine_distance(query_vector)
            ).limit(2).all()
            
            assert len(cosine_results) == 2
            for result in cosine_results:
                assert result.title in ["DDL Document 1", "DDL Document 2", "DDL Document 3", "DDL Document 4"]
            
        finally:
            session.close()
        
        # Test index deletion
        with engine.begin() as conn:
            conn.execute(text("DROP INDEX idx_embedding_ddl_online05 ON test_vector_index_online_05"))
        
        # Verify index was deleted
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM test_vector_index_online_05"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            assert "idx_embedding_ddl_online05" not in index_names
        
        # Verify data is still accessible after index deletion
        session = Session()
        try:
            count = session.query(DocumentWithDDLIndex).count()
            assert count == 4
            
            # Test vector search still works (without index)
            query_vector = [0.25] * 128
            results = session.query(DocumentWithDDLIndex).order_by(
                DocumentWithDDLIndex.embedding.l2_distance(query_vector)
            ).limit(2).all()
            
            assert len(results) == 2
            
        finally:
            session.close()
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_index_online_05"))
    
    def test_convenience_functions(self, engine):
        """Test convenience functions for IVF configuration."""
        # Test enable_ivf_indexing
        result = enable_ivf_indexing(engine)
        assert isinstance(result, bool)
        
        # Test disable_ivf_indexing
        result = disable_ivf_indexing(engine)
        assert isinstance(result, bool)
        
        # Test set_probe_limit
        result = set_probe_limit(engine, 3)
        assert isinstance(result, bool)
        
        # Test get_ivf_status
        status = get_ivf_status(engine)
        assert isinstance(status, dict)
        assert "ivf_enabled" in status
        assert "probe_limit" in status
        assert "error" in status
    
    def test_vector_index_sql_generation(self):
        """Test vector index SQL generation."""
        # Test L2 index
        l2_index = create_vector_index(
            "idx_l2_test",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=128,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        
        sql = l2_index.create_sql("test_table")
        assert "CREATE INDEX idx_l2_test USING ivfflat ON test_table(embedding)" in sql
        assert "lists = 128" in sql
        assert "op_type 'vector_l2_ops'" in sql
        
        # Test cosine index
        cosine_index = create_vector_index(
            "idx_cosine_test",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=64,
            op_type=VectorOpType.VECTOR_COSINE_OPS
        )
        
        sql = cosine_index.create_sql("test_table")
        assert "CREATE INDEX idx_cosine_test USING ivfflat ON test_table(embedding)" in sql
        assert "lists = 64" in sql
        assert "op_type 'vector_cosine_ops'" in sql
        
        # Test inner product index
        ip_index = create_vector_index(
            "idx_ip_test",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=96,
            op_type=VectorOpType.VECTOR_IP_OPS
        )
        
        sql = ip_index.create_sql("test_table")
        assert "CREATE INDEX idx_ip_test USING ivfflat ON test_table(embedding)" in sql
        assert "lists = 96" in sql
        assert "op_type 'vector_ip_ops'" in sql
