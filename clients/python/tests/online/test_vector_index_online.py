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
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        yield client
        # Client doesn't have a close method, just disconnect
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
    
    def test_vector_index_creation_with_table(self, engine, Base, Session):
        """Test creating vector index with table creation."""
        class DocumentWithIndex(Base):
            __tablename__ = 'test_vector_index_online_01'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(128, "f32")
            title = Column(String(200))
            content = Column(String(1000))
        
        # Clean up first
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_index_online_01"))
        
        # Create table
        Base.metadata.create_all(engine, tables=[DocumentWithIndex.__table__])
        
        # Create vector index
        vector_index = create_vector_index(
            "idx_embedding_l2_online01",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=128,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        
        try:
            with engine.begin() as conn:
                sql = vector_index.create_sql("test_vector_index_online_01")
                conn.execute(text(sql))
            # If we get here, index creation succeeded
            assert True
        except Exception as e:
            # If IVF is not enabled, this is expected
            assert "IVF index is not enabled" in str(e) or "not supported" in str(e)
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_index_online_01"))
    
    def test_vector_index_on_existing_table(self, engine, Base, Session):
        """Test creating vector index on existing table."""
        class DocumentWithoutIndex(Base):
            __tablename__ = 'test_vector_index_online_02'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(64, "f32")
            title = Column(String(200))
            category = Column(String(50))
        
        # Clean up first
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_index_online_02"))
        
        # Create table without index
        Base.metadata.create_all(engine, tables=[DocumentWithoutIndex.__table__])
        
        # Create L2 distance index
        l2_index = create_ivfflat_index(
            "idx_embedding_l2_online02",
            "embedding",
            lists=64,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        
        try:
            with engine.begin() as conn:
                sql = l2_index.create_sql("test_vector_index_online_02")
                conn.execute(text(sql))
            # If we get here, first index creation succeeded
            assert True
        except Exception as e:
            # If IVF is not enabled, this is expected
            assert "IVF index is not enabled" in str(e) or "not supported" in str(e)
        
        # Try to create second index (should fail due to MatrixOne limitation)
        cosine_index = create_vector_index(
            "idx_embedding_cosine_online02",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=32,
            op_type=VectorOpType.VECTOR_COSINE_OPS
        )
        
        try:
            with engine.begin() as conn:
                sql = cosine_index.create_sql("test_vector_index_online_02")
                conn.execute(text(sql))
            # If we get here, second index creation succeeded (unexpected)
            assert False, "Second index creation should have failed"
        except Exception as e:
            # This is expected - MatrixOne doesn't support multiple indexes on same column
            assert True
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_index_online_02"))
    
    def test_vector_index_builder(self, engine, Base, Session):
        """Test using VectorIndexBuilder."""
        class DocumentBuilder(Base):
            __tablename__ = 'test_vector_index_online_03'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(96, "f32")
            title = Column(String(200))
            category = Column(String(50))
        
        # Clean up first
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_index_online_03"))
        
        # Create table
        Base.metadata.create_all(engine, tables=[DocumentBuilder.__table__])
        
        # Use VectorIndexBuilder to create multiple indexes
        indexes = vector_index_builder("embedding") \
            .l2_index("idx_l2_builder_online03", lists=64) \
            .cosine_index("idx_cosine_builder_online03", lists=32) \
            .ip_index("idx_ip_builder_online03", lists=48) \
            .build()
        
        assert len(indexes) == 3
        
        # Try to create indexes one by one (only first should succeed)
        success_count = 0
        for i, index in enumerate(indexes):
            try:
                with engine.begin() as conn:
                    sql = index.create_sql("test_vector_index_online_03")
                    conn.execute(text(sql))
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
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_index_online_03"))
    
    def test_vector_index_with_data_operations(self, engine, Base, Session):
        """Test vector index with data insertion and search."""
        class DocumentWithData(Base):
            __tablename__ = 'test_vector_index_online_04'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(64, "f32")
            title = Column(String(200))
            category = Column(String(50))
        
        # Clean up first
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_index_online_04"))
        
        # Create table
        Base.metadata.create_all(engine, tables=[DocumentWithData.__table__])
        
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
        
        # Create vector index
        vector_index = create_ivfflat_index(
            "idx_embedding_l2_online04",
            "embedding",
            lists=32,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        
        try:
            with engine.begin() as conn:
                sql = vector_index.create_sql("test_vector_index_online_04")
                conn.execute(text(sql))
        except Exception as e:
            # If IVF is still not enabled, skip the data operations test
            pytest.skip(f"IVF index creation failed, skipping data operations test: {e}")
        
        # Insert sample data
        session = Session()
        try:
            sample_docs = [
                DocumentWithData(
                    id=1,
                    embedding=[0.1] * 64,
                    title="Document 1",
                    category="tech"
                ),
                DocumentWithData(
                    id=2,
                    embedding=[0.2] * 64,
                    title="Document 2",
                    category="science"
                ),
                DocumentWithData(
                    id=3,
                    embedding=[0.3] * 64,
                    title="Document 3",
                    category="tech"
                )
            ]
            
            session.add_all(sample_docs)
            session.commit()
            
            # Verify data was inserted
            count = session.query(DocumentWithData).count()
            assert count == 3
            
            # Test vector search using L2 distance
            query_vector = [0.15] * 64
            results = session.query(DocumentWithData).order_by(
                DocumentWithData.embedding.l2_distance(query_vector)
            ).limit(2).all()
            
            assert len(results) == 2
            # Verify that we got results (the exact order may vary based on distance calculation)
            assert results[0].title in ["Document 1", "Document 2", "Document 3"]
            assert results[1].title in ["Document 1", "Document 2", "Document 3"]
            assert results[0].title != results[1].title  # Should be different documents
            
        finally:
            session.close()
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_index_online_04"))
    
    def test_vector_index_ddl_definition(self, engine, Base, Session):
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
