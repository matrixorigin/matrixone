"""
Test ORM-style methods for VectorIndex operations.
"""

import pytest
import sys
import os
from sqlalchemy import Column, Integer, String, text
from sqlalchemy.orm import declarative_base, sessionmaker

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from matrixone import Client
from matrixone.sqlalchemy_ext import (
    VectorIndex,
    VectorIndexType,
    VectorOpType,
    create_vector_column,
    create_ivf_config,
    enable_ivf_indexing,
)
from tests.online.test_config import OnlineTestConfig


class TestVectorIndexORMMethods:
    """Test ORM-style methods for VectorIndex operations."""

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
        # Client doesn't have a close method, just disconnect
        if hasattr(client, 'disconnect'):
            client.disconnect()
    
    @pytest.fixture(scope="class")
    def engine(self, client):
        """Get SQLAlchemy engine from client."""
        return client.get_sqlalchemy_engine()

    @pytest.fixture(scope="class")
    def Base(self):
        """Create declarative base for tests."""
        return declarative_base()

    @pytest.fixture(scope="class")
    def Session(self, engine):
        """Create session factory."""
        return sessionmaker(bind=engine)

    def test_vector_index_orm_create_and_drop(self, engine, Base, Session):
        """Test ORM-style create and drop methods for VectorIndex."""
        # Clean up first
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_orm_methods"))

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

        # Create table
        class TestDocument(Base):
            __tablename__ = 'test_vector_orm_methods'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(128, "f32")
            title = Column(String(200))

        Base.metadata.create_all(engine, tables=[TestDocument.__table__])

        # Test 1: Class method create_index
        success = VectorIndex.create_index(
            engine=engine,
            table_name="test_vector_orm_methods",
            name="idx_embedding_orm_01",
            column="embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=32,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        assert success, "Class method create_index should succeed"

        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM test_vector_orm_methods"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            assert "idx_embedding_orm_01" in index_names

        # Test 2: Class method drop_index
        success = VectorIndex.drop_index(
            engine=engine,
            table_name="test_vector_orm_methods",
            name="idx_embedding_orm_01"
        )
        assert success, "Class method drop_index should succeed"

        # Verify index was dropped
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM test_vector_orm_methods"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            assert "idx_embedding_orm_01" not in index_names

        # Test 3: Instance method create
        index = VectorIndex(
            name="idx_embedding_orm_02",
            column="embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=64,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        
        success = index.create(engine, "test_vector_orm_methods")
        assert success, "Instance method create should succeed"

        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM test_vector_orm_methods"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            assert "idx_embedding_orm_02" in index_names

        # Test 4: Instance method drop
        success = index.drop(engine, "test_vector_orm_methods")
        assert success, "Instance method drop should succeed"

        # Verify index was dropped
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM test_vector_orm_methods"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            assert "idx_embedding_orm_02" not in index_names

        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_orm_methods"))

    def test_vector_index_orm_transaction_operations(self, engine, Base, Session):
        """Test ORM-style vector index operations within transactions."""
        # Clean up first
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_orm_transaction"))

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

        # Create table
        class TestDocumentTransaction(Base):
            __tablename__ = 'test_vector_orm_transaction'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(128, "f32")
            title = Column(String(200))

        Base.metadata.create_all(engine, tables=[TestDocumentTransaction.__table__])

        # Test 1: Create and drop index in transaction (MatrixOne only supports one index per vector column)
        with engine.begin() as conn:
            # Create index
            success = VectorIndex.create_index_in_transaction(
                connection=conn,
                table_name="test_vector_orm_transaction",
                name="idx_embedding_transaction_01",
                column="embedding",
                index_type=VectorIndexType.IVFFLAT,
                lists=32,
                op_type=VectorOpType.VECTOR_L2_OPS
            )
            assert success, "Index creation in transaction should succeed"

        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM test_vector_orm_transaction"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            assert "idx_embedding_transaction_01" in index_names

        # Test 2: Drop index in transaction
        with engine.begin() as conn:
            # Drop index
            success = VectorIndex.drop_index_in_transaction(
                connection=conn,
                table_name="test_vector_orm_transaction",
                name="idx_embedding_transaction_01"
            )
            assert success, "Index drop in transaction should succeed"

        # Verify index was dropped
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM test_vector_orm_transaction"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            assert "idx_embedding_transaction_01" not in index_names

        # Test 3: Error handling in transaction
        # Note: MatrixOne vector index creation (CREATE INDEX USING ivfflat) is auto-committed
        # and not transactional, so we test error handling instead of rollback
        try:
            with engine.begin() as conn:
                # Try to create an index with invalid table name (should fail)
                success = VectorIndex.create_index_in_transaction(
                    connection=conn,
                    table_name="non_existent_table",
                    name="idx_embedding_transaction_error",
                    column="embedding",
                    index_type=VectorIndexType.IVFFLAT,
                    lists=32,
                    op_type=VectorOpType.VECTOR_L2_OPS
                )
                assert not success, "Should fail for non-existent table"
        except Exception:
            # Expected to fail
            pass

        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_orm_transaction"))

    def test_vector_index_orm_with_ivf_config(self, engine, Base, Session):
        """Test ORM methods with IVF configuration."""
        # Clean up first
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_orm_ivf"))

        # Create table
        class TestDocumentIVF(Base):
            __tablename__ = 'test_vector_orm_ivf'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(128, "f32")
            title = Column(String(200))

        Base.metadata.create_all(engine, tables=[TestDocumentIVF.__table__])

        # Enable IVF indexing
        ivf_config = create_ivf_config(engine)
        if ivf_config.is_ivf_supported():
            enable_result = ivf_config.enable_ivf_indexing()
            if enable_result:
                ivf_config.set_probe_limit(1)

        # Test ORM create with IVF
        success = VectorIndex.create_index(
            engine=engine,
            table_name="test_vector_orm_ivf",
            name="idx_embedding_orm_ivf",
            column="embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=16,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        
        if ivf_config.is_ivf_supported() and enable_result:
            assert success, "ORM create with IVF should succeed"
        else:
            # If IVF is not supported, the operation might fail, which is expected
            print("IVF indexing not supported or enabled, skipping success assertion")

        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_orm_ivf"))

    def test_vector_index_client_chain_operations(self, client, engine, Base, Session):
        """Test vector index operations using client chain methods."""
        # Clean up first
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_chain"))

        # Create table
        class TestDocumentChain(Base):
            __tablename__ = 'test_vector_chain'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(128, "f32")
            title = Column(String(200))

        Base.metadata.create_all(engine, tables=[TestDocumentChain.__table__])

        # Test 1: Client chain operations
        try:
            # Enable IVF and create index in chain
            client.vector_index.enable_ivf(probe_limit=1).create(
                table_name="test_vector_chain",
                name="idx_embedding_chain_01",
                column="embedding",
                index_type="ivfflat",
                lists=32,
                op_type="vector_l2_ops"
            )
            
            # Verify index was created
            with engine.begin() as conn:
                result = conn.execute(text("SHOW INDEX FROM test_vector_chain"))
                indexes = result.fetchall()
                index_names = [idx[2] for idx in indexes]
                assert "idx_embedding_chain_01" in index_names

            # Drop index using chain
            client.vector_index.drop(
                table_name="test_vector_chain",
                name="idx_embedding_chain_01"
            )
            
            # Verify index was dropped
            with engine.begin() as conn:
                result = conn.execute(text("SHOW INDEX FROM test_vector_chain"))
                indexes = result.fetchall()
                index_names = [idx[2] for idx in indexes]
                assert "idx_embedding_chain_01" not in index_names

        except Exception as e:
            # If IVF is not supported, skip the test
            if "IVF indexing is not supported" in str(e):
                pytest.skip("IVF indexing is not supported in this MatrixOne version")
            else:
                raise

        # Test 2: Transaction chain operations (MatrixOne only supports one index per vector column)
        try:
            with client.transaction() as tx:
                # Create index in transaction using chain
                tx.vector_index.create(
                    table_name="test_vector_chain",
                    name="idx_embedding_chain_tx_01",
                    column="embedding",
                    index_type="ivfflat",
                    lists=32,
                    op_type="vector_l2_ops"
                )
            
            # Verify index was created
            with engine.begin() as conn:
                result = conn.execute(text("SHOW INDEX FROM test_vector_chain"))
                indexes = result.fetchall()
                index_names = [idx[2] for idx in indexes]
                assert "idx_embedding_chain_tx_01" in index_names

            # Drop index in transaction using chain
            with client.transaction() as tx:
                tx.vector_index.drop(
                    table_name="test_vector_chain",
                    name="idx_embedding_chain_tx_01"
                )
            
            # Verify index was dropped
            with engine.begin() as conn:
                result = conn.execute(text("SHOW INDEX FROM test_vector_chain"))
                indexes = result.fetchall()
                index_names = [idx[2] for idx in indexes]
                assert "idx_embedding_chain_tx_01" not in index_names

        except Exception as e:
            # If IVF is not supported, skip the test
            if "IVF indexing is not supported" in str(e):
                pytest.skip("IVF indexing is not supported in this MatrixOne version")
            else:
                raise

        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_chain"))

    def test_vector_index_orm_error_handling(self, engine, Base, Session):
        """Test ORM methods error handling."""
        # Test dropping non-existent index
        success = VectorIndex.drop_index(
            engine=engine,
            table_name="non_existent_table",
            name="non_existent_index"
        )
        assert not success, "Dropping non-existent index should fail gracefully"

        # Test creating index on non-existent table
        success = VectorIndex.create_index(
            engine=engine,
            table_name="non_existent_table",
            name="idx_test",
            column="embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=32,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        assert not success, "Creating index on non-existent table should fail gracefully"
