#!/usr/bin/env python3
"""
Example 13: Vector Index Operations with MatrixOne

This example demonstrates how to create and use vector indexes in MatrixOne
using SQLAlchemy ORM. It covers:

1. Creating tables with vector indexes
2. Creating indexes on existing tables
3. Using different index types (IVFFlat)
4. Using different operation types (L2, Cosine, Inner Product)

IMPORTANT LIMITATION: MatrixOne supports only ONE index per vector column.
Attempting to create multiple indexes on the same vector column will fail
after the first one is created. This example demonstrates this limitation.
"""

import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

from matrixone import Client
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from matrixone.sqlalchemy_ext import (
    create_vector_column, IVFVectorIndex, VectorIndexType, VectorOpType,
    create_vector_index, create_ivfflat_index, vector_index_builder,
    IVFConfig, create_ivf_config
)


def demo_vector_index_operations():
    """Demonstrate vector index operations."""
    logger = create_default_logger()
    
    logger.info("ℹ️ Note: This example demonstrates vector index creation.")
    logger.info("ℹ️ If you see 'IVF index is not enabled' errors, it means")
    logger.info("ℹ️ your MatrixOne instance doesn't have IVF indexing enabled.")
    logger.info("ℹ️ The code will still demonstrate the API usage.")
    logger.info("ℹ️ MatrixOne limitation: Only ONE index per vector column is supported.")
    
    try:
        # Get connection parameters
        host, port, user, password, database = get_connection_params()
        print_config()
        
        # Connect to MatrixOne
        logger.info("🔌 Connecting to MatrixOne...")
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect(host=host, port=port, user=user, password=password, database=database)
        engine = client.get_sqlalchemy_engine()
        
        # Enable SQL logging for SQLAlchemy engine
        engine.echo = True
        logger.info("✅ Connected to MatrixOne successfully with SQL logging enabled")
        
        # Demo 0: Enable IVF indexing
        logger.info("\n" + "="*60)
        logger.info("📋 Demo 0: Enable IVF indexing and set search parameters")
        logger.info("="*60)
        
        # Use IVF configuration manager
        ivf_config = create_ivf_config(engine)
        
        # Check if IVF is supported
        if not ivf_config.is_ivf_supported():
            logger.warning("⚠️ IVF indexing is not supported in this MatrixOne version")
            logger.info("ℹ️ Continuing with demo to show API usage")
        else:
            logger.info("✅ IVF indexing is supported")
            
            # Configure IVF
            success = ivf_config.configure_ivf(enabled=True, probe_limit=1)
            if success:
                logger.info("✅ IVF indexing enabled with probe_limit=1")
            else:
                logger.warning("⚠️ Failed to configure IVF parameters")
            
            # Show current status
            status = ivf_config.get_ivf_status()
            if status.get("error"):
                logger.warning(f"⚠️ Error getting IVF status: {status['error']}")
            else:
                logger.info(f"ℹ️ IVF enabled: {status.get('ivf_enabled')}")
                logger.info(f"ℹ️ Probe limit: {status.get('probe_limit')}")
        
        # Create declarative base
        Base = declarative_base()
        Session = sessionmaker(bind=engine)
        
        # Demo 1: Create table with vector index
        logger.info("\n" + "="*60)
        logger.info("📋 Demo 1: Create table with vector index")
        logger.info("="*60)
        
        class DocumentWithIndex(Base):
            __tablename__ = 'vector_index_demo_01'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(128, "f32")
            title = Column(String(200))
            content = Column(String(1000))
        
        # Drop table if exists
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_index_demo_01"))
        
        # Create table first
        Base.metadata.create_all(engine, tables=[DocumentWithIndex.__table__])
        logger.info("✅ Table created")
        
        # Create vector index separately
        vector_index = create_ivfflat_index(
            "idx_embedding_l2_demo01",
            "embedding",
            lists=256,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        
        try:
            with engine.begin() as conn:
                sql = vector_index.create_sql("vector_index_demo_01")
                conn.execute(text(sql))
            logger.info("✅ Vector index created")
        except Exception as e:
            logger.warning(f"⚠️ Vector index creation failed (IVF may not be enabled): {e}")
            logger.info("ℹ️ This is expected if IVF index is not enabled in MatrixOne")
        
        # Demo 2: MatrixOne Limitation - Multiple Indexes on Same Column
        logger.info("\n" + "="*60)
        logger.info("📋 Demo 2: MatrixOne Limitation - Multiple Indexes on Same Column")
        logger.info("="*60)
        
        logger.info("ℹ️ This demo demonstrates MatrixOne's limitation:")
        logger.info("ℹ️ Only ONE index per vector column is supported.")
        
        class DocumentWithoutIndex(Base):
            __tablename__ = 'vector_index_demo_02'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(128, "f32")
            title = Column(String(200))
            content = Column(String(1000))
        
        # Drop table if exists
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_index_demo_02"))
        
        # Create table without index
        Base.metadata.create_all(engine, tables=[DocumentWithoutIndex.__table__])
        logger.info("✅ Table created without index")
        
        # Create index using helper function
        l2_index = create_ivfflat_index(
            "idx_embedding_l2_demo02",
            "embedding",
            lists=128,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        
        # Create the first index (should succeed)
        try:
            with engine.begin() as conn:
                sql = l2_index.create_sql("vector_index_demo_02")
                conn.execute(text(sql))
            logger.info("✅ L2 distance index created (first index - expected to succeed)")
        except Exception as e:
            logger.warning(f"⚠️ L2 index creation failed: {e}")
        
        # Create cosine similarity index (should fail due to MatrixOne limitation)
        cosine_index = create_vector_index(
            "idx_embedding_cosine_demo02",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=64,
            op_type=VectorOpType.VECTOR_COSINE_OPS
        )
        
        try:
            with engine.begin() as conn:
                sql = cosine_index.create_sql("vector_index_demo_02")
                conn.execute(text(sql))
            logger.info("✅ Cosine similarity index created")
        except Exception as e:
            logger.info(f"ℹ️ Cosine index creation failed (expected): {e}")
            logger.info("ℹ️ This demonstrates MatrixOne's limitation: only one index per vector column")
        
        # Demo 3: Insert sample data and test
        logger.info("\n" + "="*60)
        logger.info("📋 Demo 3: Insert sample data and test")
        logger.info("="*60)
        
        session = Session()
        try:
            # Insert sample documents
            sample_docs = [
                DocumentWithIndex(
                    embedding=[0.1] * 128,
                    title="Machine Learning Basics",
                    content="Introduction to machine learning concepts and algorithms."
                ),
                DocumentWithIndex(
                    embedding=[0.2] * 128,
                    title="Deep Learning Fundamentals",
                    content="Neural networks, backpropagation, and deep architectures."
                ),
                DocumentWithIndex(
                    embedding=[0.3] * 128,
                    title="Natural Language Processing",
                    content="Text processing, tokenization, and language models."
                ),
                DocumentWithIndex(
                    embedding=[0.4] * 128,
                    title="Computer Vision",
                    content="Image processing, feature detection, and object recognition."
                ),
                DocumentWithIndex(
                    embedding=[0.5] * 128,
                    title="Data Science Pipeline",
                    content="Data collection, preprocessing, analysis, and visualization."
                )
            ]
            
            for doc in sample_docs:
                session.add(doc)
            session.commit()
            logger.info(f"✅ Inserted {len(sample_docs)} documents")
            
            # Test vector search with index
            query_vector = [0.15] * 128
            
            # Search using L2 distance (should use the index)
            results = session.query(DocumentWithIndex).order_by(
                DocumentWithIndex.embedding.l2_distance(query_vector)
            ).limit(3).all()
            
            logger.info("🔍 Top 3 similar documents (L2 distance):")
            for i, doc in enumerate(results, 1):
                logger.info(f"   {i}. {doc.title}")
            
            # Search using cosine distance
            results = session.query(DocumentWithIndex).order_by(
                DocumentWithIndex.embedding.cosine_distance(query_vector)
            ).limit(3).all()
            
            logger.info("🔍 Top 3 similar documents (cosine distance):")
            for i, doc in enumerate(results, 1):
                logger.info(f"   {i}. {doc.title}")
            
        finally:
            session.close()
        
        # Demo 4: Show index information
        logger.info("\n" + "="*60)
        logger.info("📋 Demo 4: Show index information")
        logger.info("="*60)
        
        with engine.begin() as conn:
            # Show indexes for each table
            for table_name in ["vector_index_demo_01", "vector_index_demo_02"]:
                result = conn.execute(text(f"SHOW INDEX FROM {table_name}"))
                indexes = result.fetchall()
                
                logger.info(f"📊 Indexes for {table_name}:")
                for idx in indexes:
                    logger.info(f"   - {idx[2]}: {idx[4]} ({idx[10]})")
        
        # Demo 5: Disable IVF indexing (optional)
        logger.info("\n" + "="*60)
        logger.info("📋 Demo 5: Disable IVF indexing (optional)")
        logger.info("="*60)
        
        # Disable IVF indexing using the configuration manager
        if ivf_config.is_ivf_supported():
            success = ivf_config.disable_ivf_indexing()
            if success:
                logger.info("✅ IVF indexing disabled")
                
                # Show final status
                status = ivf_config.get_ivf_status()
                if not status.get("error"):
                    logger.info(f"ℹ️ Final IVF enabled: {status.get('ivf_enabled')}")
            else:
                logger.warning("⚠️ Failed to disable IVF indexing")
        
        logger.info("\n🎉 Vector index operations demo completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error in vector index operations demo: {e}")
        raise
    finally:
        # Clean up
        try:
            with engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS vector_index_demo_01"))
                conn.execute(text("DROP TABLE IF EXISTS vector_index_demo_02"))
            logger.info("🧹 Cleanup completed")
        except Exception as e:
            logger.warning(f"⚠️ Cleanup warning: {e}")


if __name__ == "__main__":
    demo_vector_index_operations()
