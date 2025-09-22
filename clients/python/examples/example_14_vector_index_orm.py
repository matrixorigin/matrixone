#!/usr/bin/env python3
"""
Example 14: Vector Index ORM Methods

This example demonstrates the ORM-style methods for creating and dropping vector indexes.
It shows both class methods and instance methods for convenient index management.
"""

import sys
import os

# Add the parent directory to the path so we can import matrixone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import Column, Integer, String, text
from sqlalchemy.orm import declarative_base, sessionmaker

from matrixone import Client
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from matrixone.sqlalchemy_ext import (
    VectorIndex,
    VectorIndexType,
    VectorOpType,
    create_vector_column,
    create_ivf_config,
    enable_ivf_indexing,
)


def main():
    """Main function demonstrating vector index ORM methods."""
    # Print configuration
    print_config()
    
    # Get connection parameters
    host, port, user, password, database = get_connection_params()
    
    # Create logger
    logger = create_default_logger()
    
    # Create client and connect
    client = Client(logger=logger, enable_full_sql_logging=True)
    client.connect(host=host, port=port, user=user, password=password, database=database)
    
    # Get SQLAlchemy engine
    engine = client.get_sqlalchemy_engine()
    
    # Create declarative base and session
    Base = declarative_base()
    Session = sessionmaker(bind=engine)
    
    # Clean up first
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS vector_orm_demo"))
    
    print("\n" + "="*60)
    print("Vector Index ORM Methods Demo")
    print("="*60)
    
    # Enable IVF indexing first for all demos
    print("\n--- IVF Configuration ---")
    ivf_config = create_ivf_config(engine)
    if ivf_config.is_ivf_supported():
        print("✓ IVF indexing is supported")
        enable_result = ivf_config.enable_ivf_indexing()
        if enable_result:
            print("✓ IVF indexing enabled")
            ivf_config.set_probe_limit(1)
            print("✓ Probe limit set to 1")
        else:
            print("⚠ Failed to enable IVF indexing")
    else:
        print("⚠ IVF indexing is not supported in this MatrixOne version")
    
    # Create table
    class DocumentORM(Base):
        __tablename__ = 'vector_orm_demo'
        id = Column(Integer, primary_key=True)
        embedding = create_vector_column(128, "f32")
        title = Column(String(200))
        category = Column(String(50))
    
    Base.metadata.create_all(engine, tables=[DocumentORM.__table__])
    print("✓ Created table: vector_orm_demo")
    
    # Demo 1: Class method create_index
    print("\n--- Demo 1: Class Method create_index ---")
    success = VectorIndex.create_index(
        engine=engine,
        table_name="vector_orm_demo",
        name="idx_embedding_class_method",
        column="embedding",
        index_type=VectorIndexType.IVFFLAT,
        lists=32,
        op_type=VectorOpType.VECTOR_L2_OPS
    )
    print(f"✓ Class method create_index result: {success}")
    
    # Verify index was created
    with engine.begin() as conn:
        result = conn.execute(text("SHOW INDEX FROM vector_orm_demo"))
        indexes = result.fetchall()
        index_names = [idx[2] for idx in indexes]
        print(f"✓ Indexes in table: {index_names}")
    
    # Demo 2: Class method drop_index
    print("\n--- Demo 2: Class Method drop_index ---")
    success = VectorIndex.drop_index(
        engine=engine,
        table_name="vector_orm_demo",
        name="idx_embedding_class_method"
    )
    print(f"✓ Class method drop_index result: {success}")
    
    # Verify index was dropped
    with engine.begin() as conn:
        result = conn.execute(text("SHOW INDEX FROM vector_orm_demo"))
        indexes = result.fetchall()
        index_names = [idx[2] for idx in indexes]
        print(f"✓ Indexes in table after drop: {index_names}")
    
    # Demo 3: Instance method create
    print("\n--- Demo 3: Instance Method create ---")
    index = VectorIndex(
        name="idx_embedding_instance_method",
        column="embedding",
        index_type=VectorIndexType.IVFFLAT,
        lists=64,
        op_type=VectorOpType.VECTOR_L2_OPS
    )
    
    success = index.create(engine, "vector_orm_demo")
    print(f"✓ Instance method create result: {success}")
    
    # Verify index was created
    with engine.begin() as conn:
        result = conn.execute(text("SHOW INDEX FROM vector_orm_demo"))
        indexes = result.fetchall()
        index_names = [idx[2] for idx in indexes]
        print(f"✓ Indexes in table: {index_names}")
    
    # Demo 4: Instance method drop
    print("\n--- Demo 4: Instance Method drop ---")
    success = index.drop(engine, "vector_orm_demo")
    print(f"✓ Instance method drop result: {success}")
    
    # Verify index was dropped
    with engine.begin() as conn:
        result = conn.execute(text("SHOW INDEX FROM vector_orm_demo"))
        indexes = result.fetchall()
        index_names = [idx[2] for idx in indexes]
        print(f"✓ Indexes in table after drop: {index_names}")
    
    # Demo 5: ORM methods with different index configurations
    print("\n--- Demo 5: Different Index Configurations ---")
    
    # Create index with different lists parameter
    success = VectorIndex.create_index(
        engine=engine,
        table_name="vector_orm_demo",
        name="idx_embedding_different_config",
        column="embedding",
        index_type=VectorIndexType.IVFFLAT,
        lists=16,
        op_type=VectorOpType.VECTOR_L2_OPS
    )
    print(f"✓ Different config index creation result: {success}")
    
    # Verify index was created
    with engine.begin() as conn:
        result = conn.execute(text("SHOW INDEX FROM vector_orm_demo"))
        indexes = result.fetchall()
        index_names = [idx[2] for idx in indexes]
        print(f"✓ Indexes in table: {index_names}")
    
    # Drop the index
    success = VectorIndex.drop_index(
        engine=engine,
        table_name="vector_orm_demo",
        name="idx_embedding_different_config"
    )
    print(f"✓ Different config index drop result: {success}")
    
    # Demo 6: Error handling
    print("\n--- Demo 6: Error Handling ---")
    
    # Try to drop non-existent index
    success = VectorIndex.drop_index(
        engine=engine,
        table_name="vector_orm_demo",
        name="non_existent_index"
    )
    print(f"✓ Drop non-existent index result: {success} (should be False)")
    
    # Try to create index on non-existent table
    success = VectorIndex.create_index(
        engine=engine,
        table_name="non_existent_table",
        name="idx_test",
        column="embedding",
        index_type=VectorIndexType.IVFFLAT,
        lists=32,
        op_type=VectorOpType.VECTOR_L2_OPS
    )
    print(f"✓ Create index on non-existent table result: {success} (should be False)")
    
    # Clean up
    print("\n--- Cleanup ---")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS vector_orm_demo"))
    print("✓ Cleaned up test table")
    
    # Disconnect
    client.disconnect()
    print("✓ Disconnected from MatrixOne")
    
    print("\n" + "="*60)
    print("Vector Index ORM Methods Demo Completed")
    print("="*60)


if __name__ == "__main__":
    main()
