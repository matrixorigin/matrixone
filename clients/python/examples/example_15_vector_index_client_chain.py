#!/usr/bin/env python3
"""
Example 15: Vector Index Client Chain Operations

This example demonstrates the client chain operations for vector index management.
It shows how to use the client's vector_index property for fluent API operations,
including the new separated APIs for IVFFLAT and HNSW indexes.
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
from matrixone.sqlalchemy_ext import create_vector_column


def main():
    """Main function demonstrating vector index client chain operations."""
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
        conn.execute(text("DROP TABLE IF EXISTS vector_chain_demo"))
    
    print("\n" + "="*60)
    print("Vector Index Client Chain Operations Demo")
    print("="*60)
    
    # Enable IVF indexing first for all demos
    print("\n--- IVF Configuration ---")
    try:
        client.vector_index.enable_ivf(probe_limit=1)
        print("✓ IVF indexing enabled via client chain")
    except Exception as e:
        print(f"⚠ Failed to enable IVF indexing: {e}")
        print("⚠ Some demos may fail without IVF indexing")
    
    # Create table
    class DocumentChain(Base):
        __tablename__ = 'vector_chain_demo'
        id = Column(Integer, primary_key=True)
        embedding = create_vector_column(128, "f32")
        title = Column(String(200))
        category = Column(String(50))
    
    Base.metadata.create_all(engine, tables=[DocumentChain.__table__])
    print("✓ Created table: vector_chain_demo")
    
    # Demo 1: Basic client chain operations
    print("\n--- Demo 1: Basic Client Chain Operations ---")
    try:
        # Enable IVF and create index in chain
        client.vector_index.enable_ivf(probe_limit=1).create_ivf(
            table_name="vector_chain_demo",
            name="idx_embedding_chain_01",
            column="embedding",
            lists=32,
            op_type="vector_l2_ops"
        )
        print("✓ Chain operation: enable_ivf().create() - Success")
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_chain_demo"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            print(f"✓ Indexes in table: {index_names}")
        
        # Drop index using chain
        client.vector_index.drop(
            table_name="vector_chain_demo",
            name="idx_embedding_chain_01"
        )
        print("✓ Chain operation: drop() - Success")
        
        # Verify index was dropped
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_chain_demo"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            print(f"✓ Indexes in table after drop: {index_names}")
            
    except Exception as e:
        if "IVF indexing is not supported" in str(e):
            print("⚠ IVF indexing is not supported in this MatrixOne version")
        else:
            print(f"✗ Chain operation failed: {e}")
    
    # Demo 2: Transaction chain operations (MatrixOne only supports one index per vector column)
    print("\n--- Demo 2: Transaction Chain Operations ---")
    try:
        with client.transaction() as tx:
            # Create index in transaction using chain
            tx.vector_index.create_ivf(
                table_name="vector_chain_demo",
                name="idx_embedding_chain_tx_01",
                column="embedding",
                lists=32,
                op_type="vector_l2_ops"
            )
        print("✓ Transaction chain: create() - Success")
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_chain_demo"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            print(f"✓ Indexes in table: {index_names}")
        
        # Drop index in transaction using chain
        with client.transaction() as tx:
            tx.vector_index.drop(
                table_name="vector_chain_demo",
                name="idx_embedding_chain_tx_01"
            )
        print("✓ Transaction chain: drop() - Success")
        
        # Verify index was dropped
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_chain_demo"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            print(f"✓ Indexes in table after drop: {index_names}")
            
    except Exception as e:
        if "IVF indexing is not supported" in str(e):
            print("⚠ IVF indexing is not supported in this MatrixOne version")
        else:
            print(f"✗ Transaction chain operation failed: {e}")
    
    # Demo 3: Complex chain operations
    print("\n--- Demo 3: Complex Chain Operations ---")
    try:
        # Enable IVF, create index, then disable IVF in chain
        client.vector_index.enable_ivf(probe_limit=2).create_ivf(
            table_name="vector_chain_demo",
            name="idx_embedding_chain_complex",
            column="embedding",
            lists=16,
            op_type="vector_l2_ops"
        )
        print("✓ Complex chain: enable_ivf().create() - Success")
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_chain_demo"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            print(f"✓ Indexes in table: {index_names}")
        
        # Drop index and disable IVF
        client.vector_index.drop(
            table_name="vector_chain_demo",
            name="idx_embedding_chain_complex"
        ).disable_ivf()
        print("✓ Complex chain: drop().disable_ivf() - Success")
        
        # Verify index was dropped
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_chain_demo"))
            indexes = result.fetchall()
            index_names = [idx[2] for idx in indexes]
            print(f"✓ Indexes in table after drop: {index_names}")
            
    except Exception as e:
        if "IVF indexing is not supported" in str(e):
            print("⚠ IVF indexing is not supported in this MatrixOne version")
        else:
            print(f"✗ Complex chain operation failed: {e}")
    
    # Demo 4: Error handling in chain operations
    print("\n--- Demo 4: Error Handling in Chain Operations ---")
    try:
        # Try to create index without enabling IVF first
        client.vector_index.create_ivf(
            table_name="vector_chain_demo",
            name="idx_embedding_chain_error",
            column="embedding",
            lists=32,
            op_type="vector_l2_ops"
        )
        print("✓ Error handling: create without IVF - Unexpected success")
        
    except Exception as e:
        print(f"✓ Error handling: create without IVF - Expected error: {e}")
    
    try:
        # Try to drop non-existent index
        client.vector_index.drop(
            table_name="vector_chain_demo",
            name="non_existent_index"
        )
        print("✓ Error handling: drop non-existent - Unexpected success")
        
    except Exception as e:
        print(f"✓ Error handling: drop non-existent - Expected error: {e}")
    
    # Demo 5: Separated APIs demonstration
    print("\n--- Demo 5: Separated APIs Demonstration ---")
    try:
        # Create separate tables for different index types
        client.execute("""
            CREATE TABLE vector_docs_ivf_demo (
                id INT PRIMARY KEY AUTO_INCREMENT,
                embedding vecf32(128),
                content TEXT
            )
        """)
        
        client.execute("""
            CREATE TABLE vector_docs_hnsw_demo (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                embedding vecf32(128),
                content TEXT
            )
        """)
        
        # Insert sample data
        sample_data = [
            "Document about machine learning and AI",
            "Guide to database optimization techniques",
            "Introduction to vector similarity search"
        ]
        
        for i, content in enumerate(sample_data):
            embedding = [float(j) for j in range(i * 10, i * 10 + 128)]
            embedding_str = "[" + ",".join(map(str, embedding)) + "]"
            
            client.execute(f"""
                INSERT INTO vector_docs_ivf_demo (embedding, content) 
                VALUES ('{embedding_str}', '{content}')
            """)
            
            client.execute(f"""
                INSERT INTO vector_docs_hnsw_demo (embedding, content) 
                VALUES ('{embedding_str}', '{content}')
            """)
        
        print("✓ Created demo tables and inserted sample data")
        
        # Create IVFFLAT index using separated API
        print("\n5️⃣ Creating IVFFLAT index with create_ivf():")
        import time
        start_time = time.time()
        
        client.vector_index.create_ivf(
            table_name="vector_docs_ivf_demo",
            name="idx_ivf_separated",
            column="embedding",
            lists=50,
            op_type="vector_l2_ops"
        )
        
        ivf_time = time.time() - start_time
        print(f"   ✓ Created IVFFLAT index in {ivf_time:.2f} seconds")
        
        # Create HNSW index using separated API
        print("\n6️⃣ Creating HNSW index with create_hnsw():")
        start_time = time.time()
        
        client.vector_index.create_hnsw(
            table_name="vector_docs_hnsw_demo",
            name="idx_hnsw_separated",
            column="embedding",
            m=16,
            ef_construction=200,
            ef_search=50,
            op_type="vector_l2_ops"
        )
        
        hnsw_time = time.time() - start_time
        print(f"   ✓ Created HNSW index in {hnsw_time:.2f} seconds")
        
        # Demonstrate method chaining with separated APIs
        print("\n7️⃣ Demonstrating method chaining with separated APIs:")
        # Create a separate table for chaining demo to avoid multiple index limitation
        client.execute("""
            CREATE TABLE vector_chain_separated_demo (
                id INT PRIMARY KEY AUTO_INCREMENT,
                embedding vecf32(128),
                content TEXT
            )
        """)
        
        client.vector_index.create_ivf(
            table_name="vector_chain_separated_demo",
            name="idx_ivf_chain_separated",
            column="embedding",
            lists=30
        ).enable_ivf(probe_limit=2)
        print("   ✓ Created IVFFLAT index with chaining using separated API")
        
        # Clean up the chaining demo table
        client.drop_table("vector_chain_separated_demo")
        
        # Show API usage examples
        print("\n8️⃣ Vector Index API Usage Examples:")
        print("   # For IVFFLAT indexes:")
        print("   client.vector_index.create_ivf(")
        print("       table_name='table', name='idx', column='embedding', lists=100")
        print("   )")
        print()
        print("   # For HNSW indexes:")
        print("   client.vector_index.create_hnsw(")
        print("       table_name='table', name='idx', column='embedding',")
        print("       m=16, ef_construction=200, ef_search=50")
        print("   )")
        
        # Performance summary
        print(f"\n--- Performance Summary ---")
        print(f"IVFFLAT index creation time: {ivf_time:.2f} seconds")
        print(f"HNSW index creation time: {hnsw_time:.2f} seconds")
        print(f"Index creation ratio (HNSW/IVF): {hnsw_time/ivf_time:.2f}x")
        
        print("\n--- Benefits of Separated APIs ---")
        print("✓ Type safety: Each method only accepts relevant parameters")
        print("✓ Better documentation: Clear parameter descriptions for each index type")
        print("✓ IDE support: Better autocomplete and parameter hints")
        print("✓ Reduced errors: No confusion about which parameters apply to which index type")
        print("✓ Clean API: Dedicated methods for each index type")
        
        # Clean up demo tables
        client.drop_table("vector_docs_ivf_demo")
        client.drop_table("vector_docs_hnsw_demo")
        print("✓ Cleaned up demo tables")
        
    except Exception as e:
        print(f"✗ Separated APIs demonstration failed: {e}")
        # Clean up demo tables on error
        try:
            client.drop_table("vector_docs_ivf_demo")
            client.drop_table("vector_docs_hnsw_demo")
        except:
            pass
    
    # Clean up
    print("\n--- Cleanup ---")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS vector_chain_demo"))
    print("✓ Cleaned up test table")
    
    # Disconnect
    client.disconnect()
    print("✓ Disconnected from MatrixOne")
    
    print("\n" + "="*60)
    print("Vector Index Client Chain Operations Demo Completed")
    print("="*60)


if __name__ == "__main__":
    main()
