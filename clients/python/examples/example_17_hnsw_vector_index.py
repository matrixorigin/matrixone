#!/usr/bin/env python3
"""
MatrixOne HNSW Vector Index Example

This example demonstrates how to use HNSW (Hierarchical Navigable Small World)
vector indexes in MatrixOne, including:
- DDL table creation with HNSW indexes
- CREATE INDEX statements for HNSW
- HNSW configuration management
- Performance comparison between IVFFLAT and HNSW
"""

import sys
import os
import time

# Add the parent directory to the path to import matrixone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from matrixone import Client
from matrixone.config import get_connection_params, print_config
from matrixone.sqlalchemy_ext import (
    VectorIndex, VectorIndexType, VectorOpType, create_hnsw_index,
    create_ivfflat_index, enable_hnsw_indexing, enable_ivf_indexing
)


def main():
    """Main function demonstrating HNSW vector indexing"""
    
    # Print configuration
    print_config()
    
    # Get connection parameters
    host, port, user, password, database = get_connection_params()
    
    # Create client and connect
    client = Client()
    client.connect(host=host, port=port, user=user, password=password, database=database)
    
    print("=" * 80)
    print("MatrixOne HNSW Vector Index Demo")
    print("=" * 80)
    
    try:
        # Clean up any existing tables first
        print("\n--- Cleanup ---")
        client.drop_table("vector_docs_hnsw_demo")
        client.drop_table("vector_docs_ivfflat_demo")
        print("✓ Cleaned up existing tables")
        
        # Demo 1: HNSW Configuration
        print("\n--- Demo 1: HNSW Configuration ---")
        
        # Demo 2: DDL Table Creation with HNSW Index
        print("\n--- Demo 2: DDL Table Creation with HNSW Index ---")
        
        # Create table with HNSW index in DDL
        schema = {
            'a': {'type': 'bigint', 'primary_key': True},
            'b': {'type': 'vector', 'dimension': 128, 'precision': 'f32'},
            'c': {'type': 'int'}
        }
        
        # Create table using SQLAlchemy with HNSW index
        from sqlalchemy import create_engine, text, MetaData, Table, Column, BigInteger, Integer
        from matrixone.sqlalchemy_ext import Vectorf32, VectorIndex
        
        engine = client.get_sqlalchemy_engine()
        metadata = MetaData()
        
        # Create table with HNSW index
        table = Table(
            'vector_docs_hnsw_demo',
            metadata,
            Column('a', BigInteger, primary_key=True),
            Column('b', Vectorf32(128)),
            Column('c', Integer),
            # HNSW index defined in DDL
            VectorIndex('idx_hnsw', 'b', index_type=VectorIndexType.HNSW, 
                       m=48, ef_construction=64, ef_search=64, 
                       op_type=VectorOpType.VECTOR_L2_OPS)
        )
        
        # Enable HNSW indexing and create table in the same connection
        with engine.begin() as conn:
            # Enable HNSW indexing in this session
            conn.execute(text("SET experimental_hnsw_index = 1"))
            print("✓ HNSW indexing enabled in session")
            
            # Create the table with HNSW index
            metadata.create_all(bind=conn)
        print("✓ Created table with HNSW index in DDL")
        
        # Demo 3: CREATE INDEX Statement for HNSW
        print("\n--- Demo 3: CREATE INDEX Statement for HNSW ---")
        
        # Create another table without index first
        schema2 = {
            'id': {'type': 'bigint', 'primary_key': True},
            'title': {'type': 'varchar', 'length': 200},
            'embedding': {'type': 'vector', 'dimension': 128, 'precision': 'f32'},
            'category': {'type': 'varchar', 'length': 50}
        }
        
        # Create table using client interface
        client.create_table("vector_docs_ivfflat_demo", {
            'id': 'bigint',
            'title': 'varchar(200)',
            'embedding': 'vector(128,f32)',
            'category': 'varchar(50)'
        }, primary_key='id')
        print("✓ Created table without index")
        
        # Insert some sample data first (MatrixOne requires data before creating vector index)
        sample_data = [
            {
                'id': 1,
                'title': 'Document 1',
                'embedding': [0.1] * 128,
                'category': 'tech'
            },
            {
                'id': 2,
                'title': 'Document 2', 
                'embedding': [0.2] * 128,
                'category': 'science'
            }
        ]
        
        for doc in sample_data:
            client.vector_data.insert("vector_docs_ivfflat_demo", doc)
        print("✓ Inserted sample data")
        
        # Create HNSW index using CREATE INDEX statement in the same session
        with engine.begin() as conn:
            # Enable HNSW indexing in this session
            conn.execute(text("SET experimental_hnsw_index = 1"))
            print("✓ HNSW indexing enabled in session")
        
        # Create HNSW index using CREATE INDEX statement
        client.vector_index.create(
            table_name="vector_docs_ivfflat_demo",
            name="idx01",
            column="embedding",
            index_type="hnsw",
            m=48,
            ef_construction=64,
            ef_search=64,
            op_type="vector_l2_ops"
        )
        print("✓ Created HNSW index using CREATE INDEX statement")
        
        # Demo 4: Insert Sample Data
        print("\n--- Demo 4: Insert Sample Data ---")
        
        # Insert data into both tables
        docs = [
            {
                'a': 1,
                'b': [0.1] * 128,
                'c': 100
            },
            {
                'a': 2,
                'b': [0.2] * 128,
                'c': 200
            },
            {
                'a': 3,
                'b': [0.3] * 128,
                'c': 300
            },
            {
                'a': 4,
                'b': [0.4] * 128,
                'c': 400
            },
            {
                'a': 5,
                'b': [0.5] * 128,
                'c': 500
            }
        ]
        
        client.vector_data.batch_insert("vector_docs_hnsw_demo", docs)
        print("✓ Inserted 5 documents into HNSW table")
        
        # Insert data into IVFFLAT table
        docs2 = [
            {
                'id': 10,
                'title': 'Machine Learning Fundamentals',
                'embedding': [0.1] * 128,
                'category': 'AI'
            },
            {
                'id': 11,
                'title': 'Deep Learning Networks',
                'embedding': [0.2] * 128,
                'category': 'AI'
            },
            {
                'id': 12,
                'title': 'Database Systems',
                'embedding': [0.3] * 128,
                'category': 'Database'
            },
            {
                'id': 13,
                'title': 'Vector Databases',
                'embedding': [0.4] * 128,
                'category': 'Database'
            },
            {
                'id': 14,
                'title': 'SQL Optimization',
                'embedding': [0.5] * 128,
                'category': 'Database'
            }
        ]
        
        client.vector_data.batch_insert("vector_docs_ivfflat_demo", docs2)
        print("✓ Inserted 5 documents into IVFFLAT table")
        
        # Demo 5: Vector Search with HNSW Index
        print("\n--- Demo 5: Vector Search with HNSW Index ---")
        
        query_vector = [0.25] * 128
        
        # Search with HNSW index
        hnsw_results = client.vector_query.similarity_search(
            table_name="vector_docs_hnsw_demo",
            vector_column="b",
            query_vector=query_vector,
            limit=3,
            distance_type="l2",
            select_columns=["a", "c"]
        )
        
        print(f"✓ HNSW search returned {len(hnsw_results)} results:")
        for i, result in enumerate(hnsw_results):
            print(f"  {i+1}. ID: {result[0]}, Value: {result[1]} (distance: {result[-1]:.4f})")
        
        # Search with IVFFLAT index
        ivfflat_results = client.vector_query.similarity_search(
            table_name="vector_docs_ivfflat_demo",
            vector_column="embedding",
            query_vector=query_vector,
            limit=3,
            distance_type="l2",
            select_columns=["id", "title", "category"]
        )
        
        print(f"✓ IVFFLAT search returned {len(ivfflat_results)} results:")
        for i, result in enumerate(ivfflat_results):
            print(f"  {i+1}. ID: {result[0]}, Title: {result[1]}, Category: {result[2]} (distance: {result[-1]:.4f})")
        
        # Demo 6: Performance Comparison
        print("\n--- Demo 6: Performance Comparison ---")
        
        # HNSW performance test
        print("Testing HNSW performance...")
        start_time = time.time()
        for _ in range(10):
            client.vector_query.similarity_search(
                table_name="vector_docs_hnsw_demo",
                vector_column="b",
                query_vector=query_vector,
                limit=3,
                distance_type="l2",
                select_columns=["a", "c"]
            )
        hnsw_time = time.time() - start_time
        print(f"✓ HNSW search (10 iterations): {hnsw_time:.4f} seconds")
        
        # IVFFLAT performance test
        print("Testing IVFFLAT performance...")
        start_time = time.time()
        for _ in range(10):
            client.vector_query.similarity_search(
                table_name="vector_docs_ivfflat_demo",
                vector_column="embedding",
                query_vector=query_vector,
                limit=3,
                distance_type="l2",
                select_columns=["id", "title", "category"]
            )
        ivfflat_time = time.time() - start_time
        print(f"✓ IVFFLAT search (10 iterations): {ivfflat_time:.4f} seconds")
        
        # Performance comparison
        if hnsw_time < ivfflat_time:
            improvement = ((ivfflat_time - hnsw_time) / ivfflat_time * 100)
            print(f"✓ HNSW is {improvement:.1f}% faster than IVFFLAT")
        else:
            improvement = ((hnsw_time - ivfflat_time) / hnsw_time * 100)
            print(f"✓ IVFFLAT is {improvement:.1f}% faster than HNSW")
        
        # Demo 7: Different HNSW Configurations
        print("\n--- Demo 7: Different HNSW Configurations ---")
        
        # Create different HNSW configurations - each with its own table
        configs = [
            {"name": "hnsw_fast", "table": "vector_docs_hnsw_fast", "m": 16, "ef_construction": 32, "ef_search": 32},
            {"name": "hnsw_balanced", "table": "vector_docs_hnsw_balanced", "m": 48, "ef_construction": 64, "ef_search": 64},
            {"name": "hnsw_accurate", "table": "vector_docs_hnsw_accurate", "m": 64, "ef_construction": 128, "ef_search": 128}
        ]
        
        # Schema for HNSW configuration tables
        schema3 = {
            'id': {'type': 'bigint', 'primary_key': True},
            'embedding': {'type': 'vector', 'dimension': 128, 'precision': 'f32'},
            'text': {'type': 'varchar', 'length': 200}
        }
        
        # Test data
        test_docs = [
            {'id': i, 'embedding': [i * 0.1] * 128, 'text': f'Document {i}'}
            for i in range(1, 6)
        ]
        
        with engine.begin() as conn:
            # Enable HNSW indexing in this session
            conn.execute(text("SET experimental_hnsw_index = 1"))
            print("✓ HNSW indexing enabled in session")
            
            for config in configs:
                try:
                    # Create table for this configuration using client interface in transaction
                    client.create_table_in_transaction(config["table"], {
                        'id': 'bigint',
                        'embedding': 'vector(128,f32)',
                        'text': 'varchar(200)'
                    }, conn, primary_key='id')
                    print(f"✓ Created table: {config['table']}")
                    
                    # Insert test data using _in_transaction method
                    for doc in test_docs:
                        client.vector_data.insert_in_transaction(config["table"], doc, conn)
                    print(f"✓ Inserted test data into {config['table']}")
                    
                    # Create HNSW index using _in_transaction method
                    client.vector_index.create_in_transaction(
                        table_name=config["table"],
                        name="idx_hnsw",
                        column="embedding",
                        connection=conn,
                        index_type="hnsw",
                        m=config["m"],
                        ef_construction=config["ef_construction"],
                        ef_search=config["ef_search"],
                        op_type="vector_l2_ops"
                    )
                    print(f"✓ Created HNSW index: {config['name']} (M={config['m']}, EF_CONSTRUCTION={config['ef_construction']}, EF_SEARCH={config['ef_search']})")
                except Exception as e:
                    print(f"⚠️  Failed to create HNSW configuration {config['name']}: {e}")

        
        # Demo 8: HNSW vs IVFFLAT Range Search
        print("\n--- Demo 8: HNSW vs IVFFLAT Range Search ---")
        
        # Range search with HNSW
        hnsw_range_results = client.vector_query.range_search(
            table_name="vector_docs_hnsw_demo",
            vector_column="b",
            query_vector=query_vector,
            max_distance=0.5,
            distance_type="l2",
            select_columns=["a", "c"]
        )
        
        print(f"✓ HNSW range search (distance <= 0.5): {len(hnsw_range_results)} results")
        
        # Range search with IVFFLAT
        ivfflat_range_results = client.vector_query.range_search(
            table_name="vector_docs_ivfflat_demo",
            vector_column="embedding",
            query_vector=query_vector,
            max_distance=0.5,
            distance_type="l2",
            select_columns=["id", "title"]
        )
        
        print(f"✓ IVFFLAT range search (distance <= 0.5): {len(ivfflat_range_results)} results")
        
        # Demo 9: Best Practices
        print("\n--- Demo 9: Best Practices ---")
        
        print("✅ HNSW Best Practices:")
        print("1. Use HNSW for high-dimensional vectors (typically > 100 dimensions)")
        print("2. Set M between 16-64 for most use cases")
        print("3. Set EF_CONSTRUCTION higher than EF_SEARCH for better index quality")
        print("4. Use EF_SEARCH based on your accuracy vs speed requirements")
        print("5. Enable HNSW indexing before creating HNSW indexes")
        print("6. HNSW is generally faster for search but slower for index construction")
        
        print("\n✅ When to use HNSW vs IVFFLAT:")
        print("- HNSW: High-dimensional vectors, read-heavy workloads, need fast search")
        print("- IVFFLAT: Lower-dimensional vectors, write-heavy workloads, need fast index updates")
        
        # Demo 10: Transaction Operations with HNSW
        print("\n--- Demo 10: Transaction Operations with HNSW ---")
        
        # Demonstrate transaction operations with HNSW vector tables
        try:
            with client.transaction() as tx:
                print("✓ Started transaction")
                
                # Create table within transaction using client interface
                tx.create_table("vector_docs_transaction_demo", {
                    'id': 'bigint',
                    'embedding': 'vector(128,f32)',
                    'title': 'varchar(200)',
                    'category': 'varchar(50)'
                }, primary_key='id')
                print("✓ Created table within transaction")
                
                # Insert data within transaction
                tx_data = [
                    {
                        'id': 1,
                        'embedding': [0.1] * 128,
                        'title': 'Transaction Document 1',
                        'category': 'transaction'
                    },
                    {
                        'id': 2,
                        'embedding': [0.2] * 128,
                        'title': 'Transaction Document 2',
                        'category': 'transaction'
                    }
                ]
                
                for doc in tx_data:
                    tx.vector_data.insert("vector_docs_transaction_demo", doc)
                print("✓ Inserted data within transaction")
                
                # Create HNSW index within transaction
                # Enable HNSW indexing using transaction wrapper (not separate connection)
                tx.execute("SET experimental_hnsw_index = 1")
                print("✓ Enabled HNSW indexing in transaction")
                
                # Create index using transaction wrapper
                tx.vector_index.create(
                    table_name="vector_docs_transaction_demo",
                    name="idx_tx_hnsw",
                    column="embedding",
                    index_type="hnsw",
                    m=32,
                    ef_construction=64,
                    ef_search=32,
                    op_type="vector_l2_ops"
                )
                print("✓ Created HNSW index within transaction")
                
                # Perform search within transaction
                query_vector = [0.15] * 128
                results = tx.vector_query.similarity_search(
                    table_name="vector_docs_transaction_demo",
                    vector_column="embedding",
                    query_vector=query_vector,
                    limit=2,
                    distance_type="l2",
                    select_columns=["id", "title", "category"]
                )
                print(f"✓ Transaction search returned {len(results)} results:")
                for i, result in enumerate(results):
                    print(f"  {i+1}. ID: {result[0]}, Title: {result[1]}, Category: {result[2]} (distance: {result[-1]:.4f})")
                
                print("✓ Transaction completed successfully - all operations committed")
                
        except Exception as e:
            print(f"⚠️  Transaction failed: {e}")
            print("✓ Transaction rolled back automatically")
        
        # Demo 11: SQLAlchemy Transaction Mode with _in_transaction methods
        print("\n--- Demo 11: SQLAlchemy Transaction Mode with _in_transaction methods ---")
        
        # Demonstrate SQLAlchemy transaction mode using _in_transaction methods
        try:
            with engine.begin() as conn:
                print("✓ Started SQLAlchemy transaction")
                
                # Create table using client interface in transaction
                client.create_table_in_transaction("vector_docs_sqlalchemy_demo", {
                    'id': 'bigint',
                    'embedding': 'vector(128,f32)',
                    'title': 'varchar(200)',
                    'category': 'varchar(50)'
                }, conn, primary_key='id')
                print("✓ Created table using client interface in transaction")
                
                # Insert data using _in_transaction method
                sqlalchemy_data = [
                    {
                        'id': 1,
                        'embedding': [0.1] * 128,
                        'title': 'SQLAlchemy Transaction Document 1',
                        'category': 'sqlalchemy'
                    },
                    {
                        'id': 2,
                        'embedding': [0.2] * 128,
                        'title': 'SQLAlchemy Transaction Document 2',
                        'category': 'sqlalchemy'
                    }
                ]
                
                for doc in sqlalchemy_data:
                    client.vector_data.insert_in_transaction("vector_docs_sqlalchemy_demo", doc, conn)
                print("✓ Inserted data using insert_in_transaction method")
                
                # Create HNSW index using _in_transaction method
                client.vector_index.create_in_transaction(
                    table_name="vector_docs_sqlalchemy_demo",
                    name="idx_sqlalchemy_hnsw",
                    column="embedding",
                    connection=conn,
                    index_type="hnsw",
                    m=32,
                    ef_construction=64,
                    ef_search=32,
                    op_type="vector_l2_ops"
                )
                print("✓ Created HNSW index using create_in_transaction method")
                
                # Perform search using existing _in_transaction method
                query_vector = [0.15] * 128
                results = client.vector_query.similarity_search_in_transaction(
                    table_name="vector_docs_sqlalchemy_demo",
                    vector_column="embedding",
                    query_vector=query_vector,
                    limit=2,
                    distance_type="l2",
                    select_columns=["id", "title", "category"],
                    connection=conn
                )
                print(f"✓ SQLAlchemy transaction search returned {len(results)} results:")
                for i, result in enumerate(results):
                    print(f"  {i+1}. ID: {result[0]}, Title: {result[1]}, Category: {result[2]} (distance: {result[-1]:.4f})")
                
                print("✓ SQLAlchemy transaction completed successfully - all operations committed")
                
        except Exception as e:
            print(f"⚠️  SQLAlchemy transaction failed: {e}")
            print("✓ SQLAlchemy transaction rolled back automatically")
        
        # Demo 12: Cleanup
        print("\n--- Demo 12: Cleanup ---")
        
        # Drop tables
        client.drop_table("vector_docs_hnsw_demo")
        client.drop_table("vector_docs_ivfflat_demo")
        client.drop_table("vector_docs_hnsw_fast")
        client.drop_table("vector_docs_hnsw_balanced")
        client.drop_table("vector_docs_hnsw_accurate")
        client.drop_table("vector_docs_transaction_demo")
        client.drop_table("vector_docs_sqlalchemy_demo")
        print("✓ Cleaned up test tables")
        
        # Disable HNSW indexing
        client.vector_index.disable_hnsw()
        print("✓ HNSW indexing disabled")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Disconnect
        client.disconnect()
        print("✓ Disconnected from MatrixOne")
    
    print("\n" + "=" * 80)
    print("HNSW Vector Index Demo Completed")
    print("=" * 80)


if __name__ == "__main__":
    main()
