#!/usr/bin/env python3
"""
MatrixOne Vector Operations Comprehensive Example

This example demonstrates all vector operations available in MatrixOne:
- Table management (create, drop)
- Data operations (insert, update, delete)
- Vector indexing (create, drop, IVF configuration)
- Vector queries (similarity search, range search)
- Transaction support
- Performance optimization (column projection)
- PineconeCompatibleIndex (Pinecone-compatible API)
- Best practices and error handling

This replaces examples 16-19 with a single comprehensive demonstration.
"""

import sys
import os
import time

# Add the parent directory to the path to import matrixone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from matrixone import Client
from matrixone.config import get_connection_params, print_config


def main():
    """Main function demonstrating comprehensive vector operations"""
    
    # Print configuration
    print_config()
    
    # Get connection parameters
    host, port, user, password, database = get_connection_params()
    
    # Create client and connect
    client = Client()
    client.connect(host=host, port=port, user=user, password=password, database=database)
    
    print("=" * 80)
    print("MatrixOne Vector Operations Comprehensive Demo")
    print("=" * 80)
    
    try:
        # Clean up any existing tables first
        print("\n--- Cleanup ---")
        try:
            client.drop_table("vector_docs_comprehensive")
            client.drop_table("vector_docs_tx")
            print("✓ Cleaned up existing tables")
        except:
            pass  # Tables might not exist
        
        # Enable IVF indexing for vector operations
        print("\n--- IVF Configuration ---")
        client.vector_index.enable_ivf(probe_limit=1)
        print("✓ IVF indexing enabled")
        
        # Demo 1: Vector Table Operations
        print("\n--- Demo 1: Vector Table Operations ---")
        
        # Define comprehensive table schema
        schema = {
            'id': {'type': 'int', 'primary_key': True},
            'title': {'type': 'varchar', 'length': 200},
            'content': {'type': 'text'},
            'embedding': {'type': 'vector', 'dimension': 128, 'precision': 'f32'},
            'category': {'type': 'varchar', 'length': 50},
            'score': {'type': 'int'},
            'metadata': {'type': 'text'}
        }
        
        # Create table using chain operations
        client.create_table("vector_docs_comprehensive", {
            'id': 'bigint',
            'title': 'varchar(200)',
            'content': 'text',
            'embedding': 'vector(128,f32)',
            'category': 'varchar(50)',
            'score': 'int',
            'metadata': 'text'
        }, primary_key='id')
        print("✓ Created comprehensive vector table")
        
        # Demo 2: Vector Data Operations
        print("\n--- Demo 2: Vector Data Operations ---")
        
        # Insert single document
        doc1 = {
            'id': 1,
            'title': 'Machine Learning Fundamentals',
            'content': 'Introduction to machine learning concepts and algorithms',
            'embedding': [0.1] * 128,
            'category': 'AI',
            'score': 85,
            'metadata': '{"tags": ["ml", "ai"], "difficulty": "beginner"}'
        }
        
        client.vector_data.insert("vector_docs_comprehensive", doc1)
        print("✓ Inserted single document")
        
        # Batch insert multiple documents
        docs = [
            {
                'id': 2,
                'title': 'Deep Learning Networks',
                'content': 'Understanding neural networks and deep learning architectures',
                'embedding': [0.2] * 128,
                'category': 'AI',
                'score': 92,
                'metadata': '{"tags": ["dl", "neural"], "difficulty": "intermediate"}'
            },
            {
                'id': 3,
                'title': 'Database Systems',
                'content': 'Relational database fundamentals and SQL optimization',
                'embedding': [0.3] * 128,
                'category': 'Database',
                'score': 78,
                'metadata': '{"tags": ["sql", "rdbms"], "difficulty": "beginner"}'
            },
            {
                'id': 4,
                'title': 'Vector Databases',
                'content': 'Specialized databases for vector similarity search',
                'embedding': [0.4] * 128,
                'category': 'Database',
                'score': 88,
                'metadata': '{"tags": ["vector", "search"], "difficulty": "advanced"}'
            },
            {
                'id': 5,
                'title': 'SQL Optimization',
                'content': 'Advanced SQL query optimization techniques',
                'embedding': [0.5] * 128,
                'category': 'Database',
                'score': 95,
                'metadata': '{"tags": ["sql", "performance"], "difficulty": "expert"}'
            }
        ]
        
        client.vector_data.batch_insert("vector_docs_comprehensive", docs)
        print("✓ Batch inserted 4 documents")
        
        # Demo 3: Vector Index Operations
        print("\n--- Demo 3: Vector Index Operations ---")
        
        # Create vector index
        client.vector_index.create_ivf(
            table_name="vector_docs_comprehensive",
            name="idx_embedding_comprehensive",
            column="embedding",
            lists=32,
            op_type="vector_l2_ops"
        )
        print("✓ Created vector index")
        
        # Demo 4: Vector Query Operations - Performance Comparison
        print("\n--- Demo 4: Vector Query Operations - Performance Comparison ---")
        query_vector = [0.25] * 128
        
        # Search with all columns (inefficient)
        print("Searching with all columns...")
        start_time = time.time()
        all_results = client.vector_query.similarity_search(
            table_name="vector_docs_comprehensive",
            vector_column="embedding",
            query_vector=query_vector,
            limit=3,
            distance_type="l2"
        )
        all_columns_time = time.time() - start_time
        print(f"✓ All columns search took {all_columns_time:.4f} seconds")
        print(f"  Returned {len(all_results)} results with {len(all_results[0]) if all_results else 0} columns each")
        
        # Search with selected columns only (efficient)
        print("Searching with selected columns only...")
        start_time = time.time()
        selected_results = client.vector_query.similarity_search(
            table_name="vector_docs_comprehensive",
            vector_column="embedding",
            query_vector=query_vector,
            limit=3,
            distance_type="l2",
            select_columns=["id", "title", "category", "score"]
        )
        selected_columns_time = time.time() - start_time
        print(f"✓ Selected columns search took {selected_columns_time:.4f} seconds")
        print(f"  Returned {len(selected_results)} results with {len(selected_results[0]) if selected_results else 0} columns each")
        
        improvement = ((all_columns_time - selected_columns_time) / all_columns_time * 100) if all_columns_time > 0 else 0
        print(f"Performance improvement: {improvement:.1f}% faster")
        
        # Display results
        print("Search results (with column projection):")
        for i, result in enumerate(selected_results):
            print(f"  {i+1}. ID: {result[0]}, Title: {result[1]}, Category: {result[2]}, Score: {result[3]} (distance: {result[-1]:.4f})")
        
        # Demo 5: Range Search with Projection
        print("\n--- Demo 5: Range Search with Projection ---")
        
        range_results = client.vector_query.range_search(
            table_name="vector_docs_comprehensive",
            vector_column="embedding",
            query_vector=query_vector,
            max_distance=0.5,
            distance_type="l2",
            select_columns=["id", "title", "category"]
        )
        
        print(f"Range search (distance <= 0.5) returned {len(range_results)} results:")
        for i, result in enumerate(range_results):
            print(f"  {i+1}. [{result[2]}] {result[1]} (distance: {result[-1]:.4f})")
        
        # Demo 6: Transaction Operations - Method 1 (Recommended)
        print("\n--- Demo 6: Transaction Operations - Method 1 (Recommended) ---")
        print("Using client.transaction() context manager with tx.vector_query methods")
        
        with client.transaction() as tx:
            # Create another table within transaction
            tx.create_table("vector_docs_tx", {
                'id': 'bigint',
                'title': 'varchar(200)',
                'content': 'text',
                'embedding': 'vector(128,f32)',
                'category': 'varchar(50)',
                'score': 'int',
                'metadata': 'text'
            }, primary_key='id')
            print("✓ Created table in transaction")
            
            # Insert data
            tx_doc = {
                'id': 6,
                'title': 'Vector Search Optimization',
                'content': 'Advanced techniques for vector search performance',
                'embedding': [0.6] * 128,
                'category': 'Database',
                'score': 90,
                'metadata': '{"tags": ["vector", "optimization"], "difficulty": "expert"}'
            }
            tx.vector_data.insert("vector_docs_tx", tx_doc)
            print("✓ Inserted document in transaction")
            
            # Create index
            tx.vector_index.create_ivf(
                table_name="vector_docs_tx",
                name="idx_embedding_tx",
                column="embedding",
                lists=16,
                op_type="vector_l2_ops"
            )
            print("✓ Created index in transaction")
            
            # Query data with selected columns
            tx_results = tx.vector_query.similarity_search(
                table_name="vector_docs_tx",
                vector_column="embedding",
                query_vector=[0.6] * 128,
                limit=1,
                distance_type="l2",
                select_columns=["id", "title", "category"]
            )
            print(f"✓ Query in transaction returned {len(tx_results)} results")
            if tx_results:
                # Convert ResultSet to list for easier access
                results_list = list(tx_results)
                if results_list:
                    result = results_list[0]
                    print(f"  - ID: {result[0]}, Title: {result[1]}, Category: {result[2]}")
        
        print("✓ Transaction completed successfully")
        
        # Demo 7: Transaction Operations - Method 2 (Advanced)
        print("\n--- Demo 7: Transaction Operations - Method 2 (Advanced) ---")
        print("Using _in_transaction methods with explicit connection")
        
        engine = client.get_sqlalchemy_engine()
        with engine.begin() as conn:
            # Insert using raw SQL
            from sqlalchemy import text
            vector_str = "[" + ",".join(map(str, [0.7] * 128)) + "]"
            conn.execute(text("""
                INSERT INTO vector_docs_comprehensive (id, title, content, embedding, category, score, metadata)
                VALUES (:id, :title, :content, :embedding, :category, :score, :metadata)
            """), {
                'id': 7,
                'title': 'Advanced Vector Operations',
                'content': 'Complex vector operations and optimizations',
                'embedding': vector_str,
                'category': 'AI',
                'score': 96,
                'metadata': '{"tags": ["vector", "advanced"], "difficulty": "expert"}'
            })
            print("✓ Inserted document using raw SQL in transaction")
            
            # Use _in_transaction methods
            tx_similarity_results = client.vector_query.similarity_search_in_transaction(
                table_name="vector_docs_comprehensive",
                vector_column="embedding",
                query_vector=[0.7] * 128,
                limit=2,
                distance_type="l2",
                select_columns=["id", "title", "category"],
                connection=conn
            )
            print(f"✓ similarity_search_in_transaction returned {len(tx_similarity_results)} results")
            
            tx_range_results = client.vector_query.range_search_in_transaction(
                table_name="vector_docs_comprehensive",
                vector_column="embedding",
                query_vector=[0.7] * 128,
                max_distance=0.3,
                distance_type="l2",
                select_columns=["id", "title"],
                connection=conn
            )
            print(f"✓ range_search_in_transaction returned {len(tx_range_results)} results")
        
        print("✓ Transaction with _in_transaction methods completed successfully")
        
        # Demo 8: Error Handling and Rollback
        print("\n--- Demo 8: Error Handling and Rollback ---")
        
        try:
            with client.transaction() as tx:
                # Update an existing document
                tx.vector_data.update(
                    table_name="vector_docs_comprehensive",
                    data={'title': 'This Will Be Rolled Back'},
                    where_clause="id = 1"
                )
                print("✓ Updated document in transaction")
                
                # Query to verify the change
                tx_results = tx.vector_query.similarity_search(
                    table_name="vector_docs_comprehensive",
                    vector_column="embedding",
                    query_vector=[0.1] * 128,
                    limit=1,
                    distance_type="l2",
                    select_columns=["id", "title"]
                )
                if tx_results:
                    print(f"✓ Query found title: '{tx_results[0][1]}' in transaction")
                
                # Simulate error
                raise Exception("Simulated error for rollback test")
                
        except Exception as e:
            print(f"✓ Transaction rolled back due to: {e}")
        
        # Verify rollback worked by checking the title
        final_results = client.vector_query.similarity_search(
            table_name="vector_docs_comprehensive",
            vector_column="embedding",
            query_vector=[0.1] * 128,
            limit=1,
            distance_type="l2",
            select_columns=["id", "title"]
        )
        
        if final_results and final_results[0][1] == "Machine Learning Fundamentals":
            print("✓ Rollback successful - title was restored to original")
        else:
            print("⚠️  Rollback may have failed - title was not restored")
        
        # Demo 9: Complex Chain Operations
        print("\n--- Demo 9: Complex Chain Operations ---")
        
        # Update document
        client.vector_data.update(
            table_name="vector_docs_comprehensive",
            data={'title': 'Updated ML Fundamentals', 'embedding': [0.11] * 128, 'score': 87},
            where_clause="id = 1"
        )
        print("✓ Updated document using chain operations")
        
        # Verify update with query - selected columns only
        updated_results = client.vector_query.similarity_search(
            table_name="vector_docs_comprehensive",
            vector_column="embedding",
            query_vector=[0.11] * 128,
            limit=1,
            distance_type="l2",
            select_columns=["id", "title", "score"]
        )
        
        if updated_results:
            print(f"✓ Verified update: ID {updated_results[0][0]}, Title: {updated_results[0][1]}, Score: {updated_results[0][2]}")
        
        # Demo 10: Performance Comparison
        print("\n--- Demo 10: Performance Comparison ---")
        
        # Regular queries
        start_time = time.time()
        for _ in range(10):
            client.vector_query.similarity_search(
                table_name="vector_docs_comprehensive",
                vector_column="embedding",
                query_vector=query_vector,
                limit=3,
                distance_type="l2",
                select_columns=["id", "title"]
            )
        regular_time = time.time() - start_time
        print(f"Regular queries (10 iterations): {regular_time:.4f} seconds")
        
        # Transaction queries
        start_time = time.time()
        for _ in range(10):
            with client.transaction() as tx:
                tx.vector_query.similarity_search(
                    table_name="vector_docs_comprehensive",
                    vector_column="embedding",
                    query_vector=query_vector,
                    limit=3,
                    distance_type="l2",
                    select_columns=["id", "title"]
                )
        transaction_time = time.time() - start_time
        print(f"Transaction queries (10 iterations): {transaction_time:.4f} seconds")
        
        performance_diff = ((regular_time - transaction_time) / regular_time * 100) if regular_time > 0 else 0
        print(f"Performance difference: {performance_diff:.1f}%")
        
        # Demo 11: Best Practices Summary
        print("\n--- Demo 11: Best Practices Summary ---")
        
        print("✅ RECOMMENDED APPROACHES:")
        print("1. Use client.transaction() context manager for simple cases")
        print("2. Use tx.vector_query.similarity_search() within transaction context")
        print("3. Use tx.vector_query.range_search() within transaction context")
        print("4. Always specify select_columns for better performance")
        print("5. Handle exceptions properly for rollback scenarios")
        print("6. Use batch operations for multiple data insertions")
        print("7. Enable IVF indexing for better vector search performance")
        
        print("\n✅ ADVANCED APPROACHES:")
        print("1. Use _in_transaction methods for complex transaction logic")
        print("2. Pass explicit connection parameter for fine-grained control")
        print("3. Combine with raw SQL for complex operations")
        print("4. Use column projection to reduce memory usage")
        
        print("\n❌ AVOID THESE PATTERNS:")
        print("1. Don't use regular client.vector_query methods inside transactions")
        print("2. Don't forget to specify select_columns in production")
        print("3. Don't ignore transaction rollback scenarios")
        print("4. Don't mix transaction and non-transaction operations without care")
        print("5. Don't select all columns when you only need a few")
        
        # Demo 12: Final State
        print("\n--- Demo 12: Final State ---")
        
        all_results = client.vector_query.similarity_search(
            table_name="vector_docs_comprehensive",
            vector_column="embedding",
            query_vector=query_vector,
            limit=10,
            distance_type="l2",
            select_columns=["id", "title", "category", "score"]
        )
        
        print(f"Final state - {len(all_results)} documents in table:")
        for result in all_results:
            print(f"  - ID: {result[0]}, Title: {result[1]}, Category: {result[2]}, Score: {result[3]}")
        
        # Demo 13: PineconeCompatibleIndex (Pinecone-compatible API)
        print("\n--- Demo 13: PineconeCompatibleIndex (Pinecone-compatible API) ---")
        
        # Create a dedicated table for PineconeCompatibleIndex demo
        client.execute("CREATE DATABASE IF NOT EXISTS pinecone_demo")
        client.execute("USE pinecone_demo")
        
        client.execute("""
            CREATE TABLE IF NOT EXISTS pinecone_docs (
                id VARCHAR(50) PRIMARY KEY,
                title VARCHAR(200),
                content TEXT,
                category VARCHAR(50),
                embedding vecf32(128)
            )
        """)
        
        # Create vector index
        client.vector_index.create_ivf(
            table_name="pinecone_docs",
            name="idx_pinecone_embedding",
            column="embedding",
            lists=10
        )
        
        # Insert sample data
        sample_docs = [
            {
                "id": "pinecone_doc1",
                "title": "Pinecone-Compatible Search",
                "content": "This demonstrates Pinecone-compatible vector search",
                "category": "Demo",
                "embedding": [0.1] * 128
            },
            {
                "id": "pinecone_doc2", 
                "title": "Vector Search API",
                "content": "High-level vector search interface",
                "category": "API",
                "embedding": [0.2] * 128
            }
        ]
        
        for doc in sample_docs:
            vector_str = "[" + ",".join(map(str, doc["embedding"])) + "]"
            client.execute(f"""
                INSERT INTO pinecone_docs (id, title, content, category, embedding) 
                VALUES ('{doc["id"]}', '{doc["title"]}', '{doc["content"]}', '{doc["category"]}', '{vector_str}')
            """)
        
        # Get PineconeCompatibleIndex object (Pinecone-compatible interface)
        index = client.get_pinecone_index(
            table_name="pinecone_docs",
            vector_column="embedding",
            id_column="id",
            metadata_columns=["title", "content", "category"]
        )
        
        print(f"✓ Created PineconeCompatibleIndex for table 'pinecone_docs'")
        print(f"✓ Vector column: {index.vector_column}")
        print(f"✓ ID column: {index.id_column}")
        print(f"✓ Metadata columns: {index.metadata_columns}")
        
        # Query the index (Pinecone-compatible API)
        query_vector = [0.15] * 128
        results = index.query(
            vector=query_vector,
            top_k=2,
            include_metadata=True,
            include_values=False
        )
        
        print(f"\n✓ Query results:")
        print(f"  - Found {len(results.matches)} matches")
        print(f"  - Namespace: {results.namespace}")
        print(f"  - Usage: {results.usage}")
        
        for i, match in enumerate(results.matches):
            print(f"\n  Match {i+1}:")
            print(f"    - ID: {match.id}")
            print(f"    - Score: {match.score:.4f}")
            print(f"    - Title: {match.metadata.get('title', 'N/A')}")
            print(f"    - Category: {match.metadata.get('category', 'N/A')}")
        
        # Test delete functionality (only for IVF indexes)
        vector_str = "[" + ",".join(map(str, [0.4] * 128)) + "]"
        client.execute(f"""
            INSERT INTO pinecone_docs (id, title, content, category, embedding) VALUES
            ('pinecone_doc3', 'Test Document', 'This is a test document', 'Test', '{vector_str}')
        """)
        print(f"\n✓ Inserted test vector using SQL")
        
        # Test delete functionality
        index.delete(["pinecone_doc3"])
        print(f"✓ Deleted vector with ID 'pinecone_doc3'")
        
        # Get index statistics
        stats = index.describe_index_stats()
        print(f"\n✓ Index statistics:")
        print(f"  - Dimension: {stats['dimension']}")
        print(f"  - Total vectors: {stats['total_vector_count']}")
        print(f"  - Namespaces: {list(stats['namespaces'].keys())}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        print("\n--- Cleanup ---")
        try:
            client.drop_table("vector_docs_comprehensive")
            client.drop_table("vector_docs_tx")
            client.execute("DROP TABLE IF EXISTS pinecone_docs")
            client.execute("DROP DATABASE IF EXISTS pinecone_demo")
            print("✓ Cleaned up test tables")
        except:
            pass
        
        # Disconnect
        client.disconnect()
        print("✓ Disconnected from MatrixOne")
    
    print("\n" + "=" * 80)
    print("Vector Operations Comprehensive Demo Completed")
    print("Including Pinecone-compatible PineconeCompatibleIndex API")
    print("=" * 80)


if __name__ == "__main__":
    main()
