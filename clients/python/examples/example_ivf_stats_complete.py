#!/usr/bin/env python3
"""
Complete Example: Create IVF Index and Get Statistics

This example demonstrates the complete workflow:
1. Create a table with vector column
2. Create an IVF index
3. Insert some sample data
4. Get IVF index statistics
"""

import asyncio
import random
from matrixone import Client, AsyncClient


def create_sample_data():
    """Create sample vector data"""
    # Generate random 128-dimensional vectors
    vectors = []
    for i in range(100):
        vector = [random.random() for _ in range(128)]
        vectors.append(vector)
    return vectors


def sync_complete_example():
    """Complete synchronous example"""
    print("=== Complete Synchronous IVF Stats Example ===")

    # Create client and connect
    client = Client()
    client.connect(host="localhost", port=6001, user="root", password="111", database="test")

    try:
        # Step 1: Create a table with vector column
        print("\n1. Creating table with vector column...")
        try:
            client.create_table(
                "vector_docs",
                columns={"id": "int", "title": "varchar(255)", "content": "text", "embedding": "vecf32(128)"},
                primary_key="id",
            )
            print("   ✓ Table 'vector_docs' created successfully")
        except Exception as e:
            print(f"   ⚠️  Table creation: {e}")

        # Step 2: Create IVF index
        print("\n2. Creating IVF index...")
        try:
            client.vector_ops.create_ivf("vector_docs", name="idx_embedding", column="embedding", lists=10)
            print("   ✓ IVF index 'idx_embedding' created successfully")
        except Exception as e:
            print(f"   ⚠️  Index creation: {e}")

        # Step 3: Insert sample data
        print("\n3. Inserting sample data...")
        try:
            vectors = create_sample_data()
            for i, vector in enumerate(vectors[:10]):  # Insert first 10 vectors
                client.vector_ops.insert(
                    "vector_docs",
                    {"title": f"Document {i+1}", "content": f"This is the content of document {i+1}", "embedding": vector},
                )
            print("   ✓ Sample data inserted successfully")
        except Exception as e:
            print(f"   ⚠️  Data insertion: {e}")

        # Step 4: Get IVF statistics
        print("\n4. Getting IVF index statistics...")
        try:
            stats = client.vector_ops.get_ivf_stats("vector_docs", "embedding")
            print(f"   ✓ IVF stats retrieved successfully:")
            print(f"     Database: {stats['database']}")
            print(f"     Table: {stats['table_name']}")
            print(f"     Column: {stats['column_name']}")
            print(f"     Index tables: {stats['index_tables']}")

            # Display distribution info
            distribution = stats['distribution']
            print(f"     Distribution:")
            print(f"       - Total centroids: {len(distribution['centroid_id'])}")
            print(f"       - Centroid counts: {distribution['centroid_count'][:5]}...")  # Show first 5
            print(f"       - Centroid IDs: {distribution['centroid_id'][:5]}...")  # Show first 5

        except Exception as e:
            print(f"   ❌ Error getting stats: {e}")

        # Step 5: Test auto-inference
        print("\n5. Testing auto-inference...")
        try:
            stats = client.vector_ops.get_ivf_stats("vector_docs")
            print(f"   ✓ Auto-inferred column: {stats['column_name']}")
        except Exception as e:
            print(f"   ❌ Auto-inference error: {e}")

        # Step 6: Test within transaction
        print("\n6. Testing within transaction...")
        try:
            with client.transaction() as tx:
                stats = tx.vector_ops.get_ivf_stats("vector_docs", "embedding")
                print(f"   ✓ Transaction stats: {len(stats['index_tables'])} index tables found")
        except Exception as e:
            print(f"   ❌ Transaction error: {e}")

    finally:
        # Cleanup
        print("\n7. Cleaning up...")
        try:
            client.drop_table("vector_docs")
            print("   ✓ Table dropped successfully")
        except Exception as e:
            print(f"   ⚠️  Cleanup: {e}")

        client.disconnect()


async def async_complete_example():
    """Complete asynchronous example"""
    print("\n=== Complete Asynchronous IVF Stats Example ===")

    # Create async client and connect
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")

    try:
        # Step 1: Create a table with vector column
        print("\n1. Creating table with vector column...")
        try:
            await client.create_table(
                "async_vector_docs",
                columns={
                    "id": "INT AUTO_INCREMENT PRIMARY KEY",
                    "title": "VARCHAR(255)",
                    "content": "TEXT",
                    "embedding": "vecf32(128)",
                },
            )
            print("   ✓ Table 'async_vector_docs' created successfully")
        except Exception as e:
            print(f"   ⚠️  Table creation: {e}")

        # Step 2: Create IVF index
        print("\n2. Creating IVF index...")
        try:
            await client.vector_ops.create_ivf("async_vector_docs", name="idx_async_embedding", column="embedding", lists=8)
            print("   ✓ IVF index 'idx_async_embedding' created successfully")
        except Exception as e:
            print(f"   ⚠️  Index creation: {e}")

        # Step 3: Insert sample data
        print("\n3. Inserting sample data...")
        try:
            vectors = create_sample_data()
            for i, vector in enumerate(vectors[:8]):  # Insert first 8 vectors
                await client.vector_ops.insert(
                    "async_vector_docs",
                    {
                        "title": f"Async Document {i+1}",
                        "content": f"This is the content of async document {i+1}",
                        "embedding": vector,
                    },
                )
            print("   ✓ Sample data inserted successfully")
        except Exception as e:
            print(f"   ⚠️  Data insertion: {e}")

        # Step 4: Get IVF statistics
        print("\n4. Getting IVF index statistics...")
        try:
            stats = await client.vector_ops.get_ivf_stats("async_vector_docs", "embedding")
            print(f"   ✓ IVF stats retrieved successfully:")
            print(f"     Database: {stats['database']}")
            print(f"     Table: {stats['table_name']}")
            print(f"     Column: {stats['column_name']}")
            print(f"     Index tables: {stats['index_tables']}")

            # Display distribution info
            distribution = stats['distribution']
            print(f"     Distribution:")
            print(f"       - Total centroids: {len(distribution['centroid_id'])}")
            print(f"       - Centroid counts: {distribution['centroid_count']}")
            print(f"       - Centroid IDs: {distribution['centroid_id']}")

        except Exception as e:
            print(f"   ❌ Error getting stats: {e}")

        # Step 5: Test auto-inference
        print("\n5. Testing auto-inference...")
        try:
            stats = await client.vector_ops.get_ivf_stats("async_vector_docs")
            print(f"   ✓ Auto-inferred column: {stats['column_name']}")
        except Exception as e:
            print(f"   ❌ Auto-inference error: {e}")

        # Step 6: Test within transaction
        print("\n6. Testing within transaction...")
        try:
            async with client.transaction() as tx:
                stats = await tx.vector_ops.get_ivf_stats("async_vector_docs", "embedding")
                print(f"   ✓ Transaction stats: {len(stats['index_tables'])} index tables found")
        except Exception as e:
            print(f"   ❌ Transaction error: {e}")

    finally:
        # Cleanup
        print("\n7. Cleaning up...")
        try:
            await client.drop_table("async_vector_docs")
            print("   ✓ Table dropped successfully")
        except Exception as e:
            print(f"   ⚠️  Cleanup: {e}")

        await client.disconnect()


def main():
    """Main function to run complete examples"""
    print("Complete IVF Index Statistics Example")
    print("=" * 60)

    # Run synchronous example
    sync_complete_example()

    # Run asynchronous example
    asyncio.run(async_complete_example())

    print("\n" + "=" * 60)
    print("Complete example finished!")


if __name__ == "__main__":
    main()
