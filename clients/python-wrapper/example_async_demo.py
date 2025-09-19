"""
MatrixOne Python SDK - Async Client Demo
Demonstrates asynchronous operations without requiring actual database connection
"""

import asyncio
import sys
import os
from unittest.mock import Mock, AsyncMock, patch

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import AsyncClient, AsyncResultSet, SnapshotLevel
from matrixone.exceptions import MoCtlError


async def demo_async_client_creation():
    """Demo: AsyncClient creation and basic properties"""
    print("=== Demo 1: AsyncClient Creation ===")
    
    client = AsyncClient()
    print(f"✓ AsyncClient created: {type(client).__name__}")
    print(f"✓ Snapshots manager: {type(client.snapshots).__name__}")
    print(f"✓ Clone manager: {type(client.clone).__name__}")
    print(f"✓ MoCtl manager: {type(client.moctl).__name__}")
    print(f"✓ Connection timeout: {client.connection_timeout}s")
    print(f"✓ Query timeout: {client.query_timeout}s")
    print(f"✓ Auto commit: {client.auto_commit}")
    print(f"✓ Charset: {client.charset}")


async def demo_async_result_set():
    """Demo: AsyncResultSet functionality"""
    print("\n=== Demo 2: AsyncResultSet ===")
    
    # Create mock result set
    columns = ['id', 'name', 'email', 'created_at']
    rows = [
        (1, 'Alice Johnson', 'alice@example.com', '2024-01-01 10:00:00'),
        (2, 'Bob Smith', 'bob@example.com', '2024-01-02 11:00:00'),
        (3, 'Charlie Brown', 'charlie@example.com', '2024-01-03 12:00:00')
    ]
    
    result = AsyncResultSet(columns, rows)
    
    print(f"✓ Columns: {result.columns}")
    print(f"✓ Total rows: {len(result)}")
    print(f"✓ First row: {result.fetchone()}")
    print(f"✓ Scalar value: {result.scalar()}")
    
    print("✓ All rows:")
    for i, row in enumerate(result, 1):
        print(f"  {i}. ID: {row[0]}, Name: {row[1]}, Email: {row[2]}")


async def demo_async_managers():
    """Demo: Async managers functionality"""
    print("\n=== Demo 3: Async Managers ===")
    
    client = AsyncClient()
    
    # Demo snapshot manager
    print("✓ AsyncSnapshotManager methods:")
    snapshot_methods = [method for method in dir(client.snapshots) if not method.startswith('_')]
    for method in snapshot_methods:
        print(f"  - {method}")
    
    # Demo clone manager
    print("✓ AsyncCloneManager methods:")
    clone_methods = [method for method in dir(client.clone) if not method.startswith('_')]
    for method in clone_methods:
        print(f"  - {method}")
    
    # Demo moctl manager
    print("✓ AsyncMoCtlManager methods:")
    moctl_methods = [method for method in dir(client.moctl) if not method.startswith('_')]
    for method in moctl_methods:
        print(f"  - {method}")


async def demo_snapshot_level_enum():
    """Demo: SnapshotLevel enum"""
    print("\n=== Demo 4: SnapshotLevel Enum ===")
    
    print("✓ Available snapshot levels:")
    for level in SnapshotLevel:
        print(f"  - {level.name}: {level.value}")
    
    # Demo enum usage
    print("✓ Enum usage examples:")
    print(f"  - Cluster level: {SnapshotLevel.CLUSTER}")
    print(f"  - Database level: {SnapshotLevel.DATABASE}")
    print(f"  - Table level: {SnapshotLevel.TABLE}")
    
    # Demo string to enum conversion
    print("✓ String to enum conversion:")
    cluster_level = SnapshotLevel("cluster")
    print(f"  - 'cluster' -> {cluster_level}")


async def demo_async_context_manager():
    """Demo: Async context manager"""
    print("\n=== Demo 5: Async Context Manager ===")
    
    print("✓ AsyncClient supports context manager:")
    print("  async with AsyncClient() as client:")
    print("      await client.connect(...)")
    print("      # use client")
    print("      # automatically disconnects")


async def demo_async_transaction():
    """Demo: Async transaction"""
    print("\n=== Demo 6: Async Transaction ===")
    
    print("✓ Async transaction syntax:")
    print("  async with client.transaction() as tx:")
    print("      await tx.execute('INSERT INTO users ...')")
    print("      await tx.snapshots.create('snap1', SnapshotLevel.DATABASE)")
    print("      await tx.clone.clone_database('backup', 'source')")
    print("      # automatically commits or rolls back")


async def demo_concurrent_operations():
    """Demo: Concurrent operations concept"""
    print("\n=== Demo 7: Concurrent Operations ===")
    
    print("✓ Concurrent operations example:")
    print("  tasks = [")
    print("      client.execute('SELECT COUNT(*) FROM users'),")
    print("      client.execute('SELECT COUNT(*) FROM orders'),")
    print("      client.execute('SELECT COUNT(*) FROM products')")
    print("  ]")
    print("  results = await asyncio.gather(*tasks)")
    print("  # All queries execute concurrently!")


async def demo_error_handling():
    """Demo: Error handling"""
    print("\n=== Demo 8: Error Handling ===")
    
    print("✓ Async error handling:")
    print("  try:")
    print("      await client.execute('SELECT * FROM nonexistent_table')")
    print("  except QueryError as e:")
    print("      print(f'Query failed: {e}')")
    print("")
    print("  try:")
    print("      await client.moctl.flush_table('db', 'nonexistent')")
    print("  except MoCtlError as e:")
    print("      print(f'mo_ctl failed: {e}')")


async def demo_performance_benefits():
    """Demo: Performance benefits"""
    print("\n=== Demo 9: Performance Benefits ===")
    
    print("✓ Async performance advantages:")
    print("  - Non-blocking I/O operations")
    print("  - Better resource utilization")
    print("  - Higher concurrency")
    print("  - Lower memory footprint")
    print("  - Better scalability")
    
    print("\n✓ Use cases for async:")
    print("  - High-concurrency applications")
    print("  - I/O-intensive operations")
    print("  - Real-time data processing")
    print("  - Microservices architectures")
    print("  - Web APIs and services")


async def demo_migration_guide():
    """Demo: Migration from sync to async"""
    print("\n=== Demo 10: Migration Guide ===")
    
    print("✓ Sync to Async migration:")
    print("")
    print("  # Sync version")
    print("  client = Client()")
    print("  client.connect(...)")
    print("  result = client.execute('SELECT 1')")
    print("  client.disconnect()")
    print("")
    print("  # Async version")
    print("  client = AsyncClient()")
    print("  await client.connect(...)")
    print("  result = await client.execute('SELECT 1')")
    print("  await client.disconnect()")
    print("")
    print("  # Or use context manager")
    print("  async with AsyncClient() as client:")
    print("      await client.connect(...)")
    print("      result = await client.execute('SELECT 1')")


async def main():
    """Main demo function"""
    print("MatrixOne Python SDK - Async Client Demo")
    print("=" * 60)
    print("This demo shows async functionality without requiring database connection")
    print("=" * 60)
    
    try:
        await demo_async_client_creation()
        await demo_async_result_set()
        await demo_async_managers()
        await demo_snapshot_level_enum()
        await demo_async_context_manager()
        await demo_async_transaction()
        await demo_concurrent_operations()
        await demo_error_handling()
        await demo_performance_benefits()
        await demo_migration_guide()
        
        print("\n" + "=" * 60)
        print("✅ All async demos completed successfully!")
        print("\n🎉 MatrixOne AsyncClient is ready to use!")
        print("\n📚 Next steps:")
        print("  1. Connect to a real MatrixOne database")
        print("  2. Try the async operations")
        print("  3. Build high-performance applications")
        print("  4. Leverage concurrent operations for better performance")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
