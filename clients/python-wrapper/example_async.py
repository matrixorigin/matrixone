"""
MatrixOne Python SDK - Async Client Example
Demonstrates asynchronous operations with MatrixOne
"""

import asyncio
from matrixone import AsyncClient
from matrixone.snapshot import SnapshotLevel
from matrixone.exceptions import MoCtlError, ConnectionError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def async_basic_examples():
    """Basic async operations examples"""
    
    # Create async MatrixOne client
    client = AsyncClient()
    
    try:
        # Connect to MatrixOne
        await client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        logger.info("Connected to MatrixOne!")
        
        # Example 1: Basic async query
        logger.info("\n=== Example 1: Basic Async Query ===")
        
        result = await client.execute("SELECT 1 as test_value")
        logger.info(f"Query result: {result.scalar()}")
        
        # Example 2: Create table and insert data
        logger.info("\n=== Example 2: Create Table and Insert Data ===")
        
        await client.execute("""
            CREATE TABLE IF NOT EXISTS async_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert multiple users concurrently
        users = [
            ("Alice Johnson", "alice@example.com"),
            ("Bob Smith", "bob@example.com"),
            ("Charlie Brown", "charlie@example.com")
        ]
        
        tasks = []
        for name, email in users:
            task = client.execute(
                "INSERT INTO async_users (name, email) VALUES (%s, %s)",
                (name, email)
            )
            tasks.append(task)
        
        # Wait for all inserts to complete
        await asyncio.gather(*tasks)
        logger.info("Inserted users concurrently")
        
        # Query all users
        result = await client.execute("SELECT id, name, email FROM async_users ORDER BY id")
        logger.info("Users in database:")
        for row in result:
            logger.info(f"  - ID: {row[0]}, Name: {row[1]}, Email: {row[2]}")
        
        # Example 3: Async snapshot operations
        logger.info("\n=== Example 3: Async Snapshot Operations ===")
        
        # Create snapshot
        snapshot = await client.snapshots.create(
            name="async_snapshot",
            level=SnapshotLevel.DATABASE,
            database="test",
            description="Async snapshot example"
        )
        logger.info(f"Created snapshot: {snapshot.name}")
        
        # List snapshots
        snapshots = await client.snapshots.list()
        logger.info(f"Total snapshots: {len(snapshots)}")
        
        # Example 4: Async clone operations
        logger.info("\n=== Example 4: Async Clone Operations ===")
        
        # Clone database using snapshot
        await client.clone.clone_database_with_snapshot(
            "async_backup",
            "test",
            "async_snapshot",
            if_not_exists=True
        )
        logger.info("Cloned database using snapshot")
        
        # Verify clone
        backup_count = await client.execute("SELECT COUNT(*) FROM async_backup.async_users")
        original_count = await client.execute("SELECT COUNT(*) FROM async_users")
        
        logger.info(f"Original users: {original_count.scalar()}")
        logger.info(f"Backup users: {backup_count.scalar()}")
        
        # Example 5: Async mo_ctl operations
        logger.info("\n=== Example 5: Async mo_ctl Operations ===")
        
        try:
            # Force flush table
            result = await client.moctl.flush_table('test', 'async_users')
            logger.info(f"Flush table result: {result}")
            
            # Force checkpoint
            result = await client.moctl.checkpoint()
            logger.info(f"Checkpoint result: {result}")
            
        except MoCtlError as e:
            logger.error(f"mo_ctl operation failed: {e}")
        
        # Example 6: Async transaction
        logger.info("\n=== Example 6: Async Transaction ===")
        
        async with client.transaction() as tx:
            # Add more users in transaction
            await tx.execute(
                "INSERT INTO async_users (name, email) VALUES (%s, %s)",
                ("Diana Prince", "diana@example.com")
            )
            
            # Create snapshot in transaction
            tx_snapshot = await tx.snapshots.create(
                name="transaction_snapshot",
                level=SnapshotLevel.DATABASE,
                database="test"
            )
            logger.info(f"Created snapshot in transaction: {tx_snapshot.name}")
            
            # Clone in transaction
            await tx.clone.clone_database_with_snapshot(
                "tx_backup",
                "test",
                "transaction_snapshot",
                if_not_exists=True
            )
            logger.info("Cloned database in transaction")
        
        logger.info("Transaction completed successfully")
        
        # Example 7: Concurrent operations
        logger.info("\n=== Example 7: Concurrent Operations ===")
        
        # Run multiple operations concurrently
        tasks = [
            client.execute("SELECT COUNT(*) FROM async_users"),
            client.execute("SELECT COUNT(*) FROM async_backup.async_users"),
            client.execute("SELECT COUNT(*) FROM tx_backup.async_users"),
            client.snapshots.list()
        ]
        
        results = await asyncio.gather(*tasks)
        
        logger.info(f"Original users: {results[0].scalar()}")
        logger.info(f"Backup users: {results[1].scalar()}")
        logger.info(f"TX backup users: {results[2].scalar()}")
        logger.info(f"Total snapshots: {len(results[3])}")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        
        cleanup_tasks = [
            client.execute("DROP DATABASE IF EXISTS async_backup"),
            client.execute("DROP DATABASE IF EXISTS tx_backup"),
            client.snapshots.delete("async_snapshot"),
            client.snapshots.delete("transaction_snapshot"),
            client.execute("DROP TABLE IF EXISTS async_users")
        ]
        
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        logger.info("Cleanup completed")
        
    except ConnectionError as e:
        logger.error(f"Connection failed: {e}")
        logger.info("This is expected if MatrixOne is not running or accessible")
        logger.info("The async client is working correctly, but needs a database connection")
        return False
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        await client.disconnect()
        logger.info("Disconnected from MatrixOne")
    
    return True


async def async_performance_example():
    """Async performance example with concurrent operations"""
    
    logger.info("\n" + "="*60)
    logger.info("Async Performance Example")
    logger.info("="*60)
    
    client = AsyncClient()
    
    try:
        await client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        # Create test table
        await client.execute("""
            CREATE TABLE IF NOT EXISTS performance_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert data concurrently
        logger.info("Inserting data concurrently...")
        start_time = asyncio.get_event_loop().time()
        
        tasks = []
        for i in range(100):
            task = client.execute(
                "INSERT INTO performance_test (data) VALUES (%s)",
                (f"test_data_{i}",)
            )
            tasks.append(task)
        
        await asyncio.gather(*tasks)
        
        insert_time = asyncio.get_event_loop().time() - start_time
        logger.info(f"Inserted 100 records in {insert_time:.2f} seconds")
        
        # Query data concurrently
        logger.info("Querying data concurrently...")
        start_time = asyncio.get_event_loop().time()
        
        query_tasks = []
        for i in range(10):
            task = client.execute("SELECT COUNT(*) FROM performance_test")
            query_tasks.append(task)
        
        results = await asyncio.gather(*query_tasks)
        
        query_time = asyncio.get_event_loop().time() - start_time
        logger.info(f"Executed 10 queries in {query_time:.2f} seconds")
        logger.info(f"Average query time: {query_time/10:.3f} seconds")
        
        # Cleanup
        await client.execute("DROP TABLE IF EXISTS performance_test")
        
    except ConnectionError as e:
        logger.error(f"Performance example failed - no database connection: {e}")
        return False
    except Exception as e:
        logger.error(f"Performance example failed: {e}")
        raise
    finally:
        await client.disconnect()
    
    return True


async def async_error_handling_example():
    """Async error handling example"""
    
    logger.info("\n" + "="*60)
    logger.info("Async Error Handling Example")
    logger.info("="*60)
    
    client = AsyncClient()
    
    try:
        await client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        # Example 1: Query error handling
        logger.info("\n--- Example 1: Query Error Handling ---")
        
        try:
            await client.execute("SELECT * FROM nonexistent_table")
        except Exception as e:
            logger.info(f"Caught query error: {e}")
        
        # Example 2: Transaction rollback
        logger.info("\n--- Example 2: Transaction Rollback ---")
        
        try:
            async with client.transaction() as tx:
                await tx.execute("INSERT INTO test_table (name) VALUES ('test')")
                # This will cause an error
                await tx.execute("INSERT INTO nonexistent_table (name) VALUES ('test')")
        except Exception as e:
            logger.info(f"Transaction rolled back: {e}")
        
        # Example 3: mo_ctl error handling
        logger.info("\n--- Example 3: mo_ctl Error Handling ---")
        
        try:
            await client.moctl.flush_table('nonexistent_db', 'nonexistent_table')
        except MoCtlError as e:
            logger.info(f"mo_ctl error caught: {e}")
        
        # Example 4: Concurrent error handling
        logger.info("\n--- Example 4: Concurrent Error Handling ---")
        
        tasks = [
            client.execute("SELECT 1"),  # This will succeed
            client.execute("SELECT * FROM nonexistent_table"),  # This will fail
            client.execute("SELECT 2"),  # This will succeed
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.info(f"Task {i} failed: {result}")
            else:
                logger.info(f"Task {i} succeeded: {result.scalar()}")
        
    except ConnectionError as e:
        logger.error(f"Error handling example failed - no database connection: {e}")
        return False
    except Exception as e:
        logger.error(f"Error handling example failed: {e}")
        raise
    finally:
        await client.disconnect()
    
    return True


async def demo_without_database():
    """Demo async functionality without database connection"""
    
    logger.info("\n" + "="*60)
    logger.info("Async Client Demo (No Database Required)")
    logger.info("="*60)
    
    # Create async client
    client = AsyncClient()
    logger.info("✓ AsyncClient created successfully")
    
    # Show available methods
    logger.info("✓ Available async methods:")
    logger.info(f"  - Snapshots: {[m for m in dir(client.snapshots) if not m.startswith('_')]}")
    logger.info(f"  - Clone: {[m for m in dir(client.clone) if not m.startswith('_')]}")
    logger.info(f"  - MoCtl: {[m for m in dir(client.moctl) if not m.startswith('_')]}")
    
    # Show SnapshotLevel enum
    logger.info("✓ SnapshotLevel enum values:")
    for level in SnapshotLevel:
        logger.info(f"  - {level.name}: {level.value}")
    
    # Show async syntax
    logger.info("✓ Async syntax examples:")
    logger.info("  await client.connect(...)")
    logger.info("  result = await client.execute('SELECT 1')")
    logger.info("  snapshot = await client.snapshots.create(...)")
    logger.info("  await client.clone.clone_database(...)")
    logger.info("  result = await client.moctl.flush_table(...)")
    
    logger.info("✓ AsyncClient is ready to use!")


async def main():
    """Main async function"""
    print("MatrixOne Async Client Examples")
    print("="*50)
    
    try:
        # Try to run examples with database
        success = await async_basic_examples()
        
        if success:
            await async_performance_example()
            await async_error_handling_example()
            
            print("\n" + "="*50)
            print("All async examples completed successfully!")
            print("\nKey takeaways:")
            print("- Async operations provide better performance for concurrent tasks")
            print("- All MatrixOne operations are now async: execute, snapshots, clone, mo_ctl")
            print("- Transactions work seamlessly with async operations")
            print("- Error handling is consistent across sync and async operations")
            print("- Concurrent operations can significantly improve performance")
        else:
            # Run demo without database
            await demo_without_database()
            
            print("\n" + "="*50)
            print("Async client demo completed!")
            print("\nTo run full examples:")
            print("1. Start MatrixOne database")
            print("2. Update connection parameters in this script")
            print("3. Run the script again")
            print("\nThe async client is working correctly!")
        
    except Exception as e:
        print(f"Async examples failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
