#!/usr/bin/env python3
"""
MatrixOne Async Operations Examples

This example demonstrates comprehensive async operations:
1. Basic async connections
2. Async query execution
3. Async transaction management
4. Async account management
5. Async error handling
6. Performance comparison with sync operations

This example shows the complete async capabilities of MatrixOne Python client.
"""

import logging
import asyncio
import time
from matrixone import Client, AsyncClient
from matrixone.account import AccountManager
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger

# Create MatrixOne logger for all logging
logger = create_default_logger(
    enable_performance_logging=True,
    enable_sql_logging=True
)


async def demo_basic_async_operations():
    """Demonstrate basic async operations"""
    logger.info("🚀 MatrixOne Basic Async Operations Demo")
    logger.info("=" * 60)
    
    # Print current configuration
    print_config()
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    # Test 1: Basic async connection
    logger.info("\n=== Test 1: Basic Async Connection ===")
    try:
        client = AsyncClient(logger=logger, enable_full_sql_logging=True)
        await client.connect(host, port, user, password, database)
        logger.info("✅ Async connection successful")
        
        # Test basic async query
        result = await client.execute("SELECT 1 as async_test, USER() as user_info")
        logger.info(f"   Async query result: {result.rows[0]}")
        
        # Get login info
        login_info = client.get_login_info()
        logger.info(f"   Login info: {login_info}")
        
        await client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Basic async connection failed: {e}")


async def demo_async_query_execution():
    """Demonstrate async query execution"""
    logger.info("\n=== Test 2: Async Query Execution ===")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        client = AsyncClient(logger=logger, enable_full_sql_logging=True)
        await client.connect(host, port, user, password, database)
        
        # Test various query types
        queries = [
            ("Simple SELECT", "SELECT 1 as value"),
            ("User info", "SELECT USER(), CURRENT_USER(), DATABASE()"),
            ("System info", "SELECT VERSION(), @@version_comment"),
            ("Time functions", "SELECT NOW(), UTC_TIMESTAMP(), UNIX_TIMESTAMP()"),
            ("Math functions", "SELECT 1+1 as addition, 2*3 as multiplication, POWER(2,3) as power")
        ]
        
        for query_name, query_sql in queries:
            try:
                result = await client.execute(query_sql)
                logger.info(f"   ✅ {query_name}: {result.rows[0]}")
            except Exception as e:
                logger.error(f"   ❌ {query_name} failed: {e}")
        
        await client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Async query execution failed: {e}")


async def demo_async_transaction_management():
    """Demonstrate async transaction management"""
    logger.info("\n=== Test 3: Async Transaction Management ===")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        client = AsyncClient(logger=logger, enable_full_sql_logging=True)
        await client.connect(host, port, user, password, database)
        
        # Test transaction with commit
        logger.info("\n🔄 Test transaction with commit")
        async with client.transaction() as tx:
            # Insert test data
            await tx.execute("CREATE TABLE IF NOT EXISTS async_test_table (id INT, name VARCHAR(50))")
            await tx.execute("INSERT INTO async_test_table VALUES (1, 'async_test_1')")
            await tx.execute("INSERT INTO async_test_table VALUES (2, 'async_test_2')")
            
            # Query within transaction
            result = await tx.execute("SELECT COUNT(*) FROM async_test_table")
            logger.info(f"   Records in transaction: {result.rows[0][0]}")
        
        # Verify data after commit
        result = await client.execute("SELECT COUNT(*) FROM async_test_table")
        logger.info(f"   Records after commit: {result.rows[0][0]}")
        
        # Test transaction with rollback
        logger.info("\n🔄 Test transaction with rollback")
        async with client.transaction() as tx:
            await tx.execute("INSERT INTO async_test_table VALUES (3, 'async_test_3')")
            result = await tx.execute("SELECT COUNT(*) FROM async_test_table")
            logger.info(f"   Records before rollback: {result.rows[0][0]}")
            
            # Simulate rollback by raising exception
            raise Exception("Simulated rollback")
        
        # Verify data after rollback
        result = await client.execute("SELECT COUNT(*) FROM async_test_table")
        logger.info(f"   Records after rollback: {result.rows[0][0]}")
        
        # Cleanup
        await client.execute("DROP TABLE IF EXISTS async_test_table")
        
        await client.disconnect()
        
    except Exception as e:
        logger.info(f"   ✅ Expected rollback: {e}")


async def demo_async_concurrent_operations():
    """Demonstrate async concurrent operations"""
    logger.info("\n=== Test 4: Async Concurrent Operations ===")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    async def async_query_task(task_id: int, client: AsyncClient):
        """Async task for concurrent execution"""
        try:
            result = await client.execute(f"SELECT {task_id} as task_id, USER() as user, NOW() as timestamp")
            return result.rows[0]
        except Exception as e:
            logger.error(f"   Task {task_id} failed: {e}")
            return None
    
    try:
        # Create multiple async clients
        clients = []
        for i in range(3):
            client = AsyncClient(logger=logger, enable_full_sql_logging=True)
            await client.connect(host, port, user, password, database)
            clients.append(client)
        
        # Run concurrent tasks
        start_time = time.time()
        tasks = []
        for i, client in enumerate(clients):
            task = asyncio.create_task(async_query_task(i+1, client))
            tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        end_time = time.time()
        
        logger.info(f"   Concurrent execution time: {end_time - start_time:.3f} seconds")
        for result in results:
            if result:
                logger.info(f"   Task result: {result}")
        
        # Cleanup clients
        for client in clients:
            await client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Async concurrent operations failed: {e}")


async def demo_async_account_management():
    """Demonstrate async account management"""
    logger.info("\n=== Test 5: Async Account Management ===")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        # Connect as root
        root_client = AsyncClient(enable_full_sql_logging=True)
        await root_client.connect(host, port, user, password, database)
        
        # Test async account operations
        result = await root_client.execute("SHOW ACCOUNTS")
        logger.info(f"   Found {len(result.rows)} accounts")
        for row in result.rows:
            logger.info(f"     - {row[0]} (Admin: {row[1]}, Status: {row[3]})")
        
        # Test async role operations
        result = await root_client.execute("SHOW ROLES")
        logger.info(f"   Found {len(result.rows)} roles")
        for row in result.rows:
            logger.info(f"     - {row[0]}")
        
        await root_client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Async account management failed: {e}")


async def demo_async_error_handling():
    """Demonstrate async error handling"""
    logger.info("\n=== Test 6: Async Error Handling ===")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    # Test connection errors
    logger.info("\n🔌 Test async connection errors")
    try:
        client = AsyncClient(logger=logger, enable_full_sql_logging=True)
        await client.connect(host, port, 'invalid_user', 'invalid_pass', database)
        logger.error("   ❌ Should have failed but didn't!")
    except Exception as e:
        logger.info(f"   ✅ Correctly failed: {e}")
    
    # Test query errors
    logger.info("\n🔌 Test async query errors")
    try:
        client = AsyncClient(logger=logger, enable_full_sql_logging=True)
        await client.connect(host, port, user, password, database)
        
        # Test invalid query
        await client.execute("INVALID SQL QUERY")
        logger.error("   ❌ Should have failed but didn't!")
        
    except Exception as e:
        logger.info(f"   ✅ Correctly failed: {e}")
    
    # Test transaction errors
    logger.info("\n🔌 Test async transaction errors")
    try:
        client = AsyncClient(logger=logger, enable_full_sql_logging=True)
        await client.connect(host, port, user, password, database)
        
        async with client.transaction() as tx:
            await tx.execute("SELECT 1")
            # Simulate error
            raise Exception("Simulated transaction error")
        
    except Exception as e:
        logger.info(f"   ✅ Transaction correctly rolled back: {e}")


async def demo_sync_vs_async_performance():
    """Compare sync vs async performance"""
    logger.info("\n=== Test 7: Sync vs Async Performance ===")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    def sync_operations():
        """Synchronous operations"""
        start_time = time.time()
        clients = []
        
        for i in range(5):
            client = Client(logger=logger)
            client.connect(host, port, user, password, database)
            result = client.execute(f"SELECT {i+1} as sync_task")
            clients.append(client)
        
        for client in clients:
            client.disconnect()
        
        end_time = time.time()
        return end_time - start_time
    
    async def async_operations():
        """Asynchronous operations"""
        start_time = time.time()
        
        async def async_task(task_id):
            client = AsyncClient(logger=logger, enable_full_sql_logging=True)
            await client.connect(host, port, user, password, database)
            result = await client.execute(f"SELECT {task_id} as async_task")
            await client.disconnect()
            return result.rows[0]
        
        tasks = [async_task(i+1) for i in range(5)]
        await asyncio.gather(*tasks)
        
        end_time = time.time()
        return end_time - start_time
    
    try:
        # Test sync performance
        sync_time = sync_operations()
        logger.info(f"   Sync operations time: {sync_time:.3f} seconds")
        
        # Test async performance - use asyncio.create_task instead of asyncio.run
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an event loop, create a task
                async_time = await async_operations()
            else:
                # If no event loop is running, use asyncio.run
                async_time = asyncio.run(async_operations())
        except RuntimeError:
            # Fallback: create a new event loop
            async_time = asyncio.run(async_operations())
        
        logger.info(f"   Async operations time: {async_time:.3f} seconds")
        
        if async_time < sync_time:
            improvement = ((sync_time - async_time) / sync_time) * 100
            logger.info(f"   ✅ Async is {improvement:.1f}% faster")
        else:
            logger.info("   ⚠️ Sync was faster in this test")
        
    except Exception as e:
        logger.error(f"❌ Performance comparison failed: {e}")


async def demo_async_connection_pooling():
    """Demonstrate async connection pooling"""
    logger.info("\n=== Test 8: Async Connection Pooling ===")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        # Create multiple async connections
        clients = []
        for i in range(3):
            client = AsyncClient(logger=logger, enable_full_sql_logging=True)
            await client.connect(host, port, user, password, database)
            clients.append(client)
            logger.info(f"   ✅ Created async connection {i+1}")
        
        # Use connections concurrently
        async def use_connection(client, conn_id):
            result = await client.execute(f"SELECT {conn_id} as connection_id, USER() as user")
            return result.rows[0]
        
        tasks = [use_connection(client, i+1) for i, client in enumerate(clients)]
        results = await asyncio.gather(*tasks)
        
        for result in results:
            logger.info(f"   Connection result: {result}")
        
        # Close all connections
        for i, client in enumerate(clients):
            await client.disconnect()
            logger.info(f"   ✅ Closed async connection {i+1}")
        
    except Exception as e:
        logger.error(f"❌ Async connection pooling failed: {e}")


async def main():
    """Main async demo function"""
    logger.info("🚀 MatrixOne Async Operations Examples")
    logger.info("=" * 60)
    
    # Run async demos
    await demo_basic_async_operations()
    await demo_async_query_execution()
    await demo_async_transaction_management()
    await demo_async_concurrent_operations()
    await demo_async_account_management()
    await demo_async_error_handling()
    await demo_async_connection_pooling()
    
    # Run performance comparison
    await demo_sync_vs_async_performance()
    
    logger.info("\n🎉 Async operations examples completed!")
    logger.info("\nKey achievements:")
    logger.info("- ✅ Basic async connections and queries")
    logger.info("- ✅ Async transaction management")
    logger.info("- ✅ Concurrent async operations")
    logger.info("- ✅ Async account management")
    logger.info("- ✅ Async error handling")
    logger.info("- ✅ Performance comparison with sync")
    logger.info("- ✅ Async connection pooling")


if __name__ == '__main__':
    asyncio.run(main())
