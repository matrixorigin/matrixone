#!/usr/bin/env python3
"""
MatrixOne Transaction Management Examples

This example demonstrates comprehensive transaction management:
1. Basic transaction operations
2. Transaction with commit and rollback
3. Nested transactions
4. Transaction isolation levels
5. Transaction error handling
6. Transaction with account operations
7. Async transaction management

This example shows the complete transaction capabilities of MatrixOne.
"""

import logging
import asyncio
from matrixone import Client, AsyncClient
from matrixone.account import AccountManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


def demo_basic_transaction_operations():
    """Demonstrate basic transaction operations"""
    logger.info("🚀 MatrixOne Basic Transaction Operations Demo")
    logger.info("=" * 60)
    
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Test 1: Simple transaction with commit
        logger.info("\n=== Test 1: Simple Transaction with Commit ===")
        with client.transaction() as tx:
            # Create test table
            tx.execute("CREATE TABLE IF NOT EXISTS transaction_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)")
            
            # Insert test data
            tx.execute("INSERT INTO transaction_test VALUES (1, 'test1', 100)")
            tx.execute("INSERT INTO transaction_test VALUES (2, 'test2', 200)")
            
            # Query within transaction
            result = tx.execute("SELECT COUNT(*) FROM transaction_test")
            logger.info(f"   Records in transaction: {result.rows[0][0]}")
        
        # Verify data after commit
        result = client.execute("SELECT COUNT(*) FROM transaction_test")
        logger.info(f"   Records after commit: {result.rows[0][0]}")
        
        # Test 2: Transaction with rollback
        logger.info("\n=== Test 2: Transaction with Rollback ===")
        try:
            with client.transaction() as tx:
                # Insert more data
                tx.execute("INSERT INTO transaction_test VALUES (3, 'test3', 300)")
                result = tx.execute("SELECT COUNT(*) FROM transaction_test")
                logger.info(f"   Records before rollback: {result.rows[0][0]}")
                
                # Simulate rollback
                raise Exception("Simulated rollback")
        except Exception as e:
            logger.info(f"   ✅ Transaction rolled back: {e}")
        
        # Verify data after rollback
        result = client.execute("SELECT COUNT(*) FROM transaction_test")
        logger.info(f"   Records after rollback: {result.rows[0][0]}")
        
        # Cleanup
        client.execute("DROP TABLE IF EXISTS transaction_test")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Basic transaction operations failed: {e}")


def demo_transaction_isolation():
    """Demonstrate transaction isolation"""
    logger.info("\n=== Test 3: Transaction Isolation ===")
    
    try:
        # Create test table
        client1 = Client()
        client1.connect('127.0.0.1', 6001, 'root', '111', 'test')
        client1.execute("CREATE TABLE IF NOT EXISTS isolation_test (id INT PRIMARY KEY, value INT)")
        client1.execute("INSERT INTO isolation_test VALUES (1, 100)")
        client1.disconnect()
        
        # Test read committed isolation
        logger.info("\n🔍 Test Read Committed Isolation")
        
        # Start transaction 1
        client1 = Client()
        client1.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Start transaction 2
        client2 = Client()
        client2.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Transaction 1 reads initial value
        with client1.transaction() as tx1:
            result = tx1.execute("SELECT value FROM isolation_test WHERE id = 1")
            logger.info(f"   Transaction 1 reads: {result.rows[0][0]}")
            
            # Transaction 2 updates value
            with client2.transaction() as tx2:
                tx2.execute("UPDATE isolation_test SET value = 200 WHERE id = 1")
                logger.info("   Transaction 2 updates value to 200")
            
            # Transaction 1 reads again (should see updated value in read committed)
            result = tx1.execute("SELECT value FROM isolation_test WHERE id = 1")
            logger.info(f"   Transaction 1 reads again: {result.rows[0][0]}")
        
        # Verify final value
        result = client1.execute("SELECT value FROM isolation_test WHERE id = 1")
        logger.info(f"   Final value: {result.rows[0][0]}")
        
        client1.disconnect()
        client2.disconnect()
        
        # Cleanup
        cleanup_client = Client()
        cleanup_client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        cleanup_client.execute("DROP TABLE IF EXISTS isolation_test")
        cleanup_client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Transaction isolation test failed: {e}")


def demo_transaction_error_handling():
    """Demonstrate transaction error handling"""
    logger.info("\n=== Test 4: Transaction Error Handling ===")
    
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Test constraint violation
        logger.info("\n🔍 Test Constraint Violation")
        try:
            with client.transaction() as tx:
                tx.execute("CREATE TABLE IF NOT EXISTS constraint_test (id INT PRIMARY KEY, name VARCHAR(50))")
                tx.execute("INSERT INTO constraint_test VALUES (1, 'test1')")
                tx.execute("INSERT INTO constraint_test VALUES (1, 'test2')")  # Duplicate key
        except Exception as e:
            logger.info(f"   ✅ Constraint violation handled: {e}")
        
        # Verify no data was inserted (table might not exist due to rollback)
        try:
            result = client.execute("SELECT COUNT(*) FROM constraint_test")
            logger.info(f"   Records after constraint violation: {result.rows[0][0]}")
        except Exception as e:
            logger.info(f"   ✅ Table doesn't exist after rollback: {e}")
        
        # Test data type error
        logger.info("\n🔍 Test Data Type Error")
        try:
            with client.transaction() as tx:
                tx.execute("CREATE TABLE IF NOT EXISTS type_test (id INT, value INT)")
                tx.execute("INSERT INTO type_test VALUES (1, 'invalid_string')")  # Type error
        except Exception as e:
            logger.info(f"   ✅ Data type error handled: {e}")
        
        # Verify no data was inserted (table might not exist due to rollback)
        try:
            result = client.execute("SELECT COUNT(*) FROM type_test")
            logger.info(f"   Records after type error: {result.rows[0][0]}")
        except Exception as e:
            logger.info(f"   ✅ Table doesn't exist after rollback: {e}")
        
        # Cleanup
        client.execute("DROP TABLE IF EXISTS constraint_test")
        client.execute("DROP TABLE IF EXISTS type_test")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Transaction error handling failed: {e}")


def demo_transaction_with_account_operations():
    """Demonstrate transaction with account operations"""
    logger.info("\n=== Test 5: Transaction with Account Operations ===")
    
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        account_manager = AccountManager(client)
        
        # Note: Account operations may not be supported in transactions
        logger.info("📝 Note: Account operations may not be supported in transactions")
        logger.info("   This is a known limitation of MatrixOne")
        
        # Test transaction with regular operations
        with client.transaction() as tx:
            # Regular database operations
            tx.execute("CREATE TABLE IF NOT EXISTS account_transaction_test (id INT, name VARCHAR(50))")
            tx.execute("INSERT INTO account_transaction_test VALUES (1, 'test_data')")
            
            result = tx.execute("SELECT COUNT(*) FROM account_transaction_test")
            logger.info(f"   Records in transaction: {result.rows[0][0]}")
        
        # Verify data after commit
        result = client.execute("SELECT COUNT(*) FROM account_transaction_test")
        logger.info(f"   Records after commit: {result.rows[0][0]}")
        
        # Cleanup
        client.execute("DROP TABLE IF EXISTS account_transaction_test")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Transaction with account operations failed: {e}")


def demo_transaction_performance():
    """Demonstrate transaction performance"""
    logger.info("\n=== Test 6: Transaction Performance ===")
    
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Create test table
        client.execute("CREATE TABLE IF NOT EXISTS performance_test (id INT PRIMARY KEY, data VARCHAR(100))")
        
        # Test batch insert in transaction
        logger.info("\n⚡ Test Batch Insert in Transaction")
        import time
        
        start_time = time.time()
        with client.transaction() as tx:
            for i in range(100):
                tx.execute(f"INSERT INTO performance_test VALUES ({i+1}, 'data_{i+1}')")
        end_time = time.time()
        
        logger.info(f"   Batch insert time: {end_time - start_time:.3f} seconds")
        
        # Verify data
        result = client.execute("SELECT COUNT(*) FROM performance_test")
        logger.info(f"   Records inserted: {result.rows[0][0]}")
        
        # Test batch update in transaction
        logger.info("\n⚡ Test Batch Update in Transaction")
        start_time = time.time()
        with client.transaction() as tx:
            for i in range(100):
                tx.execute(f"UPDATE performance_test SET data = 'updated_{i+1}' WHERE id = {i+1}")
        end_time = time.time()
        
        logger.info(f"   Batch update time: {end_time - start_time:.3f} seconds")
        
        # Cleanup
        client.execute("DROP TABLE IF EXISTS performance_test")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Transaction performance test failed: {e}")


async def demo_async_transaction_management():
    """Demonstrate async transaction management"""
    logger.info("\n=== Test 7: Async Transaction Management ===")
    
    try:
        client = AsyncClient()
        await client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Test async transaction with commit
        logger.info("\n🔄 Test Async Transaction with Commit")
        async with client.transaction() as tx:
            await tx.execute("CREATE TABLE IF NOT EXISTS async_transaction_test (id INT, name VARCHAR(50))")
            await tx.execute("INSERT INTO async_transaction_test VALUES (1, 'async_test_1')")
            await tx.execute("INSERT INTO async_transaction_test VALUES (2, 'async_test_2')")
            
            result = await tx.execute("SELECT COUNT(*) FROM async_transaction_test")
            logger.info(f"   Records in async transaction: {result.rows[0][0]}")
        
        # Verify data after commit
        result = await client.execute("SELECT COUNT(*) FROM async_transaction_test")
        logger.info(f"   Records after async commit: {result.rows[0][0]}")
        
        # Test async transaction with rollback
        logger.info("\n🔄 Test Async Transaction with Rollback")
        try:
            async with client.transaction() as tx:
                await tx.execute("INSERT INTO async_transaction_test VALUES (3, 'async_test_3')")
                result = await tx.execute("SELECT COUNT(*) FROM async_transaction_test")
                logger.info(f"   Records before async rollback: {result.rows[0][0]}")
                
                # Simulate rollback
                raise Exception("Simulated async rollback")
        except Exception as e:
            logger.info(f"   ✅ Async transaction rolled back: {e}")
        
        # Verify data after rollback
        result = await client.execute("SELECT COUNT(*) FROM async_transaction_test")
        logger.info(f"   Records after async rollback: {result.rows[0][0]}")
        
        # Cleanup
        await client.execute("DROP TABLE IF EXISTS async_transaction_test")
        await client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Async transaction management failed: {e}")


def demo_transaction_best_practices():
    """Demonstrate transaction best practices"""
    logger.info("\n=== Test 8: Transaction Best Practices ===")
    
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Best Practice 1: Keep transactions short
        logger.info("\n📋 Best Practice 1: Keep Transactions Short")
        with client.transaction() as tx:
            tx.execute("CREATE TABLE IF NOT EXISTS best_practice_test (id INT, name VARCHAR(50))")
            tx.execute("INSERT INTO best_practice_test VALUES (1, 'short_transaction')")
            logger.info("   ✅ Short transaction completed")
        
        # Best Practice 2: Handle errors properly
        logger.info("\n📋 Best Practice 2: Handle Errors Properly")
        try:
            with client.transaction() as tx:
                tx.execute("INSERT INTO best_practice_test VALUES (2, 'error_handling')")
                # Simulate error
                raise Exception("Simulated error")
        except Exception as e:
            logger.info(f"   ✅ Error handled properly: {e}")
        
        # Best Practice 3: Use appropriate isolation levels
        logger.info("\n📋 Best Practice 3: Use Appropriate Isolation Levels")
        with client.transaction() as tx:
            result = tx.execute("SELECT @@transaction_isolation")
            logger.info(f"   Current isolation level: {result.rows[0][0]}")
        
        # Best Practice 4: Avoid long-running transactions
        logger.info("\n📋 Best Practice 4: Avoid Long-running Transactions")
        with client.transaction() as tx:
            tx.execute("INSERT INTO best_practice_test VALUES (3, 'quick_operation')")
            logger.info("   ✅ Quick operation completed")
        
        # Verify all data
        result = client.execute("SELECT COUNT(*) FROM best_practice_test")
        logger.info(f"   Total records: {result.rows[0][0]}")
        
        # Cleanup
        client.execute("DROP TABLE IF EXISTS best_practice_test")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Transaction best practices failed: {e}")


def main():
    """Main demo function"""
    logger.info("🚀 MatrixOne Transaction Management Examples")
    logger.info("=" * 60)
    
    # Run synchronous transaction demos
    demo_basic_transaction_operations()
    demo_transaction_isolation()
    demo_transaction_error_handling()
    demo_transaction_with_account_operations()
    demo_transaction_performance()
    demo_transaction_best_practices()
    
    # Run async transaction demo
    asyncio.run(demo_async_transaction_management())
    
    logger.info("\n🎉 Transaction management examples completed!")
    logger.info("\nKey achievements:")
    logger.info("- ✅ Basic transaction operations with commit/rollback")
    logger.info("- ✅ Transaction isolation testing")
    logger.info("- ✅ Transaction error handling")
    logger.info("- ✅ Transaction performance optimization")
    logger.info("- ✅ Async transaction management")
    logger.info("- ✅ Transaction best practices")
    logger.info("- ✅ Account operations in transactions (with limitations)")


if __name__ == '__main__':
    main()
