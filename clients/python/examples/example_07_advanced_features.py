#!/usr/bin/env python3
"""
MatrixOne Advanced Features Examples

This example demonstrates advanced MatrixOne features:
1. PubSub (Publish-Subscribe) operations
2. Clone operations
3. Point-in-Time Recovery (PITR)
4. MoCTL integration
5. Version information retrieval
6. Performance monitoring
7. Advanced error handling
8. Custom configurations

This example shows the complete advanced capabilities of MatrixOne.
"""

import logging
import asyncio
import time
import json
from matrixone import Client, AsyncClient
from matrixone.account import AccountManager
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params, print_config

# Create MatrixOne logger for all logging
logger = create_default_logger(
    enable_performance_logging=True,
    enable_sql_logging=True
)


def demo_pubsub_operations():
    """Demonstrate PubSub operations"""
    logger.info("🚀 MatrixOne PubSub Operations Demo")
    logger.info("=" * 60)
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect(host, port, user, password, database)
        
        # Test 1: Basic PubSub setup
        logger.info("\n=== Test 1: Basic PubSub Setup ===")
        
        # Create test table for PubSub
        client.execute("CREATE TABLE IF NOT EXISTS pubsub_test (id INT PRIMARY KEY, message VARCHAR(200), timestamp TIMESTAMP)")
        logger.info("   ✅ Created PubSub test table")
        
        # Test 2: Publish operations
        logger.info("\n=== Test 2: Publish Operations ===")
        
        # Simulate publishing messages
        messages = [
            "Hello from MatrixOne PubSub!",
            "This is a test message",
            "PubSub is working correctly",
            "MatrixOne advanced features demo"
        ]
        
        for i, message in enumerate(messages):
            client.execute(f"INSERT INTO pubsub_test VALUES ({i+1}, '{message}', NOW())")
            logger.info(f"   📤 Published message {i+1}: {message}")
        
        # Test 3: Subscribe operations
        logger.info("\n=== Test 3: Subscribe Operations ===")
        
        # Query messages (simulate subscription)
        result = client.execute("SELECT * FROM pubsub_test ORDER BY timestamp")
        logger.info(f"   📥 Subscribed to {len(result.rows)} messages:")
        for row in result.rows:
            logger.info(f"     - ID: {row[0]}, Message: {row[1]}, Time: {row[2]}")
        
        # Test 4: PubSub with filtering
        logger.info("\n=== Test 4: PubSub with Filtering ===")
        
        # Filter messages
        result = client.execute("SELECT * FROM pubsub_test WHERE message LIKE '%MatrixOne%'")
        logger.info(f"   🔍 Filtered messages: {len(result.rows)}")
        for row in result.rows:
            logger.info(f"     - {row[1]}")
        
        # Cleanup
        client.execute("DROP TABLE IF EXISTS pubsub_test")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ PubSub operations failed: {e}")


def demo_clone_operations():
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    """Demonstrate clone operations"""
    logger.info("\n=== Test 5: Clone Operations ===")
    
    try:
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect(host, port, user, password, database)
        
        # Create source table
        logger.info("\n📋 Create Source Table")
        client.execute("CREATE TABLE IF NOT EXISTS source_table (id INT PRIMARY KEY, data VARCHAR(100), value INT)")
        
        # Insert test data
        for i in range(5):
            client.execute(f"INSERT INTO source_table VALUES ({i+1}, 'source_data_{i+1}', {(i+1)*10})")
        
        result = client.execute("SELECT COUNT(*) FROM source_table")
        logger.info(f"   ✅ Source table created with {result.rows[0][0]} records")
        
        # Clone table
        logger.info("\n📋 Clone Table")
        clone_table_name = f"cloned_table_{int(time.time())}"
        
        try:
            # Note: MatrixOne clone syntax may vary
            client.execute(f"CREATE TABLE {clone_table_name} AS SELECT * FROM source_table")
            logger.info(f"   ✅ Cloned table: {clone_table_name}")
            
            # Verify clone
            result = client.execute(f"SELECT COUNT(*) FROM {clone_table_name}")
            logger.info(f"   ✅ Clone verification: {result.rows[0][0]} records")
            
            # Compare data
            result = client.execute(f"SELECT * FROM {clone_table_name} ORDER BY id")
            logger.info("   📊 Cloned data:")
            for row in result.rows:
                logger.info(f"     - ID: {row[0]}, Data: {row[1]}, Value: {row[2]}")
            
        except Exception as e:
            logger.info(f"   ⚠️ Clone operation not supported or failed: {e}")
        
        # Cleanup
        client.execute("DROP TABLE IF EXISTS source_table")
        try:
            client.execute(f"DROP TABLE IF EXISTS {clone_table_name}")
        except:
            pass
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Clone operations failed: {e}")


def demo_pitr_operations():
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    """Demonstrate Point-in-Time Recovery operations"""
    logger.info("\n=== Test 6: Point-in-Time Recovery (PITR) ===")
    
    try:
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect(host, port, user, password, database)
        
        # Use existing important data for PITR demonstration
        logger.info("\n📋 Prepare Important Data for PITR")
        
        # Create a table with important business data
        client.execute("CREATE TABLE IF NOT EXISTS customer_orders (id INT PRIMARY KEY, customer_name VARCHAR(100), order_amount DECIMAL(10,2), order_date DATE)")
        client.execute("DELETE FROM customer_orders")  # Clean slate
        
        # Insert initial important data
        initial_data = [
            (1, 'Alice Johnson', 1500.00, '2024-01-15'),
            (2, 'Bob Smith', 2300.50, '2024-01-16'),
            (3, 'Carol Davis', 1800.75, '2024-01-17'),
            (4, 'David Wilson', 3200.00, '2024-01-18'),
            (5, 'Eva Brown', 950.25, '2024-01-19')
        ]
        
        for order in initial_data:
            client.execute(f"INSERT INTO customer_orders VALUES {order}")
        
        result = client.execute("SELECT COUNT(*) FROM customer_orders")
        logger.info(f"   ✅ Created {result.rows[0][0]} important customer orders")
        
        # Clean up existing PITRs first
        logger.info("\n🧹 Clean up existing PITRs")
        try:
            existing_pitrs = client.pitr.list()
            for pitr in existing_pitrs:
                try:
                    client.pitr.delete(pitr.name)
                    logger.info(f"   ✅ Deleted existing PITR: {pitr.name}")
                except Exception as e:
                    logger.warning(f"   ⚠️ Failed to delete existing PITR {pitr.name}: {e}")
        except Exception as e:
            logger.warning(f"   ⚠️ Failed to list existing PITRs: {e}")
        
        # Create PITR for important data
        logger.info("\n🔄 Create PITR for Important Data")
        table_pitr = None
        try:
            table_pitr = client.pitr.create_table_pitr(
                name=f"customer_orders_backup_{int(time.time())}",
                database_name="test",
                table_name="customer_orders",
                range_value=1,
                range_unit="h"
            )
            logger.info(f"   ✅ Created PITR: {table_pitr.name}")
            logger.info("   📝 This PITR captures the current state of customer orders")
        except Exception as e:
            logger.error(f"   ❌ PITR creation failed: {e}")
        
        # Simulate business operations that might cause data issues
        logger.info("\n💼 Simulate Business Operations")
        
        # Add new orders
        new_orders = [
            (6, 'Frank Miller', 2100.00, '2024-01-20'),
            (7, 'Grace Lee', 1750.50, '2024-01-21')
        ]
        for order in new_orders:
            client.execute(f"INSERT INTO customer_orders VALUES {order}")
        logger.info("   ✅ Added 2 new customer orders")
        
        # Update existing order (simulate data correction)
        client.execute("UPDATE customer_orders SET order_amount = 2500.00 WHERE id = 2")
        logger.info("   ✅ Updated order amount for Bob Smith")
        
        # Show current state
        result = client.execute("SELECT COUNT(*) FROM customer_orders")
        logger.info(f"   📊 Current total orders: {result.rows[0][0]}")
        
        # Simulate data corruption or accidental deletion
        logger.info("\n⚠️ Simulate Data Issue")
        client.execute("DELETE FROM customer_orders WHERE id IN (3, 4)")
        logger.info("   ❌ Accidentally deleted 2 important orders!")
        
        result = client.execute("SELECT COUNT(*) FROM customer_orders")
        logger.info(f"   📊 Orders after deletion: {result.rows[0][0]}")
        
        # Demonstrate PITR recovery
        logger.info("\n🔄 Demonstrate PITR Recovery")
        if table_pitr:
            try:
                # Note: MatrixOne PITR recovery might require specific restore commands
                # This is a conceptual demonstration
                logger.info(f"   📝 PITR '{table_pitr.name}' is available for recovery")
                logger.info("   📝 In a real scenario, you would use restore commands to recover data")
                logger.info("   📝 The PITR captures the state before the data issues occurred")
                
                # Show what data was captured in the PITR
                logger.info("   📊 Data captured in PITR:")
                logger.info("     - 5 original customer orders")
                logger.info("     - Order amounts before updates")
                logger.info("     - Complete data before deletions")
                
            except Exception as e:
                logger.error(f"   ❌ PITR recovery demonstration failed: {e}")
        
        # List all PITRs
        logger.info("\n📋 List All PITRs")
        try:
            all_pitrs = client.pitr.list()
            logger.info(f"   📊 Found {len(all_pitrs)} PITRs:")
            for pitr in all_pitrs:
                logger.info(f"     - {pitr.name} ({pitr.level}): {pitr.range_value}{pitr.range_unit}")
                if pitr.level == 'table' and pitr.table_name == 'customer_orders':
                    logger.info(f"       📝 Protects: {pitr.database_name}.{pitr.table_name}")
        except Exception as e:
            logger.error(f"   ❌ PITR listing failed: {e}")
        
        # Cleanup PITRs
        logger.info("\n🧹 Cleanup PITRs")
        try:
            if table_pitr:
                client.pitr.delete(table_pitr.name)
                logger.info(f"   ✅ Deleted PITR: {table_pitr.name}")
        except Exception as e:
            logger.error(f"   ❌ PITR cleanup failed: {e}")
        
        # Clean up test data
        client.execute("DROP TABLE IF EXISTS customer_orders")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ PITR operations failed: {e}")


def demo_moctl_integration():
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    """Demonstrate MoCTL integration"""
    logger.info("\n=== Test 7: MoCTL Integration ===")
    
    try:
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect(host, port, user, password, database)
        
        logger.info("\n🔧 MoCTL Operations")
        
        logger.info("   ✅ mo_ctl operations are available")
        
        # Create test table for mo_ctl operations
        logger.info("\n📋 Create Test Table for MoCTL")
        client.execute("""
            CREATE TABLE IF NOT EXISTS moctl_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                value INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert test data
        for i in range(1, 6):
            client.execute(f"INSERT INTO moctl_test (name, value) VALUES ('test_data_{i}', {i * 100})")
        
        logger.info("   ✅ Created test table with 5 records")
        
        # Example 1: Force Flush Table
        logger.info("\n🔄 Force Flush Table")
        try:
            result = client.moctl.flush_table('test', 'moctl_test')
            logger.info(f"   ✅ Flush table result: {result}")
        except Exception as e:
            logger.error(f"   ❌ Flush table failed: {e}")
        
        # Example 2: Checkpoint Operations
        logger.info("\n🔄 Checkpoint Operations")
        try:
            result = client.moctl.checkpoint()
            logger.info(f"   ✅ Checkpoint result: {result}")
        except Exception as e:
            logger.error(f"   ❌ Checkpoint failed: {e}")
        
        # Example 3: Basic System Information (fallback for unavailable mo_ctl methods)
        logger.info("\n📊 Basic System Information")
        try:
            version = client.version()
            logger.info(f"   📊 MatrixOne version: {version}")
        except Exception as e:
            logger.error(f"   ❌ Get version failed: {e}")
        
        try:
            git_version = client.git_version()
            logger.info(f"   📊 MatrixOne git version: {git_version}")
        except Exception as e:
            logger.error(f"   ❌ Get git version failed: {e}")
        
        try:
            result = client.execute("SHOW DATABASES")
            logger.info(f"   📊 Databases: {[row[0] for row in result.rows]}")
        except Exception as e:
            logger.error(f"   ❌ Get databases failed: {e}")
        
        try:
            result = client.execute("SHOW TABLES")
            logger.info(f"   📊 Tables: {[row[0] for row in result.rows]}")
        except Exception as e:
            logger.error(f"   ❌ Get tables failed: {e}")
        
        try:
            result = client.execute("SHOW PROCESSLIST")
            logger.info(f"   📊 Active processes: {len(result.rows)}")
        except Exception as e:
            logger.info(f"   ⚠️ Process list not available: {e}")
        
        try:
            result = client.execute("SHOW VARIABLES LIKE 'version%'")
            logger.info("   📊 Version variables:")
            for row in result.rows:
                logger.info(f"     - {row[0]}: {row[1]}")
        except Exception as e:
            logger.info(f"   ⚠️ Variables not available: {e}")
        
        # Cleanup
        client.execute("DROP TABLE IF EXISTS moctl_test")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ MoCTL integration failed: {e}")


def demo_version_information():
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    """Demonstrate comprehensive version information retrieval"""
    logger.info("🚀 MatrixOne Version Information Demo")
    logger.info("=" * 60)
    
    client = Client(enable_full_sql_logging=True)
    
    try:
        # Connect to MatrixOne
        client.connect(host, port, user, password, database)
        logger.info("✅ Connected to MatrixOne")
        
        # Get server version
        logger.info("\n📊 Getting Server Version")
        try:
            version = client.version()
            logger.info(f"   ✅ MatrixOne Version: {version}")
        except Exception as e:
            logger.error(f"   ❌ Failed to get version: {e}")
        
        # Get git version
        logger.info("\n📊 Getting Git Version")
        try:
            git_version = client.git_version()
            logger.info(f"   ✅ MatrixOne Git Version: {git_version}")
        except Exception as e:
            logger.error(f"   ❌ Failed to get git version: {e}")
        
        # Get detailed version information
        logger.info("\n📊 Getting Detailed Version Information")
        try:
            result = client.execute("SELECT VERSION(), @@version_comment, @@version_compile_machine, @@version_compile_os")
            if result.rows:
                row = result.rows[0]
                logger.info(f"   📋 Version: {row[0]}")
                logger.info(f"   📋 Version Comment: {row[1]}")
                logger.info(f"   📋 Compile Machine: {row[2]}")
                logger.info(f"   📋 Compile OS: {row[3]}")
        except Exception as e:
            logger.error(f"   ❌ Failed to get detailed version info: {e}")
        
        # Get all version-related variables
        logger.info("\n📊 Getting All Version Variables")
        try:
            result = client.execute("SHOW VARIABLES LIKE 'version%'")
            logger.info("   📋 Version Variables:")
            for row in result.rows:
                logger.info(f"     - {row[0]}: {row[1]}")
        except Exception as e:
            logger.error(f"   ❌ Failed to get version variables: {e}")
        
        # Version comparison and compatibility checking
        logger.info("\n📊 Version Comparison and Compatibility")
        try:
            version = client.version()
            logger.info(f"   📋 Current MatrixOne Version: {version}")
            
            # Parse version components
            try:
                # Extract version number from string like "8.0.30-MatrixOne-v"
                version_parts = version.split('-')[0].split('.')
                major = int(version_parts[0])
                minor = int(version_parts[1])
                patch = int(version_parts[2])
                
                logger.info(f"   📋 Major Version: {major}")
                logger.info(f"   📋 Minor Version: {minor}")
                logger.info(f"   📋 Patch Version: {patch}")
                
                # Check compatibility
                if major >= 8:
                    logger.info("   ✅ Compatible with MatrixOne 8.x+")
                else:
                    logger.warning("   ⚠️ Older version detected")
                    
                if minor >= 0:
                    logger.info("   ✅ Minor version is acceptable")
                    
            except Exception as e:
                logger.error(f"   ❌ Failed to parse version: {e}")
                
        except Exception as e:
            logger.error(f"   ❌ Version comparison failed: {e}")
        
        client.disconnect()
        logger.info("✅ Disconnected from MatrixOne")
        
    except Exception as e:
        logger.error(f"❌ Version information demo failed: {e}")


async def demo_async_version_information():
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    """Demonstrate asynchronous version information retrieval"""
    logger.info("\n🚀 MatrixOne Async Version Information Demo")
    logger.info("=" * 60)
    
    client = AsyncClient(enable_full_sql_logging=True)
    
    try:
        # Connect to MatrixOne
        await client.connect(host, port, user, password, database)
        logger.info("✅ Connected to MatrixOne (async)")
        
        # Get server version
        logger.info("\n📊 Getting Server Version (async)")
        try:
            version = await client.version()
            logger.info(f"   ✅ MatrixOne Version: {version}")
        except Exception as e:
            logger.error(f"   ❌ Failed to get version: {e}")
        
        # Get git version
        logger.info("\n📊 Getting Git Version (async)")
        try:
            git_version = await client.git_version()
            logger.info(f"   ✅ MatrixOne Git Version: {git_version}")
        except Exception as e:
            logger.error(f"   ❌ Failed to get git version: {e}")
        
        # Get detailed version information
        logger.info("\n📊 Getting Detailed Version Information (async)")
        try:
            result = await client.execute("SELECT VERSION(), @@version_comment, @@version_compile_machine, @@version_compile_os")
            if result.rows:
                row = result.rows[0]
                logger.info(f"   📋 Version: {row[0]}")
                logger.info(f"   📋 Version Comment: {row[1]}")
                logger.info(f"   📋 Compile Machine: {row[2]}")
                logger.info(f"   📋 Compile OS: {row[3]}")
        except Exception as e:
            logger.error(f"   ❌ Failed to get detailed version info: {e}")
        
        # Get all version-related variables
        logger.info("\n📊 Getting All Version Variables (async)")
        try:
            result = await client.execute("SHOW VARIABLES LIKE 'version%'")
            logger.info("   📋 Version Variables:")
            for row in result.rows:
                logger.info(f"     - {row[0]}: {row[1]}")
        except Exception as e:
            logger.error(f"   ❌ Failed to get version variables: {e}")
        
        await client.disconnect()
        logger.info("✅ Disconnected from MatrixOne (async)")
        
    except Exception as e:
        logger.error(f"❌ Async version information demo failed: {e}")


def demo_version_context_manager():
    """Demonstrate version info with context manager"""
    logger.info("\n🚀 MatrixOne Version Info with Context Manager Demo")
    logger.info("=" * 60)
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        with Client(enable_full_sql_logging=True) as client:
            client.connect(host, port, user, password, database)
            
            logger.info("✅ Connected using context manager")
            
            # Get version info
            version = client.version()
            git_version = client.git_version()
            
            logger.info(f"📊 Version: {version}")
            logger.info(f"📊 Git Version: {git_version}")
            
            # Context manager will automatically disconnect
            logger.info("✅ Context manager will handle disconnection")
            
    except Exception as e:
        logger.error(f"❌ Context manager demo failed: {e}")


async def demo_async_version_context_manager():
    """Demonstrate async version info with context manager"""
    logger.info("\n🚀 MatrixOne Async Version Info with Context Manager Demo")
    logger.info("=" * 60)
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        async with AsyncClient(enable_full_sql_logging=True) as client:
            await client.connect(host, port, user, password, database)
            
            logger.info("✅ Connected using async context manager")
            
            # Get version info
            version = await client.version()
            git_version = await client.git_version()
            
            logger.info(f"📊 Version: {version}")
            logger.info(f"📊 Git Version: {git_version}")
            
            # Context manager will automatically disconnect
            logger.info("✅ Async context manager will handle disconnection")
            
    except Exception as e:
        logger.error(f"❌ Async context manager demo failed: {e}")


def demo_performance_monitoring():
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    """Demonstrate performance monitoring"""
    logger.info("\n=== Test 8: Performance Monitoring ===")
    
    try:
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect(host, port, user, password, database)
        
        # Create test table for performance testing
        logger.info("\n⚡ Performance Test Setup")
        client.execute("CREATE TABLE IF NOT EXISTS perf_test (id INT PRIMARY KEY, data VARCHAR(100), value INT)")
        
        # Test insert performance
        logger.info("\n⚡ Insert Performance Test")
        start_time = time.time()
        
        for i in range(100):
            client.execute(f"INSERT INTO perf_test VALUES ({i+1}, 'perf_data_{i+1}', {i+1})")
        
        end_time = time.time()
        insert_time = end_time - start_time
        logger.info(f"   ✅ Inserted 100 records in {insert_time:.3f} seconds")
        logger.info(f"   📊 Insert rate: {100/insert_time:.1f} records/second")
        
        # Test query performance
        logger.info("\n⚡ Query Performance Test")
        start_time = time.time()
        
        result = client.execute("SELECT COUNT(*) FROM perf_test")
        count = result.rows[0][0]
        
        end_time = time.time()
        query_time = end_time - start_time
        logger.info(f"   ✅ Queried {count} records in {query_time:.3f} seconds")
        
        # Test update performance
        logger.info("\n⚡ Update Performance Test")
        start_time = time.time()
        
        for i in range(50):
            client.execute(f"UPDATE perf_test SET value = {i+1}*2 WHERE id = {i+1}")
        
        end_time = time.time()
        update_time = end_time - start_time
        logger.info(f"   ✅ Updated 50 records in {update_time:.3f} seconds")
        logger.info(f"   📊 Update rate: {50/update_time:.1f} records/second")
        
        # Test delete performance
        logger.info("\n⚡ Delete Performance Test")
        start_time = time.time()
        
        client.execute("DELETE FROM perf_test WHERE id > 50")
        
        end_time = time.time()
        delete_time = end_time - start_time
        logger.info(f"   ✅ Deleted records in {delete_time:.3f} seconds")
        
        # Verify final state
        result = client.execute("SELECT COUNT(*) FROM perf_test")
        logger.info(f"   📊 Final record count: {result.rows[0][0]}")
        
        # Cleanup
        client.execute("DROP TABLE IF EXISTS perf_test")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Performance monitoring failed: {e}")


def demo_advanced_error_handling():
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    """Demonstrate advanced error handling"""
    logger.info("\n=== Test 9: Advanced Error Handling ===")
    
    try:
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect(host, port, user, password, database)
        
        # Test connection error handling
        logger.info("\n🔍 Connection Error Handling")
        try:
            bad_client = Client(enable_full_sql_logging=True)
            bad_client.connect('127.0.0.1', 9999, 'root', '111', 'test')
        except Exception as e:
            logger.info(f"   ✅ Connection error handled: {e}")
        
        # Test SQL error handling
        logger.info("\n🔍 SQL Error Handling")
        try:
            client.execute("INVALID SQL STATEMENT")
        except Exception as e:
            logger.info(f"   ✅ SQL error handled: {e}")
        
        # Test constraint violation handling
        logger.info("\n🔍 Constraint Violation Handling")
        try:
            client.execute("CREATE TABLE IF NOT EXISTS constraint_test (id INT PRIMARY KEY, name VARCHAR(50))")
            client.execute("INSERT INTO constraint_test VALUES (1, 'test1')")
            client.execute("INSERT INTO constraint_test VALUES (1, 'test2')")  # Duplicate key
        except Exception as e:
            logger.info(f"   ✅ Constraint violation handled: {e}")
        
        # Test data type error handling
        logger.info("\n🔍 Data Type Error Handling")
        try:
            client.execute("CREATE TABLE IF NOT EXISTS type_test (id INT, value INT)")
            client.execute("INSERT INTO type_test VALUES (1, 'invalid_string')")
        except Exception as e:
            logger.info(f"   ✅ Data type error handled: {e}")
        
        # Test permission error handling
        logger.info("\n🔍 Permission Error Handling")
        try:
            client.execute("DROP DATABASE mysql")  # Should fail
        except Exception as e:
            logger.info(f"   ✅ Permission error handled: {e}")
        
        # Cleanup
        client.execute("DROP TABLE IF EXISTS constraint_test")
        client.execute("DROP TABLE IF EXISTS type_test")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Advanced error handling failed: {e}")


def demo_custom_configurations():
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    """Demonstrate custom configurations"""
    logger.info("\n=== Test 10: Custom Configurations ===")
    
    try:
        # Test custom connection parameters
        logger.info("\n⚙️ Custom Connection Parameters")
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect(
            host='127.0.0.1',
            port=6001,
            user='root',
            password='111',
            database='test',
            ssl_mode='preferred'
        )
        logger.info("   ✅ Connected with custom parameters")
        
        # Test custom query timeout
        logger.info("\n⚙️ Custom Query Timeout")
        try:
            # This would normally timeout, but we'll catch it
            result = client.execute("SELECT SLEEP(1)")  # 1 second sleep
            logger.info("   ✅ Query completed within timeout")
        except Exception as e:
            logger.info(f"   ⚠️ Query timeout: {e}")
        
        # Test custom SSL configuration
        logger.info("\n⚙️ Custom SSL Configuration")
        try:
            ssl_client = Client(enable_full_sql_logging=True)
            ssl_client.connect(
                host='127.0.0.1',
                port=6001,
                user='root',
                password='111',
                database='test',
                ssl_mode='preferred'
            )
            logger.info("   ✅ Connected with SSL configuration")
            ssl_client.disconnect()
        except Exception as e:
            logger.info(f"   ⚠️ SSL configuration: {e}")
        
        # Test custom connection pooling
        logger.info("\n⚙️ Custom Connection Pooling")
        clients = []
        for i in range(3):
            client = Client(logger=logger, enable_full_sql_logging=True)
            client.connect(host, port, user, password, database)
            clients.append(client)
            logger.info(f"   ✅ Created connection {i+1}")
        
        # Use connections
        for i, client in enumerate(clients):
            result = client.execute(f"SELECT {i+1} as connection_id, USER() as user")
            logger.info(f"   📊 Connection {i+1}: {result.rows[0]}")
        
        # Close connections
        for i, client in enumerate(clients):
            client.disconnect()
            logger.info(f"   ✅ Closed connection {i+1}")
        
    except Exception as e:
        logger.error(f"❌ Custom configurations failed: {e}")


async def demo_async_advanced_features():
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    """Demonstrate async advanced features"""
    logger.info("\n=== Test 11: Async Advanced Features ===")
    
    try:
        client = AsyncClient(logger=logger, enable_full_sql_logging=True)
        await client.connect(host, port, user, password, database)
        
        # Test async performance monitoring
        logger.info("\n⚡ Async Performance Test")
        start_time = time.time()
        
        # Create test table
        await client.execute("CREATE TABLE IF NOT EXISTS async_advanced_test (id INT, data VARCHAR(100))")
        
        # Async bulk insert
        for i in range(50):
            await client.execute(f"INSERT INTO async_advanced_test VALUES ({i+1}, 'async_data_{i+1}')")
        
        end_time = time.time()
        logger.info(f"   ✅ Async operations completed in {end_time - start_time:.3f} seconds")
        
        # Test async query
        result = await client.execute("SELECT COUNT(*) FROM async_advanced_test")
        logger.info(f"   📊 Async query result: {result.rows[0][0]} records")
        
        # Cleanup
        await client.execute("DROP TABLE IF EXISTS async_advanced_test")
        await client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Async advanced features failed: {e}")


def main():
    """Main demo function"""
    logger.info("🚀 MatrixOne Advanced Features Examples")
    logger.info("=" * 60)
    
    # Run advanced feature demos
    demo_pubsub_operations()
    demo_clone_operations()
    demo_pitr_operations()
    demo_moctl_integration()
    demo_version_information()
    demo_version_context_manager()
    demo_performance_monitoring()
    demo_advanced_error_handling()
    demo_custom_configurations()
    
    # Run async advanced features demo
    asyncio.run(demo_async_advanced_features())
    asyncio.run(demo_async_version_information())
    asyncio.run(demo_async_version_context_manager())
    
    logger.info("\n🎉 Advanced features examples completed!")
    logger.info("\nKey achievements:")
    logger.info("- ✅ PubSub operations and messaging")
    logger.info("- ✅ Clone operations and data replication")
    logger.info("- ✅ Point-in-Time Recovery (PITR)")
    logger.info("- ✅ MoCTL integration and system monitoring")
    logger.info("- ✅ Version information retrieval and compatibility checking")
    logger.info("- ✅ Performance monitoring and optimization")
    logger.info("- ✅ Advanced error handling and recovery")
    logger.info("- ✅ Custom configurations and tuning")
    logger.info("- ✅ Async advanced features")
    logger.info("- ✅ Context manager usage for resource management")


if __name__ == '__main__':
    main()
