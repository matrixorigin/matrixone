#!/usr/bin/env python3
"""
MatrixOne Snapshot and Restore Examples

This example demonstrates comprehensive snapshot and restore operations:
1. Basic snapshot creation and management
2. Snapshot restoration
3. Point-in-time recovery (PITR)
4. Snapshot enumeration and information
5. Snapshot cleanup and management
6. Error handling for snapshot operations

This example shows the complete snapshot and restore capabilities of MatrixOne.
"""

import logging
import asyncio
import time
from matrixone import Client, AsyncClient
from matrixone.account import AccountManager
from matrixone.logger import create_default_logger

# Create MatrixOne logger for all logging
logger = create_default_logger(
    enable_performance_logging=True,
    enable_sql_logging=True
)


def demo_basic_snapshot_operations():
    """Demonstrate basic snapshot operations"""
    logger.info("🚀 MatrixOne Basic Snapshot Operations Demo")
    logger.info("=" * 60)
    
    try:
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Test 1: Create test data
        logger.info("\n=== Test 1: Create Test Data ===")
        client.execute("CREATE TABLE IF NOT EXISTS snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)")
        
        # Clear existing data to avoid duplicate key errors
        client.execute("DELETE FROM snapshot_test")
        client.execute("INSERT INTO snapshot_test VALUES (1, 'test1', 100)")
        client.execute("INSERT INTO snapshot_test VALUES (2, 'test2', 200)")
        
        result = client.execute("SELECT COUNT(*) FROM snapshot_test")
        logger.info(f"   Initial records: {result.rows[0][0]}")
        
        # Test 2: Create snapshot
        logger.info("\n=== Test 2: Create Snapshot ===")
        snapshot_name = f"testsnapshot{int(time.time())}"
        
        try:
            snapshot = client.snapshots.create(
                name=snapshot_name,
                level="table",
                database="test",
                table="snapshot_test",
                description="Test snapshot for basic operations"
            )
            logger.info(f"   ✅ Created snapshot: {snapshot.name}")
        except Exception as e:
            logger.info(f"   ⚠️ Snapshot creation failed: {e}")
            return
        
        # Test 3: Add more data after snapshot
        logger.info("\n=== Test 3: Add Data After Snapshot ===")
        client.execute("INSERT INTO snapshot_test VALUES (3, 'test3', 300)")
        client.execute("INSERT INTO snapshot_test VALUES (4, 'test4', 400)")
        
        result = client.execute("SELECT COUNT(*) FROM snapshot_test")
        logger.info(f"   Records after snapshot: {result.rows[0][0]}")
        
        # Test 4: List snapshots
        logger.info("\n=== Test 4: List Snapshots ===")
        try:
            snapshots = client.snapshots.list()
            logger.info(f"   Found {len(snapshots)} snapshots:")
            for snapshot in snapshots:
                logger.info(f"     - {snapshot.name} (Level: {snapshot.level}, Created: {snapshot.created_at})")
        except Exception as e:
            logger.error(f"   ❌ List snapshots failed: {e}")
        
        # Test 5: Restore from snapshot
        logger.info("\n=== Test 5: Restore from Snapshot ===")
        try:
            # Use the restore API (restore to same table name)
            client.restore.restore_table(snapshot_name, "sys", "test", "snapshot_test")
            logger.info(f"   ✅ Restored from snapshot: {snapshot_name}")
            
            # Verify data after restore
            result = client.execute("SELECT COUNT(*) FROM snapshot_test")
            logger.info(f"   Records after restore: {result.rows[0][0]}")
            
            # Verify specific data
            result = client.execute("SELECT * FROM snapshot_test ORDER BY id")
            logger.info("   Data after restore:")
            for row in result.rows:
                logger.info(f"     - ID: {row[0]}, Name: {row[1]}, Value: {row[2]}")
                
        except Exception as e:
            logger.error(f"   ❌ Snapshot restore failed: {e}")
        
        # Test 6: Cleanup
        logger.info("\n=== Test 6: Cleanup ===")
        try:
            client.snapshots.delete(snapshot_name)
            logger.info(f"   ✅ Dropped snapshot: {snapshot_name}")
        except Exception as e:
            logger.error(f"   ❌ Snapshot cleanup failed: {e}")
        
        client.execute("DROP TABLE IF EXISTS snapshot_test")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Basic snapshot operations failed: {e}")


def demo_snapshot_enumeration():
    """Demonstrate snapshot enumeration and information"""
    logger.info("\n=== Test 7: Snapshot Enumeration ===")
    
    try:
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Create test table for enumeration
        client.execute("CREATE TABLE IF NOT EXISTS snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)")
        client.execute("DELETE FROM snapshot_test")
        client.execute("INSERT INTO snapshot_test VALUES (1, 'enum_test1', 100)")
        client.execute("INSERT INTO snapshot_test VALUES (2, 'enum_test2', 200)")
        
        # Create multiple snapshots
        logger.info("\n📸 Create Multiple Snapshots")
        snapshot_names = []
        for i in range(3):
            snapshot_name = f"enumsnapshot{i}{int(time.time())}"
            try:
                snapshot = client.snapshots.create(
                    name=snapshot_name,
                    level="table",
                    database="test",
                    table="snapshot_test",
                    description=f"Enumeration test snapshot {i}"
                )
                snapshot_names.append(snapshot_name)
                logger.info(f"   ✅ Created snapshot: {snapshot_name}")
                time.sleep(1)  # Small delay between snapshots
            except Exception as e:
                logger.error(f"   ❌ Failed to create snapshot {snapshot_name}: {e}")
        
        # List all snapshots
        logger.info("\n📋 List All Snapshots")
        try:
            snapshots = client.snapshots.list()
            logger.info(f"   Found {len(snapshots)} snapshots:")
            for snapshot in snapshots:
                logger.info(f"     - {snapshot.name} (Level: {snapshot.level}, Created: {snapshot.created_at})")
        except Exception as e:
            logger.error(f"   ❌ List snapshots failed: {e}")
        
        # Get snapshot information
        logger.info("\n📋 Get Snapshot Information")
        for snapshot_name in snapshot_names:
            try:
                snapshot = client.snapshots.get(snapshot_name)
                logger.info(f"   Snapshot {snapshot_name} info:")
                logger.info(f"     - Name: {snapshot.name}")
                logger.info(f"     - Level: {snapshot.level}")
                logger.info(f"     - Database: {snapshot.database}")
                logger.info(f"     - Table: {snapshot.table}")
                logger.info(f"     - Created: {snapshot.created_at}")
            except Exception as e:
                logger.error(f"   ❌ Failed to get info for {snapshot_name}: {e}")
        
        # Cleanup snapshots
        logger.info("\n🧹 Cleanup Snapshots")
        for snapshot_name in snapshot_names:
            try:
                client.snapshots.delete(snapshot_name)
                logger.info(f"   ✅ Dropped snapshot: {snapshot_name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to drop snapshot {snapshot_name}: {e}")
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Snapshot enumeration failed: {e}")


def demo_point_in_time_recovery():
    """Demonstrate point-in-time recovery (PITR)"""
    logger.info("\n=== Test 8: Point-in-Time Recovery (PITR) ===")
    
    try:
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Create test data
        logger.info("\n📊 Create Test Data for PITR")
        client.execute("CREATE TABLE IF NOT EXISTS pitr_test (id INT PRIMARY KEY, name VARCHAR(50), timestamp TIMESTAMP)")
        
        # Clear existing data to avoid duplicate key errors
        client.execute("DELETE FROM pitr_test")
        client.execute("INSERT INTO pitr_test VALUES (1, 'initial_data', NOW())")
        
        # Create initial snapshot
        initial_snapshot = f"pitrinitial{int(time.time())}"
        snapshot1 = client.snapshots.create(
            name=initial_snapshot,
            level="table",
            database="test",
            table="pitr_test",
            description="Initial PITR snapshot"
        )
        logger.info(f"   ✅ Created initial snapshot: {initial_snapshot}")
        
        # Add more data over time
        logger.info("\n⏰ Add Data Over Time")
        for i in range(2, 6):
            client.execute(f"INSERT INTO pitr_test VALUES ({i}, 'data_{i}', NOW())")
            time.sleep(1)  # Simulate time passing
        
        # Create intermediate snapshot
        intermediate_snapshot = f"pitrintermediate{int(time.time())}"
        snapshot2 = client.snapshots.create(
            name=intermediate_snapshot,
            level="table",
            database="test",
            table="pitr_test",
            description="Intermediate PITR snapshot"
        )
        logger.info(f"   ✅ Created intermediate snapshot: {intermediate_snapshot}")
        
        # Add final data
        for i in range(6, 8):
            client.execute(f"INSERT INTO pitr_test VALUES ({i}, 'data_{i}', NOW())")
        
        # Show current state
        result = client.execute("SELECT COUNT(*) FROM pitr_test")
        logger.info(f"   Current records: {result.rows[0][0]}")
        
        # Restore to intermediate snapshot
        logger.info("\n🔄 Restore to Intermediate Snapshot")
        try:
            client.restore.restore_table(intermediate_snapshot, "sys", "test", "pitr_test")
            logger.info(f"   ✅ Restored to intermediate snapshot")
            
            result = client.execute("SELECT COUNT(*) FROM pitr_test")
            logger.info(f"   Records after restore: {result.rows[0][0]}")
            
        except Exception as e:
            logger.error(f"   ❌ PITR restore failed: {e}")
        
        # Restore to initial snapshot
        logger.info("\n🔄 Restore to Initial Snapshot")
        try:
            client.restore.restore_table(initial_snapshot, "sys", "test", "pitr_test")
            logger.info(f"   ✅ Restored to initial snapshot")
            
            result = client.execute("SELECT COUNT(*) FROM pitr_test")
            logger.info(f"   Records after restore: {result.rows[0][0]}")
            
        except Exception as e:
            logger.error(f"   ❌ PITR restore failed: {e}")
        
        # Cleanup
        logger.info("\n🧹 Cleanup PITR Test")
        try:
            client.snapshots.delete(initial_snapshot)
            client.snapshots.delete(intermediate_snapshot)
            client.execute("DROP TABLE IF EXISTS pitr_test")
            logger.info("   ✅ Cleaned up PITR test")
        except Exception as e:
            logger.error(f"   ❌ PITR cleanup failed: {e}")
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Point-in-time recovery failed: {e}")


def demo_snapshot_error_handling():
    """Demonstrate snapshot error handling"""
    logger.info("\n=== Test 9: Snapshot Error Handling ===")
    
    try:
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Test invalid snapshot name
        logger.info("\n🔍 Test Invalid Snapshot Name")
        try:
            client.snapshots.create(
                name="invalid-snapshot-name",
                level="table",
                database="test",
                table="snapshot_test"
            )
            logger.error("   ❌ Should have failed but didn't!")
        except Exception as e:
            logger.info(f"   ✅ Correctly failed: {e}")
        
        # Test duplicate snapshot name
        logger.info("\n🔍 Test Duplicate Snapshot Name")
        snapshot_name = f"duplicatetest{int(time.time())}"
        snapshot_created = False
        try:
            # Create test table first
            client.execute("CREATE TABLE IF NOT EXISTS snapshot_test (id INT PRIMARY KEY, name VARCHAR(50))")
            client.execute("DELETE FROM snapshot_test")
            client.execute("INSERT INTO snapshot_test VALUES (1, 'test')")
            
            snapshot = client.snapshots.create(
                name=snapshot_name,
                level="table",
                database="test",
                table="snapshot_test"
            )
            logger.info(f"   ✅ Created snapshot: {snapshot_name}")
            snapshot_created = True
            
            # Try to create duplicate
            client.snapshots.create(
                name=snapshot_name,
                level="table",
                database="test",
                table="snapshot_test"
            )
            logger.error("   ❌ Should have failed but didn't!")
        except Exception as e:
            logger.info(f"   ✅ Correctly failed: {e}")
        
        # Test restore from non-existent snapshot
        logger.info("\n🔍 Test Restore from Non-existent Snapshot")
        try:
            client.restore.restore_table("non_existent_snapshot", "sys", "test", "snapshot_test")
            logger.error("   ❌ Should have failed but didn't!")
        except Exception as e:
            logger.info(f"   ✅ Correctly failed: {e}")
        
        # Test drop non-existent snapshot
        logger.info("\n🔍 Test Drop Non-existent Snapshot")
        try:
            client.snapshots.delete("non_existent_snapshot")
            logger.error("   ❌ Should have failed but didn't!")
        except Exception as e:
            logger.info(f"   ✅ Correctly failed: {e}")
        
        # Cleanup - only if snapshot was actually created
        if snapshot_created:
            try:
                client.snapshots.delete(snapshot_name)
                logger.info(f"   ✅ Cleaned up snapshot: {snapshot_name}")
            except Exception as e:
                logger.error(f"   ❌ Cleanup failed: {e}")
        else:
            logger.info("   📝 No snapshot to cleanup (creation failed)")
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Snapshot error handling failed: {e}")


async def demo_async_snapshot_operations():
    """Demonstrate async snapshot operations"""
    logger.info("\n=== Test 10: Async Snapshot Operations ===")
    
    client = None
    try:
        client = AsyncClient(logger=logger, enable_full_sql_logging=True)
        await client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Create test data
        await client.execute("CREATE TABLE IF NOT EXISTS asyncsnapshottest (id INT, name VARCHAR(50))")
        await client.execute("INSERT INTO asyncsnapshottest VALUES (1, 'async_test_1')")
        
        # Create database-level snapshot (table-level has compatibility issues)
        snapshot_name = f"asyncsnapshot{int(time.time())}"
        snapshot = await client.snapshots.create(
            name=snapshot_name,
            level="database",
            database="test"
        )
        logger.info(f"   ✅ Created async database snapshot: {snapshot_name}")
        
        # Add more data
        await client.execute("INSERT INTO asyncsnapshottest VALUES (2, 'async_test_2')")
        
        # List snapshots (may fail due to mo_snapshots table not existing)
        try:
            snapshots = await client.snapshots.list()
            logger.info(f"   ✅ Async snapshot list: {len(snapshots)} snapshots found")
        except Exception as e:
            logger.info(f"   ⚠️ Async snapshot list failed (mo_snapshots table may not exist): {e}")
        
        # Get snapshot information
        try:
            snapshot_info = await client.snapshots.get(snapshot_name)
            logger.info(f"   ✅ Snapshot info: {snapshot_info.name} - {snapshot_info.level}")
        except Exception as e:
            logger.info(f"   ⚠️ Get snapshot info failed: {e}")
        
        # Test snapshot existence
        try:
            exists = await client.snapshots.exists(snapshot_name)
            logger.info(f"   ✅ Snapshot exists: {exists}")
        except Exception as e:
            logger.info(f"   ⚠️ Snapshot exists check failed: {e}")
        
        # Cleanup
        await client.snapshots.delete(snapshot_name)
        await client.execute("DROP TABLE IF EXISTS asyncsnapshottest")
        logger.info("   ✅ Async snapshot operations completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Async snapshot operations failed: {e}")
    finally:
        # Ensure proper cleanup
        if client:
            try:
                await client.disconnect()
            except Exception as e:
                logger.debug(f"Async disconnect warning: {e}")


def demo_snapshot_best_practices():
    """Demonstrate snapshot best practices"""
    logger.info("\n=== Test 11: Snapshot Best Practices ===")
    
    try:
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Best Practice 1: Use descriptive snapshot names
        logger.info("\n📋 Best Practice 1: Descriptive Snapshot Names")
        timestamp = int(time.time())
        snapshot_name = f"backupbeforemigration{timestamp}"
        
        # Create test table first
        client.execute("CREATE TABLE IF NOT EXISTS best_practice_test (id INT, data VARCHAR(100))")
        client.execute("DELETE FROM best_practice_test")
        client.execute("INSERT INTO best_practice_test VALUES (1, 'initial_data')")
        
        snapshot = client.snapshots.create(
            name=snapshot_name,
            level="table",
            database="test",
            table="best_practice_test",
            description="Backup before migration"
        )
        logger.info(f"   ✅ Created descriptive snapshot: {snapshot_name}")
        
        # Best Practice 2: Create snapshots before major operations
        logger.info("\n📋 Best Practice 2: Snapshots Before Major Operations")
        
        # Create snapshot before bulk insert
        bulk_snapshot = f"beforebulkinsert{timestamp}"
        bulk_snapshot_obj = client.snapshots.create(
            name=bulk_snapshot,
            level="table",
            database="test",
            table="best_practice_test",
            description="Before bulk insert operation"
        )
        logger.info(f"   ✅ Created snapshot before bulk operation: {bulk_snapshot}")
        
        # Perform bulk operation
        for i in range(10):
            client.execute(f"INSERT INTO best_practice_test VALUES ({i+1}, 'bulk_data_{i+1}')")
        
        # Best Practice 3: Regular snapshot cleanup
        logger.info("\n📋 Best Practice 3: Regular Snapshot Cleanup")
        snapshots = client.snapshots.list()
        logger.info(f"   Current snapshots: {len(snapshots)}")
        
        # Cleanup old snapshots (keep only recent ones)
        for snapshot in snapshots:
            if snapshot.name != snapshot_name and snapshot.name != bulk_snapshot:
                try:
                    client.snapshots.delete(snapshot.name)
                    logger.info(f"   ✅ Cleaned up old snapshot: {snapshot.name}")
                except Exception as e:
                    logger.error(f"   ❌ Failed to cleanup {snapshot.name}: {e}")
        
        # Best Practice 4: Test restore procedures
        logger.info("\n📋 Best Practice 4: Test Restore Procedures")
        try:
            client.restore.restore_table(bulk_snapshot, "sys", "test", "best_practice_test")
            logger.info("   ✅ Test restore successful")
            
            result = client.execute("SELECT COUNT(*) FROM best_practice_test")
            logger.info(f"   Records after test restore: {result.rows[0][0]}")
            
        except Exception as e:
            logger.error(f"   ❌ Test restore failed: {e}")
        
        # Cleanup
        client.snapshots.delete(snapshot_name)
        client.snapshots.delete(bulk_snapshot)
        client.execute("DROP TABLE IF EXISTS best_practice_test")
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Snapshot best practices failed: {e}")


def main():
    """Main demo function"""
    logger.info("🚀 MatrixOne Snapshot and Restore Examples")
    logger.info("=" * 60)
    
    # Run synchronous snapshot demos
    demo_basic_snapshot_operations()
    demo_snapshot_enumeration()
    demo_point_in_time_recovery()
    demo_snapshot_error_handling()
    demo_snapshot_best_practices()
    
    # Run async snapshot demo
    asyncio.run(demo_async_snapshot_operations())
    
    logger.info("\n🎉 Snapshot and restore examples completed!")
    logger.info("\nKey achievements:")
    logger.info("- ✅ Basic snapshot creation and restoration")
    logger.info("- ✅ Snapshot enumeration and information")
    logger.info("- ✅ Point-in-time recovery (PITR)")
    logger.info("- ✅ Snapshot error handling")
    logger.info("- ✅ Async snapshot operations")
    logger.info("- ✅ Snapshot best practices")
    logger.info("- ✅ Snapshot cleanup and management")


if __name__ == '__main__':
    main()
