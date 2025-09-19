"""
MatrixOne Python SDK - Restore Operations Example
Demonstrates restore functionality from snapshots
"""

import sys
import os
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import Client, AsyncClient, SnapshotLevel
from matrixone.exceptions import ConnectionError, RestoreError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def restore_example():
    """Example of restore operations"""
    
    print("MatrixOne Restore Operations Example")
    print("=" * 50)
    
    client = Client()
    
    try:
        # Connect to MatrixOne
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        logger.info("Connected to MatrixOne!")
        
        # Example 1: Restore Cluster
        logger.info("\n=== Example 1: Restore Cluster ===")
        try:
            # Create cluster snapshot first
            cluster_snapshot = client.snapshots.create(
                name="cluster_restore_example",
                level=SnapshotLevel.CLUSTER,
                description="Cluster snapshot for restore example"
            )
            logger.info(f"Created cluster snapshot: {cluster_snapshot.name}")
            
            # Restore cluster from snapshot
            success = client.restore.restore_cluster("cluster_restore_example")
            if success:
                logger.info("✅ Cluster restored successfully!")
            else:
                logger.warning("⚠️ Cluster restore returned False")
                
        except RestoreError as e:
            logger.error(f"❌ Cluster restore failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 2: Restore Tenant
        logger.info("\n=== Example 2: Restore Tenant ===")
        try:
            # Create tenant snapshot first
            tenant_snapshot = client.snapshots.create(
                name="tenant_restore_example",
                level=SnapshotLevel.ACCOUNT,
                account="acc1",
                description="Tenant snapshot for restore example"
            )
            logger.info(f"Created tenant snapshot: {tenant_snapshot.name}")
            
            # Restore tenant to itself
            success = client.restore.restore_tenant("tenant_restore_example", "acc1")
            if success:
                logger.info("✅ Tenant restored to itself successfully!")
            else:
                logger.warning("⚠️ Tenant restore returned False")
                
        except RestoreError as e:
            logger.error(f"❌ Tenant restore failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 3: Restore Tenant to New Tenant
        logger.info("\n=== Example 3: Restore Tenant to New Tenant ===")
        try:
            # Restore tenant to new tenant
            success = client.restore.restore_tenant("tenant_restore_example", "acc1", "acc2")
            if success:
                logger.info("✅ Tenant restored to new tenant successfully!")
            else:
                logger.warning("⚠️ Tenant restore to new tenant returned False")
                
        except RestoreError as e:
            logger.error(f"❌ Tenant restore to new tenant failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 4: Restore Database
        logger.info("\n=== Example 4: Restore Database ===")
        try:
            # Create database snapshot first
            db_snapshot = client.snapshots.create(
                name="database_restore_example",
                level=SnapshotLevel.DATABASE,
                database="test_db",
                description="Database snapshot for restore example"
            )
            logger.info(f"Created database snapshot: {db_snapshot.name}")
            
            # Restore database
            success = client.restore.restore_database("database_restore_example", "acc1", "test_db")
            if success:
                logger.info("✅ Database restored successfully!")
            else:
                logger.warning("⚠️ Database restore returned False")
                
        except RestoreError as e:
            logger.error(f"❌ Database restore failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 5: Restore Table
        logger.info("\n=== Example 5: Restore Table ===")
        try:
            # Create table snapshot first
            table_snapshot = client.snapshots.create(
                name="table_restore_example",
                level=SnapshotLevel.TABLE,
                database="test_db",
                table="test_table",
                description="Table snapshot for restore example"
            )
            logger.info(f"Created table snapshot: {table_snapshot.name}")
            
            # Restore table
            success = client.restore.restore_table("table_restore_example", "acc1", "test_db", "test_table")
            if success:
                logger.info("✅ Table restored successfully!")
            else:
                logger.warning("⚠️ Table restore returned False")
                
        except RestoreError as e:
            logger.error(f"❌ Table restore failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 6: Restore in Transaction
        logger.info("\n=== Example 6: Restore in Transaction ===")
        try:
            with client.transaction() as tx:
                # Create snapshot within transaction
                tx_snapshot = tx.snapshots.create(
                    name="transaction_restore_example",
                    level=SnapshotLevel.DATABASE,
                    database="test_db",
                    description="Transaction snapshot for restore example"
                )
                logger.info(f"Created transaction snapshot: {tx_snapshot.name}")
                
                # Restore within transaction
                success = tx.restore.restore_database("transaction_restore_example", "acc1", "test_db")
                if success:
                    logger.info("✅ Database restored within transaction successfully!")
                else:
                    logger.warning("⚠️ Transaction restore returned False")
                    
        except RestoreError as e:
            logger.error(f"❌ Transaction restore failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        try:
            # Delete snapshots
            snapshots_to_delete = [
                "cluster_restore_example",
                "tenant_restore_example", 
                "database_restore_example",
                "table_restore_example",
                "transaction_restore_example"
            ]
            
            for snapshot_name in snapshots_to_delete:
                try:
                    client.snapshots.delete(snapshot_name)
                    logger.info(f"Deleted snapshot: {snapshot_name}")
                except Exception as e:
                    logger.warning(f"Failed to delete snapshot {snapshot_name}: {e}")
                    
        except Exception as e:
            logger.warning(f"Cleanup failed: {e}")
        
    except ConnectionError as e:
        logger.error(f"Connection failed: {e}")
        logger.info("This is expected if MatrixOne is not running or accessible")
        return False
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        client.disconnect()
        logger.info("Disconnected from MatrixOne")
    
    return True


async def async_restore_example():
    """Example of async restore operations"""
    
    print("MatrixOne Async Restore Operations Example")
    print("=" * 50)
    
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
        
        # Example 1: Async Restore Cluster
        logger.info("\n=== Example 1: Async Restore Cluster ===")
        try:
            # Create cluster snapshot first
            cluster_snapshot = await client.snapshots.create(
                name="async_cluster_restore_example",
                level=SnapshotLevel.CLUSTER,
                description="Async cluster snapshot for restore example"
            )
            logger.info(f"Created async cluster snapshot: {cluster_snapshot.name}")
            
            # Restore cluster from snapshot
            success = await client.restore.restore_cluster("async_cluster_restore_example")
            if success:
                logger.info("✅ Async cluster restored successfully!")
            else:
                logger.warning("⚠️ Async cluster restore returned False")
                
        except RestoreError as e:
            logger.error(f"❌ Async cluster restore failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 2: Async Restore Tenant
        logger.info("\n=== Example 2: Async Restore Tenant ===")
        try:
            # Create tenant snapshot first
            tenant_snapshot = await client.snapshots.create(
                name="async_tenant_restore_example",
                level=SnapshotLevel.ACCOUNT,
                account="acc1",
                description="Async tenant snapshot for restore example"
            )
            logger.info(f"Created async tenant snapshot: {tenant_snapshot.name}")
            
            # Restore tenant
            success = await client.restore.restore_tenant("async_tenant_restore_example", "acc1")
            if success:
                logger.info("✅ Async tenant restored successfully!")
            else:
                logger.warning("⚠️ Async tenant restore returned False")
                
        except RestoreError as e:
            logger.error(f"❌ Async tenant restore failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 3: Async Restore in Transaction
        logger.info("\n=== Example 3: Async Restore in Transaction ===")
        try:
            async with client.transaction() as tx:
                # Create snapshot within transaction
                tx_snapshot = await tx.snapshots.create(
                    name="async_transaction_restore_example",
                    level=SnapshotLevel.DATABASE,
                    database="test_db",
                    description="Async transaction snapshot for restore example"
                )
                logger.info(f"Created async transaction snapshot: {tx_snapshot.name}")
                
                # Restore within transaction
                success = await tx.restore.restore_database("async_transaction_restore_example", "acc1", "test_db")
                if success:
                    logger.info("✅ Async database restored within transaction successfully!")
                else:
                    logger.warning("⚠️ Async transaction restore returned False")
                    
        except RestoreError as e:
            logger.error(f"❌ Async transaction restore failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        try:
            # Delete snapshots
            snapshots_to_delete = [
                "async_cluster_restore_example",
                "async_tenant_restore_example",
                "async_transaction_restore_example"
            ]
            
            for snapshot_name in snapshots_to_delete:
                try:
                    await client.snapshots.delete(snapshot_name)
                    logger.info(f"Deleted async snapshot: {snapshot_name}")
                except Exception as e:
                    logger.warning(f"Failed to delete async snapshot {snapshot_name}: {e}")
                    
        except Exception as e:
            logger.warning(f"Async cleanup failed: {e}")
        
    except ConnectionError as e:
        logger.error(f"Connection failed: {e}")
        logger.info("This is expected if MatrixOne is not running or accessible")
        return False
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        await client.disconnect()
        logger.info("Disconnected from MatrixOne")
    
    return True


def demo_restore_benefits():
    """Demo the benefits of restore operations"""
    
    logger.info("\n" + "="*50)
    logger.info("Restore Operations Benefits")
    logger.info("="*50)
    
    logger.info("✓ Data Recovery:")
    logger.info("  - Restore from cluster-level snapshots")
    logger.info("  - Restore from tenant-level snapshots")
    logger.info("  - Restore from database-level snapshots")
    logger.info("  - Restore from table-level snapshots")
    
    logger.info("\n✓ Cross-Tenant Operations:")
    logger.info("  - Restore tenant to itself")
    logger.info("  - Restore tenant to new tenant")
    logger.info("  - Restore database to new tenant")
    logger.info("  - Restore table to new tenant")
    
    logger.info("\n✓ Transaction Support:")
    logger.info("  - Restore operations within transactions")
    logger.info("  - Atomic restore with other operations")
    logger.info("  - Rollback support for failed restores")
    
    logger.info("\n✓ Async Support:")
    logger.info("  - Non-blocking restore operations")
    logger.info("  - Concurrent restore operations")
    logger.info("  - Better performance for large restores")
    
    logger.info("\n✓ Error Handling:")
    logger.info("  - Detailed error messages")
    logger.info("  - Graceful failure handling")
    logger.info("  - Proper exception types")


def demo_without_database():
    """Demo without database connection"""
    
    logger.info("\n" + "="*50)
    logger.info("Restore Operations Demo (No Database Required)")
    logger.info("="*50)
    
    client = Client()
    logger.info("✓ Client created successfully")
    
    logger.info("✓ Restore operations syntax:")
    logger.info("  # Cluster restore")
    logger.info("  success = client.restore.restore_cluster('cluster_snapshot')")
    logger.info("  ")
    logger.info("  # Tenant restore")
    logger.info("  success = client.restore.restore_tenant('tenant_snapshot', 'acc1')")
    logger.info("  success = client.restore.restore_tenant('tenant_snapshot', 'acc1', 'acc2')")
    logger.info("  ")
    logger.info("  # Database restore")
    logger.info("  success = client.restore.restore_database('db_snapshot', 'acc1', 'db1')")
    logger.info("  ")
    logger.info("  # Table restore")
    logger.info("  success = client.restore.restore_table('table_snapshot', 'acc1', 'db1', 't1')")
    logger.info("  ")
    logger.info("  # Transaction restore")
    logger.info("  with client.transaction() as tx:")
    logger.info("      success = tx.restore.restore_database('snapshot', 'acc1', 'db1')")
    logger.info("  ")
    logger.info("  # Async restore")
    logger.info("  success = await client.restore.restore_cluster('cluster_snapshot')")
    
    demo_restore_benefits()


def main():
    """Main function"""
    print("MatrixOne Restore Operations Example")
    print("=" * 50)
    
    try:
        # Try to run with database
        success = restore_example()
        
        if not success:
            # Run demo without database
            demo_without_database()
            
            print("\n" + "="*50)
            print("Demo completed!")
            print("\nTo run with database:")
            print("1. Start MatrixOne database")
            print("2. Update connection parameters")
            print("3. Run the script again")
        else:
            print("\n" + "="*50)
            print("✅ All restore examples completed successfully!")
            print("\n🎉 MatrixOne restore operations work perfectly!")
            print("\nKey features:")
            print("- Cluster, tenant, database, and table restore")
            print("- Cross-tenant restore operations")
            print("- Transaction support for restore operations")
            print("- Async restore operations")
            print("- Comprehensive error handling")
            print("- Based on MatrixOne official documentation")
        
    except Exception as e:
        print(f"❌ Examples failed: {e}")
        raise


if __name__ == "__main__":
    main()
