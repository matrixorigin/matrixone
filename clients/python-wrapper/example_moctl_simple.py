"""
MatrixOne Python SDK - mo_ctl Core Operations Example
Demonstrates force flush table and force checkpoint operations
"""

from matrixone import Client
from matrixone.exceptions import MoCtlError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def moctl_core_examples():
    """Core mo_ctl operations examples"""
    
    # Create MatrixOne client
    client = Client()
    
    try:
        # Connect to MatrixOne as sys tenant
        client.connect(
            host="localhost",
            port=6001,
            user="root",  # sys tenant
            password="111",
            database="test"
        )
        
        logger.info("Connected to MatrixOne as sys tenant!")
        
        # Example 1: Force Flush Table
        logger.info("\n=== Example 1: Force Flush Table ===")
        
        try:
            # Create a test table first
            client.execute("""
                CREATE TABLE IF NOT EXISTS test_flush (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert some test data
            client.execute("INSERT INTO test_flush (name) VALUES ('test1'), ('test2'), ('test3')")
            logger.info("Created test table and inserted data")
            
            # Force flush the table
            result = client.moctl.flush_table('test', 'test_flush')
            logger.info(f"Flush table result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Flush table failed: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
        
        # Example 2: Force Checkpoint
        logger.info("\n=== Example 2: Force Checkpoint ===")
        
        try:
            result = client.moctl.checkpoint()
            logger.info(f"Checkpoint result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Checkpoint failed: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
        
        # Example 3: Integration with other operations
        logger.info("\n=== Example 3: Integration Example ===")
        
        try:
            # Create snapshot
            snapshot = client.snapshots.create(
                name="moctl_test_snapshot",
                level="database",
                database="test",
                description="Snapshot for mo_ctl integration test"
            )
            logger.info(f"Created snapshot: {snapshot.name}")
            
            # Force flush table
            result = client.moctl.flush_table('test', 'test_flush')
            logger.info(f"Flushed table: {result}")
            
            # Force checkpoint
            result = client.moctl.checkpoint()
            logger.info(f"Checkpoint completed: {result}")
            
            # Clone database using snapshot
            client.clone.clone_database_with_snapshot(
                "test_backup",
                "test",
                "moctl_test_snapshot"
            )
            logger.info("Cloned database using snapshot")
            
            # Verify backup
            backup_count = client.execute("SELECT COUNT(*) FROM test_backup.test_flush").scalar()
            original_count = client.execute("SELECT COUNT(*) FROM test_flush").scalar()
            
            logger.info(f"Original data count: {original_count}")
            logger.info(f"Backup data count: {backup_count}")
            
            if backup_count == original_count:
                logger.info("✓ Data verification successful")
            else:
                logger.error("✗ Data verification failed")
            
        except Exception as e:
            logger.error(f"Integration example failed: {e}")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        try:
            client.execute("DROP DATABASE IF EXISTS test_backup")
            client.snapshots.delete("moctl_test_snapshot")
            client.execute("DROP TABLE IF EXISTS test_flush")
            logger.info("Cleanup completed")
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        client.disconnect()
        logger.info("Disconnected from MatrixOne")


if __name__ == "__main__":
    print("MatrixOne mo_ctl Core Operations Examples")
    print("="*50)
    
    try:
        moctl_core_examples()
        
        print("\n" + "="*50)
        print("All examples completed successfully!")
        print("\nKey takeaways:")
        print("- mo_ctl operations require sys tenant privileges")
        print("- Force flush table ensures data persistence")
        print("- Force checkpoint helps with WAL management")
        print("- mo_ctl integrates well with snapshots and clones")
        
    except Exception as e:
        print(f"Examples failed: {e}")
        raise
