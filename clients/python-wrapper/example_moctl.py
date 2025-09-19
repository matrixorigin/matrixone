"""
MatrixOne Python SDK - mo_ctl Operations Example
Demonstrates how to use MatrixOne control operations (mo_ctl)
"""

from matrixone import Client
from matrixone.exceptions import MoCtlError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def moctl_basic_examples():
    """Basic mo_ctl operations examples"""
    
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
        
        # Check if mo_ctl is available
        if not client.moctl.is_available():
            logger.error("mo_ctl operations are not available")
            return
        
        logger.info("mo_ctl operations are available")
        
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
        
        # Example 2: Force Flush Database
        logger.info("\n=== Example 2: Force Flush Database ===")
        
        try:
            result = client.moctl.flush_database('test')
            logger.info(f"Flush database result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Flush database failed: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
        
        # Example 3: Force Checkpoint
        logger.info("\n=== Example 3: Force Checkpoint ===")
        
        try:
            result = client.moctl.checkpoint()
            logger.info(f"Checkpoint result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Checkpoint failed: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
        
        # Example 4: Incremental Checkpoint
        logger.info("\n=== Example 4: Incremental Checkpoint ===")
        
        try:
            result = client.moctl.incremental_checkpoint()
            logger.info(f"Incremental checkpoint result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Incremental checkpoint failed: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
        
        # Example 5: Full Checkpoint
        logger.info("\n=== Example 5: Full Checkpoint ===")
        
        try:
            result = client.moctl.full_checkpoint()
            logger.info(f"Full checkpoint result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Full checkpoint failed: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        client.execute("DROP TABLE IF EXISTS test_flush")
        logger.info("Dropped test table")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        client.disconnect()
        logger.info("Disconnected from MatrixOne")


def moctl_log_operations():
    """Log control operations examples"""
    
    logger.info("\n" + "="*60)
    logger.info("Log Control Operations")
    logger.info("="*60)
    
    client = Client()
    
    try:
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        # Example 1: Set Log Level
        logger.info("\n--- Example 1: Set Log Level ---")
        
        try:
            result = client.moctl.log_level('debug')
            logger.info(f"Set log level result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Set log level failed: {e}")
        
        # Example 2: Enable Logging for Component
        logger.info("\n--- Example 2: Enable Logging ---")
        
        try:
            result = client.moctl.log_enable('sql')
            logger.info(f"Enable logging result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Enable logging failed: {e}")
        
        # Example 3: Disable Logging for Component
        logger.info("\n--- Example 3: Disable Logging ---")
        
        try:
            result = client.moctl.log_disable('sql')
            logger.info(f"Disable logging result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Disable logging failed: {e}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        client.disconnect()


def moctl_cluster_operations():
    """Cluster control operations examples"""
    
    logger.info("\n" + "="*60)
    logger.info("Cluster Control Operations")
    logger.info("="*60)
    
    client = Client()
    
    try:
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        # Example 1: Get Cluster Status
        logger.info("\n--- Example 1: Get Cluster Status ---")
        
        try:
            result = client.moctl.cluster_status()
            logger.info(f"Cluster status result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Get cluster status failed: {e}")
        
        # Example 2: Get Cluster Info
        logger.info("\n--- Example 2: Get Cluster Info ---")
        
        try:
            result = client.moctl.cluster_info()
            logger.info(f"Cluster info result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Get cluster info failed: {e}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        client.disconnect()


def moctl_node_operations():
    """Node control operations examples"""
    
    logger.info("\n" + "="*60)
    logger.info("Node Control Operations")
    logger.info("="*60)
    
    client = Client()
    
    try:
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        # Example 1: Get All Nodes Status
        logger.info("\n--- Example 1: Get All Nodes Status ---")
        
        try:
            result = client.moctl.node_status()
            logger.info(f"All nodes status result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Get all nodes status failed: {e}")
        
        # Example 2: Get Specific Node Status
        logger.info("\n--- Example 2: Get Specific Node Status ---")
        
        try:
            result = client.moctl.node_status('node1')
            logger.info(f"Node1 status result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Get node1 status failed: {e}")
        
        # Example 3: Get All Nodes Info
        logger.info("\n--- Example 3: Get All Nodes Info ---")
        
        try:
            result = client.moctl.node_info()
            logger.info(f"All nodes info result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Get all nodes info failed: {e}")
        
        # Example 4: Get Specific Node Info
        logger.info("\n--- Example 4: Get Specific Node Info ---")
        
        try:
            result = client.moctl.node_info('node1')
            logger.info(f"Node1 info result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Get node1 info failed: {e}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        client.disconnect()


def moctl_custom_operations():
    """Custom mo_ctl operations examples"""
    
    logger.info("\n" + "="*60)
    logger.info("Custom mo_ctl Operations")
    logger.info("="*60)
    
    client = Client()
    
    try:
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        # Example 1: Custom mo_ctl Command
        logger.info("\n--- Example 1: Custom mo_ctl Command ---")
        
        try:
            result = client.moctl.custom_ctl('custom', 'operation', 'param1,param2')
            logger.info(f"Custom mo_ctl result: {result}")
            
        except MoCtlError as e:
            logger.error(f"Custom mo_ctl failed: {e}")
        
        # Example 2: Get Supported Operations
        logger.info("\n--- Example 2: Get Supported Operations ---")
        
        operations = client.moctl.get_supported_operations()
        logger.info("Supported operations:")
        for category, ops in operations.items():
            logger.info(f"  {category}: {ops}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        client.disconnect()


def moctl_integration_example():
    """Integration example with other MatrixOne operations"""
    
    logger.info("\n" + "="*60)
    logger.info("mo_ctl Integration Example")
    logger.info("="*60)
    
    client = Client()
    
    try:
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        # Create test database and table
        client.execute("CREATE DATABASE IF NOT EXISTS test_moctl")
        client.execute("USE test_moctl")
        client.execute("""
            CREATE TABLE IF NOT EXISTS test_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert test data
        for i in range(1, 11):
            client.execute("INSERT INTO test_data (data) VALUES (%s)", (f"test_data_{i}",))
        
        logger.info("Created test database and inserted data")
        
        # Create snapshot
        snapshot = client.snapshots.create(
            name="moctl_test_snapshot",
            level="database",
            database="test_moctl",
            description="Snapshot for mo_ctl integration test"
        )
        logger.info(f"Created snapshot: {snapshot.name}")
        
        # Force flush the table
        result = client.moctl.flush_table('test_moctl', 'test_data')
        logger.info(f"Flushed table: {result}")
        
        # Force checkpoint
        result = client.moctl.checkpoint()
        logger.info(f"Checkpoint completed: {result}")
        
        # Clone database using snapshot
        client.clone.clone_database_with_snapshot(
            "test_moctl_backup",
            "test_moctl",
            "moctl_test_snapshot"
        )
        logger.info("Cloned database using snapshot")
        
        # Verify backup
        backup_count = client.execute("SELECT COUNT(*) FROM test_moctl_backup.test_data").scalar()
        original_count = client.execute("SELECT COUNT(*) FROM test_moctl.test_data").scalar()
        
        logger.info(f"Original data count: {original_count}")
        logger.info(f"Backup data count: {backup_count}")
        
        if backup_count == original_count:
            logger.info("✓ Data verification successful")
        else:
            logger.error("✗ Data verification failed")
        
        # Cleanup
        logger.info("\n--- Cleanup ---")
        client.execute("DROP DATABASE IF EXISTS test_moctl_backup")
        client.snapshots.delete("moctl_test_snapshot")
        client.execute("DROP DATABASE IF EXISTS test_moctl")
        logger.info("Cleanup completed")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        client.disconnect()


if __name__ == "__main__":
    print("MatrixOne mo_ctl Operations Examples")
    print("="*50)
    
    try:
        moctl_basic_examples()
        moctl_log_operations()
        moctl_cluster_operations()
        moctl_node_operations()
        moctl_custom_operations()
        moctl_integration_example()
        
        print("\n" + "="*50)
        print("All examples completed successfully!")
        print("\nKey takeaways:")
        print("- mo_ctl operations require sys tenant privileges")
        print("- Force flush operations help ensure data persistence")
        print("- Checkpoint operations help with WAL management")
        print("- Log control operations help with debugging")
        print("- Cluster and node operations provide system information")
        print("- mo_ctl integrates well with snapshots and clones")
        
    except Exception as e:
        print(f"Examples failed: {e}")
        raise
