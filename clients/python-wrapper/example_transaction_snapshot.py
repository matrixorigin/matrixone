"""
MatrixOne Python SDK - Transaction with Snapshot and Clone Example
Demonstrates how to use snapshot and clone operations within transactions
"""

from matrixone import Client
from sqlalchemy import text
from datetime import datetime


def transaction_snapshot_example():
    """Example of using snapshots and clones within transactions"""
    
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
        
        print("Connected to MatrixOne!")
        
        # Create source database and table
        client.execute("CREATE DATABASE IF NOT EXISTS source_db")
        client.execute("USE source_db")
        client.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert some test data
        with client.transaction() as tx:
            tx.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Alice", "alice@example.com"))
            tx.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Bob", "bob@example.com"))
        
        print("Source data created!")
        
        # Example 1: Create snapshot and clone within a single transaction
        print("\n=== Example 1: Snapshot and clone within transaction ===")
        
        with client.transaction() as tx:
            # Create snapshot within transaction
            snapshot = tx.snapshots.create(
                name="transaction_snapshot_001",
                level="table",
                database="source_db",
                table="users",
                description="Snapshot created within transaction"
            )
            print(f"Snapshot created within transaction: {snapshot.name}")
            
            # Clone table using the snapshot within the same transaction
            tx.clone.clone_table_with_snapshot(
                "transaction_clone_table",
                "source_db.users",
                "transaction_snapshot_001",
                if_not_exists=True
            )
            print("Table cloned within transaction using snapshot")
            
            # Add more data within the same transaction
            tx.execute("INSERT INTO source_db.users (name, email) VALUES (%s, %s)", ("Charlie", "charlie@example.com"))
            print("Additional data added within transaction")
        
        print("Transaction committed successfully!")
        
        # Verify the results
        result = client.execute("SELECT COUNT(*) FROM transaction_clone_table")
        clone_count = result.scalar()
        print(f"Clone table has {clone_count} rows (should be 2, not 3)")
        
        result = client.execute("SELECT COUNT(*) FROM source_db.users")
        current_count = result.scalar()
        print(f"Source table has {current_count} rows (should be 3)")
        
        # Example 2: Multiple operations within a transaction
        print("\n=== Example 2: Multiple snapshot and clone operations ===")
        
        with client.transaction() as tx:
            # Create multiple snapshots
            snapshot1 = tx.snapshots.create("multi_snap_1", "table", database="source_db", table="users")
            snapshot2 = tx.snapshots.create("multi_snap_2", "table", database="source_db", table="users")
            print(f"Created snapshots: {snapshot1.name}, {snapshot2.name}")
            
            # Clone to multiple targets
            tx.clone.clone_table_with_snapshot("clone1", "source_db.users", "multi_snap_1", if_not_exists=True)
            tx.clone.clone_table_with_snapshot("clone2", "source_db.users", "multi_snap_2", if_not_exists=True)
            print("Created multiple clones")
            
            # Add data between snapshots
            tx.execute("INSERT INTO source_db.users (name, email) VALUES (%s, %s)", ("David", "david@example.com"))
            
            # Create another snapshot after data change
            snapshot3 = tx.snapshots.create("multi_snap_3", "table", database="source_db", table="users")
            tx.clone.clone_table_with_snapshot("clone3", "source_db.users", "multi_snap_3", if_not_exists=True)
            print(f"Created snapshot and clone after data change: {snapshot3.name}")
        
        print("Multiple operations transaction committed!")
        
        # Verify different clone states
        for i, clone_name in enumerate(["clone1", "clone2", "clone3"], 1):
            result = client.execute(f"SELECT COUNT(*) FROM {clone_name}")
            count = result.scalar()
            print(f"{clone_name} has {count} rows")
        
        # Example 3: Transaction rollback scenario
        print("\n=== Example 3: Transaction rollback scenario ===")
        
        try:
            with client.transaction() as tx:
                # Create snapshot
                snapshot = tx.snapshots.create("rollback_snapshot", "table", database="source_db", table="users")
                print(f"Snapshot created: {snapshot.name}")
                
                # Clone table
                tx.clone.clone_table("rollback_clone", "source_db.users", if_not_exists=True)
                print("Table cloned")
                
                # Simulate an error
                tx.execute("INSERT INTO non_existent_table VALUES (1)")  # This will fail
                
        except Exception as e:
            print(f"Transaction failed and rolled back: {e}")
        
        # Verify that snapshot and clone were not created due to rollback
        try:
            client.snapshots.get("rollback_snapshot")
            print("ERROR: Snapshot should not exist after rollback!")
        except Exception:
            print("✓ Snapshot correctly rolled back")
        
        try:
            client.execute("SELECT COUNT(*) FROM rollback_clone")
            print("ERROR: Clone table should not exist after rollback!")
        except Exception:
            print("✓ Clone table correctly rolled back")
        
        # Clean up
        print("\n=== Cleanup ===")
        snapshots_to_delete = [
            "transaction_snapshot_001",
            "multi_snap_1", "multi_snap_2", "multi_snap_3"
        ]
        
        for snapshot_name in snapshots_to_delete:
            try:
                client.snapshots.delete(snapshot_name)
                print(f"Deleted snapshot: {snapshot_name}")
            except Exception as e:
                print(f"Could not delete snapshot {snapshot_name}: {e}")
        
        # Drop tables
        tables_to_drop = [
            "transaction_clone_table",
            "clone1", "clone2", "clone3"
        ]
        
        for table_name in tables_to_drop:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
                print(f"Dropped table: {table_name}")
            except Exception as e:
                print(f"Could not drop table {table_name}: {e}")
        
        client.execute("DROP DATABASE IF EXISTS source_db")
        print("Cleanup completed!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()
        print("Disconnected from MatrixOne")


def complex_transaction_workflow():
    """Example of a complex workflow using transactions with snapshots and clones"""
    
    client = Client()
    
    try:
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        print("=== Complex Transaction Workflow ===")
        
        # Step 1: Create production database
        client.execute("CREATE DATABASE IF NOT EXISTS production")
        client.execute("USE production")
        client.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                status VARCHAR(20) DEFAULT 'active'
            )
        """)
        
        # Insert production data
        with client.transaction() as tx:
            tx.execute("INSERT INTO customers (name, email) VALUES (%s, %s)", ("John Doe", "john@example.com"))
            tx.execute("INSERT INTO customers (name, email) VALUES (%s, %s)", ("Jane Smith", "jane@example.com"))
        
        print("Step 1: Production database created")
        
        # Step 2: Create backup and development environment in one transaction
        with client.transaction() as tx:
            # Create backup snapshot
            backup_snapshot = tx.snapshots.create(
                name="production_backup_001",
                level="database",
                database="production",
                description="Production backup before development setup"
            )
            print(f"Step 2a: Backup snapshot created: {backup_snapshot.name}")
            
            # Create development environment from snapshot
            tx.clone.clone_database_with_snapshot(
                "development",
                "production",
                "production_backup_001",
                if_not_exists=True
            )
            print("Step 2b: Development database cloned from backup")
            
            # Create testing environment from snapshot
            tx.clone.clone_database_with_snapshot(
                "testing",
                "production",
                "production_backup_001",
                if_not_exists=True
            )
            print("Step 2c: Testing database cloned from backup")
        
        print("Step 2: All environments created in single transaction")
        
        # Step 3: Make changes in development within transaction
        with client.transaction() as tx:
            # Add development-specific data
            tx.execute("INSERT INTO development.customers (name, email) VALUES (%s, %s)", ("Dev User", "dev@example.com"))
            
            # Update existing data
            tx.execute("UPDATE development.customers SET status = 'inactive' WHERE id = 1")
            
            # Create snapshot of development changes
            dev_snapshot = tx.snapshots.create(
                name="development_changes_001",
                level="database",
                database="development",
                description="Development changes snapshot"
            )
            print(f"Step 3a: Development changes made and snapshotted: {dev_snapshot.name}")
            
            # Create staging environment from development
            tx.clone.clone_database_with_snapshot(
                "staging",
                "development",
                "development_changes_001",
                if_not_exists=True
            )
            print("Step 3b: Staging environment created from development")
        
        print("Step 3: Development changes and staging environment created")
        
        # Step 4: Verify all environments
        environments = ["production", "development", "testing", "staging"]
        for env in environments:
            result = client.execute(f"SELECT COUNT(*) FROM {env}.customers")
            count = result.scalar()
            print(f"  {env}: {count} customers")
        
        # Step 5: Cleanup
        with client.transaction() as tx:
            # Delete snapshots
            tx.snapshots.delete("production_backup_001")
            tx.snapshots.delete("development_changes_001")
            print("Step 5a: Snapshots deleted")
            
            # Drop databases
            for env in ["development", "testing", "staging"]:
                tx.execute(f"DROP DATABASE IF EXISTS {env}")
            print("Step 5b: Environment databases dropped")
        
        client.execute("DROP DATABASE IF EXISTS production")
        print("Step 5c: Production database dropped")
        
        print("Complex workflow completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()


if __name__ == "__main__":
    print("MatrixOne Transaction with Snapshot and Clone Examples")
    print("=" * 60)
    
    transaction_snapshot_example()
    
    print("\n" + "=" * 60)
    complex_transaction_workflow()
