"""
MatrixOne Python SDK - Clone Example
Demonstrates database and table cloning functionality
"""

from matrixone import Client
from sqlalchemy import text
from datetime import datetime


def clone_example():
    """Clone functionality example"""
    
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
            tx.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Charlie", "charlie@example.com"))
        
        print("Source data created!")
        
        # Example 1: Clone table without snapshot
        print("\n=== Example 1: Clone table without snapshot ===")
        client.clone.clone_table("target_table", "source_db.users", if_not_exists=True)
        
        # Verify the clone
        result = client.execute("SELECT COUNT(*) FROM target_table")
        count = result.scalar()
        print(f"Cloned table has {count} rows")
        
        # Add more data to source table
        client.execute("INSERT INTO source_db.users (name, email) VALUES (%s, %s)", ("David", "david@example.com"))
        
        # Check that clone doesn't have the new data
        result = client.execute("SELECT COUNT(*) FROM target_table")
        clone_count = result.scalar()
        print(f"Clone still has {clone_count} rows (no new data)")
        
        # Example 2: Create snapshot and clone with snapshot
        print("\n=== Example 2: Clone table with snapshot ===")
        
        # Create a snapshot of the source table
        snapshot = client.snapshots.create(
            name="users_snapshot_001",
            level="table",
            database="source_db",
            table="users",
            description="Snapshot before adding Eve"
        )
        print(f"Snapshot created: {snapshot.name}")
        
        # Add more data after snapshot
        client.execute("INSERT INTO source_db.users (name, email) VALUES (%s, %s)", ("Eve", "eve@example.com"))
        
        # Clone table using the snapshot
        client.clone.clone_table_with_snapshot(
            "target_table_snapshot", 
            "source_db.users", 
            "users_snapshot_001",
            if_not_exists=True
        )
        
        # Verify the snapshot clone
        result = client.execute("SELECT COUNT(*) FROM target_table_snapshot")
        snapshot_clone_count = result.scalar()
        print(f"Snapshot clone has {snapshot_clone_count} rows (should be 3, not 4)")
        
        # Check current source table
        result = client.execute("SELECT COUNT(*) FROM source_db.users")
        current_count = result.scalar()
        print(f"Current source table has {current_count} rows")
        
        # Example 3: Clone entire database
        print("\n=== Example 3: Clone entire database ===")
        
        # Create another table in source database
        client.execute("""
            CREATE TABLE IF NOT EXISTS source_db.orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                product VARCHAR(100),
                amount DECIMAL(10,2)
            )
        """)
        
        # Insert some orders
        client.execute("INSERT INTO source_db.orders (user_id, product, amount) VALUES (1, 'Laptop', 999.99)")
        client.execute("INSERT INTO source_db.orders (user_id, product, amount) VALUES (2, 'Mouse', 29.99)")
        
        # Clone the entire database
        client.clone.clone_database("cloned_db", "source_db", if_not_exists=True)
        
        # Verify the database clone
        result = client.execute("SHOW TABLES FROM cloned_db")
        tables = [row[0] for row in result.fetchall()]
        print(f"Cloned database has tables: {tables}")
        
        # Check data in cloned tables
        result = client.execute("SELECT COUNT(*) FROM cloned_db.users")
        users_count = result.scalar()
        result = client.execute("SELECT COUNT(*) FROM cloned_db.orders")
        orders_count = result.scalar()
        print(f"Cloned database: {users_count} users, {orders_count} orders")
        
        # Example 4: Clone database with snapshot
        print("\n=== Example 4: Clone database with snapshot ===")
        
        # Create a database-level snapshot
        db_snapshot = client.snapshots.create(
            name="db_snapshot_001",
            level="database",
            database="source_db",
            description="Database snapshot before adding new table"
        )
        print(f"Database snapshot created: {db_snapshot.name}")
        
        # Add a new table after snapshot
        client.execute("""
            CREATE TABLE IF NOT EXISTS source_db.products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10,2)
            )
        """)
        
        # Clone database using snapshot
        client.clone.clone_database_with_snapshot(
            "cloned_db_snapshot", 
            "source_db", 
            "db_snapshot_001",
            if_not_exists=True
        )
        
        # Verify the snapshot database clone
        result = client.execute("SHOW TABLES FROM cloned_db_snapshot")
        snapshot_tables = [row[0] for row in result.fetchall()]
        print(f"Snapshot cloned database has tables: {snapshot_tables}")
        print("Note: Should not include 'products' table as it was added after snapshot")
        
        # Example 5: Cross-database table clone
        print("\n=== Example 5: Cross-database table clone ===")
        
        # Clone table from one database to another
        client.clone.clone_table("cloned_db.users_backup", "source_db.users", if_not_exists=True)
        
        # Verify cross-database clone
        result = client.execute("SELECT COUNT(*) FROM cloned_db.users_backup")
        backup_count = result.scalar()
        print(f"Cross-database clone has {backup_count} rows")
        
        # Clean up snapshots
        print("\n=== Cleanup ===")
        client.snapshots.delete("users_snapshot_001")
        client.snapshots.delete("db_snapshot_001")
        print("Snapshots deleted")
        
        # Clean up databases and tables
        client.execute("DROP DATABASE IF EXISTS source_db")
        client.execute("DROP DATABASE IF EXISTS cloned_db")
        client.execute("DROP DATABASE IF EXISTS cloned_db_snapshot")
        client.execute("DROP TABLE IF EXISTS target_table")
        client.execute("DROP TABLE IF EXISTS target_table_snapshot")
        
        print("Cleanup completed!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()
        print("Disconnected from MatrixOne")


def clone_workflow_example():
    """Example of a complete clone workflow"""
    
    client = Client()
    
    try:
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        print("=== Complete Clone Workflow Example ===")
        
        # Step 1: Create source database with data
        client.execute("CREATE DATABASE IF NOT EXISTS production_db")
        client.execute("USE production_db")
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
        
        print("Step 1: Production database created with data")
        
        # Step 2: Create snapshot for backup
        snapshot = client.snapshots.create(
            name="production_backup_001",
            level="database",
            database="production_db",
            description="Daily backup snapshot"
        )
        print(f"Step 2: Snapshot created: {snapshot.name}")
        
        # Step 3: Create development environment from snapshot
        client.clone.clone_database_with_snapshot(
            "development_db", 
            "production_db", 
            "production_backup_001",
            if_not_exists=True
        )
        print("Step 3: Development database cloned from snapshot")
        
        # Step 4: Create testing environment from snapshot
        client.clone.clone_database_with_snapshot(
            "testing_db", 
            "production_db", 
            "production_backup_001",
            if_not_exists=True
        )
        print("Step 4: Testing database cloned from snapshot")
        
        # Step 5: Verify environments
        for env_name in ["production_db", "development_db", "testing_db"]:
            result = client.execute(f"SELECT COUNT(*) FROM {env_name}.customers")
            count = result.scalar()
            print(f"  {env_name}: {count} customers")
        
        # Step 6: Make changes in development
        client.execute("INSERT INTO development_db.customers (name, email) VALUES (%s, %s)", ("Dev User", "dev@example.com"))
        client.execute("UPDATE development_db.customers SET status = 'inactive' WHERE id = 1")
        
        print("Step 5: Changes made in development environment")
        
        # Step 7: Verify isolation
        prod_count = client.execute("SELECT COUNT(*) FROM production_db.customers").scalar()
        dev_count = client.execute("SELECT COUNT(*) FROM development_db.customers").scalar()
        test_count = client.execute("SELECT COUNT(*) FROM testing_db.customers").scalar()
        
        print(f"Step 6: Environment isolation verified:")
        print(f"  Production: {prod_count} customers")
        print(f"  Development: {dev_count} customers")
        print(f"  Testing: {test_count} customers")
        
        # Cleanup
        client.snapshots.delete("production_backup_001")
        client.execute("DROP DATABASE IF EXISTS production_db")
        client.execute("DROP DATABASE IF EXISTS development_db")
        client.execute("DROP DATABASE IF EXISTS testing_db")
        
        print("Workflow completed and cleaned up!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()


if __name__ == "__main__":
    print("MatrixOne Clone Examples")
    print("=" * 50)
    
    clone_example()
    
    print("\n" + "=" * 50)
    clone_workflow_example()
