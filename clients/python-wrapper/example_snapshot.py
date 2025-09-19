"""
MatrixOne Python SDK - Snapshot Example
"""

from matrixone import Client
from sqlalchemy import text
from datetime import datetime


def snapshot_example():
    """Snapshot functionality example"""
    
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
        
        # Create a test table
        client.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert initial data
        with client.transaction() as tx:
            tx.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Alice", "alice@example.com"))
            tx.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Bob", "bob@example.com"))
        
        print("Initial data inserted!")
        
        # Create snapshot
        snapshot = client.snapshots.create(
            name="users_backup_001",
            level="table",
            database="test",
            table="users",
            description="Backup of users table"
        )
        
        print(f"Snapshot created: {snapshot.name} at {snapshot.created_at}")
        
        # Add more data after snapshot
        client.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Charlie", "charlie@example.com"))
        
        # Query current data
        current_result = client.execute("SELECT COUNT(*) FROM users")
        current_count = current_result.scalar()
        print(f"Current user count: {current_count}")
        
        # Note: Snapshot query functionality needs to be implemented based on MatrixOne's actual syntax
        print("Note: Snapshot query functionality is being developed...")
        
        # For now, let's just show that we can manage snapshots
        print(f"Snapshot '{snapshot.name}' created successfully!")
        print(f"Snapshot level: {snapshot.level}")
        print(f"Snapshot database: {snapshot.database}")
        print(f"Snapshot table: {snapshot.table}")
        
        # List all snapshots
        print("\nAll snapshots:")
        snapshots = client.snapshots.list()
        for snapshot in snapshots:
            print(f"  {snapshot.name} - {snapshot.level} - {snapshot.created_at}")
        
        # Test snapshot management
        print(f"\nSnapshot exists: {client.snapshots.exists('users_backup_001')}")
        
        # Get specific snapshot
        retrieved_snapshot = client.snapshots.get("users_backup_001")
        print(f"Retrieved snapshot: {retrieved_snapshot}")
        
        # Clean up
        client.snapshots.delete("users_backup_001")
        client.execute("DROP TABLE users")
        
        print("\nCleanup completed!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()
        print("Disconnected from MatrixOne")


if __name__ == "__main__":
    snapshot_example()
