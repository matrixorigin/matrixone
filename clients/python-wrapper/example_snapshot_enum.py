"""
MatrixOne Python SDK - SnapshotLevel Enum Example
Demonstrates how to use SnapshotLevel enum for type safety and better code quality
"""

from matrixone import Client
from matrixone.snapshot import SnapshotLevel
from datetime import datetime


def enum_usage_example():
    """Example of using SnapshotLevel enum"""
    
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
        
        # Create test database and table
        client.execute("CREATE DATABASE IF NOT EXISTS enum_test_db")
        client.execute("USE enum_test_db")
        client.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100)
            )
        """)
        
        # Insert some test data
        client.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Alice", "alice@example.com"))
        client.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Bob", "bob@example.com"))
        
        print("Test data created!")
        
        # Example 1: Using enum for type safety
        print("\n=== Example 1: Using SnapshotLevel enum ===")
        
        # Create snapshots using enum values
        cluster_snapshot = client.snapshots.create(
            name="cluster_backup",
            level=SnapshotLevel.CLUSTER,  # Type-safe enum value
            description="Cluster level backup"
        )
        print(f"Created cluster snapshot: {cluster_snapshot.name}")
        print(f"Snapshot level: {cluster_snapshot.level}")  # Shows enum value
        print(f"Level value: {cluster_snapshot.level.value}")  # Shows string value
        
        account_snapshot = client.snapshots.create(
            name="account_backup",
            level=SnapshotLevel.ACCOUNT,  # Type-safe enum value
            description="Account level backup"
        )
        print(f"Created account snapshot: {account_snapshot.name}")
        
        database_snapshot = client.snapshots.create(
            name="database_backup",
            level=SnapshotLevel.DATABASE,  # Type-safe enum value
            database="enum_test_db",
            description="Database level backup"
        )
        print(f"Created database snapshot: {database_snapshot.name}")
        
        table_snapshot = client.snapshots.create(
            name="table_backup",
            level=SnapshotLevel.TABLE,  # Type-safe enum value
            database="enum_test_db",
            table="users",
            description="Table level backup"
        )
        print(f"Created table snapshot: {table_snapshot.name}")
        
        # Example 2: Backward compatibility with strings
        print("\n=== Example 2: Backward compatibility with strings ===")
        
        # Still works with strings (converted to enum internally)
        string_snapshot = client.snapshots.create(
            name="string_backup",
            level="table",  # String value (backward compatible)
            database="enum_test_db",
            table="users"
        )
        print(f"Created snapshot with string level: {string_snapshot.name}")
        print(f"Level is now enum: {string_snapshot.level}")
        print(f"Level type: {type(string_snapshot.level)}")
        
        # Example 3: Enum comparison and validation
        print("\n=== Example 3: Enum comparison and validation ===")
        
        # List all snapshots and check their levels
        snapshots = client.snapshots.list()
        print(f"Found {len(snapshots)} snapshots:")
        
        for snapshot in snapshots:
            print(f"  - {snapshot.name}: {snapshot.level}")
            
            # Type-safe comparison using enum
            if snapshot.level == SnapshotLevel.CLUSTER:
                print(f"    -> This is a cluster-level snapshot")
            elif snapshot.level == SnapshotLevel.ACCOUNT:
                print(f"    -> This is an account-level snapshot")
            elif snapshot.level == SnapshotLevel.DATABASE:
                print(f"    -> This is a database-level snapshot")
            elif snapshot.level == SnapshotLevel.TABLE:
                print(f"    -> This is a table-level snapshot")
        
        # Example 4: Using enum in transactions
        print("\n=== Example 4: Using enum in transactions ===")
        
        with client.transaction() as tx:
            # Create snapshot using enum in transaction
            tx_snapshot = tx.snapshots.create(
                name="transaction_snapshot",
                level=SnapshotLevel.TABLE,  # Enum in transaction
                database="enum_test_db",
                table="users"
            )
            print(f"Created snapshot in transaction: {tx_snapshot.name}")
            
            # Clone using the snapshot
            tx.clone.clone_table_with_snapshot(
                "transaction_clone",
                "enum_test_db.users",
                "transaction_snapshot",
                if_not_exists=True
            )
            print("Cloned table in transaction using enum-based snapshot")
        
        print("Transaction completed successfully!")
        
        # Example 5: Error handling with invalid enum values
        print("\n=== Example 5: Error handling with invalid values ===")
        
        try:
            # This will raise an error
            invalid_snapshot = client.snapshots.create(
                name="invalid_snapshot",
                level="invalid_level",  # Invalid level
                database="enum_test_db",
                table="users"
            )
        except Exception as e:
            print(f"Expected error for invalid level: {e}")
        
        # Example 6: Enum iteration and validation
        print("\n=== Example 6: Enum iteration and validation ===")
        
        print("Available snapshot levels:")
        for level in SnapshotLevel:
            print(f"  - {level.name}: {level.value}")
        
        # Validate level before using
        def validate_snapshot_level(level_str):
            try:
                level_enum = SnapshotLevel(level_str.lower())
                return level_enum
            except ValueError:
                raise ValueError(f"Invalid snapshot level: {level_str}")
        
        # Test validation
        valid_level = validate_snapshot_level("table")
        print(f"Validated level: {valid_level}")
        
        try:
            invalid_level = validate_snapshot_level("invalid")
        except ValueError as e:
            print(f"Validation error: {e}")
        
        # Clean up
        print("\n=== Cleanup ===")
        snapshots_to_delete = [
            "cluster_backup", "account_backup", "database_backup", 
            "table_backup", "string_backup", "transaction_snapshot"
        ]
        
        for snapshot_name in snapshots_to_delete:
            try:
                client.snapshots.delete(snapshot_name)
                print(f"Deleted snapshot: {snapshot_name}")
            except Exception as e:
                print(f"Could not delete snapshot {snapshot_name}: {e}")
        
        # Drop test table and database
        client.execute("DROP TABLE IF EXISTS transaction_clone")
        client.execute("DROP DATABASE IF EXISTS enum_test_db")
        print("Cleanup completed!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()
        print("Disconnected from MatrixOne")


def enum_benefits_demonstration():
    """Demonstrate the benefits of using enums"""
    
    print("\n" + "="*60)
    print("SnapshotLevel Enum Benefits Demonstration")
    print("="*60)
    
    # Benefit 1: Type Safety
    print("\n1. Type Safety:")
    print("   - IDE autocomplete shows available levels")
    print("   - Compile-time validation of level values")
    print("   - No typos in level strings")
    
    # Benefit 2: Code Readability
    print("\n2. Code Readability:")
    print("   - SnapshotLevel.TABLE is clearer than 'table'")
    print("   - Self-documenting code")
    print("   - Easy to understand intent")
    
    # Benefit 3: Refactoring Safety
    print("\n3. Refactoring Safety:")
    print("   - IDE can find all usages of enum values")
    print("   - Safe renaming and refactoring")
    print("   - Centralized level definitions")
    
    # Benefit 4: Validation
    print("\n4. Built-in Validation:")
    print("   - Automatic validation of level values")
    print("   - Clear error messages for invalid values")
    print("   - No need for manual string validation")
    
    # Benefit 5: IDE Support
    print("\n5. IDE Support:")
    print("   - Autocomplete for all available levels")
    print("   - Go-to-definition functionality")
    print("   - Type hints and documentation")
    
    # Example of enum usage patterns
    print("\n6. Usage Patterns:")
    
    # Pattern 1: Switch-like behavior
    def handle_snapshot_by_level(snapshot):
        if snapshot.level == SnapshotLevel.CLUSTER:
            return "Handling cluster snapshot"
        elif snapshot.level == SnapshotLevel.ACCOUNT:
            return "Handling account snapshot"
        elif snapshot.level == SnapshotLevel.DATABASE:
            return "Handling database snapshot"
        elif snapshot.level == SnapshotLevel.TABLE:
            return "Handling table snapshot"
        else:
            return "Unknown snapshot level"
    
    # Pattern 2: Enum iteration
    print("   Available levels:")
    for level in SnapshotLevel:
        print(f"     - {level.name}: {level.value}")
    
    # Pattern 3: Enum comparison
    test_level = SnapshotLevel.TABLE
    if test_level in [SnapshotLevel.TABLE, SnapshotLevel.DATABASE]:
        print(f"   {test_level} is a data-level snapshot")
    
    # Pattern 4: String conversion
    level_str = SnapshotLevel.CLUSTER.value
    print(f"   String value of CLUSTER: {level_str}")


if __name__ == "__main__":
    print("MatrixOne SnapshotLevel Enum Examples")
    print("="*50)
    
    enum_usage_example()
    enum_benefits_demonstration()
    
    print("\n" + "="*50)
    print("Examples completed!")
    print("\nKey takeaways:")
    print("- Use SnapshotLevel.TABLE instead of 'table'")
    print("- Get type safety and IDE autocomplete")
    print("- Backward compatible with string values")
    print("- Better error handling and validation")
    print("- More maintainable and readable code")
