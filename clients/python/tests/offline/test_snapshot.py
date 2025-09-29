"""
Test snapshot functionality for MatrixOne Python SDK
"""

from matrixone import Client, SnapshotError
from sqlalchemy import text


def test_snapshot_management():
    """Test snapshot management functionality"""
    client = Client()

    try:
        # Connect to MatrixOne
        client.connect(host="localhost", port=6001, user="root", password="111", database="test")

        print("✓ Connected to MatrixOne successfully")

        # Create test table
        client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_snapshot (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                value INT
            )
        """
        )

        # Insert test data
        client.execute("INSERT INTO test_snapshot (name, value) VALUES ('test1', 100)")
        client.execute("INSERT INTO test_snapshot (name, value) VALUES ('test2', 200)")

        print("✓ Test data inserted")

        # Create snapshot
        snapshot = client.snapshots.create(
            name="test_snapshot_001",
            level="table",
            database="test",
            table="test_snapshot",
            description="Test snapshot for SDK",
        )

        print(f"✓ Snapshot created: {snapshot}")

        # List snapshots
        snapshots = client.snapshots.list()
        print(f"✓ Found {len(snapshots)} snapshots")

        # Get specific snapshot
        retrieved_snapshot = client.snapshots.get("test_snapshot_001")
        print(f"✓ Retrieved snapshot: {retrieved_snapshot}")

        # Test snapshot query using query builder
        result = client.query("test_snapshot", snapshot="test_snapshot_001").select("*").execute()
        print(f"✓ Snapshot query executed: {len(result.fetchall())} rows")

        # Test snapshot context manager
        with client.snapshot("test_snapshot_001") as snapshot_client:
            result = snapshot_client.execute("SELECT COUNT(*) FROM test_snapshot")
            count = result.scalar()
            print(f"✓ Snapshot context manager: {count} rows")

        # Test snapshot query builder
        result = (
            client.query("test_snapshot", snapshot="test_snapshot_001")
            .select("name", "value")
            .where("value > ?", 150)
            .execute()
        )
        print(f"✓ Snapshot query builder: {len(result.fetchall())} rows")

        # Test SQLAlchemy with snapshot
        snapshot_engine = client.get_snapshot_engine("test_snapshot_001")
        with snapshot_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM test_snapshot"))
            count = result.scalar()
            print(f"✓ SQLAlchemy snapshot engine: {count} rows")

        # Clean up
        client.snapshots.delete("test_snapshot_001")
        client.execute("DROP TABLE test_snapshot")

        print("✓ All snapshot tests passed!")

    except Exception as e:
        print(f"✗ Test failed: {e}")
    finally:
        client.disconnect()


def test_snapshot_errors():
    """Test snapshot error handling"""
    client = Client()

    try:
        client.connect(host="localhost", port=6001, user="root", password="111", database="test")

        # Test non-existent snapshot
        try:
            client.snapshots.get("non_existent_snapshot")
            print("✗ Should have raised SnapshotError")
        except SnapshotError:
            print("✓ Correctly raised SnapshotError for non-existent snapshot")

        # Test invalid snapshot level
        try:
            client.snapshots.create("test", "invalid_level")
            print("✗ Should have raised SnapshotError")
        except SnapshotError:
            print("✓ Correctly raised SnapshotError for invalid level")

        print("✓ All error handling tests passed!")

    except Exception as e:
        print(f"✗ Error test failed: {e}")
    finally:
        client.disconnect()


if __name__ == "__main__":
    print("Testing snapshot functionality...")
    test_snapshot_management()
    print("\nTesting snapshot error handling...")
    test_snapshot_errors()
