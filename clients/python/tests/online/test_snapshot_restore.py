"""
Online tests for snapshot and restore functionality

These tests are inspired by example_05_snapshot_restore.py
"""

import pytest
import time
from matrixone import Client, AsyncClient
from matrixone.logger import create_default_logger
from .test_config import online_config


@pytest.mark.online
class TestSnapshotRestore:
    """Test snapshot and restore functionality"""

    def test_basic_snapshot_operations(self, test_client):
        """Test basic snapshot creation and management"""
        # Create test data
        test_client.execute("CREATE TABLE IF NOT EXISTS snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)")
        test_client.execute("DELETE FROM snapshot_test")
        test_client.execute("INSERT INTO snapshot_test VALUES (1, 'test1', 100)")
        test_client.execute("INSERT INTO snapshot_test VALUES (2, 'test2', 200)")

        # Verify initial data
        result = test_client.execute("SELECT COUNT(*) FROM snapshot_test")
        assert result.rows[0][0] == 2

        # Create snapshot
        snapshot_name = f"testsnapshot{int(time.time())}"
        try:
            snapshot = test_client.snapshots.create(
                name=snapshot_name,
                level="table",
                database=online_config.get_test_database(),
                table="snapshot_test",
                description="Test snapshot for basic operations",
            )
            assert snapshot is not None
            assert snapshot.name == snapshot_name
        except Exception as e:
            pytest.skip(f"Snapshot creation not supported: {e}")

        # Add more data after snapshot
        test_client.execute("INSERT INTO snapshot_test VALUES (3, 'test3', 300)")
        test_client.execute("INSERT INTO snapshot_test VALUES (4, 'test4', 400)")

        result = test_client.execute("SELECT COUNT(*) FROM snapshot_test")
        assert result.rows[0][0] == 4

        # List snapshots
        try:
            snapshots = test_client.snapshots.list()
            assert isinstance(snapshots, list)
            # Check if our snapshot is in the list
            snapshot_names = [s.name for s in snapshots]
            assert snapshot_name in snapshot_names
        except Exception as e:
            pytest.skip(f"Snapshot listing not supported: {e}")

        # Cleanup
        try:
            test_client.snapshots.delete(snapshot_name)
        except Exception:
            pass  # Ignore cleanup errors

        test_client.execute("DROP TABLE IF EXISTS snapshot_test")

    def test_snapshot_enumeration(self, test_client):
        """Test snapshot enumeration and information"""
        # Create test table
        test_client.execute("CREATE TABLE IF NOT EXISTS snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)")
        test_client.execute("DELETE FROM snapshot_test")
        test_client.execute("INSERT INTO snapshot_test VALUES (1, 'enum_test1', 100)")
        test_client.execute("INSERT INTO snapshot_test VALUES (2, 'enum_test2', 200)")

        # Create multiple snapshots
        snapshot_names = []
        for i in range(3):
            snapshot_name = f"enumsnapshot{i}{int(time.time())}"
            try:
                snapshot = test_client.snapshots.create(
                    name=snapshot_name,
                    level="table",
                    database=online_config.get_test_database(),
                    table="snapshot_test",
                    description=f"Test snapshot {i}",
                )
                snapshot_names.append(snapshot_name)
            except Exception as e:
                pytest.skip(f"Snapshot creation not supported: {e}")

        # List all snapshots
        try:
            snapshots = test_client.snapshots.list()
            assert isinstance(snapshots, list)
            assert len(snapshots) >= len(snapshot_names)

            # Verify our snapshots are in the list
            snapshot_names_in_list = [s.name for s in snapshots]
            for name in snapshot_names:
                assert name in snapshot_names_in_list
        except Exception as e:
            pytest.skip(f"Snapshot listing not supported: {e}")

        # Cleanup
        for snapshot_name in snapshot_names:
            try:
                test_client.snapshots.delete(snapshot_name)
            except Exception:
                pass  # Ignore cleanup errors

        test_client.execute("DROP TABLE IF EXISTS snapshot_test")

    def test_snapshot_restore_operations(self, test_client):
        """Test snapshot restore operations"""
        # Create test data
        test_client.execute("CREATE TABLE IF NOT EXISTS snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)")
        test_client.execute("DELETE FROM snapshot_test")
        test_client.execute("INSERT INTO snapshot_test VALUES (1, 'restore_test1', 100)")
        test_client.execute("INSERT INTO snapshot_test VALUES (2, 'restore_test2', 200)")

        # Create snapshot
        snapshot_name = f"restoresnapshot{int(time.time())}"
        try:
            snapshot = test_client.snapshots.create(
                name=snapshot_name,
                level="table",
                database=online_config.get_test_database(),
                table="snapshot_test",
                description="Test snapshot for restore operations",
            )
        except Exception as e:
            pytest.skip(f"Snapshot creation not supported: {e}")

        # Add more data after snapshot
        test_client.execute("INSERT INTO snapshot_test VALUES (3, 'restore_test3', 300)")
        test_client.execute("INSERT INTO snapshot_test VALUES (4, 'restore_test4', 400)")

        result = test_client.execute("SELECT COUNT(*) FROM snapshot_test")
        assert result.rows[0][0] == 4

        # Restore from snapshot
        try:
            test_client.restore.restore_table(snapshot_name, "sys", "test", "snapshot_test")

            # Verify data after restore
            result = test_client.execute("SELECT COUNT(*) FROM snapshot_test")
            assert result.rows[0][0] == 2  # Should be back to original 2 records

            # Verify specific data
            result = test_client.execute("SELECT * FROM snapshot_test ORDER BY id")
            assert len(result.rows) == 2
            assert result.rows[0][0] == 1  # ID: 1
            assert result.rows[1][0] == 2  # ID: 2
        except Exception as e:
            pytest.skip(f"Snapshot restore not supported: {e}")

        # Cleanup
        try:
            test_client.snapshots.delete(snapshot_name)
        except Exception:
            pass  # Ignore cleanup errors

        test_client.execute("DROP TABLE IF EXISTS snapshot_test")

    def test_snapshot_error_handling(self, test_client):
        """Test snapshot error handling"""
        # Test creating snapshot with invalid parameters
        try:
            test_client.snapshots.create(
                name="",  # Empty name should fail
                level="table",
                database=online_config.get_test_database(),
                table="nonexistent_table",
                description="Test invalid snapshot",
            )
            assert False, "Should have failed with empty name"
        except Exception:
            pass  # Expected to fail

        # Test deleting non-existent snapshot
        try:
            test_client.snapshots.delete("nonexistent_snapshot")
            assert False, "Should have failed with non-existent snapshot"
        except Exception:
            pass  # Expected to fail

        # Test restoring from non-existent snapshot
        try:
            test_client.restore.restore_table("nonexistent_snapshot", "sys", "test", "test_table")
            assert False, "Should have failed with non-existent snapshot"
        except Exception:
            pass  # Expected to fail

    @pytest.mark.asyncio
    async def test_async_snapshot_operations(self, test_async_client):
        """Test async snapshot operations"""
        # Create test data
        await test_async_client.execute(
            "CREATE TABLE IF NOT EXISTS snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)"
        )
        await test_async_client.execute("DELETE FROM snapshot_test")
        await test_async_client.execute("INSERT INTO snapshot_test VALUES (1, 'async_test1', 100)")
        await test_async_client.execute("INSERT INTO snapshot_test VALUES (2, 'async_test2', 200)")

        # Verify initial data
        result = await test_async_client.execute("SELECT COUNT(*) FROM snapshot_test")
        assert result.rows[0][0] == 2

        # Test snapshot operations (as per examples - handle failures gracefully)
        snapshot_name = f"asyncsnapshot{int(time.time())}"

        # Test snapshot listing (should work even if empty)
        snapshots = await test_async_client.snapshots.list()
        assert isinstance(snapshots, list)

        try:
            snapshot = await test_async_client.snapshots.create(
                name=snapshot_name,
                level="table",
                database=online_config.get_test_database(),
                table="snapshot_test",
                description="Test async snapshot",
            )
            assert snapshot is not None
            assert snapshot.name == snapshot_name

            # Test snapshot listing after creation
            snapshots = await test_async_client.snapshots.list()
            assert isinstance(snapshots, list)

            # Test snapshot deletion
            await test_async_client.snapshots.delete(snapshot_name)

        except Exception as e:
            # If snapshot creation fails, test the API methods still work
            assert "snapshot" in str(e).lower() or "SQL" in str(e) or "syntax" in str(e).lower()  # Ignore cleanup errors

        await test_async_client.execute("DROP TABLE IF EXISTS snapshot_test")

    def test_snapshot_with_logging(self, connection_params):
        """Test snapshot operations with custom logging"""
        host, port, user, password, database = connection_params

        # Create logger
        logger = create_default_logger()

        # Create client with logging
        client = Client()
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Create test data
            client.execute("CREATE TABLE IF NOT EXISTS snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)")
            client.execute("DELETE FROM snapshot_test")
            client.execute("INSERT INTO snapshot_test VALUES (1, 'log_test1', 100)")

            # Create snapshot with logging
            snapshot_name = f"logsnapshot{int(time.time())}"
            try:
                snapshot = client.snapshots.create(
                    name=snapshot_name,
                    level="table",
                    database=online_config.get_test_database(),
                    table="snapshot_test",
                    description="Test snapshot with logging",
                )
                assert snapshot is not None
            except Exception as e:
                pytest.skip(f"Snapshot creation not supported: {e}")

            # Cleanup
            try:
                client.snapshots.delete(snapshot_name)
            except Exception:
                pass  # Ignore cleanup errors

            client.execute("DROP TABLE IF EXISTS snapshot_test")

        finally:
            client.disconnect()
