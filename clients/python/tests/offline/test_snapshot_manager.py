"""
Unit tests for SnapshotManager class
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
import os

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone.snapshot import SnapshotManager, Snapshot, SnapshotLevel
from matrixone.exceptions import (
    SnapshotError,
    ConnectionError as MatrixOneConnectionError,
    QueryError,
)


class TestSnapshot(unittest.TestCase):
    """Test Snapshot class"""

    def test_snapshot_creation(self):
        """Test snapshot object creation"""
        now = datetime.now()
        snapshot = Snapshot(
            name="test_snapshot",
            level="table",
            created_at=now,
            description="Test snapshot",
            database="test_db",
            table="test_table",
        )

        self.assertEqual(snapshot.name, "test_snapshot")
        self.assertEqual(snapshot.level, SnapshotLevel.TABLE)
        self.assertEqual(snapshot.created_at, now)
        self.assertEqual(snapshot.description, "Test snapshot")
        self.assertEqual(snapshot.database, "test_db")
        self.assertEqual(snapshot.table, "test_table")

    def test_snapshot_repr(self):
        """Test snapshot string representation"""
        now = datetime.now()
        snapshot = Snapshot("test", "table", now)
        repr_str = repr(snapshot)
        self.assertIn("test", repr_str)
        self.assertIn("SnapshotLevel.TABLE", repr_str)


class TestSnapshotManager(unittest.TestCase):
    """Test SnapshotManager class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client._engine = Mock()
        self.snapshot_manager = SnapshotManager(self.mock_client)

    def test_init(self):
        """Test SnapshotManager initialization"""
        self.assertEqual(self.snapshot_manager.client, self.mock_client)

    def test_create_cluster_snapshot(self):
        """Test creating cluster level snapshot"""
        # Mock the execute method and get method
        self.mock_client.execute = Mock()
        self.mock_client.execute.return_value = Mock()

        # Mock get method to return a snapshot
        mock_snapshot = Snapshot("cluster_snap", "cluster", datetime.now())
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("cluster_snap", "cluster")

            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT cluster_snap FOR CLUSTER")
            self.assertEqual(result, mock_snapshot)

    def test_create_account_snapshot(self):
        """Test creating account level snapshot"""
        self.mock_client.execute = Mock()
        self.mock_client.execute.return_value = Mock()

        mock_snapshot = Snapshot("account_snap", "account", datetime.now())
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("account_snap", "account")

            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT account_snap FOR ACCOUNT")
            self.assertEqual(result, mock_snapshot)

    def test_create_database_snapshot(self):
        """Test creating database level snapshot"""
        self.mock_client.execute = Mock()
        self.mock_client.execute.return_value = Mock()

        mock_snapshot = Snapshot("db_snap", "database", datetime.now(), database="test_db")
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("db_snap", "database", database="test_db")

            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT db_snap FOR DATABASE test_db")
            self.assertEqual(result, mock_snapshot)

    def test_create_table_snapshot(self):
        """Test creating table level snapshot"""
        self.mock_client.execute = Mock()
        self.mock_client.execute.return_value = Mock()

        mock_snapshot = Snapshot("table_snap", "table", datetime.now(), database="test_db", table="test_table")
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("table_snap", "table", database="test_db", table="test_table")

            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT table_snap FOR TABLE test_db test_table")
            self.assertEqual(result, mock_snapshot)

    def test_create_snapshot_missing_database(self):
        """Test creating database snapshot without database name"""
        with self.assertRaises(SnapshotError) as context:
            self.snapshot_manager.create("db_snap", "database")

        self.assertIn("Database name required", str(context.exception))

    def test_create_snapshot_missing_table_params(self):
        """Test creating table snapshot without database or table name"""
        with self.assertRaises(SnapshotError) as context:
            self.snapshot_manager.create("table_snap", "table")

        self.assertIn("Database and table names required", str(context.exception))

        with self.assertRaises(SnapshotError) as context:
            self.snapshot_manager.create("table_snap", "table", database="test_db")

        self.assertIn("Database and table names required", str(context.exception))

    def test_create_snapshot_invalid_level(self):
        """Test creating snapshot with invalid level"""
        with self.assertRaises(SnapshotError) as context:
            self.snapshot_manager.create("test_snap", "invalid_level")

        self.assertIn("Invalid snapshot level", str(context.exception))

    def test_create_snapshot_not_connected(self):
        """Test creating snapshot when not connected"""
        self.mock_client._engine = None

        with self.assertRaises(MatrixOneConnectionError) as context:
            self.snapshot_manager.create("test_snap", "cluster")

        self.assertIn("Not connected to database", str(context.exception))

    def test_create_snapshot_execution_error(self):
        """Test creating snapshot with execution error"""
        self.mock_client.execute = Mock(side_effect=Exception("Database error"))

        with self.assertRaises(SnapshotError) as context:
            self.snapshot_manager.create("test_snap", "cluster")

        self.assertIn("Failed to create snapshot", str(context.exception))

    def test_list_snapshots(self):
        """Test listing snapshots"""
        # Mock the execute method to return snapshot data
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            (
                "snap1",
                1640995200000000000,
                "cluster",
                "account1",
                None,
                None,
            ),  # nanoseconds timestamp
            ("snap2", 1640995201000000000, "table", "account1", "db1", "table1"),
        ]
        self.mock_client.execute = Mock(return_value=mock_result)

        snapshots = self.snapshot_manager.list()

        self.assertEqual(len(snapshots), 2)
        self.assertEqual(snapshots[0].name, "snap1")
        self.assertEqual(snapshots[0].level, SnapshotLevel.CLUSTER)
        self.assertEqual(snapshots[1].name, "snap2")
        self.assertEqual(snapshots[1].level, SnapshotLevel.TABLE)
        self.assertEqual(snapshots[1].database, "db1")
        self.assertEqual(snapshots[1].table, "table1")

        # Verify timestamp conversion
        self.assertIsInstance(snapshots[0].created_at, datetime)

    def test_list_snapshots_not_connected(self):
        """Test listing snapshots when not connected"""
        self.mock_client._engine = None

        with self.assertRaises(MatrixOneConnectionError) as context:
            self.snapshot_manager.list()

        self.assertIn("Not connected to database", str(context.exception))

    def test_list_snapshots_execution_error(self):
        """Test listing snapshots with execution error"""
        self.mock_client.execute = Mock(side_effect=Exception("Database error"))

        with self.assertRaises(SnapshotError) as context:
            self.snapshot_manager.list()

        self.assertIn("Failed to list snapshots", str(context.exception))

    def test_get_snapshot(self):
        """Test getting a specific snapshot"""
        # Mock the execute method to return snapshot data
        mock_result = Mock()
        mock_result.fetchone.return_value = (
            "test_snap",
            1640995200000000000,
            "table",
            "account1",
            "db1",
            "table1",
        )
        self.mock_client.execute = Mock(return_value=mock_result)

        snapshot = self.snapshot_manager.get("test_snap")

        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.TABLE)
        self.assertEqual(snapshot.database, "db1")
        self.assertEqual(snapshot.table, "table1")
        self.assertIsInstance(snapshot.created_at, datetime)

    def test_get_snapshot_not_found(self):
        """Test getting non-existent snapshot"""
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        self.mock_client.execute = Mock(return_value=mock_result)

        with self.assertRaises(SnapshotError) as context:
            self.snapshot_manager.get("non_existent")

        self.assertIn("not found", str(context.exception))

    def test_get_snapshot_not_connected(self):
        """Test getting snapshot when not connected"""
        self.mock_client._engine = None

        with self.assertRaises(MatrixOneConnectionError) as context:
            self.snapshot_manager.get("test_snap")

        self.assertIn("Not connected to database", str(context.exception))

    def test_get_snapshot_execution_error(self):
        """Test getting snapshot with execution error"""
        self.mock_client.execute = Mock(side_effect=Exception("Database error"))

        with self.assertRaises(SnapshotError) as context:
            self.snapshot_manager.get("test_snap")

        self.assertIn("Failed to get snapshot", str(context.exception))

    def test_delete_snapshot(self):
        """Test deleting a snapshot"""
        self.mock_client.execute = Mock()

        self.snapshot_manager.delete("test_snap")

        self.mock_client.execute.assert_called_once_with("DROP SNAPSHOT test_snap")

    def test_delete_snapshot_not_connected(self):
        """Test deleting snapshot when not connected"""
        self.mock_client._engine = None

        with self.assertRaises(MatrixOneConnectionError) as context:
            self.snapshot_manager.delete("test_snap")

        self.assertIn("Not connected to database", str(context.exception))

    def test_delete_snapshot_execution_error(self):
        """Test deleting snapshot with execution error"""
        self.mock_client.execute = Mock(side_effect=Exception("Database error"))

        with self.assertRaises(SnapshotError) as context:
            self.snapshot_manager.delete("test_snap")

        self.assertIn("Failed to delete snapshot", str(context.exception))

    def test_exists_snapshot_true(self):
        """Test checking if snapshot exists (returns True)"""
        with patch.object(self.snapshot_manager, 'get', return_value=Mock()):
            result = self.snapshot_manager.exists("test_snap")
            self.assertTrue(result)

    def test_exists_snapshot_false(self):
        """Test checking if snapshot exists (returns False)"""
        with patch.object(self.snapshot_manager, 'get', side_effect=SnapshotError("not found")):
            result = self.snapshot_manager.exists("test_snap")
            self.assertFalse(result)


class TestSnapshotManagerIntegration(unittest.TestCase):
    """Integration tests for SnapshotManager"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client._engine = Mock()
        self.snapshot_manager = SnapshotManager(self.mock_client)

    def test_full_workflow(self):
        """Test complete snapshot workflow"""
        # Mock all the necessary methods
        self.mock_client.execute = Mock()

        # Mock get method to return a snapshot
        mock_snapshot = Snapshot("workflow_snap", "table", datetime.now(), database="test_db", table="test_table")
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            # Create snapshot
            created_snapshot = self.snapshot_manager.create("workflow_snap", "table", database="test_db", table="test_table")
            self.assertEqual(created_snapshot.name, "workflow_snap")

            # Check if exists
            with patch.object(self.snapshot_manager, 'exists', return_value=True):
                self.assertTrue(self.snapshot_manager.exists("workflow_snap"))

            # Delete snapshot
            self.snapshot_manager.delete("workflow_snap")
            self.mock_client.execute.assert_called_with("DROP SNAPSHOT workflow_snap")


if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()

    # Add test cases
    test_suite.addTest(unittest.makeSuite(TestSnapshot))
    test_suite.addTest(unittest.makeSuite(TestSnapshotManager))
    # TestSnapshotQueryBuilder removed - SnapshotQueryBuilder is deprecated
    test_suite.addTest(unittest.makeSuite(TestSnapshotManagerIntegration))

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    print(f"{'='*50}")
