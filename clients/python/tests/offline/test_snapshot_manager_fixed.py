"""
Standalone unit tests for SnapshotManager and CloneManager classes
This version doesn't require external dependencies and can be run independently
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
import os


# Store original modules to restore later
_original_modules = {}


def setup_sqlalchemy_mocks():
    """Setup SQLAlchemy mocks for this test class"""
    global _original_modules
    _original_modules['pymysql'] = sys.modules.get('pymysql')
    _original_modules['sqlalchemy'] = sys.modules.get('sqlalchemy')
    _original_modules['sqlalchemy.engine'] = sys.modules.get('sqlalchemy.engine')

    sys.modules['pymysql'] = Mock()
    sys.modules['sqlalchemy'] = Mock()
    sys.modules['sqlalchemy.engine'] = Mock()
    sys.modules['sqlalchemy.engine'].Engine = Mock()


def teardown_sqlalchemy_mocks():
    """Restore original modules"""
    global _original_modules
    for module_name, original_module in _original_modules.items():
        if original_module is not None:
            sys.modules[module_name] = original_module
        elif module_name in sys.modules:
            del sys.modules[module_name]


# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

# Now import the classes we want to test
from matrixone.snapshot import SnapshotManager, Snapshot, CloneManager, SnapshotLevel
from matrixone.exceptions import (
    SnapshotError,
    ConnectionError as MatrixOneConnectionError,
    QueryError,
    CloneError,
)


class TestSnapshot(unittest.TestCase):
    """Test Snapshot class"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        setup_sqlalchemy_mocks()

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        teardown_sqlalchemy_mocks()

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

    def test_snapshot_optional_params(self):
        """Test snapshot creation with optional parameters"""
        now = datetime.now()
        snapshot = Snapshot("test", "cluster", now)

        self.assertEqual(snapshot.name, "test")
        self.assertEqual(snapshot.level, SnapshotLevel.CLUSTER)
        self.assertEqual(snapshot.created_at, now)
        self.assertIsNone(snapshot.description)
        self.assertIsNone(snapshot.database)
        self.assertIsNone(snapshot.table)


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


class TestCloneManager(unittest.TestCase):
    """Test CloneManager functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client._engine = Mock()
        self.mock_client.snapshots = Mock()  # Mock snapshots manager for snapshot verification
        self.clone_manager = CloneManager(self.mock_client)

    def test_clone_database_basic(self):
        """Test basic database clone without snapshot"""
        self.mock_client.execute = Mock()

        self.clone_manager.clone_database("target_db", "source_db")

        expected_sql = "CREATE DATABASE target_db CLONE source_db"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_database_with_if_not_exists(self):
        """Test database clone with IF NOT EXISTS"""
        self.mock_client.execute = Mock()

        self.clone_manager.clone_database("target_db", "source_db", if_not_exists=True)

        expected_sql = "CREATE DATABASE IF NOT EXISTS target_db CLONE source_db"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_database_with_snapshot(self):
        """Test database clone with snapshot"""
        self.mock_client.execute = Mock()

        self.clone_manager.clone_database("target_db", "source_db", snapshot_name="test_snapshot")

        expected_sql = "CREATE DATABASE target_db CLONE source_db {snapshot = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_database_with_snapshot_and_if_not_exists(self):
        """Test database clone with snapshot and IF NOT EXISTS"""
        self.mock_client.execute = Mock()

        self.clone_manager.clone_database("target_db", "source_db", snapshot_name="test_snapshot", if_not_exists=True)

        expected_sql = "CREATE DATABASE IF NOT EXISTS target_db CLONE source_db {snapshot = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_database_not_connected(self):
        """Test database clone when not connected"""
        self.mock_client._engine = None

        with self.assertRaises(MatrixOneConnectionError) as context:
            self.clone_manager.clone_database("target_db", "source_db")

        self.assertIn("Not connected to database", str(context.exception))

    def test_clone_database_execution_error(self):
        """Test database clone with execution error"""
        self.mock_client.execute = Mock(side_effect=Exception("Database error"))

        with self.assertRaises(CloneError) as context:
            self.clone_manager.clone_database("target_db", "source_db")

        self.assertIn("Failed to clone database", str(context.exception))

    def test_clone_table_basic(self):
        """Test basic table clone without snapshot"""
        self.mock_client.execute = Mock()

        self.clone_manager.clone_table("target_table", "source_table")

        expected_sql = "CREATE TABLE target_table CLONE source_table"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_table_with_database_names(self):
        """Test table clone with database names"""
        self.mock_client.execute = Mock()

        self.clone_manager.clone_table("db1.target_table", "db2.source_table")

        expected_sql = "CREATE TABLE db1.target_table CLONE db2.source_table"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_table_with_if_not_exists(self):
        """Test table clone with IF NOT EXISTS"""
        self.mock_client.execute = Mock()

        self.clone_manager.clone_table("target_table", "source_table", if_not_exists=True)

        expected_sql = "CREATE TABLE IF NOT EXISTS target_table CLONE source_table"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_table_with_snapshot(self):
        """Test table clone with snapshot"""
        self.mock_client.execute = Mock()

        self.clone_manager.clone_table("target_table", "source_table", snapshot_name="test_snapshot")

        expected_sql = "CREATE TABLE target_table CLONE source_table {snapshot = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_table_with_snapshot_and_if_not_exists(self):
        """Test table clone with snapshot and IF NOT EXISTS"""
        self.mock_client.execute = Mock()

        self.clone_manager.clone_table("target_table", "source_table", snapshot_name="test_snapshot", if_not_exists=True)

        expected_sql = "CREATE TABLE IF NOT EXISTS target_table CLONE source_table {snapshot = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_table_not_connected(self):
        """Test table clone when not connected"""
        self.mock_client._engine = None

        with self.assertRaises(MatrixOneConnectionError) as context:
            self.clone_manager.clone_table("target_table", "source_table")

        self.assertIn("Not connected to database", str(context.exception))

    def test_clone_table_execution_error(self):
        """Test table clone with execution error"""
        self.mock_client.execute = Mock(side_effect=Exception("Database error"))

        with self.assertRaises(CloneError) as context:
            self.clone_manager.clone_table("target_table", "source_table")

        self.assertIn("Failed to clone table", str(context.exception))

    def test_clone_database_with_snapshot_verification(self):
        """Test clone database with snapshot verification"""
        # Mock snapshots.exists method to return True
        self.mock_client.snapshots.exists.return_value = True
        self.mock_client.execute = Mock()

        self.clone_manager.clone_database_with_snapshot("target_db", "source_db", "test_snapshot")

        expected_sql = "CREATE DATABASE target_db CLONE source_db {snapshot = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_database_with_snapshot_not_exists(self):
        """Test clone database with non-existent snapshot"""
        # Mock snapshots.exists method to return False
        self.mock_client.snapshots.exists.return_value = False

        with self.assertRaises(CloneError) as context:
            self.clone_manager.clone_database_with_snapshot("target_db", "source_db", "non_existent_snapshot")

        self.assertIn("does not exist", str(context.exception))

    def test_clone_table_with_snapshot_verification(self):
        """Test clone table with snapshot verification"""
        # Mock snapshots.exists method to return True
        self.mock_client.snapshots.exists.return_value = True
        self.mock_client.execute = Mock()

        self.clone_manager.clone_table_with_snapshot("target_table", "source_table", "test_snapshot")

        expected_sql = "CREATE TABLE target_table CLONE source_table {snapshot = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_table_with_snapshot_not_exists(self):
        """Test clone table with non-existent snapshot"""
        # Mock snapshots.exists method to return False
        self.mock_client.snapshots.exists.return_value = False

        with self.assertRaises(CloneError) as context:
            self.clone_manager.clone_table_with_snapshot("target_table", "source_table", "non_existent_snapshot")

        self.assertIn("does not exist", str(context.exception))

    def test_clone_database_with_snapshot_and_if_not_exists(self):
        """Test clone database with snapshot and IF NOT EXISTS"""
        self.mock_client.snapshots.exists.return_value = True
        self.mock_client.execute = Mock()

        self.clone_manager.clone_database_with_snapshot("target_db", "source_db", "test_snapshot", if_not_exists=True)

        expected_sql = "CREATE DATABASE IF NOT EXISTS target_db CLONE source_db {snapshot = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)

    def test_clone_table_with_snapshot_and_if_not_exists(self):
        """Test clone table with snapshot and IF NOT EXISTS"""
        self.mock_client.snapshots.exists.return_value = True
        self.mock_client.execute = Mock()

        self.clone_manager.clone_table_with_snapshot("target_table", "source_table", "test_snapshot", if_not_exists=True)

        expected_sql = "CREATE TABLE IF NOT EXISTS target_table CLONE source_table {snapshot = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)


if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()

    # Add test cases using TestLoader
    loader = unittest.TestLoader()
    test_suite.addTests(loader.loadTestsFromTestCase(TestSnapshot))
    test_suite.addTests(loader.loadTestsFromTestCase(TestSnapshotManager))
    test_suite.addTests(loader.loadTestsFromTestCase(TestCloneManager))

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    if result.testsRun > 0:
        success_rate = (result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100
        print(f"Success rate: {success_rate:.1f}%")
    print(f"{'='*50}")

    # Exit with appropriate code
    if result.failures or result.errors:
        sys.exit(1)
    else:
        sys.exit(0)
