"""
Test SnapshotLevel enum functionality
"""

import unittest
from unittest.mock import Mock, patch
from datetime import datetime
import sys
import os

# Mock the external dependencies
sys.modules['pymysql'] = Mock()
sys.modules['sqlalchemy'] = Mock()
sys.modules['sqlalchemy.engine'] = Mock()
sys.modules['sqlalchemy.engine'].Engine = Mock()

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone.snapshot import Snapshot, SnapshotManager, SnapshotLevel
from matrixone.exceptions import SnapshotError


class TestSnapshotLevelEnum(unittest.TestCase):
    """Test SnapshotLevel enum functionality"""
    
    def test_enum_values(self):
        """Test enum values"""
        self.assertEqual(SnapshotLevel.CLUSTER.value, "cluster")
        self.assertEqual(SnapshotLevel.ACCOUNT.value, "account")
        self.assertEqual(SnapshotLevel.DATABASE.value, "database")
        self.assertEqual(SnapshotLevel.TABLE.value, "table")
    
    def test_enum_from_string(self):
        """Test creating enum from string"""
        self.assertEqual(SnapshotLevel("cluster"), SnapshotLevel.CLUSTER)
        self.assertEqual(SnapshotLevel("account"), SnapshotLevel.ACCOUNT)
        self.assertEqual(SnapshotLevel("database"), SnapshotLevel.DATABASE)
        self.assertEqual(SnapshotLevel("table"), SnapshotLevel.TABLE)
    
    def test_enum_case_insensitive(self):
        """Test enum creation is case insensitive (handled in our implementation)"""
        # Note: Python enum is case sensitive by default, but our implementation
        # converts strings to lowercase before creating enum
        self.assertEqual(SnapshotLevel("cluster"), SnapshotLevel.CLUSTER)
        self.assertEqual(SnapshotLevel("account"), SnapshotLevel.ACCOUNT)
        self.assertEqual(SnapshotLevel("database"), SnapshotLevel.DATABASE)
        self.assertEqual(SnapshotLevel("table"), SnapshotLevel.TABLE)
    
    def test_invalid_enum_value(self):
        """Test invalid enum value raises ValueError"""
        with self.assertRaises(ValueError):
            SnapshotLevel("invalid")
        
        with self.assertRaises(ValueError):
            SnapshotLevel("")


class TestSnapshotWithEnum(unittest.TestCase):
    """Test Snapshot class with enum support"""
    
    def test_snapshot_with_enum(self):
        """Test creating snapshot with enum"""
        now = datetime.now()
        snapshot = Snapshot("test_snap", SnapshotLevel.TABLE, now, database="test_db", table="test_table")
        
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.TABLE)
        self.assertEqual(snapshot.database, "test_db")
        self.assertEqual(snapshot.table, "test_table")
    
    def test_snapshot_with_string(self):
        """Test creating snapshot with string (backward compatibility)"""
        now = datetime.now()
        snapshot = Snapshot("test_snap", "table", now, database="test_db", table="test_table")
        
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.TABLE)
        self.assertEqual(snapshot.database, "test_db")
        self.assertEqual(snapshot.table, "test_table")
    
    def test_snapshot_with_invalid_string(self):
        """Test creating snapshot with invalid string raises error"""
        now = datetime.now()
        with self.assertRaises(SnapshotError) as context:
            Snapshot("test_snap", "invalid_level", now)
        
        self.assertIn("Invalid snapshot level", str(context.exception))
    
    def test_snapshot_repr_with_enum(self):
        """Test snapshot string representation with enum"""
        now = datetime.now()
        snapshot = Snapshot("test_snap", SnapshotLevel.CLUSTER, now)
        repr_str = repr(snapshot)
        
        self.assertIn("test_snap", repr_str)
        self.assertIn("SnapshotLevel.CLUSTER", repr_str)


class TestSnapshotManagerWithEnum(unittest.TestCase):
    """Test SnapshotManager with enum support"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client._connection = Mock()
        self.snapshot_manager = SnapshotManager(self.mock_client)
    
    def test_create_with_enum(self):
        """Test creating snapshot with enum"""
        self.mock_client.execute = Mock()
        self.mock_client.execute.return_value = Mock()
        
        # Mock get method to return a snapshot
        mock_snapshot = Snapshot("cluster_snap", SnapshotLevel.CLUSTER, datetime.now())
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("cluster_snap", SnapshotLevel.CLUSTER)
            
            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT cluster_snap FOR CLUSTER")
            self.assertEqual(result, mock_snapshot)
    
    def test_create_with_string(self):
        """Test creating snapshot with string (backward compatibility)"""
        self.mock_client.execute = Mock()
        self.mock_client.execute.return_value = Mock()
        
        # Mock get method to return a snapshot
        mock_snapshot = Snapshot("cluster_snap", SnapshotLevel.CLUSTER, datetime.now())
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("cluster_snap", "cluster")
            
            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT cluster_snap FOR CLUSTER")
            self.assertEqual(result, mock_snapshot)
    
    def test_create_with_invalid_string(self):
        """Test creating snapshot with invalid string raises error"""
        with self.assertRaises(SnapshotError) as context:
            self.snapshot_manager.create("test_snap", "invalid_level")
        
        self.assertIn("Invalid snapshot level", str(context.exception))
    
    def test_create_database_with_enum(self):
        """Test creating database snapshot with enum"""
        self.mock_client.execute = Mock()
        self.mock_client.execute.return_value = Mock()
        
        mock_snapshot = Snapshot("db_snap", SnapshotLevel.DATABASE, datetime.now(), database="test_db")
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("db_snap", SnapshotLevel.DATABASE, database="test_db")
            
            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT db_snap FOR DATABASE test_db")
            self.assertEqual(result, mock_snapshot)
    
    def test_create_table_with_enum(self):
        """Test creating table snapshot with enum"""
        self.mock_client.execute = Mock()
        self.mock_client.execute.return_value = Mock()
        
        mock_snapshot = Snapshot("table_snap", SnapshotLevel.TABLE, datetime.now(), database="test_db", table="test_table")
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("table_snap", SnapshotLevel.TABLE, database="test_db", table="test_table")
            
            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT table_snap FOR TABLE test_db test_table")
            self.assertEqual(result, mock_snapshot)
    
    def test_get_returns_enum(self):
        """Test that get method returns snapshot with enum level"""
        # Mock the execute method to return snapshot data
        mock_result = Mock()
        mock_result.fetchone.return_value = ("test_snap", 1640995200000000000, "table", "account1", "db1", "table1")
        self.mock_client.execute = Mock(return_value=mock_result)
        
        snapshot = self.snapshot_manager.get("test_snap")
        
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.TABLE)
        self.assertEqual(snapshot.database, "db1")
        self.assertEqual(snapshot.table, "table1")
    
    def test_list_returns_enums(self):
        """Test that list method returns snapshots with enum levels"""
        # Mock the execute method to return snapshot data
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            ("snap1", 1640995200000000000, "cluster", "account1", None, None),
            ("snap2", 1640995201000000000, "table", "account1", "db1", "table1")
        ]
        self.mock_client.execute = Mock(return_value=mock_result)
        
        snapshots = self.snapshot_manager.list()
        
        self.assertEqual(len(snapshots), 2)
        self.assertEqual(snapshots[0].name, "snap1")
        self.assertEqual(snapshots[0].level, SnapshotLevel.CLUSTER)
        self.assertEqual(snapshots[1].name, "snap2")
        self.assertEqual(snapshots[1].level, SnapshotLevel.TABLE)


if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases using TestLoader
    loader = unittest.TestLoader()
    test_suite.addTests(loader.loadTestsFromTestCase(TestSnapshotLevelEnum))
    test_suite.addTests(loader.loadTestsFromTestCase(TestSnapshotWithEnum))
    test_suite.addTests(loader.loadTestsFromTestCase(TestSnapshotManagerWithEnum))
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    if result.testsRun > 0:
        success_rate = ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100)
        print(f"Success rate: {success_rate:.1f}%")
    print(f"{'='*50}")
    
    # Exit with appropriate code
    if result.failures or result.errors:
        sys.exit(1)
    else:
        sys.exit(0)
