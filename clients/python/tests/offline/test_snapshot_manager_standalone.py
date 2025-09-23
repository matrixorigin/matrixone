"""
Standalone unit tests for SnapshotManager class
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
from matrixone.snapshot import SnapshotManager, Snapshot, SnapshotQueryBuilder, SnapshotLevel, CloneManager
from matrixone.exceptions import CloneError, ConnectionError as MatrixOneConnectionError, QueryError, SnapshotError


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
            table="test_table"
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
            ("snap1", 1640995200000000000, "cluster", "account1", None, None),  # nanoseconds timestamp
            ("snap2", 1640995201000000000, "table", "account1", "db1", "table1")
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
        mock_result.fetchone.return_value = ("test_snap", 1640995200000000000, "table", "account1", "db1", "table1")
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


class TestSnapshotQueryBuilder(unittest.TestCase):
    """Test SnapshotQueryBuilder class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.builder = SnapshotQueryBuilder("test_snapshot", self.mock_client)
    
    def test_init(self):
        """Test SnapshotQueryBuilder initialization"""
        self.assertEqual(self.builder.snapshot_name, "test_snapshot")
        self.assertEqual(self.builder.client, self.mock_client)
        self.assertEqual(self.builder._select_columns, [])
        self.assertIsNone(self.builder._from_table)
        self.assertEqual(self.builder._joins, [])
        self.assertEqual(self.builder._where_conditions, [])
        self.assertEqual(self.builder._where_params, [])
        self.assertEqual(self.builder._group_by_columns, [])
        self.assertEqual(self.builder._having_conditions, [])
        self.assertEqual(self.builder._having_params, [])
        self.assertEqual(self.builder._order_by_columns, [])
        self.assertIsNone(self.builder._limit_count)
        self.assertIsNone(self.builder._offset_count)
    
    def test_select(self):
        """Test adding SELECT columns"""
        result = self.builder.select("col1", "col2", "col3")
        
        self.assertEqual(self.builder._select_columns, ["col1", "col2", "col3"])
        self.assertEqual(result, self.builder)  # Test method chaining
    
    def test_from_table(self):
        """Test setting FROM table"""
        result = self.builder.from_table("test_table")
        
        self.assertEqual(self.builder._from_table, "test_table")
        self.assertEqual(result, self.builder)  # Test method chaining
    
    def test_join(self):
        """Test adding JOIN"""
        result = self.builder.join("table2", "table1.id = table2.id")
        
        self.assertEqual(self.builder._joins, ["JOIN table2 ON table1.id = table2.id"])
        self.assertEqual(result, self.builder)  # Test method chaining
    
    def test_left_join(self):
        """Test adding LEFT JOIN"""
        result = self.builder.left_join("table2", "table1.id = table2.id")
        
        self.assertEqual(self.builder._joins, ["LEFT JOIN table2 ON table1.id = table2.id"])
        self.assertEqual(result, self.builder)  # Test method chaining
    
    def test_right_join(self):
        """Test adding RIGHT JOIN"""
        result = self.builder.right_join("table2", "table1.id = table2.id")
        
        self.assertEqual(self.builder._joins, ["RIGHT JOIN table2 ON table1.id = table2.id"])
        self.assertEqual(result, self.builder)  # Test method chaining
    
    def test_where(self):
        """Test adding WHERE conditions"""
        result = self.builder.where("col1 > ?", 100).where("col2 = ?", "test")
        
        self.assertEqual(self.builder._where_conditions, ["col1 > ?", "col2 = ?"])
        self.assertEqual(self.builder._where_params, [100, "test"])
        self.assertEqual(result, self.builder)  # Test method chaining
    
    def test_group_by(self):
        """Test adding GROUP BY columns"""
        result = self.builder.group_by("col1", "col2")
        
        self.assertEqual(self.builder._group_by_columns, ["col1", "col2"])
        self.assertEqual(result, self.builder)  # Test method chaining
    
    def test_having(self):
        """Test adding HAVING conditions"""
        result = self.builder.having("COUNT(*) > ?", 5)
        
        self.assertEqual(self.builder._having_conditions, ["COUNT(*) > ?"])
        self.assertEqual(self.builder._having_params, [5])
        self.assertEqual(result, self.builder)  # Test method chaining
    
    def test_order_by(self):
        """Test adding ORDER BY columns"""
        result = self.builder.order_by("col1", "col2 DESC")
        
        self.assertEqual(self.builder._order_by_columns, ["col1", "col2 DESC"])
        self.assertEqual(result, self.builder)  # Test method chaining
    
    def test_limit(self):
        """Test setting LIMIT"""
        result = self.builder.limit(10)
        
        self.assertEqual(self.builder._limit_count, 10)
        self.assertEqual(result, self.builder)  # Test method chaining
    
    def test_offset(self):
        """Test setting OFFSET"""
        result = self.builder.offset(20)
        
        self.assertEqual(self.builder._offset_count, 20)
        self.assertEqual(result, self.builder)  # Test method chaining
    
    def test_execute_simple_query(self):
        """Test executing a simple query"""
        self.builder.select("col1", "col2").from_table("test_table")
        
        mock_result = Mock()
        self.mock_client.execute = Mock(return_value=mock_result)
        
        result = self.builder.execute()
        
        expected_sql = "SELECT col1, col2 FROM test_table {SNAPSHOT = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql, None)
        self.assertEqual(result, mock_result)
    
    def test_execute_complex_query(self):
        """Test executing a complex query with all clauses"""
        (self.builder
         .select("col1", "col2", "COUNT(*)")
         .from_table("test_table")
         .join("table2", "test_table.id = table2.id")
         .where("col1 > ?", 100)
         .group_by("col1")
         .having("COUNT(*) > ?", 5)
         .order_by("col1", "col2 DESC")
         .limit(10)
         .offset(20))
        
        mock_result = Mock()
        self.mock_client.execute = Mock(return_value=mock_result)
        
        result = self.builder.execute()
        
        expected_sql = ("SELECT col1, col2, COUNT(*) FROM test_table "
                       "JOIN table2 ON test_table.id = table2.id "
                       "WHERE col1 > ? "
                       "GROUP BY col1 "
                       "HAVING COUNT(*) > ? "
                       "ORDER BY col1, col2 DESC "
                       "LIMIT 10 "
                       "OFFSET 20 "
                       "{SNAPSHOT = 'test_snapshot'}")
        
        self.mock_client.execute.assert_called_once_with(expected_sql, (100, 5))
        self.assertEqual(result, mock_result)
    
    def test_execute_no_select_columns(self):
        """Test executing query without SELECT columns"""
        self.builder.from_table("test_table")
        
        with self.assertRaises(SnapshotError) as context:
            self.builder.execute()
        
        self.assertIn("No SELECT columns specified", str(context.exception))
    
    def test_execute_no_from_table(self):
        """Test executing query without FROM table"""
        self.builder.select("col1")
        
        with self.assertRaises(SnapshotError) as context:
            self.builder.execute()
        
        self.assertIn("No FROM table specified", str(context.exception))
    
    def test_method_chaining(self):
        """Test that all methods return self for chaining"""
        result = (self.builder
                 .select("col1")
                 .from_table("table1")
                 .where("col1 > ?", 1)
                 .group_by("col1")
                 .having("COUNT(*) > ?", 0)
                 .order_by("col1")
                 .limit(10)
                 .offset(5))
        
        self.assertEqual(result, self.builder)


class TestCloneManager(unittest.TestCase):
    """Test CloneManager functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client._engine = Mock()
        self.mock_client.snapshots = Mock()  # Mock snapshots manager for snapshot verification
        self.snapshot_manager = CloneManager(self.mock_client)
    
    def test_clone_database_basic(self):
        """Test basic database clone without snapshot"""
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_database("target_db", "source_db")
        
        expected_sql = "CREATE DATABASE target_db CLONE source_db"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_database_with_if_not_exists(self):
        """Test database clone with IF NOT EXISTS"""
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_database("target_db", "source_db", if_not_exists=True)
        
        expected_sql = "CREATE DATABASE IF NOT EXISTS target_db CLONE source_db"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_database_with_snapshot(self):
        """Test database clone with snapshot"""
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_database("target_db", "source_db", snapshot_name="test_snapshot")
        
        expected_sql = "CREATE DATABASE target_db CLONE source_db {SNAPSHOT = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_database_with_snapshot_and_if_not_exists(self):
        """Test database clone with snapshot and IF NOT EXISTS"""
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_database("target_db", "source_db", 
                                           snapshot_name="test_snapshot", if_not_exists=True)
        
        expected_sql = "CREATE DATABASE IF NOT EXISTS target_db CLONE source_db {SNAPSHOT = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_database_not_connected(self):
        """Test database clone when not connected"""
        self.mock_client._engine = None
        
        with self.assertRaises(MatrixOneConnectionError) as context:
            self.snapshot_manager.clone_database("target_db", "source_db")
        
        self.assertIn("Not connected to database", str(context.exception))
    
    def test_clone_database_execution_error(self):
        """Test database clone with execution error"""
        self.mock_client.execute = Mock(side_effect=Exception("Database error"))
        
        with self.assertRaises(CloneError) as context:
            self.snapshot_manager.clone_database("target_db", "source_db")
        
        self.assertIn("Failed to clone database", str(context.exception))
    
    def test_clone_table_basic(self):
        """Test basic table clone without snapshot"""
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_table("target_table", "source_table")
        
        expected_sql = "CREATE TABLE target_table CLONE source_table"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_table_with_database_names(self):
        """Test table clone with database names"""
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_table("db1.target_table", "db2.source_table")
        
        expected_sql = "CREATE TABLE db1.target_table CLONE db2.source_table"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_table_with_if_not_exists(self):
        """Test table clone with IF NOT EXISTS"""
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_table("target_table", "source_table", if_not_exists=True)
        
        expected_sql = "CREATE TABLE IF NOT EXISTS target_table CLONE source_table"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_table_with_snapshot(self):
        """Test table clone with snapshot"""
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_table("target_table", "source_table", snapshot_name="test_snapshot")
        
        expected_sql = "CREATE TABLE target_table CLONE source_table {SNAPSHOT = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_table_with_snapshot_and_if_not_exists(self):
        """Test table clone with snapshot and IF NOT EXISTS"""
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_table("target_table", "source_table", 
                                        snapshot_name="test_snapshot", if_not_exists=True)
        
        expected_sql = "CREATE TABLE IF NOT EXISTS target_table CLONE source_table {SNAPSHOT = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_table_not_connected(self):
        """Test table clone when not connected"""
        self.mock_client._engine = None
        
        with self.assertRaises(MatrixOneConnectionError) as context:
            self.snapshot_manager.clone_table("target_table", "source_table")
        
        self.assertIn("Not connected to database", str(context.exception))
    
    def test_clone_table_execution_error(self):
        """Test table clone with execution error"""
        self.mock_client.execute = Mock(side_effect=Exception("Database error"))
        
        with self.assertRaises(CloneError) as context:
            self.snapshot_manager.clone_table("target_table", "source_table")
        
        self.assertIn("Failed to clone table", str(context.exception))
    
    def test_clone_database_with_snapshot_verification(self):
        """Test clone database with snapshot verification"""
        # Mock snapshots.exists method to return True
        self.mock_client.snapshots.exists.return_value = True
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_database_with_snapshot("target_db", "source_db", "test_snapshot")
        
        expected_sql = "CREATE DATABASE target_db CLONE source_db {SNAPSHOT = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_database_with_snapshot_not_exists(self):
        """Test clone database with non-existent snapshot"""
        # Mock snapshots.exists method to return False
        self.mock_client.snapshots.exists.return_value = False
        
        with self.assertRaises(CloneError) as context:
            self.snapshot_manager.clone_database_with_snapshot("target_db", "source_db", "non_existent_snapshot")
        
        self.assertIn("does not exist", str(context.exception))
    
    def test_clone_table_with_snapshot_verification(self):
        """Test clone table with snapshot verification"""
        # Mock snapshots.exists method to return True
        self.mock_client.snapshots.exists.return_value = True
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_table_with_snapshot("target_table", "source_table", "test_snapshot")
        
        expected_sql = "CREATE TABLE target_table CLONE source_table {SNAPSHOT = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_table_with_snapshot_not_exists(self):
        """Test clone table with non-existent snapshot"""
        # Mock snapshots.exists method to return False
        self.mock_client.snapshots.exists.return_value = False
        
        with self.assertRaises(CloneError) as context:
            self.snapshot_manager.clone_table_with_snapshot("target_table", "source_table", "non_existent_snapshot")
        
        self.assertIn("does not exist", str(context.exception))
    
    def test_clone_database_with_snapshot_and_if_not_exists(self):
        """Test clone database with snapshot and IF NOT EXISTS"""
        self.mock_client.snapshots.exists.return_value = True
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_database_with_snapshot("target_db", "source_db", 
                                                         "test_snapshot", if_not_exists=True)
        
        expected_sql = "CREATE DATABASE IF NOT EXISTS target_db CLONE source_db {SNAPSHOT = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)
    
    def test_clone_table_with_snapshot_and_if_not_exists(self):
        """Test clone table with snapshot and IF NOT EXISTS"""
        self.mock_client.snapshots.exists.return_value = True
        self.mock_client.execute = Mock()
        
        self.snapshot_manager.clone_table_with_snapshot("target_table", "source_table", 
                                                      "test_snapshot", if_not_exists=True)
        
        expected_sql = "CREATE TABLE IF NOT EXISTS target_table CLONE source_table {SNAPSHOT = 'test_snapshot'}"
        self.mock_client.execute.assert_called_once_with(expected_sql)


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
    
    def test_snapshot_levels_validation(self):
        """Test validation of different snapshot levels"""
        valid_levels = ["cluster", "account", "database", "table"]
        
        for level in valid_levels:
            self.mock_client.execute = Mock()
            mock_snapshot = Snapshot(f"test_{level}", level, datetime.now())
            
            with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
                if level == "database":
                    result = self.snapshot_manager.create(f"test_{level}", level, database="test_db")
                elif level == "table":
                    result = self.snapshot_manager.create(f"test_{level}", level, database="test_db", table="test_table")
                else:
                    result = self.snapshot_manager.create(f"test_{level}", level)
                
                # Convert string level to enum for comparison
                expected_level = SnapshotLevel(level)
                self.assertEqual(result.level, expected_level)
    
    def test_error_propagation(self):
        """Test that errors are properly propagated"""
        # Test connection error
        self.mock_client._engine = None
        with self.assertRaises(MatrixOneConnectionError):
            self.snapshot_manager.create("test", "cluster")
        
        # Test execution error
        self.mock_client._engine = Mock()
        self.mock_client.execute = Mock(side_effect=Exception("DB Error"))
        with self.assertRaises(SnapshotError):
            self.snapshot_manager.create("test", "cluster")


if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases using TestLoader
    loader = unittest.TestLoader()
    test_suite.addTests(loader.loadTestsFromTestCase(TestSnapshot))
    test_suite.addTests(loader.loadTestsFromTestCase(TestSnapshotManager))
    test_suite.addTests(loader.loadTestsFromTestCase(TestSnapshotQueryBuilder))
    test_suite.addTests(loader.loadTestsFromTestCase(TestCloneManager))
    test_suite.addTests(loader.loadTestsFromTestCase(TestSnapshotManagerIntegration))
    
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
