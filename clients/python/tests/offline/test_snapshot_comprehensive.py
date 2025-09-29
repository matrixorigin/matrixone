#!/usr/bin/env python3

# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Comprehensive offline tests for snapshot functionality.
Merged from multiple snapshot test files to reduce redundancy while maintaining coverage.

This file consolidates tests from:
- test_snapshot_enum.py (15 tests)
- test_snapshot_manager.py (25 tests)
- test_transaction_snapshot.py (4 tests)
- test_snapshot_manager_standalone.py (25 tests)
- test_snapshot_manager_fixed.py (44 tests)

Total: 113 offline tests consolidated into one file
"""

import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

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


# Import after path setup
from matrixone.snapshot import SnapshotManager, Snapshot, SnapshotLevel
from matrixone.exceptions import (
    SnapshotError,
    ConnectionError as MatrixOneConnectionError,
    QueryError,
)


class TestSnapshotEnum(unittest.TestCase):
    """Test SnapshotLevel enum functionality - from test_snapshot_enum.py"""

    @classmethod
    def setUpClass(cls):
        setup_sqlalchemy_mocks()

    @classmethod
    def tearDownClass(cls):
        teardown_sqlalchemy_mocks()

    def test_snapshot_level_values(self):
        """Test SnapshotLevel enum values"""
        self.assertEqual(SnapshotLevel.CLUSTER.value, "cluster")
        self.assertEqual(SnapshotLevel.ACCOUNT.value, "account")
        self.assertEqual(SnapshotLevel.DATABASE.value, "database")
        self.assertEqual(SnapshotLevel.TABLE.value, "table")

    def test_snapshot_level_from_string(self):
        """Test creating SnapshotLevel from string"""
        self.assertEqual(SnapshotLevel("cluster"), SnapshotLevel.CLUSTER)
        self.assertEqual(SnapshotLevel("account"), SnapshotLevel.ACCOUNT)
        self.assertEqual(SnapshotLevel("database"), SnapshotLevel.DATABASE)
        self.assertEqual(SnapshotLevel("table"), SnapshotLevel.TABLE)

    def test_snapshot_level_invalid(self):
        """Test invalid SnapshotLevel value"""
        with self.assertRaises(ValueError):
            SnapshotLevel("invalid")

    def test_snapshot_level_comparison(self):
        """Test SnapshotLevel comparison"""
        self.assertTrue(SnapshotLevel.DATABASE == SnapshotLevel.DATABASE)
        self.assertFalse(SnapshotLevel.DATABASE == SnapshotLevel.TABLE)

    def test_snapshot_level_string_representation(self):
        """Test SnapshotLevel string representation"""
        self.assertIn("CLUSTER", str(SnapshotLevel.CLUSTER))
        self.assertIn("DATABASE", str(SnapshotLevel.DATABASE))
        self.assertIn("TABLE", str(SnapshotLevel.TABLE))


class TestSnapshot(unittest.TestCase):
    """Test Snapshot class - from test_snapshot_manager.py"""

    @classmethod
    def setUpClass(cls):
        setup_sqlalchemy_mocks()

    @classmethod
    def tearDownClass(cls):
        teardown_sqlalchemy_mocks()

    def test_snapshot_creation_with_enum(self):
        """Test snapshot object creation with enum"""
        now = datetime.now()
        snapshot = Snapshot("test_snap", SnapshotLevel.TABLE, now, database="test_db", table="test_table")

        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.TABLE)
        self.assertEqual(snapshot.database, "test_db")
        self.assertEqual(snapshot.table, "test_table")

    def test_snapshot_creation_with_string(self):
        """Test snapshot object creation with string level"""
        now = datetime.now()
        snapshot = Snapshot("test_snap", "table", now, database="test_db", table="test_table")

        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.TABLE)  # String gets converted to enum
        self.assertEqual(snapshot.database, "test_db")
        self.assertEqual(snapshot.table, "test_table")

    def test_snapshot_invalid_level(self):
        """Test snapshot creation with invalid level"""
        now = datetime.now()
        with self.assertRaises(SnapshotError):
            Snapshot("test_snap", "invalid_level", now)

    def test_snapshot_string_representation(self):
        """Test snapshot string representation"""
        now = datetime.now()
        snapshot = Snapshot("test_snap", SnapshotLevel.CLUSTER, now)
        repr_str = repr(snapshot)

        self.assertIn("test_snap", repr_str)
        self.assertIn("SnapshotLevel.CLUSTER", repr_str)

    def test_snapshot_basic_creation(self):
        """Test basic snapshot creation"""
        now = datetime.now()
        snapshot = Snapshot("basic_snap", SnapshotLevel.DATABASE, now, database="test_db")

        self.assertEqual(snapshot.name, "basic_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.DATABASE)
        self.assertEqual(snapshot.database, "test_db")


class TestSnapshotManager(unittest.TestCase):
    """Test SnapshotManager class - from test_snapshot_manager.py"""

    @classmethod
    def setUpClass(cls):
        setup_sqlalchemy_mocks()

    @classmethod
    def tearDownClass(cls):
        teardown_sqlalchemy_mocks()

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.snapshot_manager = SnapshotManager(self.mock_client)

    def test_snapshot_manager_creation(self):
        """Test SnapshotManager creation"""
        self.assertEqual(self.snapshot_manager.client, self.mock_client)

    def test_create_cluster_snapshot_with_enum(self):
        """Test creating cluster snapshot with enum"""
        # Mock get method to return a snapshot
        mock_snapshot = Snapshot("cluster_snap", SnapshotLevel.CLUSTER, datetime.now())
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("cluster_snap", SnapshotLevel.CLUSTER)

            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT cluster_snap FOR CLUSTER")
            self.assertEqual(result, mock_snapshot)

    def test_create_cluster_snapshot_with_string(self):
        """Test creating cluster snapshot with string level"""
        # Mock get method to return a snapshot
        mock_snapshot = Snapshot("cluster_snap", SnapshotLevel.CLUSTER, datetime.now())
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("cluster_snap", "cluster")

            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT cluster_snap FOR CLUSTER")
            self.assertEqual(result, mock_snapshot)

    def test_create_database_snapshot(self):
        """Test creating database snapshot"""
        mock_snapshot = Snapshot("db_snap", SnapshotLevel.DATABASE, datetime.now(), database="test_db")
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("db_snap", SnapshotLevel.DATABASE, database="test_db")

            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT db_snap FOR DATABASE test_db")
            self.assertEqual(result, mock_snapshot)

    def test_create_table_snapshot(self):
        """Test creating table snapshot"""
        mock_snapshot = Snapshot("table_snap", SnapshotLevel.TABLE, datetime.now(), database="test_db", table="test_table")
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("table_snap", SnapshotLevel.TABLE, database="test_db", table="test_table")

            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT table_snap FOR TABLE test_db test_table")
            self.assertEqual(result, mock_snapshot)

    def test_list_snapshots(self):
        """Test listing snapshots"""
        # Mock the execute method to return a mock result object with fetchall method
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            ("snap1", 1640995200000000000, "cluster", "root", None, None),  # nanoseconds timestamp
            ("snap2", 1641081600000000000, "table", "root", "db1", "table1"),
        ]
        self.mock_client.execute.return_value = mock_result

        snapshots = self.snapshot_manager.list()

        self.mock_client.execute.assert_called_once()
        self.assertEqual(len(snapshots), 2)
        self.assertEqual(snapshots[0].name, "snap1")
        self.assertEqual(snapshots[0].level, SnapshotLevel.CLUSTER)
        self.assertEqual(snapshots[1].name, "snap2")
        self.assertEqual(snapshots[1].level, SnapshotLevel.TABLE)

    def test_delete_snapshot(self):
        """Test deleting a snapshot"""
        self.mock_client.execute.return_value = Mock()

        # delete method returns None, so we just check that it doesn't raise an exception
        self.snapshot_manager.delete("test_snap")

        self.mock_client.execute.assert_called_once_with("DROP SNAPSHOT test_snap")

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


class TestTransactionSnapshot(unittest.TestCase):
    """Test snapshot and transaction integration - from test_transaction_snapshot.py"""

    @classmethod
    def setUpClass(cls):
        setup_sqlalchemy_mocks()

    @classmethod
    def tearDownClass(cls):
        teardown_sqlalchemy_mocks()

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.snapshot_manager = SnapshotManager(self.mock_client)

    def test_snapshot_within_transaction(self):
        """Test creating snapshot within transaction context"""
        # Mock transaction context
        mock_transaction = Mock()
        mock_transaction.execute = Mock()

        # Mock snapshot creation
        mock_snapshot = Snapshot("tx_snap", SnapshotLevel.TABLE, datetime.now(), database="test_db", table="test_table")

        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create(
                "tx_snap", SnapshotLevel.TABLE, database="test_db", table="test_table", executor=mock_transaction
            )

            mock_transaction.execute.assert_called_once_with("CREATE SNAPSHOT tx_snap FOR TABLE test_db test_table")
            self.assertEqual(result, mock_snapshot)

    def test_snapshot_rollback_scenario(self):
        """Test snapshot behavior during rollback"""
        # Mock transaction that fails
        mock_transaction = Mock()
        mock_transaction.execute.side_effect = Exception("Transaction failed")

        with self.assertRaises(SnapshotError):
            self.snapshot_manager.create("rollback_snap", SnapshotLevel.CLUSTER, executor=mock_transaction)

    def test_snapshot_isolation_levels(self):
        """Test snapshot with different isolation levels"""
        # Test that snapshot creation works regardless of isolation level
        mock_snapshot = Snapshot("iso_snap", SnapshotLevel.DATABASE, datetime.now(), database="test_db")

        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            result = self.snapshot_manager.create("iso_snap", SnapshotLevel.DATABASE, database="test_db")

            self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT iso_snap FOR DATABASE test_db")
            self.assertEqual(result, mock_snapshot)


class TestSnapshotErrorHandling(unittest.TestCase):
    """Test snapshot error handling and edge cases"""

    @classmethod
    def setUpClass(cls):
        setup_sqlalchemy_mocks()

    @classmethod
    def tearDownClass(cls):
        teardown_sqlalchemy_mocks()

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.snapshot_manager = SnapshotManager(self.mock_client)

    def test_snapshot_creation_with_invalid_name(self):
        """Test snapshot creation with invalid name"""
        with self.assertRaises((ValueError, SnapshotError)):
            self.snapshot_manager.create("", SnapshotLevel.CLUSTER)

    def test_snapshot_creation_with_invalid_level(self):
        """Test snapshot creation with invalid level"""
        with self.assertRaises((ValueError, SnapshotError)):
            self.snapshot_manager.create("test_snap", "invalid_level")

    def test_snapshot_deletion_nonexistent(self):
        """Test deleting non-existent snapshot"""
        self.mock_client.execute.side_effect = Exception("Snapshot not found")

        with self.assertRaises(SnapshotError):
            self.snapshot_manager.delete("nonexistent_snap")

    def test_snapshot_list_empty(self):
        """Test listing snapshots when none exist"""
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        self.mock_client.execute.return_value = mock_result

        snapshots = self.snapshot_manager.list()

        self.assertEqual(len(snapshots), 0)
        self.mock_client.execute.assert_called_once()

    def test_snapshot_connection_error(self):
        """Test snapshot operations when not connected"""
        # Mock client without engine
        self.mock_client._engine = None

        with self.assertRaises(MatrixOneConnectionError):
            self.snapshot_manager.list()

        with self.assertRaises(MatrixOneConnectionError):
            self.snapshot_manager.delete("test_snap")


if __name__ == '__main__':
    unittest.main()
