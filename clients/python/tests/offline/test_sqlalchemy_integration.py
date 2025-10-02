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
Test SQLAlchemy integration with MatrixOne (without database connection)
"""

import unittest
from unittest.mock import Mock, patch
from datetime import datetime
import sys
import os

# Store original modules to restore later
_original_modules = {}


def setup_sqlalchemy_mocks():
    """Setup SQLAlchemy mocks for this test class"""
    global _original_modules

    # Store original modules
    _original_modules['pymysql'] = sys.modules.get('pymysql')
    _original_modules['sqlalchemy'] = sys.modules.get('sqlalchemy')
    _original_modules['sqlalchemy.engine'] = sys.modules.get('sqlalchemy.engine')
    _original_modules['sqlalchemy.orm'] = sys.modules.get('sqlalchemy.orm')

    # Mock the external dependencies
    sys.modules['pymysql'] = Mock()
    sys.modules['sqlalchemy'] = Mock()
    sys.modules['sqlalchemy.engine'] = Mock()
    sys.modules['sqlalchemy.engine'].Engine = Mock()
    sys.modules['sqlalchemy.orm'] = Mock()
    sys.modules['sqlalchemy.orm'].sessionmaker = Mock()
    sys.modules['sqlalchemy.orm'].declarative_base = Mock()
    sys.modules['sqlalchemy'].create_engine = Mock()
    sys.modules['sqlalchemy'].text = Mock()
    sys.modules['sqlalchemy'].Column = Mock()
    sys.modules['sqlalchemy'].Integer = Mock()
    sys.modules['sqlalchemy'].String = Mock()
    sys.modules['sqlalchemy'].DateTime = Mock()


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

from matrixone.snapshot import SnapshotLevel, Snapshot, SnapshotManager, CloneManager
from matrixone.client import (
    Client,
    TransactionWrapper,
    TransactionSnapshotManager,
    TransactionCloneManager,
)
from matrixone.exceptions import SnapshotError, CloneError


class TestSQLAlchemyIntegration(unittest.TestCase):
    """Test SQLAlchemy integration concepts"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        setup_sqlalchemy_mocks()

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        teardown_sqlalchemy_mocks()

    def test_enum_usage(self):
        """Test SnapshotLevel enum usage"""
        # Test enum values
        self.assertEqual(SnapshotLevel.CLUSTER.value, "cluster")
        self.assertEqual(SnapshotLevel.ACCOUNT.value, "account")
        self.assertEqual(SnapshotLevel.DATABASE.value, "database")
        self.assertEqual(SnapshotLevel.TABLE.value, "table")

        # Test enum creation from string
        self.assertEqual(SnapshotLevel("cluster"), SnapshotLevel.CLUSTER)
        self.assertEqual(SnapshotLevel("table"), SnapshotLevel.TABLE)

    def test_snapshot_with_enum(self):
        """Test Snapshot creation with enum"""
        now = datetime.now()
        snapshot = Snapshot("test_snap", SnapshotLevel.TABLE, now, database="test_db", table="test_table")

        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.TABLE)
        self.assertEqual(snapshot.database, "test_db")
        self.assertEqual(snapshot.table, "test_table")

    def test_snapshot_manager_with_enum(self):
        """Test SnapshotManager with enum"""
        mock_client = Mock()
        mock_client._connection = Mock()
        snapshot_manager = SnapshotManager(mock_client)

        # Mock the execute method
        mock_client.execute = Mock()
        mock_client.execute.return_value = Mock()

        # Mock get method to return a snapshot
        mock_snapshot = Snapshot("test_snap", SnapshotLevel.TABLE, datetime.now(), database="test_db", table="test_table")
        with patch.object(snapshot_manager, 'get', return_value=mock_snapshot):
            result = snapshot_manager.create("test_snap", SnapshotLevel.TABLE, database="test_db", table="test_table")

            # Verify the correct SQL was generated
            expected_sql = "CREATE SNAPSHOT test_snap FOR TABLE test_db test_table"
            mock_client.execute.assert_called_once_with(expected_sql)
            self.assertEqual(result, mock_snapshot)

    def test_clone_manager_with_enum(self):
        """Test CloneManager with enum"""
        mock_client = Mock()
        mock_client._connection = Mock()
        clone_manager = CloneManager(mock_client)

        # Mock the execute method
        mock_client.execute = Mock()

        # Test database clone
        clone_manager.clone_database("target_db", "source_db")

        expected_sql = "CREATE DATABASE target_db CLONE source_db"
        mock_client.execute.assert_called_once_with(expected_sql)

    def test_transaction_wrapper(self):
        """Test TransactionWrapper with snapshots and clones"""
        mock_client = Mock()
        mock_client._connection = Mock()
        mock_tx = TransactionWrapper(mock_client._connection, mock_client)

        # Test that transaction wrapper has snapshot and clone managers
        self.assertIsInstance(mock_tx.snapshots, TransactionSnapshotManager)
        self.assertIsInstance(mock_tx.clone, TransactionCloneManager)

    def test_transaction_snapshot_manager(self):
        """Test TransactionSnapshotManager"""
        mock_client = Mock()
        mock_client._connection = Mock()
        mock_tx = Mock()
        mock_tx.execute = Mock()

        tx_snapshots = TransactionSnapshotManager(mock_client, mock_tx)

        # Mock get method
        mock_snapshot = Snapshot("test_snap", SnapshotLevel.CLUSTER, datetime.now())
        with patch.object(tx_snapshots, 'get', return_value=mock_snapshot):
            result = tx_snapshots.create("test_snap", SnapshotLevel.CLUSTER)

            # Verify that the transaction's execute was called
            mock_tx.execute.assert_called_once_with("CREATE SNAPSHOT test_snap FOR CLUSTER")

    def test_transaction_clone_manager(self):
        """Test TransactionCloneManager"""
        mock_client = Mock()
        mock_client._connection = Mock()
        mock_tx = Mock()
        mock_tx.execute = Mock()

        tx_clone = TransactionCloneManager(mock_client, mock_tx)

        # Test database clone
        tx_clone.clone_database("target_db", "source_db")

        # Verify that the transaction's execute was called
        mock_tx.execute.assert_called_once_with("CREATE DATABASE target_db CLONE source_db")

    def test_sqlalchemy_integration_pattern(self):
        """Test the integration pattern without actual SQLAlchemy"""

        # Simulate SQLAlchemy session
        class MockSession:
            def __init__(self):
                self.data = []
                self.committed = False
                self.rolled_back = False

            def add(self, item):
                self.data.append(item)

            def commit(self):
                self.committed = True

            def rollback(self):
                self.rolled_back = True
                self.data.clear()

            def close(self):
                pass

        # Simulate MatrixOne client
        mock_client = Mock()
        mock_client._connection = Mock()

        # Test the integration pattern
        session = MockSession()

        try:
            # Simulate MatrixOne transaction
            with patch.object(mock_client, 'transaction') as mock_transaction:
                mock_tx = Mock()
                mock_tx.execute = Mock()
                mock_tx.snapshots = Mock()
                mock_tx.clone = Mock()

                mock_transaction.return_value.__enter__ = Mock(return_value=mock_tx)
                mock_transaction.return_value.__exit__ = Mock(return_value=None)

                with mock_client.transaction() as tx:
                    # Simulate SQLAlchemy operations
                    session.add("user1")
                    session.add("user2")
                    session.commit()

                    # Simulate MatrixOne operations
                    tx.snapshots.create("test_snapshot", SnapshotLevel.DATABASE, database="test")
                    tx.clone.clone_database("backup", "test")

                # Verify SQLAlchemy operations
                self.assertTrue(session.committed)
                self.assertEqual(len(session.data), 2)

                # Verify MatrixOne operations were called
                tx.snapshots.create.assert_called_once()
                tx.clone.clone_database.assert_called_once()

        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def test_error_handling_pattern(self):
        """Test error handling pattern"""

        class MockSession:
            def __init__(self):
                self.data = []
                self.committed = False
                self.rolled_back = False

            def add(self, item):
                if item == "error_item":
                    raise Exception("Simulated error")
                self.data.append(item)

            def commit(self):
                self.committed = True

            def rollback(self):
                self.rolled_back = True
                self.data.clear()

            def close(self):
                pass

        mock_client = Mock()
        mock_client._connection = Mock()

        session = MockSession()

        try:
            with patch.object(mock_client, 'transaction') as mock_transaction:
                mock_tx = Mock()
                mock_tx.execute = Mock()
                mock_tx.snapshots = Mock()

                mock_transaction.return_value.__enter__ = Mock(return_value=mock_tx)
                mock_transaction.return_value.__exit__ = Mock(return_value=None)

                with mock_client.transaction() as tx:
                    # This will cause an error
                    session.add("error_item")
                    session.commit()

                    # This should not be reached
                    tx.snapshots.create("should_not_exist", SnapshotLevel.DATABASE, database="test")

        except Exception as e:
            session.rollback()
            # Verify rollback occurred
            self.assertTrue(session.rolled_back)
            self.assertEqual(len(session.data), 0)

        finally:
            session.close()

    def test_batch_operation_pattern(self):
        """Test batch operation pattern"""

        class MockSession:
            def __init__(self):
                self.data = []
                self.committed = False

            def bulk_insert_mappings(self, model, data_list):
                self.data.extend(data_list)

            def commit(self):
                self.committed = True

            def close(self):
                pass

        mock_client = Mock()
        mock_client._connection = Mock()

        session = MockSession()

        try:
            # Simulate batch data
            batch_data = [{"name": f"User_{i}", "email": f"user{i}@example.com"} for i in range(1, 11)]  # 10 users

            # Batch insert
            session.bulk_insert_mappings("User", batch_data)
            session.commit()

            # Create snapshot
            mock_client.execute = Mock()
            snapshot_manager = SnapshotManager(mock_client)
            mock_snapshot = Snapshot("batch_snapshot", SnapshotLevel.DATABASE, datetime.now(), database="test")
            with patch.object(snapshot_manager, 'get', return_value=mock_snapshot):
                result = snapshot_manager.create("batch_snapshot", SnapshotLevel.DATABASE, database="test")

            # Verify batch operation
            self.assertTrue(session.committed)
            self.assertEqual(len(session.data), 10)

        finally:
            session.close()


if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()

    # Add test cases using TestLoader
    loader = unittest.TestLoader()
    test_suite.addTests(loader.loadTestsFromTestCase(TestSQLAlchemyIntegration))

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
