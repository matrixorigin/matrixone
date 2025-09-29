"""
Test snapshot and clone operations within transactions
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

from matrixone.snapshot import SnapshotManager, CloneManager
from matrixone.client import TransactionWrapper, TransactionSnapshotManager, TransactionCloneManager
from matrixone.exceptions import SnapshotError, CloneError


class TestTransactionSnapshot(unittest.TestCase):
    """Test snapshot and clone operations within transactions"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        setup_sqlalchemy_mocks()

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        teardown_sqlalchemy_mocks()

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client._connection = Mock()
        self.snapshot_manager = SnapshotManager(self.mock_client)
        self.clone_manager = CloneManager(self.mock_client)

    def test_snapshot_create_in_transaction(self):
        """Test creating snapshot within a transaction"""
        # Create a real transaction wrapper
        mock_tx = TransactionWrapper(self.mock_client._connection, self.mock_client)
        mock_tx.execute = Mock()

        # Mock the get method to return a snapshot
        mock_snapshot = Mock()
        with patch.object(self.snapshot_manager, 'get', return_value=mock_snapshot):
            # Create snapshot within transaction
            result = self.snapshot_manager.create("test_snap", "cluster", executor=mock_tx)

            # Verify that the transaction's execute was called
            mock_tx.execute.assert_called_once_with("CREATE SNAPSHOT test_snap FOR CLUSTER")

    def test_clone_in_transaction(self):
        """Test clone operations within a transaction"""
        # Create a real transaction wrapper
        mock_tx = TransactionWrapper(self.mock_client._connection, self.mock_client)
        mock_tx.execute = Mock()

        # Clone database within transaction
        self.clone_manager.clone_database("target_db", "source_db", executor=mock_tx)

        # Verify that the transaction's execute was called
        mock_tx.execute.assert_called_once_with("CREATE DATABASE target_db CLONE source_db")

    def test_transaction_snapshot_manager(self):
        """Test TransactionSnapshotManager"""
        # Create a real transaction wrapper
        mock_tx = TransactionWrapper(self.mock_client._connection, self.mock_client)
        mock_tx.execute = Mock()

        # Create transaction snapshot manager
        tx_snapshots = TransactionSnapshotManager(self.mock_client, mock_tx)

        # Mock the get method to return a snapshot
        mock_snapshot = Mock()
        with patch.object(tx_snapshots, 'get', return_value=mock_snapshot):
            # Create snapshot using transaction snapshot manager
            result = tx_snapshots.create("test_snap", "cluster")

            # Verify that the transaction's execute was called
            mock_tx.execute.assert_called_once_with("CREATE SNAPSHOT test_snap FOR CLUSTER")

    def test_transaction_clone_manager(self):
        """Test TransactionCloneManager"""
        # Create a real transaction wrapper
        mock_tx = TransactionWrapper(self.mock_client._connection, self.mock_client)
        mock_tx.execute = Mock()

        # Create transaction clone manager
        tx_clone = TransactionCloneManager(self.mock_client, mock_tx)

        # Clone database using transaction clone manager
        tx_clone.clone_database("target_db", "source_db")

        # Verify that the transaction's execute was called
        mock_tx.execute.assert_called_once_with("CREATE DATABASE target_db CLONE source_db")


if __name__ == '__main__':
    unittest.main()
