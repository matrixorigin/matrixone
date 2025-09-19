"""
Test snapshot and clone operations within transactions
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
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

from matrixone.snapshot import SnapshotManager, CloneManager
from matrixone.client import TransactionWrapper, TransactionSnapshotManager, TransactionCloneManager
from matrixone.exceptions import SnapshotError, CloneError


class TestTransactionSnapshot(unittest.TestCase):
    """Test snapshot and clone operations within transactions"""
    
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
