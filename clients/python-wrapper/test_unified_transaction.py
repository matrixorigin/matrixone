"""
Test unified transaction integration between SQLAlchemy and MatrixOne
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
sys.modules['sqlalchemy.orm'] = Mock()
sys.modules['sqlalchemy.orm'].sessionmaker = Mock()
sys.modules['sqlalchemy.orm'].declarative_base = Mock()
sys.modules['sqlalchemy'] = Mock()
sys.modules['sqlalchemy'].create_engine = Mock()
sys.modules['sqlalchemy'].text = Mock()
sys.modules['sqlalchemy'].Column = Mock()
sys.modules['sqlalchemy'].Integer = Mock()
sys.modules['sqlalchemy'].String = Mock()
sys.modules['sqlalchemy'].DateTime = Mock()

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone.snapshot import SnapshotLevel, Snapshot, SnapshotManager, CloneManager
from matrixone.client import Client, TransactionWrapper, TransactionSnapshotManager, TransactionCloneManager
from matrixone.exceptions import SnapshotError, CloneError


class TestUnifiedTransaction(unittest.TestCase):
    """Test unified transaction integration"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client._connection = Mock()
        self.mock_client._connection_params = {
            'user': 'root',
            'password': '111',
            'host': 'localhost',
            'port': 6001,
            'database': 'test'
        }
        self.mock_client.execute = Mock()
        self.mock_client._snapshots = Mock()
        self.mock_client._clone = Mock()
    
    def test_transaction_wrapper_sqlalchemy_integration(self):
        """Test TransactionWrapper SQLAlchemy integration"""
        # Mock SQLAlchemy components
        mock_engine = Mock()
        mock_session = Mock()
        mock_session_factory = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.create_engine', return_value=mock_engine), \
             patch('sqlalchemy.orm.sessionmaker', return_value=mock_session_factory):
            
            # Create TransactionWrapper
            tx_wrapper = TransactionWrapper(self.mock_client._connection, self.mock_client)
            
            # Test get_sqlalchemy_session
            session = tx_wrapper.get_sqlalchemy_session()
            
            # Verify SQLAlchemy components were created
            self.assertEqual(session, mock_session)
            mock_session.begin.assert_called_once()
            
            # Test commit
            tx_wrapper.commit_sqlalchemy()
            mock_session.commit.assert_called_once()
            
            # Test rollback
            tx_wrapper.rollback_sqlalchemy()
            mock_session.rollback.assert_called_once()
            
            # Test close
            tx_wrapper.close_sqlalchemy()
            mock_session.close.assert_called_once()
            mock_engine.dispose.assert_called_once()
    
    def test_unified_transaction_flow(self):
        """Test unified transaction flow"""
        # Mock connection
        mock_connection = Mock()
        mock_connection.begin = Mock()
        mock_connection.commit = Mock()
        mock_connection.rollback = Mock()
        
        # Mock SQLAlchemy components
        mock_engine = Mock()
        mock_session = Mock()
        mock_session_factory = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.create_engine', return_value=mock_engine), \
             patch('sqlalchemy.orm.sessionmaker', return_value=mock_session_factory):
            
            # Create client
            client = Client()
            client._connection = mock_connection
            client._connection_params = {
                'user': 'root',
                'password': '111',
                'host': 'localhost',
                'port': 6001,
                'database': 'test'
            }
            client._snapshots = Mock()
            client._clone = Mock()
            
            # Test successful transaction
            with client.transaction() as tx:
                # Get SQLAlchemy session
                session = tx.get_sqlalchemy_session()
                
                # Simulate SQLAlchemy operations
                session.add = Mock()
                session.commit = Mock()
                
                # Simulate MatrixOne operations
                tx.snapshots.create = Mock(return_value=Snapshot("test_snap", SnapshotLevel.DATABASE, datetime.now()))
                tx.clone.clone_database = Mock()
                
                # Perform operations
                session.add("user")
                session.commit()
                tx.snapshots.create("test_snap", SnapshotLevel.DATABASE, database="test")
                tx.clone.clone_database("target", "source")
            
            # Verify transaction flow
            mock_connection.begin.assert_called_once()
            mock_session.begin.assert_called_once()
            # session.commit is called twice: once in the test and once in commit_sqlalchemy
            self.assertEqual(mock_session.commit.call_count, 2)
            mock_connection.commit.assert_called_once()
            mock_session.close.assert_called_once()
            mock_engine.dispose.assert_called_once()
    
    def test_unified_transaction_rollback(self):
        """Test unified transaction rollback on error"""
        # Mock connection
        mock_connection = Mock()
        mock_connection.begin = Mock()
        mock_connection.commit = Mock()
        mock_connection.rollback = Mock()
        
        # Mock SQLAlchemy components
        mock_engine = Mock()
        mock_session = Mock()
        mock_session_factory = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.create_engine', return_value=mock_engine), \
             patch('sqlalchemy.orm.sessionmaker', return_value=mock_session_factory):
            
            # Create client
            client = Client()
            client._connection = mock_connection
            client._connection_params = {
                'user': 'root',
                'password': '111',
                'host': 'localhost',
                'port': 6001,
                'database': 'test'
            }
            client._snapshots = Mock()
            client._clone = Mock()
            
            # Test transaction with error
            with self.assertRaises(Exception):
                with client.transaction() as tx:
                    # Get SQLAlchemy session
                    session = tx.get_sqlalchemy_session()
                    
                    # Simulate SQLAlchemy operations
                    session.add = Mock()
                    session.commit = Mock()
                    
                    # Simulate MatrixOne operations
                    tx.snapshots.create = Mock(return_value=Snapshot("test_snap", SnapshotLevel.DATABASE, datetime.now()))
                    
                    # Perform operations
                    session.add("user")
                    session.commit()
                    tx.snapshots.create("test_snap", SnapshotLevel.DATABASE, database="test")
                    
                    # Simulate error
                    raise Exception("Simulated error")
            
            # Verify rollback flow
            mock_connection.begin.assert_called_once()
            mock_session.begin.assert_called_once()
            mock_session.rollback.assert_called_once()
            mock_connection.rollback.assert_called_once()
            mock_session.close.assert_called_once()
            mock_engine.dispose.assert_called_once()
    
    def test_sqlalchemy_session_reuse(self):
        """Test SQLAlchemy session reuse within transaction"""
        # Mock SQLAlchemy components
        mock_engine = Mock()
        mock_session = Mock()
        mock_session_factory = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.create_engine', return_value=mock_engine) as mock_create_engine, \
             patch('sqlalchemy.orm.sessionmaker', return_value=mock_session_factory):
            
            # Create TransactionWrapper
            tx_wrapper = TransactionWrapper(self.mock_client._connection, self.mock_client)
            
            # Get session multiple times
            session1 = tx_wrapper.get_sqlalchemy_session()
            session2 = tx_wrapper.get_sqlalchemy_session()
            
            # Should return the same session
            self.assertEqual(session1, session2)
            self.assertEqual(session1, mock_session)
            
            # Engine should only be created once
            self.assertEqual(mock_create_engine.call_count, 1)
    
    def test_transaction_snapshot_manager_integration(self):
        """Test TransactionSnapshotManager integration"""
        # Mock transaction wrapper
        mock_tx = Mock()
        mock_tx.execute = Mock()
        
        # Create TransactionSnapshotManager
        tx_snapshots = TransactionSnapshotManager(self.mock_client, mock_tx)
        
        # Mock get method
        mock_snapshot = Snapshot("test_snap", SnapshotLevel.CLUSTER, datetime.now())
        with patch.object(tx_snapshots, 'get', return_value=mock_snapshot):
            result = tx_snapshots.create("test_snap", SnapshotLevel.CLUSTER)
            
            # Verify that the transaction's execute was called
            mock_tx.execute.assert_called_once_with("CREATE SNAPSHOT test_snap FOR CLUSTER")
            self.assertEqual(result, mock_snapshot)
    
    def test_transaction_clone_manager_integration(self):
        """Test TransactionCloneManager integration"""
        # Mock transaction wrapper
        mock_tx = Mock()
        mock_tx.execute = Mock()
        
        # Create TransactionCloneManager
        tx_clone = TransactionCloneManager(self.mock_client, mock_tx)
        
        # Test database clone
        tx_clone.clone_database("target_db", "source_db")
        
        # Verify that the transaction's execute was called
        mock_tx.execute.assert_called_once_with("CREATE DATABASE target_db CLONE source_db")
    
    def test_mixed_operations_pattern(self):
        """Test mixed SQLAlchemy and MatrixOne operations pattern"""
        # Mock connection
        mock_connection = Mock()
        mock_connection.begin = Mock()
        mock_connection.commit = Mock()
        mock_connection.rollback = Mock()
        
        # Mock SQLAlchemy components
        mock_engine = Mock()
        mock_session = Mock()
        mock_session_factory = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.create_engine', return_value=mock_engine), \
             patch('sqlalchemy.orm.sessionmaker', return_value=mock_session_factory):
            
            # Create client
            client = Client()
            client._connection = mock_connection
            client._connection_params = {
                'user': 'root',
                'password': '111',
                'host': 'localhost',
                'port': 6001,
                'database': 'test'
            }
            client._snapshots = Mock()
            client._clone = Mock()
            
            # Test mixed operations
            with client.transaction() as tx:
                # SQLAlchemy operations
                session = tx.get_sqlalchemy_session()
                session.add = Mock()
                session.flush = Mock()
                session.commit = Mock()
                
                # MatrixOne operations
                tx.snapshots.create = Mock(return_value=Snapshot("mixed_snap", SnapshotLevel.DATABASE, datetime.now()))
                tx.clone.clone_database_with_snapshot = Mock()
                
                # Perform mixed operations
                session.add("user1")
                session.flush()
                
                snapshot = tx.snapshots.create("mixed_snap", SnapshotLevel.DATABASE, database="test")
                
                tx.clone.clone_database_with_snapshot("backup", "test", "mixed_snap")
                
                session.add("user2")
                session.commit()
            
            # Verify all operations were called
            session.add.assert_any_call("user1")
            session.add.assert_any_call("user2")
            session.flush.assert_called_once()
            # session.commit is called twice: once in the test and once in commit_sqlalchemy
            self.assertEqual(session.commit.call_count, 2)
            tx.snapshots.create.assert_called_once()
            tx.clone.clone_database_with_snapshot.assert_called_once()
    
    def test_error_handling_pattern(self):
        """Test error handling pattern in unified transaction"""
        # Mock connection
        mock_connection = Mock()
        mock_connection.begin = Mock()
        mock_connection.commit = Mock()
        mock_connection.rollback = Mock()
        
        # Mock SQLAlchemy components
        mock_engine = Mock()
        mock_session = Mock()
        mock_session_factory = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.create_engine', return_value=mock_engine), \
             patch('sqlalchemy.orm.sessionmaker', return_value=mock_session_factory):
            
            # Create client
            client = Client()
            client._connection = mock_connection
            client._connection_params = {
                'user': 'root',
                'password': '111',
                'host': 'localhost',
                'port': 6001,
                'database': 'test'
            }
            client._snapshots = Mock()
            client._clone = Mock()
            
            # Test error handling
            with self.assertRaises(Exception):
                with client.transaction() as tx:
                    # SQLAlchemy operations
                    session = tx.get_sqlalchemy_session()
                    session.add = Mock()
                    session.commit = Mock(side_effect=Exception("SQLAlchemy error"))
                    
                    # MatrixOne operations
                    tx.snapshots.create = Mock(return_value=Snapshot("error_snap", SnapshotLevel.DATABASE, datetime.now()))
                    
                    # Perform operations
                    session.add("user")
                    session.commit()  # This will raise an exception
                    
                    # This should not be reached
                    tx.snapshots.create("error_snap", SnapshotLevel.DATABASE, database="test")
            
            # Verify rollback occurred
            mock_session.rollback.assert_called_once()
            mock_connection.rollback.assert_called_once()
    
    def test_connection_string_generation(self):
        """Test connection string generation for SQLAlchemy"""
        # Mock SQLAlchemy components
        mock_engine = Mock()
        mock_session = Mock()
        mock_session_factory = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.create_engine', return_value=mock_engine) as mock_create_engine, \
             patch('sqlalchemy.orm.sessionmaker', return_value=mock_session_factory):
            
            # Create TransactionWrapper
            tx_wrapper = TransactionWrapper(self.mock_client._connection, self.mock_client)
            
            # Get SQLAlchemy session
            tx_wrapper.get_sqlalchemy_session()
            
            # Verify connection string
            expected_connection_string = "mysql+pymysql://root:111@localhost:6001/test"
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0]
            self.assertEqual(call_args[0], expected_connection_string)


if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases using TestLoader
    loader = unittest.TestLoader()
    test_suite.addTests(loader.loadTestsFromTestCase(TestUnifiedTransaction))
    
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
