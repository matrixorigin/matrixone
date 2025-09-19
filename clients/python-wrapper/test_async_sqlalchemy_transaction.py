"""
Test Async SQLAlchemy Transaction Integration
"""

import unittest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
import sys
import os

# Mock the external dependencies
sys.modules['pymysql'] = Mock()
sys.modules['aiomysql'] = Mock()
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

from matrixone.async_client import AsyncClient, AsyncTransactionWrapper
from matrixone.snapshot import SnapshotLevel
from matrixone.exceptions import ConnectionError


class TestAsyncSQLAlchemyTransaction(unittest.TestCase):
    """Test Async SQLAlchemy Transaction Integration"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncClient()
        self.mock_connection = AsyncMock()
        self.client._connection = self.mock_connection
    
    async def test_transaction_wrapper_sqlalchemy_session(self):
        """Test transaction wrapper SQLAlchemy session creation"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        
        # Mock SQLAlchemy components
        mock_session = Mock()
        mock_engine = Mock()
        mock_async_sessionmaker = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.ext.asyncio.async_sessionmaker', mock_async_sessionmaker), \
             patch('sqlalchemy.ext.asyncio.create_async_engine', return_value=mock_engine):
            
            session = await tx_wrapper.get_sqlalchemy_session()
            
            self.assertEqual(session, mock_session)
            await mock_session.begin.assert_called_once()
    
    async def test_transaction_wrapper_sqlalchemy_session_reuse(self):
        """Test transaction wrapper SQLAlchemy session reuse"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        
        # Mock SQLAlchemy components
        mock_session = Mock()
        mock_engine = Mock()
        mock_async_sessionmaker = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.ext.asyncio.async_sessionmaker', mock_async_sessionmaker), \
             patch('sqlalchemy.ext.asyncio.create_async_engine', return_value=mock_engine):
            
            # First call
            session1 = await tx_wrapper.get_sqlalchemy_session()
            
            # Second call should return the same session
            session2 = await tx_wrapper.get_sqlalchemy_session()
            
            self.assertEqual(session1, session2)
            self.assertEqual(session1, mock_session)
            # begin() should only be called once
            await mock_session.begin.assert_called_once()
    
    async def test_transaction_wrapper_commit_sqlalchemy(self):
        """Test transaction wrapper SQLAlchemy commit"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        
        # Mock SQLAlchemy session
        mock_session = Mock()
        tx_wrapper._sqlalchemy_session = mock_session
        
        await tx_wrapper.commit_sqlalchemy()
        
        await mock_session.commit.assert_called_once()
    
    async def test_transaction_wrapper_rollback_sqlalchemy(self):
        """Test transaction wrapper SQLAlchemy rollback"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        
        # Mock SQLAlchemy session
        mock_session = Mock()
        tx_wrapper._sqlalchemy_session = mock_session
        
        await tx_wrapper.rollback_sqlalchemy()
        
        await mock_session.rollback.assert_called_once()
    
    async def test_transaction_wrapper_close_sqlalchemy(self):
        """Test transaction wrapper SQLAlchemy close"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        
        # Mock SQLAlchemy session and engine
        mock_session = Mock()
        mock_engine = Mock()
        tx_wrapper._sqlalchemy_session = mock_session
        tx_wrapper._sqlalchemy_engine = mock_engine
        
        await tx_wrapper.close_sqlalchemy()
        
        await mock_session.close.assert_called_once()
        await mock_engine.dispose.assert_called_once()
        self.assertIsNone(tx_wrapper._sqlalchemy_session)
        self.assertIsNone(tx_wrapper._sqlalchemy_engine)
    
    async def test_transaction_success_flow(self):
        """Test successful transaction flow"""
        mock_cursor = AsyncMock()
        self.mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_cursor.description = None
        mock_cursor.rowcount = 1
        
        # Mock SQLAlchemy components
        mock_session = Mock()
        mock_engine = Mock()
        mock_async_sessionmaker = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.ext.asyncio.async_sessionmaker', mock_async_sessionmaker), \
             patch('sqlalchemy.ext.asyncio.create_async_engine', return_value=mock_engine):
            
            async with self.client.transaction() as tx:
                # Test SQLAlchemy session
                session = await tx.get_sqlalchemy_session()
                self.assertEqual(session, mock_session)
                
                # Test MatrixOne async operations
                result = await tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
                self.assertIsNotNone(result)
                
                # Test snapshot operations
                snapshot = await tx.snapshots.create(
                    "test_snapshot",
                    SnapshotLevel.DATABASE,
                    database="test"
                )
                self.assertIsNotNone(snapshot)
                
                # Test clone operations
                await tx.clone.clone_database("backup", "test")
            
            # Verify commit order
            await mock_session.commit.assert_called_once()
            self.mock_connection.commit.assert_called_once()
    
    async def test_transaction_rollback_flow(self):
        """Test transaction rollback flow"""
        mock_cursor = AsyncMock()
        self.mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Query failed")
        
        # Mock SQLAlchemy components
        mock_session = Mock()
        mock_engine = Mock()
        mock_async_sessionmaker = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.ext.asyncio.async_sessionmaker', mock_async_sessionmaker), \
             patch('sqlalchemy.ext.asyncio.create_async_engine', return_value=mock_engine):
            
            with self.assertRaises(Exception):
                async with self.client.transaction() as tx:
                    session = await tx.get_sqlalchemy_session()
                    await tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
            
            # Verify rollback order
            await mock_session.rollback.assert_called_once()
            self.mock_connection.rollback.assert_called_once()
    
    async def test_transaction_sqlalchemy_session_not_connected(self):
        """Test SQLAlchemy session creation when not connected"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        self.client._connection_params = {}
        
        with self.assertRaises(ConnectionError):
            await tx_wrapper.get_sqlalchemy_session()
    
    async def test_transaction_connection_string_generation(self):
        """Test connection string generation for SQLAlchemy"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        
        # Set up connection parameters
        self.client._connection_params = {
            'user': 'testuser',
            'password': 'testpass',
            'host': 'localhost',
            'port': 6001,
            'db': 'testdb'
        }
        
        mock_session = Mock()
        mock_engine = Mock()
        mock_async_sessionmaker = Mock(return_value=mock_session)
        mock_create_engine = Mock(return_value=mock_engine)
        
        with patch('sqlalchemy.orm.sessionmaker', mock_sessionmaker), \
             patch('sqlalchemy.create_engine', mock_create_engine):
            
            await tx_wrapper.get_sqlalchemy_session()
            
            # Verify create_engine was called with correct connection string
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            self.assertIn('mysql+aiomysql://testuser:testpass@localhost:6001/testdb', call_args)


class TestAsyncSQLAlchemyIntegration(unittest.TestCase):
    """Test Async SQLAlchemy Integration Patterns"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncClient()
        self.mock_connection = AsyncMock()
        self.client._connection = self.mock_connection
    
    async def test_mixed_operations_pattern(self):
        """Test mixed SQLAlchemy and MatrixOne operations"""
        mock_cursor = AsyncMock()
        self.mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [(1, 'Alice'), (2, 'Bob')]
        
        # Mock SQLAlchemy components
        mock_session = Mock()
        mock_engine = Mock()
        mock_async_sessionmaker = Mock(return_value=mock_session)
        
        # Mock SQLAlchemy model
        mock_user = Mock()
        mock_user.id = 1
        mock_user.name = "Alice"
        mock_user.email = "alice@example.com"
        mock_session.query.return_value.all.return_value = [mock_user]
        
        with patch('sqlalchemy.ext.asyncio.async_sessionmaker', mock_async_sessionmaker), \
             patch('sqlalchemy.ext.asyncio.create_async_engine', return_value=mock_engine):
            
            async with self.client.transaction() as tx:
                session = await tx.get_sqlalchemy_session()
                
                # SQLAlchemy operations
                users = session.query(mock_user).all()
                self.assertEqual(len(users), 1)
                
                # MatrixOne async operations
                result = await tx.execute("SELECT id, name FROM users")
                self.assertEqual(len(result.rows), 2)
                
                # Snapshot operations
                snapshot = await tx.snapshots.create(
                    "mixed_snapshot",
                    SnapshotLevel.DATABASE,
                    database="test"
                )
                self.assertIsNotNone(snapshot)
                
                # Clone operations
                await tx.clone.clone_database("mixed_backup", "test")
    
    async def test_error_handling_pattern(self):
        """Test error handling in mixed operations"""
        mock_cursor = AsyncMock()
        self.mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Database error")
        
        # Mock SQLAlchemy components
        mock_session = Mock()
        mock_engine = Mock()
        mock_async_sessionmaker = Mock(return_value=mock_session)
        
        with patch('sqlalchemy.ext.asyncio.async_sessionmaker', mock_async_sessionmaker), \
             patch('sqlalchemy.ext.asyncio.create_async_engine', return_value=mock_engine):
            
            with self.assertRaises(Exception):
                async with self.client.transaction() as tx:
                    session = await tx.get_sqlalchemy_session()
                    
                    # SQLAlchemy operation (should succeed)
                    mock_user = Mock()
                    session.add(mock_user)
                    session.flush()
                    
                    # MatrixOne operation (should fail)
                    await tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
            
            # Verify rollback was called
            await mock_session.rollback.assert_called_once()
            self.mock_connection.rollback.assert_called_once()


def run_async_test(test_func):
    """Helper function to run async tests"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(test_func())
    finally:
        loop.close()


if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases using TestLoader
    loader = unittest.TestLoader()
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncSQLAlchemyTransaction))
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncSQLAlchemyIntegration))
    
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
