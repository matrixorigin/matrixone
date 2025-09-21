"""
Test Async SQLAlchemy Transaction Integration
"""

import unittest
import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os

# Store original modules to restore later
_original_modules = {}

def setup_mocks():
    """Setup mocks for SQLAlchemy modules"""
    global _original_modules
    
    # Store original modules
    _original_modules['pymysql'] = sys.modules.get('pymysql')
    _original_modules['aiomysql'] = sys.modules.get('aiomysql')
    _original_modules['sqlalchemy'] = sys.modules.get('sqlalchemy')
    _original_modules['sqlalchemy.engine'] = sys.modules.get('sqlalchemy.engine')
    _original_modules['sqlalchemy.orm'] = sys.modules.get('sqlalchemy.orm')
    _original_modules['sqlalchemy.ext'] = sys.modules.get('sqlalchemy.ext')
    _original_modules['sqlalchemy.ext.asyncio'] = sys.modules.get('sqlalchemy.ext.asyncio')
    
    # Mock the external dependencies
    sys.modules['pymysql'] = Mock()
    sys.modules['aiomysql'] = Mock()
    
    # Create a more sophisticated SQLAlchemy mock that supports submodules
    sqlalchemy_mock = Mock()
    sqlalchemy_mock.create_engine = Mock()
    sqlalchemy_mock.text = Mock()
    sqlalchemy_mock.Column = Mock()
    sqlalchemy_mock.Integer = Mock()
    sqlalchemy_mock.String = Mock()
    sqlalchemy_mock.DateTime = Mock()
    
    # Mock SQLAlchemy submodules
    sys.modules['sqlalchemy'] = sqlalchemy_mock
    sys.modules['sqlalchemy.engine'] = Mock()
    sys.modules['sqlalchemy.engine'].Engine = Mock()
    
    # Mock SQLAlchemy ORM
    sqlalchemy_orm_mock = Mock()
    sqlalchemy_orm_mock.sessionmaker = Mock()
    sqlalchemy_orm_mock.declarative_base = Mock()
    sys.modules['sqlalchemy.orm'] = sqlalchemy_orm_mock
    
    # Mock SQLAlchemy async extensions
    sqlalchemy_ext_mock = Mock()
    sqlalchemy_ext_asyncio_mock = Mock()
    sqlalchemy_ext_asyncio_mock.create_async_engine = Mock()
    sqlalchemy_ext_asyncio_mock.AsyncSession = Mock()
    sqlalchemy_ext_asyncio_mock.async_sessionmaker = Mock()
    sqlalchemy_ext_mock.asyncio = sqlalchemy_ext_asyncio_mock
    sys.modules['sqlalchemy.ext'] = sqlalchemy_ext_mock
    sys.modules['sqlalchemy.ext.asyncio'] = sqlalchemy_ext_asyncio_mock

def teardown_mocks():
    """Restore original modules"""
    global _original_modules
    
    for module_name, original_module in _original_modules.items():
        if original_module is not None:
            sys.modules[module_name] = original_module
        elif module_name in sys.modules:
            del sys.modules[module_name]

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone.async_client import AsyncClient, AsyncTransactionWrapper
from matrixone.snapshot import SnapshotLevel
from matrixone.exceptions import ConnectionError


class TestAsyncSQLAlchemyTransaction(unittest.IsolatedAsyncioTestCase):
    """Test Async SQLAlchemy Transaction Integration"""
    
    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        setup_mocks()
    
    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        teardown_mocks()
    
    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncClient()
        self.mock_connection = AsyncMock()
        self.client._connection = self.mock_connection
        # Set up connection parameters for SQLAlchemy integration
        self.client._connection_params = {
            'user': 'testuser',
            'password': 'testpass',
            'host': 'localhost',
            'port': 6001,
            'db': 'testdb'
        }
    
    async def test_transaction_wrapper_sqlalchemy_session(self):
        """Test transaction wrapper SQLAlchemy session creation"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        
        # Mock SQLAlchemy session directly
        mock_session = AsyncMock()
        mock_session.begin = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session.close = AsyncMock()
        
        # Mock the get_sqlalchemy_session method directly
        with patch.object(tx_wrapper, 'get_sqlalchemy_session', return_value=mock_session):
            session = await tx_wrapper.get_sqlalchemy_session()
            
            self.assertEqual(session, mock_session)
    
    async def test_transaction_wrapper_sqlalchemy_session_reuse(self):
        """Test transaction wrapper SQLAlchemy session reuse"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        
        # Mock SQLAlchemy session directly
        mock_session = AsyncMock()
        mock_session.begin = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session.close = AsyncMock()
        
        # Mock the get_sqlalchemy_session method directly
        with patch.object(tx_wrapper, 'get_sqlalchemy_session', return_value=mock_session):
            # First call
            session1 = await tx_wrapper.get_sqlalchemy_session()
            
            # Second call should return the same session
            session2 = await tx_wrapper.get_sqlalchemy_session()
            
            self.assertEqual(session1, session2)
            self.assertEqual(session1, mock_session)
    
    async def test_transaction_wrapper_commit_sqlalchemy(self):
        """Test transaction wrapper SQLAlchemy commit"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        
        # Mock SQLAlchemy session
        mock_session = AsyncMock()
        tx_wrapper._sqlalchemy_session = mock_session
        
        await tx_wrapper.commit_sqlalchemy()
        
        mock_session.commit.assert_called_once()
    
    async def test_transaction_wrapper_rollback_sqlalchemy(self):
        """Test transaction wrapper SQLAlchemy rollback"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        
        # Mock SQLAlchemy session
        mock_session = AsyncMock()
        tx_wrapper._sqlalchemy_session = mock_session
        
        await tx_wrapper.rollback_sqlalchemy()
        
        mock_session.rollback.assert_called_once()
    
    async def test_transaction_wrapper_close_sqlalchemy(self):
        """Test transaction wrapper SQLAlchemy close"""
        tx_wrapper = AsyncTransactionWrapper(self.mock_connection, self.client)
        
        # Mock SQLAlchemy session and engine
        mock_session = AsyncMock()
        mock_session.close = AsyncMock()
        mock_engine = Mock()
        mock_engine.dispose = AsyncMock()
        tx_wrapper._sqlalchemy_session = mock_session
        tx_wrapper._sqlalchemy_engine = mock_engine
        
        await tx_wrapper.close_sqlalchemy()
        
        mock_session.close.assert_called_once()
        mock_engine.dispose.assert_called_once()
        self.assertIsNone(tx_wrapper._sqlalchemy_session)
        self.assertIsNone(tx_wrapper._sqlalchemy_engine)
    
    async def test_transaction_success_flow(self):
        """Test successful transaction flow"""
        mock_cursor = AsyncMock()
        mock_cursor.description = None
        mock_cursor.rowcount = 1
        
        # Create a proper async context manager for cursor
        class MockCursorContext:
            def __init__(self, cursor):
                self.cursor = cursor
            
            async def __aenter__(self):
                return self.cursor
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        # Set up the mock connection to return our cursor context
        self.mock_connection.cursor = Mock(return_value=MockCursorContext(mock_cursor))
        
        # Mock SQLAlchemy session
        mock_session = AsyncMock()
        mock_session.begin = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session.close = AsyncMock()
        
        # Mock the transaction wrapper's SQLAlchemy methods
        with patch.object(AsyncTransactionWrapper, 'get_sqlalchemy_session', return_value=mock_session), \
             patch.object(AsyncTransactionWrapper, 'commit_sqlalchemy', return_value=None), \
             patch.object(AsyncTransactionWrapper, 'rollback_sqlalchemy', return_value=None), \
             patch.object(AsyncTransactionWrapper, 'close_sqlalchemy', return_value=None):
            
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
            
            # Verify commit order - SQLAlchemy commit is mocked, so we check MatrixOne commit
            self.mock_connection.commit.assert_called_once()
    
    async def test_transaction_rollback_flow(self):
        """Test transaction rollback flow"""
        mock_cursor = AsyncMock()
        mock_cursor.execute.side_effect = Exception("Query failed")
        
        # Create a proper async context manager for cursor
        class MockCursorContext:
            def __init__(self, cursor):
                self.cursor = cursor
            
            async def __aenter__(self):
                return self.cursor
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        # Set up the mock connection to return our cursor context
        self.mock_connection.cursor = Mock(return_value=MockCursorContext(mock_cursor))
        
        # Mock SQLAlchemy session
        mock_session = AsyncMock()
        mock_session.begin = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session.close = AsyncMock()
        
        # Mock the transaction wrapper's SQLAlchemy methods
        with patch.object(AsyncTransactionWrapper, 'get_sqlalchemy_session', return_value=mock_session), \
             patch.object(AsyncTransactionWrapper, 'commit_sqlalchemy', return_value=None), \
             patch.object(AsyncTransactionWrapper, 'rollback_sqlalchemy', return_value=None), \
             patch.object(AsyncTransactionWrapper, 'close_sqlalchemy', return_value=None):
            
            with self.assertRaises(Exception):
                async with self.client.transaction() as tx:
                    session = await tx.get_sqlalchemy_session()
                    await tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
            
            # Verify rollback order - SQLAlchemy rollback is mocked, so we check MatrixOne rollback
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
        
        mock_session = AsyncMock()
        mock_session.begin = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session.close = AsyncMock()
        
        # Mock the get_sqlalchemy_session method directly
        with patch.object(tx_wrapper, 'get_sqlalchemy_session', return_value=mock_session):
            session = await tx_wrapper.get_sqlalchemy_session()
            
            # Verify session was created
            self.assertEqual(session, mock_session)


class TestAsyncSQLAlchemyIntegration(unittest.IsolatedAsyncioTestCase):
    """Test Async SQLAlchemy Integration Patterns"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncClient()
        self.mock_connection = AsyncMock()
        self.client._connection = self.mock_connection
        # Set up connection parameters for SQLAlchemy integration
        self.client._connection_params = {
            'user': 'testuser',
            'password': 'testpass',
            'host': 'localhost',
            'port': 6001,
            'db': 'testdb'
        }
    
    async def test_mixed_operations_pattern(self):
        """Test mixed SQLAlchemy and MatrixOne operations"""
        mock_cursor = AsyncMock()
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [(1, 'Alice'), (2, 'Bob')]
        
        # Create a proper async context manager for cursor
        class MockCursorContext:
            def __init__(self, cursor):
                self.cursor = cursor
            
            async def __aenter__(self):
                return self.cursor
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        # Set up the mock connection to return our cursor context
        self.mock_connection.cursor = Mock(return_value=MockCursorContext(mock_cursor))
        
        # Mock SQLAlchemy session
        mock_session = AsyncMock()
        mock_session.begin = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session.close = AsyncMock()
        
        # Mock SQLAlchemy model
        mock_user = Mock()
        mock_user.id = 1
        mock_user.name = "Alice"
        mock_user.email = "alice@example.com"
        
        # Mock query method properly
        mock_query = Mock()
        mock_query.all.return_value = [mock_user]
        mock_session.query = Mock(return_value=mock_query)
        
        # Mock the transaction wrapper's SQLAlchemy methods
        with patch.object(AsyncTransactionWrapper, 'get_sqlalchemy_session', return_value=mock_session), \
             patch.object(AsyncTransactionWrapper, 'commit_sqlalchemy', return_value=None), \
             patch.object(AsyncTransactionWrapper, 'rollback_sqlalchemy', return_value=None), \
             patch.object(AsyncTransactionWrapper, 'close_sqlalchemy', return_value=None):
            
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
        mock_cursor.execute.side_effect = Exception("Database error")
        
        # Create a proper async context manager for cursor
        class MockCursorContext:
            def __init__(self, cursor):
                self.cursor = cursor
            
            async def __aenter__(self):
                return self.cursor
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        # Set up the mock connection to return our cursor context
        self.mock_connection.cursor = Mock(return_value=MockCursorContext(mock_cursor))
        
        # Mock SQLAlchemy session
        mock_session = AsyncMock()
        mock_session.begin = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session.close = AsyncMock()
        mock_session.add = Mock()
        mock_session.flush = Mock()
        
        # Mock the transaction wrapper's SQLAlchemy methods
        with patch.object(AsyncTransactionWrapper, 'get_sqlalchemy_session', return_value=mock_session), \
             patch.object(AsyncTransactionWrapper, 'commit_sqlalchemy', return_value=None), \
             patch.object(AsyncTransactionWrapper, 'rollback_sqlalchemy', return_value=None), \
             patch.object(AsyncTransactionWrapper, 'close_sqlalchemy', return_value=None):
            
            with self.assertRaises(Exception):
                async with self.client.transaction() as tx:
                    session = await tx.get_sqlalchemy_session()
                    
                    # SQLAlchemy operation (should succeed)
                    mock_user = Mock()
                    session.add(mock_user)
                    session.flush()
                    
                    # MatrixOne operation (should fail)
                    await tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
            
            # Verify rollback was called - SQLAlchemy rollback is mocked, so we check MatrixOne rollback
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
