"""
Test AsyncClient functionality
"""

import unittest
import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os

# Store original modules to restore later
_original_modules = {}

def setup_sqlalchemy_mocks():
    """Setup SQLAlchemy mocks for this test class"""
    global _original_modules
    _original_modules['pymysql'] = sys.modules.get('pymysql')
    _original_modules['aiomysql'] = sys.modules.get('aiomysql')
    _original_modules['sqlalchemy'] = sys.modules.get('sqlalchemy')
    _original_modules['sqlalchemy.engine'] = sys.modules.get('sqlalchemy.engine')
    _original_modules['sqlalchemy.orm'] = sys.modules.get('sqlalchemy.orm')
    _original_modules['sqlalchemy.ext.asyncio'] = sys.modules.get('sqlalchemy.ext.asyncio')
    
    sys.modules['pymysql'] = Mock()
    sys.modules['aiomysql'] = Mock()
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
    
    # Mock SQLAlchemy async engine
    sys.modules['sqlalchemy.ext.asyncio'] = Mock()
    sys.modules['sqlalchemy.ext.asyncio'].create_async_engine = Mock()
    sys.modules['sqlalchemy.ext.asyncio'].AsyncEngine = Mock()

def teardown_sqlalchemy_mocks():
    """Restore original modules"""
    global _original_modules
    for module_name, original_module in _original_modules.items():
        if original_module is not None:
            sys.modules[module_name] = original_module
        elif module_name in sys.modules:
            del sys.modules[module_name]

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone.async_client import AsyncClient, AsyncResultSet, AsyncSnapshotManager, AsyncCloneManager, AsyncMoCtlManager
from matrixone.snapshot import SnapshotLevel, Snapshot
from matrixone.exceptions import MoCtlError
from datetime import datetime


class TestAsyncResultSet(unittest.TestCase):
    """Test AsyncResultSet functionality"""
    
    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        setup_sqlalchemy_mocks()
    
    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        teardown_sqlalchemy_mocks()
    
    def test_async_result_set(self):
        """Test AsyncResultSet basic functionality"""
        columns = ['id', 'name', 'email']
        rows = [(1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')]
        
        result = AsyncResultSet(columns, rows)
        
        self.assertEqual(result.columns, columns)
        self.assertEqual(result.rows, rows)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.fetchone(), (1, 'Alice', 'alice@example.com'))
        self.assertEqual(result.scalar(), 1)
        
        # Test iteration
        row_list = list(result)
        self.assertEqual(len(row_list), 2)


class TestAsyncClient(unittest.IsolatedAsyncioTestCase):
    """Test AsyncClient functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncClient()
    
    def test_init(self):
        """Test AsyncClient initialization"""
        self.assertEqual(self.client.connection_timeout, 30)
        self.assertEqual(self.client.query_timeout, 300)
        self.assertEqual(self.client.auto_commit, True)
        self.assertEqual(self.client.charset, 'utf8mb4')
        self.assertIsNone(self.client._engine)
        self.assertIsNone(self.client._connection)
        self.assertIsInstance(self.client._snapshots, AsyncSnapshotManager)
        self.assertIsInstance(self.client._clone, AsyncCloneManager)
        self.assertIsInstance(self.client._moctl, AsyncMoCtlManager)
    
    @patch('matrixone.async_client.create_async_engine')
    async def test_connect(self, mock_create_async_engine):
        """Test async connection"""
        # Create mock connection and result
        mock_connection = AsyncMock()
        mock_result = AsyncMock()
        mock_result.returns_rows = False
        
        # Create a proper async context manager for engine.begin()
        class MockBeginContext:
            def __init__(self, connection):
                self.connection = connection
            
            async def __aenter__(self):
                return self.connection
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        # Create a mock engine class that properly implements begin()
        class MockEngine:
            def __init__(self, connection):
                self.connection = connection
            
            def begin(self):
                return MockBeginContext(self.connection)
        
        # Mock the connection.execute method - make it async
        async def mock_execute(sql, params=None):
            return mock_result
        
        mock_connection.execute = mock_execute
        
        # Create mock engine instance
        mock_engine = MockEngine(mock_connection)
        
        # Mock create_async_engine to return our mock engine
        mock_create_async_engine.return_value = mock_engine
        
        await self.client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        # Verify create_async_engine was called
        mock_create_async_engine.assert_called_once()
        self.assertEqual(self.client._engine, mock_engine)
        self.assertEqual(self.client._connection_params['host'], 'localhost')
        self.assertEqual(self.client._connection_params['port'], 6001)
        self.assertEqual(self.client._connection_params['user'], 'root')
        self.assertEqual(self.client._connection_params['password'], '111')
        self.assertEqual(self.client._connection_params['database'], 'test')
    
    @patch('matrixone.async_client.create_async_engine')
    async def test_connect_failure(self, mock_create_async_engine):
        """Test async connection failure"""
        mock_create_async_engine.side_effect = Exception("Connection failed")
        
        with self.assertRaises(Exception):
            await self.client.connect(
                host="localhost",
                port=6001,
                user="root",
                password="111",
                database="test"
            )
    
    async def test_disconnect(self):
        """Test async disconnection"""
        mock_engine = AsyncMock()
        mock_engine.dispose = AsyncMock()
        
        self.client._engine = mock_engine
        
        await self.client.disconnect()
        
        mock_engine.dispose.assert_called_once()
        self.assertIsNone(self.client._engine)
    
    async def test_execute_success(self):
        """Test successful async execution"""
        # Create a mock result class that properly implements the interface
        class MockResult:
            def __init__(self):
                self.returns_rows = True
            
            def fetchall(self):
                return [(1, 'Alice'), (2, 'Bob')]
            
            def keys(self):
                return ['id', 'name']
        
        # Create a mock connection class
        class MockConnection:
            def __init__(self):
                self.execute_called = False
                self.execute_args = None
            
            async def execute(self, sql, params=None):
                self.execute_called = True
                self.execute_args = (sql, params)
                return MockResult()
        
        # Create a proper async context manager for engine.begin()
        class MockBeginContext:
            def __init__(self, connection):
                self.connection = connection
            
            async def __aenter__(self):
                return self.connection
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        # Create a mock engine class that properly implements begin()
        class MockEngine:
            def __init__(self, connection):
                self.connection = connection
            
            def begin(self):
                return MockBeginContext(self.connection)
        
        # Create mock instances
        mock_connection = MockConnection()
        mock_engine = MockEngine(mock_connection)
        
        # Set the mock engine on the client
        self.client._engine = mock_engine
        
        result = await self.client.execute("SELECT id, name FROM users")
        
        self.assertTrue(mock_connection.execute_called)
        self.assertIsInstance(result, AsyncResultSet)
        self.assertEqual(result.columns, ['id', 'name'])
        self.assertEqual(result.rows, [(1, 'Alice'), (2, 'Bob')])
    
    async def test_execute_with_params(self):
        """Test async execution with parameters"""
        # Create a mock result class that properly implements the interface
        class MockResult:
            def __init__(self):
                self.returns_rows = False
                self.rowcount = 1
        
        # Create a mock connection class
        class MockConnection:
            def __init__(self):
                self.execute_called = False
                self.execute_args = None
            
            async def execute(self, sql, params=None):
                self.execute_called = True
                self.execute_args = (sql, params)
                return MockResult()
        
        # Create a proper async context manager for engine.begin()
        class MockBeginContext:
            def __init__(self, connection):
                self.connection = connection
            
            async def __aenter__(self):
                return self.connection
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        # Create a mock engine class that properly implements begin()
        class MockEngine:
            def __init__(self, connection):
                self.connection = connection
            
            def begin(self):
                return MockBeginContext(self.connection)
        
        # Create mock instances
        mock_connection = MockConnection()
        mock_engine = MockEngine(mock_connection)
        
        # Set the mock engine on the client
        self.client._engine = mock_engine
        
        result = await self.client.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
        
        self.assertTrue(mock_connection.execute_called)
        self.assertEqual(result.affected_rows, 1)
    
    async def test_execute_not_connected(self):
        """Test async execution without connection"""
        with self.assertRaises(Exception):
            await self.client.execute("SELECT 1")
    
    async def test_snapshot_query(self):
        """Test async snapshot query"""
        # Create a proper async context manager for engine.begin()
        class MockBeginContext:
            def __init__(self, connection):
                self.connection = connection
            
            async def __aenter__(self):
                return self.connection
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        # Create a mock engine class that properly implements begin()
        class MockEngine:
            def __init__(self, connection):
                self.connection = connection
            
            def begin(self):
                return MockBeginContext(self.connection)
        
        # Create mock connection and result
        mock_connection = Mock()
        mock_result = Mock()
        
        # Setup mock result
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = [(1, 'Alice')]
        mock_result.keys.return_value = ['id', 'name']
        
        # Setup mock connection - make execute return a coroutine
        async def mock_execute(sql, params=None):
            return mock_result
        
        mock_connection.execute = Mock(side_effect=mock_execute)
        
        # Mock the text function
        import sys
        sys.modules['sqlalchemy'].text = Mock(return_value="SELECT id, name FROM users {SNAPSHOT = 'test_snapshot'}")
        
        # Create mock engine instance
        mock_engine = MockEngine(mock_connection)
        
        self.client._engine = mock_engine
        
        result = await self.client.snapshot_query("test_snapshot", "SELECT id, name FROM users")
        
        expected_sql = "SELECT id, name FROM users {SNAPSHOT = 'test_snapshot'}"
        mock_connection.execute.assert_called_once()
        self.assertIsInstance(result, AsyncResultSet)
    
    async def test_context_manager(self):
        """Test async context manager"""
        with patch.object(self.client, 'connect') as mock_connect, \
             patch.object(self.client, 'disconnect') as mock_disconnect, \
             patch.object(self.client, 'connected') as mock_connected:
            
            # Mock connected to return True so disconnect will be called
            mock_connected.return_value = True
            
            async with self.client as client:
                self.assertEqual(client, self.client)
            
            # disconnect should be called since connected() returns True
            mock_disconnect.assert_called_once()


class TestAsyncSnapshotManager(unittest.IsolatedAsyncioTestCase):
    """Test AsyncSnapshotManager functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = AsyncMock()
        self.snapshot_manager = AsyncSnapshotManager(self.mock_client)
    
    async def test_create_cluster_snapshot(self):
        """Test creating cluster snapshot"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        snapshot = await self.snapshot_manager.create("test_snap", SnapshotLevel.CLUSTER)
        
        self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT test_snap FOR CLUSTER")
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.CLUSTER)
    
    async def test_create_database_snapshot(self):
        """Test creating database snapshot"""
        # Mock the result to include database information
        mock_result = AsyncResultSet([], [])
        mock_result.database = "test_db"
        self.mock_client.execute.return_value = mock_result
        
        snapshot = await self.snapshot_manager.create("test_snap", SnapshotLevel.DATABASE, database="test_db")
        
        self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT test_snap FOR DATABASE test_db")
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.DATABASE)
        self.assertEqual(snapshot.database, "test_db")
    
    async def test_create_table_snapshot(self):
        """Test creating table snapshot"""
        # Mock the result to include database and table information
        mock_result = AsyncResultSet([], [])
        mock_result.database = "test_db"
        mock_result.table = "test_table"
        self.mock_client.execute.return_value = mock_result
        
        snapshot = await self.snapshot_manager.create("test_snap", SnapshotLevel.TABLE, database="test_db", table="test_table")
        
        self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT test_snap FOR TABLE test_db test_table")
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.TABLE)
        self.assertEqual(snapshot.database, "test_db")
        self.assertEqual(snapshot.table, "test_table")
    
    async def test_get_snapshot(self):
        """Test getting snapshot"""
        # Use integer timestamp instead of datetime object
        timestamp_ns = int(datetime.now().timestamp() * 1000000000)
        mock_result = AsyncResultSet(
            ['sname', 'ts', 'level', 'account_name', 'database_name', 'table_name'],
            [('test_snap', timestamp_ns, 'database', 'sys', 'test_db', None)]
        )
        self.mock_client.execute.return_value = mock_result
        
        snapshot = await self.snapshot_manager.get("test_snap")
        
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.DATABASE)
        self.assertEqual(snapshot.database, "test_db")
    
    async def test_list_snapshots(self):
        """Test listing snapshots"""
        # Use integer timestamps instead of datetime objects
        timestamp_ns = int(datetime.now().timestamp() * 1000000000)
        mock_result = AsyncResultSet(
            ['sname', 'ts', 'level', 'account_name', 'database_name', 'table_name'],
            [
                ('snap1', timestamp_ns, 'database', 'sys', 'db1', None),
                ('snap2', timestamp_ns, 'table', 'sys', 'db2', 'table2')
            ]
        )
        self.mock_client.execute.return_value = mock_result
        
        snapshots = await self.snapshot_manager.list()
        
        self.assertEqual(len(snapshots), 2)
        self.assertEqual(snapshots[0].name, "snap1")
        self.assertEqual(snapshots[1].name, "snap2")
    
    async def test_delete_snapshot(self):
        """Test deleting snapshot"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        await self.snapshot_manager.delete("test_snap")
        
        self.mock_client.execute.assert_called_once_with("DROP SNAPSHOT test_snap")
    
    async def test_exists_snapshot(self):
        """Test checking snapshot existence"""
        # Test exists
        timestamp_ns = int(datetime.now().timestamp() * 1000000000)
        mock_result = AsyncResultSet(
            ['sname', 'ts', 'level', 'account_name', 'database_name', 'table_name'],
            [('test_snap', timestamp_ns, 'database', 'sys', 'test_db', None)]
        )
        self.mock_client.execute.return_value = mock_result
        
        exists = await self.snapshot_manager.exists("test_snap")
        self.assertTrue(exists)
        
        # Test not exists
        from matrixone.exceptions import SnapshotError
        self.mock_client.execute.side_effect = SnapshotError("Snapshot 'nonexistent' not found")
        
        exists = await self.snapshot_manager.exists("nonexistent")
        self.assertFalse(exists)


class TestAsyncCloneManager(unittest.IsolatedAsyncioTestCase):
    """Test AsyncCloneManager functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = AsyncMock()
        self.clone_manager = AsyncCloneManager(self.mock_client)
    
    async def test_clone_database(self):
        """Test cloning database"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        await self.clone_manager.clone_database("target_db", "source_db")
        
        self.mock_client.execute.assert_called_once_with("CREATE DATABASE target_db  CLONE source_db")
    
    async def test_clone_database_with_snapshot(self):
        """Test cloning database with snapshot"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        await self.clone_manager.clone_database_with_snapshot("target_db", "source_db", "test_snapshot")
        
        self.mock_client.execute.assert_called_once_with("CREATE DATABASE target_db  CLONE source_db FOR SNAPSHOT 'test_snapshot'")
    
    async def test_clone_database_if_not_exists(self):
        """Test cloning database with if not exists"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        await self.clone_manager.clone_database("target_db", "source_db", if_not_exists=True)
        
        self.mock_client.execute.assert_called_once_with("CREATE DATABASE target_db IF NOT EXISTS CLONE source_db")
    
    async def test_clone_table(self):
        """Test cloning table"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        await self.clone_manager.clone_table("target_table", "source_table")
        
        self.mock_client.execute.assert_called_once_with("CREATE TABLE target_table  CLONE source_table")
    
    async def test_clone_table_with_snapshot(self):
        """Test cloning table with snapshot"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        await self.clone_manager.clone_table_with_snapshot("target_table", "source_table", "test_snapshot")
        
        self.mock_client.execute.assert_called_once_with("CREATE TABLE target_table  CLONE source_table FOR SNAPSHOT 'test_snapshot'")


class TestAsyncMoCtlManager(unittest.IsolatedAsyncioTestCase):
    """Test AsyncMoCtlManager functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = AsyncMock()
        self.moctl_manager = AsyncMoCtlManager(self.mock_client)
    
    async def test_flush_table(self):
        """Test async flush table"""
        mock_result = AsyncResultSet(
            ['result'],
            [('{"method": "Flush", "result": [{"returnStr": "OK"}]}',)]
        )
        self.mock_client.execute.return_value = mock_result
        
        result = await self.moctl_manager.flush_table('db1', 'users')
        
        expected_sql = "SELECT mo_ctl('dn', 'flush', 'db1.users')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        self.assertEqual(result['method'], 'Flush')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    async def test_increment_checkpoint(self):
        """Test async increment checkpoint"""
        mock_result = AsyncResultSet(
            ['result'],
            [('{"method": "Checkpoint", "result": [{"returnStr": "OK"}]}',)]
        )
        self.mock_client.execute.return_value = mock_result
        
        result = await self.moctl_manager.increment_checkpoint()
        
        expected_sql = "SELECT mo_ctl('dn', 'checkpoint', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        self.assertEqual(result['method'], 'Checkpoint')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    async def test_global_checkpoint(self):
        """Test async global checkpoint"""
        mock_result = AsyncResultSet(
            ['result'],
            [('{"method": "GlobalCheckpoint", "result": [{"returnStr": "OK"}]}',)]
        )
        self.mock_client.execute.return_value = mock_result
        
        result = await self.moctl_manager.global_checkpoint()
        
        expected_sql = "SELECT mo_ctl('dn', 'globalcheckpoint', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        self.assertEqual(result['method'], 'GlobalCheckpoint')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    async def test_moctl_error_handling(self):
        """Test async mo_ctl error handling"""
        mock_result = AsyncResultSet(
            ['result'],
            [('{"method": "Flush", "result": [{"returnStr": "ERROR: Table not found"}]}',)]
        )
        self.mock_client.execute.return_value = mock_result
        
        with self.assertRaises(MoCtlError) as context:
            await self.moctl_manager.flush_table('db1', 'nonexistent')
        
        self.assertIn("ERROR: Table not found", str(context.exception))


class TestAsyncTransaction(unittest.IsolatedAsyncioTestCase):
    """Test async transaction functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncClient()
        self.mock_connection = AsyncMock()
        
        # Create a mock engine class that properly implements begin()
        class MockEngine:
            def __init__(self, connection):
                self.connection = connection
            
            def begin(self):
                # Create a proper async context manager for engine.begin()
                class MockBeginContext:
                    def __init__(self, connection):
                        self.connection = connection
                    
                    async def __aenter__(self):
                        return self.connection
                    
                    async def __aexit__(self, exc_type, exc_val, exc_tb):
                        pass
                
                return MockBeginContext(self.connection)
        
        self.mock_engine = MockEngine(self.mock_connection)
        self.client._engine = self.mock_engine
    
    async def test_transaction_success(self):
        """Test successful async transaction"""
        mock_result = AsyncMock()
        mock_result.returns_rows = False
        mock_result.rowcount = 1
        
        # Mock the connection.execute method - make it async
        async def mock_execute(sql, params=None):
            return mock_result
        
        self.mock_connection.execute = Mock(side_effect=mock_execute)
        
        async with self.client.transaction() as tx:
            await tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
        
        # Verify that the transaction was used
        self.mock_connection.execute.assert_called_once()
    
    async def test_transaction_rollback(self):
        """Test async transaction rollback"""
        # Mock the connection.execute method to raise an exception
        async def mock_execute_error(sql, params=None):
            raise Exception("Query failed")
        
        self.mock_connection.execute = mock_execute_error
        
        with self.assertRaises(Exception):
            async with self.client.transaction() as tx:
                await tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))


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
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncResultSet))
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncClient))
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncSnapshotManager))
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncCloneManager))
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncMoCtlManager))
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncTransaction))
    
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
