"""
Test AsyncClient functionality
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

from matrixone.async_client import AsyncClient, AsyncResultSet, AsyncSnapshotManager, AsyncCloneManager, AsyncMoCtlManager
from matrixone.snapshot import SnapshotLevel, Snapshot
from matrixone.exceptions import MoCtlError
from datetime import datetime


class TestAsyncResultSet(unittest.TestCase):
    """Test AsyncResultSet functionality"""
    
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


class TestAsyncClient(unittest.TestCase):
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
        self.assertIsNone(self.client._connection)
        self.assertIsInstance(self.client._snapshots, AsyncSnapshotManager)
        self.assertIsInstance(self.client._clone, AsyncCloneManager)
        self.assertIsInstance(self.client._moctl, AsyncMoCtlManager)
    
    @patch('aiomysql.connect')
    async def test_connect(self, mock_connect):
        """Test async connection"""
        mock_connection = AsyncMock()
        mock_connect.return_value = mock_connection
        
        await self.client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        mock_connect.assert_called_once()
        self.assertEqual(self.client._connection, mock_connection)
        self.assertEqual(self.client._connection_params['host'], 'localhost')
        self.assertEqual(self.client._connection_params['port'], 6001)
        self.assertEqual(self.client._connection_params['user'], 'root')
        self.assertEqual(self.client._connection_params['password'], '111')
        self.assertEqual(self.client._connection_params['database'], 'test')
    
    @patch('aiomysql.connect')
    async def test_connect_failure(self, mock_connect):
        """Test async connection failure"""
        mock_connect.side_effect = Exception("Connection failed")
        
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
        mock_connection = AsyncMock()
        self.client._connection = mock_connection
        
        await self.client.disconnect()
        
        mock_connection.close.assert_called_once()
        await mock_connection.wait_closed.assert_called_once()
        self.assertIsNone(self.client._connection)
    
    async def test_execute_success(self):
        """Test successful async execution"""
        mock_connection = AsyncMock()
        mock_cursor = AsyncMock()
        mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
        
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [(1, 'Alice'), (2, 'Bob')]
        
        self.client._connection = mock_connection
        
        result = await self.client.execute("SELECT id, name FROM users")
        
        mock_cursor.execute.assert_called_once_with("SELECT id, name FROM users", None)
        self.assertIsInstance(result, AsyncResultSet)
        self.assertEqual(result.columns, ['id', 'name'])
        self.assertEqual(result.rows, [(1, 'Alice'), (2, 'Bob')])
    
    async def test_execute_with_params(self):
        """Test async execution with parameters"""
        mock_connection = AsyncMock()
        mock_cursor = AsyncMock()
        mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
        
        mock_cursor.description = None
        mock_cursor.rowcount = 1
        
        self.client._connection = mock_connection
        
        result = await self.client.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
        
        mock_cursor.execute.assert_called_once_with("INSERT INTO users (name) VALUES (%s)", ("Alice",))
        self.assertEqual(result.affected_rows, 1)
    
    async def test_execute_not_connected(self):
        """Test async execution without connection"""
        with self.assertRaises(Exception):
            await self.client.execute("SELECT 1")
    
    async def test_snapshot_query(self):
        """Test async snapshot query"""
        mock_connection = AsyncMock()
        mock_cursor = AsyncMock()
        mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
        
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [(1, 'Alice')]
        
        self.client._connection = mock_connection
        
        result = await self.client.snapshot_query("test_snapshot", "SELECT id, name FROM users")
        
        expected_sql = "SELECT id, name FROM users FOR SNAPSHOT 'test_snapshot'"
        mock_cursor.execute.assert_called_once_with(expected_sql, None)
        self.assertIsInstance(result, AsyncResultSet)
    
    async def test_context_manager(self):
        """Test async context manager"""
        with patch.object(self.client, 'connect') as mock_connect, \
             patch.object(self.client, 'disconnect') as mock_disconnect:
            
            async with self.client as client:
                self.assertEqual(client, self.client)
            
            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()


class TestAsyncSnapshotManager(unittest.TestCase):
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
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        snapshot = await self.snapshot_manager.create("test_snap", SnapshotLevel.DATABASE, database="test_db")
        
        self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT test_snap FOR DATABASE test_db")
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.DATABASE)
        self.assertEqual(snapshot.database, "test_db")
    
    async def test_create_table_snapshot(self):
        """Test creating table snapshot"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        snapshot = await self.snapshot_manager.create("test_snap", SnapshotLevel.TABLE, database="test_db", table="test_table")
        
        self.mock_client.execute.assert_called_once_with("CREATE SNAPSHOT test_snap FOR TABLE test_db.test_table")
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.TABLE)
        self.assertEqual(snapshot.database, "test_db")
        self.assertEqual(snapshot.table, "test_table")
    
    async def test_get_snapshot(self):
        """Test getting snapshot"""
        mock_result = AsyncResultSet(
            ['name', 'level', 'created_at', 'database_name', 'table_name', 'description'],
            [('test_snap', 'database', datetime.now(), 'test_db', None, 'Test snapshot')]
        )
        self.mock_client.execute.return_value = mock_result
        
        snapshot = await self.snapshot_manager.get("test_snap")
        
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.DATABASE)
        self.assertEqual(snapshot.database, "test_db")
    
    async def test_list_snapshots(self):
        """Test listing snapshots"""
        mock_result = AsyncResultSet(
            ['name', 'level', 'created_at', 'database_name', 'table_name', 'description'],
            [
                ('snap1', 'database', datetime.now(), 'db1', None, 'Snapshot 1'),
                ('snap2', 'table', datetime.now(), 'db2', 'table2', 'Snapshot 2')
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
        mock_result = AsyncResultSet(
            ['name', 'level', 'created_at', 'database_name', 'table_name', 'description'],
            [('test_snap', 'database', datetime.now(), 'test_db', None, 'Test snapshot')]
        )
        self.mock_client.execute.return_value = mock_result
        
        exists = await self.snapshot_manager.exists("test_snap")
        self.assertTrue(exists)
        
        # Test not exists
        self.mock_client.execute.side_effect = Exception("Not found")
        
        exists = await self.snapshot_manager.exists("nonexistent")
        self.assertFalse(exists)


class TestAsyncCloneManager(unittest.TestCase):
    """Test AsyncCloneManager functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = AsyncMock()
        self.clone_manager = AsyncCloneManager(self.mock_client)
    
    async def test_clone_database(self):
        """Test cloning database"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        await self.clone_manager.clone_database("target_db", "source_db")
        
        self.mock_client.execute.assert_called_once_with("CREATE DATABASE target_db CLONE source_db")
    
    async def test_clone_database_with_snapshot(self):
        """Test cloning database with snapshot"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        await self.clone_manager.clone_database_with_snapshot("target_db", "source_db", "test_snapshot")
        
        self.mock_client.execute.assert_called_once_with("CREATE DATABASE target_db CLONE source_db FOR SNAPSHOT 'test_snapshot'")
    
    async def test_clone_database_if_not_exists(self):
        """Test cloning database with if not exists"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        await self.clone_manager.clone_database("target_db", "source_db", if_not_exists=True)
        
        self.mock_client.execute.assert_called_once_with("CREATE DATABASE target_db IF NOT EXISTS CLONE source_db")
    
    async def test_clone_table(self):
        """Test cloning table"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        await self.clone_manager.clone_table("target_table", "source_table")
        
        self.mock_client.execute.assert_called_once_with("CREATE TABLE target_table CLONE source_table")
    
    async def test_clone_table_with_snapshot(self):
        """Test cloning table with snapshot"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])
        
        await self.clone_manager.clone_table_with_snapshot("target_table", "source_table", "test_snapshot")
        
        self.mock_client.execute.assert_called_once_with("CREATE TABLE target_table CLONE source_table FOR SNAPSHOT 'test_snapshot'")


class TestAsyncMoCtlManager(unittest.TestCase):
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
    
    async def test_checkpoint(self):
        """Test async checkpoint"""
        mock_result = AsyncResultSet(
            ['result'],
            [('{"method": "Checkpoint", "result": [{"returnStr": "OK"}]}',)]
        )
        self.mock_client.execute.return_value = mock_result
        
        result = await self.moctl_manager.checkpoint()
        
        expected_sql = "SELECT mo_ctl('dn', 'checkpoint', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        self.assertEqual(result['method'], 'Checkpoint')
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


class TestAsyncTransaction(unittest.TestCase):
    """Test async transaction functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncClient()
        self.mock_connection = AsyncMock()
        self.client._connection = self.mock_connection
    
    async def test_transaction_success(self):
        """Test successful async transaction"""
        mock_cursor = AsyncMock()
        self.mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_cursor.description = None
        mock_cursor.rowcount = 1
        
        async with self.client.transaction() as tx:
            await tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
        
        self.mock_connection.begin.assert_called_once()
        self.mock_connection.commit.assert_called_once()
    
    async def test_transaction_rollback(self):
        """Test async transaction rollback"""
        mock_cursor = AsyncMock()
        self.mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Query failed")
        
        with self.assertRaises(Exception):
            async with self.client.transaction() as tx:
                await tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
        
        self.mock_connection.begin.assert_called_once()
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
