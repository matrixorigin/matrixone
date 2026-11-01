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
Comprehensive Async Client Query Tests

This file consolidates all async client and query-related tests from:
- test_async.py (comprehensive async client tests)
- test_async_simple.py (basic async client tests)
- test_async_sqlalchemy_transaction.py (SQLAlchemy integration tests)

The merged file eliminates redundancy while maintaining full test coverage.
"""

import unittest
import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os
from datetime import datetime, timezone

# Store original modules to restore later
_original_modules = {}


# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone.async_client import (
    AsyncClient,
    AsyncResultSet,
    AsyncSnapshotManager,
    AsyncCloneManager,
    AsyncMoCtlManager,
    AsyncSession,
)
from matrixone.snapshot import SnapshotLevel, Snapshot
from matrixone.exceptions import MoCtlError, ConnectionError


class TestAsyncResultSet(unittest.TestCase):
    """Test AsyncResultSet functionality"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        pass

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        pass

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


class TestAsyncClientBasic(unittest.IsolatedAsyncioTestCase):
    """Test AsyncClient basic functionality and imports"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        pass

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        pass

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

    def test_managers_properties(self):
        """Test async managers properties"""
        # Test snapshot manager
        snapshot_manager = self.client.snapshots
        self.assertIsInstance(snapshot_manager, AsyncSnapshotManager)

        # Test clone manager
        clone_manager = self.client.clone
        self.assertIsInstance(clone_manager, AsyncCloneManager)

        # Test moctl manager
        moctl_manager = self.client.moctl
        self.assertIsInstance(moctl_manager, AsyncMoCtlManager)

    def test_imports(self):
        """Test async imports"""
        try:
            from matrixone import AsyncClient, AsyncResultSet
            from matrixone.async_client import (
                AsyncSnapshotManager,
                AsyncCloneManager,
                AsyncMoCtlManager,
            )
            from matrixone.snapshot import SnapshotLevel

            # Test enum import
            self.assertIsNotNone(list(SnapshotLevel))
            self.assertTrue(True)  # If we get here, imports work
        except ImportError as e:
            self.fail(f"Import error: {e}")

    async def test_context_manager(self):
        """Test async context manager"""
        with patch.object(self.client, 'connect') as mock_connect, patch.object(
            self.client, 'disconnect'
        ) as mock_disconnect, patch.object(self.client, 'connected') as mock_connected:

            # Mock connected to return True so disconnect will be called
            mock_connected.return_value = True

            async with self.client as client:
                self.assertEqual(client, self.client)

            # disconnect should be called since connected() returns True
            mock_disconnect.assert_called_once()


class TestAsyncClientConnection(unittest.IsolatedAsyncioTestCase):
    """Test AsyncClient connection functionality"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        pass

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        pass

    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncClient()

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
                from sqlalchemy.ext.asyncio import create_async_engine

                self.native_engine = create_async_engine('mysql+aiomysql://unused@127.0.0.1:1/unused')
                self.sync_engine = self.native_engine.sync_engine
                self.dialect = self.native_engine.dialect

            def begin(self):
                return MockBeginContext(self.connection)

        # Mock the connection.execute method - make it async
        async def mock_execute(sql, params=None):
            return mock_result

        mock_connection.execute = mock_execute

        # Create mock engine instance
        mock_engine = MockEngine(mock_connection)
        self.addAsyncCleanup(mock_engine.native_engine.dispose)

        # Mock create_async_engine to return our mock engine
        mock_create_async_engine.return_value = mock_engine

        await self.client.connect(
            host="localhost", port=6001, user="root", password="111", database="test", on_connect=[]
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
                host="localhost", port=6001, user="root", password="111", database="test", on_connect=[]
            )

    async def test_disconnect(self):
        """Test async disconnection"""
        mock_engine = AsyncMock()
        mock_engine.dispose = AsyncMock()

        self.client._engine = mock_engine

        await self.client.disconnect()

        mock_engine.dispose.assert_called_once()
        self.assertIsNone(self.client._engine)


class TestAsyncClientQuery(unittest.IsolatedAsyncioTestCase):
    """Test AsyncClient query functionality"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        pass

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        pass

    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncClient()

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

        # Setup mock connection - make exec_driver_sql return a coroutine
        async def mock_exec_driver_sql(sql):
            return mock_result

        mock_connection.exec_driver_sql = Mock(side_effect=mock_exec_driver_sql)

        # Also setup execute for fallback
        async def mock_execute(sql, params=None):
            return mock_result

        mock_connection.execute = Mock(side_effect=mock_execute)

        # Create mock engine instance
        mock_engine = MockEngine(mock_connection)

        self.client._engine = mock_engine

        result = await self.client.query("users", snapshot="test_snapshot").select("id", "name").execute()

        expected_sql = "SELECT id, name FROM users{snapshot = 'test_snapshot'}"
        # Check that exec_driver_sql or execute was called
        mock_connection.exec_driver_sql.assert_called_once_with(expected_sql)
        self.assertIsInstance(result, AsyncResultSet)


class TestAsyncSnapshotManager(unittest.IsolatedAsyncioTestCase):
    """Test AsyncSnapshotManager functionality"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        pass

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        pass

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = AsyncMock()
        self.snapshot_manager = AsyncSnapshotManager(self.mock_client)

    async def test_create_cluster_snapshot(self):
        """Test creating cluster snapshot"""
        timestamp_ns = int(datetime.now().timestamp() * 1000000000)
        self.mock_client.execute.side_effect = [
            AsyncResultSet([], []),
            AsyncResultSet(
                ['sname', 'ts', 'level', 'account_name', 'database_name', 'table_name'],
                [('test_snap', timestamp_ns, 'cluster', 'sys', None, None)],
            ),
        ]

        snapshot = await self.snapshot_manager.create("test_snap", SnapshotLevel.CLUSTER)

        self.mock_client.execute.assert_any_await("CREATE SNAPSHOT test_snap FOR CLUSTER")
        self.assertEqual(self.mock_client.execute.await_count, 2)
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.CLUSTER)

    async def test_create_database_snapshot(self):
        """Test creating database snapshot"""
        timestamp_ns = int(datetime.now().timestamp() * 1000000000)
        self.mock_client.execute.side_effect = [
            AsyncResultSet([], []),
            AsyncResultSet(
                ['sname', 'ts', 'level', 'account_name', 'database_name', 'table_name'],
                [('test_snap', timestamp_ns, 'database', 'sys', 'test_db', None)],
            ),
        ]

        snapshot = await self.snapshot_manager.create("test_snap", SnapshotLevel.DATABASE, database="test_db")

        self.mock_client.execute.assert_any_await("CREATE SNAPSHOT test_snap FOR DATABASE test_db")
        self.assertEqual(self.mock_client.execute.await_count, 2)
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.DATABASE)
        self.assertEqual(snapshot.database, "test_db")

    async def test_create_table_snapshot(self):
        """Test creating table snapshot"""
        timestamp_ns = int(datetime.now().timestamp() * 1000000000)
        self.mock_client.execute.side_effect = [
            AsyncResultSet([], []),
            AsyncResultSet(
                ['sname', 'ts', 'level', 'account_name', 'database_name', 'table_name'],
                [('test_snap', timestamp_ns, 'table', 'sys', 'test_db', 'test_table')],
            ),
        ]

        snapshot = await self.snapshot_manager.create(
            "test_snap", SnapshotLevel.TABLE, database="test_db", table="test_table"
        )

        self.mock_client.execute.assert_any_await("CREATE SNAPSHOT test_snap FOR TABLE test_db test_table")
        self.assertEqual(self.mock_client.execute.await_count, 2)
        self.assertEqual(snapshot.name, "test_snap")
        self.assertEqual(snapshot.level, SnapshotLevel.TABLE)
        self.assertEqual(snapshot.database, "test_db")
        self.assertEqual(snapshot.table, "test_table")

    async def test_create_returns_persisted_snapshot_metadata(self):
        """Creation should return the metadata persisted by MatrixOne."""
        created_at = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        timestamp_ns = 1735787045000000000
        self.mock_client.execute.side_effect = [
            AsyncResultSet([], []),
            AsyncResultSet(
                ['sname', 'ts', 'level', 'account_name', 'database_name', 'table_name'],
                [('persisted_snap', timestamp_ns, 'database', 'sys', 'persisted_db', None)],
            ),
        ]

        snapshot = await self.snapshot_manager.create(
            "persisted_snap",
            SnapshotLevel.DATABASE,
            database="persisted_db",
            description="client-side description",
        )

        self.mock_client.execute.assert_any_await("CREATE SNAPSHOT persisted_snap FOR DATABASE persisted_db")
        self.assertEqual(self.mock_client.execute.await_count, 2)
        lookup_call = self.mock_client.execute.await_args_list[1]
        self.assertIn("WHERE sname = LOWER(?)", lookup_call.args[0])
        self.assertEqual(lookup_call.args[1], ("persisted_snap",))
        self.assertEqual(snapshot.created_at, created_at)
        self.assertIsNone(snapshot.description)
        self.assertEqual(snapshot.database, "persisted_db")

    async def test_create_uses_real_async_client_parameter_binding(self):
        """Snapshot lookup parameters should cross the real async execution boundary."""
        timestamp_ns = int(datetime(2025, 1, 2, 3, 4, 5).timestamp() * 1000000000)
        columns = ['sname', 'ts', 'level', 'account_name', 'database_name', 'table_name']
        rows = [('review_snap', timestamp_ns, 'database', 'sys', 'review_db', None)]

        class MockResult:
            def __init__(self, returns_rows=False):
                self.returns_rows = returns_rows
                self.rowcount = 0

            def fetchall(self):
                return rows

            def keys(self):
                return columns

        class MockConnection:
            def __init__(self):
                self.statements = []

            async def exec_driver_sql(self, sql):
                self.statements.append(sql)
                return MockResult(returns_rows=sql.lstrip().startswith("SELECT"))

        class MockBeginContext:
            def __init__(self, connection):
                self.connection = connection

            async def __aenter__(self):
                return self.connection

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        class MockEngine:
            def __init__(self, connection):
                self.connection = connection

            def begin(self):
                return MockBeginContext(self.connection)

        client = AsyncClient()
        connection = MockConnection()
        client._engine = MockEngine(connection)

        snapshot = await client.snapshots.create("review_snap", SnapshotLevel.DATABASE, database="review_db")

        self.assertEqual(len(connection.statements), 2)
        self.assertEqual(connection.statements[0], "CREATE SNAPSHOT review_snap FOR DATABASE review_db")
        self.assertIn("WHERE sname = LOWER('review_snap')", connection.statements[1])
        self.assertNotIn(":name", connection.statements[1])
        self.assertEqual(snapshot.name, "review_snap")
        self.assertEqual(snapshot.database, "review_db")

    async def test_create_canonicalizes_snapshot_name_for_lookup(self):
        """Creation should query the catalog with MatrixOne's canonical identifier."""
        timestamp_ns = int(datetime(2025, 1, 2, 3, 4, 5).timestamp() * 1000000000)
        columns = ['sname', 'ts', 'level', 'account_name', 'database_name', 'table_name']

        class MockResult:
            def __init__(self, rows, returns_rows=False):
                self.rows = rows
                self.returns_rows = returns_rows
                self.rowcount = 0

            def fetchall(self):
                return self.rows

            def keys(self):
                return columns

        class MockConnection:
            def __init__(self, lookup_name, canonical_name):
                self.lookup_name = lookup_name
                self.canonical_name = canonical_name
                self.statements = []

            async def exec_driver_sql(self, sql):
                self.statements.append(sql)
                is_select = sql.lstrip().startswith("SELECT")
                escaped_lookup = self.lookup_name.replace("'", "''")
                matched = is_select and f"WHERE sname = LOWER('{escaped_lookup}')" in sql
                rows = []
                if matched:
                    rows = [(self.canonical_name, timestamp_ns, 'cluster', 'sys', None, None)]
                return MockResult(rows, returns_rows=is_select)

        class MockBeginContext:
            def __init__(self, connection):
                self.connection = connection

            async def __aenter__(self):
                return self.connection

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        class MockEngine:
            def __init__(self, connection):
                self.connection = connection

            def begin(self):
                return MockBeginContext(self.connection)

        cases = (
            ("ReviewSnap", "ReviewSnap", "reviewsnap"),
            ("`ReviewSnap`", "ReviewSnap", "reviewsnap"),
            ("`Review``Snap`", "Review`Snap", "review`snap"),
            ("`Review'Snap`", "Review'Snap", "review'snap"),
            ('"ReviewSnap"', "ReviewSnap", "reviewsnap"),
            ('"Review""Snap"', 'Review"Snap', 'review"snap'),
            ("İSnap", "İSnap", "isnap"),
            ("ΟΣ", "ΟΣ", "οσ"),
            ("\u1c89Snap", "\u1c89Snap", "\u1c8asnap"),
        )
        for supplied_name, lookup_name, canonical_name in cases:
            with self.subTest(
                name=supplied_name,
                lookup=lookup_name,
                canonical=canonical_name,
            ):
                client = AsyncClient()
                connection = MockConnection(lookup_name, canonical_name)
                client._engine = MockEngine(connection)

                snapshot = await client.snapshots.create(supplied_name, SnapshotLevel.CLUSTER)

                self.assertEqual(len(connection.statements), 2)
                escaped_lookup = lookup_name.replace("'", "''")
                self.assertIn(
                    f"WHERE sname = LOWER('{escaped_lookup}')",
                    connection.statements[1],
                )
                self.assertEqual(snapshot.name, canonical_name)

    async def test_get_snapshot(self):
        """Test getting snapshot"""
        # Use integer timestamp instead of datetime object
        timestamp_ns = int(datetime.now().timestamp() * 1000000000)
        mock_result = AsyncResultSet(
            ['sname', 'ts', 'level', 'account_name', 'database_name', 'table_name'],
            [('test_snap', timestamp_ns, 'database', 'sys', 'test_db', None)],
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
                ('snap2', timestamp_ns, 'table', 'sys', 'db2', 'table2'),
            ],
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
            [('test_snap', timestamp_ns, 'database', 'sys', 'test_db', None)],
        )
        self.mock_client.execute.return_value = mock_result

        exists = await self.snapshot_manager.exists("test_snap")
        self.assertTrue(exists)

        # Test not exists
        from matrixone.exceptions import SnapshotError

        self.mock_client.execute.return_value = AsyncResultSet([], [])

        exists = await self.snapshot_manager.exists("nonexistent")
        self.assertFalse(exists)

        self.mock_client.execute.side_effect = SnapshotError('lookup permission denied')
        with self.assertRaisesRegex(SnapshotError, 'permission denied'):
            await self.snapshot_manager.exists('unreadable')


class TestAsyncCloneManager(unittest.IsolatedAsyncioTestCase):
    """Test AsyncCloneManager functionality"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        pass

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        pass

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = AsyncMock()
        self.mock_client.query_timeout = 300
        self.clone_manager = AsyncCloneManager(self.mock_client)

        async def execute(sql):
            if sql.startswith('SHOW CREATE'):
                return AsyncResultSet(['name', 'ddl'], [('target', 'CREATE target')])
            return AsyncResultSet([], [])

        self.mock_client.execute.side_effect = execute
        self.mock_client.snapshots.exists = AsyncMock(return_value=True)

    def assert_clone_calls(self, expected):
        from unittest.mock import call

        kind = 'DATABASE' if expected.startswith('CREATE DATABASE') else 'TABLE'
        target = 'target_db' if kind == 'DATABASE' else 'target_table'
        calls = [call(expected.replace('IF NOT EXISTS ', '')), call('SHOW CREATE ' + kind + ' ' + target)]
        if 'IF NOT EXISTS ' in expected:
            calls.insert(0, call(self.clone_manager._existence_sql(kind, target)))
        self.assertEqual(self.mock_client.execute.await_args_list, calls)

    async def test_clone_database(self):
        """Test cloning database"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])

        await self.clone_manager.clone_database("target_db", "source_db")

        self.assert_clone_calls("CREATE DATABASE target_db CLONE source_db")

    async def test_clone_database_with_snapshot(self):
        """Test cloning database with snapshot"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])

        await self.clone_manager.clone_database_with_snapshot("target_db", "source_db", "test_snapshot")

        self.assert_clone_calls("CREATE DATABASE target_db CLONE source_db {snapshot = 'test_snapshot'}")

    async def test_clone_database_if_not_exists(self):
        """Test cloning database with if not exists"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])

        await self.clone_manager.clone_database("target_db", "source_db", if_not_exists=True)

        self.assert_clone_calls("CREATE DATABASE IF NOT EXISTS target_db CLONE source_db")

    async def test_clone_database_with_snapshot_and_if_not_exists(self):
        """Test cloning database with both grammar options"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])

        await self.clone_manager.clone_database(
            "target_db", "source_db", snapshot_name="test_snapshot", if_not_exists=True
        )

        self.assert_clone_calls("CREATE DATABASE IF NOT EXISTS target_db CLONE source_db {snapshot = 'test_snapshot'}")

    async def test_clone_table(self):
        """Test cloning table"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])

        await self.clone_manager.clone_table("target_table", "source_table")

        self.assert_clone_calls("CREATE TABLE target_table CLONE source_table")

    async def test_clone_table_with_snapshot(self):
        """Test cloning table with snapshot"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])

        await self.clone_manager.clone_table_with_snapshot("target_table", "source_table", "test_snapshot")

        self.assert_clone_calls("CREATE TABLE target_table CLONE source_table {snapshot = 'test_snapshot'}")

    async def test_clone_table_if_not_exists(self):
        """Test cloning table with if not exists"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])

        await self.clone_manager.clone_table("target_table", "source_table", if_not_exists=True)

        self.assert_clone_calls("CREATE TABLE IF NOT EXISTS target_table CLONE source_table")

    async def test_clone_table_with_snapshot_and_if_not_exists(self):
        """Test cloning table with both grammar options"""
        self.mock_client.execute.return_value = AsyncResultSet([], [])

        await self.clone_manager.clone_table(
            "target_table", "source_table", snapshot_name="test_snapshot", if_not_exists=True
        )

        self.assert_clone_calls(
            "CREATE TABLE IF NOT EXISTS target_table CLONE source_table {snapshot = 'test_snapshot'}"
        )


class TestAsyncMoCtlManager(unittest.IsolatedAsyncioTestCase):
    """Test AsyncMoCtlManager functionality"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        pass

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        pass

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = AsyncMock()
        self.moctl_manager = AsyncMoCtlManager(self.mock_client)

    async def test_flush_table(self):
        """Test async flush table"""
        mock_result = AsyncResultSet(['result'], [('{"method": "Flush", "result": [{"returnStr": "OK"}]}',)])
        self.mock_client.execute.return_value = mock_result

        result = await self.moctl_manager.flush_table('db1', 'users')

        expected_sql = "SELECT mo_ctl('dn', 'flush', 'db1.users')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        self.assertEqual(result['method'], 'Flush')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')

    async def test_increment_checkpoint(self):
        """Test async increment checkpoint"""
        mock_result = AsyncResultSet(['result'], [('{"method": "Checkpoint", "result": [{"returnStr": "OK"}]}',)])
        self.mock_client.execute.return_value = mock_result

        result = await self.moctl_manager.increment_checkpoint()

        expected_sql = "SELECT mo_ctl('dn', 'checkpoint', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        self.assertEqual(result['method'], 'Checkpoint')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')

    async def test_global_checkpoint(self):
        """Test async global checkpoint"""
        mock_result = AsyncResultSet(['result'], [('{"method": "GlobalCheckpoint", "result": [{"returnStr": "OK"}]}',)])
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
            [('{"method": "Flush", "result": [{"returnStr": "ERROR: Table not found"}]}',)],
        )
        self.mock_client.execute.return_value = mock_result

        with self.assertRaises(MoCtlError) as context:
            await self.moctl_manager.flush_table('db1', 'nonexistent')

        self.assertIn("ERROR: Table not found", str(context.exception))


class TestAsyncSQLAlchemyIntegration(unittest.IsolatedAsyncioTestCase):
    """Test Async SQLAlchemy Integration Patterns"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        pass

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        pass

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

        # Set up connection parameters for SQLAlchemy integration
        self.client._connection_params = {
            'user': 'testuser',
            'password': 'testpass',
            'host': 'localhost',
            'port': 6001,
            'db': 'testdb',
        }


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
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncClientBasic))
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncClientConnection))
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncClientQuery))
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncSnapshotManager))
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncCloneManager))
    test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncMoCtlManager))
    # test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncTransaction))
    # test_suite.addTests(loader.loadTestsFromTestCase(TestAsyncSQLAlchemyTransaction))
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
        success_rate = (result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100
        print(f"Success rate: {success_rate:.1f}%")
    print(f"{'='*50}")

    # Exit with appropriate code
    if result.failures or result.errors:
        sys.exit(1)
    else:
        sys.exit(0)
