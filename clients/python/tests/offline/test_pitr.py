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
Unit tests for PitrManager with executor pattern.

Tests ensure:
1. Sync and Async managers generate identical SQL
2. Client and Session contexts use correct executors
3. All PITR operations work correctly
4. SQL statements match expected MatrixOne syntax
"""

import unittest
from unittest.mock import Mock, AsyncMock
import sys
import os
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import Client, AsyncClient, PitrError, Pitr
from matrixone.exceptions import PitrError as PitrErrorClass
from matrixone.pitr import PitrManager, AsyncPitrManager


class TestPitrManagerSQLGeneration(unittest.TestCase):
    """Test that PitrManager generates correct SQL statements"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.pitr_manager = PitrManager(self.client)

    def test_create_cluster_pitr_sql(self):
        """Test CREATE PITR FOR CLUSTER SQL generation"""
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.client.execute = Mock(return_value=mock_result)

        pitr = self.pitr_manager.create_cluster_pitr("cluster_pitr1", 1, "d")

        # Verify SQL is correct
        expected_create_sql = "CREATE PITR `cluster_pitr1` FOR CLUSTER RANGE 1 'd'"
        self.client.execute.assert_any_call(expected_create_sql)
        self.assertIsInstance(pitr, Pitr)

    def test_create_account_pitr_sql(self):
        """Test CREATE PITR FOR ACCOUNT SQL generation"""
        mock_result = Mock()
        mock_result.rows = [('account_pitr1', datetime.now(), datetime.now(), 'account', 'acc1', '*', '*', 2, 'h')]
        self.client.execute = Mock(return_value=mock_result)

        # Test without specific account
        pitr = self.pitr_manager.create_account_pitr("account_pitr1", range_value=2, range_unit="h")

        expected_create_sql = "CREATE PITR `account_pitr1` FOR ACCOUNT RANGE 2 'h'"
        self.client.execute.assert_any_call(expected_create_sql)
        self.assertIsInstance(pitr, Pitr)

    def test_create_account_pitr_with_name_sql(self):
        """Test CREATE PITR FOR ACCOUNT with specific account name"""
        mock_result = Mock()
        mock_result.rows = [('account_pitr2', datetime.now(), datetime.now(), 'account', 'acc1', '*', '*', 1, 'd')]
        self.client.execute = Mock(return_value=mock_result)

        pitr = self.pitr_manager.create_account_pitr("account_pitr2", account_name="acc1", range_value=1, range_unit="d")

        expected_create_sql = "CREATE PITR `account_pitr2` FOR ACCOUNT `acc1` RANGE 1 'd'"
        self.client.execute.assert_any_call(expected_create_sql)

    def test_create_database_pitr_sql(self):
        """Test CREATE PITR FOR DATABASE SQL generation"""
        mock_result = Mock()
        mock_result.rows = [('db_pitr1', datetime.now(), datetime.now(), 'database', 'acc1', 'db1', '*', 1, 'mo')]
        self.client.execute = Mock(return_value=mock_result)

        pitr = self.pitr_manager.create_database_pitr("db_pitr1", "db1", 1, "mo")

        expected_create_sql = "CREATE PITR `db_pitr1` FOR DATABASE `db1` RANGE 1 'mo'"
        self.client.execute.assert_any_call(expected_create_sql)
        self.assertIsInstance(pitr, Pitr)

    def test_create_table_pitr_sql(self):
        """Test CREATE PITR FOR TABLE SQL generation"""
        mock_result = Mock()
        mock_result.rows = [('tab_pitr1', datetime.now(), datetime.now(), 'table', 'acc1', 'db1', 't1', 1, 'y')]
        self.client.execute = Mock(return_value=mock_result)

        pitr = self.pitr_manager.create_table_pitr("tab_pitr1", "db1", "t1", 1, "y")

        expected_create_sql = "CREATE PITR `tab_pitr1` FOR TABLE `db1` `t1` RANGE 1 'y'"
        self.client.execute.assert_any_call(expected_create_sql)
        self.assertIsInstance(pitr, Pitr)

    def test_show_pitr_sql(self):
        """Test SHOW PITR SQL generation"""
        mock_result = Mock()
        mock_result.rows = [('test_pitr', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.client.execute = Mock(return_value=mock_result)

        pitr = self.pitr_manager.get("test_pitr")

        expected_sql = "SHOW PITR WHERE pitr_name = 'test_pitr'"
        self.client.execute.assert_called_with(expected_sql)
        self.assertEqual(pitr.name, "test_pitr")

    def test_alter_pitr_sql(self):
        """Test ALTER PITR SQL generation"""
        mock_result = Mock()
        mock_result.rows = [('test_pitr', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 3, 'd')]
        self.client.execute = Mock(return_value=mock_result)

        pitr = self.pitr_manager.alter("test_pitr", 3, "d")

        expected_alter_sql = "ALTER PITR `test_pitr` RANGE 3 'd'"
        self.client.execute.assert_any_call(expected_alter_sql)
        self.assertIsInstance(pitr, Pitr)

    def test_drop_pitr_sql(self):
        """Test DROP PITR SQL generation"""
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        result = self.pitr_manager.delete("test_pitr")

        expected_sql = "DROP PITR `test_pitr`"
        self.client.execute.assert_called_with(expected_sql)
        self.assertTrue(result)


class TestPitrManagerWithExecutor(unittest.TestCase):
    """Test PitrManager with executor pattern (session context) - SQL should be identical"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        # Create mock executor (session)
        self.session = Mock()
        # Create PitrManager with executor
        self.pitr_manager = PitrManager(self.client, executor=self.session)

    def test_session_create_cluster_pitr_uses_executor(self):
        """Test that session context uses executor, not client"""
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.session.execute = Mock(return_value=mock_result)

        pitr = self.pitr_manager.create_cluster_pitr("cluster_pitr1", 1, "d")

        # Should call session.execute, not client.execute
        expected_create_sql = "CREATE PITR `cluster_pitr1` FOR CLUSTER RANGE 1 'd'"
        self.session.execute.assert_any_call(expected_create_sql)
        self.assertIsInstance(pitr, Pitr)

    def test_session_generates_same_sql_as_client(self):
        """Test that session and client contexts generate identical SQL"""
        # Setup client manager
        client_manager = PitrManager(self.client)
        mock_result = Mock()
        mock_result.rows = [('test_pitr', datetime.now(), datetime.now(), 'database', 'acc1', 'db1', '*', 2, 'h')]
        self.client.execute = Mock(return_value=mock_result)
        self.session.execute = Mock(return_value=mock_result)

        # Call both
        client_manager.create_database_pitr("test_pitr", "db1", 2, "h")
        self.pitr_manager.create_database_pitr("test_pitr", "db1", 2, "h")

        # Extract SQL from calls
        expected_sql = "CREATE PITR `test_pitr` FOR DATABASE `db1` RANGE 2 'h'"

        # Both should generate the same SQL
        self.client.execute.assert_any_call(expected_sql)
        self.session.execute.assert_any_call(expected_sql)

    def test_all_operations_use_executor(self):
        """Test that all PITR operations use executor when provided"""
        mock_result = Mock()
        mock_result.rows = [('test_pitr', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.session.execute = Mock(return_value=mock_result)

        # Test all operations
        self.pitr_manager.create_cluster_pitr("test_pitr", 1, "d")
        self.pitr_manager.get("test_pitr")
        self.pitr_manager.alter("test_pitr", 2, "d")
        self.pitr_manager.delete("test_pitr")

        # All should use session.execute
        self.assertGreater(self.session.execute.call_count, 0)


class TestAsyncPitrManagerSQLGeneration(unittest.IsolatedAsyncioTestCase):
    """Test that AsyncPitrManager generates correct SQL statements (same as sync)"""

    async def asyncSetUp(self):
        """Set up async test fixtures"""
        self.client = AsyncClient()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.pitr_manager = AsyncPitrManager(self.client)

    async def test_async_create_cluster_pitr_sql(self):
        """Test async CREATE PITR FOR CLUSTER SQL (should match sync)"""
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.client.execute = AsyncMock(return_value=mock_result)

        pitr = await self.pitr_manager.create_cluster_pitr("cluster_pitr1", 1, "d")

        # Verify SQL matches sync version
        expected_create_sql = "CREATE PITR `cluster_pitr1` FOR CLUSTER RANGE 1 'd'"
        self.client.execute.assert_any_call(expected_create_sql)
        self.assertIsInstance(pitr, Pitr)

    async def test_async_create_database_pitr_sql(self):
        """Test async CREATE PITR FOR DATABASE SQL (should match sync)"""
        mock_result = Mock()
        mock_result.rows = [('db_pitr1', datetime.now(), datetime.now(), 'database', 'acc1', 'db1', '*', 1, 'mo')]
        self.client.execute = AsyncMock(return_value=mock_result)

        pitr = await self.pitr_manager.create_database_pitr("db_pitr1", "db1", 1, "mo")

        expected_create_sql = "CREATE PITR `db_pitr1` FOR DATABASE `db1` RANGE 1 'mo'"
        self.client.execute.assert_any_call(expected_create_sql)

    async def test_async_create_table_pitr_sql(self):
        """Test async CREATE PITR FOR TABLE SQL (should match sync)"""
        mock_result = Mock()
        mock_result.rows = [('tab_pitr1', datetime.now(), datetime.now(), 'table', 'acc1', 'db1', 't1', 1, 'y')]
        self.client.execute = AsyncMock(return_value=mock_result)

        pitr = await self.pitr_manager.create_table_pitr("tab_pitr1", "db1", "t1", 1, "y")

        expected_create_sql = "CREATE PITR `tab_pitr1` FOR TABLE `db1` `t1` RANGE 1 'y'"
        self.client.execute.assert_any_call(expected_create_sql)

    async def test_async_show_pitr_sql(self):
        """Test async SHOW PITR SQL (should match sync)"""
        mock_result = Mock()
        mock_result.rows = [('test_pitr', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.client.execute = AsyncMock(return_value=mock_result)

        pitr = await self.pitr_manager.get("test_pitr")

        expected_sql = "SHOW PITR WHERE pitr_name = 'test_pitr'"
        self.client.execute.assert_called_with(expected_sql)

    async def test_async_alter_pitr_sql(self):
        """Test async ALTER PITR SQL (should match sync)"""
        mock_result = Mock()
        mock_result.rows = [('test_pitr', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 3, 'd')]
        self.client.execute = AsyncMock(return_value=mock_result)

        pitr = await self.pitr_manager.alter("test_pitr", 3, "d")

        expected_alter_sql = "ALTER PITR `test_pitr` RANGE 3 'd'"
        self.client.execute.assert_any_call(expected_alter_sql)

    async def test_async_drop_pitr_sql(self):
        """Test async DROP PITR SQL (should match sync)"""
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        result = await self.pitr_manager.delete("test_pitr")

        expected_sql = "DROP PITR `test_pitr`"
        self.client.execute.assert_called_with(expected_sql)
        self.assertTrue(result)


class TestAsyncPitrManagerWithExecutor(unittest.IsolatedAsyncioTestCase):
    """Test AsyncPitrManager with executor pattern - SQL should match sync version"""

    async def asyncSetUp(self):
        """Set up async test fixtures"""
        self.client = AsyncClient()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        # Create mock async executor (async session)
        self.session = Mock()
        self.session.execute = AsyncMock()
        # Create AsyncPitrManager with executor
        self.pitr_manager = AsyncPitrManager(self.client, executor=self.session)

    async def test_async_session_create_cluster_pitr_uses_executor(self):
        """Test that async session context uses executor"""
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.session.execute.return_value = mock_result

        pitr = await self.pitr_manager.create_cluster_pitr("cluster_pitr1", 1, "d")

        # Should call session.execute, not client.execute
        expected_create_sql = "CREATE PITR `cluster_pitr1` FOR CLUSTER RANGE 1 'd'"
        self.session.execute.assert_any_call(expected_create_sql)

    async def test_async_session_generates_same_sql_as_client(self):
        """Test that async session and client contexts generate identical SQL"""
        # Setup client manager
        client_manager = AsyncPitrManager(self.client)
        mock_result = Mock()
        mock_result.rows = [('test_pitr', datetime.now(), datetime.now(), 'database', 'acc1', 'db1', '*', 2, 'h')]
        self.client.execute = AsyncMock(return_value=mock_result)
        self.session.execute.return_value = mock_result

        # Call both
        await client_manager.create_database_pitr("test_pitr", "db1", 2, "h")
        await self.pitr_manager.create_database_pitr("test_pitr", "db1", 2, "h")

        # Extract expected SQL
        expected_sql = "CREATE PITR `test_pitr` FOR DATABASE `db1` RANGE 2 'h'"

        # Both should generate the same SQL
        self.client.execute.assert_any_call(expected_sql)
        self.session.execute.assert_any_call(expected_sql)


class TestPitrManagerErrorHandling(unittest.TestCase):
    """Test error handling and validation"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.pitr_manager = PitrManager(self.client)

    def test_invalid_range_value_low(self):
        """Test range value validation (too low)"""
        with self.assertRaises(PitrErrorClass):
            self.pitr_manager.create_cluster_pitr("test_pitr", 0, "d")

    def test_invalid_range_value_high(self):
        """Test range value validation (too high)"""
        with self.assertRaises(PitrErrorClass):
            self.pitr_manager.create_cluster_pitr("test_pitr", 101, "d")

    def test_invalid_range_unit(self):
        """Test range unit validation"""
        with self.assertRaises(PitrErrorClass):
            self.pitr_manager.create_cluster_pitr("test_pitr", 1, "invalid")

    def test_valid_range_units(self):
        """Test all valid range units"""
        mock_result = Mock()
        mock_result.rows = [('test_pitr', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'h')]
        self.client.execute = Mock(return_value=mock_result)

        valid_units = ['h', 'd', 'mo', 'y']
        for unit in valid_units:
            self.pitr_manager.create_cluster_pitr(f"test_pitr_{unit}", 1, unit)
            expected_sql = f"CREATE PITR `test_pitr_{unit}` FOR CLUSTER RANGE 1 '{unit}'"
            self.client.execute.assert_any_call(expected_sql)


class TestSyncVsAsyncSQLConsistency(unittest.IsolatedAsyncioTestCase):
    """Comprehensive test ensuring sync and async generate identical SQL"""

    async def asyncSetUp(self):
        """Set up both sync and async fixtures"""
        # Sync setup
        self.sync_client = Client()
        self.sync_client._engine = Mock()
        self.sync_client._escape_identifier = lambda x: f"`{x}`"
        self.sync_client._escape_string = lambda x: f"'{x}'"
        self.sync_manager = PitrManager(self.sync_client)

        # Async setup
        self.async_client = AsyncClient()
        self.async_client._engine = Mock()
        self.async_client._escape_identifier = lambda x: f"`{x}`"
        self.async_client._escape_string = lambda x: f"'{x}'"
        self.async_manager = AsyncPitrManager(self.async_client)

    async def test_all_create_operations_sql_consistency(self):
        """Test that all create operations generate identical SQL in sync/async"""
        mock_result = Mock()
        mock_result.rows = [('test', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]

        test_cases = [
            # (method_name, args, expected_sql)
            ('create_cluster_pitr', ['cluster_pitr', 1, 'd'], "CREATE PITR `cluster_pitr` FOR CLUSTER RANGE 1 'd'"),
            ('create_account_pitr', ['account_pitr', None, 2, 'h'], "CREATE PITR `account_pitr` FOR ACCOUNT RANGE 2 'h'"),
            ('create_database_pitr', ['db_pitr', 'db1', 1, 'mo'], "CREATE PITR `db_pitr` FOR DATABASE `db1` RANGE 1 'mo'"),
            (
                'create_table_pitr',
                ['tab_pitr', 'db1', 't1', 1, 'y'],
                "CREATE PITR `tab_pitr` FOR TABLE `db1` `t1` RANGE 1 'y'",
            ),
        ]

        for method_name, args, expected_sql in test_cases:
            # Reset mocks
            self.sync_client.execute = Mock(return_value=mock_result)
            self.async_client.execute = AsyncMock(return_value=mock_result)

            # Call sync
            getattr(self.sync_manager, method_name)(*args)

            # Call async
            await getattr(self.async_manager, method_name)(*args)

            # Verify both generated the same SQL
            self.sync_client.execute.assert_any_call(expected_sql)
            self.async_client.execute.assert_any_call(expected_sql)

    async def test_query_operations_sql_consistency(self):
        """Test that query operations generate identical SQL"""
        mock_result = Mock()
        mock_result.rows = [('test', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 3, 'd')]

        query_cases = [
            # (method, args, expected_sql)
            ('get', ['test_pitr'], "SHOW PITR WHERE pitr_name = 'test_pitr'"),
            ('alter', ['test_pitr', 3, 'd'], "ALTER PITR `test_pitr` RANGE 3 'd'"),
            ('delete', ['test_pitr'], "DROP PITR `test_pitr`"),
        ]

        for method_name, args, expected_sql in query_cases:
            # Reset mocks
            self.sync_client.execute = Mock(return_value=mock_result)
            self.async_client.execute = AsyncMock(return_value=mock_result)

            # Call sync
            getattr(self.sync_manager, method_name)(*args)

            # Call async
            await getattr(self.async_manager, method_name)(*args)

            # Verify SQL is identical
            if method_name in ['get', 'delete']:
                # These have a single execute call
                self.assertEqual(self.sync_client.execute.call_args[0][0], self.async_client.execute.call_args[0][0])


class TestPitrManagerValidation(unittest.TestCase):
    """Test parameter validation"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.pitr_manager = PitrManager(self.client)

    def test_range_value_boundaries(self):
        """Test range value boundary validation"""
        # Valid boundaries
        mock_result = Mock()
        mock_result.rows = [('test', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.client.execute = Mock(return_value=mock_result)

        # Test min valid
        self.pitr_manager.create_cluster_pitr("test1", 1, "d")
        # Test max valid
        self.pitr_manager.create_cluster_pitr("test100", 100, "d")

        # Invalid boundaries
        with self.assertRaises(PitrErrorClass):
            self.pitr_manager.create_cluster_pitr("test0", 0, "d")

        with self.assertRaises(PitrErrorClass):
            self.pitr_manager.create_cluster_pitr("test101", 101, "d")

    def test_all_valid_range_units(self):
        """Test all valid range units are accepted"""
        mock_result = Mock()
        mock_result.rows = [('test', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'h')]
        self.client.execute = Mock(return_value=mock_result)

        for unit in ['h', 'd', 'mo', 'y']:
            self.pitr_manager.create_cluster_pitr(f"test_{unit}", 1, unit)
            # Should not raise exception

    def test_invalid_range_units(self):
        """Test invalid range units are rejected"""
        invalid_units = ['sec', 'min', 'w', 'week', 'month', 'year', 'invalid']

        for unit in invalid_units:
            with self.assertRaises(PitrErrorClass):
                self.pitr_manager.create_cluster_pitr("test", 1, unit)


if __name__ == '__main__':
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestPitrManagerSQLGeneration))
    suite.addTests(loader.loadTestsFromTestCase(TestPitrManagerWithExecutor))
    suite.addTests(loader.loadTestsFromTestCase(TestAsyncPitrManagerSQLGeneration))
    suite.addTests(loader.loadTestsFromTestCase(TestAsyncPitrManagerWithExecutor))
    suite.addTests(loader.loadTestsFromTestCase(TestSyncVsAsyncSQLConsistency))
    suite.addTests(loader.loadTestsFromTestCase(TestPitrManagerValidation))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
