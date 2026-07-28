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
Unit tests for PubSubManager with executor pattern.

Tests ensure:
1. Sync and Async managers generate identical SQL
2. Client and Session contexts use correct executors
3. All PubSub operations work correctly
4. SQL statements match expected MatrixOne syntax
"""

import unittest
from unittest.mock import Mock, AsyncMock
import sys
import os
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import Client, AsyncClient, PubSubError, Publication, Subscription
from matrixone.exceptions import PubSubError as PubSubErrorClass
from matrixone.pubsub import PubSubManager, AsyncPubSubManager


class TestPubSubManagerSQLGeneration(unittest.TestCase):
    """Test that PubSubManager generates correct SQL statements"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.pubsub_manager = PubSubManager(self.client)

    def test_create_database_publication_sql(self):
        """Test CREATE PUBLICATION DATABASE SQL generation"""
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = Mock(return_value=mock_result)

        pub = self.pubsub_manager.create_database_publication("db_pub1", "central_db", "acc1")

        # Verify SQL is correct
        expected_create_sql = "CREATE PUBLICATION `db_pub1` DATABASE `central_db` ACCOUNT `acc1`"
        self.client.execute.assert_any_call(expected_create_sql)
        self.assertIsInstance(pub, Publication)

    def test_create_table_publication_sql(self):
        """Test CREATE PUBLICATION TABLE SQL generation"""
        mock_result = Mock()
        mock_result.rows = [('table_pub1', 'central_db', 'users', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = Mock(return_value=mock_result)

        pub = self.pubsub_manager.create_table_publication("table_pub1", "central_db", "users", "acc1")

        # Verify SQL is correct
        expected_create_sql = "CREATE PUBLICATION `table_pub1` DATABASE `central_db` TABLE `users` ACCOUNT `acc1`"
        self.client.execute.assert_any_call(expected_create_sql)
        self.assertIsInstance(pub, Publication)

    def test_get_publication_sql(self):
        """Test SHOW PUBLICATIONS SQL generation"""
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = Mock(return_value=mock_result)

        pub = self.pubsub_manager.get_publication("db_pub1")

        # get_publication uses SHOW PUBLICATIONS (no WHERE support) and filters
        expected_sql = "SHOW PUBLICATIONS"
        self.client.execute.assert_called_with(expected_sql)
        self.assertEqual(pub.name, "db_pub1")

    def test_drop_publication_sql(self):
        """Test DROP PUBLICATION SQL generation"""
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        result = self.pubsub_manager.drop_publication("db_pub1")

        expected_sql = "DROP PUBLICATION `db_pub1`"
        self.client.execute.assert_called_with(expected_sql)
        self.assertTrue(result)

    def test_create_subscription_sql(self):
        """Test CREATE DATABASE (subscription) SQL generation"""
        mock_result = Mock()
        mock_result.rows = [('pub1', 'acc1', 'central_db', '*', None, datetime.now(), 'sub_db1', datetime.now())]
        self.client.execute = Mock(return_value=mock_result)

        sub = self.pubsub_manager.create_subscription("sub_db1", "pub1", "acc1")

        # Verify SQL is correct
        expected_create_sql = "CREATE DATABASE `sub_db1` FROM `acc1` PUBLICATION `pub1`"
        self.client.execute.assert_any_call(expected_create_sql)
        self.assertIsInstance(sub, Subscription)


class TestPubSubManagerWithExecutor(unittest.TestCase):
    """Test PubSubManager with executor pattern (session context) - SQL should be identical"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        # Create mock executor (session)
        self.session = Mock()
        # Create PubSubManager with executor
        self.pubsub_manager = PubSubManager(self.client, executor=self.session)

    def test_session_create_database_publication_uses_executor(self):
        """Test that session context uses executor, not client"""
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
        self.session.execute = Mock(return_value=mock_result)

        pub = self.pubsub_manager.create_database_publication("db_pub1", "central_db", "acc1")

        # Should call session.execute, not client.execute
        expected_create_sql = "CREATE PUBLICATION `db_pub1` DATABASE `central_db` ACCOUNT `acc1`"
        self.session.execute.assert_any_call(expected_create_sql)
        self.assertIsInstance(pub, Publication)

    def test_session_generates_same_sql_as_client(self):
        """Test that session and client contexts generate identical SQL"""
        # Setup client manager
        client_manager = PubSubManager(self.client)
        mock_result = Mock()
        mock_result.rows = [('table_pub', 'db1', 'users', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = Mock(return_value=mock_result)
        self.session.execute = Mock(return_value=mock_result)

        # Call both
        client_manager.create_table_publication("table_pub", "db1", "users", "acc1")
        self.pubsub_manager.create_table_publication("table_pub", "db1", "users", "acc1")

        # Extract SQL from calls
        expected_sql = "CREATE PUBLICATION `table_pub` DATABASE `db1` TABLE `users` ACCOUNT `acc1`"

        # Both should generate the same SQL
        self.client.execute.assert_any_call(expected_sql)
        self.session.execute.assert_any_call(expected_sql)


class TestAsyncPubSubManagerSQLGeneration(unittest.IsolatedAsyncioTestCase):
    """Test that AsyncPubSubManager generates correct SQL statements (same as sync)"""

    async def asyncSetUp(self):
        """Set up async test fixtures"""
        self.client = AsyncClient()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.pubsub_manager = AsyncPubSubManager(self.client)

    async def test_async_create_database_publication_sql(self):
        """Test async CREATE PUBLICATION DATABASE SQL (should match sync)"""
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = AsyncMock(return_value=mock_result)

        pub = await self.pubsub_manager.create_database_publication("db_pub1", "central_db", "acc1")

        # Verify SQL matches sync version
        expected_create_sql = "CREATE PUBLICATION `db_pub1` DATABASE `central_db` ACCOUNT `acc1`"
        self.client.execute.assert_any_call(expected_create_sql)
        self.assertIsInstance(pub, Publication)

    async def test_async_create_table_publication_sql(self):
        """Test async CREATE PUBLICATION TABLE SQL (should match sync)"""
        mock_result = Mock()
        mock_result.rows = [('table_pub1', 'central_db', 'users', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = AsyncMock(return_value=mock_result)

        pub = await self.pubsub_manager.create_table_publication("table_pub1", "central_db", "users", "acc1")

        expected_create_sql = "CREATE PUBLICATION `table_pub1` DATABASE `central_db` TABLE `users` ACCOUNT `acc1`"
        self.client.execute.assert_any_call(expected_create_sql)

    async def test_async_get_publication_sql(self):
        """Test async SHOW PUBLICATIONS SQL (should match sync)"""
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = AsyncMock(return_value=mock_result)

        pub = await self.pubsub_manager.get_publication("db_pub1")

        # Both sync and async use SHOW PUBLICATIONS and filter
        expected_sql = "SHOW PUBLICATIONS"
        self.client.execute.assert_called_with(expected_sql)
        self.assertEqual(pub.name, "db_pub1")

    async def test_async_drop_publication_sql(self):
        """Test async DROP PUBLICATION SQL (should match sync)"""
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        result = await self.pubsub_manager.drop_publication("db_pub1")

        expected_sql = "DROP PUBLICATION `db_pub1`"
        self.client.execute.assert_called_with(expected_sql)
        self.assertTrue(result)

    async def test_async_create_subscription_sql(self):
        """Test async CREATE DATABASE (subscription) SQL (should match sync)"""
        mock_result = Mock()
        mock_result.rows = [('pub1', 'acc1', 'central_db', '*', None, datetime.now(), 'sub_db1', datetime.now())]
        self.client.execute = AsyncMock(return_value=mock_result)

        sub = await self.pubsub_manager.create_subscription("sub_db1", "pub1", "acc1")

        expected_create_sql = "CREATE DATABASE `sub_db1` FROM `acc1` PUBLICATION `pub1`"
        self.client.execute.assert_any_call(expected_create_sql)


class TestAsyncPubSubManagerWithExecutor(unittest.IsolatedAsyncioTestCase):
    """Test AsyncPubSubManager with executor pattern - SQL should match sync version"""

    async def asyncSetUp(self):
        """Set up async test fixtures"""
        self.client = AsyncClient()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        # Create mock async executor (async session)
        self.session = Mock()
        self.session.execute = AsyncMock()
        # Create AsyncPubSubManager with executor
        self.pubsub_manager = AsyncPubSubManager(self.client, executor=self.session)

    async def test_async_session_create_publication_uses_executor(self):
        """Test that async session context uses executor"""
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
        self.session.execute.return_value = mock_result

        pub = await self.pubsub_manager.create_database_publication("db_pub1", "central_db", "acc1")

        # Should call session.execute, not client.execute
        expected_create_sql = "CREATE PUBLICATION `db_pub1` DATABASE `central_db` ACCOUNT `acc1`"
        self.session.execute.assert_any_call(expected_create_sql)

    async def test_async_session_generates_same_sql_as_client(self):
        """Test that async session and client contexts generate identical SQL"""
        # Setup client manager
        client_manager = AsyncPubSubManager(self.client)
        mock_result = Mock()
        mock_result.rows = [('table_pub', 'db1', 'users', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = AsyncMock(return_value=mock_result)
        self.session.execute.return_value = mock_result

        # Call both
        await client_manager.create_table_publication("table_pub", "db1", "users", "acc1")
        await self.pubsub_manager.create_table_publication("table_pub", "db1", "users", "acc1")

        # Extract expected SQL
        expected_sql = "CREATE PUBLICATION `table_pub` DATABASE `db1` TABLE `users` ACCOUNT `acc1`"

        # Both should generate the same SQL
        self.client.execute.assert_any_call(expected_sql)
        self.session.execute.assert_any_call(expected_sql)


class TestSyncVsAsyncPubSubSQLConsistency(unittest.IsolatedAsyncioTestCase):
    """Comprehensive test ensuring sync and async generate identical SQL"""

    async def asyncSetUp(self):
        """Set up both sync and async fixtures"""
        # Sync setup
        self.sync_client = Client()
        self.sync_client._engine = Mock()
        self.sync_client._escape_identifier = lambda x: f"`{x}`"
        self.sync_client._escape_string = lambda x: f"'{x}'"
        self.sync_manager = PubSubManager(self.sync_client)

        # Async setup
        self.async_client = AsyncClient()
        self.async_client._engine = Mock()
        self.async_client._escape_identifier = lambda x: f"`{x}`"
        self.async_client._escape_string = lambda x: f"'{x}'"
        self.async_manager = AsyncPubSubManager(self.async_client)

    async def test_create_operations_sql_consistency(self):
        """Test that all create operations generate identical SQL in sync/async"""
        test_cases = [
            # (method_name, args, pub_name, mock_rows, expected_sql)
            (
                'create_database_publication',
                ['db_pub', 'db1', 'acc1'],
                'db_pub',
                [('db_pub', 'db1', '*', 'acc1', '', datetime.now(), None, None)],
                "CREATE PUBLICATION `db_pub` DATABASE `db1` ACCOUNT `acc1`",
            ),
            (
                'create_table_publication',
                ['table_pub', 'db1', 'users', 'acc1'],
                'table_pub',
                [('table_pub', 'db1', 'users', 'acc1', '', datetime.now(), None, None)],
                "CREATE PUBLICATION `table_pub` DATABASE `db1` TABLE `users` ACCOUNT `acc1`",
            ),
        ]

        for method_name, args, pub_name, mock_rows, expected_sql in test_cases:
            # Setup mock to return the publication for get_publication call
            mock_result = Mock()
            mock_result.rows = mock_rows

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
        mock_pub_result = Mock()
        mock_pub_result.rows = [('test_pub', 'db1', '*', 'acc1', '', datetime.now(), None, None)]

        mock_sub_result = Mock()
        mock_sub_result.rows = [('pub1', 'acc1', 'db1', '*', None, datetime.now(), 'sub_db1', datetime.now())]

        query_cases = [
            # (method, args, mock_result, expected_sql)
            ('get_publication', ['test_pub'], mock_pub_result, "SHOW PUBLICATIONS"),
            ('list_publications', [], mock_pub_result, "SHOW PUBLICATIONS"),
            ('drop_publication', ['test_pub'], mock_pub_result, "DROP PUBLICATION `test_pub`"),
            ('get_subscription', ['sub_db1'], mock_sub_result, "SHOW SUBSCRIPTIONS"),
            ('list_subscriptions', [], mock_sub_result, "SHOW SUBSCRIPTIONS"),
        ]

        for method_name, args, mock_result, expected_sql in query_cases:
            # Reset mocks
            self.sync_client.execute = Mock(return_value=mock_result)
            self.async_client.execute = AsyncMock(return_value=mock_result)

            # Call sync
            getattr(self.sync_manager, method_name)(*args)

            # Call async
            await getattr(self.async_manager, method_name)(*args)

            # Verify SQL is identical
            if method_name in ['drop_publication']:
                # Single execute call
                self.assertEqual(self.sync_client.execute.call_args[0][0], self.async_client.execute.call_args[0][0])


class TestPubSubManagerErrorHandling(unittest.TestCase):
    """Test error handling"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.pubsub_manager = PubSubManager(self.client)

    def test_get_publication_not_found(self):
        """Test getting non-existent publication"""
        mock_result = Mock()
        mock_result.rows = []
        self.client.execute = Mock(return_value=mock_result)

        with self.assertRaises(PubSubErrorClass):
            self.pubsub_manager.get_publication("nonexistent")

    def test_create_publication_failure(self):
        """Test publication creation failure"""
        self.client.execute = Mock(side_effect=Exception("Permission denied"))

        with self.assertRaises(PubSubErrorClass):
            self.pubsub_manager.create_database_publication("db_pub1", "db1", "acc1")


class TestPubSubManagerFiltering(unittest.TestCase):
    """Test publication/subscription filtering logic"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.pubsub_manager = PubSubManager(self.client)

    def test_get_publication_finds_correct_publication(self):
        """Test that get_publication filters correctly"""
        mock_result = Mock()
        mock_result.rows = [
            ('pub1', 'db1', '*', 'acc1', '', datetime.now(), None, None),
            ('pub2', 'db2', '*', 'acc2', '', datetime.now(), None, None),
            ('target_pub', 'db3', '*', 'acc3', '', datetime.now(), None, None),
        ]
        self.client.execute = Mock(return_value=mock_result)

        pub = self.pubsub_manager.get_publication("target_pub")

        self.assertEqual(pub.name, "target_pub")
        self.assertEqual(pub.database, "db3")

    def test_list_publications_filters_by_database(self):
        """Test that list_publications filters by database"""
        mock_result = Mock()
        mock_result.rows = [
            ('pub1', 'db1', '*', 'acc1', '', datetime.now(), None, None),
            ('pub2', 'db2', '*', 'acc2', '', datetime.now(), None, None),
            ('pub3', 'db1', '*', 'acc3', '', datetime.now(), None, None),
        ]
        self.client.execute = Mock(return_value=mock_result)

        pubs = self.pubsub_manager.list_publications(database='db1')

        # Should return pub1 and pub3 (both for db1)
        self.assertEqual(len(pubs), 2)
        self.assertTrue(all(p.database == 'db1' for p in pubs))


if __name__ == '__main__':
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestPubSubManagerSQLGeneration))
    suite.addTests(loader.loadTestsFromTestCase(TestPubSubManagerWithExecutor))
    suite.addTests(loader.loadTestsFromTestCase(TestAsyncPubSubManagerSQLGeneration))
    suite.addTests(loader.loadTestsFromTestCase(TestAsyncPubSubManagerWithExecutor))
    suite.addTests(loader.loadTestsFromTestCase(TestSyncVsAsyncPubSubSQLConsistency))
    suite.addTests(loader.loadTestsFromTestCase(TestPubSubManagerErrorHandling))
    suite.addTests(loader.loadTestsFromTestCase(TestPubSubManagerFiltering))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
