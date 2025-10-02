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
Unit tests for PubSubManager
"""

import unittest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import Client, AsyncClient, PubSubError, Publication, Subscription
from matrixone.exceptions import PubSubError as PubSubErrorClass
from matrixone.pubsub import TransactionPubSubManager


class TestPubSubManager(unittest.TestCase):
    """Test cases for PubSubManager"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        # Initialize managers manually for testing
        from matrixone.pubsub import PubSubManager

        self.pubsub_manager = PubSubManager(self.client)

    def test_create_database_publication_success(self):
        """Test successful database publication creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = Mock(return_value=mock_result)

        # Test create database publication
        pub = self.pubsub_manager.create_database_publication("db_pub1", "central_db", "acc1")

        # Verify
        self.assertIsInstance(pub, Publication)
        self.assertEqual(pub.name, "db_pub1")
        self.assertEqual(pub.database, "central_db")
        self.assertEqual(pub.tables, "*")
        self.assertEqual(pub.sub_account, "acc1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE PUBLICATION `db_pub1` DATABASE `central_db` ACCOUNT `acc1`")

    def test_create_table_publication_success(self):
        """Test successful table publication creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('table_pub1', 'central_db', 'products', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = Mock(return_value=mock_result)

        # Test create table publication
        pub = self.pubsub_manager.create_table_publication("table_pub1", "central_db", "products", "acc1")

        # Verify
        self.assertIsInstance(pub, Publication)
        self.assertEqual(pub.name, "table_pub1")
        self.assertEqual(pub.database, "central_db")
        self.assertEqual(pub.tables, "products")
        self.assertEqual(pub.sub_account, "acc1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call(
            "CREATE PUBLICATION `table_pub1` DATABASE `central_db` TABLE `products` ACCOUNT `acc1`"
        )

    def test_get_publication_success(self):
        """Test successful publication retrieval"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = Mock(return_value=mock_result)

        # Test get publication
        pub = self.pubsub_manager.get_publication("db_pub1")

        # Verify
        self.assertIsInstance(pub, Publication)
        self.assertEqual(pub.name, "db_pub1")
        self.client.execute.assert_called_with("SHOW PUBLICATIONS")

    def test_get_publication_not_found(self):
        """Test publication not found"""
        # Mock empty result
        mock_result = Mock()
        mock_result.rows = []
        self.client.execute = Mock(return_value=mock_result)

        # Test get publication not found
        with self.assertRaises(PubSubErrorClass):
            self.pubsub_manager.get_publication("nonexistent_pub")

    def test_list_publications_success(self):
        """Test successful publication listing"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            ('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None),
            ('table_pub1', 'central_db', 'products', 'acc1', '', datetime.now(), None, None),
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test list publications
        pubs = self.pubsub_manager.list_publications()

        # Verify
        self.assertEqual(len(pubs), 2)
        self.assertIsInstance(pubs[0], Publication)
        self.assertIsInstance(pubs[1], Publication)
        self.client.execute.assert_called_with("SHOW PUBLICATIONS")

    def test_list_publications_with_filters(self):
        """Test publication listing with filters"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = Mock(return_value=mock_result)

        # Test list publications with filters
        pubs = self.pubsub_manager.list_publications(account="acc1", database="central_db")

        # Verify
        self.assertEqual(len(pubs), 1)
        self.client.execute.assert_called_with("SHOW PUBLICATIONS")

    def test_alter_publication_success(self):
        """Test successful publication alteration"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'sys', 'central_db', '*', None, datetime.now())]
        self.client.execute = Mock(return_value=mock_result)

        # Test alter publication
        pub = self.pubsub_manager.alter_publication("db_pub1", account="acc2")

        # Verify
        self.assertIsInstance(pub, Publication)
        # Should be called twice: once for ALTER, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("ALTER PUBLICATION `db_pub1` ACCOUNT `acc2`")

    def test_drop_publication_success(self):
        """Test successful publication deletion"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        # Test drop publication
        result = self.pubsub_manager.drop_publication("db_pub1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_with("DROP PUBLICATION `db_pub1`")

    def test_create_subscription_success(self):
        """Test successful subscription creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            (
                'db_pub1',
                'sys',
                'central_db',
                '*',
                None,
                datetime.now(),
                'sub_db1',
                datetime.now(),
                0,
            )
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test create subscription
        sub = self.pubsub_manager.create_subscription("sub_db1", "db_pub1", "sys")

        # Verify
        self.assertIsInstance(sub, Subscription)
        self.assertEqual(sub.sub_name, "sub_db1")
        self.assertEqual(sub.pub_name, "db_pub1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE DATABASE `sub_db1` FROM `sys` PUBLICATION `db_pub1`")

    def test_get_subscription_success(self):
        """Test successful subscription retrieval"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            (
                'db_pub1',
                'sys',
                'central_db',
                '*',
                None,
                datetime.now(),
                'sub_db1',
                datetime.now(),
                0,
            )
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test get subscription
        sub = self.pubsub_manager.get_subscription("sub_db1")

        # Verify
        self.assertIsInstance(sub, Subscription)
        self.assertEqual(sub.sub_name, "sub_db1")
        self.client.execute.assert_called_with("SHOW SUBSCRIPTIONS")

    def test_list_subscriptions_success(self):
        """Test successful subscription listing"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            (
                'db_pub1',
                'sys',
                'central_db',
                '*',
                None,
                datetime.now(),
                'sub_db1',
                datetime.now(),
                0,
            ),
            (
                'table_pub1',
                'sys',
                'central_db',
                'products',
                None,
                datetime.now(),
                'sub_table1',
                datetime.now(),
                0,
            ),
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test list subscriptions
        subs = self.pubsub_manager.list_subscriptions()

        # Verify
        self.assertEqual(len(subs), 2)
        self.assertIsInstance(subs[0], Subscription)
        self.assertIsInstance(subs[1], Subscription)
        self.client.execute.assert_called_with("SHOW SUBSCRIPTIONS")


class TestAsyncPubSubManager(unittest.IsolatedAsyncioTestCase):
    """Test cases for AsyncPubSubManager"""

    async def asyncSetUp(self):
        """Set up async test fixtures"""
        self.client = AsyncClient()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        # Initialize managers manually for testing
        from matrixone.async_client import AsyncPubSubManager

        self.pubsub_manager = AsyncPubSubManager(self.client)

    async def test_async_create_database_publication_success(self):
        """Test successful async database publication creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test create database publication
        pub = await self.pubsub_manager.create_database_publication("db_pub1", "central_db", "acc1")

        # Verify
        self.assertIsInstance(pub, Publication)
        self.assertEqual(pub.name, "db_pub1")
        self.assertEqual(pub.database, "central_db")
        self.assertEqual(pub.tables, "*")
        self.assertEqual(pub.sub_account, "acc1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE PUBLICATION `db_pub1` DATABASE `central_db` ACCOUNT `acc1`")

    async def test_async_create_table_publication_success(self):
        """Test successful async table publication creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('table_pub1', 'central_db', 'products', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test create table publication
        pub = await self.pubsub_manager.create_table_publication("table_pub1", "central_db", "products", "acc1")

        # Verify
        self.assertIsInstance(pub, Publication)
        self.assertEqual(pub.name, "table_pub1")
        self.assertEqual(pub.database, "central_db")
        self.assertEqual(pub.tables, "products")
        self.assertEqual(pub.sub_account, "acc1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call(
            "CREATE PUBLICATION `table_pub1` DATABASE `central_db` TABLE `products` ACCOUNT `acc1`"
        )

    async def test_async_get_publication_success(self):
        """Test successful async publication retrieval"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test get publication
        pub = await self.pubsub_manager.get_publication("db_pub1")

        # Verify
        self.assertIsInstance(pub, Publication)
        self.assertEqual(pub.name, "db_pub1")
        self.client.execute.assert_called_with("SHOW PUBLICATIONS")

    async def test_async_list_publications_success(self):
        """Test successful async publication listing"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            ('db_pub1', 'sys', 'central_db', '*', None, datetime.now()),
            ('table_pub1', 'sys', 'central_db', 'products', None, datetime.now()),
        ]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test list publications
        pubs = await self.pubsub_manager.list_publications()

        # Verify
        self.assertEqual(len(pubs), 2)
        self.assertIsInstance(pubs[0], Publication)
        self.assertIsInstance(pubs[1], Publication)
        self.client.execute.assert_called_with("SHOW PUBLICATIONS")

    async def test_async_alter_publication_success(self):
        """Test successful async publication alteration"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'sys', 'central_db', '*', None, datetime.now())]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test alter publication
        pub = await self.pubsub_manager.alter_publication("db_pub1", account="acc2")

        # Verify
        self.assertIsInstance(pub, Publication)
        # Should be called twice: once for ALTER, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("ALTER PUBLICATION `db_pub1` ACCOUNT `acc2`")

    async def test_async_drop_publication_success(self):
        """Test successful async publication deletion"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test drop publication
        result = await self.pubsub_manager.drop_publication("db_pub1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_with("DROP PUBLICATION `db_pub1`")

    async def test_async_create_subscription_success(self):
        """Test successful async subscription creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            (
                'db_pub1',
                'sys',
                'central_db',
                '*',
                None,
                datetime.now(),
                'sub_db1',
                datetime.now(),
                0,
            )
        ]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test create subscription
        sub = await self.pubsub_manager.create_subscription("sub_db1", "db_pub1", "sys")

        # Verify
        self.assertIsInstance(sub, Subscription)
        self.assertEqual(sub.sub_name, "sub_db1")
        self.assertEqual(sub.pub_name, "db_pub1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE DATABASE `sub_db1` FROM `sys` PUBLICATION `db_pub1`")

    async def test_async_get_subscription_success(self):
        """Test successful async subscription retrieval"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            (
                'db_pub1',
                'sys',
                'central_db',
                '*',
                None,
                datetime.now(),
                'sub_db1',
                datetime.now(),
                0,
            )
        ]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test get subscription
        sub = await self.pubsub_manager.get_subscription("sub_db1")

        # Verify
        self.assertIsInstance(sub, Subscription)
        self.assertEqual(sub.sub_name, "sub_db1")
        self.client.execute.assert_called_with("SHOW SUBSCRIPTIONS")

    async def test_async_list_subscriptions_success(self):
        """Test successful async subscription listing"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            (
                'db_pub1',
                'sys',
                'central_db',
                '*',
                None,
                datetime.now(),
                'sub_db1',
                datetime.now(),
                0,
            ),
            (
                'table_pub1',
                'sys',
                'central_db',
                'products',
                None,
                datetime.now(),
                'sub_table1',
                datetime.now(),
                0,
            ),
        ]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test list subscriptions
        subs = await self.pubsub_manager.list_subscriptions()

        # Verify
        self.assertEqual(len(subs), 2)
        self.assertIsInstance(subs[0], Subscription)
        self.assertIsInstance(subs[1], Subscription)
        self.client.execute.assert_called_with("SHOW SUBSCRIPTIONS")


class TestTransactionPubSubManager(unittest.TestCase):
    """Test cases for TransactionPubSubManager"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.transaction_wrapper = Mock()
        self.transaction_pubsub_manager = TransactionPubSubManager(self.client, self.transaction_wrapper)

    def test_transaction_create_database_publication(self):
        """Test create database publication within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'sys', 'central_db', '*', None, datetime.now())]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test create database publication
        pub = self.transaction_pubsub_manager.create_database_publication("db_pub1", "central_db", "acc1")

        # Verify
        self.assertIsInstance(pub, Publication)
        self.assertEqual(pub.name, "db_pub1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.transaction_wrapper.execute.call_count, 2)
        self.transaction_wrapper.execute.assert_any_call("CREATE PUBLICATION `db_pub1` DATABASE `central_db` ACCOUNT `acc1`")

    def test_transaction_create_table_publication(self):
        """Test create table publication within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('table_pub1', 'sys', 'central_db', 'products', None, datetime.now())]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test create table publication
        pub = self.transaction_pubsub_manager.create_table_publication("table_pub1", "central_db", "products", "acc1")

        # Verify
        self.assertIsInstance(pub, Publication)
        self.assertEqual(pub.name, "table_pub1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.transaction_wrapper.execute.call_count, 2)
        self.transaction_wrapper.execute.assert_any_call(
            "CREATE PUBLICATION `table_pub1` DATABASE `central_db` TABLE `products` ACCOUNT `acc1`"
        )

    def test_transaction_get_publication(self):
        """Test get publication within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'sys', 'central_db', '*', None, datetime.now())]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test get publication
        pub = self.transaction_pubsub_manager.get_publication("db_pub1")

        # Verify
        self.assertIsInstance(pub, Publication)
        self.assertEqual(pub.name, "db_pub1")
        self.transaction_wrapper.execute.assert_called_with("SHOW PUBLICATIONS WHERE pub_name = 'db_pub1'")

    def test_transaction_list_publications(self):
        """Test list publications within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            ('db_pub1', 'sys', 'central_db', '*', None, datetime.now()),
            ('table_pub1', 'sys', 'central_db', 'products', None, datetime.now()),
        ]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test list publications
        pubs = self.transaction_pubsub_manager.list_publications()

        # Verify
        self.assertEqual(len(pubs), 2)
        self.assertIsInstance(pubs[0], Publication)
        self.assertIsInstance(pubs[1], Publication)
        self.transaction_wrapper.execute.assert_called_with("SHOW PUBLICATIONS")

    def test_transaction_alter_publication(self):
        """Test alter publication within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pub1', 'sys', 'central_db', '*', None, datetime.now())]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test alter publication
        pub = self.transaction_pubsub_manager.alter_publication("db_pub1", account="acc2")

        # Verify
        self.assertIsInstance(pub, Publication)
        # Should be called twice: once for ALTER, once for SHOW
        self.assertEqual(self.transaction_wrapper.execute.call_count, 2)
        self.transaction_wrapper.execute.assert_any_call("ALTER PUBLICATION `db_pub1` ACCOUNT `acc2`")

    def test_transaction_drop_publication(self):
        """Test drop publication within transaction"""
        # Mock successful execution
        mock_result = Mock()
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test drop publication
        result = self.transaction_pubsub_manager.drop_publication("db_pub1")

        # Verify
        self.assertTrue(result)
        self.transaction_wrapper.execute.assert_called_with("DROP PUBLICATION `db_pub1`")

    def test_transaction_create_subscription(self):
        """Test create subscription within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            (
                'db_pub1',
                'sys',
                'central_db',
                '*',
                None,
                datetime.now(),
                'sub_db1',
                datetime.now(),
                0,
            )
        ]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test create subscription
        sub = self.transaction_pubsub_manager.create_subscription("sub_db1", "db_pub1", "sys")

        # Verify
        self.assertIsInstance(sub, Subscription)
        self.assertEqual(sub.sub_name, "sub_db1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.transaction_wrapper.execute.call_count, 2)
        self.transaction_wrapper.execute.assert_any_call("CREATE DATABASE `sub_db1` FROM `sys` PUBLICATION `db_pub1`")

    def test_transaction_get_subscription(self):
        """Test get subscription within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            (
                'db_pub1',
                'sys',
                'central_db',
                '*',
                None,
                datetime.now(),
                'sub_db1',
                datetime.now(),
                0,
            )
        ]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test get subscription
        sub = self.transaction_pubsub_manager.get_subscription("sub_db1")

        # Verify
        self.assertIsInstance(sub, Subscription)
        self.assertEqual(sub.sub_name, "sub_db1")
        self.transaction_wrapper.execute.assert_called_with("SHOW SUBSCRIPTIONS")

    def test_transaction_list_subscriptions(self):
        """Test list subscriptions within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            (
                'db_pub1',
                'sys',
                'central_db',
                '*',
                None,
                datetime.now(),
                'sub_db1',
                datetime.now(),
                0,
            ),
            (
                'table_pub1',
                'sys',
                'central_db',
                'products',
                None,
                datetime.now(),
                'sub_table1',
                datetime.now(),
                0,
            ),
        ]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test list subscriptions
        subs = self.transaction_pubsub_manager.list_subscriptions()

        # Verify
        self.assertEqual(len(subs), 2)
        self.assertIsInstance(subs[0], Subscription)
        self.assertIsInstance(subs[1], Subscription)
        self.transaction_wrapper.execute.assert_called_with("SHOW SUBSCRIPTIONS")


if __name__ == '__main__':
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestPubSubManager))
    suite.addTests(loader.loadTestsFromTestCase(TestAsyncPubSubManager))
    suite.addTests(loader.loadTestsFromTestCase(TestTransactionPubSubManager))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
