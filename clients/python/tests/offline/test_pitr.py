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
Unit tests for PitrManager
"""

import unittest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import Client, AsyncClient, PitrError, Pitr
from matrixone.exceptions import PitrError as PitrErrorClass
from matrixone.pitr import TransactionPitrManager


class TestPitrManager(unittest.TestCase):
    """Test cases for PitrManager"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        # Initialize managers manually for testing
        from matrixone.pitr import PitrManager

        self.pitr_manager = PitrManager(self.client)

    def test_create_cluster_pitr_success(self):
        """Test successful cluster PITR creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.client.execute = Mock(return_value=mock_result)

        # Test create cluster PITR
        pitr = self.pitr_manager.create_cluster_pitr("cluster_pitr1", 1, "d")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "cluster_pitr1")
        self.assertEqual(pitr.level, "cluster")
        self.assertEqual(pitr.range_value, 1)
        self.assertEqual(pitr.range_unit, "d")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE PITR `cluster_pitr1` FOR CLUSTER RANGE 1 'd'")

    def test_create_cluster_pitr_failure(self):
        """Test cluster PITR creation failure"""
        # Mock execution failure
        self.client.execute = Mock(side_effect=Exception("Permission denied"))

        # Test create cluster PITR failure
        with self.assertRaises(PitrErrorClass):
            self.pitr_manager.create_cluster_pitr("cluster_pitr1", 1, "d")

    def test_create_account_pitr_success(self):
        """Test successful account PITR creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('account_pitr1', datetime.now(), datetime.now(), 'account', 'acc1', '*', '*', 2, 'h')]
        self.client.execute = Mock(return_value=mock_result)

        # Test create account PITR for current account
        pitr = self.pitr_manager.create_account_pitr("account_pitr1", range_value=2, range_unit="h")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "account_pitr1")
        self.assertEqual(pitr.level, "account")
        self.assertEqual(pitr.range_value, 2)
        self.assertEqual(pitr.range_unit, "h")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE PITR `account_pitr1` FOR ACCOUNT RANGE 2 'h'")

    def test_create_account_pitr_for_specific_account(self):
        """Test create account PITR for specific account"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('account_pitr1', datetime.now(), datetime.now(), 'account', 'acc1', '*', '*', 1, 'd')]
        self.client.execute = Mock(return_value=mock_result)

        # Test create account PITR for specific account
        pitr = self.pitr_manager.create_account_pitr("account_pitr1", "acc1", 1, "d")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE PITR `account_pitr1` FOR ACCOUNT `acc1` RANGE 1 'd'")

    def test_create_database_pitr_success(self):
        """Test successful database PITR creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pitr1', datetime.now(), datetime.now(), 'database', 'acc1', 'db1', '*', 1, 'y')]
        self.client.execute = Mock(return_value=mock_result)

        # Test create database PITR
        pitr = self.pitr_manager.create_database_pitr("db_pitr1", "db1", 1, "y")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "db_pitr1")
        self.assertEqual(pitr.level, "database")
        self.assertEqual(pitr.database_name, "db1")
        self.assertEqual(pitr.range_value, 1)
        self.assertEqual(pitr.range_unit, "y")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE PITR `db_pitr1` FOR DATABASE `db1` RANGE 1 'y'")

    def test_create_table_pitr_success(self):
        """Test successful table PITR creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('tab_pitr1', datetime.now(), datetime.now(), 'table', 'acc1', 'db1', 't1', 1, 'y')]
        self.client.execute = Mock(return_value=mock_result)

        # Test create table PITR
        pitr = self.pitr_manager.create_table_pitr("tab_pitr1", "db1", "t1", 1, "y")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "tab_pitr1")
        self.assertEqual(pitr.level, "table")
        self.assertEqual(pitr.database_name, "db1")
        self.assertEqual(pitr.table_name, "t1")
        self.assertEqual(pitr.range_value, 1)
        self.assertEqual(pitr.range_unit, "y")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE PITR `tab_pitr1` FOR TABLE `db1` `t1` RANGE 1 'y'")

    def test_get_pitr_success(self):
        """Test successful PITR retrieval"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.client.execute = Mock(return_value=mock_result)

        # Test get PITR
        pitr = self.pitr_manager.get("cluster_pitr1")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "cluster_pitr1")
        self.client.execute.assert_called_with("SHOW PITR WHERE pitr_name = 'cluster_pitr1'")

    def test_get_pitr_not_found(self):
        """Test PITR not found"""
        # Mock empty result
        mock_result = Mock()
        mock_result.rows = []
        self.client.execute = Mock(return_value=mock_result)

        # Test get PITR not found
        with self.assertRaises(PitrErrorClass):
            self.pitr_manager.get("nonexistent_pitr")

    def test_list_pitrs_success(self):
        """Test successful PITR listing"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            ('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd'),
            ('account_pitr1', datetime.now(), datetime.now(), 'account', 'acc1', '*', '*', 2, 'h'),
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test list PITRs
        pitrs = self.pitr_manager.list()

        # Verify
        self.assertEqual(len(pitrs), 2)
        self.assertIsInstance(pitrs[0], Pitr)
        self.assertIsInstance(pitrs[1], Pitr)
        self.client.execute.assert_called_with("SHOW PITR")

    def test_list_pitrs_with_filters(self):
        """Test PITR listing with filters"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('account_pitr1', datetime.now(), datetime.now(), 'account', 'acc1', '*', '*', 2, 'h')]
        self.client.execute = Mock(return_value=mock_result)

        # Test list PITRs with filters
        pitrs = self.pitr_manager.list(level="account", account_name="acc1")

        # Verify
        self.assertEqual(len(pitrs), 1)
        self.client.execute.assert_called_with("SHOW PITR WHERE pitr_level = 'account' AND account_name = 'acc1'")

    def test_alter_pitr_success(self):
        """Test successful PITR alteration"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 2, 'h')]
        self.client.execute = Mock(return_value=mock_result)

        # Test alter PITR
        pitr = self.pitr_manager.alter("cluster_pitr1", 2, "h")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.range_value, 2)
        self.assertEqual(pitr.range_unit, "h")
        # Should be called twice: once for ALTER, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("ALTER PITR `cluster_pitr1` RANGE 2 'h'")

    def test_delete_pitr_success(self):
        """Test successful PITR deletion"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        # Test delete PITR
        result = self.pitr_manager.delete("cluster_pitr1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_with("DROP PITR `cluster_pitr1`")

    def test_validate_range_invalid_value(self):
        """Test range validation with invalid value"""
        # Test invalid range value
        with self.assertRaises(PitrErrorClass):
            self.pitr_manager.create_cluster_pitr("test", 0, "d")

        with self.assertRaises(PitrErrorClass):
            self.pitr_manager.create_cluster_pitr("test", 101, "d")

    def test_validate_range_invalid_unit(self):
        """Test range validation with invalid unit"""
        # Test invalid range unit
        with self.assertRaises(PitrErrorClass):
            self.pitr_manager.create_cluster_pitr("test", 1, "invalid")


class TestAsyncPitrManager(unittest.IsolatedAsyncioTestCase):
    """Test cases for AsyncPitrManager"""

    async def asyncSetUp(self):
        """Set up async test fixtures"""
        self.client = AsyncClient()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        # Initialize managers manually for testing
        from matrixone.async_client import AsyncPitrManager

        self.pitr_manager = AsyncPitrManager(self.client)

    async def test_async_create_cluster_pitr_success(self):
        """Test successful async cluster PITR creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test create cluster PITR
        pitr = await self.pitr_manager.create_cluster_pitr("cluster_pitr1", 1, "d")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "cluster_pitr1")
        self.assertEqual(pitr.level, "cluster")
        self.assertEqual(pitr.range_value, 1)
        self.assertEqual(pitr.range_unit, "d")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE PITR `cluster_pitr1` FOR CLUSTER RANGE 1 'd'")

    async def test_async_create_account_pitr_success(self):
        """Test successful async account PITR creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('account_pitr1', datetime.now(), datetime.now(), 'account', 'acc1', '*', '*', 2, 'h')]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test create account PITR
        pitr = await self.pitr_manager.create_account_pitr("account_pitr1", range_value=2, range_unit="h")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "account_pitr1")
        self.assertEqual(pitr.level, "account")
        self.assertEqual(pitr.range_value, 2)
        self.assertEqual(pitr.range_unit, "h")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE PITR `account_pitr1` FOR ACCOUNT RANGE 2 'h'")

    async def test_async_create_database_pitr_success(self):
        """Test successful async database PITR creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pitr1', datetime.now(), datetime.now(), 'database', 'acc1', 'db1', '*', 1, 'y')]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test create database PITR
        pitr = await self.pitr_manager.create_database_pitr("db_pitr1", "db1", 1, "y")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "db_pitr1")
        self.assertEqual(pitr.level, "database")
        self.assertEqual(pitr.database_name, "db1")
        self.assertEqual(pitr.range_value, 1)
        self.assertEqual(pitr.range_unit, "y")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE PITR `db_pitr1` FOR DATABASE `db1` RANGE 1 'y'")

    async def test_async_create_table_pitr_success(self):
        """Test successful async table PITR creation"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('tab_pitr1', datetime.now(), datetime.now(), 'table', 'acc1', 'db1', 't1', 1, 'y')]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test create table PITR
        pitr = await self.pitr_manager.create_table_pitr("tab_pitr1", "db1", "t1", 1, "y")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "tab_pitr1")
        self.assertEqual(pitr.level, "table")
        self.assertEqual(pitr.database_name, "db1")
        self.assertEqual(pitr.table_name, "t1")
        self.assertEqual(pitr.range_value, 1)
        self.assertEqual(pitr.range_unit, "y")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("CREATE PITR `tab_pitr1` FOR TABLE `db1` `t1` RANGE 1 'y'")

    async def test_async_get_pitr_success(self):
        """Test successful async PITR retrieval"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test get PITR
        pitr = await self.pitr_manager.get("cluster_pitr1")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "cluster_pitr1")
        self.client.execute.assert_called_with("SHOW PITR WHERE pitr_name = 'cluster_pitr1'")

    async def test_async_list_pitrs_success(self):
        """Test successful async PITR listing"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            ('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd'),
            ('account_pitr1', datetime.now(), datetime.now(), 'account', 'acc1', '*', '*', 2, 'h'),
        ]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test list PITRs
        pitrs = await self.pitr_manager.list()

        # Verify
        self.assertEqual(len(pitrs), 2)
        self.assertIsInstance(pitrs[0], Pitr)
        self.assertIsInstance(pitrs[1], Pitr)
        self.client.execute.assert_called_with("SHOW PITR")

    async def test_async_alter_pitr_success(self):
        """Test successful async PITR alteration"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 2, 'h')]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test alter PITR
        pitr = await self.pitr_manager.alter("cluster_pitr1", 2, "h")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.range_value, 2)
        self.assertEqual(pitr.range_unit, "h")
        # Should be called twice: once for ALTER, once for SHOW
        self.assertEqual(self.client.execute.call_count, 2)
        self.client.execute.assert_any_call("ALTER PITR `cluster_pitr1` RANGE 2 'h'")

    async def test_async_delete_pitr_success(self):
        """Test successful async PITR deletion"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test delete PITR
        result = await self.pitr_manager.delete("cluster_pitr1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_with("DROP PITR `cluster_pitr1`")


class TestTransactionPitrManager(unittest.TestCase):
    """Test cases for TransactionPitrManager"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.transaction_wrapper = Mock()
        self.transaction_pitr_manager = TransactionPitrManager(self.client, self.transaction_wrapper)

    def test_transaction_create_cluster_pitr(self):
        """Test create cluster PITR within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test create cluster PITR
        pitr = self.transaction_pitr_manager.create_cluster_pitr("cluster_pitr1", 1, "d")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "cluster_pitr1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.transaction_wrapper.execute.call_count, 2)
        self.transaction_wrapper.execute.assert_any_call("CREATE PITR `cluster_pitr1` FOR CLUSTER RANGE 1 'd'")

    def test_transaction_create_account_pitr(self):
        """Test create account PITR within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('account_pitr1', datetime.now(), datetime.now(), 'account', 'acc1', '*', '*', 2, 'h')]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test create account PITR
        pitr = self.transaction_pitr_manager.create_account_pitr("account_pitr1", range_value=2, range_unit="h")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "account_pitr1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.transaction_wrapper.execute.call_count, 2)
        self.transaction_wrapper.execute.assert_any_call("CREATE PITR `account_pitr1` FOR ACCOUNT RANGE 2 'h'")

    def test_transaction_create_database_pitr(self):
        """Test create database PITR within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('db_pitr1', datetime.now(), datetime.now(), 'database', 'acc1', 'db1', '*', 1, 'y')]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test create database PITR
        pitr = self.transaction_pitr_manager.create_database_pitr("db_pitr1", "db1", 1, "y")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "db_pitr1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.transaction_wrapper.execute.call_count, 2)
        self.transaction_wrapper.execute.assert_any_call("CREATE PITR `db_pitr1` FOR DATABASE `db1` RANGE 1 'y'")

    def test_transaction_create_table_pitr(self):
        """Test create table PITR within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('tab_pitr1', datetime.now(), datetime.now(), 'table', 'acc1', 'db1', 't1', 1, 'y')]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test create table PITR
        pitr = self.transaction_pitr_manager.create_table_pitr("tab_pitr1", "db1", "t1", 1, "y")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "tab_pitr1")
        # Should be called twice: once for CREATE, once for SHOW
        self.assertEqual(self.transaction_wrapper.execute.call_count, 2)
        self.transaction_wrapper.execute.assert_any_call("CREATE PITR `tab_pitr1` FOR TABLE `db1` TABLE `t1` RANGE 1 'y'")

    def test_transaction_get_pitr(self):
        """Test get PITR within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd')]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test get PITR
        pitr = self.transaction_pitr_manager.get("cluster_pitr1")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.name, "cluster_pitr1")
        self.transaction_wrapper.execute.assert_called_with("SHOW PITR WHERE pitr_name = 'cluster_pitr1'")

    def test_transaction_list_pitrs(self):
        """Test list PITRs within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            ('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 1, 'd'),
            ('account_pitr1', datetime.now(), datetime.now(), 'account', 'acc1', '*', '*', 2, 'h'),
        ]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test list PITRs
        pitrs = self.transaction_pitr_manager.list()

        # Verify
        self.assertEqual(len(pitrs), 2)
        self.assertIsInstance(pitrs[0], Pitr)
        self.assertIsInstance(pitrs[1], Pitr)
        self.transaction_wrapper.execute.assert_called_with("SHOW PITR")

    def test_transaction_alter_pitr(self):
        """Test alter PITR within transaction"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('cluster_pitr1', datetime.now(), datetime.now(), 'cluster', '*', '*', '*', 2, 'h')]
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test alter PITR
        pitr = self.transaction_pitr_manager.alter("cluster_pitr1", 2, "h")

        # Verify
        self.assertIsInstance(pitr, Pitr)
        self.assertEqual(pitr.range_value, 2)
        self.assertEqual(pitr.range_unit, "h")
        # Should be called twice: once for ALTER, once for SHOW
        self.assertEqual(self.transaction_wrapper.execute.call_count, 2)
        self.transaction_wrapper.execute.assert_any_call("ALTER PITR `cluster_pitr1` RANGE 2 'h'")

    def test_transaction_delete_pitr(self):
        """Test delete PITR within transaction"""
        # Mock successful execution
        mock_result = Mock()
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test delete PITR
        result = self.transaction_pitr_manager.delete("cluster_pitr1")

        # Verify
        self.assertTrue(result)
        self.transaction_wrapper.execute.assert_called_with("DROP PITR `cluster_pitr1`")


if __name__ == '__main__':
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestPitrManager))
    suite.addTests(loader.loadTestsFromTestCase(TestAsyncPitrManager))
    suite.addTests(loader.loadTestsFromTestCase(TestTransactionPitrManager))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
