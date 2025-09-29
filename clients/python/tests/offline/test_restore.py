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
Unit tests for RestoreManager
"""

import unittest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import Client, AsyncClient, RestoreError
from matrixone.exceptions import RestoreError as RestoreErrorClass
from matrixone.restore import TransactionRestoreManager


class TestRestoreManager(unittest.TestCase):
    """Test cases for RestoreManager"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        # Initialize managers manually for testing
        from matrixone.restore import RestoreManager

        self.restore_manager = RestoreManager(self.client)

    def test_restore_cluster_success(self):
        """Test successful cluster restore"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        # Test restore cluster
        result = self.restore_manager.restore_cluster("cluster_snapshot_1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `cluster_snapshot_1`")

    def test_restore_cluster_failure(self):
        """Test cluster restore failure"""
        # Mock execution failure
        self.client.execute = Mock(side_effect=Exception("Connection failed"))

        # Test restore cluster failure
        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_cluster("cluster_snapshot_1")

    def test_restore_tenant_success(self):
        """Test successful tenant restore"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        # Test restore tenant to itself
        result = self.restore_manager.restore_tenant("acc1_snap1", "acc1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1`")

    def test_restore_tenant_to_new_tenant(self):
        """Test restore tenant to new tenant"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        # Test restore tenant to new tenant
        result = self.restore_manager.restore_tenant("acc1_snap1", "acc1", "acc2")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1` TO ACCOUNT `acc2`")

    def test_restore_tenant_failure(self):
        """Test tenant restore failure"""
        # Mock execution failure
        self.client.execute = Mock(side_effect=Exception("Permission denied"))

        # Test restore tenant failure
        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_tenant("acc1_snap1", "acc1")

    def test_restore_database_success(self):
        """Test successful database restore"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        # Test restore database
        result = self.restore_manager.restore_database("acc1_db_snap1", "acc1", "db1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` DATABASE `db1` FROM SNAPSHOT `acc1_db_snap1`")

    def test_restore_database_to_new_tenant(self):
        """Test restore database to new tenant"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        # Test restore database to new tenant
        result = self.restore_manager.restore_database("acc1_db_snap1", "acc1", "db1", "acc2")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` FROM SNAPSHOT `acc1_db_snap1` TO ACCOUNT `acc2`"
        )

    def test_restore_database_failure(self):
        """Test database restore failure"""
        # Mock execution failure
        self.client.execute = Mock(side_effect=Exception("Database not found"))

        # Test restore database failure
        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_database("acc1_db_snap1", "acc1", "db1")

    def test_restore_table_success(self):
        """Test successful table restore"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        # Test restore table
        result = self.restore_manager.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` TABLE `t1` FROM SNAPSHOT `acc1_tab_snap1`"
        )

    def test_restore_table_to_new_tenant(self):
        """Test restore table to new tenant"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        # Test restore table to new tenant
        result = self.restore_manager.restore_table("acc1_tab_snap1", "acc1", "db1", "t1", "acc2")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` TABLE `t1` FROM SNAPSHOT `acc1_tab_snap1` TO ACCOUNT `acc2`"
        )

    def test_restore_table_failure(self):
        """Test table restore failure"""
        # Mock execution failure
        self.client.execute = Mock(side_effect=Exception("Table not found"))

        # Test restore table failure
        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")

    def test_restore_with_executor_cluster(self):
        """Test restore cluster with custom executor"""
        # Mock executor
        mock_executor = Mock()
        mock_result = Mock()
        mock_executor.execute = Mock(return_value=mock_result)

        # Test restore cluster with executor
        result = self.restore_manager.restore_with_executor('cluster', 'cluster_snap1', executor=mock_executor)

        # Verify
        self.assertTrue(result)
        mock_executor.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `cluster_snap1`")

    def test_restore_with_executor_tenant(self):
        """Test restore tenant with custom executor"""
        # Mock executor
        mock_executor = Mock()
        mock_result = Mock()
        mock_executor.execute = Mock(return_value=mock_result)

        # Test restore tenant with executor
        result = self.restore_manager.restore_with_executor(
            'tenant', 'acc1_snap1', account_name='acc1', executor=mock_executor
        )

        # Verify
        self.assertTrue(result)
        mock_executor.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1`")

    def test_restore_with_executor_database(self):
        """Test restore database with custom executor"""
        # Mock executor
        mock_executor = Mock()
        mock_result = Mock()
        mock_executor.execute = Mock(return_value=mock_result)

        # Test restore database with executor
        result = self.restore_manager.restore_with_executor(
            'database',
            'acc1_db_snap1',
            account_name='acc1',
            database_name='db1',
            executor=mock_executor,
        )

        # Verify
        self.assertTrue(result)
        mock_executor.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` DATABASE `db1` FROM SNAPSHOT `acc1_db_snap1`")

    def test_restore_with_executor_table(self):
        """Test restore table with custom executor"""
        # Mock executor
        mock_executor = Mock()
        mock_result = Mock()
        mock_executor.execute = Mock(return_value=mock_result)

        # Test restore table with executor
        result = self.restore_manager.restore_with_executor(
            'table',
            'acc1_tab_snap1',
            account_name='acc1',
            database_name='db1',
            table_name='t1',
            executor=mock_executor,
        )

        # Verify
        self.assertTrue(result)
        mock_executor.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` TABLE `t1` FROM SNAPSHOT `acc1_tab_snap1`"
        )

    def test_restore_with_executor_invalid_type(self):
        """Test restore with invalid type"""
        # Test invalid restore type
        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_with_executor('invalid', 'snap1')

    def test_restore_with_executor_missing_params(self):
        """Test restore with missing required parameters"""
        # Test missing account name for tenant restore
        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_with_executor('tenant', 'snap1')

        # Test missing database name for database restore
        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_with_executor('database', 'snap1', account_name='acc1')

        # Test missing table name for table restore
        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_with_executor('table', 'snap1', account_name='acc1', database_name='db1')


class TestAsyncRestoreManager(unittest.IsolatedAsyncioTestCase):
    """Test cases for AsyncRestoreManager"""

    async def asyncSetUp(self):
        """Set up async test fixtures"""
        self.client = AsyncClient()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        # Initialize managers manually for testing
        from matrixone.async_client import AsyncRestoreManager

        self.restore_manager = AsyncRestoreManager(self.client)

    async def test_async_restore_cluster_success(self):
        """Test successful async cluster restore"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test restore cluster
        result = await self.restore_manager.restore_cluster("cluster_snapshot_1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `cluster_snapshot_1`")

    async def test_async_restore_cluster_failure(self):
        """Test async cluster restore failure"""
        # Mock execution failure
        self.client.execute = AsyncMock(side_effect=Exception("Connection failed"))

        # Test restore cluster failure
        with self.assertRaises(RestoreErrorClass):
            await self.restore_manager.restore_cluster("cluster_snapshot_1")

    async def test_async_restore_tenant_success(self):
        """Test successful async tenant restore"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test restore tenant
        result = await self.restore_manager.restore_tenant("acc1_snap1", "acc1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1`")

    async def test_async_restore_tenant_to_new_tenant(self):
        """Test async restore tenant to new tenant"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test restore tenant to new tenant
        result = await self.restore_manager.restore_tenant("acc1_snap1", "acc1", "acc2")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1` TO ACCOUNT `acc2`")

    async def test_async_restore_database_success(self):
        """Test successful async database restore"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test restore database
        result = await self.restore_manager.restore_database("acc1_db_snap1", "acc1", "db1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` DATABASE `db1` FROM SNAPSHOT `acc1_db_snap1`")

    async def test_async_restore_table_success(self):
        """Test successful async table restore"""
        # Mock successful execution
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test restore table
        result = await self.restore_manager.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")

        # Verify
        self.assertTrue(result)
        self.client.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` TABLE `t1` FROM SNAPSHOT `acc1_tab_snap1`"
        )


class TestTransactionRestoreManager(unittest.TestCase):
    """Test cases for TransactionRestoreManager"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.transaction_wrapper = Mock()
        self.transaction_restore_manager = TransactionRestoreManager(self.client, self.transaction_wrapper)

    def test_transaction_restore_cluster(self):
        """Test restore cluster within transaction"""
        # Mock successful execution
        mock_result = Mock()
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test restore cluster
        result = self.transaction_restore_manager.restore_cluster("cluster_snap1")

        # Verify
        self.assertTrue(result)
        self.transaction_wrapper.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `cluster_snap1`")

    def test_transaction_restore_tenant(self):
        """Test restore tenant within transaction"""
        # Mock successful execution
        mock_result = Mock()
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test restore tenant
        result = self.transaction_restore_manager.restore_tenant("acc1_snap1", "acc1")

        # Verify
        self.assertTrue(result)
        self.transaction_wrapper.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1`")

    def test_transaction_restore_database(self):
        """Test restore database within transaction"""
        # Mock successful execution
        mock_result = Mock()
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test restore database
        result = self.transaction_restore_manager.restore_database("acc1_db_snap1", "acc1", "db1")

        # Verify
        self.assertTrue(result)
        self.transaction_wrapper.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` FROM SNAPSHOT `acc1_db_snap1`"
        )

    def test_transaction_restore_table(self):
        """Test restore table within transaction"""
        # Mock successful execution
        mock_result = Mock()
        self.transaction_wrapper.execute = Mock(return_value=mock_result)

        # Test restore table
        result = self.transaction_restore_manager.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")

        # Verify
        self.assertTrue(result)
        self.transaction_wrapper.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` TABLE `t1` FROM SNAPSHOT `acc1_tab_snap1`"
        )


if __name__ == '__main__':
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestRestoreManager))
    suite.addTests(loader.loadTestsFromTestCase(TestAsyncRestoreManager))
    suite.addTests(loader.loadTestsFromTestCase(TestTransactionRestoreManager))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
