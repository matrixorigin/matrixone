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
Unit tests for RestoreManager with executor pattern.

Tests cover:
1. RestoreManager basic functionality (client context)
2. RestoreManager with executor (session context)
3. AsyncRestoreManager basic functionality
4. AsyncRestoreManager with executor
5. Error handling and edge cases
"""

import unittest
from unittest.mock import Mock, AsyncMock
import sys
import os

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import Client, AsyncClient
from matrixone.exceptions import RestoreError as RestoreErrorClass
from matrixone.restore import RestoreManager, AsyncRestoreManager


class TestRestoreManager(unittest.TestCase):
    """Test cases for RestoreManager"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.restore_manager = RestoreManager(self.client)

    def test_restore_cluster_success(self):
        """Test successful cluster restore"""
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        result = self.restore_manager.restore_cluster("cluster_snapshot_1")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `cluster_snapshot_1`")

    def test_restore_cluster_failure(self):
        """Test cluster restore failure"""
        self.client.execute = Mock(side_effect=Exception("Connection failed"))

        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_cluster("cluster_snapshot_1")

    def test_restore_tenant_success(self):
        """Test successful tenant restore"""
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        result = self.restore_manager.restore_tenant("acc1_snap1", "acc1")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1`")

    def test_restore_tenant_to_new_tenant(self):
        """Test restore tenant to new tenant"""
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        result = self.restore_manager.restore_tenant("acc1_snap1", "acc1", "acc2")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1` TO ACCOUNT `acc2`")

    def test_restore_tenant_failure(self):
        """Test tenant restore failure"""
        self.client.execute = Mock(side_effect=Exception("Permission denied"))

        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_tenant("acc1_snap1", "acc1")

    def test_restore_database_success(self):
        """Test successful database restore"""
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        result = self.restore_manager.restore_database("acc1_db_snap1", "acc1", "db1")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` DATABASE `db1` FROM SNAPSHOT `acc1_db_snap1`")

    def test_restore_database_to_new_tenant(self):
        """Test restore database to new tenant"""
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        result = self.restore_manager.restore_database("acc1_db_snap1", "acc1", "db1", "acc2")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` FROM SNAPSHOT `acc1_db_snap1` TO ACCOUNT `acc2`"
        )

    def test_restore_database_failure(self):
        """Test database restore failure"""
        self.client.execute = Mock(side_effect=Exception("Database not found"))

        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_database("acc1_db_snap1", "acc1", "db1")

    def test_restore_table_success(self):
        """Test successful table restore"""
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        result = self.restore_manager.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` TABLE `t1` FROM SNAPSHOT `acc1_tab_snap1`"
        )

    def test_restore_table_to_new_tenant(self):
        """Test restore table to new tenant"""
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        result = self.restore_manager.restore_table("acc1_tab_snap1", "acc1", "db1", "t1", "acc2")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` TABLE `t1` FROM SNAPSHOT `acc1_tab_snap1` TO ACCOUNT `acc2`"
        )

    def test_restore_table_failure(self):
        """Test table restore failure"""
        self.client.execute = Mock(side_effect=Exception("Table not found"))

        with self.assertRaises(RestoreErrorClass):
            self.restore_manager.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")


class TestRestoreManagerWithExecutor(unittest.TestCase):
    """Test RestoreManager with executor pattern (session context)"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"

        # Create mock executor (session)
        self.mock_session = Mock()
        self.mock_session.execute = Mock()

        # Create RestoreManager with executor
        self.restore_manager = RestoreManager(self.client, executor=self.mock_session)

    def test_restore_cluster_with_executor(self):
        """Test restore cluster uses executor instead of client"""
        mock_result = Mock()
        self.mock_session.execute.return_value = mock_result

        result = self.restore_manager.restore_cluster("cluster_snap1")

        self.assertTrue(result)
        # Should call session.execute, not client.execute
        self.mock_session.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `cluster_snap1`")

    def test_restore_tenant_with_executor(self):
        """Test restore tenant uses executor"""
        mock_result = Mock()
        self.mock_session.execute.return_value = mock_result

        result = self.restore_manager.restore_tenant("acc1_snap1", "acc1")

        self.assertTrue(result)
        self.mock_session.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1`")

    def test_restore_database_with_executor(self):
        """Test restore database uses executor"""
        mock_result = Mock()
        self.mock_session.execute.return_value = mock_result

        result = self.restore_manager.restore_database("acc1_db_snap1", "acc1", "db1")

        self.assertTrue(result)
        self.mock_session.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` FROM SNAPSHOT `acc1_db_snap1`"
        )

    def test_restore_table_with_executor(self):
        """Test restore table uses executor"""
        mock_result = Mock()
        self.mock_session.execute.return_value = mock_result

        result = self.restore_manager.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")

        self.assertTrue(result)
        self.mock_session.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` TABLE `t1` FROM SNAPSHOT `acc1_tab_snap1`"
        )

    def test_executor_pattern_isolation(self):
        """Test that executor pattern properly isolates client and session contexts"""
        # Create two managers: one with client, one with session
        client_manager = RestoreManager(self.client)
        session_manager = RestoreManager(self.client, executor=self.mock_session)

        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)
        self.mock_session.execute = Mock(return_value=mock_result)

        # Call both
        client_manager.restore_cluster("snap1")
        session_manager.restore_cluster("snap2")

        # Verify correct executors were used
        self.client.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `snap1`")
        self.mock_session.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `snap2`")


class TestAsyncRestoreManager(unittest.IsolatedAsyncioTestCase):
    """Test cases for AsyncRestoreManager"""

    async def asyncSetUp(self):
        """Set up async test fixtures"""
        self.client = AsyncClient()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.restore_manager = AsyncRestoreManager(self.client)

    async def test_async_restore_cluster_success(self):
        """Test successful async cluster restore"""
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        result = await self.restore_manager.restore_cluster("cluster_snapshot_1")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `cluster_snapshot_1`")

    async def test_async_restore_cluster_failure(self):
        """Test async cluster restore failure"""
        self.client.execute = AsyncMock(side_effect=Exception("Connection failed"))

        with self.assertRaises(RestoreErrorClass):
            await self.restore_manager.restore_cluster("cluster_snapshot_1")

    async def test_async_restore_tenant_success(self):
        """Test successful async tenant restore"""
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        result = await self.restore_manager.restore_tenant("acc1_snap1", "acc1")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1`")

    async def test_async_restore_tenant_to_new_tenant(self):
        """Test async restore tenant to new tenant"""
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        result = await self.restore_manager.restore_tenant("acc1_snap1", "acc1", "acc2")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1` TO ACCOUNT `acc2`")

    async def test_async_restore_database_success(self):
        """Test successful async database restore"""
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        result = await self.restore_manager.restore_database("acc1_db_snap1", "acc1", "db1")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` DATABASE `db1` FROM SNAPSHOT `acc1_db_snap1`")

    async def test_async_restore_table_success(self):
        """Test successful async table restore"""
        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)

        result = await self.restore_manager.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")

        self.assertTrue(result)
        self.client.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` TABLE `t1` FROM SNAPSHOT `acc1_tab_snap1`"
        )


class TestAsyncRestoreManagerWithExecutor(unittest.IsolatedAsyncioTestCase):
    """Test AsyncRestoreManager with executor pattern (async session context)"""

    async def asyncSetUp(self):
        """Set up async test fixtures"""
        self.client = AsyncClient()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"

        # Create mock async executor (async session)
        self.mock_session = Mock()
        self.mock_session.execute = AsyncMock()

        # Create AsyncRestoreManager with executor
        self.restore_manager = AsyncRestoreManager(self.client, executor=self.mock_session)

    async def test_async_restore_cluster_with_executor(self):
        """Test async restore cluster uses executor instead of client"""
        mock_result = Mock()
        self.mock_session.execute.return_value = mock_result

        result = await self.restore_manager.restore_cluster("cluster_snap1")

        self.assertTrue(result)
        # Should call session.execute, not client.execute
        self.mock_session.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `cluster_snap1`")

    async def test_async_restore_tenant_with_executor(self):
        """Test async restore tenant uses executor"""
        mock_result = Mock()
        self.mock_session.execute.return_value = mock_result

        result = await self.restore_manager.restore_tenant("acc1_snap1", "acc1")

        self.assertTrue(result)
        self.mock_session.execute.assert_called_once_with("RESTORE ACCOUNT `acc1` FROM SNAPSHOT `acc1_snap1`")

    async def test_async_restore_database_with_executor(self):
        """Test async restore database uses executor"""
        mock_result = Mock()
        self.mock_session.execute.return_value = mock_result

        result = await self.restore_manager.restore_database("acc1_db_snap1", "acc1", "db1")

        self.assertTrue(result)
        self.mock_session.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` FROM SNAPSHOT `acc1_db_snap1`"
        )

    async def test_async_restore_table_with_executor(self):
        """Test async restore table uses executor"""
        mock_result = Mock()
        self.mock_session.execute.return_value = mock_result

        result = await self.restore_manager.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")

        self.assertTrue(result)
        self.mock_session.execute.assert_called_once_with(
            "RESTORE ACCOUNT `acc1` DATABASE `db1` TABLE `t1` FROM SNAPSHOT `acc1_tab_snap1`"
        )

    async def test_async_executor_pattern_isolation(self):
        """Test that executor pattern properly isolates client and session contexts in async"""
        # Create two managers: one with client, one with session
        client_manager = AsyncRestoreManager(self.client)
        session_manager = AsyncRestoreManager(self.client, executor=self.mock_session)

        mock_result = Mock()
        self.client.execute = AsyncMock(return_value=mock_result)
        self.mock_session.execute = AsyncMock(return_value=mock_result)

        # Call both
        await client_manager.restore_cluster("snap1")
        await session_manager.restore_cluster("snap2")

        # Verify correct executors were used
        self.client.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `snap1`")
        self.mock_session.execute.assert_called_once_with("RESTORE CLUSTER FROM SNAPSHOT `snap2`")


class TestRestoreManagerEdgeCases(unittest.TestCase):
    """Test edge cases and error handling"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Client()
        self.client._engine = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.restore_manager = RestoreManager(self.client)

    def test_restore_with_none_result(self):
        """Test restore when execute returns None"""
        self.client.execute = Mock(return_value=None)

        result = self.restore_manager.restore_cluster("cluster_snap1")

        # Should return False when result is None
        self.assertFalse(result)

    def test_restore_database_cross_tenant(self):
        """Test cross-tenant database restore"""
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        result = self.restore_manager.restore_database("snap1", "acc1", "db1", to_account="acc2")

        self.assertTrue(result)
        expected_sql = "RESTORE ACCOUNT `acc1` DATABASE `db1` FROM SNAPSHOT `snap1` TO ACCOUNT `acc2`"
        self.client.execute.assert_called_once_with(expected_sql)

    def test_restore_table_cross_tenant(self):
        """Test cross-tenant table restore"""
        mock_result = Mock()
        self.client.execute = Mock(return_value=mock_result)

        result = self.restore_manager.restore_table("snap1", "acc1", "db1", "t1", to_account="acc2")

        self.assertTrue(result)
        expected_sql = "RESTORE ACCOUNT `acc1` DATABASE `db1` TABLE `t1` FROM SNAPSHOT `snap1` TO ACCOUNT `acc2`"
        self.client.execute.assert_called_once_with(expected_sql)


if __name__ == '__main__':
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestRestoreManager))
    suite.addTests(loader.loadTestsFromTestCase(TestRestoreManagerWithExecutor))
    suite.addTests(loader.loadTestsFromTestCase(TestAsyncRestoreManager))
    suite.addTests(loader.loadTestsFromTestCase(TestAsyncRestoreManagerWithExecutor))
    suite.addTests(loader.loadTestsFromTestCase(TestRestoreManagerEdgeCases))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
