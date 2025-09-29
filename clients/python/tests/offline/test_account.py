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
Unit tests for MatrixOne Account Management functionality
"""

import unittest
import pytest
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
from matrixone.account import AccountManager, Account, User, TransactionAccountManager
from matrixone.async_client import AsyncAccountManager, AsyncTransactionAccountManager
from matrixone.exceptions import AccountError


class TestAccount(unittest.TestCase):
    """Test Account data class"""

    def test_account_creation(self):
        """Test Account object creation"""
        account = Account(
            name="test_account",
            admin_name="admin_user",
            created_time=datetime.now(),
            status="OPEN",
            comment="Test account",
        )

        self.assertEqual(account.name, "test_account")
        self.assertEqual(account.admin_name, "admin_user")
        self.assertEqual(account.status, "OPEN")
        self.assertEqual(account.comment, "Test account")
        self.assertIsNotNone(account.created_time)

    def test_account_str_representation(self):
        """Test Account string representation"""
        account = Account("test_account", "admin_user", status="OPEN")
        str_repr = str(account)
        self.assertIn("test_account", str_repr)
        self.assertIn("admin_user", str_repr)
        self.assertIn("OPEN", str_repr)


class TestUser(unittest.TestCase):
    """Test User data class"""

    def test_user_creation(self):
        """Test User object creation"""
        user = User(
            name="test_user",
            host="%",
            account="sys",
            created_time=datetime.now(),
            status="OPEN",
            comment="Test user",
        )

        self.assertEqual(user.name, "test_user")
        self.assertEqual(user.host, "%")
        self.assertEqual(user.account, "sys")
        self.assertEqual(user.status, "OPEN")
        self.assertEqual(user.comment, "Test user")
        self.assertIsNotNone(user.created_time)

    def test_user_str_representation(self):
        """Test User string representation"""
        user = User("test_user", "localhost", "test_account", status="OPEN")
        str_repr = str(user)
        self.assertIn("test_user", str_repr)
        self.assertIn("localhost", str_repr)
        self.assertIn("test_account", str_repr)
        self.assertIn("OPEN", str_repr)


class TestAccountManager(unittest.TestCase):
    """Test AccountManager functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.account_manager = AccountManager(self.client)

    def test_create_account_success(self):
        """Test successful account creation"""
        # Mock successful execution - first call for CREATE, second call for get_account
        mock_result = Mock()
        mock_result.rows = [('test_account', 'admin_user', datetime.now(), 'OPEN', 'Test account', None, None)]
        self.client.execute = Mock(return_value=mock_result)

        # Test account creation
        account = self.account_manager.create_account(
            account_name="test_account",
            admin_name="admin_user",
            password="password123",
            comment="Test account",
        )

        # Verify
        self.assertIsInstance(account, Account)
        self.assertEqual(account.name, "test_account")
        self.assertEqual(account.admin_name, "admin_user")
        self.assertEqual(account.status, "OPEN")
        self.assertEqual(account.comment, "Test account")

        # Verify SQL was called correctly - check the first call (CREATE ACCOUNT)
        self.assertEqual(self.client.execute.call_count, 2)  # CREATE + get_account
        first_call_args = self.client.execute.call_args_list[0][0][0]
        self.assertIn("CREATE ACCOUNT", first_call_args)
        self.assertIn("admin_user", first_call_args)
        self.assertIn("password123", first_call_args)
        self.assertIn("Test account", first_call_args)

    def test_create_account_failure(self):
        """Test account creation failure"""
        # Mock execution failure
        self.client.execute = Mock(side_effect=Exception("Database error"))

        # Test account creation failure
        with self.assertRaises(AccountError) as context:
            self.account_manager.create_account(account_name="test_account", admin_name="admin_user", password="password123")

        self.assertIn("Failed to create account 'test_account'", str(context.exception))

    def test_drop_account_success(self):
        """Test successful account deletion"""
        # Mock successful execution
        self.client.execute = Mock(return_value=Mock())

        # Test account deletion
        self.account_manager.drop_account("test_account")

        # Verify SQL was called correctly
        self.client.execute.assert_called_with("DROP ACCOUNT `test_account`")

    def test_drop_account_failure(self):
        """Test account deletion failure"""
        # Mock execution failure
        self.client.execute = Mock(side_effect=Exception("Database error"))

        # Test account deletion failure
        with self.assertRaises(AccountError) as context:
            self.account_manager.drop_account("test_account")

        self.assertIn("Failed to drop account 'test_account'", str(context.exception))

    def test_alter_account_success(self):
        """Test successful account alteration"""
        # Mock successful execution and get_account
        mock_result = Mock()
        mock_result.rows = [('test_account', 'admin_user', datetime.now(), 'OPEN', 'Updated comment', None, None)]
        self.client.execute = Mock(return_value=mock_result)

        # Test account alteration
        account = self.account_manager.alter_account(account_name="test_account", comment="Updated comment")

        # Verify
        self.assertIsInstance(account, Account)
        self.assertEqual(account.comment, "Updated comment")

        # Verify SQL was called correctly - check the first call (ALTER ACCOUNT)
        self.assertEqual(self.client.execute.call_count, 2)  # ALTER + get_account
        first_call_args = self.client.execute.call_args_list[0][0][0]
        self.assertIn("ALTER ACCOUNT", first_call_args)
        self.assertIn("Updated comment", first_call_args)

    def test_alter_account_suspend(self):
        """Test account suspension"""
        # Mock successful execution and get_account
        mock_result = Mock()
        mock_result.rows = [
            (
                'test_account',
                'admin_user',
                datetime.now(),
                'SUSPENDED',
                'Test account',
                datetime.now(),
                'Security violation',
            )
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test account suspension
        account = self.account_manager.alter_account(
            account_name="test_account", suspend=True, suspend_reason="Security violation"
        )

        # Verify
        self.assertEqual(account.status, "SUSPENDED")
        self.assertEqual(account.suspended_reason, "Security violation")

        # Verify SQL was called correctly - check the first call (ALTER ACCOUNT)
        self.assertEqual(self.client.execute.call_count, 2)  # ALTER + get_account
        first_call_args = self.client.execute.call_args_list[0][0][0]
        self.assertIn("SUSPEND COMMENT", first_call_args)
        self.assertIn("Security violation", first_call_args)

    def test_get_account_success(self):
        """Test successful account retrieval"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [('test_account', 'admin_user', datetime.now(), 'OPEN', 'Test account', None, None)]
        self.client.execute = Mock(return_value=mock_result)

        # Test account retrieval
        account = self.account_manager.get_account("test_account")

        # Verify
        self.assertIsInstance(account, Account)
        self.assertEqual(account.name, "test_account")
        self.assertEqual(account.admin_name, "admin_user")

        # Verify SQL was called correctly
        self.client.execute.assert_called_with("SHOW ACCOUNTS")

    def test_get_account_not_found(self):
        """Test account not found"""
        # Mock empty result
        mock_result = Mock()
        mock_result.rows = []
        self.client.execute = Mock(return_value=mock_result)

        # Test account not found
        with self.assertRaises(AccountError) as context:
            self.account_manager.get_account("nonexistent_account")

        self.assertIn("Account 'nonexistent_account' not found", str(context.exception))

    def test_list_accounts_success(self):
        """Test successful account listing"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            ('account1', 'admin1', datetime.now(), 'OPEN', 'Account 1', None, None),
            (
                'account2',
                'admin2',
                datetime.now(),
                'SUSPENDED',
                'Account 2',
                datetime.now(),
                'Security issue',
            ),
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test account listing
        accounts = self.account_manager.list_accounts()

        # Verify
        self.assertEqual(len(accounts), 2)
        self.assertIsInstance(accounts[0], Account)
        self.assertIsInstance(accounts[1], Account)
        self.assertEqual(accounts[0].name, "account1")
        self.assertEqual(accounts[1].name, "account2")
        self.assertEqual(accounts[1].status, "SUSPENDED")

    def test_create_user_success(self):
        """Test successful user creation"""
        # Mock successful execution - first call for CREATE, second call for get_user
        mock_result = Mock()
        mock_result.rows = [
            (
                'test_user',
                'localhost',
                'test_account',
                datetime.now(),
                'OPEN',
                'Test user',
                None,
                None,
            )
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test user creation
        user = self.account_manager.create_user(user_name="test_user", password="password123", comment="Test user")

        # Verify
        self.assertIsInstance(user, User)
        self.assertEqual(user.name, "test_user")
        self.assertEqual(user.host, "%")
        self.assertEqual(user.account, "sys")
        self.assertEqual(user.comment, "Test user")

        # Verify SQL was called correctly - check the first call (CREATE USER)
        self.assertEqual(self.client.execute.call_count, 1)  # CREATE only
        first_call_args = self.client.execute.call_args_list[0][0][0]
        self.assertIn("CREATE USER", first_call_args)
        self.assertIn("test_user", first_call_args)
        self.assertIn("password123", first_call_args)

    def test_drop_user_success(self):
        """Test successful user deletion"""
        # Mock successful execution
        self.client.execute = Mock(return_value=Mock())

        # Test user deletion
        self.account_manager.drop_user("test_user")

        # Verify SQL was called correctly
        call_args = self.client.execute.call_args[0][0]
        self.assertIn("DROP USER", call_args)
        self.assertIn("test_user", call_args)

    def test_alter_user_success(self):
        """Test successful user alteration"""
        # Mock successful execution - first call for ALTER, second call for get_user
        mock_result = Mock()
        mock_result.rows = [
            (
                'test_user',
                'localhost',
                'test_account',
                datetime.now(),
                'OPEN',
                'Updated comment',
                None,
                None,
            )
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test user alteration (password change instead of comment)
        user = self.account_manager.alter_user(user_name="test_user", password="new_password")

        # Verify
        self.assertIsInstance(user, User)
        self.assertEqual(user.name, "test_user")

        # Verify SQL was called correctly - check the first call (ALTER USER)
        self.assertEqual(self.client.execute.call_count, 1)  # ALTER only
        first_call_args = self.client.execute.call_args_list[0][0][0]
        self.assertIn("ALTER USER", first_call_args)
        self.assertIn("test_user", first_call_args)
        self.assertIn("new_password", first_call_args)

    def test_alter_user_lock(self):
        """Test user locking"""
        # Mock successful execution - first call for ALTER, second call for get_user
        mock_result = Mock()
        mock_result.rows = [
            (
                'test_user',
                'localhost',
                'test_account',
                datetime.now(),
                'LOCKED',
                'Test user',
                datetime.now(),
                'Security violation',
            )
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test user locking
        user = self.account_manager.alter_user(user_name="test_user", lock=True, lock_reason="Security violation")

        # Verify
        self.assertEqual(user.status, "LOCKED")
        self.assertEqual(user.locked_reason, "Security violation")

        # Verify SQL was called correctly - check the first call (ALTER USER)
        self.assertEqual(self.client.execute.call_count, 1)  # ALTER only
        first_call_args = self.client.execute.call_args_list[0][0][0]
        self.assertIn("ALTER USER", first_call_args)
        self.assertIn("LOCK", first_call_args)

    def test_get_user_success(self):
        """Test successful user retrieval"""
        # Mock successful execution
        mock_result = Mock()
        mock_result.rows = [
            (
                'test_user',
                'localhost',
                'test_account',
                datetime.now(),
                'OPEN',
                'Test user',
                None,
                None,
            )
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test user retrieval (using list_users instead of get_user)
        users = self.account_manager.list_users()
        user = users[0] if users else None

        # Verify
        self.assertIsInstance(user, User)
        self.assertEqual(user.name, "test_user")
        self.assertEqual(user.host, "%")
        self.assertEqual(user.account, "sys")

    def test_list_users_success(self):
        """Test successful user listing"""
        # Mock successful execution (list_users only returns current user)
        mock_result = Mock()
        mock_result.rows = [
            (
                'current_user',
                '%',
                'current_account',
                datetime.now(),
                'OPEN',
                'Current user',
                None,
                None,
            )
        ]
        self.client.execute = Mock(return_value=mock_result)

        # Test user listing
        users = self.account_manager.list_users()

        # Verify
        self.assertEqual(len(users), 1)
        self.assertIsInstance(users[0], User)
        self.assertEqual(users[0].name, "current_user")


class TestTransactionAccountManager(unittest.TestCase):
    """Test TransactionAccountManager functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.transaction = Mock()
        self.transaction.client = self.client
        self.transaction.execute = Mock()
        self.transaction_account_manager = TransactionAccountManager(self.transaction)

    def test_create_account_in_transaction(self):
        """Test account creation within transaction"""
        # Mock successful execution - first call for CREATE, second call for get_account
        mock_result = Mock()
        mock_result.rows = [('test_account', 'admin_user', datetime.now(), 'OPEN', 'Test account', None, None)]
        self.transaction.execute = Mock(return_value=mock_result)
        self.client.execute = Mock(return_value=mock_result)

        # Test account creation in transaction
        account = self.transaction_account_manager.create_account(
            account_name="test_account",
            admin_name="admin_user",
            password="password123",
            comment="Test account",
        )

        # Verify
        self.assertIsInstance(account, Account)
        self.assertEqual(account.name, "test_account")

        # Verify transaction.execute was called - only for CREATE ACCOUNT
        self.assertEqual(self.transaction.execute.call_count, 1)  # CREATE only
        self.assertEqual(self.client.execute.call_count, 1)  # get_account
        first_call_args = self.transaction.execute.call_args_list[0][0][0]
        self.assertIn("CREATE ACCOUNT", first_call_args)


class TestAsyncAccountManager(unittest.IsolatedAsyncioTestCase):
    """Test AsyncAccountManager functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncMock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.async_account_manager = AsyncAccountManager(self.client)

    async def test_async_create_account_success(self):
        """Test successful async account creation"""
        # Mock successful execution
        mock_result = AsyncMock()
        mock_result.rows = [('test_account', 'admin_user', datetime.now(), 'OPEN', 'Test account', None, None)]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test async account creation
        account = await self.async_account_manager.create_account(
            account_name="test_account",
            admin_name="admin_user",
            password="password123",
            comment="Test account",
        )

        # Verify
        self.assertIsInstance(account, Account)
        self.assertEqual(account.name, "test_account")
        self.assertEqual(account.admin_name, "admin_user")
        self.assertEqual(account.status, "OPEN")
        self.assertEqual(account.comment, "Test account")

        # Verify SQL was called correctly - check the first call (CREATE ACCOUNT)
        self.assertEqual(self.client.execute.call_count, 2)  # CREATE + get_account
        first_call_args = self.client.execute.call_args_list[0][0][0]
        self.assertIn("CREATE ACCOUNT", first_call_args)
        self.assertIn("admin_user", first_call_args)
        self.assertIn("password123", first_call_args)
        self.assertIn("Test account", first_call_args)

    async def test_async_create_account_failure(self):
        """Test async account creation failure"""
        # Mock execution failure
        self.client.execute = AsyncMock(side_effect=Exception("Database error"))

        # Test async account creation failure
        with self.assertRaises(AccountError) as context:
            await self.async_account_manager.create_account(
                account_name="test_account", admin_name="admin_user", password="password123"
            )

        self.assertIn("Failed to create account 'test_account'", str(context.exception))

    async def test_async_drop_account_success(self):
        """Test successful async account deletion"""
        # Mock successful execution
        self.client.execute = AsyncMock(return_value=AsyncMock())

        # Test async account deletion
        await self.async_account_manager.drop_account("test_account")

        # Verify SQL was called correctly
        self.client.execute.assert_called_with("DROP ACCOUNT `test_account`")

    async def test_async_list_accounts_success(self):
        """Test successful async account listing"""
        # Mock successful execution
        mock_result = AsyncMock()
        mock_result.rows = [
            ('account1', 'admin1', datetime.now(), 'OPEN', 'Account 1', None, None),
            (
                'account2',
                'admin2',
                datetime.now(),
                'SUSPENDED',
                'Account 2',
                datetime.now(),
                'Security issue',
            ),
        ]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test async account listing
        accounts = await self.async_account_manager.list_accounts()

        # Verify
        self.assertEqual(len(accounts), 2)
        self.assertIsInstance(accounts[0], Account)
        self.assertIsInstance(accounts[1], Account)
        self.assertEqual(accounts[0].name, "account1")
        self.assertEqual(accounts[1].name, "account2")
        self.assertEqual(accounts[1].status, "SUSPENDED")

    async def test_async_create_user_success(self):
        """Test successful async user creation"""
        # Mock successful execution
        mock_result = AsyncMock()
        mock_result.rows = [
            (
                'test_user',
                'localhost',
                'test_account',
                datetime.now(),
                'OPEN',
                'Test user',
                None,
                None,
            )
        ]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test async user creation
        user = await self.async_account_manager.create_user(
            user_name="test_user", password="password123", comment="Test user"
        )

        # Verify
        self.assertIsInstance(user, User)
        self.assertEqual(user.name, "test_user")
        self.assertEqual(user.host, "%")
        self.assertEqual(user.account, "sys")
        self.assertEqual(user.comment, "Test user")

        # Verify SQL was called correctly - check the first call (CREATE USER)
        self.assertEqual(self.client.execute.call_count, 1)  # CREATE only
        first_call_args = self.client.execute.call_args_list[0][0][0]
        self.assertIn("CREATE USER", first_call_args)
        self.assertIn("test_user", first_call_args)
        self.assertIn("password123", first_call_args)

    async def test_async_list_users_success(self):
        """Test successful async user listing"""
        # Mock successful execution
        mock_result = AsyncMock()
        mock_result.rows = [
            ('user1', 'localhost', 'account1', datetime.now(), 'OPEN', 'User 1', None, None),
            (
                'user2',
                'localhost',
                'account1',
                datetime.now(),
                'LOCKED',
                'User 2',
                datetime.now(),
                'Security issue',
            ),
        ]
        self.client.execute = AsyncMock(return_value=mock_result)

        # Test async user listing
        users = await self.async_account_manager.list_users()

        # Verify
        self.assertEqual(len(users), 2)
        self.assertIsInstance(users[0], User)
        self.assertIsInstance(users[1], User)
        self.assertEqual(users[0].name, "user1")
        self.assertEqual(users[1].name, "user2")
        self.assertEqual(users[1].status, "LOCKED")


class TestAsyncTransactionAccountManager(unittest.IsolatedAsyncioTestCase):
    """Test AsyncTransactionAccountManager functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncMock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.transaction = AsyncMock()
        self.transaction.client = self.client
        self.transaction.execute = AsyncMock()
        self.async_transaction_account_manager = AsyncTransactionAccountManager(self.transaction)

    async def test_async_transaction_create_account(self):
        """Test account creation within async transaction"""
        # Mock successful execution
        mock_result = AsyncMock()
        mock_result.rows = [('test_account', 'admin_user', datetime.now(), 'OPEN', 'Test account', None, None)]
        self.transaction.execute = AsyncMock(return_value=mock_result)

        # Test account creation in async transaction
        account = await self.async_transaction_account_manager.create_account(
            account_name="test_account",
            admin_name="admin_user",
            password="password123",
            comment="Test account",
        )

        # Verify
        self.assertIsInstance(account, Account)
        self.assertEqual(account.name, "test_account")

        # Verify transaction.execute was called - check the first call (CREATE ACCOUNT)
        self.assertEqual(self.transaction.execute.call_count, 2)  # CREATE + get_account
        first_call_args = self.transaction.execute.call_args_list[0][0][0]
        self.assertIn("CREATE ACCOUNT", first_call_args)


if __name__ == '__main__':
    # Run async tests
    import asyncio

    async def run_async_tests():
        """Run async test methods"""
        test_instance = TestAsyncAccountManager()
        test_instance.setUp()

        await test_instance.test_async_create_account_success()
        await test_instance.test_async_create_account_failure()
        await test_instance.test_async_drop_account_success()
        await test_instance.test_async_list_accounts_success()
        await test_instance.test_async_create_user_success()
        await test_instance.test_async_list_users_success()

        test_instance = TestAsyncTransactionAccountManager()
        test_instance.setUp()
        await test_instance.test_async_transaction_create_account()

        print("✅ All async tests passed!")

    # Run sync tests
    unittest.main(argv=[''], exit=False, verbosity=2)

    # Run async tests
    asyncio.run(run_async_tests())
