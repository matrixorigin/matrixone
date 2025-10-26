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
from unittest.mock import Mock, AsyncMock
from datetime import datetime
from matrixone.account import AccountManager, Account, User, AsyncAccountManager
from matrixone.exceptions import AccountError


class TestAccountSQLConsistency(unittest.TestCase):
    """Test SQL generation consistency across sync/async/session interfaces"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"

        self.session = Mock()
        self.session.client = self.client

        # Create managers
        self.sync_manager = AccountManager(self.client)
        self.session_manager = AccountManager(self.client, executor=self.session)
        self.async_manager = AsyncAccountManager(self.client)
        self.async_session_manager = AsyncAccountManager(self.client, executor=self.session)

    def test_create_account_sql_consistency(self):
        """Test that create_account generates identical SQL across all versions"""
        # Capture SQL from sync version (first call is CREATE, second is SHOW ACCOUNTS)
        self.client.execute = Mock(return_value=Mock(rows=[]))
        self.session.execute = Mock(return_value=Mock(rows=[]))

        try:
            self.sync_manager.create_account("test_acc", "admin", "pass123", "comment")
        except Exception:
            pass
        sync_sql = self.client.execute.call_args_list[0][0][0]  # First call

        try:
            self.session_manager.create_account("test_acc", "admin", "pass123", "comment")
        except Exception:
            pass
        session_sql = self.session.execute.call_args_list[0][0][0]  # First call

        # Verify SQL is identical
        expected_sql = "CREATE ACCOUNT `test_acc` ADMIN_NAME 'admin' IDENTIFIED BY 'pass123' COMMENT 'comment'"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)
        self.assertEqual(sync_sql, session_sql)

    def test_drop_account_sql_consistency(self):
        """Test that drop_account generates identical SQL"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        self.sync_manager.drop_account("test_acc")
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.drop_account("test_acc")
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "DROP ACCOUNT `test_acc`"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_drop_account_if_exists_sql_consistency(self):
        """Test DROP ACCOUNT IF EXISTS SQL"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        self.sync_manager.drop_account("test_acc", if_exists=True)
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.drop_account("test_acc", if_exists=True)
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "DROP ACCOUNT IF EXISTS `test_acc`"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_get_account_sql_consistency(self):
        """Test that get_account generates identical SQL"""
        self.client.execute = Mock(return_value=Mock(rows=[]))
        self.session.execute = Mock(return_value=Mock(rows=[]))

        try:
            self.sync_manager.get_account("test_acc")
        except Exception:
            pass
        sync_sql = self.client.execute.call_args[0][0]

        try:
            self.session_manager.get_account("test_acc")
        except Exception:
            pass
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "SHOW ACCOUNTS"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_list_accounts_sql_consistency(self):
        """Test that list_accounts generates identical SQL"""
        self.client.execute = Mock(return_value=Mock(rows=[]))
        self.session.execute = Mock(return_value=Mock(rows=[]))

        self.sync_manager.list_accounts()
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.list_accounts()
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "SHOW ACCOUNTS"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_create_user_sql_consistency(self):
        """Test that create_user generates identical SQL"""

        # Mock to return proper results for both CREATE USER and SHOW ACCOUNTS
        def mock_sync_execute(sql):
            result = Mock()
            if sql.startswith('SHOW ACCOUNTS'):
                result.rows = [('sys', 'root', None, 'OPEN', None)]
            else:
                result.rows = []
            return result

        def mock_session_execute(sql):
            result = Mock()
            if sql.startswith('SHOW ACCOUNTS'):
                result.rows = [('sys', 'root', None, 'OPEN', None)]
            else:
                result.rows = []
            return result

        self.client.execute = Mock(side_effect=mock_sync_execute)
        self.session.execute = Mock(side_effect=mock_session_execute)

        self.sync_manager.create_user("test_user", "pass123")
        sync_sql = self.client.execute.call_args_list[0][0][0]  # First call is CREATE USER

        self.session_manager.create_user("test_user", "pass123")
        session_sql = self.session.execute.call_args_list[0][0][0]  # First call is CREATE USER

        expected_sql = "CREATE USER `test_user` IDENTIFIED BY 'pass123'"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_drop_user_sql_consistency(self):
        """Test that drop_user generates identical SQL"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        self.sync_manager.drop_user("test_user")
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.drop_user("test_user")
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "DROP USER `test_user`"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_drop_user_if_exists_sql_consistency(self):
        """Test DROP USER IF EXISTS SQL"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        self.sync_manager.drop_user("test_user", if_exists=True)
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.drop_user("test_user", if_exists=True)
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "DROP USER IF EXISTS `test_user`"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)


class TestAsyncAccountSQLConsistency(unittest.IsolatedAsyncioTestCase):
    """Test SQL generation consistency for async versions"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncMock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"

        self.session = AsyncMock()
        self.session.client = self.client

        self.async_manager = AsyncAccountManager(self.client)
        self.async_session_manager = AsyncAccountManager(self.client, executor=self.session)

    async def test_async_create_account_sql_consistency(self):
        """Test async create_account SQL"""
        self.client.execute = AsyncMock(return_value=AsyncMock(rows=[]))
        self.session.execute = AsyncMock(return_value=AsyncMock(rows=[]))

        try:
            await self.async_manager.create_account("test_acc", "admin", "pass123", "comment")
        except Exception:
            pass
        async_sql = self.client.execute.call_args_list[0][0][0]  # First call

        try:
            await self.async_session_manager.create_account("test_acc", "admin", "pass123", "comment")
        except Exception:
            pass
        async_session_sql = self.session.execute.call_args_list[0][0][0]  # First call

        expected_sql = "CREATE ACCOUNT `test_acc` ADMIN_NAME 'admin' IDENTIFIED BY 'pass123' COMMENT 'comment'"
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)

    async def test_async_drop_account_sql_consistency(self):
        """Test async drop_account SQL"""
        self.client.execute = AsyncMock()
        self.session.execute = AsyncMock()

        await self.async_manager.drop_account("test_acc")
        async_sql = self.client.execute.call_args[0][0]

        await self.async_session_manager.drop_account("test_acc")
        async_session_sql = self.session.execute.call_args[0][0]

        expected_sql = "DROP ACCOUNT `test_acc`"
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)

    async def test_async_create_user_sql_consistency(self):
        """Test async create_user SQL"""

        # Mock to return proper results for both CREATE USER and SHOW ACCOUNTS
        async def mock_async_execute(sql):
            result = AsyncMock()
            if sql.startswith('SHOW ACCOUNTS'):
                result.rows = [('sys', 'root', None, 'OPEN', None)]
            else:
                result.rows = []
            return result

        async def mock_session_execute(sql):
            result = AsyncMock()
            if sql.startswith('SHOW ACCOUNTS'):
                result.rows = [('sys', 'root', None, 'OPEN', None)]
            else:
                result.rows = []
            return result

        self.client.execute = AsyncMock(side_effect=mock_async_execute)
        self.session.execute = AsyncMock(side_effect=mock_session_execute)

        await self.async_manager.create_user("test_user", "pass123")
        async_sql = self.client.execute.call_args_list[0][0][0]  # First call is CREATE USER

        await self.async_session_manager.create_user("test_user", "pass123")
        async_session_sql = self.session.execute.call_args_list[0][0][0]  # First call is CREATE USER

        expected_sql = "CREATE USER `test_user` IDENTIFIED BY 'pass123'"
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)


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

        # Mock successful execution
        # First call: CREATE USER (no result needed)
        # Second call: SHOW ACCOUNTS (returns multiple accounts for sys)
        def mock_execute(sql):
            result = Mock()
            if sql.startswith('SHOW ACCOUNTS'):
                # Multiple accounts means sys account
                result.rows = [
                    ('sys', 'root', datetime.now(), 'OPEN', 'System account'),
                    ('test_account', 'admin', datetime.now(), 'OPEN', 'Test account'),
                ]
            else:
                result.rows = []
            return result

        self.client.execute = Mock(side_effect=mock_execute)

        # Test user creation
        user = self.account_manager.create_user(user_name="test_user", password="password123", comment="Test user")

        # Verify
        self.assertIsInstance(user, User)
        self.assertEqual(user.name, "test_user")
        self.assertEqual(user.host, "%")
        self.assertEqual(user.account, "sys")
        self.assertEqual(user.comment, "Test user")

        # Verify SQL was called correctly - CREATE USER + SHOW ACCOUNTS
        self.assertEqual(self.client.execute.call_count, 2)
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
        # First call: SELECT CURRENT_USER()
        # Second call: SHOW ACCOUNTS (returns multiple accounts for sys)
        def mock_execute(sql):
            result = Mock()
            if sql.startswith('SELECT CURRENT_USER'):
                result.rows = [['test_user']]
            elif sql.startswith('SHOW ACCOUNTS'):
                # Multiple accounts means sys account
                result.rows = [
                    ('sys', 'root', datetime.now(), 'OPEN', 'System account'),
                    ('test_account', 'admin', datetime.now(), 'OPEN', 'Test account'),
                ]
            else:
                result.rows = []
            return result

        self.client.execute = Mock(side_effect=mock_execute)

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

        # Mock successful execution
        # First call: SELECT CURRENT_USER() (returns username)
        # Second call: SHOW ACCOUNTS (returns multiple accounts for sys)
        def mock_execute(sql):
            result = Mock()
            if sql.startswith('SELECT CURRENT_USER'):
                result.rows = [['current_user']]
            elif sql.startswith('SHOW ACCOUNTS'):
                # Multiple accounts means sys account
                result.rows = [
                    ('sys', 'root', datetime.now(), 'OPEN', 'System account'),
                    ('test_account', 'admin', datetime.now(), 'OPEN', 'Test account'),
                ]
            else:
                result.rows = []
            return result

        self.client.execute = Mock(side_effect=mock_execute)

        # Test user listing
        users = self.account_manager.list_users()

        # Verify
        self.assertEqual(len(users), 1)
        self.assertIsInstance(users[0], User)
        self.assertEqual(users[0].name, "current_user")


class TestTransactionAccountManager(unittest.TestCase):
    """Test AccountManager with executor (session) functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.transaction = Mock()
        self.transaction.client = self.client
        self.transaction.execute = Mock()
        self.transaction_account_manager = AccountManager(self.client, executor=self.transaction)

    def test_create_account_in_transaction(self):
        """Test account creation within transaction"""
        # Mock successful execution - first call for CREATE, second call for get_account (SHOW ACCOUNTS)
        mock_result = Mock()
        # SHOW ACCOUNTS format: account_name, admin_name, created, status, comments
        mock_result.rows = [['test_account', 'admin_user', datetime.now(), 'OPEN', 'Test account']]
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

        # Verify transaction.execute was called twice - CREATE and get_account (both use executor)
        self.assertEqual(self.transaction.execute.call_count, 2)
        self.assertEqual(self.client.execute.call_count, 0)  # Not used when executor is provided
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
        # First call: CREATE USER
        # Second call: SHOW ACCOUNTS (returns multiple accounts for sys)
        async def mock_execute(sql):
            result = AsyncMock()
            if sql.startswith('SHOW ACCOUNTS'):
                # Multiple accounts means sys account
                result.rows = [
                    ('sys', 'root', datetime.now(), 'OPEN', 'System account'),
                    ('test_account', 'admin', datetime.now(), 'OPEN', 'Test account'),
                ]
            else:
                result.rows = []
            return result

        self.client.execute = AsyncMock(side_effect=mock_execute)

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

        # Verify SQL was called correctly - CREATE USER + SHOW ACCOUNTS
        self.assertEqual(self.client.execute.call_count, 2)
        first_call_args = self.client.execute.call_args_list[0][0][0]
        self.assertIn("CREATE USER", first_call_args)
        self.assertIn("test_user", first_call_args)
        self.assertIn("password123", first_call_args)

    async def test_async_list_users_success(self):
        """Test successful async user listing"""

        # Mock successful execution
        # First call: SELECT CURRENT_USER()
        # Second call: SHOW ACCOUNTS (returns multiple accounts for sys)
        async def mock_execute(sql):
            result = AsyncMock()
            if sql.startswith('SELECT CURRENT_USER'):
                result.rows = [['current_user']]
            elif sql.startswith('SHOW ACCOUNTS'):
                # Multiple accounts means sys account
                result.rows = [
                    ('sys', 'root', datetime.now(), 'OPEN', 'System account'),
                    ('test_account', 'admin', datetime.now(), 'OPEN', 'Test account'),
                ]
            else:
                result.rows = []
            return result

        self.client.execute = AsyncMock(side_effect=mock_execute)

        # Test async user listing
        users = await self.async_account_manager.list_users()

        # Verify
        self.assertEqual(len(users), 1)
        self.assertIsInstance(users[0], User)
        self.assertEqual(users[0].name, "current_user")
        self.assertEqual(users[0].account, "sys")


class TestAsyncTransactionAccountManager(unittest.IsolatedAsyncioTestCase):
    """Test AsyncAccountManager with executor (session) functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncMock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"
        self.transaction = AsyncMock()
        self.transaction.client = self.client
        self.transaction.execute = AsyncMock()
        self.async_transaction_account_manager = AsyncAccountManager(self.client, executor=self.transaction)

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
