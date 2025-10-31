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
Online tests for account management functionality

These tests are inspired by example_02_account_management.py
"""

import pytest
from matrixone.account import AccountManager, AccountError


@pytest.mark.online
class TestAccountManagement:
    """Test account management functionality"""

    def test_create_and_drop_account(self, test_client):
        """Test creating and dropping an account"""
        account_manager = AccountManager(test_client)

        # Create account
        account_name = "test_account_001"
        admin_name = "test_admin_001"
        password = "test_password_001"

        # Try to drop the account first (in case it exists from previous test runs)
        try:
            account_manager.drop_account(account_name)
        except Exception:
            pass  # Ignore if account doesn't exist

        account = account_manager.create_account(account_name, admin_name, password)
        assert account is not None
        assert account.name == account_name

        # Drop account
        account_manager.drop_account(account_name)

        # Verify account is dropped (should raise error when trying to get it)
        try:
            account_manager.get_account(account_name)
            assert False, "Account should have been dropped"
        except AccountError:
            pass  # Expected

    def test_create_and_drop_user(self, test_client):
        """Test creating and dropping a user"""
        account_manager = AccountManager(test_client)

        # Create user
        username = "test_user_001"
        password = "test_password_001"

        # Try to drop user first in case it exists from a previous test
        try:
            account_manager.drop_user(username)
        except Exception:
            pass  # Ignore if user doesn't exist

        user = account_manager.create_user(username, password)
        assert user is not None
        assert user.name == username

        # List users (MatrixOne only returns current user)
        users = account_manager.list_users()
        assert isinstance(users, list)

        # Drop user
        account_manager.drop_user(username)

    def test_alter_account(self, test_client):
        """Test altering an account"""
        account_manager = AccountManager(test_client)

        # Create account first
        account_name = "test_account_002"
        admin_name = "test_admin_002"
        password = "test_password_002"

        # Try to drop the account first (in case it exists from previous test runs)
        try:
            account_manager.drop_account(account_name)
        except Exception:
            pass  # Ignore if account doesn't exist

        account = account_manager.create_account(account_name, admin_name, password)
        assert account is not None

        try:
            # Alter account (add comment)
            account_manager.alter_account(account_name, comment="Updated account")

            # Verify account still exists
            updated_account = account_manager.get_account(account_name)
            assert updated_account is not None
            assert updated_account.name == account_name

        finally:
            # Clean up
            account_manager.drop_account(account_name)

    def test_alter_user(self, test_client):
        """Test altering a user"""
        account_manager = AccountManager(test_client)

        # Create user first
        username = "test_user_002"
        password = "test_password_002"

        # Try to drop user first in case it exists from a previous test
        try:
            account_manager.drop_user(username)
        except Exception:
            pass  # Ignore if user doesn't exist

        user = account_manager.create_user(username, password)
        assert user is not None

        try:
            # Alter user (change password)
            new_password = "new_password_002"
            account_manager.alter_user(username, password=new_password)

        finally:
            # Clean up
            account_manager.drop_user(username)

    def test_account_management_with_transaction(self, test_client):
        """Test account management with transaction (limited by MatrixOne)"""
        account_manager = AccountManager(test_client)

        # Create account outside transaction (MatrixOne limitation)
        account_name = "test_account_003"
        admin_name = "test_admin_003"
        password = "test_password_003"

        # Try to drop the account first (in case it exists from previous test runs)
        try:
            account_manager.drop_account(account_name)
        except Exception:
            pass  # Ignore if account doesn't exist

        account = account_manager.create_account(account_name, admin_name, password)
        assert account is not None

        try:
            # Create user outside transaction (MatrixOne limitation)
            username = "test_user_003"
            user_password = "test_password_003"

            # Try to drop user first in case it exists from a previous test
            try:
                account_manager.drop_user(username)
            except Exception:
                pass  # Ignore if user doesn't exist

            user = account_manager.create_user(username, user_password)
            assert user is not None

            # Test transaction with regular SQL operations only
            with test_client.session():
                # Only regular SQL operations are allowed in transactions
                result = test_client.execute("SELECT 1 as test_value")
                assert result is not None
                assert len(result.rows) > 0

            # Alter user outside transaction
            account_manager.alter_user(username, password="new_password_003")

            # Drop user outside transaction
            account_manager.drop_user(username)

        finally:
            # Clean up account
            account_manager.drop_account(account_name)

    @pytest.mark.asyncio
    async def test_async_account_management(self, test_async_client):
        """Test async account management"""
        # Test async client with direct SQL operations instead of AccountManager
        # since AccountManager is designed for sync clients only

        # Test basic async operations
        result = await test_async_client.execute("SELECT 1 as test_value, USER() as user_info")
        assert result is not None
        assert len(result.rows) > 0
        assert result.rows[0][0] == 1  # test_value should be 1

        # Test async transaction
        async with test_async_client.session():
            result = await test_async_client.execute("SELECT 2 as test_value")
            assert result is not None
            assert len(result.rows) > 0
            assert result.rows[0][0] == 2

        # Test async database info queries
        result = await test_async_client.execute("SHOW DATABASES")
        assert result is not None
        assert len(result.rows) > 0

    def test_account_error_handling(self, test_client):
        """Test account error handling"""
        account_manager = AccountManager(test_client)

        # Test creating account with invalid name
        try:
            account_manager.create_account("", "", "password")
            assert False, "Should have failed with empty admin name"
        except AccountError:
            pass  # Expected

        # Test dropping non-existent account
        try:
            account_manager.drop_account("non_existent_account")
            assert False, "Should have failed with non-existent account"
        except AccountError:
            pass  # Expected

    def test_show_accounts_and_current_account_detection(self, test_client):
        """
        Test SHOW ACCOUNTS behavior and current account detection.

        This test verifies:
        1. SHOW ACCOUNTS returns different results for sys vs normal accounts
        2. _get_current_account_from_results correctly identifies the current account
        3. The logic works for both sys account and created accounts
        """
        account_manager = AccountManager(test_client)

        # Part 1: Test with sys account (current connection)
        print("\n=== Part 1: Testing with sys account ===")
        result = test_client.execute("SHOW ACCOUNTS")
        assert result is not None
        assert hasattr(result, 'rows')

        print(f"\nSHOW ACCOUNTS returned {len(result.rows)} rows:")
        for i, row in enumerate(result.rows):
            print(f"  Row {i}: {row}")

        # sys account should see multiple accounts
        current_account = account_manager._get_current_account_from_results(result.rows)
        print(f"\nDetected current account: {current_account}")

        if len(result.rows) > 1:
            assert current_account == "sys", f"Expected 'sys' for multiple accounts, got '{current_account}'"
            print("✓ Correctly identified as sys account (can see multiple accounts)")
        else:
            print("⚠ Warning: Only one account visible, may not be sys account")

        # Part 2: Test creating a new account and simulating what it would see
        print("\n=== Part 2: Testing account detection logic ===")
        test_account_name = "test_account_detection_001"

        try:
            # Create a test account
            try:
                account_manager.drop_account(test_account_name, if_exists=True)
            except Exception:
                pass

            account = account_manager.create_account(
                test_account_name, "test_admin", "test_password", "Test account for detection"
            )
            assert account is not None
            print(f"✓ Created test account: {test_account_name}")

            # Simulate what SHOW ACCOUNTS would return for a normal account (single row)
            simulated_single_account = [(test_account_name, 'test_admin', None, 'OPEN', 'Test account')]
            detected = account_manager._get_current_account_from_results(simulated_single_account)
            assert detected == test_account_name, f"Expected '{test_account_name}', got '{detected}'"
            print(f"✓ Correctly identified '{test_account_name}' from single account result")

            # Test with multiple accounts (sys scenario)
            simulated_multi_accounts = [
                ('sys', 'root', None, 'OPEN', 'System'),
                (test_account_name, 'test_admin', None, 'OPEN', 'Test'),
            ]
            detected_multi = account_manager._get_current_account_from_results(simulated_multi_accounts)
            assert detected_multi == "sys", f"Expected 'sys' for multiple accounts, got '{detected_multi}'"
            print("✓ Correctly identified 'sys' from multiple accounts result")

            # Test with empty result
            detected_empty = account_manager._get_current_account_from_results([])
            assert detected_empty == "sys", f"Expected 'sys' for empty result, got '{detected_empty}'"
            print("✓ Correctly defaulted to 'sys' for empty result")

        finally:
            # Clean up
            try:
                account_manager.drop_account(test_account_name)
                print(f"✓ Cleaned up test account: {test_account_name}")
            except Exception:
                pass

        print("\n✓ All account detection logic tests passed!")
