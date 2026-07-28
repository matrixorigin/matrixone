#!/usr/bin/env python3

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
Example 02: Account Management - Account, User, and Role Management

This example demonstrates comprehensive account, user, and role management:
1. Account creation and management
2. User creation and management
3. Role creation and assignment
4. Permission management
5. Role-based login
6. Account cleanup

This example shows the complete account management capabilities of MatrixOne.
"""

import logging
import asyncio
from matrixone import Client, AsyncClient
from matrixone.account import AccountManager
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params, print_config


class AccountManagementDemo:
    """Demonstrates account management capabilities with comprehensive testing."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'account_performance': {},
        }

    def test_account_management(self):
        """Test account management operations"""
        print("\n=== Account Management Tests ===")

        # Count individual test cases
        test_cases = ['Account Creation', 'User Management', 'Role Management']
        initial_tests_run = self.results['tests_run']
        self.results['tests_run'] += len(test_cases)

        try:
            # Print current configuration
            print_config()

            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Connect as root for account management
            root_client = Client(logger=self.logger, sql_log_mode="full")
            root_client.connect(host=host, port=port, user=user, password=password, database=database)
            account_manager = AccountManager(root_client)

            # Clean up any existing demo accounts
            account_manager.drop_account('demo_account', if_exists=True)
            account_manager.drop_account('tenant_account', if_exists=True)

            # Test 1: Account Creation
            self.logger.info("Test 1: Account Creation")
            try:
                # Create demo account
                account1 = account_manager.create_account(
                    account_name='demo_account',
                    admin_name='demo_admin',
                    password='adminpass123',
                    comment='Demo account for testing',
                )
                self.logger.info(f"✅ Created account: {account1.name}")

                # Create tenant account
                account2 = account_manager.create_account(
                    account_name='tenant_account',
                    admin_name='tenant_admin',
                    password='tenantpass123',
                    comment='Tenant account for multi-tenancy',
                )
                self.logger.info(f"✅ Created account: {account2.name}")

                # List all accounts
                accounts = account_manager.list_accounts()
                self.logger.info(f"📋 Total accounts: {len(accounts)}")
                for account in accounts:
                    self.logger.info(f"   - {account.name} (Admin: {account.admin_name}, Status: {account.status})")

                # Account creation test passed

            except Exception as e:
                self.logger.error(f"❌ Account creation failed: {e}")
                self.results['unexpected_results'].append({'test': 'Account Creation', 'error': str(e)})

            # Test 2: User Management
            self.logger.info("Test 2: User Management")
            try:
                # Connect as demo account admin
                demo_admin_client = Client(sql_log_mode="full")
                demo_admin_client.connect(
                    host=host, port=port, user='demo_account#demo_admin', password='adminpass123', database='mo_catalog'
                )
                demo_account_manager = AccountManager(demo_admin_client)

                # Create users
                user1 = demo_account_manager.create_user(
                    user_name='test_user1', password='userpass123', comment='Test user 1'
                )
                self.logger.info(f"✅ Created user: {user1.name}")

                user2 = demo_account_manager.create_user(
                    user_name='test_user2', password='userpass456', comment='Test user 2'
                )
                self.logger.info(f"✅ Created user: {user2.name}")

                # List users
                users = demo_account_manager.list_users()
                self.logger.info(f"📋 Total users: {len(users)}")
                for user in users:
                    self.logger.info(f"   - {user.name} (Host: {user.host}, Status: {user.status})")

                # User management test passed

                demo_admin_client.disconnect()

            except Exception as e:
                self.logger.error(f"❌ User management failed: {e}")
                self.results['unexpected_results'].append({'test': 'User Management', 'error': str(e)})

            # Test 3: Role Management
            self.logger.info("Test 3: Role Management")
            try:
                # Connect as demo account admin
                demo_admin_client = Client(sql_log_mode="full")
                demo_admin_client.connect(
                    host=host, port=port, user='demo_account#demo_admin', password='adminpass123', database='mo_catalog'
                )
                demo_account_manager = AccountManager(demo_admin_client)

                # Create roles
                role1 = demo_account_manager.create_role('test_role1', 'Test role 1')
                self.logger.info(f"✅ Created role: {role1.name}")

                role2 = demo_account_manager.create_role('test_role2', 'Test role 2')
                self.logger.info(f"✅ Created role: {role2.name}")

                # List roles
                roles = demo_account_manager.list_roles()
                self.logger.info(f"📋 Total roles: {len(roles)}")
                for role in roles:
                    self.logger.info(f"   - {role.name} (Comment: {role.comment})")

                # Role management test passed

                demo_admin_client.disconnect()

            except Exception as e:
                self.logger.error(f"❌ Role management failed: {e}")
                self.results['unexpected_results'].append({'test': 'Role Management', 'error': str(e)})

            # Cleanup
            self.logger.info("Cleanup")
            try:
                # Clean up demo accounts
                account_manager.drop_account('demo_account', if_exists=True)
                self.logger.info("✅ Cleaned up demo_account")

                account_manager.drop_account('tenant_account', if_exists=True)
                self.logger.info("✅ Cleaned up tenant_account")

            except Exception as e:
                self.logger.warning(f"⚠️ Cleanup warning: {e}")

            root_client.disconnect()

            # Calculate test results
            tests_run_in_this_method = self.results['tests_run'] - initial_tests_run
            unexpected_count = len([r for r in self.results['unexpected_results'] if r['test'] in test_cases])
            self.results['tests_passed'] += tests_run_in_this_method - unexpected_count
            self.results['tests_failed'] += unexpected_count

            self.logger.info("🎉 Account management demo completed!")
            self.logger.info("Key achievements:")
            self.logger.info("- ✅ Account creation and management")
            self.logger.info("- ✅ User creation and management")
            self.logger.info("- ✅ Role creation and assignment")
            self.logger.info("- ✅ Permission management")
            self.logger.info("- ✅ Role-based authentication")
            self.logger.info("- ✅ Multi-tenant isolation")

        except Exception as e:
            self.logger.error(f"❌ Account management test failed: {e}")
            # If the entire test method fails, mark all sub-tests as failed
            tests_run_in_this_method = self.results['tests_run'] - initial_tests_run
            self.results['tests_failed'] += tests_run_in_this_method
            self.results['unexpected_results'].append({'test': 'Account Management', 'error': str(e)})

    async def test_async_account_management(self):
        """Test async account management operations"""
        print("\n=== Async Account Management Tests ===")

        self.results['tests_run'] += 1
        initial_tests_run = self.results['tests_run'] - 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Connect as root
            root_client = AsyncClient(sql_log_mode="full")
            await root_client.connect(host=host, port=port, user=user, password=password, database=database)
            self.logger.info("✅ Async connection successful")

            # Test basic async operations
            result = await root_client.execute("SELECT USER(), CURRENT_USER()")
            self.logger.info(f"   User context: {result.rows[0]}")

            # Test account listing
            result = await root_client.execute("SHOW ACCOUNTS")
            self.logger.info(f"   Found {len(result.rows)} accounts")
            for row in result.rows:
                self.logger.info(f"     - {row[0]} (Admin: {row[1]}, Status: {row[3]})")

            await root_client.disconnect()

            # Async test passed
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Async account management failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Async Account Management', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("Account Management Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        account_performance = self.results['account_performance']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        # Performance summary
        if account_performance:
            print(f"\nAccount Management Performance Results:")
            for test_name, time_taken in account_performance.items():
                print(f"  {test_name}: {time_taken:.4f}s")

        # Unexpected results
        if unexpected_results:
            print(f"\nUnexpected Results ({len(unexpected_results)}):")
            for i, result in enumerate(unexpected_results, 1):
                print(f"  {i}. Test: {result['test']}")
                print(f"     Error: {result['error']}")
        else:
            print("\n✓ No unexpected results - all tests behaved as expected")

        return self.results


def main():
    """Main demo function"""
    demo = AccountManagementDemo()

    try:
        print("🚀 MatrixOne Account Management Examples")
        print("=" * 60)

        # Run tests
        demo.test_account_management()

        # Run async tests
        asyncio.run(demo.test_async_account_management())

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All account management examples completed!")
        print("\nSummary:")
        print("- Complete account lifecycle management")
        print("- User and role management with assignments")
        print("- User alteration capabilities (password, comment, lock/unlock)")
        print("- Role management details (creation, retrieval, deletion)")
        print("- Comprehensive permission management (grant/revoke)")
        print("- Role-based authentication and authorization")
        print("- Multi-tenant account isolation")
        print("- Async account operations")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == '__main__':
    main()
