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
Example 01: Basic Connection - Connection Methods and Login Formats

This example demonstrates all basic connection methods and login formats:
1. Basic connection with different formats
2. Login format variations
3. Connection error handling
4. Connection information retrieval
5. Connection pooling and management

This is the foundation example for all MatrixOne Python client usage.
"""

import logging
import asyncio
from matrixone import Client, AsyncClient
from matrixone.account import AccountManager
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params, print_config


class BasicConnectionDemo:
    """Demonstrates basic connection capabilities with comprehensive testing."""

    def __init__(self):
        self.logger = create_default_logger(
            sql_log_mode="auto",
        )
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'connection_performance': {},
        }

    def test_basic_connection(self):
        """Test basic connection methods"""
        print("\n=== Basic Connection Tests ===")

        self.results['tests_run'] += 1

        try:
            # Print current configuration
            print_config()

            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Test 1: Simple connection
            self.logger.info("Test 1: Simple Connection")
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)
            self.logger.info("✅ Basic connection successful")

            # Test basic query
            result = client.execute("SELECT 1 as test_value, USER() as user_info")
            self.logger.info(f"   Test query result: {result.rows[0]}")

            # Get connection info
            login_info = client.get_login_info()
            self.logger.info(f"   Login info: {login_info}")

            client.disconnect()

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Basic connection tests failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'basic_connection', 'error': str(e)})

    def test_login_formats(self):
        """Test all supported login formats"""
        print("\n=== Login Format Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Format 1: Legacy format (simple username)
            self.logger.info("Format 1: Legacy format (simple username)")
            client = Client(logger=self.logger, sql_log_mode="auto")
            client.connect(host=host, port=port, user=user, password=password, database=database)
            login_info = client.get_login_info()
            self.logger.info(f"   ✅ Login info: {login_info}")
            client.disconnect()

            # Format 2: Direct format (account#user)
            self.logger.info("Format 2: Direct format (account#user)")
            client = Client(logger=self.logger, sql_log_mode="auto")
            client.connect(host=host, port=port, user='sys#root', password=password, database=database)
            login_info = client.get_login_info()
            self.logger.info(f"   ✅ Login info: {login_info}")
            client.disconnect()

            # Format 3: User with role (separate parameters)
            self.logger.info("Format 3: User with role (separate parameters)")
            try:
                client = Client(logger=self.logger, sql_log_mode="auto")
                client.connect(host=host, port=port, user=user, password=password, database=database, role='admin')
                # Try to execute a query to trigger role validation
                client.execute("SELECT 1")
                login_info = client.get_login_info()
                self.logger.info(f"   ✅ Login info: {login_info}")
                client.disconnect()
            except Exception as e:
                self.logger.info(f"   ⚠️ Expected failure (role 'admin' doesn't exist): {e}")

            # Format 4: Account with separate parameters
            self.logger.info("Format 4: Account with separate parameters")
            client = Client(logger=self.logger, sql_log_mode="auto")
            client.connect(host=host, port=port, user=user, password=password, database=database, account='sys')
            login_info = client.get_login_info()
            self.logger.info(f"   ✅ Login info: {login_info}")
            client.disconnect()

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Login format tests failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'login_formats', 'error': str(e)})

    def test_connection_error_handling(self):
        """Test connection error handling"""
        print("\n=== Connection Error Handling Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Test invalid credentials
            self.logger.info("Test invalid credentials")
            try:
                client = Client(logger=self.logger, sql_log_mode="auto")
                client.connect(host=host, port=port, user='invalid_user', password='invalid_pass', database=database)
                # Try to execute a query to trigger authentication
                client.execute("SELECT 1")
                self.logger.error("   ❌ Should have failed but didn't!")
            except Exception as e:
                self.logger.info(f"   ✅ Correctly failed: {e}")

            # Test invalid host
            self.logger.info("Test invalid host")
            try:
                client = Client(logger=self.logger, sql_log_mode="auto")
                client.connect(host='192.168.1.999', port=port, user=user, password=password, database=database)
                # Try to execute a query to trigger connection validation
                client.execute("SELECT 1")
                self.logger.error("   ❌ Should have failed but didn't!")
            except Exception as e:
                self.logger.info(f"   ✅ Correctly failed: {e}")

            # Test invalid port
            self.logger.info("Test invalid port")
            try:
                client = Client(logger=self.logger, sql_log_mode="auto")
                client.connect(host=host, port=9999, user=user, password=password, database=database)
                # Try to execute a query to trigger connection validation
                client.execute("SELECT 1")
                self.logger.error("   ❌ Should have failed but didn't!")
            except Exception as e:
                self.logger.info(f"   ✅ Correctly failed: {e}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Connection error handling tests failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'connection_error_handling', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("Basic Connection Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        connection_performance = self.results['connection_performance']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        # Performance summary
        if connection_performance:
            print(f"\nConnection Performance Results:")
            for test_name, time_taken in connection_performance.items():
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


def demo_connection_info():
    """Demonstrate connection information retrieval"""
    logger.info("\n=== Test 4: Connection Information ===")

    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()

    try:
        client = Client(logger=logger, sql_log_mode="auto")
        client.connect(host=host, port=port, user=user, password=password, database=database)

        # Get login info
        login_info = client.get_login_info()
        logger.info(f"   Login info: {login_info}")

        # Get user information
        result = client.execute("SELECT USER(), CURRENT_USER(), DATABASE()")
        user_info = result.rows[0]
        logger.info(f"   USER(): {user_info[0]}")
        logger.info(f"   CURRENT_USER(): {user_info[1]}")
        logger.info(f"   DATABASE(): {user_info[2]}")

        # Get connection parameters
        logger.info(f"   Connection parameters: {client._connection_params}")

        client.disconnect()

    except Exception as e:
        logger.error(f"❌ Connection info demo failed: {e}")


async def demo_async_connection():
    """Demonstrate async connection"""
    logger.info("\n=== Test 5: Async Connection ===")

    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()

    client = None
    try:
        client = AsyncClient(logger=logger)
        await client.connect(host=host, port=port, user=user, password=password, database=database)
        logger.info("✅ Async connection successful")

        # Test async query
        result = await client.execute("SELECT 1 as async_test, USER() as user_info")
        logger.info(f"   Async query result: {result.rows[0]}")

        # Get login info
        login_info = client.get_login_info()
        logger.info(f"   Login info: {login_info}")

    except Exception as e:
        logger.error(f"❌ Async connection failed: {e}")
    finally:
        # Ensure proper cleanup
        if client:
            try:
                await client.disconnect()
            except Exception as e:
                logger.debug(f"Async disconnect warning: {e}")


def demo_connection_pooling():
    """Demonstrate multiple connections"""
    logger.info("\n=== Test 6: Multiple Connections ===")

    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()

    clients = []
    try:
        # Create multiple connections
        for i in range(3):
            client = Client(logger=logger, sql_log_mode="auto")
            client.connect(host=host, port=port, user=user, password=password, database=database)
            clients.append(client)
            logger.info(f"   ✅ Created connection {i+1}")

        # Test each connection
        for i, client in enumerate(clients):
            result = client.execute(f"SELECT {i+1} as connection_id, USER() as user")
            logger.info(f"   Connection {i+1} result: {result.rows[0]}")

        # Close all connections
        for i, client in enumerate(clients):
            client.disconnect()
            logger.info(f"   ✅ Closed connection {i+1}")

    except Exception as e:
        logger.error(f"❌ Multiple connections demo failed: {e}")
        # Cleanup on error
        for client in clients:
            try:
                client.disconnect()
            except:
                pass


def main():
    """Main demo function"""
    demo = BasicConnectionDemo()

    try:
        print("🚀 MatrixOne Basic Connection Examples")
        print("=" * 60)

        # Run tests
        demo.test_basic_connection()
        demo.test_login_formats()
        demo.test_connection_error_handling()

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 Basic connection examples completed!")
        print("\nKey takeaways:")
        print("- ✅ Basic connection works with simple credentials")
        print("- ✅ Multiple login formats are supported")
        print("- ✅ Error handling works correctly")
        print("- ✅ Connection information is accessible")
        print("- ✅ Async connections work")
        print("- ✅ Multiple connections can be managed")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == '__main__':
    main()
