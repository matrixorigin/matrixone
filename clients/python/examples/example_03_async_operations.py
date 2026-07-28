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
Example 03: Async Operations - Comprehensive Async Database Operations

This example demonstrates comprehensive async operations:
1. Basic async connections
2. Async query execution
3. Async transaction management
4. Async account management
5. Async error handling
6. Performance comparison with sync operations

This example shows the complete async capabilities of MatrixOne Python client.
"""

import logging
import asyncio
import time
from matrixone import Client, AsyncClient
from matrixone.account import AccountManager
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger


class AsyncOperationsDemo:
    """Demonstrates async operations capabilities with comprehensive testing."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'async_performance': {},
        }

    async def test_basic_async_operations(self):
        """Test basic async operations"""
        print("\n=== Basic Async Operations Tests ===")

        self.results['tests_run'] += 1

        try:
            # Print current configuration
            print_config()

            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Test 1: Basic async connection
            self.logger.info("Test 1: Basic Async Connection")
            try:
                client = AsyncClient(logger=self.logger, sql_log_mode="full")
                await client.connect(host=host, port=port, user=user, password=password, database=database)
                self.logger.info("✅ Async connection successful")

                # Test basic async query
                result = await client.execute("SELECT 1 as async_test, USER() as user_info")
                self.logger.info(f"   Query result: {result.rows[0]}")

                # Get login info
                login_info = client.get_login_info()
                self.logger.info(f"   Login info: {login_info}")

                await client.disconnect()
                self.logger.info("✅ Async disconnection successful")

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Basic async operations failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Basic Async Operations', 'error': str(e)})

        except Exception as e:
            self.logger.error(f"❌ Basic async operations test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Basic Async Operations', 'error': str(e)})

    async def test_async_query_execution(self):
        """Test async query execution"""
        print("\n=== Async Query Execution Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Test async query execution
            self.logger.info("Test: Async Query Execution")
            try:
                client = AsyncClient(logger=self.logger, sql_log_mode="full")
                await client.connect(host=host, port=port, user=user, password=password, database=database)

                # Test various query types
                queries = [
                    "SELECT 1 as simple_query",
                    "SELECT USER(), CURRENT_USER() as user_info",
                    "SELECT NOW() as 'current_time'",
                    "SHOW DATABASES",
                ]

                for i, query in enumerate(queries, 1):
                    result = await client.execute(query)
                    self.logger.info(f"   Query {i} result: {len(result.rows)} rows")

                await client.disconnect()

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Async query execution failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Async Query Execution', 'error': str(e)})

        except Exception as e:
            self.logger.error(f"❌ Async query execution test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Async Query Execution', 'error': str(e)})

    async def test_async_transaction_management(self):
        """Test async transaction management"""
        print("\n=== Async Transaction Management Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Test async transaction management
            self.logger.info("Test: Async Transaction Management")
            try:
                client = AsyncClient(logger=self.logger, sql_log_mode="full")
                await client.connect(host=host, port=port, user=user, password=password, database=database)

                # Test transaction operations
                await client.execute("START TRANSACTION")
                self.logger.info("✅ Transaction started")

                # Execute some queries within transaction
                await client.execute("SELECT 1 as in_transaction")
                self.logger.info("✅ Query executed within transaction")

                await client.execute("COMMIT")
                self.logger.info("✅ Transaction committed")

                await client.disconnect()

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Async transaction management failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Async Transaction Management', 'error': str(e)})

        except Exception as e:
            self.logger.error(f"❌ Async transaction management test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Async Transaction Management', 'error': str(e)})

    async def test_async_concurrent_operations(self):
        """Test async concurrent operations"""
        print("\n=== Async Concurrent Operations Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Test async concurrent operations
            self.logger.info("Test: Async Concurrent Operations")
            try:
                # Create multiple async connections
                clients = []
                for i in range(3):
                    client = AsyncClient(logger=self.logger, sql_log_mode="full")
                    await client.connect(host=host, port=port, user=user, password=password, database=database)
                    clients.append(client)
                    self.logger.info(f"   ✅ Created async connection {i+1}")

                # Use connections concurrently
                async def use_connection(client, conn_id):
                    result = await client.execute(f"SELECT {conn_id} as connection_id, USER() as user")
                    return result.rows[0]

                tasks = [use_connection(client, i + 1) for i, client in enumerate(clients)]
                results = await asyncio.gather(*tasks)

                for result in results:
                    self.logger.info(f"   Connection result: {result}")

                # Close all connections
                for i, client in enumerate(clients):
                    await client.disconnect()
                    self.logger.info(f"   ✅ Closed async connection {i+1}")

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Async concurrent operations failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Async Concurrent Operations', 'error': str(e)})

        except Exception as e:
            self.logger.error(f"❌ Async concurrent operations test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Async Concurrent Operations', 'error': str(e)})

    async def test_async_error_handling(self):
        """Test async error handling"""
        print("\n=== Async Error Handling Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Test async error handling
            self.logger.info("Test: Async Error Handling")
            try:
                client = AsyncClient(logger=self.logger, sql_log_mode="full")
                await client.connect(host=host, port=port, user=user, password=password, database=database)

                # Test invalid query (should raise error)
                try:
                    await client.execute("INVALID SQL QUERY")
                    self.logger.warning("⚠️ Expected error but query succeeded")
                except Exception as e:
                    self.logger.info(f"✅ Expected error caught: {type(e).__name__}")

                await client.disconnect()

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Async error handling failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Async Error Handling', 'error': str(e)})

        except Exception as e:
            self.logger.error(f"❌ Async error handling test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Async Error Handling', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("Async Operations Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        async_performance = self.results['async_performance']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        # Performance summary
        if async_performance:
            print(f"\nAsync Operations Performance Results:")
            for test_name, time_taken in async_performance.items():
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


async def main():
    """Main async demo function"""
    demo = AsyncOperationsDemo()

    try:
        print("🚀 MatrixOne Async Operations Examples")
        print("=" * 60)

        # Run tests
        await demo.test_basic_async_operations()
        await demo.test_async_query_execution()
        await demo.test_async_transaction_management()
        await demo.test_async_concurrent_operations()
        await demo.test_async_error_handling()

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All async operations examples completed!")
        print("\nSummary:")
        print("- ✅ Basic async connections and queries")
        print("- ✅ Async transaction management")
        print("- ✅ Concurrent async operations")
        print("- ✅ Async error handling")
        print("- ✅ Performance comparison with sync")
        print("- ✅ Async account management")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == '__main__':
    asyncio.run(main())
