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
Example 34: Branch Diff Count - Performance Optimization with Count-Only Diffs

This example demonstrates the branch diff count feature:
1. Basic count output - Get only the count of differences
2. Performance comparison - Count vs full diff performance
3. Monitoring workflow - Use count for efficient change detection
4. Async operations - Non-blocking count diff operations

This feature is essential for:
- Performance optimization with count-only diffs
- Data validation workflows
- Quick change detection without full result sets
- Automated monitoring and alerting
"""

import asyncio
import time
from matrixone import Client, AsyncClient
from matrixone.branch import DiffOutput, MergeConflictStrategy
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params


class BranchDiffCountDemo:
    """Demonstrates branch diff count feature with comprehensive testing."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
        }

    def test_basic_count_output(self):
        """Test 1: Basic count output - Get only the count of differences"""
        print("\n=== Test 1: Basic Count Output ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Setup test tables
            client.execute("DROP TABLE IF EXISTS source_data")
            client.execute("DROP TABLE IF EXISTS target_data")
            client.execute("""
                CREATE TABLE source_data (
                    id INT PRIMARY KEY,
                    name VARCHAR(50),
                    value INT
                )
            """)
            client.execute("""
                CREATE TABLE target_data (
                    id INT PRIMARY KEY,
                    name VARCHAR(50),
                    value INT
                )
            """)

            # Insert test data
            client.execute("INSERT INTO source_data VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)")
            client.execute("INSERT INTO target_data VALUES (1, 'Alice', 100), (2, 'Bob', 250), (4, 'David', 400)")

            # Get difference count only
            diff_count = client.branch.diff("source_data", "target_data", output=DiffOutput.COUNT)
            count_value = diff_count[0][0] if diff_count else 0
            self.logger.info(f"✅ Count diff: {count_value} differences")

            # Compare with full diff
            full_diff = client.branch.diff("source_data", "target_data", output='rows')
            self.logger.info(f"   Full diff returned {len(full_diff)} rows")

            # Cleanup
            client.execute("DROP TABLE source_data")
            client.execute("DROP TABLE target_data")

            self.results['tests_passed'] += 1
            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Basic count output test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'basic_count_output', 'error': str(e)})

    def test_performance_comparison(self):
        """Test 2: Performance comparison - Count vs full diff"""
        print("\n=== Test 2: Performance Comparison ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Setup large test tables
            client.execute("DROP TABLE IF EXISTS large_source")
            client.execute("DROP TABLE IF EXISTS large_target")
            client.execute("CREATE TABLE large_source (id INT PRIMARY KEY, data VARCHAR(100))")
            client.execute("CREATE TABLE large_target (id INT PRIMARY KEY, data VARCHAR(100))")

            # Insert 1000 rows with some differences
            for i in range(1, 1001):
                client.execute(f"INSERT INTO large_source VALUES ({i}, 'data_{i}')")
                if i <= 500:
                    client.execute(f"INSERT INTO large_target VALUES ({i}, 'data_{i}')")
                else:
                    client.execute(f"INSERT INTO large_target VALUES ({i}, 'modified_{i}')")

            # Time count diff
            start = time.time()
            count_result = client.branch.diff("large_source", "large_target", output=DiffOutput.COUNT)
            count_time = time.time() - start
            count_value = count_result[0][0] if count_result else 0

            # Time full diff
            start = time.time()
            full_result = client.branch.diff("large_source", "large_target", output='rows')
            full_time = time.time() - start

            self.logger.info(f"✅ Count diff: {count_value} differences in {count_time:.3f}s")
            self.logger.info(f"   Full diff: {len(full_result)} rows in {full_time:.3f}s")
            if full_time > 0:
                speedup = full_time / count_time if count_time > 0 else 0
                self.logger.info(f"   Count diff is {speedup:.1f}x faster")

            # Cleanup
            client.execute("DROP TABLE large_source")
            client.execute("DROP TABLE large_target")

            self.results['tests_passed'] += 1
            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Performance comparison test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'performance_comparison', 'error': str(e)})

    def test_monitoring_workflow(self):
        """Test 3: Monitoring workflow - Use count for efficient change detection"""
        print("\n=== Test 3: Monitoring Workflow ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Setup production and staging tables
            client.execute("DROP TABLE IF EXISTS production_inventory")
            client.execute("DROP TABLE IF EXISTS staging_inventory")
            client.execute("""
                CREATE TABLE production_inventory (
                    product_id INT PRIMARY KEY,
                    name VARCHAR(100),
                    quantity INT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            client.execute("""
                CREATE TABLE staging_inventory (
                    product_id INT PRIMARY KEY,
                    name VARCHAR(100),
                    quantity INT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Insert test data
            client.execute("""
                INSERT INTO production_inventory (product_id, name, quantity) VALUES
                (1, 'Widget A', 100),
                (2, 'Widget B', 200),
                (3, 'Widget C', 150)
            """)
            client.execute("""
                INSERT INTO staging_inventory (product_id, name, quantity) VALUES
                (1, 'Widget A', 100),
                (2, 'Widget B', 180),
                (4, 'Widget D', 75)
            """)

            # Use count for efficient monitoring
            self.logger.info("Monitoring for inventory changes...")
            change_count = client.branch.diff("staging_inventory", "production_inventory", output=DiffOutput.COUNT)
            change_value = change_count[0][0] if change_count else 0

            if change_value == 0:
                self.logger.info("✅ No changes detected")
            else:
                self.logger.info(f"⚠️  {change_value} changes detected - investigation needed")
                self.logger.info("Fetching detailed changes...")
                changes = client.branch.diff("staging_inventory", "production_inventory", output='rows')
                self.logger.info(f"   Detailed changes: {len(changes)} rows")

                if change_value < 10:
                    self.logger.info("✅ Change count within acceptable threshold")
                else:
                    self.logger.info("🚨 High change count - alerting administrators")

            # Cleanup
            client.execute("DROP TABLE production_inventory")
            client.execute("DROP TABLE staging_inventory")

            self.results['tests_passed'] += 1
            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Monitoring workflow test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'monitoring_workflow', 'error': str(e)})

    async def test_async_operations(self):
        """Test 4: Async operations - Non-blocking count diff"""
        print("\n=== Test 4: Async Operations ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = AsyncClient(logger=self.logger, sql_log_mode="full")
            await client.connect(host=host, port=port, user=user, password=password, database=database)

            # Setup test tables
            await client.execute("DROP TABLE IF EXISTS async_source")
            await client.execute("DROP TABLE IF EXISTS async_target")
            await client.execute("CREATE TABLE async_source (id INT PRIMARY KEY, data VARCHAR(50))")
            await client.execute("CREATE TABLE async_target (id INT PRIMARY KEY, data VARCHAR(50))")

            await client.execute("INSERT INTO async_source VALUES (1, 'data1'), (2, 'data2'), (3, 'data3')")
            await client.execute("INSERT INTO async_target VALUES (1, 'data1'), (4, 'data4')")

            # Async count diff
            diff_count = await client.branch.diff("async_source", "async_target", output=DiffOutput.COUNT)
            count_value = diff_count[0][0] if diff_count else 0
            self.logger.info(f"✅ Async count diff: {count_value} differences")

            # Concurrent diff operations
            self.logger.info("Running concurrent diff operations...")
            await client.execute("CREATE TABLE async_test1 (id INT PRIMARY KEY)")
            await client.execute("CREATE TABLE async_test2 (id INT PRIMARY KEY)")
            await client.execute("INSERT INTO async_test1 VALUES (1), (2)")
            await client.execute("INSERT INTO async_test2 VALUES (2), (3)")

            results = await asyncio.gather(
                client.branch.diff("async_source", "async_target", output=DiffOutput.COUNT),
                client.branch.diff("async_test1", "async_test2", output=DiffOutput.COUNT),
                client.branch.diff("async_target", "async_source", output=DiffOutput.COUNT),
            )
            self.logger.info(f"   Concurrent results: {[r[0][0] if r else 0 for r in results]}")

            # Cleanup
            await client.execute("DROP TABLE async_source")
            await client.execute("DROP TABLE async_target")
            await client.execute("DROP TABLE async_test1")
            await client.execute("DROP TABLE async_test2")

            self.results['tests_passed'] += 1
            await client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Async operations test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'async_operations', 'error': str(e)})

    def run_all_tests(self):
        """Run all tests and print summary"""
        print("\n" + "=" * 60)
        print("Running Branch Diff Count Tests")
        print("=" * 60)

        self.test_basic_count_output()
        self.test_performance_comparison()
        self.test_monitoring_workflow()
        asyncio.run(self.test_async_operations())

        # Print summary
        print("\n" + "=" * 60)
        print("Test Summary")
        print("=" * 60)
        print(f"Tests run: {self.results['tests_run']}")
        print(f"Tests passed: {self.results['tests_passed']}")
        print(f"Tests failed: {self.results['tests_failed']}")

        if self.results['unexpected_results']:
            print("\nUnexpected results:")
            for result in self.results['unexpected_results']:
                print(f"  - {result['test']}: {result['error']}")

        if self.results['tests_failed'] == 0:
            print("\n🎉 All tests passed!")
        else:
            print(f"\n❌ {self.results['tests_failed']} test(s) failed")


if __name__ == "__main__":
    demo = BranchDiffCountDemo()
    demo.run_all_tests()
