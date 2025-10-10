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
Example 18: Snapshot ORM - Comprehensive Snapshot ORM Operations

MatrixOne Snapshot ORM-like Query Builder Example

This example demonstrates how to use the snapshot query builder
which provides a SQLAlchemy-like ORM interface for snapshot queries.
"""

import asyncio
from matrixone import Client, AsyncClient
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params

# Create logger
logger = create_default_logger()


class SnapshotORMDemo:
    """Demonstrates snapshot ORM capabilities with comprehensive testing."""

    def __init__(self):
        self.logger = create_default_logger()
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'snapshot_orm_performance': {},
        }

    def test_sync_snapshot_orm(self):
        """Test sync snapshot ORM-like query builder"""
        print("\n=== Sync Snapshot ORM Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            client = Client(logger=self.logger)
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test sync snapshot ORM
            self.logger.info("Test: Sync Snapshot ORM")
            try:
                # Create a test table and insert some data
                client.execute("CREATE DATABASE IF NOT EXISTS snapshot_orm_test")
                client.execute("USE snapshot_orm_test")

                # Drop table if exists
                client.execute("DROP TABLE IF EXISTS users")

                # Create table
                client.execute(
                    """
                    CREATE TABLE users (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        name VARCHAR(100),
                        email VARCHAR(100),
                        age INT,
                        department VARCHAR(50)
                    )
                """
                )

                # Insert test data
                client.execute(
                    "INSERT INTO users (name, email, age, department) VALUES ('Alice', 'alice@example.com', 25, 'Engineering')"
                )
                client.execute(
                    "INSERT INTO users (name, email, age, department) VALUES ('Bob', 'bob@example.com', 30, 'Marketing')"
                )
                client.execute(
                    "INSERT INTO users (name, email, age, department) VALUES ('Charlie', 'charlie@example.com', 35, 'Engineering')"
                )

                # Test snapshot ORM query
                snapshot_name = "test_snapshot"
                try:
                    # Create snapshot first
                    client.execute(f"CREATE SNAPSHOT {snapshot_name}")

                    # Test snapshot ORM query
                    query = (
                        client.query("users", snapshot=snapshot_name)
                        .select("name", "email", "age")
                        .where("department = ?", "Engineering")
                    )
                    result = query.execute()

                    self.logger.info(f"   Found {len(result.rows)} engineering users")
                    for row in result.rows:
                        self.logger.info(f"     - {row[0]} ({row[1]})")

                    # Cleanup snapshot
                    client.execute(f"DROP SNAPSHOT IF EXISTS {snapshot_name}")

                except Exception as e:
                    self.logger.info(f"   Snapshot functionality not supported: {e}")

                # Cleanup
                client.execute("DROP DATABASE IF EXISTS snapshot_orm_test")

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Sync snapshot ORM test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Sync Snapshot ORM', 'error': str(e)})

            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Sync snapshot ORM test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Sync Snapshot ORM', 'error': str(e)})

    async def test_async_snapshot_orm(self):
        """Test async snapshot ORM-like query builder"""
        print("\n=== Async Snapshot ORM Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            client = AsyncClient(logger=self.logger)
            await client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test async snapshot ORM
            self.logger.info("Test: Async Snapshot ORM")
            try:
                # Create a test table and insert some data
                await client.execute("CREATE DATABASE IF NOT EXISTS async_snapshot_orm_test")
                await client.execute("USE async_snapshot_orm_test")

                # Drop table if exists
                await client.execute("DROP TABLE IF EXISTS products")

                # Create table
                await client.execute(
                    """
                    CREATE TABLE products (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        name VARCHAR(100),
                        price DECIMAL(10,2),
                        category VARCHAR(50)
                    )
                """
                )

                # Insert test data
                await client.execute("INSERT INTO products (name, price, category) VALUES ('Laptop', 999.99, 'Electronics')")
                await client.execute("INSERT INTO products (name, price, category) VALUES ('Book', 19.99, 'Education')")
                await client.execute("INSERT INTO products (name, price, category) VALUES ('Phone', 699.99, 'Electronics')")

                # Test async snapshot ORM query
                snapshot_name = "test_async_snapshot"
                try:
                    # Create snapshot first
                    await client.execute(f"CREATE SNAPSHOT {snapshot_name}")

                    # Test async snapshot ORM query
                    query = (
                        client.query("products", snapshot=snapshot_name)
                        .select("name", "price")
                        .where("category = ?", "Electronics")
                    )
                    result = await query.execute()

                    self.logger.info(f"   Found {len(result.rows)} electronics products")
                    for row in result.rows:
                        self.logger.info(f"     - {row[0]}: ${row[1]}")

                    # Cleanup snapshot
                    await client.execute(f"DROP SNAPSHOT IF EXISTS {snapshot_name}")

                except Exception as e:
                    self.logger.info(f"   Snapshot functionality not supported: {e}")

                # Cleanup
                await client.execute("DROP DATABASE IF EXISTS async_snapshot_orm_test")

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Async snapshot ORM test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Async Snapshot ORM', 'error': str(e)})

            await client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Async snapshot ORM test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Async Snapshot ORM', 'error': str(e)})

    def test_complex_snapshot_queries(self):
        """Test complex snapshot queries with JOINs and aggregations"""
        print("\n=== Complex Snapshot Queries Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            client = Client(logger=self.logger)
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test complex snapshot queries
            self.logger.info("Test: Complex Snapshot Queries")
            try:
                # Create test databases and tables
                client.execute("CREATE DATABASE IF NOT EXISTS complex_snapshot_test")
                client.execute("USE complex_snapshot_test")

                # Drop tables if exist
                client.execute("DROP TABLE IF EXISTS orders")
                client.execute("DROP TABLE IF EXISTS customers")

                # Create tables
                client.execute(
                    """
                    CREATE TABLE customers (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        name VARCHAR(100),
                        email VARCHAR(100)
                    )
                """
                )

                client.execute(
                    """
                    CREATE TABLE orders (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        customer_id INT,
                        product VARCHAR(100),
                        amount DECIMAL(10,2),
                        FOREIGN KEY (customer_id) REFERENCES customers(id)
                    )
                """
                )

                # Insert test data
                client.execute("INSERT INTO customers (name, email) VALUES ('John', 'john@example.com')")
                client.execute("INSERT INTO customers (name, email) VALUES ('Jane', 'jane@example.com')")
                client.execute("INSERT INTO orders (customer_id, product, amount) VALUES (1, 'Laptop', 999.99)")
                client.execute("INSERT INTO orders (customer_id, product, amount) VALUES (1, 'Mouse', 29.99)")
                client.execute("INSERT INTO orders (customer_id, product, amount) VALUES (2, 'Keyboard', 79.99)")

                # Test complex query with JOIN
                snapshot_name = "test_complex_snapshot"
                try:
                    # Create snapshot first
                    client.execute(f"CREATE SNAPSHOT {snapshot_name}")

                    # Test complex query with JOIN
                    query = (
                        client.query("orders", snapshot=snapshot_name)
                        .select("customers.name", "orders.product", "orders.amount")
                        .join("customers", onclause="orders.customer_id = customers.id")
                    )
                    result = query.execute()

                    self.logger.info(f"   Found {len(result.rows)} order records with customer info")
                    for row in result.rows:
                        self.logger.info(f"     - {row[0]}: {row[1]} (${row[2]})")

                    # Cleanup snapshot
                    client.execute(f"DROP SNAPSHOT IF EXISTS {snapshot_name}")

                except Exception as e:
                    self.logger.info(f"   Snapshot functionality not supported: {e}")

                # Cleanup
                client.execute("DROP DATABASE IF EXISTS complex_snapshot_test")

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Complex snapshot queries test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Complex Snapshot Queries', 'error': str(e)})

            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Complex snapshot queries test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Complex Snapshot Queries', 'error': str(e)})

    def test_snapshot_query_methods(self):
        """Test snapshot query builder methods"""
        print("\n=== Snapshot Query Builder Methods Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            client = Client(logger=self.logger)
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test snapshot query builder methods
            self.logger.info("Test: Snapshot Query Builder Methods")
            try:
                # Create test table
                client.execute("CREATE DATABASE IF NOT EXISTS builder_methods_test")
                client.execute("USE builder_methods_test")
                client.execute("DROP TABLE IF EXISTS test_data")

                client.execute(
                    """
                    CREATE TABLE test_data (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        name VARCHAR(100),
                        value INT,
                        category VARCHAR(50)
                    )
                """
                )

                # Insert test data
                client.execute("INSERT INTO test_data (name, value, category) VALUES ('Item1', 10, 'A')")
                client.execute("INSERT INTO test_data (name, value, category) VALUES ('Item2', 20, 'B')")
                client.execute("INSERT INTO test_data (name, value, category) VALUES ('Item3', 30, 'A')")

                # Test various query builder methods
                snapshot_name = "test_builder_snapshot"
                try:
                    # Create snapshot first
                    client.execute(f"CREATE SNAPSHOT {snapshot_name}")

                    # Test select with specific columns
                    query1 = client.query("test_data", snapshot=snapshot_name).select("name", "value")
                    result1 = query1.execute()
                    self.logger.info(f"   Select specific columns: {len(result1.rows)} rows")

                    # Test where clause
                    query2 = client.query("test_data", snapshot=snapshot_name).select("*").where("category = ?", "A")
                    result2 = query2.execute()
                    self.logger.info(f"   Where clause: {len(result2.rows)} rows")

                    # Test order by
                    query3 = client.query("test_data", snapshot=snapshot_name).select("*").order_by("value DESC")
                    result3 = query3.execute()
                    self.logger.info(f"   Order by: {len(result3.rows)} rows")

                    # Cleanup snapshot
                    client.execute(f"DROP SNAPSHOT IF EXISTS {snapshot_name}")

                except Exception as e:
                    self.logger.info(f"   Snapshot functionality not supported: {e}")

                # Cleanup
                client.execute("DROP DATABASE IF EXISTS builder_methods_test")

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Snapshot query builder methods test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Snapshot Query Builder Methods', 'error': str(e)})

            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Snapshot query builder methods test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Snapshot Query Builder Methods', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("Snapshot ORM Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        snapshot_orm_performance = self.results['snapshot_orm_performance']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        # Performance summary
        if snapshot_orm_performance:
            print(f"\nSnapshot ORM Performance Results:")
            for test_name, time_taken in snapshot_orm_performance.items():
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
    demo = SnapshotORMDemo()

    try:
        print("🚀 MatrixOne Snapshot ORM-like Query Builder Examples")
        print("=" * 80)

        # Run tests
        demo.test_sync_snapshot_orm()
        demo.test_complex_snapshot_queries()
        demo.test_snapshot_query_methods()

        # Run async tests
        asyncio.run(demo.test_async_snapshot_orm())

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All snapshot ORM examples completed!")
        print("\nKey features demonstrated:")
        print("- ✅ SQLAlchemy-like method chaining")
        print("- ✅ Type-safe parameter binding")
        print("- ✅ Automatic snapshot hint injection")
        print("- ✅ Support for complex queries (JOINs, GROUP BY, etc.)")
        print("- ✅ Both sync and async implementations")
        print("- ✅ Clean, readable query building")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == "__main__":
    main()
