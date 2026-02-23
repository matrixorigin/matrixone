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
Example 32: Branch Operations - Git-Style Version Control for Data

This example demonstrates Git-style branch operations with real-world scenarios:
1. Basic API - Simple string-based operations
2. ORM Model Support - SQLAlchemy model integration
3. Development Workflow - Create dev branch, test, merge
4. Safe Data Migration - Snapshot-based testing before applying
5. A/B Testing - Compare different data transformations
6. Multi-Team Development - Independent branches with conflict resolution
7. Database-Level Branching - Schema changes across entire databases
8. Async Operations - Non-blocking branch operations

This example demonstrates the complete branch functionality of MatrixOne.
"""

import asyncio
import time
from matrixone import Client, AsyncClient
from matrixone.branch import DiffOutput, MergeConflictStrategy
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, Numeric
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params, print_config

Base = declarative_base()


class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(Numeric(10, 2))


class BranchOperationsDemo:
    """Demonstrates branch operations with comprehensive testing."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
        }

    def test_basic_api(self):
        """Test 1: Basic API - Simple string-based operations"""
        print("\n=== Test 1: Basic API ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Create and delete
            client.execute("DROP TABLE IF EXISTS users")
            client.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))")
            client.execute("INSERT INTO users VALUES (1, 'Alice')")

            client.branch.create('users_branch', 'users')
            client.branch.delete('users_branch')
            client.execute("DROP TABLE users")

            self.logger.info("✅ Basic API test passed")
            self.results['tests_passed'] += 1
            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Basic API test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'basic_api', 'error': str(e)})

    def test_orm_model_support(self):
        """Test 2: ORM Model Support - SQLAlchemy model integration"""
        print("\n=== Test 2: ORM Model Support ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            client.execute("DROP TABLE IF EXISTS products")
            client.execute("DROP TABLE IF EXISTS products_branch")

            client.create_table(Product)
            client.insert(Product, {'id': 1, 'name': 'Laptop', 'price': 999.99})

            client.branch.create('products_branch', Product)
            client.branch.delete('products_branch')
            client.drop_table(Product)

            self.logger.info("✅ ORM model support test passed")
            self.results['tests_passed'] += 1
            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ ORM model support test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'orm_model_support', 'error': str(e)})

    def test_dev_workflow(self):
        """Test 3: Development Workflow - Create dev branch, test, merge"""
        print("\n=== Test 3: Development Workflow ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Cleanup first
            client.execute("DROP TABLE IF EXISTS users_dev")
            client.execute("DROP TABLE IF EXISTS users_prod")

            # Setup production
            client.execute("""
                CREATE TABLE users_prod (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    status VARCHAR(20)
                )
            """)
            client.execute("INSERT INTO users_prod VALUES (1, 'Alice', 'alice@example.com', 'active')")
            client.execute("INSERT INTO users_prod VALUES (2, 'Bob', 'bob@example.com', 'active')")

            # Create dev branch
            client.branch.create('users_dev', 'users_prod')

            # Make changes in dev (data only, not schema)
            client.execute("INSERT INTO users_dev VALUES (3, 'Charlie', 'charlie@example.com', 'active')")

            # Compare (get count of differences)
            diffs = client.branch.diff('users_dev', 'users_prod', output=DiffOutput.COUNT)

            # Merge
            client.branch.merge('users_dev', 'users_prod', on_conflict=MergeConflictStrategy.ACCEPT)
            result = client.execute("SELECT COUNT(*) FROM users_prod")
            count = result.rows[0][0]

            client.execute("DROP TABLE users_dev")
            client.execute("DROP TABLE users_prod")

            self.logger.info(f"✅ Development workflow test passed (merged {count} users)")
            self.results['tests_passed'] += 1
            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Development workflow test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'dev_workflow', 'error': str(e)})

    def test_safe_migration(self):
        """Test 4: Safe Data Migration - Snapshot-based testing before applying"""
        print("\n=== Test 4: Safe Data Migration ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Setup
            client.execute("DROP TABLE IF EXISTS orders")
            client.execute("""
                CREATE TABLE orders (
                    id INT PRIMARY KEY,
                    customer_id INT,
                    amount DECIMAL(10,2),
                    status VARCHAR(20)
                )
            """)
            client.execute("INSERT INTO orders VALUES (1, 101, 100.00, 'pending')")
            client.execute("INSERT INTO orders VALUES (2, 102, 200.00, 'completed')")

            # Create snapshot
            snap_name = f"orders_backup_{int(time.time())}"
            client.execute(f"CREATE SNAPSHOT {snap_name} FOR TABLE test orders")

            # Create branch from snapshot
            client.branch.create('orders_test', 'orders', snapshot=snap_name)

            # Test migration
            client.execute("UPDATE orders_test SET status = 'processing' WHERE status = 'pending'")
            result = client.execute("SELECT COUNT(*) FROM orders_test WHERE status = 'processing'")
            migrated = result.rows[0][0]

            # Apply to production
            client.execute("UPDATE orders SET status = 'processing' WHERE status = 'pending'")

            client.execute("DROP TABLE orders_test")
            client.execute("DROP TABLE orders")
            client.execute(f"DROP SNAPSHOT {snap_name}")

            self.logger.info(f"✅ Safe migration test passed ({migrated} orders updated)")
            self.results['tests_passed'] += 1
            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Safe migration test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'safe_migration', 'error': str(e)})

    def test_ab_testing(self):
        """Test 5: A/B Testing - Compare different data transformations"""
        print("\n=== Test 5: A/B Testing ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Setup
            client.execute("DROP TABLE IF EXISTS products")
            client.execute("""
                CREATE TABLE products (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    price DECIMAL(10,2)
                )
            """)
            client.execute("INSERT INTO products VALUES (1, 'Laptop', 1000.00)")
            client.execute("INSERT INTO products VALUES (2, 'Mouse', 50.00)")
            client.execute("INSERT INTO products VALUES (3, 'Desk', 300.00)")

            # Branch A: 10% discount
            client.branch.create('products_a', 'products')
            client.execute("UPDATE products_a SET price = price * 0.9")
            result = client.execute("SELECT AVG(price) FROM products_a")
            avg_a = float(result.rows[0][0])

            # Branch B: 15% discount
            client.branch.create('products_b', 'products')
            client.execute("UPDATE products_b SET price = price * 0.85")
            result = client.execute("SELECT AVG(price) FROM products_b")
            avg_b = float(result.rows[0][0])

            # Merge winner
            client.branch.merge('products_b', 'products', on_conflict=MergeConflictStrategy.ACCEPT)

            client.execute("DROP TABLE products_a")
            client.execute("DROP TABLE products_b")
            client.execute("DROP TABLE products")

            self.logger.info(f"✅ A/B testing test passed (A: ${avg_a:.2f}, B: ${avg_b:.2f})")
            self.results['tests_passed'] += 1
            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ A/B testing test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'ab_testing', 'error': str(e)})

    def test_multi_team(self):
        """Test 6: Multi-Team Development - Independent branches with conflict resolution"""
        print("\n=== Test 6: Multi-Team Development ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Setup
            client.execute("DROP TABLE IF EXISTS inventory")
            client.execute("""
                CREATE TABLE inventory (
                    id INT PRIMARY KEY,
                    item VARCHAR(100),
                    quantity INT,
                    warehouse VARCHAR(50)
                )
            """)
            client.execute("INSERT INTO inventory VALUES (1, 'Widget A', 100, 'Warehouse 1')")
            client.execute("INSERT INTO inventory VALUES (2, 'Widget B', 50, 'Warehouse 2')")

            # Team A: Update warehouse
            client.branch.create('inventory_team_a', 'inventory')
            client.execute("UPDATE inventory_team_a SET warehouse = 'Warehouse 1' WHERE id = 1")

            # Team B: Update quantities
            client.branch.create('inventory_team_b', 'inventory')
            client.execute("UPDATE inventory_team_b SET quantity = 150 WHERE id = 1")

            # Merge both
            client.branch.merge('inventory_team_a', 'inventory', on_conflict=MergeConflictStrategy.ACCEPT)
            client.branch.merge('inventory_team_b', 'inventory', on_conflict=MergeConflictStrategy.ACCEPT)

            client.execute("DROP TABLE inventory_team_a")
            client.execute("DROP TABLE inventory_team_b")
            client.execute("DROP TABLE inventory")

            self.logger.info("✅ Multi-team development test passed")
            self.results['tests_passed'] += 1
            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Multi-team development test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'multi_team', 'error': str(e)})

    def test_database_branching(self):
        """Test 7: Database-Level Branching - Schema changes across entire databases"""
        print("\n=== Test 7: Database-Level Branching ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            prod_db = f"shop_prod_{int(time.time())}"
            dev_db = f"shop_dev_{int(time.time())}"

            # Setup production database
            client.execute(f"DROP DATABASE IF EXISTS {prod_db}")
            client.execute(f"CREATE DATABASE {prod_db}")
            client.execute(f"USE {prod_db}")
            client.execute("""
                CREATE TABLE customers (
                    id INT PRIMARY KEY,
                    name VARCHAR(100)
                )
            """)
            client.execute("""
                CREATE TABLE orders (
                    id INT PRIMARY KEY,
                    customer_id INT,
                    amount DECIMAL(10,2)
                )
            """)
            client.execute("INSERT INTO customers VALUES (1, 'Alice')")
            client.execute("INSERT INTO orders VALUES (1, 1, 100.00)")

            # Create dev database branch
            client.branch.create(dev_db, prod_db, database=True)

            # Test schema changes
            client.execute(f"USE {dev_db}")
            client.execute("ALTER TABLE customers ADD COLUMN email VARCHAR(100)")
            client.execute("ALTER TABLE orders ADD COLUMN status VARCHAR(20)")
            result = client.execute("DESCRIBE customers")
            columns = len(result.rows)

            # Cleanup
            client.execute(f"USE test")
            client.execute(f"DROP DATABASE {prod_db}")
            client.execute(f"DROP DATABASE {dev_db}")

            self.logger.info(f"✅ Database-level branching test passed ({columns} columns)")
            self.results['tests_passed'] += 1
            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Database-level branching test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'database_branching', 'error': str(e)})

    async def test_async_operations(self):
        """Test 8: Async Operations - Non-blocking branch operations"""
        print("\n=== Test 8: Async Operations ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            async_client = AsyncClient(logger=self.logger, sql_log_mode="full")
            await async_client.connect(host=host, port=port, user=user, password=password, database=database)

            # Create table
            await async_client.execute("DROP TABLE IF EXISTS async_test")
            await async_client.execute("CREATE TABLE async_test (id INT PRIMARY KEY)")
            await async_client.execute("INSERT INTO async_test VALUES (1)")

            # Async branch operations
            await async_client.branch.create('async_test_branch', 'async_test')
            diffs = await async_client.branch.diff('async_test_branch', 'async_test', output=DiffOutput.COUNT)
            await async_client.branch.delete('async_test_branch')

            await async_client.execute("DROP TABLE async_test")
            await async_client.disconnect()

            self.logger.info("✅ Async operations test passed")
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Async operations test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'async_operations', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("Branch Operations Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        if unexpected_results:
            print(f"\nUnexpected Results ({len(unexpected_results)}):")
            for i, result in enumerate(unexpected_results, 1):
                print(f"  {i}. Test: {result['test']}")
                print(f"     Error: {result['error']}")
        else:
            print("\n✓ No unexpected results - all tests behaved as expected")

        return self.results

    def run_all_tests(self):
        """Run all tests"""
        print("\n" + "=" * 80)
        print("MatrixOne Branch Operations - Complete Examples")
        print("=" * 80)
        print_config()

        self.test_basic_api()
        self.test_orm_model_support()
        self.test_dev_workflow()
        self.test_safe_migration()
        self.test_ab_testing()
        self.test_multi_team()
        self.test_database_branching()
        asyncio.run(self.test_async_operations())

        results = self.generate_summary_report()

        print("\n" + "=" * 80)
        print("Key Features Demonstrated:")
        print("=" * 80)
        print("1. ✓ Simple API: create(), delete(), diff(), merge()")
        print("2. ✓ ORM Model Support: Works with SQLAlchemy models")
        print("3. ✓ Development Workflow: Isolate, test, merge changes")
        print("4. ✓ Safe Migration: Snapshot-based testing")
        print("5. ✓ A/B Testing: Compare different data transformations")
        print("6. ✓ Multi-Team: Independent branches with conflict resolution")
        print("7. ✓ Database Branching: Schema changes across entire databases")
        print("8. ✓ Async Support: Non-blocking operations")

        return results


def main():
    """Main entry point"""
    demo = BranchOperationsDemo()
    demo.run_all_tests()


if __name__ == '__main__':
    main()
