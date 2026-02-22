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
Example: Branch Operations - Git-Style Version Control for Data

This example demonstrates Git-style branch operations with Pythonic API:
1. Simple method names: create(), delete(), diff(), merge()
2. Support for strings, "db.table", and ORM models
3. Snapshot-based branching
4. Conflict resolution strategies
5. Async operations

This example shows the complete branch capabilities of MatrixOne.
"""

import asyncio
import time
from matrixone import Client, AsyncClient
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, Numeric
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params

Base = declarative_base()

class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(Numeric(10, 2))

class ProductBranch(Base):
    __tablename__ = 'products_branch'
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

    def test_simple_string_api(self):
        """Test simple string-based API"""
        print("\n=== Simple String API Tests ===")

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test 1: Create and delete
            self.logger.info("Test 1: Create and Delete")
            self.results['tests_run'] += 1
            try:
                client.execute("DROP TABLE IF EXISTS users")
                client.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))")
                client.execute("INSERT INTO users VALUES (1, 'Alice')")

                # Simple create
                client.branch.create('users_branch', 'users')
                
                # Simple delete
                client.branch.delete('users_branch')
                
                self.logger.info("✅ Simple create/delete successful")
                self.results['tests_passed'] += 1

                client.execute("DROP TABLE users")

            except Exception as e:
                self.logger.error(f"❌ Simple API failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Simple API', 'error': str(e)})

            # Test 2: Diff and merge
            self.logger.info("Test 2: Diff and Merge")
            self.results['tests_run'] += 1
            try:
                client.execute("DROP TABLE IF EXISTS inventory")
                client.execute("CREATE TABLE inventory (id INT PRIMARY KEY, qty INT)")
                client.execute("INSERT INTO inventory VALUES (1, 100)")

                client.branch.create('inventory_branch', 'inventory')
                client.execute("UPDATE inventory_branch SET qty = 150 WHERE id = 1")

                # Simple diff
                diffs = client.branch.diff('inventory_branch', 'inventory')
                
                # Simple merge with on_conflict parameter
                client.branch.merge('inventory_branch', 'inventory', on_conflict='accept')

                result = client.execute("SELECT qty FROM inventory WHERE id = 1")
                if result.rows[0][0] == 150:
                    self.logger.info("✅ Diff and merge successful")
                    self.results['tests_passed'] += 1
                else:
                    self.logger.error("❌ Merge failed")
                    self.results['tests_failed'] += 1

                client.execute("DROP TABLE inventory_branch")
                client.execute("DROP TABLE inventory")

            except Exception as e:
                self.logger.error(f"❌ Diff/merge failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Diff/Merge', 'error': str(e)})

            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Simple string API test failed: {e}")
            self.results['tests_failed'] += 1

    def test_orm_model_api(self):
        """Test ORM model-based API"""
        print("\n=== ORM Model API Tests ===")

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test: ORM models
            self.logger.info("Test: ORM Model Support")
            self.results['tests_run'] += 1
            try:
                # Create using ORM
                client.create_table(Product)
                client.insert(Product, {'id': 1, 'name': 'Laptop', 'price': 999.99})

                # Branch using ORM models
                client.branch.create(ProductBranch, Product)
                
                # Diff using ORM models
                diffs = client.branch.diff(ProductBranch, Product)
                
                # Delete using ORM model
                client.branch.delete(ProductBranch)

                self.logger.info("✅ ORM model API successful")
                self.results['tests_passed'] += 1

                client.drop_table(Product)

            except Exception as e:
                self.logger.error(f"❌ ORM API failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'ORM API', 'error': str(e)})

            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ ORM model API test failed: {e}")
            self.results['tests_failed'] += 1

    def test_database_branch(self):
        """Test database branch operations"""
        print("\n=== Database Branch Tests ===")

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test: Database branch
            self.logger.info("Test: Database Branch")
            self.results['tests_run'] += 1
            try:
                src_db = f"shop_{int(time.time())}"
                dev_db = f"shop_dev_{int(time.time())}"

                client.execute(f"DROP DATABASE IF EXISTS {src_db}")
                client.execute(f"CREATE DATABASE {src_db}")
                client.execute(f"USE {src_db}")
                client.execute("CREATE TABLE orders (id INT PRIMARY KEY)")

                # Create database branch
                client.branch.create(dev_db, src_db, database=True)
                
                # Delete database branch
                client.branch.delete(dev_db, database=True)

                self.logger.info("✅ Database branch successful")
                self.results['tests_passed'] += 1

                client.execute(f"DROP DATABASE {src_db}")
                client.execute("USE test")

            except Exception as e:
                self.logger.error(f"❌ Database branch failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Database Branch', 'error': str(e)})

            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Database branch test failed: {e}")
            self.results['tests_failed'] += 1

    def test_snapshot_branch(self):
        """Test snapshot-based branching"""
        print("\n=== Snapshot Branch Tests ===")

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test: Snapshot branch
            self.logger.info("Test: Snapshot-based Branch")
            self.results['tests_run'] += 1
            try:
                client.execute("DROP TABLE IF EXISTS accounts")
                client.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance DECIMAL(10,2))")
                client.execute("INSERT INTO accounts VALUES (1, 1000.00)")

                snap_name = f"accounts_snap_{int(time.time())}"
                client.execute(f"CREATE SNAPSHOT {snap_name} FOR TABLE test accounts")

                client.execute("UPDATE accounts SET balance = 500.00 WHERE id = 1")

                # Create branch from snapshot
                client.branch.create('accounts_branch', 'accounts', snapshot=snap_name)

                result = client.execute("SELECT balance FROM accounts_branch WHERE id = 1")
                balance = float(result.rows[0][0])

                if balance == 1000.00:
                    self.logger.info("✅ Snapshot branch successful")
                    self.results['tests_passed'] += 1
                else:
                    self.logger.error(f"❌ Wrong balance: {balance}")
                    self.results['tests_failed'] += 1

                client.execute("DROP TABLE accounts_branch")
                client.execute("DROP TABLE accounts")
                client.execute(f"DROP SNAPSHOT {snap_name}")

            except Exception as e:
                self.logger.error(f"❌ Snapshot branch failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Snapshot Branch', 'error': str(e)})

            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Snapshot branch test failed: {e}")
            self.results['tests_failed'] += 1

    def test_async_operations(self):
        """Test async branch operations"""
        print("\n=== Async Operations Tests ===")

        async def run_async_tests():
            try:
                host, port, user, password, database = get_connection_params()
                client = AsyncClient(logger=self.logger, sql_log_mode="full")
                await client.connect(host=host, port=port, user=user, password=password, database=database)

                # Test: Async operations
                self.logger.info("Test: Async Operations")
                self.results['tests_run'] += 1
                try:
                    await client.execute("DROP TABLE IF EXISTS async_test")
                    await client.execute("CREATE TABLE async_test (id INT PRIMARY KEY)")
                    await client.execute("INSERT INTO async_test VALUES (1)")

                    # Async create
                    await client.branch.create('async_test_branch', 'async_test')
                    
                    # Async diff
                    diffs = await client.branch.diff('async_test_branch', 'async_test')
                    
                    # Async delete
                    await client.branch.delete('async_test_branch')

                    self.logger.info("✅ Async operations successful")
                    self.results['tests_passed'] += 1

                    await client.execute("DROP TABLE async_test")

                except Exception as e:
                    self.logger.error(f"❌ Async operations failed: {e}")
                    self.results['tests_failed'] += 1
                    self.results['unexpected_results'].append({'test': 'Async Operations', 'error': str(e)})

                await client.disconnect()

            except Exception as e:
                self.logger.error(f"❌ Async operations test failed: {e}")
                self.results['tests_failed'] += 1

        asyncio.run(run_async_tests())

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


def main():
    """Main demo function"""
    demo = BranchOperationsDemo()

    try:
        print("🚀 MatrixOne Branch Operations - Pythonic API Examples")
        print("=" * 60)

        # Run tests
        demo.test_simple_string_api()
        demo.test_orm_model_api()
        demo.test_database_branch()
        demo.test_snapshot_branch()
        demo.test_async_operations()

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All branch operation examples completed!")
        print("\nKey Features:")
        print("- ✅ Simple API: create(), delete(), diff(), merge()")
        print("- ✅ String support: 'table', 'db.table'")
        print("- ✅ ORM model support: TableModel")
        print("- ✅ Snapshot-based branching")
        print("- ✅ Conflict resolution: on_conflict='skip'|'accept'")
        print("- ✅ Async operations")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == '__main__':
    main()
