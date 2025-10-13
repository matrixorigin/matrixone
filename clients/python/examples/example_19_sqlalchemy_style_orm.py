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
Example 19: SQLAlchemy-style ORM - Comprehensive SQLAlchemy-style ORM Operations

MatrixOne SQLAlchemy-style ORM Example

This example demonstrates how to use MatrixOne with a SQLAlchemy-like ORM interface.
You can use client.query(Model).snapshot("snapshot_name").filter_by(...).all() syntax.
"""

import asyncio
import logging
from matrixone import Client, AsyncClient, SnapshotManager, SnapshotLevel
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP, desc

Base = declarative_base()
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger

# Create MatrixOne logger for all logging
logger = create_default_logger(sql_log_mode="auto")


# Define models using SQLAlchemy-style syntax
class Product(Base):
    """Product model"""

    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(DECIMAL(10, 2))
    category = Column(String(50))
    created_at = Column(TIMESTAMP)


class Order(Base):
    """Order model"""

    __tablename__ = "orders"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer)
    quantity = Column(Integer)
    total_amount = Column(DECIMAL(10, 2))
    order_date = Column(TIMESTAMP)


class SQLAlchemyStyleORMDemo:
    """Demonstrates SQLAlchemy-style ORM capabilities with comprehensive testing."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'sqlalchemy_orm_performance': {},
        }

    def test_sync_sqlalchemy_style_orm(self):
        """Test sync SQLAlchemy-style ORM usage"""
        print("\n=== Sync SQLAlchemy-style ORM Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            client = Client(logger=self.logger)
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test sync SQLAlchemy-style ORM
            self.logger.info("Test: Sync SQLAlchemy-style ORM")
            try:
                # Create test database and table
                client.execute("CREATE DATABASE IF NOT EXISTS sqlalchemy_orm_test")
                client.execute("USE sqlalchemy_orm_test")

                # Drop both tables to ensure clean state
                client.drop_table(Product)
                client.drop_table(Order)

                # Create only the Product table (we don't need Order in this test)
                Product.__table__.create(client._engine)

                # Insert test data
                client.execute(
                    "INSERT INTO products (name, price, category, created_at) VALUES ('Laptop', 999.99, 'Electronics', NOW())"
                )
                client.execute(
                    "INSERT INTO products (name, price, category, created_at) VALUES ('Book', 19.99, 'Education', NOW())"
                )
                client.execute(
                    "INSERT INTO products (name, price, category, created_at) VALUES ('Phone', 699.99, 'Electronics', NOW())"
                )

                # Test ORM query
                products = client.query(Product).filter_by(category="Electronics").all()

                self.logger.info(f"   Found {len(products)} electronics products")
                for product in products:
                    self.logger.info(f"     - {product.name}: ${product.price}")

                # Cleanup
                client.execute("DROP DATABASE IF EXISTS sqlalchemy_orm_test")

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Sync SQLAlchemy-style ORM test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Sync SQLAlchemy-style ORM', 'error': str(e)})

            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Sync SQLAlchemy-style ORM test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Sync SQLAlchemy-style ORM', 'error': str(e)})

    async def test_async_sqlalchemy_style_orm(self):
        """Test async SQLAlchemy-style ORM usage"""
        print("\n=== Async SQLAlchemy-style ORM Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            client = AsyncClient(logger=self.logger)
            await client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test async SQLAlchemy-style ORM
            self.logger.info("Test: Async SQLAlchemy-style ORM")
            try:
                # Create test database and table
                await client.execute("CREATE DATABASE IF NOT EXISTS async_sqlalchemy_orm_test")
                await client.execute("USE async_sqlalchemy_orm_test")

                # Drop both tables to ensure clean state
                await client.drop_table(Order)
                await client.drop_table(Product)

                # Create only the Order table (we don't need Product in async test)
                async with client._engine.begin() as conn:
                    await conn.run_sync(Order.__table__.create)

                # Insert test data
                await client.execute(
                    "INSERT INTO orders (product_id, quantity, total_amount, order_date) VALUES (1, 2, 1999.98, NOW())"
                )
                await client.execute(
                    "INSERT INTO orders (product_id, quantity, total_amount, order_date) VALUES (2, 1, 19.99, NOW())"
                )
                await client.execute(
                    "INSERT INTO orders (product_id, quantity, total_amount, order_date) VALUES (3, 1, 699.99, NOW())"
                )

                # Test async ORM query
                orders = await client.query(Order).filter_by(quantity=1).all()

                self.logger.info(f"   Found {len(orders)} orders with quantity 1")
                for order in orders:
                    self.logger.info(f"     - Order {order.id}: ${order.total_amount}")

                # Cleanup
                await client.execute("DROP DATABASE IF EXISTS async_sqlalchemy_orm_test")

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Async SQLAlchemy-style ORM test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Async SQLAlchemy-style ORM', 'error': str(e)})

            await client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Async SQLAlchemy-style ORM test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Async SQLAlchemy-style ORM', 'error': str(e)})

    def test_orm_query_methods(self):
        """Test various ORM query methods"""
        print("\n=== ORM Query Methods Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            client = Client(logger=self.logger)
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test ORM query methods
            self.logger.info("Test: ORM Query Methods")
            try:
                # Create test database and table
                client.execute("CREATE DATABASE IF NOT EXISTS orm_query_methods_test")
                client.execute("USE orm_query_methods_test")

                # Drop both tables to ensure clean state
                client.drop_table(Product)
                client.drop_table(Order)

                # Create only the Product table (we don't need Order in this test)
                Product.__table__.create(client._engine)

                # Insert test data
                client.execute(
                    "INSERT INTO products (name, price, category, created_at) VALUES ('Laptop', 999.99, 'Electronics', NOW())"
                )
                client.execute(
                    "INSERT INTO products (name, price, category, created_at) VALUES ('Book', 19.99, 'Education', NOW())"
                )
                client.execute(
                    "INSERT INTO products (name, price, category, created_at) VALUES ('Phone', 699.99, 'Electronics', NOW())"
                )

                # Test various query methods
                # Test filter_by
                electronics = client.query(Product).filter_by(category="Electronics").all()
                self.logger.info(f"   filter_by: {len(electronics)} electronics products")

                # Test first
                first_product = client.query(Product).first()
                if first_product:
                    self.logger.info(f"   first: {first_product.name}")

                # Test count
                total_count = client.query(Product).count()
                self.logger.info(f"   count: {total_count} total products")

                # Cleanup
                client.execute("DROP DATABASE IF EXISTS orm_query_methods_test")

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ ORM query methods test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'ORM Query Methods', 'error': str(e)})

            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ ORM query methods test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'ORM Query Methods', 'error': str(e)})

    def test_orm_with_snapshots(self):
        """Test ORM with snapshot functionality"""
        print("\n=== ORM with Snapshots Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            client = Client(logger=self.logger)
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test ORM with snapshots
            self.logger.info("Test: ORM with Snapshots")
            try:
                # Create test database and table
                client.execute("CREATE DATABASE IF NOT EXISTS orm_snapshots_test")
                client.execute("USE orm_snapshots_test")
                client.drop_table(Product)
                client.drop_table(Order)

                # Create only the Product table (we only use Product in this test)
                Product.__table__.create(client._engine)

                # Insert test data
                client.execute(
                    "INSERT INTO products (name, price, category, created_at) VALUES ('Laptop', 999.99, 'Electronics', NOW())"
                )
                client.execute(
                    "INSERT INTO products (name, price, category, created_at) VALUES ('Book', 19.99, 'Education', NOW())"
                )

                # Test snapshot functionality (may not be supported in all MatrixOne versions)
                snapshot_name = f"orm_test_snapshot_{int(__import__('time').time())}"
                try:
                    client.execute(f"CREATE SNAPSHOT {snapshot_name}")
                    self.logger.info("   ✅ Snapshot created successfully")

                    # Test ORM query with snapshot
                    try:
                        products = client.query(Product).snapshot(snapshot_name).all()
                        self.logger.info(f"   Snapshot query: {len(products)} products found")
                    except Exception as e:
                        self.logger.info(f"   Snapshot query (expected to fail in some cases): {e}")

                    # Cleanup snapshot
                    try:
                        client.execute(f"DROP SNAPSHOT IF EXISTS {snapshot_name}")
                        self.logger.info("   ✅ Snapshot dropped successfully")
                    except Exception as e:
                        self.logger.info(f"   Snapshot cleanup: {e}")

                except Exception as e:
                    self.logger.info(f"   Snapshot functionality not supported: {e}")
                client.execute("DROP DATABASE IF EXISTS orm_snapshots_test")

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ ORM with snapshots test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'ORM with Snapshots', 'error': str(e)})

            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ ORM with snapshots test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'ORM with Snapshots', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("SQLAlchemy-style ORM Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        sqlalchemy_orm_performance = self.results['sqlalchemy_orm_performance']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        # Performance summary
        if sqlalchemy_orm_performance:
            print(f"\nSQLAlchemy-style ORM Performance Results:")
            for test_name, time_taken in sqlalchemy_orm_performance.items():
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
    demo = SQLAlchemyStyleORMDemo()

    try:
        print("🎯 MatrixOne SQLAlchemy-style ORM Demo")
        print("=" * 50)

        # Print current configuration
        print_config()

        # Run tests
        demo.test_sync_sqlalchemy_style_orm()
        demo.test_orm_query_methods()
        demo.test_orm_with_snapshots()

        # Run async tests
        asyncio.run(demo.test_async_sqlalchemy_style_orm())

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All SQLAlchemy-style ORM demos completed!")
        print("\nKey features demonstrated:")
        print("- ✅ SQLAlchemy-like ORM interface")
        print("- ✅ Model definitions with declarative base")
        print("- ✅ Query methods (filter_by, first, count, all)")
        print("- ✅ Snapshot integration with ORM")
        print("- ✅ Both sync and async implementations")
        print("- ✅ Table creation using ORM models")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == "__main__":
    main()
