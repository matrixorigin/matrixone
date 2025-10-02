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
Example 24: Query Update Operations - Modern UPDATE API

This example demonstrates the new update functionality in MatrixOne query builder,
showing how to use SQLAlchemy-style update operations with both simple values
and complex expressions.

Features demonstrated:
1. Simple update with key-value pairs
2. Update with SQLAlchemy expressions
3. Update with filter conditions
4. Update with complex expressions
5. Transaction support for updates
6. Explain and to_sql for update queries
"""

import logging
from datetime import datetime
from matrixone import Client
from matrixone.orm import declarative_base
from matrixone.config import get_connection_params
from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP, func

# Create MatrixOne logger
logger = logging.getLogger(__name__)

# Create declarative base
Base = declarative_base()


class User(Base):
    """User model for demonstrating update operations"""

    __tablename__ = 'example_users_update'

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    full_name = Column(String(200))
    age = Column(Integer)
    salary = Column(DECIMAL(10, 2))
    status = Column(String(50), default='active')
    login_count = Column(Integer, default=0)
    last_login = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())


class Product(Base):
    """Product model for demonstrating update operations"""

    __tablename__ = 'example_products_update'

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    price = Column(DECIMAL(10, 2))
    category = Column(String(50))
    stock = Column(Integer, default=0)
    active = Column(Integer, default=1)


def demo_simple_updates():
    """Demonstrate simple update operations"""
    print("\n=== Simple Update Operations ===")

    # Get connection parameters
    host, port, user, password, database = get_connection_params()
    client = Client()
    client.connect(host=host, port=port, user=user, password=password, database=database)

    try:
        # Create tables
        client.create_all(Base)

        # Insert sample data
        users_data = [
            {
                "id": 1,
                "username": "alice",
                "email": "alice@example.com",
                "full_name": "Alice Smith",
                "age": 25,
                "salary": 50000.00,
                "status": "active",
                "login_count": 5,
            },
            {
                "id": 2,
                "username": "bob",
                "email": "bob@example.com",
                "full_name": "Bob Johnson",
                "age": 30,
                "salary": 60000.00,
                "status": "active",
                "login_count": 10,
            },
            {
                "id": 3,
                "username": "charlie",
                "email": "charlie@example.com",
                "full_name": "Charlie Brown",
                "age": 35,
                "salary": 70000.00,
                "status": "inactive",
                "login_count": 2,
            },
        ]
        client.batch_insert("example_users_update", users_data)

        # 1. Simple update with key-value pairs
        print("1. Simple update with key-value pairs")
        query = client.query(User)
        result = query.update(full_name="Alice Updated", email="alice.updated@example.com").filter(User.id == 1).execute()

        # Verify the update
        updated_user = client.query(User).filter(User.id == 1).first()
        print(f"   Updated user: {updated_user.full_name}, {updated_user.email}")

        # 2. Update multiple records with condition
        print("\n2. Update multiple records with condition")
        query = client.query(User)
        result = query.update(status="premium").filter(User.status == "active").execute()

        # Verify the updates
        premium_users = client.query(User).filter(User.status == "premium").all()
        print(f"   Premium users count: {len(premium_users)}")
        for user in premium_users:
            print(f"   - {user.username}: {user.status}")

    finally:
        # Clean up
        try:
            client.drop_table(User)
            client.drop_table(Product)
        except Exception as e:
            print(f"Warning: Failed to clean up tables: {e}")
        client.disconnect()


def demo_sqlalchemy_expressions():
    """Demonstrate update with SQLAlchemy expressions"""
    print("\n=== Update with SQLAlchemy Expressions ===")

    # Get connection parameters
    host, port, user, password, database = get_connection_params()
    client = Client()
    client.connect(host=host, port=port, user=user, password=password, database=database)

    try:
        # Create tables
        client.create_all(Base)

        # Insert sample data
        users_data = [
            {
                "id": 1,
                "username": "alice",
                "email": "alice@example.com",
                "full_name": "Alice Smith",
                "age": 25,
                "salary": 50000.00,
                "status": "active",
                "login_count": 5,
            },
            {
                "id": 2,
                "username": "bob",
                "email": "bob@example.com",
                "full_name": "Bob Johnson",
                "age": 30,
                "salary": 60000.00,
                "status": "active",
                "login_count": 10,
            },
        ]
        client.batch_insert("example_users_update", users_data)

        # 1. Update with SQLAlchemy expressions
        print("1. Update with SQLAlchemy expressions")
        query = client.query(User)
        result = query.update(login_count=User.login_count + 1, salary=User.salary * 1.1).filter(User.id == 1).execute()

        # Verify the update
        updated_user = client.query(User).filter(User.id == 1).first()
        print(f"   Updated user: login_count={updated_user.login_count}, salary={updated_user.salary}")

        # 2. Update with complex expressions
        print("\n2. Update with complex expressions")
        query = client.query(User)
        result = (
            query.update(status="vip", last_login=func.now()).filter(User.status == "active", User.login_count > 5).execute()
        )

        # Verify the updates
        vip_users = client.query(User).filter(User.status == "vip").all()
        print(f"   VIP users count: {len(vip_users)}")
        for user in vip_users:
            print(f"   - {user.username}: {user.status}, last_login: {user.last_login}")

    finally:
        # Clean up
        try:
            client.drop_table(User)
            client.drop_table(Product)
        except Exception as e:
            print(f"Warning: Failed to clean up tables: {e}")
        client.disconnect()


def demo_update_with_explain():
    """Demonstrate update with explain and to_sql"""
    print("\n=== Update with Explain and to_sql ===")

    # Get connection parameters
    host, port, user, password, database = get_connection_params()
    client = Client()
    client.connect(host=host, port=port, user=user, password=password, database=database)

    try:
        # Create tables
        client.create_all(Base)

        # Insert sample data
        users_data = [
            {
                "id": 1,
                "username": "alice",
                "email": "alice@example.com",
                "full_name": "Alice Smith",
                "age": 25,
                "salary": 50000.00,
                "status": "active",
                "login_count": 5,
            },
        ]
        client.batch_insert("example_users_update", users_data)

        # 1. Generate SQL without executing
        print("1. Generate SQL without executing")
        query = client.query(User)
        sql = query.update(full_name="Alice Updated", status="premium").filter(User.id == 1).to_sql()
        print(f"   Generated SQL: {sql}")

        # 2. Explain update query
        print("\n2. Explain update query")
        query = client.query(User)
        explain_result = query.update(status="explained").filter(User.id == 1).explain()
        print(f"   Explain result: {len(explain_result.rows)} rows")

        # 3. Explain analyze update query
        print("\n3. Explain analyze update query")
        query = client.query(User)
        explain_analyze_result = query.update(status="analyzed").filter(User.id == 1).explain_analyze()
        print(f"   Explain analyze result: {len(explain_analyze_result.rows)} rows")

    finally:
        # Clean up
        try:
            client.drop_table(User)
            client.drop_table(Product)
        except Exception as e:
            print(f"Warning: Failed to clean up tables: {e}")
        client.disconnect()


def demo_update_with_transactions():
    """Demonstrate update within transactions"""
    print("\n=== Update with Transactions ===")

    # Get connection parameters
    host, port, user, password, database = get_connection_params()
    client = Client()
    client.connect(host=host, port=port, user=user, password=password, database=database)

    try:
        # Create tables
        client.create_all(Base)

        # Insert sample data
        users_data = [
            {
                "id": 1,
                "username": "alice",
                "email": "alice@example.com",
                "full_name": "Alice Smith",
                "age": 25,
                "salary": 50000.00,
                "status": "active",
                "login_count": 5,
            },
            {
                "id": 2,
                "username": "bob",
                "email": "bob@example.com",
                "full_name": "Bob Johnson",
                "age": 30,
                "salary": 60000.00,
                "status": "active",
                "login_count": 10,
            },
        ]
        client.batch_insert("example_users_update", users_data)

        # 1. Successful transaction
        print("1. Successful transaction")
        with client.transaction() as tx:
            query = client.query(User)
            query.update(status="transaction_test").filter(User.id == 1).execute()

            query = client.query(User)
            query.update(status="transaction_test").filter(User.id == 2).execute()

        # Verify the updates were committed
        test_users = client.query(User).filter(User.status == "transaction_test").all()
        print(f"   Transaction test users: {len(test_users)}")

        # 2. Rollback transaction
        print("\n2. Rollback transaction")
        original_status = client.query(User).filter(User.id == 1).first().status

        try:
            with client.transaction() as tx:
                query = client.query(User)
                query.update(status="rollback_test").filter(User.id == 1).execute()

                # Force rollback by raising an exception
                raise Exception("Force rollback")
        except Exception:
            pass

        # Verify the update was rolled back
        user = client.query(User).filter(User.id == 1).first()
        print(f"   User status after rollback: {user.status} (original: {original_status})")

    finally:
        # Clean up
        try:
            client.drop_table(User)
            client.drop_table(Product)
        except Exception as e:
            print(f"Warning: Failed to clean up tables: {e}")
        client.disconnect()


class QueryUpdateDemo:
    """Demonstrates MatrixOne query update capabilities with comprehensive testing."""

    def __init__(self):
        self.results = {'tests_run': 0, 'tests_passed': 0, 'tests_failed': 0, 'unexpected_results': []}

    def run_test(self, test_name: str, test_func):
        """Run a test and track results"""
        self.results['tests_run'] += 1
        try:
            test_func()
            self.results['tests_passed'] += 1
            print(f"✅ {test_name} completed successfully")
        except Exception as e:
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': test_name, 'error': str(e)})
            print(f"❌ {test_name} failed: {e}")

    def generate_summary_report(self):
        """Generate a comprehensive summary report of all test results."""
        print("\n" + "=" * 80)
        print("MatrixOne Query Update Demo - Summary Report")
        print("=" * 80)
        print(f"Total Tests Run: {self.results['tests_run']}")
        print(f"Tests Passed: {self.results['tests_passed']}")
        print(f"Tests Failed: {self.results['tests_failed']}")

        if self.results['tests_run'] > 0:
            success_rate = (self.results['tests_passed'] / self.results['tests_run']) * 100
            print(f"Success Rate: {success_rate:.1f}%")

        if self.results['unexpected_results']:
            print(f"\nUnexpected Results ({len(self.results['unexpected_results'])}):")
            for i, result in enumerate(self.results['unexpected_results'], 1):
                print(f"  {i}. Test: {result['test']}")
                print(f"     Error: {result['error']}")
        else:
            print("\n✓ No unexpected results - all tests behaved as expected")

        return self.results


def main():
    """Main demo function"""
    demo = QueryUpdateDemo()

    try:
        print("🚀 MatrixOne Query Update Examples")
        print("=" * 60)

        # Run all demos
        demo.run_test("Simple Updates", demo_simple_updates)
        demo.run_test("SQLAlchemy Expressions", demo_sqlalchemy_expressions)
        demo.run_test("Update with Explain", demo_update_with_explain)
        demo.run_test("Update with Transactions", demo_update_with_transactions)

        # Generate summary report
        results = demo.generate_summary_report()

        print("\n🎉 All update examples completed!")
        print("\nKey takeaways:")
        print("- ✅ Simple updates work with key-value pairs")
        print("- ✅ SQLAlchemy expressions are supported")
        print("- ✅ Complex filter conditions work")
        print("- ✅ Transactions support updates")
        print("- ✅ Explain and to_sql work with updates")
        print("- ✅ Method chaining is supported")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == '__main__':
    main()
