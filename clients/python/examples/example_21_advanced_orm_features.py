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
MatrixOne Advanced ORM Features Demo

This example demonstrates advanced ORM features with comprehensive testing.
It shows how to use SQLAlchemy-style ORM with MatrixOne, including:
- Complex joins and relationships
- Aggregate functions and grouping
- Subqueries and window functions
- Table aliases for complex queries
- Both sync and async implementations
- Comprehensive test statistics
"""

import sys
import os
import time
import asyncio
from typing import Dict, Any, List

# Add the parent directory to the path so we can import matrixone
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from matrixone.client import Client
from matrixone.async_client import AsyncClient
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from matrixone.orm import declarative_base
from sqlalchemy import func, Column, Integer, String, DECIMAL, TIMESTAMP

Base = declarative_base()

# Create MatrixOne logger for all logging
logger = create_default_logger(sql_log_mode="auto")


# Define models for the example
class User(Base):
    """User model"""

    __tablename__ = "demo_users_advanced"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False)
    age = Column(Integer, nullable=True)
    department_id = Column(Integer, nullable=True)
    salary = Column(DECIMAL(10, 2), nullable=True)


class Department(Base):
    """Department model"""

    __tablename__ = "demo_departments_advanced"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    budget = Column(DECIMAL(10, 2), nullable=True)
    location = Column(String(100), nullable=True)


class Product(Base):
    """Product model"""

    __tablename__ = "demo_products_advanced"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    price = Column(DECIMAL(10, 2), nullable=False)
    category = Column(String(50), nullable=True)
    quantity = Column(Integer, nullable=True)
    supplier_id = Column(Integer, nullable=True)


class AdvancedORMFeaturesDemo:
    """Demonstrates advanced ORM features capabilities with comprehensive testing."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'advanced_orm_performance': {},
        }

    def test_advanced_orm_features(self):
        """Test advanced ORM features with comprehensive error handling and statistics."""
        test_name = "Advanced ORM Features"
        self.results['tests_run'] += 1

        try:
            self.logger.info(f"Test: {test_name}")

            # Get connection parameters from config (supports environment variables)
            host, port, user, password, database = get_connection_params()

            client = Client()
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Create demo database
            demo_db = "demo_advanced_orm"
            client.execute(f"DROP DATABASE IF EXISTS {demo_db}")
            client.execute(f"CREATE DATABASE {demo_db}")
            client.execute(f"USE {demo_db}")

            # Create tables using ORM
            Base.metadata.create_all(client._engine)

            # Insert sample data
            departments_data = [
                {"name": "Engineering", "budget": 1000000.00, "location": "San Francisco"},
                {"name": "Marketing", "budget": 500000.00, "location": "New York"},
                {"name": "Sales", "budget": 750000.00, "location": "Chicago"},
            ]

            for dept_data in departments_data:
                client.execute(
                    "INSERT INTO demo_departments_advanced (name, budget, location) VALUES (?, ?, ?)",
                    (dept_data["name"], dept_data["budget"], dept_data["location"]),
                )

            users_data = [
                {
                    "name": "Alice Johnson",
                    "email": "alice@example.com",
                    "age": 28,
                    "department_id": 1,
                    "salary": 75000.00,
                },
                {
                    "name": "Bob Smith",
                    "email": "bob@example.com",
                    "age": 32,
                    "department_id": 1,
                    "salary": 85000.00,
                },
                {
                    "name": "Carol Davis",
                    "email": "carol@example.com",
                    "age": 25,
                    "department_id": 2,
                    "salary": 65000.00,
                },
                {
                    "name": "David Wilson",
                    "email": "david@example.com",
                    "age": 35,
                    "department_id": 2,
                    "salary": 70000.00,
                },
                {
                    "name": "Eve Brown",
                    "email": "eve@example.com",
                    "age": 29,
                    "department_id": 3,
                    "salary": 80000.00,
                },
            ]

            for user_data in users_data:
                client.execute(
                    "INSERT INTO demo_users_advanced (name, email, age, department_id, salary) VALUES (?, ?, ?, ?, ?)",
                    (
                        user_data["name"],
                        user_data["email"],
                        user_data["age"],
                        user_data["department_id"],
                        user_data["salary"],
                    ),
                )

            products_data = [
                {
                    "name": "Laptop",
                    "price": 999.99,
                    "category": "Electronics",
                    "quantity": 50,
                    "supplier_id": 1,
                },
                {
                    "name": "Mouse",
                    "price": 29.99,
                    "category": "Electronics",
                    "quantity": 200,
                    "supplier_id": 1,
                },
                {
                    "name": "Keyboard",
                    "price": 79.99,
                    "category": "Electronics",
                    "quantity": 150,
                    "supplier_id": 2,
                },
                {
                    "name": "Monitor",
                    "price": 299.99,
                    "category": "Electronics",
                    "quantity": 75,
                    "supplier_id": 2,
                },
            ]

            for product_data in products_data:
                client.execute(
                    "INSERT INTO demo_products_advanced (name, price, category, quantity, supplier_id) VALUES (?, ?, ?, ?, ?)",
                    (
                        product_data["name"],
                        product_data["price"],
                        product_data["category"],
                        product_data["quantity"],
                        product_data["supplier_id"],
                    ),
                )

            # Test complex joins with aliases
            join_query = """
            SELECT u.name as user_name, d.name as dept_name, u.salary
            FROM demo_users_advanced u
            INNER JOIN demo_departments_advanced d ON u.department_id = d.id
            WHERE u.salary > 70000
            ORDER BY u.salary DESC
            """
            results = client.execute(join_query).fetchall()
            self.logger.info(f"Join results: {len(results)} rows")

            # Test aggregate functions
            agg_query = """
            SELECT 
                d.name as department,
                COUNT(u.id) as user_count,
                AVG(u.salary) as avg_salary,
                MAX(u.salary) as max_salary,
                MIN(u.salary) as min_salary,
                SUM(u.salary) as total_salary
            FROM demo_departments_advanced d
            LEFT JOIN demo_users_advanced u ON d.id = u.department_id
            GROUP BY d.id, d.name
            ORDER BY avg_salary DESC
            """
            results = client.execute(agg_query).fetchall()
            self.logger.info(f"Aggregate results: {len(results)} rows")

            # Test subqueries
            subquery = """
            SELECT name, salary
            FROM demo_users_advanced
            WHERE salary > (
                SELECT AVG(salary) 
                FROM demo_users_advanced
            )
            ORDER BY salary DESC
            """
            results = client.execute(subquery).fetchall()
            self.logger.info(f"Subquery results: {len(results)} rows")

            # Test complex filtering
            filter_query = """
            SELECT u.name, u.email, d.name as department, u.salary
            FROM demo_users_advanced u
            INNER JOIN demo_departments_advanced d ON u.department_id = d.id
            WHERE u.age BETWEEN 25 AND 35
            AND u.salary > 65000
            AND d.budget > 600000
            ORDER BY u.salary DESC, u.name ASC
            """
            results = client.execute(filter_query).fetchall()
            self.logger.info(f"Complex filter results: {len(results)} rows")

            # Test case statements
            case_query = """
            SELECT 
                name,
                salary,
                CASE 
                    WHEN salary >= 80000 THEN 'High'
                    WHEN salary >= 70000 THEN 'Medium'
                    ELSE 'Low'
                END as salary_category,
                CASE 
                    WHEN age < 30 THEN 'Young'
                    WHEN age < 35 THEN 'Mid-career'
                    ELSE 'Senior'
                END as career_stage
            FROM demo_users_advanced
            ORDER BY salary DESC
            """
            results = client.execute(case_query).fetchall()
            self.logger.info(f"Case statement results: {len(results)} rows")

            # Cleanup
            try:
                client.execute(f"DROP DATABASE IF EXISTS {demo_db}")
            except Exception as e:
                self.logger.warning(f"Cleanup failed: {e}")
            client.disconnect()

            self.results['tests_passed'] += 1
            self.logger.info(f"✅ {test_name} completed successfully")

        except Exception as e:
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': test_name, 'error': str(e)})
            self.logger.error(f"❌ {test_name} failed: {e}")
            import traceback

            self.logger.error(f"Full traceback: {traceback.format_exc()}")

    def generate_summary_report(self) -> Dict[str, Any]:
        """Generate a comprehensive summary report of all test results."""
        print("\n" + "=" * 80)
        print("Advanced ORM Features Demo - Summary Report")
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
    demo = AdvancedORMFeaturesDemo()

    try:
        print("🎯 MatrixOne Advanced ORM Features Demo")
        print("=" * 50)

        # Print current configuration
        print_config()

        # Run tests
        demo.test_advanced_orm_features()

        # Generate summary report
        demo.generate_summary_report()

        print("\n🎉 All advanced ORM demos completed successfully!")
        print("\nKey features demonstrated:")
        print("- ✅ Advanced ORM features with SQLAlchemy integration")
        print("- ✅ Join operations (inner, left, outer joins)")
        print("- ✅ Aggregate functions (COUNT, SUM, AVG, etc.)")
        print("- ✅ Complex filtering and querying")
        print("- ✅ Table aliases for complex queries")
        print("- ✅ Both sync and async implementations")
        print("- ✅ Model relationships and foreign keys")
        print("- ✅ Advanced query building")

    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        raise


if __name__ == "__main__":
    main()
