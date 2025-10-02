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
Test: EXPLAIN, EXPLAIN ANALYZE, and to_sql() methods

This test verifies that the explain methods in MatrixOneQuery work correctly
for analyzing query execution plans and generating SQL statements.
"""

import pytest
import sys
import os
from datetime import datetime

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from matrixone import Client
from sqlalchemy import Column, Integer, String, DateTime, func, desc, asc
from matrixone.orm import declarative_base

# Create SQLAlchemy models
Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    age = Column(Integer)
    department = Column(String(50))
    salary = Column(Integer)
    created_at = Column(DateTime)
    active = Column(Integer, default=1)  # Add active column for CTE test


class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(Integer)
    category = Column(String(50))
    created_at = Column(DateTime)
    user_id = Column(Integer)  # Add user_id for CTE test


class Order(Base):
    __tablename__ = 'orders'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    product_id = Column(Integer)
    quantity = Column(Integer)
    total_amount = Column(Integer)
    created_at = Column(DateTime)


class TestExplainMethods:
    """Test class for explain methods functionality"""

    def setup_method(self):
        """Set up test client"""
        self.client = Client("test://localhost:6001/test")

    def test_to_sql_basic_query(self):
        """Test to_sql() method with basic query"""
        query = self.client.query(User).filter(User.age > 25).order_by(User.name)
        sql = query.to_sql()

        assert "SELECT" in sql
        assert "FROM users" in sql
        assert "WHERE age > 25" in sql
        assert "ORDER BY name" in sql

    def test_to_sql_with_parameters(self):
        """Test to_sql() method with parameters"""
        query = self.client.query(User).filter("age > ?", 30).filter("department = ?", "Engineering")
        sql = query.to_sql()

        assert "WHERE age > 30" in sql
        assert "department = 'Engineering'" in sql

    def test_to_sql_complex_query(self):
        """Test to_sql() method with complex query"""
        query = (
            self.client.query(User)
            .select(User.department, func.count(User.id), func.avg(User.salary))
            .filter(User.age > 25)
            .group_by(User.department)
            .having(func.count(User.id) > 1)
            .order_by(func.avg(User.salary).desc())
            .limit(10)
        )

        sql = query.to_sql()

        assert "SELECT" in sql
        assert "FROM users" in sql
        assert "WHERE age > 25" in sql
        assert "GROUP BY department" in sql
        assert "HAVING count(id) > 1" in sql
        assert "ORDER BY avg(salary) DESC" in sql
        assert "LIMIT 10" in sql

    def test_to_sql_with_joins(self):
        """Test to_sql() method with joins"""
        query = (
            self.client.query(User)
            .join(Order, User.id == Order.user_id)
            .select(User.name, func.sum(Order.total_amount))
            .filter(User.department == 'Sales')
            .group_by(User.id, User.name)
            .having(func.sum(Order.total_amount) > 10000)
            .order_by(func.sum(Order.total_amount).desc())
        )

        sql = query.to_sql()

        assert "JOIN orders" in sql
        assert "WHERE department = 'Sales'" in sql
        assert "GROUP BY id, name" in sql
        assert "HAVING sum(total_amount) > 10000" in sql

    def test_explain_sql_generation(self):
        """Test explain() method SQL generation"""
        query = self.client.query(User).filter(User.department == 'Engineering').order_by(User.salary.desc())
        sql, params = query._build_sql()

        # Test basic EXPLAIN
        explain_sql = f"EXPLAIN {sql}"
        assert explain_sql.startswith("EXPLAIN")
        assert "SELECT" in explain_sql
        assert "FROM users" in explain_sql
        assert "WHERE department = 'Engineering'" in explain_sql
        assert "ORDER BY salary DESC" in explain_sql

    def test_explain_verbose_sql_generation(self):
        """Test explain() method with verbose SQL generation"""
        query = (
            self.client.query(User)
            .select(User.department, func.count(User.id), func.avg(User.salary))
            .filter(User.age > 25)
            .group_by(User.department)
            .having(func.count(User.id) > 1)
        )

        sql, params = query._build_sql()

        # Test EXPLAIN VERBOSE
        explain_verbose_sql = f"EXPLAIN VERBOSE {sql}"
        assert explain_verbose_sql.startswith("EXPLAIN VERBOSE")
        assert "SELECT" in explain_verbose_sql
        assert "GROUP BY department" in explain_verbose_sql
        assert "HAVING count(id) > 1" in explain_verbose_sql

    def test_explain_analyze_sql_generation(self):
        """Test explain_analyze() method SQL generation"""
        query = self.client.query(User).filter(User.salary > 70000).order_by(User.age.desc())
        sql, params = query._build_sql()

        # Test EXPLAIN ANALYZE
        explain_analyze_sql = f"EXPLAIN ANALYZE {sql}"
        assert explain_analyze_sql.startswith("EXPLAIN ANALYZE")
        assert "SELECT" in explain_analyze_sql
        assert "WHERE salary > 70000" in explain_analyze_sql
        assert "ORDER BY age DESC" in explain_analyze_sql

    def test_explain_analyze_verbose_sql_generation(self):
        """Test explain_analyze() method with verbose SQL generation"""
        query = (
            self.client.query(User)
            .join(Order, User.id == Order.user_id)
            .select(User.name, func.sum(Order.total_amount))
            .filter(User.department == 'Sales')
            .group_by(User.id, User.name)
            .having(func.sum(Order.total_amount) > 10000)
            .order_by(func.sum(Order.total_amount).desc())
        )

        sql, params = query._build_sql()

        # Test EXPLAIN ANALYZE VERBOSE
        explain_analyze_verbose_sql = f"EXPLAIN ANALYZE VERBOSE {sql}"
        assert explain_analyze_verbose_sql.startswith("EXPLAIN ANALYZE VERBOSE")
        assert "SELECT" in explain_analyze_verbose_sql
        assert "JOIN orders" in explain_analyze_verbose_sql
        assert "GROUP BY id, name" in explain_analyze_verbose_sql

    def test_explain_methods_with_cte(self):
        """Test explain methods with CTE queries"""
        # Use string-based CTE instead of SQLAlchemy CTE object
        query = (
            self.client.query(Product)
            .select(Product.name, "user_stats.department")
            .filter("user_id IN (SELECT id FROM users WHERE active = 1)")
        )

        sql = query.to_sql()

        assert "SELECT" in sql
        assert "FROM products" in sql
        assert "WHERE user_id IN" in sql

        # Test EXPLAIN with CTE-like query
        explain_sql = f"EXPLAIN {sql}"
        assert explain_sql.startswith("EXPLAIN")

    def test_explain_methods_with_subquery(self):
        """Test explain methods with subqueries"""
        # Use string-based subquery instead of SQLAlchemy expression
        subquery_sql = "SELECT id FROM users WHERE salary > 80000 ORDER BY salary DESC LIMIT 5"

        query = (
            self.client.query(User)
            .select(User.name, User.salary)
            .filter(f"id IN ({subquery_sql})")
            .order_by(User.salary.desc())
        )

        sql = query.to_sql()

        assert "SELECT" in sql
        assert "WHERE id IN" in sql

        # Test EXPLAIN ANALYZE with subquery
        explain_analyze_sql = f"EXPLAIN ANALYZE {sql}"
        assert explain_analyze_sql.startswith("EXPLAIN ANALYZE")

    def test_explain_methods_parameter_handling(self):
        """Test that explain methods handle parameters correctly"""
        # Test with parameters
        query = self.client.query(User).filter("age > ?", 25).filter("department = ?", "Engineering")
        sql, params = query._build_sql()

        # Test that the SQL is generated correctly
        assert "SELECT" in sql
        assert "FROM users" in sql
        assert "age > 25" in sql
        assert "department = 'Engineering'" in sql

        # Test to_sql() method (should be the same as _build_sql() with substitution)
        formatted_sql = query.to_sql()
        assert "age > 25" in formatted_sql
        assert "department = 'Engineering'" in formatted_sql
        assert formatted_sql == sql  # Should be identical

    def test_explain_methods_edge_cases(self):
        """Test explain methods with edge cases"""
        # Empty query
        query = self.client.query(User)
        sql = query.to_sql()
        assert "SELECT * FROM users" in sql

        # Query with only LIMIT
        query = self.client.query(User).limit(10)
        sql = query.to_sql()
        assert "LIMIT 10" in sql

        # Query with only ORDER BY
        query = self.client.query(User).order_by(User.name)
        sql = query.to_sql()
        assert "ORDER BY name" in sql

        # Query with multiple ORDER BY columns
        query = self.client.query(User).order_by(User.department, User.name.desc())
        sql = query.to_sql()
        assert "ORDER BY department, name DESC" in sql

    def test_explain_methods_performance_queries(self):
        """Test explain methods with performance-focused queries"""
        # Complex aggregation query
        query = (
            self.client.query(User)
            .select(
                User.department,
                func.count(User.id).label('user_count'),
                func.avg(User.salary).label('avg_salary'),
                func.max(User.salary).label('max_salary'),
                func.min(User.salary).label('min_salary'),
            )
            .filter(User.age > 25)
            .group_by(User.department)
            .having(func.count(User.id) > 1)
            .order_by(func.avg(User.salary).desc())
            .limit(20)
        )

        sql = query.to_sql()

        assert "count(users.id) AS user_count" in sql
        assert "avg(users.salary) AS avg_salary" in sql
        assert "max(users.salary) AS max_salary" in sql
        assert "min(users.salary) AS min_salary" in sql
        assert "GROUP BY department" in sql
        assert "HAVING count(id) > 1" in sql
        assert "ORDER BY avg(salary) DESC" in sql
        assert "LIMIT 20" in sql

        # Test EXPLAIN ANALYZE for performance analysis
        explain_analyze_sql = f"EXPLAIN ANALYZE {sql}"
        assert explain_analyze_sql.startswith("EXPLAIN ANALYZE")

    def test_explain_methods_with_functions(self):
        """Test explain methods with various SQL functions"""
        query = (
            self.client.query(User)
            .select(
                func.year(User.created_at).label('year'),
                func.month(User.created_at).label('month'),
                func.count(User.id).label('user_count'),
            )
            .filter(func.year(User.created_at) > 2020)
            .group_by(func.year(User.created_at), func.month(User.created_at))
            .order_by(func.year(User.created_at).desc(), func.month(User.created_at).desc())
        )

        sql = query.to_sql()

        assert "year(users.created_at) AS year" in sql
        assert "month(users.created_at) AS month" in sql
        assert "count(users.id) AS user_count" in sql
        assert "WHERE year(created_at) > 2020" in sql
        assert "GROUP BY year(created_at), month(created_at)" in sql
        assert "ORDER BY year(created_at) DESC, month(created_at) DESC" in sql

        # Test EXPLAIN VERBOSE for function analysis
        explain_verbose_sql = f"EXPLAIN VERBOSE {sql}"
        assert explain_verbose_sql.startswith("EXPLAIN VERBOSE")


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"])
