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
Test: filter(logical_in(...)) syntax for enhanced IN functionality

This test verifies that the filter(logical_in(...)) syntax works correctly with various
value types including FulltextFilter objects, lists, and SQLAlchemy expressions.
"""

import pytest
import sys
import os
from datetime import datetime

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from matrixone import Client
from sqlalchemy import Column, Integer, String, DateTime, func
from matrixone.orm import declarative_base, logical_in
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match

# Create SQLAlchemy models
Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    age = Column(Integer)
    city = Column(String(50))
    department = Column(String(50))
    salary = Column(Integer)
    created_at = Column(DateTime)


class Article(Base):
    __tablename__ = 'articles'
    id = Column(Integer, primary_key=True)
    title = Column(String(100))
    content = Column(String(500))
    author = Column(String(50))
    created_at = Column(DateTime)


class TestFilterLogicalIn:
    """Test class for filter(logical_in(...)) syntax functionality"""

    def setup_method(self):
        """Set up test client"""
        self.client = Client("test://localhost:6001/test")

    def test_filter_logical_in_with_list_values(self):
        """Test filter(logical_in(...)) with list of values"""
        # Test with integer list
        query1 = self.client.query(User).filter(logical_in(User.id, [1, 2, 3, 4]))
        sql1 = query1.to_sql()

        assert "SELECT" in sql1
        assert "FROM users" in sql1
        assert "WHERE id IN (1,2,3,4)" in sql1

        # Test with string list
        query2 = self.client.query(User).filter(logical_in("city", ["北京", "上海", "广州"]))
        sql2 = query2.to_sql()

        assert "WHERE city IN ('北京','上海','广州')" in sql2

        # Test with empty list
        query3 = self.client.query(User).filter(logical_in(User.id, []))
        sql3 = query3.to_sql()

        assert "WHERE 1=0" in sql3  # Empty list should result in always false condition

    def test_filter_logical_in_with_single_value(self):
        """Test filter(logical_in(...)) with single value"""
        query = self.client.query(User).filter(logical_in(User.id, 5))
        sql = query.to_sql()

        assert "WHERE id IN (5)" in sql

    def test_filter_logical_in_with_fulltext_filter(self):
        """Test filter(logical_in(...)) with FulltextFilter objects"""
        # Basic boolean_match
        query1 = self.client.query(Article).filter(logical_in(Article.id, boolean_match("title", "content").must("python")))
        sql1 = query1.to_sql()

        assert "SELECT" in sql1
        assert "FROM articles" in sql1
        assert "WHERE id IN (SELECT id FROM table WHERE MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE))" in sql1

        # Complex boolean_match
        query2 = self.client.query(Article).filter(
            logical_in(
                Article.id, boolean_match("title", "content").must("python").encourage("programming").must_not("java")
            )
        )
        sql2 = query2.to_sql()

        assert (
            "WHERE id IN (SELECT id FROM table WHERE MATCH(title, content) AGAINST('+python programming -java' IN BOOLEAN MODE))"
            in sql2
        )

    def test_filter_logical_in_with_subquery(self):
        """Test filter(logical_in(...)) with subquery objects"""
        # Create a subquery
        subquery = self.client.query(User).select(User.id).filter(User.age > 25)

        query = self.client.query(Article).filter(logical_in(Article.author, subquery))
        sql = query.to_sql()

        assert "WHERE author IN (SELECT users.id AS id FROM users WHERE age > 25)" in sql

    def test_filter_logical_in_with_sqlalchemy_expressions(self):
        """Test filter(logical_in(...)) with SQLAlchemy expressions"""
        # Test with function expression
        query = self.client.query(User).filter(logical_in("id", func.count(User.id)))
        sql = query.to_sql()

        assert "WHERE id IN (count(id))" in sql

    def test_filter_logical_in_with_string_column(self):
        """Test filter(logical_in(...)) with string column names"""
        query = self.client.query(User).filter(logical_in("department", ["Engineering", "Sales", "Marketing"]))
        sql = query.to_sql()

        assert "WHERE department IN ('Engineering','Sales','Marketing')" in sql

    def test_filter_logical_in_with_sqlalchemy_column(self):
        """Test filter(logical_in(...)) with SQLAlchemy column objects"""
        query = self.client.query(User).filter(logical_in(User.department, ["Engineering", "Sales", "Marketing"]))
        sql = query.to_sql()

        assert "WHERE department IN ('Engineering','Sales','Marketing')" in sql

    def test_filter_logical_in_combined_with_other_conditions(self):
        """Test filter(logical_in(...)) combined with other filter conditions"""
        query = (
            self.client.query(Article)
            .filter(Article.author == "张三")
            .filter(logical_in(Article.id, boolean_match("title", "content").must("python")))
            .filter("created_at > ?", "2023-01-01")
        )

        sql = query.to_sql()

        assert "WHERE author = '张三'" in sql
        assert "AND id IN (SELECT id FROM table WHERE MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE))" in sql
        assert "AND created_at > '2023-01-01'" in sql

    def test_filter_logical_in_with_tuple_values(self):
        """Test filter(logical_in(...)) with tuple values"""
        query = self.client.query(User).filter(logical_in(User.id, (1, 2, 3, 4, 5)))
        sql = query.to_sql()

        assert "WHERE id IN (1,2,3,4,5)" in sql

    def test_filter_logical_in_edge_cases(self):
        """Test filter(logical_in(...)) with edge cases"""
        # Test with None value
        query1 = self.client.query(User).filter(logical_in(User.id, None))
        sql1 = query1.to_sql()

        assert "WHERE id IN (None)" in sql1

        # Test with mixed types in list
        query2 = self.client.query(User).filter(logical_in("id", [1, "2", 3.0]))
        sql2 = query2.to_sql()

        assert "WHERE id IN (1,'2',3.0)" in sql2

    def test_filter_logical_in_method_chaining(self):
        """Test filter(logical_in(...)) with method chaining"""
        query = (
            self.client.query(User)
            .filter(logical_in(User.city, ["北京", "上海"]))
            .filter(logical_in(User.department, ["Engineering", "Sales"]))
            .filter(User.age > 25)
            .order_by(User.name)
            .limit(10)
        )

        sql = query.to_sql()

        assert "WHERE city IN ('北京','上海')" in sql
        assert "AND department IN ('Engineering','Sales')" in sql
        assert "AND age > 25" in sql
        assert "ORDER BY name" in sql
        assert "LIMIT 10" in sql

    def test_filter_logical_in_performance_queries(self):
        """Test filter(logical_in(...)) with performance-focused queries"""
        # Complex query with multiple logical_in conditions
        query = (
            self.client.query(User)
            .select(User.name, User.department, func.count(User.id))
            .filter(logical_in(User.city, ["北京", "上海", "广州", "深圳"]))
            .filter(logical_in(User.department, ["Engineering", "Sales", "Marketing"]))
            .filter(User.age > 25)
            .group_by(User.department)
            .having(func.count(User.id) > 1)
            .order_by(func.count(User.id).desc())
            .limit(20)
        )

        sql = query.to_sql()

        assert "SELECT" in sql
        assert "FROM users" in sql
        assert "WHERE city IN ('北京','上海','广州','深圳')" in sql
        assert "AND department IN ('Engineering','Sales','Marketing')" in sql
        assert "AND age > 25" in sql
        assert "GROUP BY department" in sql
        assert "HAVING count(id) > 1" in sql
        assert "ORDER BY count(id) DESC" in sql
        assert "LIMIT 20" in sql


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"])
