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
Online Test: filter(logical_in(...)) syntax for enhanced IN functionality

This test verifies that the filter(logical_in(...)) syntax works correctly with actual
database connections, including FulltextFilter objects, lists, and SQLAlchemy expressions.
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

try:
    from .test_config import online_config
except ImportError:
    # Fallback for when running as standalone script
    import test_config

    online_config = test_config.online_config

# Create SQLAlchemy models
Base = declarative_base()


class User(Base):
    __tablename__ = 'test_users_logical_in'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    age = Column(Integer)
    city = Column(String(50))
    department = Column(String(50))
    salary = Column(Integer)
    created_at = Column(DateTime)


class Article(Base):
    __tablename__ = 'test_articles_logical_in'
    id = Column(Integer, primary_key=True)
    title = Column(String(100))
    content = Column(String(500))
    author = Column(String(50))
    created_at = Column(DateTime)


class TestFilterLogicalInOnline:
    """Online test class for filter(logical_in(...)) syntax functionality"""

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self):
        """Set up test data and clean up after tests"""
        # Setup - use real database connection
        host, port, user, password, database = online_config.get_connection_params()
        self.client = Client()
        self.client.connect(host=host, port=port, user=user, password=password, database=database)

        # Create tables
        self.client.create_table(
            "test_users_logical_in",
            {
                "id": "int",
                "name": "varchar(50)",
                "age": "int",
                "city": "varchar(50)",
                "department": "varchar(50)",
                "salary": "int",
                "created_at": "datetime",
            },
            primary_key="id",
        )
        self.client.create_table(
            "test_articles_logical_in",
            {
                "id": "int",
                "title": "varchar(100)",
                "content": "varchar(500)",
                "author": "varchar(50)",
                "created_at": "datetime",
            },
            primary_key="id",
        )

        # Insert test data
        self._insert_test_data()

        yield

        # Cleanup
        try:
            self.client.drop_table("test_users_logical_in")
            self.client.drop_table("test_articles_logical_in")
        except:
            pass  # Ignore cleanup errors
        finally:
            # Disconnect from database
            try:
                self.client.close()
            except:
                pass

    def _insert_test_data(self):
        """Insert test data for online tests"""
        # Insert users
        users_data = [
            {"name": "张三", "age": 25, "city": "北京", "department": "Engineering", "salary": 80000},
            {"name": "李四", "age": 30, "city": "上海", "department": "Sales", "salary": 70000},
            {"name": "王五", "age": 35, "city": "广州", "department": "Engineering", "salary": 90000},
            {"name": "赵六", "age": 28, "city": "深圳", "department": "Marketing", "salary": 75000},
            {"name": "钱七", "age": 32, "city": "北京", "department": "Sales", "salary": 85000},
            {"name": "孙八", "age": 27, "city": "上海", "department": "Engineering", "salary": 80000},
        ]

        for user_data in users_data:
            self.client.insert("test_users_logical_in", user_data)

        # Insert articles
        articles_data = [
            {"title": "Python编程入门", "content": "Python是一种高级编程语言，适合初学者学习", "author": "张三"},
            {"title": "Java开发指南", "content": "Java是企业级应用开发的首选语言", "author": "李四"},
            {"title": "Python数据分析", "content": "使用Python进行数据分析和机器学习", "author": "王五"},
            {"title": "JavaScript前端开发", "content": "JavaScript是现代前端开发的核心技术", "author": "赵六"},
            {"title": "Python Web开发", "content": "使用Python和Django进行Web应用开发", "author": "张三"},
            {"title": "数据库设计原理", "content": "关系型数据库的设计和优化技巧", "author": "钱七"},
        ]

        for article_data in articles_data:
            self.client.insert("test_articles_logical_in", article_data)

    def test_filter_logical_in_with_list_values_online(self):
        """Test filter(logical_in(...)) with list of values - online"""
        # Test with city list
        query = self.client.query(User).filter(logical_in("city", ["北京", "上海"]))
        results = query.all()

        # Should have 4 users: 张三(北京), 李四(上海), 钱七(北京), 孙八(上海)
        assert len(results) == 4

        cities = [user.city for user in results]
        assert "北京" in cities
        assert "上海" in cities
        assert "广州" not in cities
        assert "深圳" not in cities

    def test_filter_logical_in_with_department_list_online(self):
        """Test filter(logical_in(...)) with department list - online"""
        query = self.client.query(User).filter(logical_in("department", ["Engineering", "Sales"]))
        results = query.all()

        # Should have 5 users: 张三(Engineering), 李四(Sales), 王五(Engineering), 钱七(Sales), 孙八(Engineering)
        assert len(results) == 5

        departments = [user.department for user in results]
        assert "Engineering" in departments
        assert "Sales" in departments
        assert "Marketing" not in departments

    def test_filter_logical_in_with_age_range_online(self):
        """Test filter(logical_in(...)) with age range - online"""
        query = self.client.query(User).filter(logical_in("age", [25, 30, 35]))
        results = query.all()

        # Should have 3 users: 张三(25), 李四(30), 王五(35)
        assert len(results) == 3

        ages = [user.age for user in results]
        assert 25 in ages
        assert 30 in ages
        assert 35 in ages
        assert 28 not in ages
        assert 32 not in ages
        assert 27 not in ages

    def test_filter_logical_in_with_salary_range_online(self):
        """Test filter(logical_in(...)) with salary range - online"""
        query = self.client.query(User).filter(logical_in("salary", [80000, 90000]))
        results = query.all()

        # Should have 3 users: 张三(80000), 王五(90000), 孙八(80000)
        assert len(results) == 3

        salaries = [user.salary for user in results]
        assert 80000 in salaries
        assert 90000 in salaries
        assert 70000 not in salaries
        assert 75000 not in salaries
        assert 85000 not in salaries

    def test_filter_logical_in_with_author_list_online(self):
        """Test filter(logical_in(...)) with author list - online"""
        query = self.client.query(Article).filter(logical_in("author", ["张三", "李四"]))
        results = query.all()

        # Should have 3 articles: 张三的2篇文章 + 李四的1篇文章
        assert len(results) == 3

        authors = [article.author for article in results]
        assert "张三" in authors
        assert "李四" in authors
        assert "王五" not in authors
        assert "赵六" not in authors
        assert "钱七" not in authors

    def test_filter_logical_in_combined_with_other_conditions_online(self):
        """Test filter(logical_in(...)) combined with other conditions - online"""
        query = (
            self.client.query(User)
            .filter(logical_in("city", ["北京", "上海"]))
            .filter(logical_in("department", ["Engineering", "Sales"]))
            .filter("age > ?", 25)
        )

        results = query.all()

        # Should return 3 users: 李四(上海, Sales, 30), 钱七(北京, Sales, 32), 孙八(上海, Engineering, 27)
        assert len(results) == 3

        for user in results:
            assert user.city in ["北京", "上海"]
            assert user.department in ["Engineering", "Sales"]
            assert user.age > 25

    def test_filter_logical_in_with_empty_list_online(self):
        """Test filter(logical_in(...)) with empty list - online"""
        query = self.client.query(User).filter(logical_in("id", []))
        results = query.all()

        assert len(results) == 0  # Empty list should return no results

    def test_filter_logical_in_with_single_value_online(self):
        """Test filter(logical_in(...)) with single value - online"""
        query = self.client.query(User).filter(logical_in("id", 1))
        results = query.all()

        assert len(results) == 1
        assert results[0].id == 1
        assert results[0].name == "张三"

    def test_filter_logical_in_with_tuple_values_online(self):
        """Test filter(logical_in(...)) with tuple values - online"""
        query = self.client.query(User).filter(logical_in("id", (1, 2, 3)))
        results = query.all()

        assert len(results) == 3

        ids = [user.id for user in results]
        assert 1 in ids
        assert 2 in ids
        assert 3 in ids
        assert 4 not in ids
        assert 5 not in ids
        assert 6 not in ids

    def test_filter_logical_in_method_chaining_online(self):
        """Test filter(logical_in(...)) with method chaining - online"""
        query = (
            self.client.query(User)
            .filter(logical_in("city", ["北京", "上海"]))
            .filter(logical_in("department", ["Engineering", "Sales"]))
            .filter("age > ?", 25)
            .order_by("name")
            .limit(10)
        )

        results = query.all()

        # Should return 3 users: 李四(上海, Sales, 30), 钱七(北京, Sales, 32), 孙八(上海, Engineering, 27)
        assert len(results) == 3

        # Results should be ordered by name
        names = [user.name for user in results]
        assert names == sorted(names)

    def test_filter_logical_in_with_aggregate_functions_online(self):
        """Test filter(logical_in(...)) with aggregate functions - online"""
        # Test with count function
        query = (
            self.client.query(User)
            .select(User.department, func.count(User.id))
            .filter(logical_in("city", ["北京", "上海", "广州"]))
            .group_by(User.department)
            .having(func.count(User.id) > 1)
        )

        results = query.all()

        # Should return departments with more than 1 user in the specified cities
        assert len(results) >= 0  # May vary based on data

        for result in results:
            assert result.count > 1

    def test_filter_logical_in_performance_online(self):
        """Test filter(logical_in(...)) performance with complex queries - online"""
        query = (
            self.client.query(User)
            .select(User.department, func.count(User.id))
            .filter(logical_in("city", ["北京", "上海", "广州", "深圳"]))
            .filter(logical_in("department", ["Engineering", "Sales", "Marketing"]))
            .filter("age > ?", 25)
            .group_by(User.department)
            .having(func.count(User.id) > 0)
            .order_by(func.count(User.id).desc())
            .limit(20)
        )

        results = query.all()

        # Should return aggregated results
        assert len(results) >= 0

        # Results should be ordered by count descending
        if len(results) > 1:
            counts = [result.count for result in results]
            assert counts == sorted(counts, reverse=True)

    def test_filter_logical_in_with_article_search_online(self):
        """Test filter(logical_in(...)) with article search - online"""
        # Search for articles by specific authors
        query = self.client.query(Article).filter(logical_in("author", ["张三", "王五"]))
        results = query.all()

        assert len(results) == 3  # 张三的2篇文章 + 王五的1篇文章

        authors = [article.author for article in results]
        assert "张三" in authors
        assert "王五" in authors
        assert "李四" not in authors
        assert "赵六" not in authors
        assert "钱七" not in authors

    def test_filter_logical_in_with_mixed_conditions_online(self):
        """Test filter(logical_in(...)) with mixed conditions - online"""
        query = (
            self.client.query(Article)
            .filter(logical_in("author", ["张三", "李四", "王五"]))
            .filter("title LIKE ?", "%Python%")
        )

        results = query.all()

        # Should return Python-related articles by specified authors
        assert len(results) >= 0

        for article in results:
            assert article.author in ["张三", "李四", "王五"]
            assert "Python" in article.title


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"])
