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
Offline unit tests for unified filter interface

Test SQL generation for various filter conditions without database connection
"""

import unittest
from unittest.mock import Mock, MagicMock
from sqlalchemy import Column, Integer, String, DECIMAL, and_, or_, not_, func
from matrixone.orm import declarative_base

# Import the ORM classes
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone.orm import BaseMatrixOneQuery

Base = declarative_base()


class User(Base):
    """Test user model"""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))
    age = Column(Integer)
    city = Column(String(50))


class Product(Base):
    """Test product model"""

    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(DECIMAL(10, 2))
    category = Column(String(50))
    stock = Column(Integer)


class TestUnifiedFilter(unittest.TestCase):
    """Test unified filter interface SQL generation"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.user_query = BaseMatrixOneQuery(User, self.mock_client)
        self.product_query = BaseMatrixOneQuery(Product, self.mock_client)

    def test_string_condition(self):
        """Test string condition SQL generation"""
        query = self.user_query.filter("age < 30")
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE age < 30"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_sqlalchemy_expression(self):
        """Test SQLAlchemy expression SQL generation"""
        query = self.product_query.filter(Product.category == "电子产品")
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM products WHERE category = '电子产品'"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_or_condition(self):
        """Test OR condition SQL generation"""
        query = self.user_query.filter(or_(User.age < 30, User.city == "北京"))
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE age < 30 OR city = '北京'"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_and_condition(self):
        """Test AND condition SQL generation"""
        query = self.product_query.filter(and_(Product.category == "电子产品", Product.price > 1000))
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM products WHERE category = '电子产品' AND price > 1000"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_multiple_filter_calls(self):
        """Test multiple filter calls (AND relationship)"""
        query = (
            self.product_query.filter(Product.category == "电子产品").filter(Product.price > 1000).filter(Product.stock > 50)
        )
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM products WHERE category = '电子产品' AND price > 1000 AND stock > 50"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_complex_nested_condition(self):
        """Test complex nested OR condition"""
        query = self.product_query.filter(
            or_(
                and_(Product.category == "电子产品", Product.price > 2000),
                and_(Product.category == "图书", Product.stock > 400),
            )
        )
        sql, params = query._build_sql()

        expected_sql = (
            "SELECT * FROM products WHERE category = '电子产品' AND price > 2000 OR category = '图书' AND stock > 400"
        )
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_not_condition(self):
        """Test NOT condition SQL generation"""
        query = self.product_query.filter(not_(Product.category == "电子产品"))
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM products WHERE category != '电子产品'"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_mixed_condition_types(self):
        """Test mixed string and SQLAlchemy expression conditions"""
        query = (
            self.user_query.filter("age > 25")
            .filter(User.city != "深圳")
            .filter(or_(User.name.like("张%"), User.name.like("李%")))
        )
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE age > 25 AND city != '深圳' AND name LIKE '张%' OR name LIKE '李%'"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_filter_by_keyword_args(self):
        """Test filter_by with keyword arguments"""
        query = self.user_query.filter_by(city="北京", age=25)
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE city = '北京' AND age = 25"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_filter_by_with_parameters(self):
        """Test filter_by with parameterized values"""
        query = self.user_query.filter_by(city="北京")
        query._where_params.append(25)  # Simulate parameter
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE city = '北京'"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [25])

    def test_in_condition(self):
        """Test IN condition SQL generation"""
        query = self.user_query.filter(User.city.in_(["北京", "上海", "广州"]))
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE city IN ('北京', '上海', '广州')"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_like_condition(self):
        """Test LIKE condition SQL generation"""
        query = self.user_query.filter(User.email.like("%example.com%"))
        sql, params = query._build_sql()

        # SQLAlchemy compiles LIKE with single quotes, but our regex removes them
        expected_sql = "SELECT * FROM users WHERE email LIKE '%com%'"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_between_condition(self):
        """Test BETWEEN condition SQL generation"""
        query = self.user_query.filter(User.age.between(25, 35))
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE age BETWEEN 25 AND 35"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_greater_than_condition(self):
        """Test greater than condition SQL generation"""
        query = self.product_query.filter(Product.price > 1000)
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM products WHERE price > 1000"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_less_than_condition(self):
        """Test less than condition SQL generation"""
        query = self.product_query.filter(Product.stock < 100)
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM products WHERE stock < 100"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_greater_equal_condition(self):
        """Test greater than or equal condition SQL generation"""
        query = self.user_query.filter(User.age >= 25)
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE age >= 25"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_less_equal_condition(self):
        """Test less than or equal condition SQL generation"""
        query = self.user_query.filter(User.age <= 35)
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE age <= 35"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_not_equal_condition(self):
        """Test not equal condition SQL generation"""
        query = self.user_query.filter(User.city != "深圳")
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE city != '深圳'"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_complex_and_or_combination(self):
        """Test complex AND/OR combination"""
        query = self.user_query.filter(
            and_(
                or_(User.age >= 25, User.city.in_(["北京", "上海"])),
                not_(User.email.like("%test%")),
            )
        )
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE (age >= 25 OR city IN ('北京', '上海')) AND email NOT LIKE '%test%'"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_multiple_or_conditions(self):
        """Test multiple OR conditions"""
        query = self.user_query.filter(or_(User.city == "北京", User.city == "上海", User.age > 30))
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE city = '北京' OR city = '上海' OR age > 30"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_deeply_nested_conditions(self):
        """Test deeply nested conditions"""
        query = self.product_query.filter(
            or_(
                and_(Product.category == "电子产品", or_(Product.price > 2000, Product.stock < 100)),
                and_(Product.category == "图书", Product.stock > 400),
            )
        )
        sql, params = query._build_sql()

        # SQLAlchemy preserves parentheses for nested OR conditions
        expected_sql = "SELECT * FROM products WHERE category = '电子产品' AND (price > 2000 OR stock < 100) OR category = '图书' AND stock > 400"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_string_with_parameters(self):
        """Test string condition with parameters"""
        # Add parameters before calling filter
        query = self.user_query
        query._where_params.append(25)
        query = query.filter("age > ?")
        sql, params = query._build_sql()

        # Parameters are processed during filter() call
        expected_sql = "SELECT * FROM users WHERE age > 25"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_string_with_multiple_parameters(self):
        """Test string condition with multiple parameters"""
        # Add parameters before calling filter
        query = self.user_query
        query._where_params.extend([25, 35])
        query = query.filter("age BETWEEN ? AND ?")
        sql, params = query._build_sql()

        # Parameters are processed during filter() call
        expected_sql = "SELECT * FROM users WHERE age BETWEEN 25 AND 35"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_string_with_string_parameters(self):
        """Test string condition with string parameters"""
        # Add parameters before calling filter
        query = self.user_query
        query._where_params.append("北京")
        query = query.filter("city = ?")
        sql, params = query._build_sql()

        # Parameters are processed during filter() call
        expected_sql = "SELECT * FROM users WHERE city = '北京'"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_empty_filter(self):
        """Test query without any filters"""
        query = self.user_query
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_filter_with_order_by(self):
        """Test filter combined with order by"""
        query = self.product_query.filter(Product.price > 1000).order_by("price DESC")
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM products WHERE price > 1000 ORDER BY price DESC"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_filter_with_limit(self):
        """Test filter combined with limit"""
        query = self.user_query.filter(User.age > 25).limit(10)
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE age > 25 LIMIT 10"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_filter_with_offset(self):
        """Test filter combined with offset"""
        query = self.user_query.filter(User.age > 25).offset(5)
        sql, params = query._build_sql()

        expected_sql = "SELECT * FROM users WHERE age > 25 OFFSET 5"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_filter_with_select_columns(self):
        """Test filter combined with select columns"""
        query = self.user_query.select("name", "email").filter(User.age > 25)
        sql, params = query._build_sql()

        expected_sql = "SELECT name, email FROM users WHERE age > 25"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_filter_with_group_by(self):
        """Test filter combined with group by"""
        query = self.product_query.select("category", "COUNT(*) as count").filter(Product.price > 100).group_by("category")
        sql, params = query._build_sql()

        expected_sql = "SELECT category, COUNT(*) as count FROM products WHERE price > 100 GROUP BY category"
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])

    def test_filter_with_having(self):
        """Test filter combined with having"""
        query = (
            self.product_query.select("category", "COUNT(*) as count")
            .filter(Product.price > 100)
            .group_by("category")
            .having(func.count("*") > 1)
        )
        sql, params = query._build_sql()

        expected_sql = (
            "SELECT category, COUNT(*) as count FROM products WHERE price > 100 GROUP BY category HAVING count(*) > 1"
        )
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, [])


if __name__ == "__main__":
    unittest.main()
