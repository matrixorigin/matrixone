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
Online tests for model-based insert operations
"""

import pytest
import os
import sys
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client
from sqlalchemy import Column, Integer, String, Text, DateTime, DECIMAL
from matrixone.orm import declarative_base
from matrixone.exceptions import QueryError
from .test_config import online_config

Base = declarative_base()


class User(Base):
    """Test user model for insert operations"""

    __tablename__ = "test_insert_users"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))
    age = Column(Integer)
    created_at = Column(DateTime)


class Product(Base):
    """Test product model for insert operations"""

    __tablename__ = "test_insert_products"

    id = Column(Integer, primary_key=True)
    name = Column(String(200))
    description = Column(Text)
    price = Column(DECIMAL(10, 2))
    category = Column(String(50))
    in_stock = Column(Integer)


class Order(Base):
    """Test order model for insert operations"""

    __tablename__ = "test_insert_orders"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    product_id = Column(Integer)
    quantity = Column(Integer)
    total_amount = Column(DECIMAL(10, 2))
    order_date = Column(DateTime)


class TestModelInsert:
    """Test model-based insert operations"""

    @pytest.fixture(scope="class")
    def test_client(self):
        """Create and connect Client for testing"""
        host, port, user, password, database = online_config.get_connection_params()
        client = Client()
        client.connect(host=host, port=port, user=user, password=password, database=database)
        try:
            yield client
        finally:
            try:
                client.disconnect()
            except:
                pass

    @pytest.fixture(scope="class")
    def test_database(self, test_client):
        """Set up test database and tables"""
        test_db = "test_model_insert_db"

        try:
            test_client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            test_client.execute(f"USE {test_db}")

            # Create test tables using models
            test_client.create_table(User)
            test_client.create_table(Product)
            test_client.create_table(Order)

            yield test_db

        finally:
            # Clean up
            try:
                test_client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            except Exception as e:
                print(f"Cleanup failed: {e}")

    def test_single_insert_with_model(self, test_client, test_database):
        """Test inserting a single record using model class"""
        # Insert a single user using model class
        result = test_client.insert(
            User, {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30, 'created_at': datetime.now()}
        )

        assert result.affected_rows == 1

        # Verify the data was inserted correctly
        users = test_client.query(User).all()
        assert len(users) == 1
        assert users[0].name == 'John Doe'
        assert users[0].email == 'john@example.com'
        assert users[0].age == 30

    def test_batch_insert_with_model(self, test_client, test_database):
        """Test batch inserting multiple records using model class"""
        # Insert multiple products using model class
        products_data = [
            {
                'id': 1,
                'name': 'Laptop',
                'description': 'High-performance laptop',
                'price': 999.99,
                'category': 'Electronics',
                'in_stock': 10,
            },
            {
                'id': 2,
                'name': 'Book',
                'description': 'Programming guide',
                'price': 29.99,
                'category': 'Education',
                'in_stock': 50,
            },
            {
                'id': 3,
                'name': 'Phone',
                'description': 'Smartphone with AI features',
                'price': 699.99,
                'category': 'Electronics',
                'in_stock': 25,
            },
        ]

        result = test_client.batch_insert(Product, products_data)
        assert result.affected_rows == 3

        # Verify the data was inserted correctly
        products = test_client.query(Product).all()
        assert len(products) == 3

        # Check specific products
        laptop = test_client.query(Product).filter_by(name='Laptop').first()
        assert laptop is not None
        assert float(laptop.price) == 999.99
        assert laptop.category == 'Electronics'
        assert laptop.in_stock == 10

    def test_insert_with_null_values(self, test_client, test_database):
        """Test inserting records with NULL values using model class"""
        # Insert a user with some NULL values
        result = test_client.insert(
            User, {'id': 2, 'name': 'Jane Smith', 'email': None, 'age': 25, 'created_at': None}  # NULL value  # NULL value
        )

        assert result.affected_rows == 1

        # Verify the data was inserted correctly
        user = test_client.query(User).filter_by(id=2).first()
        assert user is not None
        assert user.name == 'Jane Smith'
        assert user.email is None
        assert user.age == 25
        assert user.created_at is None

    def test_insert_with_different_data_types(self, test_client, test_database):
        """Test inserting records with different data types using model class"""
        # Insert an order with various data types
        result = test_client.insert(
            Order,
            {
                'id': 1,
                'user_id': 1,
                'product_id': 1,
                'quantity': 2,
                'total_amount': 1999.98,
                'order_date': datetime(2024, 1, 15, 10, 30, 0),
            },
        )

        assert result.affected_rows == 1

        # Verify the data was inserted correctly
        order = test_client.query(Order).filter_by(id=1).first()
        assert order is not None
        assert order.user_id == 1
        assert order.product_id == 1
        assert order.quantity == 2
        assert float(order.total_amount) == 1999.98
        assert order.order_date.year == 2024
        assert order.order_date.month == 1
        assert order.order_date.day == 15

    def test_insert_error_handling(self, test_client, test_database):
        """Test error handling when inserting invalid data using model class"""
        # Try to insert a user with duplicate primary key
        with pytest.raises(QueryError):
            test_client.insert(
                User,
                {'id': 1, 'name': 'Duplicate User', 'email': 'duplicate@example.com', 'age': 35},  # Duplicate primary key
            )

    def test_insert_vs_string_table_name(self, test_client, test_database):
        """Test that both model class and string table name work the same way"""
        # Clear existing data first
        test_client.execute("DELETE FROM test_insert_users")

        # Insert using model class
        result1 = test_client.insert(User, {'id': 3, 'name': 'Model User', 'email': 'model@example.com', 'age': 28})

        # Insert using string table name
        result2 = test_client.insert(
            'test_insert_users', {'id': 4, 'name': 'String User', 'email': 'string@example.com', 'age': 32}
        )

        assert result1.affected_rows == 1
        assert result2.affected_rows == 1

        # Verify both records were inserted
        users = test_client.query(User).all()
        assert len(users) == 2  # Only the two we just inserted

        # Check specific users
        model_user = test_client.query(User).filter_by(name='Model User').first()
        string_user = test_client.query(User).filter_by(name='String User').first()

        assert model_user is not None
        assert string_user is not None
        assert model_user.email == 'model@example.com'
        assert string_user.email == 'string@example.com'
