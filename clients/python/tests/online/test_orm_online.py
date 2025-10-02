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
Online tests for ORM functionality - tests actual database operations
"""

import pytest
import os
import sys
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client
from sqlalchemy import Column, Integer, String, DECIMAL

# SQLAlchemy compatibility import
try:
    from sqlalchemy.orm import declarative_base
except ImportError:
    from matrixone.orm import declarative_base

Base = declarative_base()
from matrixone.exceptions import QueryError
from .test_config import online_config


class User(Base):
    """Test user model"""

    __tablename__ = "test_users"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))
    age = Column(Integer)


class Product(Base):
    """Test product model"""

    __tablename__ = "test_products"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(DECIMAL(10, 2))
    category = Column(String(50))


class TestORMOnline:
    """Online tests for ORM functionality"""

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
            except Exception as e:
                print(f"Warning: Failed to disconnect client: {e}")

    @pytest.fixture(scope="class")
    def test_database(self, test_client):
        """Set up test database and tables"""
        test_db = "test_orm_db"

        try:
            test_client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            test_client.execute(f"USE {test_db}")

            # Create test tables using SQLAlchemy models
            Base.metadata.create_all(test_client._engine)

            # Insert test data
            test_client.execute(
                """
                INSERT INTO test_users VALUES 
                (1, 'John Doe', 'john@example.com', 30),
                (2, 'Jane Smith', 'jane@example.com', 25),
                (3, 'Bob Johnson', 'bob@example.com', 35)
            """
            )

            test_client.execute(
                """
                INSERT INTO test_products VALUES 
                (1, 'Laptop', 999.99, 'Electronics'),
                (2, 'Book', 19.99, 'Education'),
                (3, 'Phone', 699.99, 'Electronics')
            """
            )

            yield test_db

        finally:
            # Clean up
            try:
                test_client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            except Exception as e:
                print(f"Cleanup failed: {e}")

    def test_model_creation_and_attributes(self):
        """Test model creation and attribute access"""
        user = User(id=1, name="Test User", email="test@example.com", age=30)

        assert user.id == 1
        assert user.name == "Test User"
        assert user.email == "test@example.com"
        assert user.age == 30

        # Test attribute access (SQLAlchemy models don't have to_dict by default)
        assert hasattr(user, 'id')
        assert hasattr(user, 'name')
        assert hasattr(user, 'email')
        assert hasattr(user, 'age')

    def test_model_table_creation(self):
        """Test creating tables from models"""
        # This test verifies that the model metadata is correctly set up
        assert User.__tablename__ == "test_users"
        assert Product.__tablename__ == "test_products"

        # Check that columns are properly defined
        assert hasattr(User, '__table__')
        assert 'id' in [col.name for col in User.__table__.columns]
        assert 'name' in [col.name for col in User.__table__.columns]
        assert 'email' in [col.name for col in User.__table__.columns]
        assert 'age' in [col.name for col in User.__table__.columns]

    def test_orm_query_basic_operations(self, test_client, test_database):
        """Test basic ORM query operations"""
        # Test all() method
        users = test_client.query(User).all()
        assert len(users) == 3

        # Test first() method
        first_user = test_client.query(User).first()
        assert first_user is not None
        assert isinstance(first_user, User)

        # Test filter_by() method
        john = test_client.query(User).filter_by(name="John Doe").first()
        assert john is not None
        assert john.name == "John Doe"
        assert john.email == "john@example.com"

    def test_orm_query_filtering(self, test_client, test_database):
        """Test ORM query filtering"""
        # Test filter_by with multiple conditions
        electronics = test_client.query(Product).filter_by(category="Electronics").all()
        assert len(electronics) == 2

        # Test filter() method with conditions
        expensive_products = test_client.query(Product).filter("price > ?", 500).all()
        assert len(expensive_products) == 2

        # Test combined filtering
        expensive_electronics = test_client.query(Product).filter_by(category="Electronics").filter("price > ?", 500).all()
        assert len(expensive_electronics) == 2

    def test_orm_query_counting(self, test_client, test_database):
        """Test ORM query counting"""
        # Test count() method
        total_users = test_client.query(User).count()
        assert total_users == 3

        # Test count() with filter
        electronics_count = test_client.query(Product).filter_by(category="Electronics").count()
        assert electronics_count == 2

        # Test count() with complex filter
        expensive_count = test_client.query(Product).filter("price > ?", 500).count()
        assert expensive_count == 2

    def test_orm_query_ordering(self, test_client, test_database):
        """Test ORM query ordering"""
        from matrixone.orm import desc

        # Test order_by ascending (default)
        users_asc = test_client.query(User).order_by("age").all()
        assert len(users_asc) == 3
        assert users_asc[0].age <= users_asc[1].age <= users_asc[2].age

        # Test order_by descending
        users_desc = test_client.query(User).order_by(desc("age")).all()
        assert len(users_desc) == 3
        assert users_desc[0].age >= users_desc[1].age >= users_desc[2].age

    def test_orm_query_limiting(self, test_client, test_database):
        """Test ORM query limiting"""
        # Test limit()
        limited_users = test_client.query(User).limit(2).all()
        assert len(limited_users) == 2

        # Test limit() with order_by
        top_users = test_client.query(User).order_by("age").limit(1).all()
        assert len(top_users) == 1
        assert top_users[0].age == 25  # Youngest user

    def test_orm_model_instantiation(self, test_client, test_database):
        """Test model instantiation from query results"""
        # Get a user from database
        user = test_client.query(User).filter_by(name="John Doe").first()
        assert user is not None
        assert isinstance(user, User)
        assert user.id == 1
        assert user.name == "John Doe"
        assert user.email == "john@example.com"
        assert user.age == 30

        # Test that we can access all attributes
        assert hasattr(user, 'id')
        assert hasattr(user, 'name')
        assert hasattr(user, 'email')
        assert hasattr(user, 'age')

    def test_orm_error_handling(self, test_client, test_database):
        """Test ORM error handling"""
        # Test querying non-existent table
        with pytest.raises(QueryError):
            test_client.query(User).filter("invalid_column = ?", "value").all()

        # Test invalid filter syntax
        with pytest.raises(QueryError):
            test_client.query(User).filter("invalid_syntax").all()

    def test_orm_complex_queries(self, test_client, test_database):
        """Test complex ORM queries"""
        # Test multiple filters
        result = (
            test_client.query(Product)
            .filter_by(category="Electronics")
            .filter("price > ?", 500)
            .order_by("price")
            .limit(1)
            .all()
        )

        assert len(result) == 1
        assert result[0].category == "Electronics"
        assert result[0].price > 500

        # Test chaining multiple operations
        users = test_client.query(User).filter("age > ?", 25).order_by("age").limit(2).all()

        assert len(users) == 2
        assert all(user.age > 25 for user in users)
        assert users[0].age <= users[1].age  # Should be ordered by age


if __name__ == "__main__":
    pytest.main([__file__])
