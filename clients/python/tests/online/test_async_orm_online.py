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
Online tests for Async ORM functionality - tests actual database operations
"""

import pytest
import pytest_asyncio
import asyncio
import os
import sys
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import AsyncClient
from sqlalchemy import Column, Integer, String, DECIMAL

# SQLAlchemy compatibility import
try:
    from sqlalchemy.orm import declarative_base
except ImportError:
    from matrixone.orm import declarative_base

Base = declarative_base()
from matrixone.exceptions import QueryError
from .test_config import online_config


class AsyncUser(Base):
    """Test user model for async operations"""

    __tablename__ = "test_async_users"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))
    age = Column(Integer)

    def to_dict(self):
        """Convert model instance to dictionary"""
        return {'id': self.id, 'name': self.name, 'email': self.email, 'age': self.age}

    @classmethod
    def from_dict(cls, data):
        """Create model instance from dictionary"""
        return cls(**data)


class AsyncProduct(Base):
    """Test product model for async operations"""

    __tablename__ = "test_async_products"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(DECIMAL(10, 2))
    category = Column(String(50))

    def to_dict(self):
        """Convert model instance to dictionary"""
        return {
            'id': self.id,
            'name': self.name,
            'price': float(self.price) if self.price is not None else None,
            'category': self.category,
        }

    @classmethod
    def from_dict(cls, data):
        """Create model instance from dictionary"""
        return cls(**data)


class TestAsyncORMOnline:
    """Online tests for Async ORM functionality"""

    @pytest_asyncio.fixture(scope="function")
    async def test_database(self):
        """Set up test database and return database name"""
        return "test_async_orm_db"

    @pytest_asyncio.fixture(scope="function")
    async def test_async_client(self, test_database):
        """Create and connect AsyncClient for testing"""
        host, port, user, password, _ = online_config.get_connection_params()

        # First create the database using default connection
        temp_client = AsyncClient()
        await temp_client.connect(
            host=host, port=port, user=user, password=password, database="test"
        )  # Connect to default database
        await temp_client.execute(f"CREATE DATABASE IF NOT EXISTS {test_database}")
        await temp_client.disconnect()

        # Now connect to the test database
        client = AsyncClient()
        await client.connect(host=host, port=port, user=user, password=password, database=test_database)

        try:
            # Create test tables
            await client.execute(
                """
                CREATE TABLE IF NOT EXISTS test_async_users (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    age INT
                )
            """
            )

            await client.execute(
                """
                CREATE TABLE IF NOT EXISTS test_async_products (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    price DECIMAL(10,2),
                    category VARCHAR(50)
                )
            """
            )

            # Insert test data
            await client.execute(
                """
                INSERT INTO test_async_users VALUES 
                (1, 'Alice Johnson', 'alice@example.com', 28),
                (2, 'Charlie Brown', 'charlie@example.com', 32),
                (3, 'Diana Prince', 'diana@example.com', 27)
            """
            )

            await client.execute(
                """
                INSERT INTO test_async_products VALUES 
                (1, 'Tablet', 299.99, 'Electronics'),
                (2, 'Notebook', 5.99, 'Stationery'),
                (3, 'Headphones', 149.99, 'Electronics')
            """
            )

            yield client

        finally:
            try:
                await client.disconnect()
                # Clean up database
                cleanup_client = AsyncClient()
                await cleanup_client.connect(host=host, port=port, user=user, password=password, database="test")
                await cleanup_client.execute(f"DROP DATABASE IF EXISTS {test_database}")
                await cleanup_client.disconnect()
            except Exception as e:
                print(f"Warning: Failed to cleanup or disconnect: {e}")

    @pytest.mark.asyncio
    async def test_async_orm_query_basic_operations(self, test_async_client):
        """Test basic async ORM query operations"""
        # Test all() method - like SQLAlchemy, no database parameter needed
        users = await test_async_client.query(AsyncUser).all()
        assert len(users) == 3

        # Test first() method
        first_user = await test_async_client.query(AsyncUser).first()
        assert first_user is not None
        assert isinstance(first_user, AsyncUser)

        # Test filter_by() method
        alice = await test_async_client.query(AsyncUser).filter_by(name="Alice Johnson").first()
        assert alice is not None
        assert alice.name == "Alice Johnson"
        assert alice.email == "alice@example.com"

    @pytest.mark.asyncio
    async def test_async_orm_query_filtering(self, test_async_client):
        """Test async ORM query filtering"""
        # Test filter_by with multiple conditions
        electronics = await test_async_client.query(AsyncProduct).filter_by(category="Electronics").all()
        assert len(electronics) == 2

        # Test filter() method with conditions
        expensive_products = await test_async_client.query(AsyncProduct).filter("price > ?", 100).all()
        assert len(expensive_products) == 2

        # Test combined filtering
        expensive_electronics = (
            await test_async_client.query(AsyncProduct).filter_by(category="Electronics").filter("price > ?", 100).all()
        )
        assert len(expensive_electronics) == 2

    @pytest.mark.asyncio
    async def test_async_orm_query_ordering_and_limiting(self, test_async_client):
        """Test async ORM query ordering and limiting"""
        # Test order_by
        users_by_age = await test_async_client.query(AsyncUser).order_by("age DESC").all()
        assert len(users_by_age) == 3
        # Should be ordered by age descending
        assert users_by_age[0].age >= users_by_age[1].age

        # Test limit
        limited_users = await test_async_client.query(AsyncUser).limit(2).all()
        assert len(limited_users) == 2

        # Test combined ordering and limiting
        top_products = await test_async_client.query(AsyncProduct).order_by("price DESC").limit(2).all()
        assert len(top_products) == 2
        assert top_products[0].price >= top_products[1].price

    @pytest.mark.asyncio
    async def test_async_orm_query_count(self, test_async_client):
        """Test async ORM query count functionality"""
        # Test count() method
        total_users = await test_async_client.query(AsyncUser).count()
        assert total_users == 3

        # Test count with filtering
        electronics_count = await test_async_client.query(AsyncProduct).filter_by(category="Electronics").count()
        assert electronics_count == 2

        # Test count with complex filtering
        expensive_count = await test_async_client.query(AsyncProduct).filter("price > ?", 50).count()
        assert expensive_count == 2

    @pytest.mark.asyncio
    async def test_async_orm_query_parameter_handling(self, test_async_client):
        """Test async ORM query parameter handling"""
        # Test string parameters
        user = await test_async_client.query(AsyncUser).filter("name = ?", "Charlie Brown").first()
        assert user is not None
        assert user.name == "Charlie Brown"

        # Test numeric parameters
        user = await test_async_client.query(AsyncUser).filter("age = ?", 28).first()
        assert user is not None
        assert user.age == 28

        # Test multiple parameters
        user = await test_async_client.query(AsyncUser).filter("name = ? AND age > ?", "Diana Prince", 25).first()
        assert user is not None
        assert user.name == "Diana Prince"
        assert user.age > 25

    @pytest.mark.asyncio
    async def test_async_orm_query_error_handling(self, test_async_client):
        """Test async ORM query error handling"""
        # Test query with invalid condition
        with pytest.raises(QueryError):
            await test_async_client.query(AsyncUser).filter("invalid_syntax").all()

    @pytest.mark.asyncio
    async def test_async_orm_model_serialization(self, test_async_client):
        """Test async ORM model serialization methods"""
        # Test model creation and serialization
        user = AsyncUser(id=1, name="Test User", email="test@example.com", age=30)

        # Test to_dict
        user_dict = user.to_dict()
        assert isinstance(user_dict, dict)
        assert user_dict['id'] == 1
        assert user_dict['name'] == "Test User"

        # Test from_dict
        new_user = AsyncUser.from_dict(user_dict)
        assert new_user.id == user.id
        assert new_user.name == user.name
        assert new_user.email == user.email
        assert new_user.age == user.age

    @pytest.mark.asyncio
    async def test_async_orm_concurrent_operations(self, test_async_client):
        """Test async ORM concurrent operations"""
        # Test concurrent queries
        tasks = [
            test_async_client.query(AsyncUser).filter_by(name="Alice Johnson").first(),
            test_async_client.query(AsyncUser).filter_by(name="Charlie Brown").first(),
            test_async_client.query(AsyncUser).filter_by(name="Diana Prince").first(),
        ]

        results = await asyncio.gather(*tasks)

        # All queries should complete successfully
        assert len(results) == 3
        for result in results:
            assert result is not None
            assert isinstance(result, AsyncUser)


if __name__ == '__main__':
    pytest.main([__file__])
