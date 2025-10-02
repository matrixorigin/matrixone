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
Test MatrixOneQuery and AsyncMatrixOneQuery ORM functionality.
Comprehensive online tests for SQLAlchemy-style ORM operations.
"""

import pytest
import pytest_asyncio
import asyncio
import sys
import os
from datetime import datetime
from typing import List, Optional

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from matrixone import Client, AsyncClient, SnapshotManager, SnapshotLevel
from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP
from matrixone.orm import desc, declarative_base

Base = declarative_base()
from .test_config import online_config


# Define test models
class User(Base):
    """User model for testing"""

    __tablename__ = "test_users"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))
    age = Column(Integer)
    department = Column(String(50))
    salary = Column(DECIMAL(10, 2))
    created_at = Column(TIMESTAMP)


class Product(Base):
    """Product model for testing"""

    __tablename__ = "test_products"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(DECIMAL(10, 2))
    category = Column(String(50))
    stock = Column(Integer)
    created_at = Column(TIMESTAMP)


class Order(Base):
    """Order model for testing"""

    __tablename__ = "test_orders"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    product_id = Column(Integer)
    quantity = Column(Integer)
    total_amount = Column(DECIMAL(10, 2))
    order_date = Column(TIMESTAMP)


class TestMatrixOneQueryORM:
    """Test MatrixOneQuery ORM functionality"""

    @pytest.fixture(scope="class")
    def client(self):
        """Create and connect MatrixOne client."""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(host=host, port=port, user=user, password=password, database=database)
        yield client
        if hasattr(client, 'disconnect'):
            client.disconnect()

    @pytest.fixture(scope="class")
    def test_data_setup(self, client):
        """Set up test data"""
        # Clean up any existing test data
        try:
            client.execute("DROP TABLE IF EXISTS test_orders")
            client.execute("DROP TABLE IF EXISTS test_products")
            client.execute("DROP TABLE IF EXISTS test_users")
        except:
            pass

        # Create test tables
        client.execute(
            """
            CREATE TABLE test_users (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INT,
                department VARCHAR(50),
                salary DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        client.execute(
            """
            CREATE TABLE test_products (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10,2),
                category VARCHAR(50),
                stock INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        client.execute(
            """
            CREATE TABLE test_orders (
                id INT PRIMARY KEY,
                user_id INT,
                product_id INT,
                quantity INT,
                total_amount DECIMAL(10,2),
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert test data
        client.execute(
            """
            INSERT INTO test_users (id, name, email, age, department, salary) VALUES
            (1, 'Alice Johnson', 'alice@example.com', 28, 'Engineering', 75000.00),
            (2, 'Bob Smith', 'bob@example.com', 32, 'Marketing', 65000.00),
            (3, 'Charlie Brown', 'charlie@example.com', 25, 'Engineering', 70000.00),
            (4, 'Diana Prince', 'diana@example.com', 30, 'Sales', 60000.00),
            (5, 'Eve Wilson', 'eve@example.com', 27, 'Engineering', 80000.00)
        """
        )

        client.execute(
            """
            INSERT INTO test_products (id, name, price, category, stock) VALUES
            (1, 'Laptop Pro', 1299.99, 'Electronics', 50),
            (2, 'Wireless Mouse', 29.99, 'Electronics', 200),
            (3, 'Office Chair', 199.99, 'Furniture', 30),
            (4, 'Coffee Mug', 12.99, 'Accessories', 100),
            (5, 'Monitor 4K', 399.99, 'Electronics', 25)
        """
        )

        client.execute(
            """
            INSERT INTO test_orders (id, user_id, product_id, quantity, total_amount) VALUES
            (1, 1, 1, 1, 1299.99),
            (2, 2, 2, 2, 59.98),
            (3, 3, 3, 1, 199.99),
            (4, 1, 4, 3, 38.97),
            (5, 4, 5, 1, 399.99)
        """
        )

        yield

        # Cleanup
        try:
            client.execute("DROP TABLE test_orders")
            client.execute("DROP TABLE test_products")
            client.execute("DROP TABLE test_users")
        except:
            pass

    def test_basic_query_all(self, client, test_data_setup):
        """Test basic query all functionality"""
        users = client.query(User).all()
        assert len(users) == 5
        assert users[0].name == 'Alice Johnson'
        assert users[0].email == 'alice@example.com'

    def test_filter_by(self, client, test_data_setup):
        """Test filter_by functionality"""
        # Test single filter
        engineering_users = client.query(User).filter_by(department='Engineering').all()
        assert len(engineering_users) == 3
        assert all(user.department == 'Engineering' for user in engineering_users)

        # Test multiple filters
        young_engineers = client.query(User).filter_by(department='Engineering', age=25).all()
        assert len(young_engineers) == 1
        assert young_engineers[0].name == 'Charlie Brown'

    def test_filter(self, client, test_data_setup):
        """Test filter functionality with custom conditions"""
        # Test numeric comparison
        high_salary_users = client.query(User).filter("salary > ?", 70000.0).all()
        assert len(high_salary_users) == 2
        assert all(user.salary > 70000.0 for user in high_salary_users)

        # Test string comparison
        electronics_products = client.query(Product).filter("category = ?", 'Electronics').all()
        assert len(electronics_products) == 3
        assert all(product.category == 'Electronics' for product in electronics_products)

    def test_order_by(self, client, test_data_setup):
        """Test order_by functionality"""
        # Test ascending order
        users_by_age = client.query(User).order_by('age').all()
        assert users_by_age[0].age == 25
        assert users_by_age[-1].age == 32

        # Test descending order
        users_by_salary_desc = client.query(User).order_by(desc('salary')).all()
        assert users_by_salary_desc[0].salary == 80000.0
        assert users_by_salary_desc[-1].salary == 60000.0

    def test_limit(self, client, test_data_setup):
        """Test limit functionality"""
        top_users = client.query(User).order_by(desc('salary')).limit(3).all()
        assert len(top_users) == 3
        assert top_users[0].salary == 80000.0

    def test_offset(self, client, test_data_setup):
        """Test offset functionality"""
        # MatrixOne requires LIMIT with OFFSET, so we use a large limit
        users_skip_first = client.query(User).order_by('id').limit(100).offset(2).all()
        assert len(users_skip_first) == 3
        assert users_skip_first[0].id == 3

    def test_limit_offset_combination(self, client, test_data_setup):
        """Test limit and offset combination"""
        users_page2 = client.query(User).order_by('id').limit(2).offset(2).all()
        assert len(users_page2) == 2
        assert users_page2[0].id == 3
        assert users_page2[1].id == 4

    def test_count(self, client, test_data_setup):
        """Test count functionality"""
        total_users = client.query(User).count()
        assert total_users == 5

        engineering_count = client.query(User).filter_by(department='Engineering').count()
        assert engineering_count == 3

    def test_first(self, client, test_data_setup):
        """Test first functionality"""
        first_user = client.query(User).order_by('id').first()
        assert first_user is not None
        assert first_user.id == 1
        assert first_user.name == 'Alice Johnson'

        # Test first with filter
        engineering_user = client.query(User).filter_by(department='Engineering').first()
        assert engineering_user is not None
        assert engineering_user.department == 'Engineering'

    def test_complex_queries(self, client, test_data_setup):
        """Test complex query combinations"""
        # Complex query: high-salary engineering users ordered by age
        result = client.query(User).filter_by(department='Engineering').filter("salary > ?", 70000.0).order_by('age').all()

        assert len(result) == 2
        assert result[0].name == 'Eve Wilson'  # age 27 (younger of the two)
        assert result[1].name == 'Alice Johnson'  # age 28 (older of the two)

    def test_model_attributes(self, client, test_data_setup):
        """Test model attribute access"""
        user = client.query(User).first()
        assert hasattr(user, 'id')
        assert hasattr(user, 'name')
        assert hasattr(user, 'email')
        assert hasattr(user, 'age')
        assert hasattr(user, 'department')
        assert hasattr(user, 'salary')
        assert hasattr(user, 'created_at')

    def test_empty_results(self, client, test_data_setup):
        """Test queries that return empty results"""
        # Non-existent department
        empty_result = client.query(User).filter_by(department='NonExistent').all()
        assert len(empty_result) == 0

        # Non-existent user
        empty_first = client.query(User).filter_by(department='NonExistent').first()
        assert empty_first is None

        # Count of non-existent records
        empty_count = client.query(User).filter_by(department='NonExistent').count()
        assert empty_count == 0


# Define a global AsyncUser class for all async tests
class AsyncUser(Base):
    __tablename__ = "async_test_users"
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))
    age = Column(Integer)
    department = Column(String(50))
    salary = Column(DECIMAL(10, 2))
    created_at = Column(TIMESTAMP)


# Define a global TestModel class for edge case tests
class TempTable1(Base):
    __tablename__ = "test_malformed_filter"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))


class TestAsyncMatrixOneQueryORM:
    """Test AsyncMatrixOneQuery ORM functionality"""

    @pytest_asyncio.fixture(scope="function")
    async def async_client(self):
        """Create and connect async MatrixOne client."""
        client = AsyncClient()
        host, port, user, password, database = online_config.get_connection_params()
        await client.connect(host=host, port=port, user=user, password=password, database=database)
        yield client
        if hasattr(client, 'disconnect'):
            await client.disconnect()

    @pytest_asyncio.fixture(scope="function")
    async def async_test_data_setup(self, async_client):
        """Set up test data for async tests"""
        # Clean up any existing test data
        try:
            await async_client.execute("DROP TABLE IF EXISTS async_test_orders")
            await async_client.execute("DROP TABLE IF EXISTS async_test_products")
            await async_client.execute("DROP TABLE IF EXISTS async_test_users")
        except:
            pass

        # Create test tables
        await async_client.execute(
            """
            CREATE TABLE async_test_users (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INT,
                department VARCHAR(50),
                salary DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        await async_client.execute(
            """
            CREATE TABLE async_test_products (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10,2),
                category VARCHAR(50),
                stock INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert test data
        await async_client.execute(
            """
            INSERT INTO async_test_users (id, name, email, age, department, salary) VALUES
            (1, 'Async Alice', 'async.alice@example.com', 28, 'Engineering', 75000.00),
            (2, 'Async Bob', 'async.bob@example.com', 32, 'Marketing', 65000.00),
            (3, 'Async Charlie', 'async.charlie@example.com', 25, 'Engineering', 70000.00),
            (4, 'Async Diana', 'async.diana@example.com', 30, 'Sales', 60000.00),
            (5, 'Async Eve', 'async.eve@example.com', 27, 'Engineering', 80000.00)
        """
        )

        await async_client.execute(
            """
            INSERT INTO async_test_products (id, name, price, category, stock) VALUES
            (1, 'Async Laptop', 1299.99, 'Electronics', 50),
            (2, 'Async Mouse', 29.99, 'Electronics', 200),
            (3, 'Async Chair', 199.99, 'Furniture', 30),
            (4, 'Async Mug', 12.99, 'Accessories', 100),
            (5, 'Async Monitor', 399.99, 'Electronics', 25)
        """
        )

        yield

        # Cleanup
        try:
            await async_client.execute("DROP TABLE async_test_products")
            await async_client.execute("DROP TABLE async_test_users")
        except:
            pass

    @pytest.mark.asyncio
    async def test_async_basic_query_all(self, async_client, async_test_data_setup):
        """Test async basic query all functionality"""
        users = await async_client.query(AsyncUser).all()
        assert len(users) == 5
        assert users[0].name == 'Async Alice'

    @pytest.mark.asyncio
    async def test_async_filter_by(self, async_client, async_test_data_setup):
        """Test async filter_by functionality"""
        engineering_users = await async_client.query(AsyncUser).filter_by(department='Engineering').all()
        assert len(engineering_users) == 3
        assert all(user.department == 'Engineering' for user in engineering_users)

    @pytest.mark.asyncio
    async def test_async_filter(self, async_client, async_test_data_setup):
        """Test async filter functionality"""
        high_salary_users = await async_client.query(AsyncUser).filter("salary > ?", 70000.0).all()
        assert len(high_salary_users) == 2
        assert all(user.salary > 70000.0 for user in high_salary_users)

    @pytest.mark.asyncio
    async def test_async_order_by(self, async_client, async_test_data_setup):
        """Test async order_by functionality"""
        users_by_salary_desc = await async_client.query(AsyncUser).order_by(desc('salary')).all()
        assert users_by_salary_desc[0].salary == 80000.0
        assert users_by_salary_desc[-1].salary == 60000.0

    @pytest.mark.asyncio
    async def test_async_limit_offset(self, async_client, async_test_data_setup):
        """Test async limit and offset functionality"""
        top_users = await async_client.query(AsyncUser).order_by(desc('salary')).limit(3).all()
        assert len(top_users) == 3
        assert top_users[0].salary == 80000.0

        users_skip_first = await async_client.query(AsyncUser).order_by('id').limit(100).offset(2).all()
        assert len(users_skip_first) == 3
        assert users_skip_first[0].id == 3

    @pytest.mark.asyncio
    async def test_async_count(self, async_client, async_test_data_setup):
        """Test async count functionality"""
        total_users = await async_client.query(AsyncUser).count()
        assert total_users == 5

        engineering_count = await async_client.query(AsyncUser).filter_by(department='Engineering').count()
        assert engineering_count == 3

    @pytest.mark.asyncio
    async def test_async_first(self, async_client, async_test_data_setup):
        """Test async first functionality"""
        first_user = await async_client.query(AsyncUser).order_by('id').first()
        assert first_user is not None
        assert first_user.id == 1
        assert first_user.name == 'Async Alice'

    @pytest.mark.asyncio
    async def test_async_complex_queries(self, async_client, async_test_data_setup):
        """Test async complex query combinations"""
        result = (
            await async_client.query(AsyncUser)
            .filter_by(department='Engineering')
            .filter("salary > ?", 70000.0)
            .order_by('age')
            .all()
        )

        assert len(result) == 2
        assert result[0].name == 'Async Eve'  # age 27 (younger of the two)
        assert result[1].name == 'Async Alice'  # age 28 (older of the two)

    @pytest.mark.asyncio
    async def test_async_empty_results(self, async_client, async_test_data_setup):
        """Test async queries that return empty results"""
        # Non-existent department
        empty_result = await async_client.query(AsyncUser).filter_by(department='NonExistent').all()
        assert len(empty_result) == 0

        # Non-existent user
        empty_first = await async_client.query(AsyncUser).filter_by(department='NonExistent').first()
        assert empty_first is None

        # Count of non-existent records
        empty_count = await async_client.query(AsyncUser).filter_by(department='NonExistent').count()
        assert empty_count == 0


class TestMatrixOneQueryEdgeCases:
    """Test edge cases and error conditions"""

    @pytest.fixture(scope="class")
    def client(self):
        """Create and connect MatrixOne client."""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(host=host, port=port, user=user, password=password, database=database)
        yield client
        if hasattr(client, 'disconnect'):
            client.disconnect()

    def test_invalid_model(self, client):
        """Test query with invalid model"""

        class InvalidModel(Base):
            __tablename__ = "non_existent_table"
            id = Column(Integer, primary_key=True)

        with pytest.raises(Exception):
            client.query(InvalidModel).all()

    def test_malformed_filter(self, client):
        """Test malformed filter conditions"""
        # Create a simple test table first
        try:
            client.execute("DROP TABLE IF EXISTS test_malformed")
            client.execute("CREATE TABLE test_malformed (id INT PRIMARY KEY, name VARCHAR(50))")
            client.execute("INSERT INTO test_malformed (id, name) VALUES (1, 'test')")

            # This should raise an exception due to invalid SQL syntax
            with pytest.raises(Exception):
                client.query(TempTable1).filter("invalid_syntax").all()

        finally:
            try:
                client.execute("DROP TABLE test_malformed")
            except:
                pass


if __name__ == "__main__":
    pytest.main([__file__])
