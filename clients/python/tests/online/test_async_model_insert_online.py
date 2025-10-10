"""
Online tests for async model-based insert operations
"""

import pytest
import pytest_asyncio
import os
import sys
import asyncio
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import AsyncClient
from matrixone.orm import declarative_base
from matrixone.exceptions import QueryError
from sqlalchemy import Column, Integer, String, Text, DECIMAL, DateTime
from .test_config import online_config

Base = declarative_base()


class AsyncUser(Base):
    __tablename__ = 'test_async_insert_users'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    email = Column(String(100))
    age = Column(Integer)
    created_at = Column(DateTime)


class AsyncProduct(Base):
    __tablename__ = 'test_async_insert_products'
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    description = Column(Text)
    price = Column(DECIMAL(10, 2))
    category = Column(String(50))
    in_stock = Column(Integer)


class AsyncOrder(Base):
    __tablename__ = 'test_async_insert_orders'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    product_id = Column(Integer)
    quantity = Column(Integer)
    total_amount = Column(DECIMAL(10, 2))
    order_date = Column(DateTime)


class TestAsyncModelInsert:
    """Test async model-based insert operations"""

    async def _setup_test_environment(self):
        """Set up test environment for each test"""
        host, port, user, password, database = online_config.get_connection_params()
        client = AsyncClient()
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        test_db = "test_async_model_insert_db"
        await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        await client.execute(f"USE {test_db}")

        # Create test tables using models
        await client.create_table(AsyncUser)
        await client.create_table(AsyncProduct)
        await client.create_table(AsyncOrder)

        return client, test_db

    async def _cleanup_test_environment(self, client, test_db):
        """Clean up test environment"""
        try:
            await client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            await client.disconnect()
        except Exception as e:
            print(f"Cleanup failed: {e}")

    @pytest.mark.asyncio
    async def test_async_single_insert_with_model(self):
        """Test inserting a single record using model class with async client"""
        client, test_db = await self._setup_test_environment()
        try:
            # Insert a single user using model class
            result = await client.insert(
                AsyncUser,
                {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30, 'created_at': datetime.now()},
            )

            assert result.affected_rows == 1

            # Verify the data was inserted correctly
            users = await client.query(AsyncUser).all()
            assert len(users) == 1

            user = users[0]
            assert user.name == 'John Doe'
            assert user.email == 'john@example.com'
            assert user.age == 30

        finally:
            await self._cleanup_test_environment(client, test_db)

    @pytest.mark.asyncio
    async def test_async_batch_insert_with_model(self):
        """Test batch inserting multiple records using model class with async client"""
        client, test_db = await self._setup_test_environment()
        try:
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

            result = await client.batch_insert(AsyncProduct, products_data)
            assert result.affected_rows == 3

            # Verify the data was inserted correctly
            products = await client.query(AsyncProduct).all()
            assert len(products) == 3

            # Check specific products
            laptop = await client.query(AsyncProduct).filter_by(name='Laptop').first()
            assert laptop is not None
            assert float(laptop.price) == 999.99
            assert laptop.category == 'Electronics'
            assert laptop.in_stock == 10

        finally:
            await self._cleanup_test_environment(client, test_db)

    @pytest.mark.asyncio
    async def test_async_insert_with_null_values(self):
        """Test inserting records with NULL values using model class with async client"""
        client, test_db = await self._setup_test_environment()
        try:
            # Insert a user with some NULL values
            result = await client.insert(
                AsyncUser,
                {'id': 2, 'name': 'Jane Smith', 'email': None, 'age': 25, 'created_at': None},  # NULL value  # NULL value
            )

            assert result.affected_rows == 1

            # Verify the data was inserted correctly
            user = await client.query(AsyncUser).filter_by(id=2).first()
            assert user is not None
            assert user.name == 'Jane Smith'
            assert user.email is None
            assert user.age == 25
            assert user.created_at is None

        finally:
            await self._cleanup_test_environment(client, test_db)

    @pytest.mark.asyncio
    async def test_async_insert_with_different_data_types(self):
        """Test inserting records with different data types using model class with async client"""
        client, test_db = await self._setup_test_environment()
        try:
            # Insert an order with various data types
            result = await client.insert(
                AsyncOrder,
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
            order = await client.query(AsyncOrder).filter_by(id=1).first()
            assert order is not None
            assert order.user_id == 1
            assert order.product_id == 1
            assert order.quantity == 2
            assert float(order.total_amount) == 1999.98
            assert order.order_date == datetime(2024, 1, 15, 10, 30, 0)

        finally:
            await self._cleanup_test_environment(client, test_db)

    @pytest.mark.asyncio
    async def test_async_insert_error_handling(self):
        """Test error handling when inserting invalid data using model class with async client"""
        client, test_db = await self._setup_test_environment()
        try:
            # First insert a user
            await client.insert(AsyncUser, {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30})

            # Try to insert a user with duplicate primary key
            with pytest.raises(QueryError):
                await client.insert(
                    AsyncUser,
                    {
                        'id': 1,  # Duplicate primary key
                        'name': 'Duplicate User',
                        'email': 'duplicate@example.com',
                        'age': 35,
                    },
                )

        finally:
            await self._cleanup_test_environment(client, test_db)

    @pytest.mark.asyncio
    async def test_async_insert_vs_string_table_name(self):
        """Test that both model class and string table name work the same way with async client"""
        client, test_db = await self._setup_test_environment()
        try:
            # Insert using model class
            result1 = await client.insert(
                AsyncUser, {'id': 3, 'name': 'Model User', 'email': 'model@example.com', 'age': 28}
            )

            # Insert using string table name
            result2 = await client.insert(
                'test_async_insert_users', {'id': 4, 'name': 'String User', 'email': 'string@example.com', 'age': 32}
            )

            assert result1.affected_rows == 1
            assert result2.affected_rows == 1

            # Verify both records were inserted
            users = await client.query(AsyncUser).all()
            assert len(users) == 2

            # Check specific users
            model_user = await client.query(AsyncUser).filter_by(name='Model User').first()
            string_user = await client.query(AsyncUser).filter_by(name='String User').first()

            assert model_user is not None
            assert string_user is not None
            assert model_user.email == 'model@example.com'
            assert string_user.email == 'string@example.com'

        finally:
            await self._cleanup_test_environment(client, test_db)

    @pytest.mark.asyncio
    async def test_async_transaction_insert_with_model(self):
        """Test inserting records within a transaction using model class with async client"""
        client, test_db = await self._setup_test_environment()
        try:
            async with client.transaction() as tx:
                # Insert a user within transaction
                result = await tx.insert(
                    AsyncUser, {'id': 100, 'name': 'Transaction User', 'email': 'tx@example.com', 'age': 45}
                )

                assert result.affected_rows == 1

                # Verify within transaction
                user = await tx.query(AsyncUser).filter_by(id=100).first()
                assert user is not None
                assert user.name == 'Transaction User'

            # Verify after commit
            user_after_commit = await client.query(AsyncUser).filter_by(id=100).first()
            assert user_after_commit is not None
            assert user_after_commit.name == 'Transaction User'

        finally:
            await self._cleanup_test_environment(client, test_db)
