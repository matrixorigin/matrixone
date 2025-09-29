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
Online tests for query update functionality
"""

import pytest
import os
import sys
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP, func

Base = declarative_base()

try:
    from .test_config import online_config
except ImportError:
    # Fallback for when running as standalone script
    import test_config

    online_config = test_config.online_config


class User(Base):
    """User model for testing update operations"""

    __tablename__ = "test_users_update"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))
    age = Column(Integer)
    salary = Column(DECIMAL(10, 2))
    status = Column(String(50))
    login_count = Column(Integer, default=0)
    last_login = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())


class Product(Base):
    """Product model for testing update operations"""

    __tablename__ = "test_products_update"

    id = Column(Integer, primary_key=True)
    name = Column(String(200))
    price = Column(DECIMAL(10, 2))
    category = Column(String(50))
    stock = Column(Integer)
    active = Column(Integer, default=1)  # Using Integer for boolean-like field


class TestQueryUpdateOnline:
    """Online tests for query update functionality"""

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
        test_db = "test_query_update_db"

        # Create test database
        test_client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        test_client.execute(f"USE {test_db}")

        # Create tables using ORM
        test_client.create_all(Base)

        try:
            yield test_db
        finally:
            # Clean up
            try:
                test_client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            except Exception as e:
                print(f"Warning: Failed to drop test database: {e}")

    @pytest.fixture(autouse=True, scope="class")
    def setup_test_data(self, test_client, test_database):
        """Set up test data before each test"""
        # Ensure we're using the test database
        test_client.execute(f"USE {test_database}")

        # Clear existing data (ignore errors if tables don't exist)
        try:
            test_client.query(User).delete()
        except Exception:
            pass
        try:
            test_client.query(Product).delete()
        except Exception:
            pass

        # Insert test users
        users_data = [
            {
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "age": 25,
                "salary": 50000.00,
                "status": "active",
                "login_count": 5,
            },
            {
                "id": 2,
                "name": "Bob",
                "email": "bob@example.com",
                "age": 30,
                "salary": 60000.00,
                "status": "active",
                "login_count": 10,
            },
            {
                "id": 3,
                "name": "Charlie",
                "email": "charlie@example.com",
                "age": 35,
                "salary": 70000.00,
                "status": "inactive",
                "login_count": 2,
            },
            {
                "id": 4,
                "name": "David",
                "email": "david@example.com",
                "age": 28,
                "salary": 55000.00,
                "status": "active",
                "login_count": 8,
            },
        ]
        test_client.batch_insert("test_users_update", users_data)

        # Insert test products
        products_data = [
            {"id": 1, "name": "Laptop", "price": 999.99, "category": "Electronics", "stock": 10, "active": 1},
            {"id": 2, "name": "Phone", "price": 699.99, "category": "Electronics", "stock": 20, "active": 1},
            {"id": 3, "name": "Book", "price": 29.99, "category": "Education", "stock": 50, "active": 0},
            {"id": 4, "name": "Tablet", "price": 499.99, "category": "Electronics", "stock": 15, "active": 1},
        ]
        test_client.batch_insert("test_products_update", products_data)

    def test_simple_update_single_record(self, test_client):
        """Test updating a single record with simple values"""
        # Update Alice's name and email
        query = test_client.query(User)
        result = query.update(name="Alice Updated", email="alice.updated@example.com").filter(User.id == 1).execute()

        # Verify the update
        updated_user = test_client.query(User).filter(User.id == 1).first()
        assert updated_user.name == "Alice Updated"
        assert updated_user.email == "alice.updated@example.com"
        assert updated_user.age == 25  # Should remain unchanged

    def test_update_multiple_records_with_condition(self, test_client):
        """Test updating multiple records with a condition"""
        # Update all active users' status to "premium"
        query = test_client.query(User)
        result = query.update(status="premium").filter(User.status == "active").execute()

        # Verify the updates
        premium_users = test_client.query(User).filter(User.status == "premium").all()
        assert len(premium_users) == 3  # Alice, Bob, and David

        # Check that inactive user (Charlie) was not updated
        charlie = test_client.query(User).filter(User.id == 3).first()
        assert charlie.status == "inactive"

    def test_update_with_sqlalchemy_expressions(self, test_client):
        """Test updating with SQLAlchemy expressions"""
        # Update login count using SQLAlchemy expression
        query = test_client.query(User)
        result = query.update(login_count=User.login_count + 1).filter(User.id == 1).execute()

        # Verify the update
        updated_user = test_client.query(User).filter(User.id == 1).first()
        assert updated_user.login_count == 6  # Was 5, now 6

    def test_update_with_complex_expressions(self, test_client):
        """Test updating with complex SQLAlchemy expressions"""
        # Update salary with a percentage increase
        query = test_client.query(User)
        result = query.update(salary=User.salary * 1.1).filter(User.id == 2).execute()

        # Verify the update
        updated_user = test_client.query(User).filter(User.id == 2).first()
        assert updated_user.salary == 66000.00  # 60000 * 1.1

    def test_update_with_multiple_conditions(self, test_client, test_database):
        """Test updating with multiple filter conditions"""
        # Ensure we're using the test database
        test_client.execute(f"USE {test_database}")

        # Create test records specifically for this test
        test_client.execute(
            """
            INSERT INTO test_users_update (id, name, email, age, salary, status, login_count) 
            VALUES (200, 'Test User 1', 'test1@example.com', 25, 50000, 'active', 10),
                   (201, 'Test User 2', 'test2@example.com', 30, 60000, 'active', 5),
                   (202, 'Test User 3', 'test3@example.com', 35, 70000, 'active', 8)
        """
        )

        # Update users who are active and have login_count > 5
        query = test_client.query(User)
        result = query.update(status="vip").filter(User.status == "active").filter(User.login_count > 5).execute()

        # Verify the updates
        vip_users = test_client.query(User).filter(User.status == "vip").all()
        assert len(vip_users) == 2  # User 1 (login_count=10) and User 3 (login_count=8)

        # Check that User 2 (login_count=5) was not updated
        user2 = test_client.query(User).filter(User.id == 201).first()
        assert user2.status == "active"

        # Clean up
        test_client.query(User).filter(User.id.in_([200, 201, 202])).delete()

    def test_update_with_none_values(self, test_client):
        """Test updating with None values"""
        # Update Charlie's last_login to None
        query = test_client.query(User)
        result = query.update(last_login=None).filter(User.id == 3).execute()

        # Verify the update
        updated_user = test_client.query(User).filter(User.id == 3).first()
        assert updated_user.last_login is None

    def test_update_with_numeric_values(self, test_client):
        """Test updating with numeric values"""
        # Update product price
        query = test_client.query(Product)
        result = query.update(price=1099.99).filter(Product.id == 1).execute()

        # Verify the update
        updated_product = test_client.query(Product).filter(Product.id == 1).first()
        assert float(updated_product.price) == 1099.99

    def test_update_with_boolean_like_values(self, test_client):
        """Test updating with boolean-like values"""
        # Update product active status
        query = test_client.query(Product)
        result = query.update(active=0).filter(Product.id == 2).execute()

        # Verify the update
        updated_product = test_client.query(Product).filter(Product.id == 2).first()
        assert updated_product.active == 0

    def test_update_with_string_conditions(self, test_client):
        """Test updating with string-based filter conditions"""
        # Update products in Electronics category
        query = test_client.query(Product)
        result = query.update(stock=Product.stock + 5).filter("category = ?", "Electronics").execute()

        # Verify the updates
        electronics_products = test_client.query(Product).filter("category = ?", "Electronics").all()
        for product in electronics_products:
            if product.id == 1:  # Laptop
                assert product.stock == 15  # Was 10, now 15
            elif product.id == 2:  # Phone
                assert product.stock == 25  # Was 20, now 25
            elif product.id == 4:  # Tablet
                assert product.stock == 20  # Was 15, now 20

    def test_update_with_mixed_conditions(self, test_client):
        """Test updating with mixed SQLAlchemy and string conditions"""
        # Update users with mixed conditions
        query = test_client.query(User)
        result = query.update(age=User.age + 1).filter(User.status == "active").filter("age < ?", 30).execute()

        # Verify the updates
        young_active_users = test_client.query(User).filter(User.status == "active").filter("age < ?", 30).all()
        for user in young_active_users:
            if user.id == 1:  # Alice, was 25, now 26
                assert user.age == 26
            elif user.id == 4:  # David, was 28, now 29
                assert user.age == 29

    def test_update_no_matching_records(self, test_client):
        """Test updating when no records match the condition"""
        # Try to update a non-existent user
        query = test_client.query(User)
        result = query.update(name="Non-existent").filter(User.id == 999).execute()

        # Verify no records were updated
        non_existent = test_client.query(User).filter(User.id == 999).first()
        assert non_existent is None

    def test_update_with_transaction(self, test_client, test_database):
        """Test updating within a transaction"""
        # Ensure we're using the test database
        test_client.execute(f"USE {test_database}")

        # Create a test record specifically for this test
        test_client.execute(
            """
            INSERT INTO test_users_update (id, name, email, age, salary, status, login_count) 
            VALUES (100, 'Transaction Test User', 'transaction@test.com', 25, 50000, 'active', 0)
        """
        )

        with test_client.transaction() as tx:
            # Ensure we're using the test database in transaction context
            test_client.execute(f"USE {test_database}")
            # Update the test record in a transaction
            query = test_client.query(User)
            query.update(status="transaction_committed").filter(User.id == 100).execute()

        # Verify the update was committed
        updated_user = test_client.query(User).filter(User.id == 100).first()
        assert updated_user is not None
        assert updated_user.status == "transaction_committed"

        # Clean up
        test_client.query(User).filter(User.id == 100).delete()

    def test_update_with_rollback(self, test_client, test_database):
        """Test updating with transaction rollback"""
        # Ensure we're using the test database
        test_client.execute(f"USE {test_database}")

        # Create a test record specifically for this test
        test_client.execute(
            """
            INSERT INTO test_users_update (id, name, email, age, salary, status, login_count) 
            VALUES (101, 'Rollback Test User', 'rollback@test.com', 30, 60000, 'original_status', 0)
        """
        )

        # Verify initial state
        user = test_client.query(User).filter(User.id == 101).first()
        assert user is not None
        assert user.status == "original_status"

        # Test transaction rollback
        try:
            with test_client.transaction() as tx:
                # Ensure we're using the test database in transaction context
                tx.execute(f"USE {test_database}")
                # Update the test record using transaction connection
                tx.execute("UPDATE test_users_update SET status = 'rollback_test' WHERE id = 101")

                # Verify the update happened within the transaction
                result = tx.execute("SELECT status FROM test_users_update WHERE id = 101")
                user_in_tx = result.fetchone()
                assert user_in_tx[0] == "rollback_test"

                # Force rollback by raising an exception
                raise Exception("Force rollback")
        except Exception:
            pass

        # Verify the update was rolled back - status should be back to "original_status"
        user = test_client.query(User).filter(User.id == 101).first()
        assert user is not None
        assert user.status == "original_status"

        # Clean up
        test_client.query(User).filter(User.id == 101).delete()

    def test_update_with_explain(self, test_client):
        """Test explaining an update query"""
        query = test_client.query(User)
        explain_result = query.update(name="Explain Test").filter(User.id == 1).explain()

        # Verify explain returns a result
        assert explain_result is not None
        assert len(explain_result.rows) > 0

    def test_update_with_to_sql(self, test_client):
        """Test generating SQL for an update query"""
        query = test_client.query(User)
        sql = query.update(name="SQL Test", email="sql@example.com").filter(User.id == 1).to_sql()

        # Verify SQL structure
        assert "UPDATE test_users_update SET" in sql
        assert "name = 'SQL Test'" in sql
        assert "email = 'sql@example.com'" in sql
        assert "WHERE id = 1" in sql

    def test_update_with_explain_analyze(self, test_client):
        """Test explaining and analyzing an update query"""
        query = test_client.query(User)
        explain_result = query.update(name="Explain Analyze Test").filter(User.id == 1).explain_analyze()

        # Verify explain analyze returns a result
        assert explain_result is not None
        assert len(explain_result.rows) > 0

    def test_update_with_verbose_explain(self, test_client):
        """Test verbose explain for an update query"""
        query = test_client.query(User)
        explain_result = query.update(name="Verbose Explain Test").filter(User.id == 1).explain(verbose=True)

        # Verify verbose explain returns a result
        assert explain_result is not None
        assert len(explain_result.rows) > 0


if __name__ == "__main__":
    pytest.main([__file__])
