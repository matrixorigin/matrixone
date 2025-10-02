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
Online tests for PubSub operations

These tests are inspired by example_08_pubsub_operations.py
"""

import pytest
import time
from datetime import datetime
from matrixone import Client, AsyncClient
from matrixone.logger import create_default_logger
from .test_config import online_config


@pytest.mark.online
class TestPubSubOperations:
    """Test PubSub operations functionality"""

    def test_basic_pubsub_operations(self, test_client):
        """Test basic PubSub operations within the same account"""
        # Create test database and tables
        test_client.execute("CREATE DATABASE IF NOT EXISTS pubsub_test")
        test_client.execute("USE pubsub_test")

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2),
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_id INT,
                quantity INT,
                total_price DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        try:
            # Test 1: Basic PubSub setup (as per examples)
            # Insert sample data
            products_data = [
                ("Laptop", 999.99, "Electronics"),
                ("Mouse", 29.99, "Electronics"),
                ("Keyboard", 79.99, "Electronics"),
                ("Monitor", 299.99, "Electronics"),
            ]

            for name, price, category in products_data:
                test_client.execute(f"INSERT INTO products (name, price, category) VALUES ('{name}', {price}, '{category}')")

            # Test 2: List publications (as per examples - should work even if empty)
            publications = test_client.pubsub.list_publications()
            assert isinstance(publications, list)

            # Test 3: List subscriptions (as per examples - should work even if empty)
            subscriptions = test_client.pubsub.list_subscriptions()
            assert isinstance(subscriptions, list)

            # Test 4: Data operations
            test_client.execute("INSERT INTO orders (product_id, quantity, total_price) VALUES (1, 2, 199.98)")

            # Verify data
            result = test_client.execute("SELECT COUNT(*) FROM products")
            assert result.rows[0][0] == 4

            result = test_client.execute("SELECT COUNT(*) FROM orders")
            assert result.rows[0][0] == 1

        finally:
            # Cleanup
            try:
                test_client.pubsub.drop_subscription("product_subscription")
                test_client.pubsub.drop_publication("product_updates")
            except Exception as e:
                print(f"Warning: Failed to cleanup pubsub resources: {e}")
                # Don't ignore - this could indicate resource leaks

            test_client.execute("DROP DATABASE IF EXISTS pubsub_test")

    def test_pubsub_with_multiple_tables(self, test_client):
        """Test PubSub with multiple tables"""
        # Create test database
        test_client.execute("CREATE DATABASE IF NOT EXISTS pubsub_multi_test")
        test_client.execute("USE pubsub_multi_test")

        # Create multiple tables
        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY AUTO_INCREMENT,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
                id INT PRIMARY KEY AUTO_INCREMENT,
                user_id INT,
                title VARCHAR(200) NOT NULL,
                content TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS comments (
                id INT PRIMARY KEY AUTO_INCREMENT,
                post_id INT,
                user_id INT,
                content TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        try:
            # Test multi-table PubSub operations (as per examples)
            # Insert sample data
            test_client.execute("INSERT INTO users (username, email) VALUES ('testuser', 'test@example.com')")
            test_client.execute("INSERT INTO posts (user_id, title, content) VALUES (1, 'Test Post', 'This is a test post')")
            test_client.execute("INSERT INTO comments (post_id, user_id, content) VALUES (1, 1, 'This is a test comment')")

            # Test list operations (as per examples - should work even if empty)
            publications = test_client.pubsub.list_publications()
            assert isinstance(publications, list)

            subscriptions = test_client.pubsub.list_subscriptions()
            assert isinstance(subscriptions, list)

            # Verify data across all tables
            result = test_client.execute("SELECT COUNT(*) FROM users")
            assert result.rows[0][0] == 1

            result = test_client.execute("SELECT COUNT(*) FROM posts")
            assert result.rows[0][0] == 1

            result = test_client.execute("SELECT COUNT(*) FROM comments")
            assert result.rows[0][0] == 1

        finally:
            # Cleanup
            try:
                test_client.pubsub.drop_publication("multi_table_publication")
            except Exception as e:
                print(f"Warning: Failed to cleanup multi-table publication: {e}")

            test_client.execute("DROP DATABASE IF EXISTS pubsub_multi_test")

    def test_pubsub_error_handling(self, test_client):
        """Test PubSub error handling"""
        # Test creating publication with invalid parameters
        try:
            test_client.pubsub.create_database_publication(
                name="",  # Empty name should fail
                database="nonexistent_db",
                account=online_config.get_test_account(),
            )
            assert False, "Should have failed with empty name"
        except Exception:
            pass  # Expected to fail

        # Test creating subscription with invalid parameters
        try:
            test_client.pubsub.create_subscription(
                name="",  # Empty name should fail
                publication="nonexistent_publication",
                account=online_config.get_test_account(),
            )
            assert False, "Should have failed with empty name"
        except Exception:
            pass  # Expected to fail

        # Test dropping non-existent publication
        try:
            test_client.pubsub.drop_publication("nonexistent_publication")
            assert False, "Should have failed with non-existent publication"
        except Exception:
            pass  # Expected to fail

        # Test dropping non-existent subscription
        try:
            test_client.pubsub.drop_subscription("nonexistent_subscription")
            assert False, "Should have failed with non-existent subscription"
        except Exception:
            pass  # Expected to fail

    def test_pubsub_listing_operations(self, test_client):
        """Test PubSub listing operations"""
        # Create test database
        test_client.execute("CREATE DATABASE IF NOT EXISTS pubsub_list_test")
        test_client.execute("USE pubsub_list_test")

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_table (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50) NOT NULL
            )
        """
        )

        try:
            # Test listing operations (as per examples - should work even if empty)
            # Test listing publications
            publications = test_client.pubsub.list_publications()
            assert isinstance(publications, list)

            # Test listing subscriptions
            subscriptions = test_client.pubsub.list_subscriptions()
            assert isinstance(subscriptions, list)

            # Insert test data
            test_client.execute("INSERT INTO test_table (name) VALUES ('test_item')")

            # Verify data
            result = test_client.execute("SELECT COUNT(*) FROM test_table")
            assert result.rows[0][0] == 1

        finally:
            # Cleanup
            try:
                test_client.pubsub.drop_subscription("list_test_subscription")
                test_client.pubsub.drop_publication("list_test_publication")
            except Exception as e:
                print(f"Warning: Failed to cleanup list test resources: {e}")

            test_client.execute("DROP DATABASE IF EXISTS pubsub_list_test")

    @pytest.mark.asyncio
    async def test_async_pubsub_operations(self, test_async_client):
        """Test async PubSub operations"""
        # Create test database
        await test_async_client.execute("CREATE DATABASE IF NOT EXISTS async_pubsub_test")
        await test_async_client.execute("USE async_pubsub_test")

        await test_async_client.execute(
            """
            CREATE TABLE IF NOT EXISTS async_test_table (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50) NOT NULL,
                value INT
            )
        """
        )

        try:
            # Test async PubSub operations (as per examples)
            # Test list operations (should work even if empty)
            publications = await test_async_client.pubsub.list_publications()
            assert isinstance(publications, list)

            subscriptions = await test_async_client.pubsub.list_subscriptions()
            assert isinstance(subscriptions, list)

            # Test async data operations
            await test_async_client.execute("INSERT INTO async_test_table (name, value) VALUES ('async_test', 42)")

            # Verify data
            result = await test_async_client.execute("SELECT COUNT(*) FROM async_test_table")
            assert result.rows[0][0] == 1

        finally:
            # Cleanup
            try:
                await test_async_client.pubsub.drop_subscription("async_test_subscription")
                await test_async_client.pubsub.drop_publication("async_test_publication")
            except Exception as e:
                print(f"Warning: Failed to cleanup async pubsub resources: {e}")

            await test_async_client.execute("DROP DATABASE IF EXISTS async_pubsub_test")

    def test_pubsub_with_logging(self, connection_params):
        """Test PubSub operations with custom logging"""
        host, port, user, password, database = connection_params

        # Create logger
        logger = create_default_logger()

        # Create client with logging
        client = Client()
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Create test database
            client.execute("CREATE DATABASE IF NOT EXISTS pubsub_log_test")
            client.execute("USE pubsub_log_test")

            client.execute(
                """
                CREATE TABLE IF NOT EXISTS log_test_table (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(50) NOT NULL
                )
            """
            )

            # Test PubSub operations with logging (as per examples)
            # Test list operations (should work even if empty)
            publications = client.pubsub.list_publications()
            assert isinstance(publications, list)

            subscriptions = client.pubsub.list_subscriptions()
            assert isinstance(subscriptions, list)

            # Test data operations with logging
            client.execute("INSERT INTO log_test_table (name) VALUES ('log_test')")

            # Verify data
            result = client.execute("SELECT COUNT(*) FROM log_test_table")
            assert result.rows[0][0] == 1

            # Cleanup
            try:
                client.pubsub.drop_publication("log_test_publication")
            except Exception as e:
                print(f"Warning: Failed to cleanup logging test publication: {e}")

            client.execute("DROP DATABASE IF EXISTS pubsub_log_test")

        finally:
            client.disconnect()
