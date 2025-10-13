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
Test query methods in transaction contexts
"""

import pytest
import pytest_asyncio
from matrixone import Client, AsyncClient
from tests.online.conftest import online_config


class TestTransactionQueryMethods:
    """Test query methods in transaction contexts"""

    @pytest.fixture(scope="function")
    def sync_client_setup(self):
        """Setup sync client for testing"""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(host=host, port=port, user=user, password=password, database=database)

        # Create test database
        test_db = "sync_query_test"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        client.execute(f"USE {test_db}")

        # Create test table
        client.execute(
            f"""
            CREATE TABLE {test_db}.test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                age INT,
                email VARCHAR(255)
            )
        """
        )

        # Insert test data
        client.execute(
            f"""
            INSERT INTO {test_db}.test_table (name, age, email) VALUES 
            ('Alice', 25, 'alice@example.com'),
            ('Bob', 30, 'bob@example.com'),
            ('Charlie', 35, 'charlie@example.com')
        """
        )

        yield client, test_db

        # Cleanup
        try:
            client.execute(f"DROP DATABASE {test_db}")
        except Exception as e:
            print(f"Cleanup warning: {e}")
        finally:
            client.disconnect()

    @pytest_asyncio.fixture(scope="function")
    async def async_client_setup(self):
        """Setup async client for testing"""
        client = AsyncClient()
        host, port, user, password, database = online_config.get_connection_params()
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        # Create test database
        test_db = "async_query_test"
        await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        await client.execute(f"USE {test_db}")

        # Create test table
        await client.execute(
            f"""
            CREATE TABLE {test_db}.test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                age INT,
                email VARCHAR(255)
            )
        """
        )

        # Insert test data
        await client.execute(
            f"""
            INSERT INTO {test_db}.test_table (name, age, email) VALUES 
            ('David', 28, 'david@example.com'),
            ('Eve', 32, 'eve@example.com'),
            ('Frank', 38, 'frank@example.com')
        """
        )

        yield client, test_db

        # Cleanup
        try:
            await client.execute(f"DROP DATABASE {test_db}")
        except Exception as e:
            print(f"Cleanup warning: {e}")
        finally:
            await client.disconnect()

    def test_sync_transaction_query_method_exists(self, sync_client_setup):
        """Test that query method exists in sync transaction context"""
        client, test_db = sync_client_setup

        with client.transaction() as tx:
            # Test that query method exists
            assert hasattr(tx, 'query'), "TransactionWrapper should have query method"

            # Test that query method is callable
            assert callable(tx.query), "TransactionWrapper.query should be callable"

    @pytest.mark.asyncio
    async def test_async_transaction_query_method_exists(self, async_client_setup):
        """Test that query method exists in async transaction context"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test that query method exists
            assert hasattr(tx, 'query'), "AsyncTransactionWrapper should have query method"

            # Test that query method is callable
            assert callable(tx.query), "AsyncTransactionWrapper.query should be callable"

    def test_sync_transaction_query_with_simple_select(self, sync_client_setup):
        """Test query method with simple SELECT in sync transaction context"""
        client, test_db = sync_client_setup

        with client.transaction() as tx:
            # Test simple query using execute (since we don't have ORM models in this test)
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == 3

            # Test that we can insert and query within the same transaction
            tx.execute(
                f"INSERT INTO {test_db}.test_table (name, age, email) VALUES ('Transaction Test', 40, 'tx@example.com')"
            )

            # Verify the insert
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == 4

    @pytest.mark.asyncio
    async def test_async_transaction_query_with_simple_select(self, async_client_setup):
        """Test query method with simple SELECT in async transaction context"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test simple query using execute (since we don't have ORM models in this test)
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == 3

            # Test that we can insert and query within the same transaction
            await tx.execute(
                f"INSERT INTO {test_db}.test_table (name, age, email) VALUES ('Async Transaction Test', 42, 'async_tx@example.com')"
            )

            # Verify the insert
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == 4

    def test_sync_transaction_query_rollback(self, sync_client_setup):
        """Test that query operations are rolled back on transaction failure"""
        client, test_db = sync_client_setup

        try:
            with client.transaction() as tx:
                # Insert data
                tx.execute(
                    f"INSERT INTO {test_db}.test_table (name, age, email) VALUES ('Rollback Test', 50, 'rollback@example.com')"
                )

                # Verify insert
                result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
                assert result.rows[0][0] == 4

                # Force rollback
                raise Exception("Force rollback")
        except Exception:
            pass

        # Verify data was rolled back
        result = client.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
        assert result.rows[0][0] == 3

    @pytest.mark.asyncio
    async def test_async_transaction_query_rollback(self, async_client_setup):
        """Test that query operations are rolled back on async transaction failure"""
        client, test_db = async_client_setup

        try:
            async with client.transaction() as tx:
                # Insert data
                await tx.execute(
                    f"INSERT INTO {test_db}.test_table (name, age, email) VALUES ('Async Rollback Test', 52, 'async_rollback@example.com')"
                )

                # Verify insert
                result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
                assert result.rows[0][0] == 4

                # Force rollback
                raise Exception("Force rollback")
        except Exception:
            pass

        # Verify data was rolled back
        result = await client.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
        assert result.rows[0][0] == 3

    def test_sync_transaction_mixed_operations(self, sync_client_setup):
        """Test mixing query, insert, and batch_insert in sync transaction"""
        client, test_db = sync_client_setup

        with client.transaction() as tx:
            # Query initial count
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            initial_count = result.rows[0][0]
            assert initial_count == 3

            # Insert single record
            tx.execute(
                f"INSERT INTO {test_db}.test_table (name, age, email) VALUES ('Mixed Test 1', 45, 'mixed1@example.com')"
            )

            # Query count after insert
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == initial_count + 1

            # Insert multiple records
            tx.execute(
                f"""
                INSERT INTO {test_db}.test_table (name, age, email) VALUES 
                ('Mixed Test 2', 46, 'mixed2@example.com'),
                ('Mixed Test 3', 47, 'mixed3@example.com')
            """
            )

            # Query final count
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == initial_count + 3

    @pytest.mark.asyncio
    async def test_async_transaction_mixed_operations(self, async_client_setup):
        """Test mixing query, insert, and batch_insert in async transaction"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Query initial count
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            initial_count = result.rows[0][0]
            assert initial_count == 3

            # Insert single record
            await tx.execute(
                f"INSERT INTO {test_db}.test_table (name, age, email) VALUES ('Async Mixed Test 1', 48, 'async_mixed1@example.com')"
            )

            # Query count after insert
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == initial_count + 1

            # Insert multiple records
            await tx.execute(
                f"""
                INSERT INTO {test_db}.test_table (name, age, email) VALUES 
                ('Async Mixed Test 2', 49, 'async_mixed2@example.com'),
                ('Async Mixed Test 3', 50, 'async_mixed3@example.com')
            """
            )

            # Query final count
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == initial_count + 3
