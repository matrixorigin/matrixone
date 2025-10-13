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
Test insert and batch_insert methods in transaction contexts
"""

import pytest
import pytest_asyncio
from matrixone import Client, AsyncClient
from tests.online.conftest import online_config


class TestTransactionInsertMethods:
    """Test insert and batch_insert methods in transaction contexts"""

    @pytest.fixture(scope="function")
    def sync_client_setup(self):
        """Setup sync client for testing"""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(host=host, port=port, user=user, password=password, database=database)

        # Create test database
        test_db = "sync_insert_test"
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
        test_db = "async_insert_test"
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

        yield client, test_db

        # Cleanup
        try:
            await client.execute(f"DROP DATABASE {test_db}")
        except Exception as e:
            print(f"Cleanup warning: {e}")
        finally:
            await client.disconnect()

    def test_sync_transaction_insert(self, sync_client_setup):
        """Test insert method in sync transaction context"""
        client, test_db = sync_client_setup

        with client.transaction() as tx:
            # Test single insert
            data = {'name': 'John Doe', 'age': 30, 'email': 'john@example.com'}
            result = tx.insert(f"{test_db}.test_table", data)
            assert result.affected_rows == 1

            # Verify insert
            result = tx.execute(f"SELECT * FROM {test_db}.test_table WHERE name = 'John Doe'")
            assert len(result.rows) == 1
            assert result.rows[0][1] == 'John Doe'
            assert result.rows[0][2] == 30
            assert result.rows[0][3] == 'john@example.com'

    def test_sync_transaction_batch_insert(self, sync_client_setup):
        """Test batch_insert method in sync transaction context"""
        client, test_db = sync_client_setup

        with client.transaction() as tx:
            # Test batch insert
            data_list = [
                {'name': 'Alice', 'age': 25, 'email': 'alice@example.com'},
                {'name': 'Bob', 'age': 35, 'email': 'bob@example.com'},
                {'name': 'Charlie', 'age': 28, 'email': 'charlie@example.com'},
            ]
            result = tx.batch_insert(f"{test_db}.test_table", data_list)
            assert result.affected_rows == 3

            # Verify batch insert
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == 3

            # Verify individual records
            result = tx.execute(f"SELECT * FROM {test_db}.test_table ORDER BY name")
            assert len(result.rows) == 3
            assert result.rows[0][1] == 'Alice'
            assert result.rows[1][1] == 'Bob'
            assert result.rows[2][1] == 'Charlie'

    def test_sync_transaction_insert_with_vectors(self, sync_client_setup):
        """Test insert method with vector data in sync transaction context"""
        client, test_db = sync_client_setup

        # Create table with vector column
        client.execute(
            f"""
            CREATE TABLE {test_db}.vector_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                vector JSON
            )
        """
        )

        with client.transaction() as tx:
            # Test insert with vector data
            data = {'name': 'Vector Test', 'vector': [1.0, 2.0, 3.0, 4.0]}
            result = tx.insert(f"{test_db}.vector_table", data)
            assert result.affected_rows == 1

            # Verify insert
            result = tx.execute(f"SELECT * FROM {test_db}.vector_table WHERE name = 'Vector Test'")
            assert len(result.rows) == 1
            assert result.rows[0][1] == 'Vector Test'

    def test_sync_transaction_empty_batch_insert(self, sync_client_setup):
        """Test batch_insert with empty list in sync transaction context"""
        client, test_db = sync_client_setup

        with client.transaction() as tx:
            # Test empty batch insert
            result = tx.batch_insert(f"{test_db}.test_table", [])
            assert result.affected_rows == 0
            assert len(result.rows) == 0

    @pytest.mark.asyncio
    async def test_async_transaction_insert(self, async_client_setup):
        """Test insert method in async transaction context"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test single insert
            data = {'name': 'Jane Doe', 'age': 32, 'email': 'jane@example.com'}
            result = await tx.insert(f"{test_db}.test_table", data)
            assert result.affected_rows == 1

            # Verify insert
            result = await tx.execute(f"SELECT * FROM {test_db}.test_table WHERE name = 'Jane Doe'")
            assert len(result.rows) == 1
            assert result.rows[0][1] == 'Jane Doe'
            assert result.rows[0][2] == 32
            assert result.rows[0][3] == 'jane@example.com'

    @pytest.mark.asyncio
    async def test_async_transaction_batch_insert(self, async_client_setup):
        """Test batch_insert method in async transaction context"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test batch insert
            data_list = [
                {'name': 'David', 'age': 27, 'email': 'david@example.com'},
                {'name': 'Eve', 'age': 33, 'email': 'eve@example.com'},
                {'name': 'Frank', 'age': 29, 'email': 'frank@example.com'},
            ]
            result = await tx.batch_insert(f"{test_db}.test_table", data_list)
            assert result.affected_rows == 3

            # Verify batch insert
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == 3

            # Verify individual records
            result = await tx.execute(f"SELECT * FROM {test_db}.test_table ORDER BY name")
            assert len(result.rows) == 3
            assert result.rows[0][1] == 'David'
            assert result.rows[1][1] == 'Eve'
            assert result.rows[2][1] == 'Frank'

    @pytest.mark.asyncio
    async def test_async_transaction_insert_with_vectors(self, async_client_setup):
        """Test insert method with vector data in async transaction context"""
        client, test_db = async_client_setup

        # Create table with vector column
        await client.execute(
            f"""
            CREATE TABLE {test_db}.vector_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                vector JSON
            )
        """
        )

        async with client.transaction() as tx:
            # Test insert with vector data
            data = {'name': 'Async Vector Test', 'vector': [5.0, 6.0, 7.0, 8.0]}
            result = await tx.insert(f"{test_db}.vector_table", data)
            assert result.affected_rows == 1

            # Verify insert
            result = await tx.execute(f"SELECT * FROM {test_db}.vector_table WHERE name = 'Async Vector Test'")
            assert len(result.rows) == 1
            assert result.rows[0][1] == 'Async Vector Test'

    @pytest.mark.asyncio
    async def test_async_transaction_empty_batch_insert(self, async_client_setup):
        """Test batch_insert with empty list in async transaction context"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test empty batch insert
            result = await tx.batch_insert(f"{test_db}.test_table", [])
            assert result.affected_rows == 0
            assert len(result.rows) == 0

    def test_sync_transaction_rollback_insert(self, sync_client_setup):
        """Test that insert operations are rolled back on transaction failure"""
        client, test_db = sync_client_setup

        try:
            with client.transaction() as tx:
                # Insert data
                data = {'name': 'Rollback Test', 'age': 40, 'email': 'rollback@example.com'}
                result = tx.insert(f"{test_db}.test_table", data)
                assert result.affected_rows == 1

                # Force rollback by raising an exception
                raise Exception("Force rollback")
        except Exception:
            pass

        # Verify data was rolled back
        result = client.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
        assert result.rows[0][0] == 0

    @pytest.mark.asyncio
    async def test_async_transaction_rollback_insert(self, async_client_setup):
        """Test that insert operations are rolled back on async transaction failure"""
        client, test_db = async_client_setup

        try:
            async with client.transaction() as tx:
                # Insert data
                data = {
                    'name': 'Async Rollback Test',
                    'age': 42,
                    'email': 'async_rollback@example.com',
                }
                result = await tx.insert(f"{test_db}.test_table", data)
                assert result.affected_rows == 1

                # Force rollback by raising an exception
                raise Exception("Force rollback")
        except Exception:
            pass

        # Verify data was rolled back
        result = await client.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
        assert result.rows[0][0] == 0

    def test_sync_transaction_mixed_operations(self, sync_client_setup):
        """Test mixing insert, batch_insert, and execute in sync transaction"""
        client, test_db = sync_client_setup

        with client.transaction() as tx:
            # Single insert
            data1 = {'name': 'Mixed Test 1', 'age': 20, 'email': 'mixed1@example.com'}
            result = tx.insert(f"{test_db}.test_table", data1)
            assert result.affected_rows == 1

            # Batch insert
            data_list = [
                {'name': 'Mixed Test 2', 'age': 21, 'email': 'mixed2@example.com'},
                {'name': 'Mixed Test 3', 'age': 22, 'email': 'mixed3@example.com'},
            ]
            result = tx.batch_insert(f"{test_db}.test_table", data_list)
            assert result.affected_rows == 2

            # Direct execute
            result = tx.execute(
                f"INSERT INTO {test_db}.test_table (name, age, email) VALUES ('Mixed Test 4', 23, 'mixed4@example.com')"
            )
            assert result.affected_rows == 1

            # Verify all operations
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == 4

    @pytest.mark.asyncio
    async def test_async_transaction_mixed_operations(self, async_client_setup):
        """Test mixing insert, batch_insert, and execute in async transaction"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Single insert
            data1 = {'name': 'Async Mixed Test 1', 'age': 24, 'email': 'async_mixed1@example.com'}
            result = await tx.insert(f"{test_db}.test_table", data1)
            assert result.affected_rows == 1

            # Batch insert
            data_list = [
                {'name': 'Async Mixed Test 2', 'age': 25, 'email': 'async_mixed2@example.com'},
                {'name': 'Async Mixed Test 3', 'age': 26, 'email': 'async_mixed3@example.com'},
            ]
            result = await tx.batch_insert(f"{test_db}.test_table", data_list)
            assert result.affected_rows == 2

            # Direct execute
            result = await tx.execute(
                f"INSERT INTO {test_db}.test_table (name, age, email) VALUES ('Async Mixed Test 4', 27, 'async_mixed4@example.com')"
            )
            assert result.affected_rows == 1

            # Verify all operations
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.test_table")
            assert result.rows[0][0] == 4
