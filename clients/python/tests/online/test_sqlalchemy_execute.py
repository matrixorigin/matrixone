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
Online tests for Client and AsyncClient execute() with SQLAlchemy statements.

Tests that Client.execute() and AsyncClient.execute() properly support:
- SQLAlchemy select() statements
- SQLAlchemy update() statements
- SQLAlchemy delete() statements
- SQLAlchemy insert() statements
- SQLAlchemy text() statements
- Table reflection and autoload
"""

import pytest
import pytest_asyncio
import uuid
from sqlalchemy import Table, MetaData, Column, Integer, String, select, update, delete, insert, text
from sqlalchemy.orm import declarative_base

from matrixone import Client, AsyncClient
from .test_config import online_config

Base = declarative_base()


@pytest.mark.online
class TestClientSQLAlchemyExecute:
    """Test Client.execute() with SQLAlchemy statements"""

    @pytest.fixture(scope="class")
    def client(self):
        """Create and connect Client."""
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

    @pytest.fixture
    def table_name(self):
        """Generate unique table name."""
        return f"test_sqlalchemy_{uuid.uuid4().hex[:8]}"

    def test_client_execute_text_statement(self, client, table_name):
        """Test Client.execute() with SQLAlchemy text() statement"""
        try:
            # Create table
            client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100))")

            # Test text() statement with parameter binding
            stmt = text(f"INSERT INTO {table_name} (id, name) VALUES (:id, :name)")
            result = client.execute(stmt.bindparams(id=1, name='Alice'))

            # Verify
            assert result.affected_rows == 1

            # Verify data
            verify_result = client.execute(f"SELECT * FROM {table_name}")
            rows = verify_result.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 'Alice'

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_client_execute_select_with_text(self, client, table_name):
        """Test Client.execute() with SQLAlchemy text() for SELECT"""
        try:
            # Create table and insert data
            client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 30)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (3, 'Charlie', 35)")

            # Use text() with parameter binding
            stmt = text(f"SELECT * FROM {table_name} WHERE age > :min_age")
            result = client.execute(stmt.bindparams(min_age=28))

            # Verify
            rows = result.fetchall()
            assert len(rows) == 2
            names = {row[1] for row in rows}
            assert names == {'Bob', 'Charlie'}

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_client_execute_update_with_text(self, client, table_name):
        """Test Client.execute() with SQLAlchemy text() for UPDATE"""
        try:
            # Create table and insert data
            client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")

            # Test update with text()
            stmt = text(f"UPDATE {table_name} SET age = :new_age WHERE id = :user_id")
            result = client.execute(stmt.bindparams(new_age=26, user_id=1))

            # Verify update
            assert result.affected_rows == 1

            # Verify data changed
            verify_result = client.execute(f"SELECT age FROM {table_name} WHERE id = 1")
            assert verify_result.fetchone()[0] == 26

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_client_execute_delete_with_text(self, client, table_name):
        """Test Client.execute() with SQLAlchemy text() for DELETE"""
        try:
            # Create table and insert data
            client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 30)")

            # Test delete with text()
            stmt = text(f"DELETE FROM {table_name} WHERE age < :max_age")
            result = client.execute(stmt.bindparams(max_age=28))

            # Verify deletion
            assert result.affected_rows == 1

            # Verify remaining data
            verify_result = client.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert verify_result.fetchone()[0] == 1

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_client_execute_select_with_table_reflection(self, client, table_name):
        """Test Client.execute() with SQLAlchemy select() using Table reflection"""
        try:
            # Create table with string SQL
            client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")

            # Insert test data
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 30)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (3, 'Charlie', 35)")

            # Test select() with Table autoload
            metadata = MetaData()
            users_table = Table(table_name, metadata, autoload_with=client.get_sqlalchemy_engine())

            # Verify table was reflected correctly
            assert len(users_table.columns) == 3
            assert 'id' in users_table.columns
            assert 'name' in users_table.columns
            assert 'age' in users_table.columns

            # Use select() with reflected table
            stmt = select(users_table).where(users_table.c.age > 28)
            result = client.execute(stmt)

            # Verify results
            rows = result.fetchall()
            assert len(rows) == 2
            names = {row[1] for row in rows}
            assert names == {'Bob', 'Charlie'}

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_client_execute_update_with_table_reflection(self, client, table_name):
        """Test Client.execute() with SQLAlchemy update() using Table reflection"""
        try:
            # Create table and insert data
            client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 30)")

            # Test update() with Table autoload
            metadata = MetaData()
            users_table = Table(table_name, metadata, autoload_with=client.get_sqlalchemy_engine())

            stmt = update(users_table).where(users_table.c.id == 1).values(age=26)
            result = client.execute(stmt)

            # Verify update
            assert result.affected_rows == 1

            # Verify data changed
            verify_result = client.execute(f"SELECT age FROM {table_name} WHERE id = 1")
            assert verify_result.fetchone()[0] == 26

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_client_execute_delete_with_table_reflection(self, client, table_name):
        """Test Client.execute() with SQLAlchemy delete() using Table reflection"""
        try:
            # Create table and insert data
            client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 30)")
            client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (3, 'Charlie', 35)")

            # Test delete() with Table autoload
            metadata = MetaData()
            users_table = Table(table_name, metadata, autoload_with=client.get_sqlalchemy_engine())

            stmt = delete(users_table).where(users_table.c.age < 30)
            result = client.execute(stmt)

            # Verify deletion
            assert result.affected_rows == 1

            # Verify remaining data
            verify_result = client.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert verify_result.fetchone()[0] == 2

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_client_execute_insert_with_table_reflection(self, client, table_name):
        """Test Client.execute() with SQLAlchemy insert() using Table reflection"""
        try:
            # Create table
            client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")

            # Test insert() with Table autoload
            metadata = MetaData()
            users_table = Table(table_name, metadata, autoload_with=client.get_sqlalchemy_engine())

            stmt = insert(users_table).values(id=1, name='Alice', age=25)
            result = client.execute(stmt)

            # Verify insertion
            assert result.affected_rows == 1

            # Verify data
            verify_result = client.execute(f"SELECT * FROM {table_name} WHERE id = 1")
            row = verify_result.fetchone()
            assert row[1] == 'Alice'
            assert row[2] == 25

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


@pytest.mark.online
class TestAsyncClientSQLAlchemyExecute:
    """Test AsyncClient.execute() with SQLAlchemy statements"""

    @pytest_asyncio.fixture
    async def async_client(self):
        """Create and connect AsyncClient."""
        host, port, user, password, database = online_config.get_connection_params()
        client = AsyncClient()
        await client.connect(host=host, port=port, user=user, password=password, database=database)
        try:
            yield client
        finally:
            try:
                await client.disconnect()
            except Exception as e:
                print(f"Warning: Failed to disconnect async client: {e}")

    @pytest.fixture
    def table_name(self):
        """Generate unique table name."""
        return f"test_async_sqlalchemy_{uuid.uuid4().hex[:8]}"

    @pytest.mark.asyncio
    async def test_async_client_execute_text_statement(self, async_client, table_name):
        """Test AsyncClient.execute() with SQLAlchemy text() statement"""
        try:
            # Create table
            await async_client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100))")

            # Test text() statement
            stmt = text(f"INSERT INTO {table_name} (id, name) VALUES (:id, :name)")
            result = await async_client.execute(stmt.bindparams(id=1, name='Alice'))

            # Verify
            assert result.affected_rows == 1

            # Verify data
            verify_result = await async_client.execute(f"SELECT * FROM {table_name}")
            rows = verify_result.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 'Alice'

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_async_client_execute_select_with_text(self, async_client, table_name):
        """Test AsyncClient.execute() with SQLAlchemy text() for SELECT"""
        try:
            # Create table and insert data
            await async_client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 30)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (3, 'Charlie', 35)")

            # Use text() with parameter binding
            stmt = text(f"SELECT * FROM {table_name} WHERE age > :min_age")
            result = await async_client.execute(stmt.bindparams(min_age=28))

            # Verify
            rows = result.fetchall()
            assert len(rows) == 2

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_async_client_execute_update_with_text(self, async_client, table_name):
        """Test AsyncClient.execute() with SQLAlchemy text() for UPDATE"""
        try:
            # Create table and insert data
            await async_client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")

            # Test update with text()
            stmt = text(f"UPDATE {table_name} SET age = :new_age WHERE id = :user_id")
            result = await async_client.execute(stmt.bindparams(new_age=26, user_id=1))

            # Verify update
            assert result.affected_rows == 1

            # Verify data changed
            verify_result = await async_client.execute(f"SELECT age FROM {table_name} WHERE id = 1")
            assert verify_result.fetchone()[0] == 26

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_async_client_execute_delete_with_text(self, async_client, table_name):
        """Test AsyncClient.execute() with SQLAlchemy text() for DELETE"""
        try:
            # Create table and insert data
            await async_client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 30)")

            # Test delete with text()
            stmt = text(f"DELETE FROM {table_name} WHERE age < :max_age")
            result = await async_client.execute(stmt.bindparams(max_age=28))

            # Verify deletion
            assert result.affected_rows == 1

            # Verify remaining data
            verify_result = await async_client.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert verify_result.fetchone()[0] == 1

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_async_client_execute_select_with_table_reflection(self, async_client, table_name):
        """Test AsyncClient.execute() with SQLAlchemy select() using Table reflection"""
        try:
            # Create table and insert data
            await async_client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 30)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (3, 'Charlie', 35)")

            # Use async engine with run_sync() for Table autoload
            metadata = MetaData()
            async_engine = async_client.get_sqlalchemy_engine()

            # Use run_sync() to execute synchronous reflection in async context
            def reflect_table(sync_conn):
                return Table(table_name, metadata, autoload_with=sync_conn)

            async with async_engine.connect() as conn:
                users_table = await conn.run_sync(reflect_table)

            # Verify table was reflected correctly
            assert len(users_table.columns) == 3

            stmt = select(users_table).where(users_table.c.age > 28)
            result = await async_client.execute(stmt)

            # Verify
            rows = result.fetchall()
            assert len(rows) == 2
            names = {row[1] for row in rows}
            assert names == {'Bob', 'Charlie'}

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_async_client_execute_update_with_table_reflection(self, async_client, table_name):
        """Test AsyncClient.execute() with SQLAlchemy update() using Table reflection"""
        try:
            # Create table and insert data
            await async_client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 30)")

            # Use async engine with run_sync() for Table autoload
            metadata = MetaData()
            async_engine = async_client.get_sqlalchemy_engine()

            def reflect_table(sync_conn):
                return Table(table_name, metadata, autoload_with=sync_conn)

            async with async_engine.connect() as conn:
                users_table = await conn.run_sync(reflect_table)

            stmt = update(users_table).where(users_table.c.id == 1).values(age=26)
            result = await async_client.execute(stmt)

            # Verify update
            assert result.affected_rows == 1

            # Verify data changed
            verify_result = await async_client.execute(f"SELECT age FROM {table_name} WHERE id = 1")
            assert verify_result.fetchone()[0] == 26

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_async_client_execute_delete_with_table_reflection(self, async_client, table_name):
        """Test AsyncClient.execute() with SQLAlchemy delete() using Table reflection"""
        try:
            # Create table and insert data
            await async_client.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(100), age INT)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 25)")
            await async_client.execute(f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 30)")

            # Use async engine with run_sync() for Table autoload
            metadata = MetaData()
            async_engine = async_client.get_sqlalchemy_engine()

            def reflect_table(sync_conn):
                return Table(table_name, metadata, autoload_with=sync_conn)

            async with async_engine.connect() as conn:
                users_table = await conn.run_sync(reflect_table)

            stmt = delete(users_table).where(users_table.c.age < 30)
            result = await async_client.execute(stmt)

            # Verify deletion
            assert result.affected_rows == 1

            # Verify remaining data
            verify_result = await async_client.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert verify_result.fetchone()[0] == 1

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
