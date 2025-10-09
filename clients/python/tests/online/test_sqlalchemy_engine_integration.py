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
Online tests for SQLAlchemy Engine integration - tests Client and AsyncClient construction from SQLAlchemy engines
"""

import pytest
import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client, AsyncClient
from .test_config import online_config


class TestSQLAlchemyEngineIntegration:
    """Online tests for SQLAlchemy Engine integration"""

    @pytest.fixture(scope="class")
    def connection_params(self):
        """Get connection parameters"""
        return online_config.get_connection_params()

    @pytest.fixture(scope="class")
    def test_database(self):
        """Get test database name"""
        return "test_engine_integration_db"

    def test_client_from_sync_engine(self, connection_params, test_database):
        """Test creating Client from SQLAlchemy sync engine"""
        host, port, user, password, database = connection_params

        # Create SQLAlchemy engine
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        try:
            # Create Client from engine
            client = Client.from_engine(engine)

            # Test basic functionality
            assert client.connected() is True

            # Test query execution
            result = client.execute("SELECT 1 as test_value")
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1

            # Test database operations
            client.execute(f"CREATE DATABASE IF NOT EXISTS {test_database}")
            client.execute(f"USE {test_database}")

            # Test table creation
            client.execute(
                """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT PRIMARY KEY,
                    name VARCHAR(100)
                )
            """
            )

            # Test data insertion
            client.execute("INSERT INTO test_table VALUES (1, 'test')")

            # Test data retrieval
            result = client.execute("SELECT * FROM test_table")
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1
            assert rows[0][1] == 'test'

        finally:
            # Cleanup
            try:
                client.execute(f"DROP DATABASE IF EXISTS {test_database}")
            except Exception as e:
                print(f"Cleanup failed: {e}")
            client.disconnect()
            engine.dispose()

    def test_client_from_sync_engine_with_config(self, connection_params, test_database):
        """Test creating Client from SQLAlchemy sync engine with custom configuration"""
        host, port, user, password, database = connection_params

        # Create SQLAlchemy engine
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        try:
            # Create Client from engine with custom configuration
            client = Client.from_engine(engine, sql_log_mode="auto", slow_query_threshold=0.1)

            # Test basic functionality
            assert client.connected() is True

            # Test query execution
            result = client.execute("SELECT 2 as test_value")
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 2

        finally:
            # Cleanup
            client.disconnect()
            engine.dispose()

    @pytest.mark.asyncio
    async def test_async_client_from_async_engine(self, connection_params, test_database):
        """Test creating AsyncClient from SQLAlchemy async engine"""
        host, port, user, password, database = connection_params

        # Create SQLAlchemy async engine
        connection_string = f"mysql+aiomysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_async_engine(connection_string)

        try:
            # Create AsyncClient from engine
            client = AsyncClient.from_engine(engine)

            # Test basic functionality
            assert client.connected() is True

            # Test query execution
            result = await client.execute("SELECT 3 as test_value")
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 3

            # Test database operations
            await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_database}")
            await client.execute(f"USE {test_database}")

            # Test table creation
            await client.execute(
                """
                CREATE TABLE IF NOT EXISTS async_test_table (
                    id INT PRIMARY KEY,
                    name VARCHAR(100)
                )
            """
            )

            # Test data insertion
            await client.execute("INSERT INTO async_test_table VALUES (1, 'async_test')")

            # Test data retrieval
            result = await client.execute("SELECT * FROM async_test_table")
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1
            assert rows[0][1] == 'async_test'

        finally:
            # Cleanup
            try:
                await client.execute(f"DROP DATABASE IF EXISTS {test_database}")
            except Exception as e:
                print(f"Cleanup failed: {e}")
            await client.disconnect()
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_async_client_from_async_engine_with_config(self, connection_params, test_database):
        """Test creating AsyncClient from SQLAlchemy async engine with custom configuration"""
        host, port, user, password, database = connection_params

        # Create SQLAlchemy async engine
        connection_string = f"mysql+aiomysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_async_engine(connection_string)

        try:
            # Create AsyncClient from engine with custom configuration
            client = AsyncClient.from_engine(engine, sql_log_mode="auto", slow_query_threshold=0.1)

            # Test basic functionality
            assert client.connected() is True

            # Test query execution
            result = await client.execute("SELECT 4 as test_value")
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 4

        finally:
            # Cleanup
            await client.disconnect()
            await engine.dispose()

    def test_client_engine_reuse(self, connection_params):
        """Test that the same engine can be used to create multiple clients"""
        host, port, user, password, database = connection_params

        # Create SQLAlchemy engine
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        try:
            # Create multiple clients from the same engine
            client1 = Client.from_engine(engine)
            client2 = Client.from_engine(engine)

            # Both clients should be connected
            assert client1.connected() is True
            assert client2.connected() is True

            # Test that both clients can execute queries
            result1 = client1.execute("SELECT 5 as test_value")
            result2 = client2.execute("SELECT 6 as test_value")

            rows1 = result1.fetchall()
            rows2 = result2.fetchall()

            assert len(rows1) == 1
            assert len(rows2) == 1
            assert rows1[0][0] == 5
            assert rows2[0][0] == 6

        finally:
            # Cleanup
            client1.disconnect()
            client2.disconnect()
            engine.dispose()

    @pytest.mark.asyncio
    async def test_async_client_engine_reuse(self, connection_params):
        """Test that the same async engine can be used to create multiple async clients"""
        host, port, user, password, database = connection_params

        # Create SQLAlchemy async engine
        connection_string = f"mysql+aiomysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_async_engine(connection_string)

        try:
            # Create multiple async clients from the same engine
            client1 = AsyncClient.from_engine(engine)
            client2 = AsyncClient.from_engine(engine)

            # Both clients should be connected
            assert client1.connected() is True
            assert client2.connected() is True

            # Test that both clients can execute queries
            result1 = await client1.execute("SELECT 7 as test_value")
            result2 = await client2.execute("SELECT 8 as test_value")

            rows1 = result1.fetchall()
            rows2 = result2.fetchall()

            assert len(rows1) == 1
            assert len(rows2) == 1
            assert rows1[0][0] == 7
            assert rows2[0][0] == 8

        finally:
            # Cleanup
            await client1.disconnect()
            await client2.disconnect()
            await engine.dispose()

    def test_client_managers_initialization(self, connection_params):
        """Test that all managers are properly initialized when creating client from engine"""
        host, port, user, password, database = connection_params

        # Create SQLAlchemy engine
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        try:
            # Create Client from engine
            client = Client.from_engine(engine)

            # Test that all managers are initialized
            assert hasattr(client, 'snapshots')
            assert hasattr(client, 'clone')
            assert hasattr(client, 'moctl')
            assert hasattr(client, 'restore')
            assert hasattr(client, 'pitr')
            assert hasattr(client, 'pubsub')
            assert hasattr(client, 'account')

            # Test that managers are functional
            # Test snapshot manager
            assert client.snapshots is not None

            # Test account manager
            assert client.account is not None

        finally:
            # Cleanup
            client.disconnect()
            engine.dispose()

    @pytest.mark.asyncio
    async def test_async_client_managers_initialization(self, connection_params):
        """Test that all managers are properly initialized when creating async client from engine"""
        host, port, user, password, database = connection_params

        # Create SQLAlchemy async engine
        connection_string = f"mysql+aiomysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_async_engine(connection_string)

        try:
            # Create AsyncClient from engine
            client = AsyncClient.from_engine(engine)

            # Test that vector managers are initialized (if available)
            # Note: Some managers might be None if dependencies are not available
            assert hasattr(client, '_vector')
            assert hasattr(client, '_fulltext_index')

        finally:
            # Cleanup
            await client.disconnect()
            await engine.dispose()


if __name__ == "__main__":
    pytest.main([__file__])
