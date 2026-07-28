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
Online tests for MySQL driver validation in Client.from_engine() and AsyncClient.from_engine()
"""

import pytest
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client, AsyncClient
from matrixone.exceptions import ConnectionError
from .test_config import online_config


class TestMySQLDriverValidation:
    """Online tests for MySQL driver validation"""

    @pytest.fixture(scope="class")
    def connection_params(self):
        """Get connection parameters"""
        return online_config.get_connection_params()

    def test_client_from_mysql_engine_success(self, connection_params):
        """Test creating Client from valid MySQL engine"""
        host, port, user, password, database = connection_params

        # Create valid MySQL engine
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        try:
            # Should succeed with MySQL engine
            client = Client.from_engine(engine)
            assert client.connected() is True

            # Test basic functionality
            result = client.execute("SELECT 1 as test_value")
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1

        finally:
            client.disconnect()
            engine.dispose()

    def test_client_from_non_mysql_engine_failure(self):
        """Test creating Client from non-MySQL engine fails"""
        # Test with SQLite engine (should fail) - SQLite doesn't require external drivers
        engine = create_engine("sqlite:///:memory:")

        try:
            # Should raise ConnectionError
            with pytest.raises(ConnectionError) as exc_info:
                Client.from_engine(engine)

            assert "MatrixOne Client only supports MySQL drivers" in str(exc_info.value)
            assert "sqlite" in str(exc_info.value).lower()

        finally:
            engine.dispose()

    def test_client_from_sqlite_engine_failure(self):
        """Test creating Client from SQLite engine fails"""
        # Create SQLite engine (should fail)
        engine = create_engine("sqlite:///:memory:")

        try:
            # Should raise ConnectionError
            with pytest.raises(ConnectionError) as exc_info:
                Client.from_engine(engine)

            assert "MatrixOne Client only supports MySQL drivers" in str(exc_info.value)
            assert "sqlite" in str(exc_info.value).lower()

        finally:
            engine.dispose()

    def test_client_from_mysqlconnector_engine_success(self, connection_params):
        """Test creating Client from mysql+mysqlconnector engine"""
        # Test the driver detection method directly without creating actual engine
        # since mysql-connector-python might not be installed
        connection_string = "mysql+mysqlconnector://user:pass@localhost:3306/db"

        # Create a mock engine object to test the detection logic
        class MockEngine:
            def __init__(self, url):
                self.url = url
                self.dialect = MockDialect()

        class MockDialect:
            def __init__(self):
                self.name = "mysql"

        mock_engine = MockEngine(connection_string)

        # Test that our detection method correctly identifies MySQL connector
        assert Client._is_mysql_engine(mock_engine) is True

    @pytest.mark.asyncio
    async def test_async_client_from_mysql_engine_success(self, connection_params):
        """Test creating AsyncClient from valid MySQL async engine"""
        host, port, user, password, database = connection_params

        # Create valid MySQL async engine
        connection_string = f"mysql+aiomysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_async_engine(connection_string)

        try:
            # Should succeed with MySQL async engine
            client = AsyncClient.from_engine(engine)
            assert client.connected() is True

            # Test basic functionality
            result = await client.execute("SELECT 3 as test_value")
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 3

        finally:
            await client.disconnect()
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_async_client_from_non_mysql_engine_failure(self):
        """Test creating AsyncClient from non-MySQL async engine fails"""
        # Test the driver detection method directly without creating actual engine
        # since aiosqlite might not be installed
        connection_string = "sqlite+aiosqlite:///:memory:"

        # Create a mock engine object to test the detection logic
        class MockAsyncEngine:
            def __init__(self, url):
                self.url = url
                self.dialect = MockAsyncDialect()

        class MockAsyncDialect:
            def __init__(self):
                self.name = "sqlite"

        mock_engine = MockAsyncEngine(connection_string)

        # Test that our detection method correctly identifies non-MySQL driver
        assert AsyncClient._is_mysql_async_engine(mock_engine) is False

    def test_client_mysql_driver_detection_methods(self):
        """Test the _is_mysql_engine method with various engines"""
        # Test with MySQL engines (only test pymysql since it's available)
        mysql_engines = [
            create_engine("mysql+pymysql://user:pass@localhost:3306/db"),
        ]

        for engine in mysql_engines:
            assert Client._is_mysql_engine(engine) is True
            engine.dispose()

        # Test with non-MySQL engines (only test SQLite since it doesn't require external drivers)
        non_mysql_engines = [
            create_engine("sqlite:///:memory:"),
        ]

        for engine in non_mysql_engines:
            assert Client._is_mysql_engine(engine) is False
            engine.dispose()

    @pytest.mark.asyncio
    async def test_async_client_mysql_driver_detection_methods(self):
        """Test the _is_mysql_async_engine method with various async engines"""
        # Test with MySQL async engines (only test aiomysql since it's available)
        mysql_async_engines = [
            create_async_engine("mysql+aiomysql://user:pass@localhost:3306/db"),
        ]

        for engine in mysql_async_engines:
            assert AsyncClient._is_mysql_async_engine(engine) is True
            await engine.dispose()

        # Test with non-MySQL async engines using mock objects
        # since aiosqlite might not be installed
        connection_string = "sqlite+aiosqlite:///:memory:"

        class MockAsyncEngine:
            def __init__(self, url):
                self.url = url
                self.dialect = MockAsyncDialect()

        class MockAsyncDialect:
            def __init__(self):
                self.name = "sqlite"

        mock_engine = MockAsyncEngine(connection_string)
        assert AsyncClient._is_mysql_async_engine(mock_engine) is False


if __name__ == "__main__":
    pytest.main([__file__])
