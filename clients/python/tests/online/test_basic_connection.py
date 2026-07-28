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
Online tests for basic connection functionality

These tests are inspired by example_01_basic_connection.py
"""

import pytest
from matrixone import Client, AsyncClient
from matrixone.logger import create_default_logger


@pytest.mark.online
class TestBasicConnection:
    """Test basic connection functionality"""

    def test_basic_connection_and_query(self, test_client):
        """Test basic connection and simple query"""
        # Test basic query
        result = test_client.execute("SELECT 1 as test_value, USER() as user_info")
        assert result is not None
        assert len(result.rows) > 0
        assert result.rows[0][0] == 1  # test_value should be 1

    def test_database_info_queries(self, test_client):
        """Test database information queries"""
        # Test SHOW DATABASES
        result = test_client.execute("SHOW DATABASES")
        assert result is not None
        assert len(result.rows) > 0

        # Test SHOW TABLES
        result = test_client.execute("SHOW TABLES")
        assert result is not None

    def test_connection_with_logging(self, connection_params):
        """Test connection with custom logging"""
        host, port, user, password, database = connection_params

        # Create logger
        logger = create_default_logger()

        # Create client with logging
        client = Client()
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Test query with logging
            result = client.execute("SELECT 1 as test_value")
            assert result is not None
            assert len(result.rows) > 0
        finally:
            client.disconnect()

    def test_connection_error_handling(self, connection_params):
        """Test connection error handling"""
        host, port, user, password, database = connection_params

        # Test with invalid port
        try:
            client = Client()
            client.connect(host=host, port=9999, user=user, password=password, database=database)
            # With the new connection pool architecture, connection is tested on first query
            client.execute("SELECT 1")
            assert False, "Should have failed with invalid port"
        except Exception as e:
            assert "connection" in str(e).lower() or "refused" in str(e).lower()

    def test_client_context_manager(self, connection_params):
        """Test client context manager"""
        host, port, user, password, database = connection_params

        with Client() as client:
            client.connect(host=host, port=port, user=user, password=password, database=database)
            result = client.execute("SELECT 1 as test_value")
            assert result is not None
            assert len(result.rows) > 0

    @pytest.mark.asyncio
    async def test_async_basic_connection(self, test_async_client):
        """Test basic async connection and query"""
        # Test basic query
        result = await test_async_client.execute("SELECT 1 as test_value, USER() as user_info")
        assert result is not None
        assert len(result.rows) > 0
        assert result.rows[0][0] == 1  # test_value should be 1

    @pytest.mark.asyncio
    async def test_async_connection_with_logging(self, connection_params):
        """Test async connection with custom logging"""
        host, port, user, password, database = connection_params

        # Create logger
        logger = create_default_logger()

        # Create async client with logging
        client = AsyncClient()
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Test query with logging
            result = await client.execute("SELECT 1 as test_value")
            assert result is not None
            assert len(result.rows) > 0
        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_async_client_context_manager(self, connection_params):
        """Test async client context manager"""
        host, port, user, password, database = connection_params

        async with AsyncClient() as client:
            await client.connect(host=host, port=port, user=user, password=password, database=database)
            result = await client.execute("SELECT 1 as test_value")
            assert result is not None
            assert len(result.rows) > 0
