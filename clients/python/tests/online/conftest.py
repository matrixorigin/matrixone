#!/usr/bin/env python3

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
Pytest configuration for online tests

This file ensures that fixtures are properly registered for online tests.
"""

import pytest
import pytest_asyncio
import asyncio
from typing import Tuple
from matrixone import Client, AsyncClient
from matrixone.logger import create_default_logger
from .test_config import online_config


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


def pytest_configure(config):
    """Configure pytest with online test settings"""
    config.addinivalue_line("markers", "online: mark test as requiring database connection")
    config.addinivalue_line("markers", "slow: mark test as slow running")


class DatabaseTestHelper:
    """Helper class for database operations in tests"""

    def __init__(self):
        self.logger = create_default_logger()

    def create_client(self, host: str, port: int, user: str, password: str, database: str) -> Client:
        """Create a synchronous client"""
        return Client()

    def connect_client(self, client: Client, host: str, port: int, user: str, password: str, database: str) -> bool:
        """Connect a client and return success status"""
        try:
            client.connect(host=host, port=port, user=user, password=password, database=database)
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect client: {e}")
            return False

    def create_async_client(self, host: str, port: int, user: str, password: str, database: str) -> AsyncClient:
        """Create an asynchronous client"""
        return AsyncClient()

    async def connect_async_client(
        self, client: AsyncClient, host: str, port: int, user: str, password: str, database: str
    ) -> bool:
        """Connect an async client and return success status"""
        try:
            await client.connect(host=host, port=port, user=user, password=password, database=database)
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect async client: {e}")
            return False

    def test_connection(self, host: str, port: int, user: str, password: str, database: str) -> bool:
        """Test database connection"""
        try:
            client = self.create_client(host, port, user, password, database)
            return self.connect_client(client, host, port, user, password, database)
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False

    async def test_async_connection(self, host: str, port: int, user: str, password: str, database: str) -> bool:
        """Test async database connection"""
        try:
            client = self.create_async_client(host, port, user, password, database)
            return await self.connect_async_client(client, host, port, user, password, database)
        except Exception as e:
            self.logger.error(f"Async connection test failed: {e}")
            return False


@pytest.fixture(scope="session")
def db_config():
    """Provide database configuration"""
    return online_config


@pytest.fixture(scope="session")
def connection_params():
    """Provide connection parameters"""
    return online_config.get_connection_params()


@pytest.fixture(scope="session")
def test_database():
    """Provide test database name"""
    return online_config.get_test_database()


@pytest.fixture(scope="session")
def sys_database():
    """Provide system database name"""
    return online_config.get_sys_database()


@pytest.fixture(scope="session")
def test_account():
    """Provide test account name"""
    return online_config.get_test_account()


@pytest.fixture(scope="session")
def table_prefix():
    """Provide table prefix for tests"""
    return online_config.table_prefix


@pytest.fixture(scope="session")
def snapshot_prefix():
    """Provide snapshot prefix for tests"""
    return online_config.snapshot_prefix


@pytest.fixture(scope="session")
def vector_dimensions():
    """Provide vector dimensions for tests"""
    return online_config.get_vector_dimensions()


@pytest.fixture(scope="session")
def vector_test_data_size():
    """Provide vector test data size"""
    return online_config.get_vector_test_data_size()


@pytest.fixture(scope="session")
def db_helper():
    """Provide database helper instance"""
    return DatabaseTestHelper()


@pytest.fixture(scope="session")
def db_connection_test(db_helper):
    """Test database connection and provide result"""
    host, port, user, password, database = online_config.get_connection_params()
    success = db_helper.test_connection(host, port, user, password, database)
    if not success:
        pytest.skip("Database connection failed - skipping online tests")
    return success


@pytest_asyncio.fixture(scope="session")
async def async_db_connection_test(db_helper):
    """Test async database connection and provide result"""
    host, port, user, password, database = online_config.get_connection_params()
    success = await db_helper.test_async_connection(host, port, user, password, database)
    if not success:
        pytest.skip("Async database connection failed - skipping online tests")
    return success


@pytest.fixture
def test_client(db_helper, db_connection_test):
    """Provide a connected test client"""
    host, port, user, password, database = online_config.get_connection_params()
    client = db_helper.create_client(host, port, user, password, database)
    db_helper.connect_client(client, host, port, user, password, database)
    try:
        yield client
    finally:
        try:
            client.disconnect()
        except Exception as e:
            print(f"Warning: Failed to disconnect test client: {e}")
            # Don't ignore - this could indicate a real problem
            raise


@pytest_asyncio.fixture
async def test_async_client(db_helper, async_db_connection_test):
    """Provide a connected async test client"""
    host, port, user, password, database = online_config.get_connection_params()
    client = db_helper.create_async_client(host, port, user, password, database)
    await db_helper.connect_async_client(client, host, port, user, password, database)
    try:
        yield client
    finally:
        # Clean disconnect - the improved disconnect method should handle cleanup properly
        try:
            await client.disconnect()
        except Exception as e:
            # If async disconnect fails, try sync cleanup as fallback
            try:
                client.disconnect_sync()
            except Exception:
                pass
            # Log the error for debugging
            print(f"Warning: Failed to disconnect async client: {e}")
