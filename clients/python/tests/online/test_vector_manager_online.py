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
Online integration tests for VectorManager in different contexts.
Tests VectorManager usage with Client, Session, AsyncClient, and AsyncSession.
"""

import pytest
import pytest_asyncio
import sys
import os
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from matrixone import Client, AsyncClient
from .test_config import online_config


class TestVectorManagerClient:
    """Test VectorManager with Client context"""

    @pytest.fixture(scope="class")
    def client(self):
        """Create and connect Client"""
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
        """Generate unique table name"""
        return f"test_vector_{uuid.uuid4().hex[:8]}"

    def test_enable_disable_ivf(self, client):
        """Test enable and disable IVF indexing"""
        # Enable IVF
        result = client.vector_ops.enable_ivf(probe_limit=2)
        assert result is client.vector_ops  # Check chaining

        # Disable IVF
        result = client.vector_ops.disable_ivf()
        assert result is client.vector_ops

    def test_enable_disable_hnsw(self, client):
        """Test enable and disable HNSW indexing"""
        # Enable HNSW
        result = client.vector_ops.enable_hnsw()
        assert result is client.vector_ops

        # Disable HNSW
        result = client.vector_ops.disable_hnsw()
        assert result is client.vector_ops

    def test_vector_ops_insert(self, client, table_name):
        """Test insert through vector_ops"""
        try:
            # Create test table
            client.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY,
                    embedding VECF32(3)
                )
            """
            )

            # Insert via vector_ops
            result = client.vector_ops.insert(table_name, {'id': 1, 'embedding': [0.1, 0.2, 0.3]})
            assert result is client.vector_ops

            # Verify insertion
            rows = client.execute(f"SELECT * FROM {table_name}").fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_vector_ops_batch_insert(self, client, table_name):
        """Test batch insert through vector_ops"""
        try:
            # Create test table
            client.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY,
                    embedding VECF32(3)
                )
            """
            )

            # Batch insert via vector_ops
            data_list = [{'id': i, 'embedding': [i * 0.1, i * 0.2, i * 0.3]} for i in range(1, 6)]
            result = client.vector_ops.batch_insert(table_name, data_list)
            assert result is client.vector_ops

            # Verify insertions
            rows = client.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            assert rows[0] == 5

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


class TestVectorManagerSession:
    """Test VectorManager with Session context"""

    @pytest.fixture(scope="class")
    def client(self):
        """Create and connect Client"""
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
        """Generate unique table name"""
        return f"test_vector_{uuid.uuid4().hex[:8]}"

    def test_session_enable_disable_ivf(self, client):
        """Test enable/disable IVF in session context"""
        with client.session() as session:
            # Enable IVF
            result = session.vector_ops.enable_ivf(probe_limit=3)
            assert result is session.vector_ops

            # Disable IVF
            result = session.vector_ops.disable_ivf()
            assert result is session.vector_ops

    def test_session_enable_disable_hnsw(self, client):
        """Test enable/disable HNSW in session context"""
        with client.session() as session:
            # Enable HNSW
            result = session.vector_ops.enable_hnsw()
            assert result is session.vector_ops

            # Disable HNSW
            result = session.vector_ops.disable_hnsw()
            assert result is session.vector_ops

    def test_session_vector_insert(self, client, table_name):
        """Test vector insert in session context"""
        try:
            # Create test table
            client.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY,
                    embedding VECF32(3)
                )
            """
            )

            # Insert in session
            with client.session() as session:
                result = session.vector_ops.insert(table_name, {'id': 1, 'embedding': [0.1, 0.2, 0.3]})
                assert result is session.vector_ops

            # Verify insertion
            rows = client.execute(f"SELECT * FROM {table_name}").fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_session_vector_batch_insert(self, client, table_name):
        """Test vector batch insert in session context"""
        try:
            # Create test table
            client.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY,
                    embedding VECF32(3)
                )
            """
            )

            # Batch insert in session
            with client.session() as session:
                data_list = [{'id': i, 'embedding': [i * 0.1, i * 0.2, i * 0.3]} for i in range(1, 4)]
                result = session.vector_ops.batch_insert(table_name, data_list)
                assert result is session.vector_ops

            # Verify insertions
            rows = client.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            assert rows[0] == 3

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


class TestAsyncVectorManagerClient:
    """Test AsyncVectorManager with AsyncClient context"""

    @pytest_asyncio.fixture(scope="function")
    async def async_client(self):
        """Create and connect AsyncClient"""
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
        """Generate unique table name"""
        return f"test_vector_{uuid.uuid4().hex[:8]}"

    @pytest.mark.asyncio
    async def test_async_enable_disable_ivf(self, async_client):
        """Test enable and disable IVF indexing async"""
        # Enable IVF
        result = await async_client.vector_ops.enable_ivf(probe_limit=2)
        assert result is async_client.vector_ops

        # Disable IVF
        result = await async_client.vector_ops.disable_ivf()
        assert result is async_client.vector_ops

    @pytest.mark.asyncio
    async def test_async_enable_disable_hnsw(self, async_client):
        """Test enable and disable HNSW indexing async"""
        # Enable HNSW
        result = await async_client.vector_ops.enable_hnsw()
        assert result is async_client.vector_ops

        # Disable HNSW
        result = await async_client.vector_ops.disable_hnsw()
        assert result is async_client.vector_ops

    @pytest.mark.asyncio
    async def test_async_vector_insert(self, async_client, table_name):
        """Test async vector insert"""
        try:
            # Create test table
            await async_client.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY,
                    embedding VECF32(3)
                )
            """
            )

            # Insert via vector_ops
            result = await async_client.vector_ops.insert(table_name, {'id': 1, 'embedding': [0.1, 0.2, 0.3]})
            assert result is async_client.vector_ops

            # Verify insertion
            result = await async_client.execute(f"SELECT * FROM {table_name}")
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_async_vector_batch_insert(self, async_client, table_name):
        """Test async vector batch insert"""
        try:
            # Create test table
            await async_client.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY,
                    embedding VECF32(3)
                )
            """
            )

            # Batch insert via vector_ops
            data_list = [{'id': i, 'embedding': [i * 0.1, i * 0.2, i * 0.3]} for i in range(1, 6)]
            result = await async_client.vector_ops.batch_insert(table_name, data_list)
            assert result is async_client.vector_ops

            # Verify insertions
            result = await async_client.execute(f"SELECT COUNT(*) FROM {table_name}")
            rows = result.fetchone()
            assert rows[0] == 5

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


class TestAsyncVectorManagerSession:
    """Test AsyncVectorManager with AsyncSession context"""

    @pytest_asyncio.fixture(scope="function")
    async def async_client(self):
        """Create and connect AsyncClient"""
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
        """Generate unique table name"""
        return f"test_vector_{uuid.uuid4().hex[:8]}"

    @pytest.mark.asyncio
    async def test_async_session_enable_disable_ivf(self, async_client):
        """Test enable/disable IVF in async session context"""

        async def _test():
            async with async_client.session() as session:
                # Enable IVF
                result = await session.vector_ops.enable_ivf(probe_limit=3)
                assert result is session.vector_ops

                # Disable IVF
                result = await session.vector_ops.disable_ivf()
                assert result is session.vector_ops
            await async_client.disconnect()

        await _test()

    @pytest.mark.asyncio
    async def test_async_session_enable_disable_hnsw(self, async_client):
        """Test enable/disable HNSW in async session context"""

        async def _test():
            async with async_client.session() as session:
                # Enable HNSW
                result = await session.vector_ops.enable_hnsw()
                assert result is session.vector_ops

                # Disable HNSW
                result = await session.vector_ops.disable_hnsw()
                assert result is session.vector_ops
            await async_client.disconnect()

        await _test()

    @pytest.mark.asyncio
    async def test_async_session_vector_insert(self, async_client, table_name):
        """Test vector insert in async session context"""

        async def _test():
            try:
                # Create test table
                await async_client.execute(
                    f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        embedding VECF32(3)
                    )
                """
                )

                # Insert in session
                async with async_client.session() as session:
                    result = await session.vector_ops.insert(table_name, {'id': 1, 'embedding': [0.1, 0.2, 0.3]})
                    assert result is session.vector_ops

                # Verify insertion
                result = await async_client.execute(f"SELECT * FROM {table_name}")
                rows = result.fetchall()
                assert len(rows) == 1
                assert rows[0][0] == 1

            finally:
                try:
                    await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
                except Exception:
                    pass
            await async_client.disconnect()

        await _test()

    @pytest.mark.asyncio
    async def test_async_session_vector_batch_insert(self, async_client, table_name):
        """Test vector batch insert in async session context"""

        async def _test():
            try:
                # Create test table
                await async_client.execute(
                    f"""
                    CREATE TABLE {table_name} (
                        id INT PRIMARY KEY,
                        embedding VECF32(3)
                    )
                """
                )

                # Batch insert in session
                async with async_client.session() as session:
                    data_list = [{'id': i, 'embedding': [i * 0.1, i * 0.2, i * 0.3]} for i in range(1, 4)]
                    result = await session.vector_ops.batch_insert(table_name, data_list)
                    assert result is session.vector_ops

                # Verify insertions
                result = await async_client.execute(f"SELECT COUNT(*) FROM {table_name}")
                rows = result.fetchone()
                assert rows[0] == 3

            finally:
                try:
                    await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
                except Exception:
                    pass
            await async_client.disconnect()

        await _test()
