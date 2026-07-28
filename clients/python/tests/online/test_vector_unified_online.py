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
Unified online tests for Vector operations across all execution contexts.

Test Strategy:
1. Test each vector operation in 4 contexts: Client, Session, AsyncClient, AsyncSession
2. Verify identical input produces identical output across contexts
3. Verify Session contexts exhibit transaction behavior (commit/rollback)
4. Eliminate redundant and duplicate tests
"""

import pytest
import pytest_asyncio
import uuid
from matrixone import Client, AsyncClient
from .test_config import online_config


class TestVectorOperationsUnified:
    """
    Unified tests for vector operations across all execution contexts.

    Tests the same operation in Client and Session contexts, verifying:
    - Same input produces same output
    - Session operations are transactional
    """

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
        return f"test_vector_{uuid.uuid4().hex[:8]}"

    def test_vector_insert_consistency(self, client, table_name):
        """Test that insert returns consistent results across Client and Session."""
        try:
            # Create table
            client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")

            # Test data
            data = {'id': 1, 'embedding': [0.1, 0.2, 0.3]}

            # Client mode - auto-commit
            result_client = client.vector_ops.insert(table_name, data)
            assert result_client is client.vector_ops

            # Verify insertion
            rows = client.execute(f"SELECT * FROM {table_name}").fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1

            # Session mode - transactional
            data2 = {'id': 2, 'embedding': [0.4, 0.5, 0.6]}
            with client.session() as session:
                result_session = session.vector_ops.insert(table_name, data2)
                assert result_session is session.vector_ops

                # Inside transaction - data visible
                rows_in_tx = session.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                assert rows_in_tx[0] == 2

            # After commit - data persisted
            rows_after = client.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            assert rows_after[0] == 2

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_vector_insert_rollback(self, client, table_name):
        """Test that Session rollback works for vector insert."""
        try:
            client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")

            # Insert in client mode (committed)
            client.vector_ops.insert(table_name, {'id': 1, 'embedding': [0.1, 0.2, 0.3]})

            # Try insert in session with rollback
            try:
                with client.session() as session:
                    session.vector_ops.insert(table_name, {'id': 2, 'embedding': [0.4, 0.5, 0.6]})
                    raise Exception("Test rollback")
            except Exception as e:
                if "Test rollback" not in str(e):
                    raise

            # Verify rollback - only first row should exist
            rows = client.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            assert rows[0] == 1

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_similarity_search_consistency(self, client, table_name):
        """Test similarity_search returns identical results in Client and Session."""
        try:
            # Create table and insert data
            client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, title VARCHAR(100), embedding VECF32(3))")
            client.vector_ops.insert(table_name, {'id': 1, 'title': 'Doc1', 'embedding': [0.1, 0.2, 0.3]})
            client.vector_ops.insert(table_name, {'id': 2, 'title': 'Doc2', 'embedding': [0.4, 0.5, 0.6]})
            client.vector_ops.insert(table_name, {'id': 3, 'title': 'Doc3', 'embedding': [0.7, 0.8, 0.9]})

            query_vector = [0.1, 0.2, 0.3]

            # Client mode search
            results_client = client.vector_ops.similarity_search(table_name, "embedding", query_vector, limit=2)

            # Session mode search
            with client.session() as session:
                results_session = session.vector_ops.similarity_search(table_name, "embedding", query_vector, limit=2)

            # Verify results are identical
            assert len(results_client) == len(results_session)
            assert len(results_client) == 2

            # Verify same data in same order
            for i in range(len(results_client)):
                assert results_client[i]['id'] == results_session[i]['id']
                assert results_client[i]['title'] == results_session[i]['title']
                assert abs(results_client[i]['distance'] - results_session[i]['distance']) < 0.0001

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_similarity_search_distance_types(self, client, table_name):
        """Test similarity_search with different distance types."""
        try:
            # Create table and insert data
            client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")
            client.vector_ops.insert(table_name, {'id': 1, 'embedding': [1.0, 0.0, 0.0]})
            client.vector_ops.insert(table_name, {'id': 2, 'embedding': [0.0, 1.0, 0.0]})

            query_vector = [1.0, 0.0, 0.0]

            # Test L2 distance
            results_l2 = client.vector_ops.similarity_search(
                table_name, "embedding", query_vector, limit=2, distance_type="l2"
            )
            assert len(results_l2) == 2
            assert results_l2[0]['id'] == 1
            assert results_l2[0]['distance'] < 0.0001

            # Test cosine distance
            results_cosine = client.vector_ops.similarity_search(
                table_name, "embedding", query_vector, limit=2, distance_type="cosine"
            )
            assert len(results_cosine) == 2
            assert 'distance' in results_cosine[0]

            # Test inner product
            results_ip = client.vector_ops.similarity_search(
                table_name, "embedding", query_vector, limit=2, distance_type="inner_product"
            )
            assert len(results_ip) == 2
            assert 'distance' in results_ip[0]

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_batch_insert_consistency(self, client, table_name):
        """Test batch_insert in Client and Session contexts."""
        try:
            client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")

            # Prepare batch data
            data_list = [{'id': i, 'embedding': [i * 0.1, i * 0.2, i * 0.3]} for i in range(1, 6)]

            # Client mode batch insert
            result = client.vector_ops.batch_insert(table_name, data_list)
            assert result is client.vector_ops

            # Verify all inserted
            count = client.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            assert count == 5

            # Session mode batch insert
            data_list2 = [{'id': i, 'embedding': [i * 0.1, i * 0.2, i * 0.3]} for i in range(6, 11)]

            with client.session() as session:
                result_session = session.vector_ops.batch_insert(table_name, data_list2)
                assert result_session is session.vector_ops

            # Verify total count
            count_after = client.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            assert count_after == 10

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_create_drop_ivf_index(self, client, table_name):
        """Test create and drop IVF index in both contexts."""
        try:
            # Create table
            client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")

            # Create IVF index in client mode
            result = client.vector_ops.create_ivf(table_name, "idx_test", "embedding", lists=2)
            assert result is client.vector_ops

            # Verify index exists
            indexes = client.execute(f"SHOW INDEX FROM {table_name}").fetchall()
            assert any('idx_test' in str(row) for row in indexes)

            # Drop index in session mode
            with client.session() as session:
                result_drop = session.vector_ops.drop(table_name, "idx_test")
                assert result_drop is session.vector_ops

            # Verify index dropped
            indexes_after = client.execute(f"SHOW INDEX FROM {table_name}").fetchall()
            assert not any('idx_test' in str(row) for row in indexes_after)

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


class TestAsyncVectorOperationsUnified:
    """Unified tests for async vector operations."""

    @pytest_asyncio.fixture(scope="function")
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
        return f"test_async_vector_{uuid.uuid4().hex[:8]}"

    @pytest.mark.asyncio
    async def test_async_insert_consistency(self, async_client, table_name):
        """Test async insert returns consistent results across AsyncClient and AsyncSession."""
        try:
            # Create table
            await async_client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")

            # AsyncClient mode
            data1 = {'id': 1, 'embedding': [0.1, 0.2, 0.3]}
            result_client = await async_client.vector_ops.insert(table_name, data1)
            assert result_client is async_client.vector_ops

            # Verify
            result = await async_client.execute(f"SELECT * FROM {table_name}")
            rows = result.fetchall()
            assert len(rows) == 1

            # AsyncSession mode
            data2 = {'id': 2, 'embedding': [0.4, 0.5, 0.6]}
            async with async_client.session() as session:
                result_session = await session.vector_ops.insert(table_name, data2)
                assert result_session is session.vector_ops

            # Verify after commit
            result_after = await async_client.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = result_after.fetchone()[0]
            assert count == 2

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_async_session_rollback(self, async_client, table_name):
        """Test AsyncSession rollback for vector operations."""
        try:
            await async_client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")

            # Insert in AsyncClient mode (committed)
            await async_client.vector_ops.insert(table_name, {'id': 1, 'embedding': [0.1, 0.2, 0.3]})

            # Try insert in session with rollback
            try:
                async with async_client.session() as session:
                    await session.vector_ops.insert(table_name, {'id': 2, 'embedding': [0.4, 0.5, 0.6]})
                    raise Exception("Test rollback")
            except Exception as e:
                if "Test rollback" not in str(e):
                    raise

            # Verify rollback - only first row
            result = await async_client.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = result.fetchone()[0]
            assert count == 1

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_async_similarity_search_consistency(self, async_client, table_name):
        """Test async similarity_search returns identical results across contexts."""
        try:
            # Create and populate table
            await async_client.execute(
                f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, title VARCHAR(100), embedding VECF32(3))"
            )
            await async_client.vector_ops.insert(table_name, {'id': 1, 'title': 'Doc1', 'embedding': [0.1, 0.2, 0.3]})
            await async_client.vector_ops.insert(table_name, {'id': 2, 'title': 'Doc2', 'embedding': [0.4, 0.5, 0.6]})

            query_vector = [0.1, 0.2, 0.3]

            # AsyncClient mode
            results_client = await async_client.vector_ops.similarity_search(table_name, "embedding", query_vector, limit=2)

            # AsyncSession mode
            async with async_client.session() as session:
                results_session = await session.vector_ops.similarity_search(table_name, "embedding", query_vector, limit=2)

            # Verify identical results
            assert len(results_client) == len(results_session)
            for i in range(len(results_client)):
                assert results_client[i]['id'] == results_session[i]['id']
                assert results_client[i]['title'] == results_session[i]['title']
                assert abs(results_client[i]['distance'] - results_session[i]['distance']) < 0.0001

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


class TestVectorIndexOperations:
    """Test vector index creation and management."""

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
        return f"test_vector_{uuid.uuid4().hex[:8]}"

    def test_ivf_index_lifecycle(self, client, table_name):
        """Test complete IVF index lifecycle: create -> use -> drop."""
        try:
            # Create table
            client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")

            # Insert data
            client.vector_ops.insert(table_name, {'id': 1, 'embedding': [0.1, 0.2, 0.3]})
            client.vector_ops.insert(table_name, {'id': 2, 'embedding': [0.4, 0.5, 0.6]})

            # Create IVF index (lists=2 means 2 cluster centers)
            client.vector_ops.create_ivf(table_name, "idx_test", "embedding", lists=2)

            # Verify index created
            indexes = client.execute(f"SHOW INDEX FROM {table_name}").fetchall()
            assert any('idx_test' in str(row) for row in indexes)

            # Enable IVF with probe_limit=2 (scan all 2 clusters to get all results)
            # probe_limit < lists may return fewer results if data is distributed across clusters
            client.vector_ops.enable_ivf(probe_limit=2)

            # Use IVF - with probe_limit=2 we scan all clusters
            results_with_ivf = client.vector_ops.similarity_search(table_name, "embedding", [0.1, 0.2, 0.3], limit=2)
            assert len(results_with_ivf) == 2  # All data found
            assert 'distance' in results_with_ivf[0]
            assert results_with_ivf[0]['id'] == 1  # Closest result

            # Disable IVF - should get same results without index
            client.vector_ops.disable_ivf()
            results_without_ivf = client.vector_ops.similarity_search(table_name, "embedding", [0.1, 0.2, 0.3], limit=2)
            assert len(results_without_ivf) == 2

            # Both should return same IDs in same order
            assert [r['id'] for r in results_with_ivf] == [r['id'] for r in results_without_ivf]

            # Drop index
            client.vector_ops.drop(table_name, "idx_test")

            # Verify index dropped
            indexes_after = client.execute(f"SHOW INDEX FROM {table_name}").fetchall()
            assert not any('idx_test' in str(row) for row in indexes_after)

        finally:
            try:
                # Ensure IVF is disabled for other tests
                try:
                    client.execute("SET experimental_ivf_index = 0")
                except:
                    pass
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_hnsw_index_lifecycle(self, client, table_name):
        """Test complete HNSW index lifecycle."""
        try:
            # Create table
            client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")

            # Insert data
            client.vector_ops.insert(table_name, {'id': 1, 'embedding': [0.1, 0.2, 0.3]})

            # Try to create HNSW index (may not be supported in all versions)
            try:
                client.vector_ops.create_hnsw(table_name, "idx_hnsw", "embedding", m=16, ef_construction=200)

                # Enable HNSW
                client.vector_ops.enable_hnsw()

                # Search
                results = client.vector_ops.similarity_search(table_name, "embedding", [0.1, 0.2, 0.3], limit=1)
                assert len(results) >= 1

                # Disable and drop
                client.vector_ops.disable_hnsw()
                client.vector_ops.drop(table_name, "idx_hnsw")
            except Exception as e:
                # HNSW may not be supported, skip this test
                if "HNSW" in str(e) or "not supported" in str(e):
                    pytest.skip(f"HNSW index not supported: {e}")
                raise

        finally:
            try:
                # Ensure HNSW is disabled for other tests
                try:
                    client.execute("SET experimental_hnsw_index = 0")
                except:
                    pass
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


class TestVectorStatsOperations:
    """Test vector statistics operations."""

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

    def test_get_ivf_stats_consistency(self, client):
        """Test get_ivf_stats returns same results in Client and Session."""
        table_name = f"test_stats_{uuid.uuid4().hex[:8]}"

        try:
            # Create table and index
            client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")
            client.vector_ops.create_ivf(table_name, "idx_stats", "embedding", lists=2)

            # Insert data
            for i in range(5):
                client.vector_ops.insert(table_name, {'id': i + 1, 'embedding': [i * 0.1, i * 0.2, i * 0.3]})

            # Client mode
            stats_client = client.vector_ops.get_ivf_stats(table_name, column_name="embedding")

            # Session mode
            with client.session() as session:
                stats_session = session.vector_ops.get_ivf_stats(table_name, column_name="embedding")

            # Verify structure
            assert 'index_tables' in stats_client
            assert 'distribution' in stats_client
            assert 'index_tables' in stats_session
            assert 'distribution' in stats_session

            # Verify values match
            assert stats_client['column_name'] == stats_session['column_name']
            assert stats_client['table_name'] == stats_session['table_name']
            assert len(stats_client['distribution']['centroid_id']) == len(stats_session['distribution']['centroid_id'])

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass

    def test_get_ivf_stats_auto_inference(self, client):
        """Test get_ivf_stats with auto-inferred column name."""
        table_name = f"test_auto_{uuid.uuid4().hex[:8]}"

        try:
            # Create table with single vector column
            client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")
            client.vector_ops.create_ivf(table_name, "idx_auto", "embedding", lists=2)
            client.vector_ops.insert(table_name, {'id': 1, 'embedding': [0.1, 0.2, 0.3]})

            # Auto-infer - Client mode
            stats_client = client.vector_ops.get_ivf_stats(table_name)
            assert stats_client['column_name'] == 'embedding'

            # Auto-infer - Session mode
            with client.session() as session:
                stats_session = session.vector_ops.get_ivf_stats(table_name)
                assert stats_session['column_name'] == 'embedding'

            # Verify consistency
            assert stats_client['column_name'] == stats_session['column_name']

        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


class TestAsyncVectorStatsOperations:
    """Test async vector statistics operations."""

    @pytest_asyncio.fixture(scope="function")
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

    @pytest.mark.asyncio
    async def test_async_get_ivf_stats_consistency(self, async_client):
        """Test async get_ivf_stats returns same structure across contexts."""
        table_name = f"test_async_stats_{uuid.uuid4().hex[:8]}"

        try:
            # Create table and index
            await async_client.execute(f"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, embedding VECF32(3))")
            await async_client.vector_ops.create_ivf(table_name, "idx_async_stats", "embedding", lists=2)

            # Insert data
            for i in range(5):
                await async_client.vector_ops.insert(table_name, {'id': i + 1, 'embedding': [i * 0.1, i * 0.2, i * 0.3]})

            # AsyncClient mode
            stats_client = await async_client.vector_ops.get_ivf_stats(table_name, column_name="embedding")

            # AsyncSession mode
            async with async_client.session() as session:
                stats_session = await session.vector_ops.get_ivf_stats(table_name, column_name="embedding")

            # Verify identical structure and values
            assert stats_client['column_name'] == stats_session['column_name']
            assert stats_client['table_name'] == stats_session['table_name']
            assert 'index_tables' in stats_client and 'index_tables' in stats_session
            assert 'distribution' in stats_client and 'distribution' in stats_session

        finally:
            try:
                await async_client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception:
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
