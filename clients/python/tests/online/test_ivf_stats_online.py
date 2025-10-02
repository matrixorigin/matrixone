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
Online tests for IVF index statistics functionality.
Tests the get_ivf_stats method for both sync and async clients.
"""

import pytest
import pytest_asyncio
import sys
import os
import random

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from matrixone import Client, AsyncClient
from .test_config import online_config


class TestIVFStatsSync:
    """Test IVF stats functionality with synchronous client"""

    @pytest.fixture(scope="class")
    def test_client(self):
        """Create and connect MatrixOne client for testing"""
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

    @pytest.fixture(scope="function")
    def test_table(self, test_client):
        """Create a test table with IVF index"""
        table_name = "test_ivf_stats_table"

        # Drop table if exists
        try:
            test_client.drop_table(table_name)
        except:
            pass

        # Create table with vector column
        test_client.create_table(
            table_name, columns={"id": "int", "title": "varchar(255)", "embedding": "vecf32(128)"}, primary_key="id"
        )

        # Create IVF index
        test_client.vector_ops.create_ivf(table_name, name="idx_test_embedding", column="embedding", lists=5)

        # Insert sample data
        for i in range(30):
            vector = [random.random() for _ in range(128)]
            test_client.vector_ops.insert(table_name, {"id": i + 1, "title": f"Document {i+1}", "embedding": vector})

        yield table_name

        # Cleanup
        try:
            test_client.drop_table(table_name)
        except Exception as e:
            print(f"Warning: Failed to drop table {table_name}: {e}")

    def test_get_ivf_stats_with_column_name(self, test_client, test_table):
        """Test get_ivf_stats with explicit column name"""
        stats = test_client.vector_ops.get_ivf_stats(test_table, "embedding")

        # Verify structure
        assert 'index_tables' in stats
        assert 'distribution' in stats
        assert 'database' in stats
        assert 'table_name' in stats
        assert 'column_name' in stats

        # Verify values
        assert stats['table_name'] == test_table
        assert stats['column_name'] == 'embedding'
        assert stats['database'] == test_client._connection_params['database']

        # Verify index tables
        assert 'metadata' in stats['index_tables']
        assert 'centroids' in stats['index_tables']
        assert 'entries' in stats['index_tables']

        # Verify distribution
        assert 'centroid_count' in stats['distribution']
        assert 'centroid_id' in stats['distribution']
        assert 'centroid_version' in stats['distribution']

        # Verify distribution has data
        assert isinstance(stats['distribution']['centroid_count'], list)
        assert isinstance(stats['distribution']['centroid_id'], list)
        assert isinstance(stats['distribution']['centroid_version'], list)
        assert len(stats['distribution']['centroid_count']) > 0
        assert len(stats['distribution']['centroid_id']) > 0
        assert len(stats['distribution']['centroid_version']) > 0

    def test_get_ivf_stats_auto_inference(self, test_client, test_table):
        """Test get_ivf_stats with auto-inferred column name"""
        stats = test_client.vector_ops.get_ivf_stats(test_table)

        # Verify auto-inference worked
        assert stats['column_name'] == 'embedding'

        # Verify structure
        assert 'index_tables' in stats
        assert 'distribution' in stats

    def test_get_ivf_stats_within_transaction(self, test_client, test_table):
        """Test get_ivf_stats within transaction context"""
        with test_client.transaction() as tx:
            stats = tx.vector_ops.get_ivf_stats(test_table, "embedding")

            # Verify structure
            assert 'index_tables' in stats
            assert 'distribution' in stats
            assert stats['table_name'] == test_table
            assert stats['column_name'] == 'embedding'

    def test_get_ivf_stats_nonexistent_table(self, test_client):
        """Test get_ivf_stats with non-existent table"""
        with pytest.raises(Exception) as exc_info:
            test_client.vector_ops.get_ivf_stats("nonexistent_table", "embedding")

        assert "No IVF index found" in str(exc_info.value)

    def test_get_ivf_stats_no_ivf_index(self, test_client):
        """Test get_ivf_stats on table without IVF index"""
        table_name = "test_no_ivf_index"

        try:
            # Create table without IVF index
            test_client.create_table(table_name, columns={"id": "int", "embedding": "vecf32(128)"}, primary_key="id")

            # Try to get stats - should fail
            with pytest.raises(Exception) as exc_info:
                test_client.vector_ops.get_ivf_stats(table_name, "embedding")

            assert "No IVF index found" in str(exc_info.value)

        finally:
            try:
                test_client.drop_table(table_name)
            except:
                pass

    def test_get_ivf_stats_multiple_vector_columns(self, test_client):
        """Test get_ivf_stats with multiple vector columns"""
        table_name = "test_multi_vector_cols"

        try:
            # Create table with multiple vector columns
            test_client.create_table(
                table_name, columns={"id": "int", "embedding1": "vecf32(128)", "embedding2": "vecf32(256)"}, primary_key="id"
            )

            # Create IVF index on first column
            test_client.vector_ops.create_ivf(table_name, name="idx_embedding1", column="embedding1", lists=3)

            # Insert some data
            for i in range(10):
                vector1 = [random.random() for _ in range(128)]
                vector2 = [random.random() for _ in range(256)]
                test_client.vector_ops.insert(table_name, {"id": i + 1, "embedding1": vector1, "embedding2": vector2})

            # Without column_name - should raise error asking to specify
            with pytest.raises(Exception) as exc_info:
                test_client.vector_ops.get_ivf_stats(table_name)

            assert "Multiple vector columns found" in str(exc_info.value)
            assert "embedding1" in str(exc_info.value) or "embedding2" in str(exc_info.value)

            # With explicit column_name - should work
            stats = test_client.vector_ops.get_ivf_stats(table_name, "embedding1")
            assert stats['column_name'] == 'embedding1'

        finally:
            try:
                test_client.drop_table(table_name)
            except:
                pass

    def test_get_ivf_stats_distribution_details(self, test_client, test_table):
        """Test that distribution details are correctly populated"""
        stats = test_client.vector_ops.get_ivf_stats(test_table, "embedding")

        distribution = stats['distribution']

        # Verify all three lists have the same length
        assert len(distribution['centroid_count']) == len(distribution['centroid_id'])
        assert len(distribution['centroid_count']) == len(distribution['centroid_version'])

        # Verify data types
        for count in distribution['centroid_count']:
            assert isinstance(count, int)

        for centroid_id in distribution['centroid_id']:
            assert isinstance(centroid_id, int)

        for version in distribution['centroid_version']:
            assert isinstance(version, int)

        # Verify total count matches inserted data
        total_vectors = sum(distribution['centroid_count'])
        assert total_vectors == 30  # We inserted 30 vectors in the fixture


class TestIVFStatsAsync:
    """Test IVF stats functionality with asynchronous client"""

    @pytest_asyncio.fixture(scope="function")
    async def test_async_client(self):
        """Create and connect AsyncClient for testing"""
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

    @pytest_asyncio.fixture(scope="function")
    async def test_async_table(self, test_async_client):
        """Create a test table with IVF index for async tests"""
        table_name = "test_async_ivf_stats_table"

        # Drop table if exists
        try:
            await test_async_client.drop_table(table_name)
        except:
            pass

        # Create table with vector column
        await test_async_client.create_table(
            table_name, columns={"id": "int", "title": "varchar(255)", "embedding": "vecf32(128)"}, primary_key="id"
        )

        # Create IVF index
        await test_async_client.vector_ops.create_ivf(
            table_name, name="idx_async_test_embedding", column="embedding", lists=4
        )

        # Insert sample data
        for i in range(20):
            vector = [random.random() for _ in range(128)]
            await test_async_client.vector_ops.insert(
                table_name, {"id": i + 1, "title": f"Async Document {i+1}", "embedding": vector}
            )

        yield table_name

        # Cleanup
        try:
            await test_async_client.drop_table(table_name)
        except Exception as e:
            print(f"Warning: Failed to drop table {table_name}: {e}")

    @pytest.mark.asyncio
    async def test_async_get_ivf_stats_with_column_name(self, test_async_client, test_async_table):
        """Test async get_ivf_stats with explicit column name"""
        stats = await test_async_client.vector_ops.get_ivf_stats(test_async_table, "embedding")

        # Verify structure
        assert 'index_tables' in stats
        assert 'distribution' in stats
        assert 'database' in stats
        assert 'table_name' in stats
        assert 'column_name' in stats

        # Verify values
        assert stats['table_name'] == test_async_table
        assert stats['column_name'] == 'embedding'

        # Verify index tables
        assert 'metadata' in stats['index_tables']
        assert 'centroids' in stats['index_tables']
        assert 'entries' in stats['index_tables']

        # Verify distribution
        assert 'centroid_count' in stats['distribution']
        assert 'centroid_id' in stats['distribution']
        assert 'centroid_version' in stats['distribution']

    @pytest.mark.asyncio
    async def test_async_get_ivf_stats_auto_inference(self, test_async_client, test_async_table):
        """Test async get_ivf_stats with auto-inferred column name"""
        stats = await test_async_client.vector_ops.get_ivf_stats(test_async_table)

        # Verify auto-inference worked
        assert stats['column_name'] == 'embedding'

        # Verify structure
        assert 'index_tables' in stats
        assert 'distribution' in stats

    @pytest.mark.asyncio
    async def test_async_get_ivf_stats_within_transaction(self, test_async_client, test_async_table):
        """Test async get_ivf_stats within transaction context"""
        async with test_async_client.transaction() as tx:
            stats = await tx.vector_ops.get_ivf_stats(test_async_table, "embedding")

            # Verify structure
            assert 'index_tables' in stats
            assert 'distribution' in stats
            assert stats['table_name'] == test_async_table
            assert stats['column_name'] == 'embedding'

    @pytest.mark.asyncio
    async def test_async_get_ivf_stats_distribution_details(self, test_async_client, test_async_table):
        """Test that async distribution details are correctly populated"""
        stats = await test_async_client.vector_ops.get_ivf_stats(test_async_table, "embedding")

        distribution = stats['distribution']

        # Verify all three lists have the same length
        assert len(distribution['centroid_count']) == len(distribution['centroid_id'])
        assert len(distribution['centroid_count']) == len(distribution['centroid_version'])

        # Verify data types
        for count in distribution['centroid_count']:
            assert isinstance(count, int)

        for centroid_id in distribution['centroid_id']:
            assert isinstance(centroid_id, int)

        for version in distribution['centroid_version']:
            assert isinstance(version, int)

        # Verify total count matches inserted data
        total_vectors = sum(distribution['centroid_count'])
        assert total_vectors == 20  # We inserted 20 vectors in the fixture

    @pytest.mark.asyncio
    async def test_async_get_ivf_stats_multiple_vector_columns(self, test_async_client):
        """Test async get_ivf_stats with multiple vector columns"""
        table_name = "test_async_multi_vector_cols"

        try:
            # Create table with multiple vector columns
            await test_async_client.create_table(
                table_name, columns={"id": "int", "embedding1": "vecf32(128)", "embedding2": "vecf32(256)"}, primary_key="id"
            )

            # Create IVF index on first column
            await test_async_client.vector_ops.create_ivf(
                table_name, name="idx_async_embedding1", column="embedding1", lists=3
            )

            # Insert some data
            for i in range(10):
                vector1 = [random.random() for _ in range(128)]
                vector2 = [random.random() for _ in range(256)]
                await test_async_client.vector_ops.insert(
                    table_name, {"id": i + 1, "embedding1": vector1, "embedding2": vector2}
                )

            # Without column_name - should raise error asking to specify
            with pytest.raises(Exception) as exc_info:
                await test_async_client.vector_ops.get_ivf_stats(table_name)

            assert "Multiple vector columns found" in str(exc_info.value)

            # With explicit column_name - should work
            stats = await test_async_client.vector_ops.get_ivf_stats(table_name, "embedding1")
            assert stats['column_name'] == 'embedding1'

        finally:
            try:
                await test_async_client.drop_table(table_name)
            except:
                pass


class TestIVFStatsEdgeCases:
    """Test edge cases for IVF stats functionality"""

    @pytest.fixture(scope="class")
    def test_client(self):
        """Create and connect MatrixOne client for testing"""
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

    def test_get_ivf_stats_empty_table(self, test_client):
        """Test get_ivf_stats on table with IVF index but no data"""
        table_name = "test_empty_ivf_table"

        try:
            # Create table with vector column
            test_client.create_table(table_name, columns={"id": "int", "embedding": "vecf32(64)"}, primary_key="id")

            # Create IVF index
            test_client.vector_ops.create_ivf(table_name, name="idx_empty_embedding", column="embedding", lists=3)

            # Get stats without inserting data
            stats = test_client.vector_ops.get_ivf_stats(table_name, "embedding")

            # Verify structure exists even with empty data
            assert 'index_tables' in stats
            assert 'distribution' in stats

            # Distribution might be empty or have initial structure
            assert isinstance(stats['distribution']['centroid_count'], list)

        finally:
            try:
                test_client.drop_table(table_name)
            except:
                pass

    def test_get_ivf_stats_different_vector_dimensions(self, test_client):
        """Test get_ivf_stats with different vector dimensions (f32 vs f64)"""
        table_name_f32 = "test_ivf_vecf32"
        table_name_f64 = "test_ivf_vecf64"

        try:
            # Test with vecf32
            test_client.create_table(table_name_f32, columns={"id": "int", "embedding": "vecf32(64)"}, primary_key="id")
            test_client.vector_ops.create_ivf(table_name_f32, "idx_f32", "embedding", lists=3)

            # Insert data
            for i in range(15):
                test_client.vector_ops.insert(
                    table_name_f32, {"id": i + 1, "embedding": [random.random() for _ in range(64)]}
                )

            stats_f32 = test_client.vector_ops.get_ivf_stats(table_name_f32, "embedding")
            assert stats_f32['column_name'] == 'embedding'
            assert sum(stats_f32['distribution']['centroid_count']) == 15

            # Test with vecf64
            test_client.create_table(table_name_f64, columns={"id": "int", "embedding": "vecf64(64)"}, primary_key="id")
            test_client.vector_ops.create_ivf(table_name_f64, "idx_f64", "embedding", lists=3)

            # Insert data
            for i in range(15):
                test_client.vector_ops.insert(
                    table_name_f64, {"id": i + 1, "embedding": [random.random() for _ in range(64)]}
                )

            stats_f64 = test_client.vector_ops.get_ivf_stats(table_name_f64, "embedding")
            assert stats_f64['column_name'] == 'embedding'
            assert sum(stats_f64['distribution']['centroid_count']) == 15

        finally:
            try:
                test_client.drop_table(table_name_f32)
            except:
                pass
            try:
                test_client.drop_table(table_name_f64)
            except:
                pass

    def test_get_ivf_stats_no_vector_column(self, test_client):
        """Test get_ivf_stats on table without vector columns"""
        table_name = "test_no_vector_col"

        try:
            # Create table without vector column
            test_client.create_table(table_name, columns={"id": "int", "name": "varchar(100)"}, primary_key="id")

            # Try to get stats - should fail
            with pytest.raises(Exception) as exc_info:
                test_client.vector_ops.get_ivf_stats(table_name)

            assert "No vector columns found" in str(exc_info.value)

        finally:
            try:
                test_client.drop_table(table_name)
            except:
                pass


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
