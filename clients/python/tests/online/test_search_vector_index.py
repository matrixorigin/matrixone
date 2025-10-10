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
Test PineconeCompatibleIndex functionality - Pinecone-compatible vector search interface
"""

import pytest
import pytest_asyncio
from matrixone import Client, AsyncClient
from matrixone.search_vector_index import PineconeCompatibleIndex, VectorMatch, QueryResponse


class TestPineconeCompatibleIndex:
    """Test PineconeCompatibleIndex functionality"""

    def test_get_pinecone_index_sync(self, test_client):
        """Test getting PineconeCompatibleIndex from sync client"""
        # Create a test table with vector column
        test_client.execute("CREATE DATABASE IF NOT EXISTS search_vector_test")
        test_client.execute("USE search_vector_test")

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_vectors (
                id VARCHAR(50) PRIMARY KEY,
                title VARCHAR(200),
                content TEXT,
                embedding vecf32(128)
            )
        """
        )

        # Create vector index
        test_client.vector_ops.create_ivf("test_vectors", name="idx_embedding", column="embedding", lists=100)

        try:
            # Get PineconeCompatibleIndex object
            index = test_client.get_pinecone_index("test_vectors", vector_column="embedding")

            assert isinstance(index, PineconeCompatibleIndex)
            assert index.table_name == "test_vectors"
            assert index.vector_column == "embedding"
            assert index._get_id_column() == "id"
            assert index.metadata_columns == ["title", "content"]

        finally:
            # Clean up
            test_client.execute("DROP TABLE test_vectors")
            test_client.execute("DROP DATABASE search_vector_test")

    @pytest.mark.asyncio
    async def test_get_pinecone_index_async(self, test_async_client):
        """Test getting PineconeCompatibleIndex from async client"""
        # Create a test table with vector column
        await test_async_client.execute("CREATE DATABASE IF NOT EXISTS async_search_vector_test")
        await test_async_client.execute("USE async_search_vector_test")

        await test_async_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_vectors_async (
                id VARCHAR(50) PRIMARY KEY,
                title VARCHAR(200),
                content TEXT,
                embedding vecf32(128)
            )
        """
        )

        # Create vector index
        await test_async_client.vector_ops.create_ivf(
            "test_vectors_async",
            name="idx_embedding_async",
            column="embedding",
            lists=100,
        )

        try:
            # Get PineconeCompatibleIndex object
            index = test_async_client.get_pinecone_index("test_vectors_async", vector_column="embedding")

            assert isinstance(index, PineconeCompatibleIndex)
            assert index.table_name == "test_vectors_async"
            assert index.vector_column == "embedding"
            assert await index._get_id_column_async() == "id"
            assert await index._get_metadata_columns_async() == ["title", "content"]

        finally:
            # Clean up
            await test_async_client.execute("DROP TABLE test_vectors_async")
            await test_async_client.execute("DROP DATABASE async_search_vector_test")

    def test_parse_index_info(self, test_client):
        """Test parsing index information from CREATE TABLE statement"""
        # Create a test table with vector column and index
        test_client.execute("CREATE DATABASE IF NOT EXISTS parse_test")
        test_client.execute("USE parse_test")

        # Drop table if exists to ensure clean state
        test_client.execute("DROP TABLE IF EXISTS test_parse")

        test_client.execute(
            """
            CREATE TABLE test_parse (
                id BIGINT PRIMARY KEY,
                title VARCHAR(200),
                embedding vecf32(256)
            )
        """
        )

        # Create vector index
        test_client.vector_ops.create_hnsw("test_parse", name="idx_hnsw", column="embedding", m=16, ef_construction=200)

        try:
            # Get PineconeCompatibleIndex object
            index = test_client.get_pinecone_index("test_parse", vector_column="embedding")

            # Test parsing index info
            index_info = index._get_index_info()

            assert index_info["dimensions"] == 256
            assert index_info["algorithm"] == "hnsw"
            assert index_info["metric"] == "l2"  # vector_l2_ops maps to l2
            assert "m" in index_info["parameters"]
            assert "ef_construction" in index_info["parameters"]

        finally:
            # Clean up
            test_client.execute("DROP TABLE test_parse")
            test_client.execute("DROP DATABASE parse_test")

    def test_query_basic(self, test_client):
        """Test basic query functionality"""
        # Create a test table with vector column
        test_client.execute("CREATE DATABASE IF NOT EXISTS query_test")
        test_client.execute("USE query_test")

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_query (
                id VARCHAR(50) PRIMARY KEY,
                title VARCHAR(200),
                category VARCHAR(50),
                embedding vecf32(64)
            )
        """
        )

        # Create vector index
        test_client.vector_ops.create_ivf("test_query", name="idx_query", column="embedding", lists=10)

        # Insert test data
        test_client.execute(
            """
            INSERT INTO test_query (id, title, category, embedding) VALUES
            ('doc1', 'Machine Learning Guide', 'AI', '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4]'),
            ('doc2', 'Python Programming', 'Programming', '[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5]'),
            ('doc3', 'Database Design', 'Database', '[0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6]')
        """
        )

        try:
            # Get PineconeCompatibleIndex object
            index = test_client.get_pinecone_index("test_query", vector_column="embedding")

            # Test query
            query_vector = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0] * 6 + [
                0.1,
                0.2,
                0.3,
                0.4,
            ]
            results = index.query(query_vector, top_k=2, include_metadata=True)

            assert isinstance(results, QueryResponse)
            assert len(results.matches) <= 2
            assert results.namespace == ""
            assert results.usage is not None

            for match in results.matches:
                assert isinstance(match, VectorMatch)
                assert match.id in ["doc1", "doc2", "doc3"]
                assert isinstance(match.score, float)
                assert "title" in match.metadata
                assert "category" in match.metadata

        finally:
            # Clean up
            test_client.execute("DROP TABLE test_query")
            test_client.execute("DROP DATABASE query_test")

    @pytest.mark.asyncio
    async def test_query_async(self, test_async_client):
        """Test async query functionality"""
        # Create a test table with vector column
        await test_async_client.execute("CREATE DATABASE IF NOT EXISTS async_query_test")
        await test_async_client.execute("USE async_query_test")

        await test_async_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_query_async (
                id VARCHAR(50) PRIMARY KEY,
                title VARCHAR(200),
                embedding vecf32(32)
            )
        """
        )

        # Create vector index
        await test_async_client.vector_ops.create_ivf(
            "test_query_async", name="idx_query_async", column="embedding", lists=5
        )

        # Insert test data
        await test_async_client.execute(
            """
            INSERT INTO test_query_async (id, title, embedding) VALUES
            ('doc1', 'Test Document 1', '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2]'),
            ('doc2', 'Test Document 2', '[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3]')
        """
        )

        try:
            # Get PineconeCompatibleIndex object
            index = test_async_client.get_pinecone_index("test_query_async", vector_column="embedding")

            # Test async query
            query_vector = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0] * 3 + [0.1, 0.2]
            results = await index.query_async(query_vector, top_k=1, include_metadata=True)

            assert isinstance(results, QueryResponse)
            assert len(results.matches) <= 1
            assert results.namespace == ""

            for match in results.matches:
                assert isinstance(match, VectorMatch)
                assert match.id in ["doc1", "doc2"]
                assert isinstance(match.score, float)
                assert "title" in match.metadata

        finally:
            # Clean up
            await test_async_client.execute("DROP TABLE test_query_async")
            await test_async_client.execute("DROP DATABASE async_query_test")

    def test_delete_functionality(self, test_client):
        """Test delete functionality (IVF index only)"""
        # Create a test table with vector column
        test_client.execute("CREATE DATABASE IF NOT EXISTS upsert_test")
        test_client.execute("USE upsert_test")

        # Drop table if exists to ensure clean state
        test_client.execute("DROP TABLE IF EXISTS test_upsert")

        test_client.execute(
            """
            CREATE TABLE test_upsert (
                id VARCHAR(50) PRIMARY KEY,
                title VARCHAR(200),
                embedding vecf32(16)
            )
        """
        )

        # Create IVF vector index (required for upsert/delete operations)
        test_client.vector_ops.create_ivf("test_upsert", name="idx_upsert", column="embedding", lists=5)

        try:
            # Get PineconeCompatibleIndex object
            index = test_client.get_pinecone_index("test_upsert", vector_column="embedding")

            # Insert test data directly using SQL
            test_client.execute(
                """
                INSERT INTO test_upsert (id, title, embedding) VALUES
                ('test1', 'Test Document 1', '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6]'),
                ('test2', 'Test Document 2', '[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7]')
            """
            )

            # Verify data was inserted
            count_result = test_client.execute("SELECT COUNT(*) FROM test_upsert")
            assert count_result.rows[0][0] == 2

            # Test delete with string IDs
            index.delete(["test1"])

            # Verify data was deleted
            count_result = test_client.execute("SELECT COUNT(*) FROM test_upsert")
            assert count_result.rows[0][0] == 1

            # Test delete with mixed ID types (if we had more data)
            # This demonstrates that delete can handle different ID types
            # index.delete([1, "test2", 3.14])  # Mixed types

        finally:
            # Clean up
            test_client.execute("DROP TABLE test_upsert")
            test_client.execute("DROP DATABASE upsert_test")

    def test_describe_index_stats(self, test_client):
        """Test describe index stats functionality"""
        # Create a test table with vector column
        test_client.execute("CREATE DATABASE IF NOT EXISTS stats_test")
        test_client.execute("USE stats_test")

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_stats (
                id VARCHAR(50) PRIMARY KEY,
                title VARCHAR(200),
                embedding vecf32(64)
            )
        """
        )

        # Create vector index
        test_client.vector_ops.create_ivf("test_stats", name="idx_stats", column="embedding", lists=10)

        # Insert test data
        test_client.execute(
            """
            INSERT INTO test_stats (id, title, embedding) VALUES
            ('doc1', 'Document 1', '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4]'),
            ('doc2', 'Document 2', '[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5]')
        """
        )

        try:
            # Get PineconeCompatibleIndex object
            index = test_client.get_pinecone_index("test_stats", vector_column="embedding")

            # Test describe index stats
            stats = index.describe_index_stats()

            assert isinstance(stats, dict)
            assert "dimension" in stats
            assert "total_vector_count" in stats
            assert "namespaces" in stats
            assert stats["dimension"] == 64
            assert stats["total_vector_count"] == 2
            assert "" in stats["namespaces"]
            assert stats["namespaces"][""]["vector_count"] == 2

        finally:
            # Clean up
            test_client.execute("DROP TABLE test_stats")
            test_client.execute("DROP DATABASE stats_test")

    def test_hnsw_upsert_not_supported(self, test_client):
        """Test that HNSW index does not support upsert operations"""
        # Create a test table with vector column
        test_client.execute("CREATE DATABASE IF NOT EXISTS hnsw_upsert_test")
        test_client.execute("USE hnsw_upsert_test")

        # Drop table if exists to ensure clean state
        test_client.execute("DROP TABLE IF EXISTS test_hnsw_upsert")

        test_client.execute(
            """
            CREATE TABLE test_hnsw_upsert (
                id BIGINT PRIMARY KEY,
                title VARCHAR(200),
                embedding vecf32(64)
            )
        """
        )

        # Create HNSW vector index
        test_client.vector_ops.create_hnsw(
            "test_hnsw_upsert",
            name="idx_hnsw_upsert",
            column="embedding",
            m=16,
            ef_construction=200,
        )

        try:
            # Get PineconeCompatibleIndex object
            index = test_client.get_pinecone_index("test_hnsw_upsert", vector_column="embedding")

            # Test that HNSW index only supports query operations
            # (upsert and delete are not supported for HNSW indexes)

            # Test that delete also raises ValueError for HNSW index
            with pytest.raises(ValueError, match="HNSW index does not support delete operations"):
                index.delete(["test1"])

            # Test with different ID types
            with pytest.raises(ValueError, match="HNSW index does not support delete operations"):
                index.delete([1, 2, 3])  # Integer IDs

        finally:
            # Clean up
            test_client.execute("DROP TABLE test_hnsw_upsert")
            test_client.execute("DROP DATABASE hnsw_upsert_test")


class TestPineconeCompatibleIndexCaseInsensitive:
    """Test case-insensitive column name handling in PineconeCompatibleIndex"""

    def test_case_insensitive_column_names_sync(self, test_client):
        """Test that column names are handled case-insensitively in sync client"""
        # Create a test table with mixed case column names
        test_client.execute("CREATE DATABASE IF NOT EXISTS case_test")
        test_client.execute("USE case_test")

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_case_vectors (
                ID VARCHAR(50) PRIMARY KEY,
                Title VARCHAR(200),
                Content TEXT,
                Embedding vecf32(64)
            )
        """
        )

        # Create vector index
        test_client.vector_ops.create_ivf("test_case_vectors", name="idx_case_embedding", column="Embedding", lists=10)

        # Insert test data
        test_client.execute(
            """
            INSERT INTO test_case_vectors (ID, Title, Content, Embedding) VALUES
            ('doc1', 'Test Document 1', 'Content 1', '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4]'),
            ('doc2', 'Test Document 2', 'Content 2', '[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5]')
        """
        )

        try:
            # Test with different case variations of vector column name
            test_cases = [
                "Embedding",  # Original case
                "embedding",  # Lowercase
                "EMBEDDING",  # Uppercase
                "Embedding",  # Mixed case
            ]

            for vector_col in test_cases:
                # Get PineconeCompatibleIndex object
                index = test_client.get_pinecone_index("test_case_vectors", vector_column=vector_col)

                # Test that metadata columns are correctly identified (case-insensitive)
                metadata_cols = index.metadata_columns
                assert "Title" in metadata_cols or "title" in metadata_cols
                assert "Content" in metadata_cols or "content" in metadata_cols
                assert len(metadata_cols) == 2  # Should exclude ID and Embedding

                # Test query functionality
                query_vector = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0] * 6 + [
                    0.1,
                    0.2,
                    0.3,
                    0.4,
                ]
                results = index.query(query_vector, top_k=1, include_metadata=True)

                assert isinstance(results, QueryResponse)
                assert len(results.matches) >= 0

                if results.matches:
                    match = results.matches[0]
                    assert match.id in ["doc1", "doc2"]
                    assert "Title" in match.metadata or "title" in match.metadata
                    assert "Content" in match.metadata or "content" in match.metadata

        finally:
            # Clean up
            test_client.execute("DROP TABLE test_case_vectors")
            test_client.execute("DROP DATABASE case_test")

    @pytest.mark.asyncio
    async def test_case_insensitive_column_names_async(self, test_async_client):
        """Test that column names are handled case-insensitively in async client"""
        # Create a test table with mixed case column names
        await test_async_client.execute("CREATE DATABASE IF NOT EXISTS async_case_test")
        await test_async_client.execute("USE async_case_test")

        await test_async_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_case_vectors_async (
                ID VARCHAR(50) PRIMARY KEY,
                Title VARCHAR(200),
                Content TEXT,
                Embedding vecf32(32)
            )
        """
        )

        # Create vector index
        await test_async_client.vector_ops.create_ivf(
            "test_case_vectors_async",
            name="idx_case_embedding_async",
            column="Embedding",
            lists=5,
        )

        # Insert test data
        await test_async_client.execute(
            """
            INSERT INTO test_case_vectors_async (ID, Title, Content, Embedding) VALUES
            ('doc1', 'Test Document 1', 'Content 1', '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2]'),
            ('doc2', 'Test Document 2', 'Content 2', '[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3]')
        """
        )

        try:
            # Test with different case variations of vector column name
            test_cases = [
                "Embedding",  # Original case
                "embedding",  # Lowercase
                "EMBEDDING",  # Uppercase
                "Embedding",  # Mixed case
            ]

            for vector_col in test_cases:
                # Get PineconeCompatibleIndex object
                index = test_async_client.get_pinecone_index("test_case_vectors_async", vector_column=vector_col)

                # Test that metadata columns are correctly identified (case-insensitive)
                metadata_cols = await index._get_metadata_columns_async()
                assert "Title" in metadata_cols or "title" in metadata_cols
                assert "Content" in metadata_cols or "content" in metadata_cols
                assert len(metadata_cols) == 2  # Should exclude ID and Embedding

                # Test async query functionality
                query_vector = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0] * 3 + [0.1, 0.2]
                results = await index.query_async(query_vector, top_k=1, include_metadata=True)

                assert isinstance(results, QueryResponse)
                assert len(results.matches) >= 0

                if results.matches:
                    match = results.matches[0]
                    assert match.id in ["doc1", "doc2"]
                    assert "Title" in match.metadata or "title" in match.metadata
                    assert "Content" in match.metadata or "content" in match.metadata

        finally:
            # Clean up
            await test_async_client.execute("DROP TABLE test_case_vectors_async")
            await test_async_client.execute("DROP DATABASE async_case_test")
