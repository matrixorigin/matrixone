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
Online tests for Pinecone-compatible filter functionality in vector search.
"""

import pytest
import asyncio
from typing import List, Dict, Any
from matrixone import Client, AsyncClient
from matrixone.sqlalchemy_ext import create_vector_column
from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime

Base = declarative_base()


class MovieDocument(Base):
    """Test model for movie documents with metadata"""

    __tablename__ = 'test_movies'

    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    genre = Column(String(50))
    year = Column(Integer)
    rating = Column(Float)
    director = Column(String(100))
    embedding = create_vector_column(64, "f32")
    created_at = Column(DateTime, default=datetime.now)


class TestPineconeFilter:
    """Test Pinecone-compatible filter functionality"""

    @pytest.fixture(scope="class")
    def client(self):
        """Create test client"""
        return Client(host="127.0.0.1", port=6001, user="dump", password="111", database="test")

    @pytest.fixture(scope="class")
    def async_client(self):
        """Create test async client"""
        return AsyncClient()

    @pytest.fixture(scope="class")
    def test_database(self, client):
        """Create test database"""
        db_name = "test_pinecone_filter_db"
        try:
            client.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            client.execute(f"USE {db_name}")
            yield db_name
        finally:
            try:
                client.execute(f"DROP DATABASE IF EXISTS {db_name}")
            except Exception as e:
                print(f"Cleanup failed: {e}")

    @pytest.fixture(scope="class")
    def test_data_setup(self, client, test_database):
        """Set up test data"""
        # Create tables
        client.create_all(Base)

        # Enable vector index
        client.vector_ops.enable_ivf()

        # Create vector index
        client.vector_ops.create_ivf(
            "test_movies",
            name="movies_ivf_index",
            column="embedding",
            op_type="vector_l2_ops",
        )

        # Insert test data
        test_movies = [
            {
                "id": 1,
                "title": "The Matrix",
                "genre": "action",
                "year": 1999,
                "rating": 8.7,
                "director": "Lana Wachowski",
                "embedding": [0.1] * 64,
            },
            {
                "id": 2,
                "title": "Inception",
                "genre": "sci-fi",
                "year": 2010,
                "rating": 8.8,
                "director": "Christopher Nolan",
                "embedding": [0.2] * 64,
            },
            {
                "id": 3,
                "title": "The Dark Knight",
                "genre": "action",
                "year": 2008,
                "rating": 9.0,
                "director": "Christopher Nolan",
                "embedding": [0.3] * 64,
            },
            {
                "id": 4,
                "title": "Interstellar",
                "genre": "sci-fi",
                "year": 2014,
                "rating": 8.6,
                "director": "Christopher Nolan",
                "embedding": [0.4] * 64,
            },
            {
                "id": 5,
                "title": "Pulp Fiction",
                "genre": "crime",
                "year": 1994,
                "rating": 8.9,
                "director": "Quentin Tarantino",
                "embedding": [0.5] * 64,
            },
        ]

        client.vector_ops.batch_insert("test_movies", test_movies)

        yield test_movies

        # Cleanup
        try:
            client.drop_all(Base)
        except Exception as e:
            print(f"Cleanup failed: {e}")

    def test_basic_filter_equality(self, client, test_data_setup):
        """Test basic equality filter"""
        # Create Pinecone-compatible index
        index = client.get_pinecone_index("test_movies", "embedding")

        # Test filter by genre
        query_vector = [0.15] * 64
        results = index.query(vector=query_vector, top_k=10, filter={"genre": "action"})

        assert len(results.matches) == 2  # The Matrix and The Dark Knight
        for match in results.matches:
            assert match.metadata["genre"] == "action"

    def test_filter_with_operators(self, client, test_data_setup):
        """Test filter with various operators"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test $gt operator
        results = index.query(vector=query_vector, top_k=10, filter={"year": {"$gt": 2000}})

        assert len(results.matches) >= 3  # Inception, The Dark Knight, Interstellar
        for match in results.matches:
            assert match.metadata["year"] > 2000

        # Test $gte operator
        results = index.query(vector=query_vector, top_k=10, filter={"rating": {"$gte": 8.8}})

        assert len(results.matches) >= 2  # Inception and The Dark Knight
        for match in results.matches:
            assert match.metadata["rating"] >= 8.8

    def test_filter_with_in_operator(self, client, test_data_setup):
        """Test filter with $in operator"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test $in operator
        results = index.query(vector=query_vector, top_k=10, filter={"genre": {"$in": ["action", "sci-fi"]}})

        assert len(results.matches) == 4  # The Matrix, Inception, The Dark Knight, Interstellar
        for match in results.matches:
            assert match.metadata["genre"] in ["action", "sci-fi"]

    def test_filter_with_and_operator(self, client, test_data_setup):
        """Test filter with $and operator"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test $and operator
        results = index.query(
            vector=query_vector,
            top_k=10,
            filter={"$and": [{"genre": "sci-fi"}, {"year": {"$gte": 2010}}]},
        )

        assert len(results.matches) == 2  # Inception and Interstellar
        for match in results.matches:
            assert match.metadata["genre"] == "sci-fi"
            assert match.metadata["year"] >= 2010

    def test_filter_with_or_operator(self, client, test_data_setup):
        """Test filter with $or operator"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test $or operator
        results = index.query(
            vector=query_vector,
            top_k=10,
            filter={"$or": [{"director": "Christopher Nolan"}, {"rating": {"$gte": 8.9}}]},
        )

        assert len(results.matches) >= 3  # Inception, The Dark Knight, Interstellar, Pulp Fiction
        for match in results.matches:
            assert match.metadata["director"] == "Christopher Nolan" or match.metadata["rating"] >= 8.9

    def test_filter_with_multiple_conditions(self, client, test_data_setup):
        """Test filter with multiple conditions"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test multiple conditions
        results = index.query(vector=query_vector, top_k=10, filter={"genre": "action", "year": {"$gte": 2000}})

        assert len(results.matches) == 1  # Only The Dark Knight
        for match in results.matches:
            assert match.metadata["genre"] == "action"
            assert match.metadata["year"] >= 2000

    def test_filter_with_nin_operator(self, client, test_data_setup):
        """Test filter with $nin (not in) operator"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test $nin operator
        results = index.query(vector=query_vector, top_k=10, filter={"genre": {"$nin": ["action", "sci-fi"]}})

        assert len(results.matches) == 1  # Only Pulp Fiction
        for match in results.matches:
            assert match.metadata["genre"] not in ["action", "sci-fi"]

    def test_filter_with_range_operators(self, client, test_data_setup):
        """Test filter with range operators"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test $lt operator
        results = index.query(vector=query_vector, top_k=10, filter={"year": {"$lt": 2000}})

        assert len(results.matches) == 2  # The Matrix and Pulp Fiction
        for match in results.matches:
            assert match.metadata["year"] < 2000

        # Test $lte operator
        results = index.query(vector=query_vector, top_k=10, filter={"rating": {"$lte": 8.7}})

        assert len(results.matches) >= 2  # The Matrix and Interstellar
        for match in results.matches:
            assert match.metadata["rating"] <= 8.7

    def test_filter_with_ne_operator(self, client, test_data_setup):
        """Test filter with $ne (not equal) operator"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test $ne operator
        results = index.query(vector=query_vector, top_k=10, filter={"director": {"$ne": "Christopher Nolan"}})

        assert len(results.matches) == 2  # The Matrix and Pulp Fiction
        for match in results.matches:
            assert match.metadata["director"] != "Christopher Nolan"

    @pytest.mark.asyncio
    async def test_async_filter_functionality(self, async_client, test_data_setup):
        """Test async filter functionality"""
        # Connect async client
        await async_client.connect(
            host="127.0.0.1",
            port=6001,
            user="dump",
            password="111",
            database="test_pinecone_filter_db",
        )

        try:
            # Create async Pinecone-compatible index
            index = async_client.get_pinecone_index("test_movies", "embedding")

            # Test basic filter
            query_vector = [0.15] * 64
            results = await index.query_async(vector=query_vector, top_k=10, filter={"genre": "action"})

            assert len(results.matches) == 2  # The Matrix and The Dark Knight
            for match in results.matches:
                assert match.metadata["genre"] == "action"
        finally:
            # Properly disconnect async client to avoid event loop warnings
            await async_client.disconnect()

    def test_filter_with_complex_nested_conditions(self, client, test_data_setup):
        """Test filter with complex nested conditions"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test complex nested conditions
        results = index.query(
            vector=query_vector,
            top_k=10,
            filter={
                "$and": [
                    {"genre": {"$in": ["action", "sci-fi"]}},
                    {"$or": [{"year": {"$gte": 2010}}, {"rating": {"$gte": 8.9}}]},
                ]
            },
        )

        # Should match: Inception (sci-fi, 2010), The Dark Knight (action, rating 9.0), Interstellar (sci-fi, 2014)
        assert len(results.matches) >= 3
        for match in results.matches:
            assert match.metadata["genre"] in ["action", "sci-fi"]
            assert match.metadata["year"] >= 2010 or match.metadata["rating"] >= 8.9

    def test_filter_with_no_results(self, client, test_data_setup):
        """Test filter that returns no results"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test filter that should return no results
        results = index.query(vector=query_vector, top_k=10, filter={"genre": "horror"})

        assert len(results.matches) == 0

    def test_filter_without_filter_parameter(self, client, test_data_setup):
        """Test query without filter parameter (should return all results)"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test without filter
        results = index.query(vector=query_vector, top_k=10)

        assert len(results.matches) == 5  # All movies
        assert results.usage["read_units"] == 5

    def test_query_with_include_metadata_false(self, client, test_data_setup):
        """Test query with include_metadata=False"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        results = index.query(vector=query_vector, top_k=3, include_metadata=False)

        assert len(results.matches) == 3
        for match in results.matches:
            assert match.metadata == {}  # No metadata should be included
            assert match.id is not None
            assert match.score is not None

    def test_query_with_include_values_true(self, client, test_data_setup):
        """Test query with include_values=True"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        results = index.query(vector=query_vector, top_k=2, include_values=True)

        assert len(results.matches) == 2
        for match in results.matches:
            assert match.values is not None
            # Values might be returned as string representation of vector
            if isinstance(match.values, str):
                # Parse string representation like "[0.1,0.2,...]"
                import ast

                values_list = ast.literal_eval(match.values)
                assert isinstance(values_list, list)
                assert len(values_list) == 64
            else:
                assert isinstance(match.values, list)
                assert len(match.values) == 64

    def test_query_with_include_metadata_and_values_false(self, client, test_data_setup):
        """Test query with both include_metadata=False and include_values=False"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        results = index.query(vector=query_vector, top_k=2, include_metadata=False, include_values=False)

        assert len(results.matches) == 2
        for match in results.matches:
            assert match.metadata == {}
            assert match.values is None
            assert match.id is not None
            assert match.score is not None

    def test_query_with_top_k_one(self, client, test_data_setup):
        """Test query with top_k=1"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        results = index.query(vector=query_vector, top_k=1)

        assert len(results.matches) == 1
        assert results.usage["read_units"] == 1

    def test_query_with_namespace_parameter(self, client, test_data_setup):
        """Test query with namespace parameter (should be ignored but not cause errors)"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        results = index.query(
            vector=query_vector,
            top_k=3,
            namespace="test_namespace",  # Should be ignored in MatrixOne
        )

        assert len(results.matches) == 3
        assert results.namespace == "test_namespace"

    def test_filter_with_string_numbers(self, client, test_data_setup):
        """Test filter with string numbers"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test with string year
        results = index.query(vector=query_vector, top_k=10, filter={"year": "2010"})

        assert len(results.matches) == 1  # Only Inception
        assert results.matches[0].metadata["year"] == 2010

    def test_filter_with_float_comparison(self, client, test_data_setup):
        """Test filter with float comparisons"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test with float rating
        results = index.query(vector=query_vector, top_k=10, filter={"rating": {"$gt": 8.7}})

        assert len(results.matches) >= 2  # The Matrix (8.7), Inception (8.8), The Dark Knight (9.0)
        for match in results.matches:
            assert match.metadata["rating"] > 8.7

    def test_filter_with_boolean_like_values(self, client, test_data_setup):
        """Test filter with boolean-like values"""
        # First, let's add some test data with boolean-like values
        client.execute(
            """
            INSERT INTO test_movies (id, title, genre, year, rating, director, embedding) VALUES
            (6, 'Test Movie 1', 'action', 2020, 7.5, 'Test Director', '[0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6]')
        """
        )

        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test with string comparison
        results = index.query(vector=query_vector, top_k=10, filter={"director": "Test Director"})

        assert len(results.matches) == 1
        assert results.matches[0].metadata["director"] == "Test Director"

    def test_filter_with_special_characters(self, client, test_data_setup):
        """Test filter with special characters in values"""
        # Add test data with special characters
        client.execute(
            """
            INSERT INTO test_movies (id, title, genre, year, rating, director, embedding) VALUES
            (7, 'Movie with "quotes"', 'drama', 2021, 8.0, 'Director with apostrophe''s name', '[0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7]')
        """
        )

        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test with special characters in filter
        results = index.query(vector=query_vector, top_k=10, filter={"director": "Director with apostrophe's name"})

        assert len(results.matches) == 1
        assert results.matches[0].metadata["director"] == "Director with apostrophe's name"

    def test_filter_with_large_in_list(self, client, test_data_setup):
        """Test filter with large $in list"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test with large $in list
        large_list = [str(i) for i in range(1000)]  # Large list
        large_list.extend(["action", "sci-fi", "crime"])  # Include actual values

        results = index.query(vector=query_vector, top_k=10, filter={"genre": {"$in": large_list}})

        # Should match all movies (including any added in previous tests)
        assert len(results.matches) >= 5  # At least the original 5 movies

    def test_filter_with_empty_in_list(self, client, test_data_setup):
        """Test filter with empty $in list"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        results = index.query(
            vector=query_vector,
            top_k=10,
            filter={"genre": {"$in": []}},  # Empty list should return no results
        )

        assert len(results.matches) == 0

    def test_filter_with_empty_nin_list(self, client, test_data_setup):
        """Test filter with empty $nin list"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        results = index.query(
            vector=query_vector,
            top_k=10,
            filter={"genre": {"$nin": []}},  # Empty list should return all results
        )

        assert len(results.matches) >= 5  # All movies should match

    def test_filter_with_mixed_data_types_in_list(self, client, test_data_setup):
        """Test filter with mixed data types in $in list"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test with mixed types (should work with string conversion)
        results = index.query(
            vector=query_vector,
            top_k=10,
            filter={"year": {"$in": [1999, "2010", 2008]}},  # Mixed int and string
        )

        assert len(results.matches) == 3  # The Matrix, Inception, The Dark Knight

    def test_filter_with_deeply_nested_conditions(self, client, test_data_setup):
        """Test filter with deeply nested $and and $or conditions"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test deeply nested conditions
        results = index.query(
            vector=query_vector,
            top_k=10,
            filter={
                "$and": [
                    {"$or": [{"genre": "action"}, {"genre": "sci-fi"}]},
                    {
                        "$and": [
                            {"year": {"$gte": 2000}},
                            {"$or": [{"rating": {"$gte": 8.8}}, {"director": "Christopher Nolan"}]},
                        ]
                    },
                ]
            },
        )

        # Should match: Inception (sci-fi, 2010, 8.8), The Dark Knight (action, 2008, 9.0, Christopher Nolan), Interstellar (sci-fi, 2014, Christopher Nolan)
        assert len(results.matches) >= 3
        for match in results.matches:
            assert match.metadata["genre"] in ["action", "sci-fi"]
            assert match.metadata["year"] >= 2000
            assert match.metadata["rating"] >= 8.8 or match.metadata["director"] == "Christopher Nolan"

    @pytest.mark.asyncio
    async def test_async_query_with_all_parameters(self, async_client, test_data_setup):
        """Test async query with all parameters"""
        await async_client.connect(
            host="127.0.0.1",
            port=6001,
            user="dump",
            password="111",
            database="test_pinecone_filter_db",
        )

        try:
            index = async_client.get_pinecone_index("test_movies", "embedding")
            query_vector = [0.15] * 64

            results = await index.query_async(
                vector=query_vector,
                top_k=2,
                include_metadata=True,
                include_values=True,
                filter={"genre": "action"},
                namespace="async_test",
            )

            assert len(results.matches) >= 1  # At least one action movie
            assert results.namespace == "async_test"
            for match in results.matches:
                assert match.metadata["genre"] == "action"
                if match.values is not None:
                    # Values might be returned as string representation of vector
                    if isinstance(match.values, str):
                        import ast

                        values_list = ast.literal_eval(match.values)
                        assert len(values_list) == 64
                    else:
                        assert len(match.values) == 64
        finally:
            await async_client.disconnect()

    @pytest.mark.asyncio
    async def test_async_query_with_complex_filter(self, async_client, test_data_setup):
        """Test async query with complex filter"""
        await async_client.connect(
            host="127.0.0.1",
            port=6001,
            user="dump",
            password="111",
            database="test_pinecone_filter_db",
        )

        try:
            index = async_client.get_pinecone_index("test_movies", "embedding")
            query_vector = [0.15] * 64

            results = await index.query_async(
                vector=query_vector,
                top_k=10,
                filter={
                    "$and": [
                        {"year": {"$gte": 2008}},
                        {"$or": [{"rating": {"$gte": 8.8}}, {"director": "Christopher Nolan"}]},
                    ]
                },
            )

            # Should match: Inception (2010, 8.8), The Dark Knight (2008, 9.0, Christopher Nolan), Interstellar (2014, Christopher Nolan)
            assert len(results.matches) >= 3
            for match in results.matches:
                assert match.metadata["year"] >= 2008
                assert match.metadata["rating"] >= 8.8 or match.metadata["director"] == "Christopher Nolan"
        finally:
            await async_client.disconnect()

    def test_edge_case_empty_table(self, client, test_data_setup):
        """Test query on empty table"""
        # Create empty table
        client.execute(
            """
            CREATE TABLE empty_movies (
                id INT PRIMARY KEY,
                title VARCHAR(200),
                embedding VECF32(64)
            )
        """
        )

        index = client.get_pinecone_index("empty_movies", "embedding")
        query_vector = [0.15] * 64

        results = index.query(vector=query_vector, top_k=10)

        assert len(results.matches) == 0
        assert results.usage["read_units"] == 0

    def test_edge_case_single_record(self, client, test_data_setup):
        """Test query on table with single record"""
        # Create table with single record
        client.execute(
            """
            CREATE TABLE single_movie (
                id INT PRIMARY KEY,
                title VARCHAR(200),
                embedding VECF32(64)
            )
        """
        )

        client.execute(
            """
            INSERT INTO single_movie (id, title, embedding) VALUES
            (1, 'Single Movie', '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]')
        """
        )

        index = client.get_pinecone_index("single_movie", "embedding")
        query_vector = [0.5] * 64  # Exact match

        results = index.query(vector=query_vector, top_k=10)

        assert len(results.matches) == 1
        assert results.matches[0].metadata["title"] == "Single Movie"
        assert results.matches[0].score == 0.0  # Should be exact match

    def test_performance_large_top_k(self, client, test_data_setup):
        """Test performance with large top_k value"""
        index = client.get_pinecone_index("test_movies", "embedding")
        query_vector = [0.15] * 64

        # Test with very large top_k
        results = index.query(vector=query_vector, top_k=10000)  # Very large number

        # Should return all available records (at least 5, possibly more from previous tests)
        assert len(results.matches) >= 5
        assert results.usage["read_units"] == len(results.matches)


if __name__ == "__main__":
    pytest.main([__file__])
