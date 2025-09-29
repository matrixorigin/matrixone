"""
Online tests for vector operations (create, search, order by, limit).
Uses real database connections to test actual functionality.
"""

import pytest
import sys
import os
from sqlalchemy import create_engine, text, select, and_, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from matrixone import Client
from matrixone.sqlalchemy_ext import (
    VectorType,
    Vectorf32,
    Vectorf64,
    VectorColumn,
    create_vector_column,
    vector_distance_functions,
)
from matrixone.config import get_connection_params
from .test_config import online_config


class TestVectorOperationsOnline:
    """Test vector operations with real database connections."""

    @pytest.fixture(scope="class")
    def client(self):
        """Create and connect MatrixOne client."""
        from matrixone.config import get_connection_params
        from matrixone.logger import create_default_logger

        # Get connection parameters
        host, port, user, password, database = get_connection_params()

        # Create logger
        logger = create_default_logger()

        # Create client and connect
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect(host=host, port=port, user=user, password=password, database=database)

        yield client

        # Cleanup
        if hasattr(client, 'disconnect'):
            client.disconnect()

    @pytest.fixture(scope="class")
    def engine(self, client):
        """Get SQLAlchemy engine from client."""
        return client.get_sqlalchemy_engine()

    @pytest.fixture(scope="class")
    def Base(self):
        """Create declarative base for each test class."""
        return declarative_base()

    @pytest.fixture(scope="class")
    def Session(self, engine):
        """Create session maker."""
        return sessionmaker(bind=engine)

    def setup_method(self):
        """Set up test data and clean up before each test."""
        pass

    def teardown_method(self):
        """Clean up after each test."""
        pass

    def test_vector_table_creation_and_drop(self, client, Base, Session):
        """Test creating and dropping vector tables using client interface."""

        class VectorTestTable(Base):
            __tablename__ = 'test_vector_ops_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(10, "f32")
            description = Column(String(200))
            category = Column(String(50))

        # Drop table if exists
        client.drop_all(Base)

        # Create table using client interface
        client.create_all(Base)

        # Verify table exists
        result = client.execute("SHOW TABLES LIKE 'test_vector_ops_table'")
        assert result.fetchone() is not None

        # Drop table using client interface
        client.drop_all(Base)

        # Verify table is dropped
        result = client.execute("SHOW TABLES LIKE 'test_vector_ops_table'")
        assert result.fetchone() is None

    def test_vector_data_insertion_and_retrieval(self, client, Base, Session):
        """Test inserting and retrieving vector data using ORM."""

        class VectorDataTable(Base):
            __tablename__ = 'test_vector_data_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(5, "f32")
            description = Column(String(200))

        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

        # Insert test data using ORM
        session = Session()
        try:
            test_docs = [
                VectorDataTable(embedding=[1.0, 2.0, 3.0, 4.0, 5.0], description="Document A"),
                VectorDataTable(embedding=[6.0, 7.0, 8.0, 9.0, 10.0], description="Document B"),
                VectorDataTable(embedding=[11.0, 12.0, 13.0, 14.0, 15.0], description="Document C"),
            ]

            session.add_all(test_docs)
            session.commit()

            # Retrieve and verify data
            docs = session.query(VectorDataTable).all()
            assert len(docs) == 3

            # Verify vector data
            for i, doc in enumerate(docs):
                assert doc.description == f"Document {chr(65 + i)}"  # A, B, C
                assert len(doc.embedding) == 5

        finally:
            session.close()

        # Clean up using client interface
        client.drop_all(Base)

    def test_vector_search_l2_distance(self, client, Base, Session):
        """Test vector search using L2 distance with ORM and client interface."""

        class VectorSearchTable(Base):
            __tablename__ = 'test_vector_search_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(3, "f32")
            description = Column(String(200))

        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

        # Insert test data using ORM
        session = Session()
        try:
            test_docs = [
                VectorSearchTable(embedding=[1.0, 0.0, 0.0], description="Unit vector X"),
                VectorSearchTable(embedding=[0.0, 1.0, 0.0], description="Unit vector Y"),
                VectorSearchTable(embedding=[0.0, 0.0, 1.0], description="Unit vector Z"),
                VectorSearchTable(embedding=[1.0, 1.0, 0.0], description="Diagonal vector XY"),
                VectorSearchTable(embedding=[0.0, 1.0, 1.0], description="Diagonal vector YZ"),
            ]

            session.add_all(test_docs)
            session.commit()

            # Search using L2 distance with ORM
            query_vector = [1.0, 1.0, 0.0]  # Should match diagonal vector XY

            # Find most similar document using ORM
            results = (
                session.query(VectorSearchTable)
                .order_by(VectorSearchTable.embedding.l2_distance(query_vector))
                .limit(1)
                .all()
            )

            assert len(results) == 1
            assert results[0].description == "Diagonal vector XY"

            # Search with distance threshold using ORM
            results = (
                session.query(VectorSearchTable).filter(VectorSearchTable.embedding.l2_distance(query_vector) < 1.0).all()
            )

            assert len(results) >= 1

            # Also test using ORM with different query patterns
            # Test with explicit column selection
            orm_results = (
                session.query(
                    VectorSearchTable.id,
                    VectorSearchTable.description,
                    VectorSearchTable.embedding.l2_distance(query_vector).label('distance'),
                )
                .order_by(VectorSearchTable.embedding.l2_distance(query_vector))
                .limit(3)
                .all()
            )

            assert len(orm_results) >= 1
            assert orm_results[0][1] == "Diagonal vector XY"  # description column

        finally:
            session.close()

        # Clean up using client interface
        client.drop_all(Base)

    def test_vector_search_cosine_distance(self, client, Base, Session):
        """Test vector search using cosine distance."""

        class VectorCosineTable(Base):
            __tablename__ = 'test_vector_cosine_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(3, "f32")
            description = Column(String(200))

        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

        # Insert test data
        session = Session()
        try:
            test_docs = [
                VectorCosineTable(embedding=[1.0, 0.0, 0.0], description="Unit vector X"),
                VectorCosineTable(embedding=[0.0, 1.0, 0.0], description="Unit vector Y"),
                VectorCosineTable(embedding=[1.0, 1.0, 0.0], description="Diagonal vector XY"),
                VectorCosineTable(embedding=[2.0, 2.0, 0.0], description="Scaled diagonal vector"),
            ]

            for doc in test_docs:
                session.add(doc)
            session.commit()

            # Search using cosine distance
            query_vector = [1.0, 1.0, 0.0]

            # Find most similar document
            results = (
                session.query(VectorCosineTable)
                .order_by(VectorCosineTable.embedding.cosine_distance(query_vector))
                .limit(1)
                .all()
            )

            assert len(results) == 1
            # Should match diagonal vector XY (cosine distance = 0)
            assert results[0].description == "Diagonal vector XY"

        finally:
            session.close()

        # Clean up using client interface
        client.drop_all(Base)

    def test_vector_search_l2_distance_squared(self, client, Base, Session):
        """Test vector search using L2 distance squared."""

        class VectorL2SqTable(Base):
            __tablename__ = 'test_vector_l2sq_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(3, "f32")
            description = Column(String(200))

        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

        # Insert test data
        session = Session()
        try:
            test_docs = [
                VectorL2SqTable(embedding=[1.0, 0.0, 0.0], description="Unit vector X"),
                VectorL2SqTable(embedding=[0.0, 1.0, 0.0], description="Unit vector Y"),
                VectorL2SqTable(embedding=[1.0, 1.0, 0.0], description="Diagonal vector XY"),
                VectorL2SqTable(embedding=[2.0, 0.0, 0.0], description="Scaled vector X"),
            ]

            for doc in test_docs:
                session.add(doc)
            session.commit()

            # Search using L2 distance squared
            query_vector = [1.0, 1.0, 0.0]

            # Find most similar document
            results = (
                session.query(VectorL2SqTable)
                .order_by(VectorL2SqTable.embedding.l2_distance_sq(query_vector))
                .limit(1)
                .all()
            )

            assert len(results) == 1
            assert results[0].description == "Diagonal vector XY"

        finally:
            session.close()

        # Clean up using client interface
        client.drop_all(Base)

    def test_vector_search_complex_query(self, client, Base, Session):
        """Test complex vector search with multiple criteria."""

        class VectorComplexTable(Base):
            __tablename__ = 'test_vector_complex_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(3, "f32")
            description = Column(String(200))
            category = Column(String(50))
            score = Column(Float)

        # Clean up and create table
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

        # Insert test data
        session = Session()
        try:
            test_docs = [
                VectorComplexTable(
                    embedding=[1.0, 0.0, 0.0],
                    description="Science Doc A",
                    category="science",
                    score=0.9,
                ),
                VectorComplexTable(
                    embedding=[0.0, 1.0, 0.0],
                    description="Science Doc B",
                    category="science",
                    score=0.8,
                ),
                VectorComplexTable(embedding=[1.0, 1.0, 0.0], description="Tech Doc A", category="tech", score=0.7),
                VectorComplexTable(embedding=[0.0, 0.0, 1.0], description="Tech Doc B", category="tech", score=0.6),
                VectorComplexTable(
                    embedding=[1.0, 1.0, 1.0],
                    description="Science Doc C",
                    category="science",
                    score=0.5,
                ),
            ]

            for doc in test_docs:
                session.add(doc)
            session.commit()

            # Complex search: science category, L2 distance < 2.0, ordered by L2 distance
            query_vector = [1.0, 1.0, 0.0]

            results = (
                session.query(VectorComplexTable)
                .filter(
                    and_(
                        VectorComplexTable.category == "science",
                        VectorComplexTable.embedding.l2_distance(query_vector) < 2.0,
                    )
                )
                .order_by(VectorComplexTable.embedding.l2_distance(query_vector))
                .all()
            )

            assert len(results) >= 1
            # All results should be science category
            for result in results:
                assert result.category == "science"

        finally:
            session.close()

        # Clean up
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

    def test_vector_batch_insertion_and_search(self, client, Base, Session):
        """Test batch insertion and search operations."""

        class VectorBatchTable(Base):
            __tablename__ = 'test_vector_batch_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(4, "f32")
            description = Column(String(200))

        # Clean up and create table
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

        # Batch insert test data
        session = Session()
        try:
            batch_data = [{"embedding": [1.0, 2.0, 3.0, 4.0], "description": f"Batch Doc {i}"} for i in range(10)]

            session.bulk_insert_mappings(VectorBatchTable, batch_data)
            session.commit()

            # Verify batch insertion
            count = session.query(VectorBatchTable).count()
            assert count == 10

            # Search in batch data
            query_vector = [1.0, 2.0, 3.0, 4.0]

            # Find top 3 most similar
            results = (
                session.query(VectorBatchTable).order_by(VectorBatchTable.embedding.l2_distance(query_vector)).limit(3).all()
            )

            assert len(results) == 3

        finally:
            session.close()

        # Clean up
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

    def test_vector_search_with_limit_and_offset(self, client, Base, Session):
        """Test vector search with limit and offset for pagination."""

        class VectorPaginationTable(Base):
            __tablename__ = 'test_vector_pagination_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(3, "f32")
            description = Column(String(200))

        # Clean up and create table
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

        # Insert test data
        session = Session()
        try:
            test_docs = [
                VectorPaginationTable(embedding=[i * 0.1, i * 0.2, i * 0.3], description=f"Doc {i}") for i in range(20)
            ]

            for doc in test_docs:
                session.add(doc)
            session.commit()

            # Test pagination
            query_vector = [1.0, 2.0, 3.0]

            # First page
            page1 = (
                session.query(VectorPaginationTable)
                .order_by(VectorPaginationTable.embedding.l2_distance(query_vector))
                .limit(5)
                .offset(0)
                .all()
            )

            # Second page
            page2 = (
                session.query(VectorPaginationTable)
                .order_by(VectorPaginationTable.embedding.l2_distance(query_vector))
                .limit(5)
                .offset(5)
                .all()
            )

            assert len(page1) == 5
            assert len(page2) == 5

            # Pages should be different
            page1_ids = {doc.id for doc in page1}
            page2_ids = {doc.id for doc in page2}
            assert page1_ids.isdisjoint(page2_ids)

        finally:
            session.close()

        # Clean up
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

    def test_vector_search_performance(self, client, Base, Session):
        """Test vector search performance with larger dataset."""

        class VectorPerfTable(Base):
            __tablename__ = 'test_vector_perf_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(10, "f32")
            description = Column(String(200))

        # Clean up and create table
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

        # Insert larger dataset
        session = Session()
        try:
            # Insert 100 documents
            test_docs = [
                VectorPerfTable(embedding=[i * 0.1 for i in range(10)], description=f"Performance Doc {i}")
                for i in range(100)
            ]

            for doc in test_docs:
                session.add(doc)
            session.commit()

            # Test search performance
            query_vector = [i * 0.1 for i in range(10)]

            # Time the search
            import time

            start_time = time.time()

            results = (
                session.query(VectorPerfTable).order_by(VectorPerfTable.embedding.l2_distance(query_vector)).limit(10).all()
            )

            end_time = time.time()
            search_time = end_time - start_time

            assert len(results) == 10
            assert search_time < 5.0  # Should complete within 5 seconds

        finally:
            session.close()

        # Clean up
        # Clean up and create table using client interface
        client.drop_all(Base)
        client.create_all(Base)

    def test_pinecone_upsert_without_index(self, client, Base, Session):
        """Test Pinecone-compatible upsert API without vector index."""

        class VectorUpsertTable(Base):
            __tablename__ = 'test_vector_upsert_no_index'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(8, "f32")
            description = Column(String(200))
            category = Column(String(50))

        # Clean up and create table
        client.drop_all(Base)
        client.create_all(Base)

        # Get Pinecone-compatible index
        index = client.get_pinecone_index('test_vector_upsert_no_index', 'embedding')

        # Test upsert new vectors (using real primary key field name)
        vectors_to_upsert = [
            {
                'id': 1,  # Primary key field
                'embedding': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],  # Vector field
                'description': 'Vector 1',
                'category': 'tech',
            },
            {
                'id': 2,
                'embedding': [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
                'description': 'Vector 2',
                'category': 'science',
            },
            {
                'id': 3,
                'embedding': [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                'description': 'Vector 3',
                'category': 'tech',
            },
        ]

        # Upsert vectors
        result = index.upsert(vectors_to_upsert)
        assert result['upserted_count'] == 3

        # Verify data was inserted
        session = Session()
        try:
            docs = session.query(VectorUpsertTable).all()
            assert len(docs) == 3

            # Verify specific data
            doc1 = session.query(VectorUpsertTable).filter(VectorUpsertTable.id == 1).first()
            assert doc1 is not None
            assert doc1.description == 'Vector 1'
            assert doc1.category == 'tech'
            assert doc1.embedding == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]

        finally:
            session.close()

        # Test upsert update existing vector
        update_vectors = [
            {
                'id': 1,  # Update existing
                'embedding': [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5],
                'description': 'Updated Vector 1',
                'category': 'updated',
            },
            {
                'id': 4,  # New vector
                'embedding': [4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0],
                'description': 'Vector 4',
                'category': 'new',
            },
        ]

        result = index.upsert(update_vectors)
        assert result['upserted_count'] == 2

        # Verify update and insert
        session = Session()
        try:
            # Check updated vector
            doc1 = session.query(VectorUpsertTable).filter(VectorUpsertTable.id == 1).first()
            assert doc1.description == 'Updated Vector 1'
            assert doc1.category == 'updated'
            assert doc1.embedding == [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5]

            # Check new vector
            doc4 = session.query(VectorUpsertTable).filter(VectorUpsertTable.id == 4).first()
            assert doc4 is not None
            assert doc4.description == 'Vector 4'
            assert doc4.category == 'new'

            # Total count should be 4
            total_count = session.query(VectorUpsertTable).count()
            assert total_count == 4

        finally:
            session.close()

        # Test query functionality
        query_results = index.query([1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5], top_k=3)
        assert len(query_results.matches) >= 1

        # Test delete functionality
        index.delete([2, 3])

        # Verify deletion
        session = Session()
        try:
            remaining_count = session.query(VectorUpsertTable).count()
            assert remaining_count == 2

            # Check specific deletions
            doc2 = session.query(VectorUpsertTable).filter(VectorUpsertTable.id == 2).first()
            doc3 = session.query(VectorUpsertTable).filter(VectorUpsertTable.id == 3).first()
            assert doc2 is None
            assert doc3 is None

        finally:
            session.close()

        # Clean up
        client.drop_all(Base)

    def test_pinecone_upsert_with_ivf_index(self, client, Base, Session):
        """Test Pinecone-compatible upsert API with IVF vector index."""

        class VectorUpsertIndexTable(Base):
            __tablename__ = 'test_vector_upsert_with_index'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(16, "f32")
            description = Column(String(200))
            category = Column(String(50))
            score = Column(Float)

        # Clean up and create table
        client.drop_all(Base)
        client.create_all(Base)

        # Enable IVF indexing
        client.vector_ops.enable_ivf()

        # Create IVF index
        try:
            client.vector_ops.create_ivf(
                name="idx_upsert_embedding",
                table_name="test_vector_upsert_with_index",
                column="embedding",
                lists=32,
                op_type="vector_l2_ops",
            )
        except Exception as e:
            # If IVF is not supported, skip this test
            pytest.skip(f"IVF index creation failed, skipping upsert with index test: {e}")

        # Get Pinecone-compatible index
        index = client.get_pinecone_index('test_vector_upsert_with_index', 'embedding')

        # Test upsert with larger vectors
        vectors_to_upsert = [
            {
                'id': 1,
                'embedding': [i * 0.1 for i in range(16)],
                'description': 'Document 1',
                'category': 'tech',
                'score': 0.9,
            },
            {
                'id': 2,
                'embedding': [i * 0.2 for i in range(16)],
                'description': 'Document 2',
                'category': 'science',
                'score': 0.8,
            },
            {
                'id': 3,
                'embedding': [i * 0.3 for i in range(16)],
                'description': 'Document 3',
                'category': 'tech',
                'score': 0.7,
            },
        ]

        # Upsert vectors
        result = index.upsert(vectors_to_upsert)
        assert result['upserted_count'] == 3

        # Verify data was inserted
        session = Session()
        try:
            docs = session.query(VectorUpsertIndexTable).all()
            assert len(docs) == 3

            # Verify specific data
            doc1 = session.query(VectorUpsertIndexTable).filter(VectorUpsertIndexTable.id == 1).first()
            assert doc1 is not None
            assert doc1.description == 'Document 1'
            assert doc1.category == 'tech'
            assert doc1.score == 0.9
            assert len(doc1.embedding) == 16

        finally:
            session.close()

        # Test upsert update with different metadata
        update_vectors = [
            {
                'id': 1,  # Update existing
                'embedding': [i * 0.15 for i in range(16)],  # Different vector
                'description': 'Updated Document 1',
                'category': 'updated',
                'score': 0.95,
            },
            {
                'id': 4,  # New vector
                'embedding': [i * 0.4 for i in range(16)],
                'description': 'Document 4',
                'category': 'new',
                'score': 0.6,
            },
        ]

        result = index.upsert(update_vectors)
        assert result['upserted_count'] == 2

        # Verify update and insert
        session = Session()
        try:
            # Check updated vector
            doc1 = session.query(VectorUpsertIndexTable).filter(VectorUpsertIndexTable.id == 1).first()
            assert doc1.description == 'Updated Document 1'
            assert doc1.category == 'updated'
            assert doc1.score == 0.95
            # Check embedding with tolerance for floating point precision
            expected_embedding = [i * 0.15 for i in range(16)]
            assert len(doc1.embedding) == len(expected_embedding)
            for i, (actual, expected) in enumerate(zip(doc1.embedding, expected_embedding)):
                assert abs(actual - expected) < 1e-6, f"Embedding mismatch at index {i}: {actual} != {expected}"

            # Check new vector
            doc4 = session.query(VectorUpsertIndexTable).filter(VectorUpsertIndexTable.id == 4).first()
            assert doc4 is not None
            assert doc4.description == 'Document 4'
            assert doc4.category == 'new'

            # Total count should be 4
            total_count = session.query(VectorUpsertIndexTable).count()
            assert total_count == 4

        finally:
            session.close()

        # Test vector search with index
        query_vector = [i * 0.15 for i in range(16)]
        query_results = index.query(query_vector, top_k=3, include_metadata=True)
        assert len(query_results.matches) >= 1

        # Verify search results contain expected data
        found_ids = {match.id for match in query_results.matches}
        assert '1' in found_ids  # Should find the updated doc1

        # Test filtered search
        filter_dict = {"category": {"$in": ["tech", "updated"]}}
        filtered_results = index.query(query_vector, top_k=5, filter=filter_dict, include_metadata=True)
        assert len(filtered_results.matches) >= 1

        # All results should match the filter
        for match in filtered_results.matches:
            assert match.metadata['category'] in ['tech', 'updated']

        # Test delete functionality
        index.delete([2, 3])

        # Verify deletion
        session = Session()
        try:
            remaining_count = session.query(VectorUpsertIndexTable).count()
            assert remaining_count == 2

            # Check specific deletions
            doc2 = session.query(VectorUpsertIndexTable).filter(VectorUpsertIndexTable.id == 2).first()
            doc3 = session.query(VectorUpsertIndexTable).filter(VectorUpsertIndexTable.id == 3).first()
            assert doc2 is None
            assert doc3 is None

        finally:
            session.close()

        # Test describe_index_stats
        stats = index.describe_index_stats()
        assert 'total_vector_count' in stats
        assert stats['total_vector_count'] == 2

        # Clean up
        client.drop_all(Base)

    def test_pinecone_upsert_batch_operations(self, client, Base, Session):
        """Test Pinecone-compatible upsert with batch operations."""

        class VectorBatchUpsertTable(Base):
            __tablename__ = 'test_vector_batch_upsert'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(12, "f32")
            description = Column(String(200))
            batch_id = Column(String(50))

        # Clean up and create table
        client.drop_all(Base)
        client.create_all(Base)

        # Get Pinecone-compatible index
        index = client.get_pinecone_index('test_vector_batch_upsert', 'embedding')

        # Test large batch upsert
        batch_size = 20
        vectors_batch = []
        for i in range(batch_size):
            vectors_batch.append(
                {
                    'id': i,
                    'embedding': [j * 0.1 + i for j in range(12)],
                    'description': f'Batch Vector {i}',
                    'batch_id': 'batch_1',
                }
            )

        # Upsert batch
        result = index.upsert(vectors_batch)
        assert result['upserted_count'] == batch_size

        # Verify batch insertion
        session = Session()
        try:
            count = session.query(VectorBatchUpsertTable).count()
            assert count == batch_size

            # Verify some specific records
            doc5 = session.query(VectorBatchUpsertTable).filter(VectorBatchUpsertTable.id == 5).first()
            assert doc5 is not None
            assert doc5.description == 'Batch Vector 5'
            assert doc5.batch_id == 'batch_1'

        finally:
            session.close()

        # Test batch update
        update_batch = []
        for i in range(0, batch_size, 2):  # Update every other vector
            update_batch.append(
                {
                    'id': i,
                    'embedding': [j * 0.2 + i for j in range(12)],  # Different values
                    'description': f'Updated Batch Vector {i}',
                    'batch_id': 'batch_2',
                }
            )

        result = index.upsert(update_batch)
        assert result['upserted_count'] == len(update_batch)

        # Verify batch update
        session = Session()
        try:
            # Check updated records
            doc0 = session.query(VectorBatchUpsertTable).filter(VectorBatchUpsertTable.id == 0).first()
            assert doc0.description == 'Updated Batch Vector 0'
            assert doc0.batch_id == 'batch_2'

            # Check unchanged records
            doc1 = session.query(VectorBatchUpsertTable).filter(VectorBatchUpsertTable.id == 1).first()
            assert doc1.description == 'Batch Vector 1'
            assert doc1.batch_id == 'batch_1'

            # Total count should still be batch_size
            total_count = session.query(VectorBatchUpsertTable).count()
            assert total_count == batch_size

        finally:
            session.close()

        # Test batch delete
        ids_to_delete = [i for i in range(0, batch_size, 3)]  # Delete every 3rd vector
        index.delete(ids_to_delete)

        # Verify batch deletion
        session = Session()
        try:
            remaining_count = session.query(VectorBatchUpsertTable).count()
            expected_remaining = batch_size - len(ids_to_delete)
            assert remaining_count == expected_remaining

            # Check specific deletions
            for i in range(0, batch_size, 3):
                doc = session.query(VectorBatchUpsertTable).filter(VectorBatchUpsertTable.id == i).first()
                assert doc is None

        finally:
            session.close()

        # Clean up
        client.drop_all(Base)

    def test_pinecone_upsert_custom_pk_string(self, client, Base, Session):
        """Test upsert with custom primary key (string type)"""

        # Define table with string primary key
        class VectorCustomPKTable(Base):
            __tablename__ = 'test_vector_custom_pk_string'
            doc_id = Column(String(50), primary_key=True)  # String primary key
            embedding = create_vector_column(8, "f32")
            description = Column(String(200))
            category = Column(String(50))

        # Create table
        client.create_all(Base)

        # Get Pinecone-compatible index
        index = client.get_pinecone_index('test_vector_custom_pk_string', 'embedding')

        # Test upsert with string primary keys
        vectors_to_upsert = [
            {
                'doc_id': 'doc_001',  # String primary key
                'embedding': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
                'description': 'Document 001',
                'category': 'tech',
            },
            {
                'doc_id': 'doc_002',
                'embedding': [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
                'description': 'Document 002',
                'category': 'science',
            },
            {
                'doc_id': 'doc_003',
                'embedding': [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                'description': 'Document 003',
                'category': 'tech',
            },
        ]

        # Upsert vectors
        result = index.upsert(vectors_to_upsert)
        assert result['upserted_count'] == 3

        # Verify data insertion
        session = Session()
        try:
            total_count = session.query(VectorCustomPKTable).count()
            assert total_count == 3

            # Verify specific data
            doc1 = session.query(VectorCustomPKTable).filter(VectorCustomPKTable.doc_id == 'doc_001').first()
            assert doc1 is not None
            assert doc1.description == 'Document 001'
            assert doc1.category == 'tech'
            assert doc1.embedding == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]

        finally:
            session.close()

        # Test upsert update existing vector
        update_vectors = [
            {
                'doc_id': 'doc_001',  # Update existing
                'embedding': [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5],
                'description': 'Updated Document 001',
                'category': 'updated',
            },
            {
                'doc_id': 'doc_004',  # New vector
                'embedding': [4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0],
                'description': 'Document 004',
                'category': 'new',
            },
        ]

        result = index.upsert(update_vectors)
        assert result['upserted_count'] == 2

        # Verify update and insert
        session = Session()
        try:
            total_count = session.query(VectorCustomPKTable).count()
            assert total_count == 4

            # Verify updated document
            doc1 = session.query(VectorCustomPKTable).filter(VectorCustomPKTable.doc_id == 'doc_001').first()
            assert doc1.description == 'Updated Document 001'
            assert doc1.category == 'updated'
            assert doc1.embedding == [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5]

            # Verify new document
            doc4 = session.query(VectorCustomPKTable).filter(VectorCustomPKTable.doc_id == 'doc_004').first()
            assert doc4 is not None
            assert doc4.description == 'Document 004'
            assert doc4.category == 'new'

        finally:
            session.close()

        # Test query functionality
        query_results = index.query([1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5], top_k=3)
        assert len(query_results.matches) >= 1

        # Verify query results contain correct primary key field
        for match in query_results.matches:
            assert 'doc_id' in match.metadata
            assert match.metadata['doc_id'] in ['doc_001', 'doc_002', 'doc_003', 'doc_004']

        # Test delete functionality
        index.delete(['doc_002', 'doc_003'])

        # Verify deletion
        session = Session()
        try:
            remaining_count = session.query(VectorCustomPKTable).count()
            assert remaining_count == 2

            # Verify specific deletions
            doc2 = session.query(VectorCustomPKTable).filter(VectorCustomPKTable.doc_id == 'doc_002').first()
            doc3 = session.query(VectorCustomPKTable).filter(VectorCustomPKTable.doc_id == 'doc_003').first()
            assert doc2 is None
            assert doc3 is None

        finally:
            session.close()

        # Clean up
        client.drop_all(Base)

    def test_pinecone_upsert_custom_pk_uuid(self, client, Base, Session):
        """Test upsert with custom primary key (UUID-like string)"""
        import uuid

        # Define table with UUID primary key
        class VectorUUIDPKTable(Base):
            __tablename__ = 'test_vector_uuid_pk'
            uuid_id = Column(String(36), primary_key=True)  # UUID string primary key
            embedding = create_vector_column(6, "f32")
            title = Column(String(100))
            tags = Column(String(200))

        # Create table
        client.create_all(Base)

        # Get Pinecone-compatible index
        index = client.get_pinecone_index('test_vector_uuid_pk', 'embedding')

        # Generate UUIDs for testing
        uuid1 = str(uuid.uuid4())
        uuid2 = str(uuid.uuid4())
        uuid3 = str(uuid.uuid4())

        # Test upsert with UUID primary keys
        vectors_to_upsert = [
            {
                'uuid_id': uuid1,
                'embedding': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
                'title': 'Article 1',
                'tags': 'machine-learning,ai',
            },
            {
                'uuid_id': uuid2,
                'embedding': [0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
                'title': 'Article 2',
                'tags': 'deep-learning,neural-networks',
            },
            {
                'uuid_id': uuid3,
                'embedding': [0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
                'title': 'Article 3',
                'tags': 'nlp,transformer',
            },
        ]

        # Upsert vectors
        result = index.upsert(vectors_to_upsert)
        assert result['upserted_count'] == 3

        # Verify data insertion
        session = Session()
        try:
            total_count = session.query(VectorUUIDPKTable).count()
            assert total_count == 3

            # Verify specific data
            doc1 = session.query(VectorUUIDPKTable).filter(VectorUUIDPKTable.uuid_id == uuid1).first()
            assert doc1 is not None
            assert doc1.title == 'Article 1'
            assert doc1.tags == 'machine-learning,ai'
            assert doc1.embedding == [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]

        finally:
            session.close()

        # Test query functionality
        query_results = index.query([0.15, 0.25, 0.35, 0.45, 0.55, 0.65], top_k=2)
        assert len(query_results.matches) >= 1

        # Verify query results contain correct primary key field
        for match in query_results.matches:
            assert 'uuid_id' in match.metadata
            assert match.metadata['uuid_id'] in [uuid1, uuid2, uuid3]
            assert len(match.metadata['uuid_id']) == 36  # UUID length

        # Test update with new UUID
        uuid4 = str(uuid.uuid4())
        update_vectors = [
            {
                'uuid_id': uuid1,  # Update existing
                'embedding': [0.11, 0.21, 0.31, 0.41, 0.51, 0.61],
                'title': 'Updated Article 1',
                'tags': 'updated,ai',
            },
            {
                'uuid_id': uuid4,  # New vector
                'embedding': [0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
                'title': 'Article 4',
                'tags': 'computer-vision,opencv',
            },
        ]

        result = index.upsert(update_vectors)
        assert result['upserted_count'] == 2

        # Verify update and insert
        session = Session()
        try:
            total_count = session.query(VectorUUIDPKTable).count()
            assert total_count == 4

            # Verify updated document
            doc1 = session.query(VectorUUIDPKTable).filter(VectorUUIDPKTable.uuid_id == uuid1).first()
            assert doc1.title == 'Updated Article 1'
            assert doc1.tags == 'updated,ai'
            assert doc1.embedding == [0.11, 0.21, 0.31, 0.41, 0.51, 0.61]

            # Verify new document
            doc4 = session.query(VectorUUIDPKTable).filter(VectorUUIDPKTable.uuid_id == uuid4).first()
            assert doc4 is not None
            assert doc4.title == 'Article 4'
            assert doc4.tags == 'computer-vision,opencv'

        finally:
            session.close()

        # Test delete functionality
        index.delete([uuid2, uuid3])

        # Verify deletion
        session = Session()
        try:
            remaining_count = session.query(VectorUUIDPKTable).count()
            assert remaining_count == 2

            # Verify specific deletions
            doc2 = session.query(VectorUUIDPKTable).filter(VectorUUIDPKTable.uuid_id == uuid2).first()
            doc3 = session.query(VectorUUIDPKTable).filter(VectorUUIDPKTable.uuid_id == uuid3).first()
            assert doc2 is None
            assert doc3 is None

        finally:
            session.close()

        # Clean up
        client.drop_all(Base)

    def test_pinecone_batch_insert(self, client, Base, Session):
        """Test batch_insert functionality"""

        # Define table for batch insert
        class VectorBatchInsertTable(Base):
            __tablename__ = 'test_vector_batch_insert'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(8, "f32")
            description = Column(String(200))
            category = Column(String(50))

        # Create table
        client.create_all(Base)

        # Get Pinecone-compatible index
        index = client.get_pinecone_index('test_vector_batch_insert', 'embedding')

        # Test batch insert
        vectors_to_insert = [
            {
                'id': 1,
                'embedding': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
                'description': 'Batch Vector 1',
                'category': 'tech',
            },
            {
                'id': 2,
                'embedding': [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
                'description': 'Batch Vector 2',
                'category': 'science',
            },
            {
                'id': 3,
                'embedding': [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                'description': 'Batch Vector 3',
                'category': 'tech',
            },
            {
                'id': 4,
                'embedding': [4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0],
                'description': 'Batch Vector 4',
                'category': 'science',
            },
            {
                'id': 5,
                'embedding': [5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
                'description': 'Batch Vector 5',
                'category': 'tech',
            },
        ]

        # Batch insert vectors
        result = index.batch_insert(vectors_to_insert)
        assert result['inserted_count'] == 5

        # Verify data insertion
        session = Session()
        try:
            total_count = session.query(VectorBatchInsertTable).count()
            assert total_count == 5

            # Verify specific data
            doc1 = session.query(VectorBatchInsertTable).filter(VectorBatchInsertTable.id == 1).first()
            assert doc1 is not None
            assert doc1.description == 'Batch Vector 1'
            assert doc1.category == 'tech'
            assert doc1.embedding == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]

            doc3 = session.query(VectorBatchInsertTable).filter(VectorBatchInsertTable.id == 3).first()
            assert doc3 is not None
            assert doc3.description == 'Batch Vector 3'
            assert doc3.category == 'tech'
            assert doc3.embedding == [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]

        finally:
            session.close()

        # Test query functionality
        query_results = index.query([2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5], top_k=3)
        assert len(query_results.matches) >= 1

        # Verify query results contain correct primary key field
        for match in query_results.matches:
            assert 'id' in match.metadata
            assert match.metadata['id'] in [1, 2, 3, 4, 5]

        # Test empty batch insert
        result = index.batch_insert([])
        assert result['inserted_count'] == 0

        # Verify no additional data was inserted
        session = Session()
        try:
            total_count = session.query(VectorBatchInsertTable).count()
            assert total_count == 5
        finally:
            session.close()

        # Clean up
        client.drop_all(Base)

    def test_pinecone_batch_insert_custom_pk(self, client, Base, Session):
        """Test batch_insert with custom primary key"""

        # Define table with custom primary key
        class VectorCustomBatchTable(Base):
            __tablename__ = 'test_vector_custom_batch'
            doc_id = Column(String(50), primary_key=True)  # String primary key
            embedding = create_vector_column(6, "f32")
            title = Column(String(100))
            tags = Column(String(200))

        # Create table
        client.create_all(Base)

        # Get Pinecone-compatible index
        index = client.get_pinecone_index('test_vector_custom_batch', 'embedding')

        # Test batch insert with custom primary key
        vectors_to_insert = [
            {
                'doc_id': 'batch_001',
                'embedding': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
                'title': 'Batch Article 1',
                'tags': 'machine-learning,ai',
            },
            {
                'doc_id': 'batch_002',
                'embedding': [0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
                'title': 'Batch Article 2',
                'tags': 'deep-learning,neural-networks',
            },
            {
                'doc_id': 'batch_003',
                'embedding': [0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
                'title': 'Batch Article 3',
                'tags': 'nlp,transformer',
            },
        ]

        # Batch insert vectors
        result = index.batch_insert(vectors_to_insert)
        assert result['inserted_count'] == 3

        # Verify data insertion
        session = Session()
        try:
            total_count = session.query(VectorCustomBatchTable).count()
            assert total_count == 3

            # Verify specific data
            doc1 = session.query(VectorCustomBatchTable).filter(VectorCustomBatchTable.doc_id == 'batch_001').first()
            assert doc1 is not None
            assert doc1.title == 'Batch Article 1'
            assert doc1.tags == 'machine-learning,ai'
            assert doc1.embedding == [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]

        finally:
            session.close()

        # Test query functionality
        query_results = index.query([0.15, 0.25, 0.35, 0.45, 0.55, 0.65], top_k=2)
        assert len(query_results.matches) >= 1

        # Verify query results contain correct primary key field
        for match in query_results.matches:
            assert 'doc_id' in match.metadata
            assert match.metadata['doc_id'] in ['batch_001', 'batch_002', 'batch_003']

        # Clean up
        client.drop_all(Base)
