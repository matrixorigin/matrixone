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
    VectorType, Vectorf32, Vectorf64, VectorColumn, 
    create_vector_column, vector_distance_functions
)
from matrixone.config import get_connection_params
from .test_config import online_config


class TestVectorOperationsOnline:
    """Test vector operations with real database connections."""
    
    @pytest.fixture(scope="class")
    def client(self):
        """Create and connect MatrixOne client."""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        yield client
        # Client doesn't have a close method, just disconnect
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
    
    def test_vector_table_creation_and_drop(self, engine, Base, Session):
        """Test creating and dropping vector tables."""
        class VectorTestTable(Base):
            __tablename__ = 'test_vector_ops_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(10, "f32")
            description = Column(String(200))
            category = Column(String(50))
        
        # Drop table if exists
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_ops_table"))
        
        # Create table
        Base.metadata.create_all(engine, tables=[VectorTestTable.__table__])
        
        # Verify table exists
        with engine.begin() as conn:
            result = conn.execute(text("SHOW TABLES LIKE 'test_vector_ops_table'"))
            assert result.fetchone() is not None
        
        # Drop table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE test_vector_ops_table"))
        
        # Verify table is dropped
        with engine.begin() as conn:
            result = conn.execute(text("SHOW TABLES LIKE 'test_vector_ops_table'"))
            assert result.fetchone() is None
    
    def test_vector_data_insertion_and_retrieval(self, engine, Base, Session):
        """Test inserting and retrieving vector data."""
        class VectorDataTable(Base):
            __tablename__ = 'test_vector_data_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(5, "f32")
            description = Column(String(200))
        
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_data_table"))
        
        # Create table
        Base.metadata.create_all(engine, tables=[VectorDataTable.__table__])
        
        # Insert test data
        session = Session()
        try:
            test_docs = [
                VectorDataTable(embedding=[1.0, 2.0, 3.0, 4.0, 5.0], description="Document A"),
                VectorDataTable(embedding=[6.0, 7.0, 8.0, 9.0, 10.0], description="Document B"),
                VectorDataTable(embedding=[11.0, 12.0, 13.0, 14.0, 15.0], description="Document C")
            ]
            
            for doc in test_docs:
                session.add(doc)
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
            
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE test_vector_data_table"))
    
    def test_vector_search_l2_distance(self, engine, Base, Session):
        """Test vector search using L2 distance."""
        class VectorSearchTable(Base):
            __tablename__ = 'test_vector_search_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(3, "f32")
            description = Column(String(200))
        
        # Clean up and create table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_search_table"))
        
        Base.metadata.create_all(engine, tables=[VectorSearchTable.__table__])
        
        # Insert test data
        session = Session()
        try:
            test_docs = [
                VectorSearchTable(embedding=[1.0, 0.0, 0.0], description="Unit vector X"),
                VectorSearchTable(embedding=[0.0, 1.0, 0.0], description="Unit vector Y"),
                VectorSearchTable(embedding=[0.0, 0.0, 1.0], description="Unit vector Z"),
                VectorSearchTable(embedding=[1.0, 1.0, 0.0], description="Diagonal vector XY"),
                VectorSearchTable(embedding=[0.0, 1.0, 1.0], description="Diagonal vector YZ")
            ]
            
            for doc in test_docs:
                session.add(doc)
            session.commit()
            
            # Search using L2 distance
            query_vector = [1.0, 1.0, 0.0]  # Should match diagonal vector XY
            
            # Find most similar document
            results = session.query(VectorSearchTable).order_by(
                VectorSearchTable.embedding.l2_distance(query_vector)
            ).limit(1).all()
            
            assert len(results) == 1
            assert results[0].description == "Diagonal vector XY"
            
            # Search with distance threshold
            results = session.query(VectorSearchTable).filter(
                VectorSearchTable.embedding.l2_distance(query_vector) < 1.0
            ).all()
            
            assert len(results) >= 1
            
        finally:
            session.close()
            
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE test_vector_search_table"))
    
    def test_vector_search_cosine_distance(self, engine, Base, Session):
        """Test vector search using cosine distance."""
        class VectorCosineTable(Base):
            __tablename__ = 'test_vector_cosine_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(3, "f32")
            description = Column(String(200))
        
        # Clean up and create table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_cosine_table"))
        
        Base.metadata.create_all(engine, tables=[VectorCosineTable.__table__])
        
        # Insert test data
        session = Session()
        try:
            test_docs = [
                VectorCosineTable(embedding=[1.0, 0.0, 0.0], description="Unit vector X"),
                VectorCosineTable(embedding=[0.0, 1.0, 0.0], description="Unit vector Y"),
                VectorCosineTable(embedding=[1.0, 1.0, 0.0], description="Diagonal vector XY"),
                VectorCosineTable(embedding=[2.0, 2.0, 0.0], description="Scaled diagonal vector")
            ]
            
            for doc in test_docs:
                session.add(doc)
            session.commit()
            
            # Search using cosine distance
            query_vector = [1.0, 1.0, 0.0]
            
            # Find most similar document
            results = session.query(VectorCosineTable).order_by(
                VectorCosineTable.embedding.cosine_distance(query_vector)
            ).limit(1).all()
            
            assert len(results) == 1
            # Should match diagonal vector XY (cosine distance = 0)
            assert results[0].description == "Diagonal vector XY"
            
        finally:
            session.close()
            
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE test_vector_cosine_table"))
    
    
    
    def test_vector_search_l2_distance_squared(self, engine, Base, Session):
        """Test vector search using L2 distance squared."""
        class VectorL2SqTable(Base):
            __tablename__ = 'test_vector_l2sq_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(3, "f32")
            description = Column(String(200))
        
        # Clean up and create table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_l2sq_table"))
        
        Base.metadata.create_all(engine, tables=[VectorL2SqTable.__table__])
        
        # Insert test data
        session = Session()
        try:
            test_docs = [
                VectorL2SqTable(embedding=[1.0, 0.0, 0.0], description="Unit vector X"),
                VectorL2SqTable(embedding=[0.0, 1.0, 0.0], description="Unit vector Y"),
                VectorL2SqTable(embedding=[1.0, 1.0, 0.0], description="Diagonal vector XY"),
                VectorL2SqTable(embedding=[2.0, 0.0, 0.0], description="Scaled vector X")
            ]
            
            for doc in test_docs:
                session.add(doc)
            session.commit()
            
            # Search using L2 distance squared
            query_vector = [1.0, 1.0, 0.0]
            
            # Find most similar document
            results = session.query(VectorL2SqTable).order_by(
                VectorL2SqTable.embedding.l2_distance_sq(query_vector)
            ).limit(1).all()
            
            assert len(results) == 1
            assert results[0].description == "Diagonal vector XY"
            
        finally:
            session.close()
            
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE test_vector_l2sq_table"))
    
    def test_vector_search_complex_query(self, engine, Base, Session):
        """Test complex vector search with multiple criteria."""
        class VectorComplexTable(Base):
            __tablename__ = 'test_vector_complex_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(3, "f32")
            description = Column(String(200))
            category = Column(String(50))
            score = Column(Float)
        
        # Clean up and create table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_complex_table"))
        
        Base.metadata.create_all(engine, tables=[VectorComplexTable.__table__])
        
        # Insert test data
        session = Session()
        try:
            test_docs = [
                VectorComplexTable(embedding=[1.0, 0.0, 0.0], description="Science Doc A", category="science", score=0.9),
                VectorComplexTable(embedding=[0.0, 1.0, 0.0], description="Science Doc B", category="science", score=0.8),
                VectorComplexTable(embedding=[1.0, 1.0, 0.0], description="Tech Doc A", category="tech", score=0.7),
                VectorComplexTable(embedding=[0.0, 0.0, 1.0], description="Tech Doc B", category="tech", score=0.6),
                VectorComplexTable(embedding=[1.0, 1.0, 1.0], description="Science Doc C", category="science", score=0.5)
            ]
            
            for doc in test_docs:
                session.add(doc)
            session.commit()
            
            # Complex search: science category, L2 distance < 2.0, ordered by L2 distance
            query_vector = [1.0, 1.0, 0.0]
            
            results = session.query(VectorComplexTable).filter(
                and_(
                    VectorComplexTable.category == "science",
                    VectorComplexTable.embedding.l2_distance(query_vector) < 2.0
                )
            ).order_by(
                VectorComplexTable.embedding.l2_distance(query_vector)
            ).all()
            
            assert len(results) >= 1
            # All results should be science category
            for result in results:
                assert result.category == "science"
            
        finally:
            session.close()
            
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE test_vector_complex_table"))
    
    def test_vector_batch_insertion_and_search(self, engine, Base, Session):
        """Test batch insertion and search operations."""
        class VectorBatchTable(Base):
            __tablename__ = 'test_vector_batch_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(4, "f32")
            description = Column(String(200))
        
        # Clean up and create table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_batch_table"))
        
        Base.metadata.create_all(engine, tables=[VectorBatchTable.__table__])
        
        # Batch insert test data
        session = Session()
        try:
            batch_data = [
                {"embedding": [1.0, 2.0, 3.0, 4.0], "description": f"Batch Doc {i}"}
                for i in range(10)
            ]
            
            session.bulk_insert_mappings(VectorBatchTable, batch_data)
            session.commit()
            
            # Verify batch insertion
            count = session.query(VectorBatchTable).count()
            assert count == 10
            
            # Search in batch data
            query_vector = [1.0, 2.0, 3.0, 4.0]
            
            # Find top 3 most similar
            results = session.query(VectorBatchTable).order_by(
                VectorBatchTable.embedding.l2_distance(query_vector)
            ).limit(3).all()
            
            assert len(results) == 3
            
        finally:
            session.close()
            
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE test_vector_batch_table"))
    
    def test_vector_search_with_limit_and_offset(self, engine, Base, Session):
        """Test vector search with limit and offset for pagination."""
        class VectorPaginationTable(Base):
            __tablename__ = 'test_vector_pagination_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(3, "f32")
            description = Column(String(200))
        
        # Clean up and create table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_pagination_table"))
        
        Base.metadata.create_all(engine, tables=[VectorPaginationTable.__table__])
        
        # Insert test data
        session = Session()
        try:
            test_docs = [
                VectorPaginationTable(embedding=[i * 0.1, i * 0.2, i * 0.3], description=f"Doc {i}")
                for i in range(20)
            ]
            
            for doc in test_docs:
                session.add(doc)
            session.commit()
            
            # Test pagination
            query_vector = [1.0, 2.0, 3.0]
            
            # First page
            page1 = session.query(VectorPaginationTable).order_by(
                VectorPaginationTable.embedding.l2_distance(query_vector)
            ).limit(5).offset(0).all()
            
            # Second page
            page2 = session.query(VectorPaginationTable).order_by(
                VectorPaginationTable.embedding.l2_distance(query_vector)
            ).limit(5).offset(5).all()
            
            assert len(page1) == 5
            assert len(page2) == 5
            
            # Pages should be different
            page1_ids = {doc.id for doc in page1}
            page2_ids = {doc.id for doc in page2}
            assert page1_ids.isdisjoint(page2_ids)
            
        finally:
            session.close()
            
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE test_vector_pagination_table"))
    
    def test_vector_search_performance(self, engine, Base, Session):
        """Test vector search performance with larger dataset."""
        class VectorPerfTable(Base):
            __tablename__ = 'test_vector_perf_table'
            id = Column(Integer, primary_key=True)
            embedding = create_vector_column(10, "f32")
            description = Column(String(200))
        
        # Clean up and create table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_vector_perf_table"))
        
        Base.metadata.create_all(engine, tables=[VectorPerfTable.__table__])
        
        # Insert larger dataset
        session = Session()
        try:
            # Insert 100 documents
            test_docs = [
                VectorPerfTable(
                    embedding=[i * 0.1 for i in range(10)],
                    description=f"Performance Doc {i}"
                )
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
            
            results = session.query(VectorPerfTable).order_by(
                VectorPerfTable.embedding.l2_distance(query_vector)
            ).limit(10).all()
            
            end_time = time.time()
            search_time = end_time - start_time
            
            assert len(results) == 10
            assert search_time < 5.0  # Should complete within 5 seconds
            
        finally:
            session.close()
            
        # Clean up
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE test_vector_perf_table"))
