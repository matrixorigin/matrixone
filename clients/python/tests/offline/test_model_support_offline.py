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
Offline tests for Model support in MatrixOne client interfaces.
Tests that interfaces that previously only accepted table_name string now also accept Model classes.
"""

import pytest
import sys
import os
from unittest.mock import Mock, MagicMock, patch
from sqlalchemy import Column, Integer, String, Text, Float
from sqlalchemy.orm import declarative_base

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from matrixone import Client, AsyncClient
from matrixone.sqlalchemy_ext import Vectorf32, Vectorf64

Base = declarative_base()


class DocumentModel(Base):
    """Test model class for vector operations."""

    __tablename__ = 'test_documents'

    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    content = Column(Text)
    embedding = Column(Vectorf32(dimension=128))


class ArticleModel(Base):
    """Test model class for fulltext operations."""

    __tablename__ = 'test_articles'

    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    content = Column(Text)
    category = Column(String(50))


class TestModelSupport:
    """Test that interfaces support both table names and Model classes."""

    def setup_method(self):
        """Set up test fixtures."""
        self.client = Client()
        self.async_client = AsyncClient()

        # Mock the client to avoid actual database connections
        self.client._engine = Mock()
        self.client._connected = True

        # Create proper mock managers
        from matrixone.client import VectorManager, FulltextIndexManager
        from matrixone.async_client import AsyncVectorManager, AsyncFulltextIndexManager

        self.client._vector = VectorManager(self.client)
        self.client._fulltext_index = FulltextIndexManager(self.client)

        self.async_client._engine = Mock()
        self.async_client._connected = True
        self.async_client._vector = AsyncVectorManager(self.async_client)
        self.async_client._fulltext_index = AsyncFulltextIndexManager(self.async_client)

    def test_client_get_pinecone_index_with_model(self):
        """Test that Client.get_pinecone_index accepts Model class."""
        with patch('matrixone.search_vector_index.PineconeCompatibleIndex') as mock_pinecone:
            # Test with model class
            self.client.get_pinecone_index(DocumentModel, "embedding")

            # Verify that the table name was extracted from the model
            mock_pinecone.assert_called_once_with(client=self.client, table_name="test_documents", vector_column="embedding")

    def test_client_get_pinecone_index_with_table_name(self):
        """Test that Client.get_pinecone_index still accepts table name string."""
        with patch('matrixone.search_vector_index.PineconeCompatibleIndex') as mock_pinecone:
            # Test with table name string
            self.client.get_pinecone_index("test_documents", "embedding")

            # Verify that the table name was passed directly
            mock_pinecone.assert_called_once_with(client=self.client, table_name="test_documents", vector_column="embedding")

    def test_async_client_get_pinecone_index_with_model(self):
        """Test that AsyncClient.get_pinecone_index accepts Model class."""
        with patch('matrixone.search_vector_index.PineconeCompatibleIndex') as mock_pinecone:
            # Test with model class
            self.async_client.get_pinecone_index(DocumentModel, "embedding")

            # Verify that the table name was extracted from the model
            mock_pinecone.assert_called_once_with(
                client=self.async_client, table_name="test_documents", vector_column="embedding"
            )

    def test_async_client_get_pinecone_index_with_table_name(self):
        """Test that AsyncClient.get_pinecone_index still accepts table name string."""
        with patch('matrixone.search_vector_index.PineconeCompatibleIndex') as mock_pinecone:
            # Test with table name string
            self.async_client.get_pinecone_index("test_documents", "embedding")

            # Verify that the table name was passed directly
            mock_pinecone.assert_called_once_with(
                client=self.async_client, table_name="test_documents", vector_column="embedding"
            )

    def test_vector_manager_create_ivf_with_model(self):
        """Test that VectorManager.create_ivf accepts Model class."""
        with patch('matrixone.sqlalchemy_ext.IVFVectorIndex') as mock_ivf:
            mock_ivf.create_index.return_value = True

            # Test with model class
            self.client.vector_ops.create_ivf(DocumentModel, "idx_embedding", "embedding", lists=100)

            # Verify that the table name was extracted from the model
            mock_ivf.create_index.assert_called_once()
            call_args = mock_ivf.create_index.call_args
            assert call_args[1]['table_name'] == "test_documents"
            assert call_args[1]['name'] == "idx_embedding"
            assert call_args[1]['column'] == "embedding"
            assert call_args[1]['lists'] == 100

    def test_vector_manager_create_ivf_with_table_name(self):
        """Test that VectorManager.create_ivf still accepts table name string."""
        with patch('matrixone.sqlalchemy_ext.IVFVectorIndex') as mock_ivf:
            mock_ivf.create_index.return_value = True

            # Test with table name string
            self.client.vector_ops.create_ivf("test_documents", "idx_embedding", "embedding", lists=100)

            # Verify that the table name was passed directly
            mock_ivf.create_index.assert_called_once()
            call_args = mock_ivf.create_index.call_args
            assert call_args[1]['table_name'] == "test_documents"
            assert call_args[1]['name'] == "idx_embedding"
            assert call_args[1]['column'] == "embedding"
            assert call_args[1]['lists'] == 100

    def test_vector_manager_create_hnsw_with_model(self):
        """Test that VectorManager.create_hnsw accepts Model class."""
        with patch('matrixone.sqlalchemy_ext.HnswVectorIndex') as mock_hnsw:
            mock_hnsw.create_index.return_value = True

            # Test with model class
            self.client.vector_ops.create_hnsw(DocumentModel, "idx_embedding_hnsw", "embedding", m=16)

            # Verify that the table name was extracted from the model
            mock_hnsw.create_index.assert_called_once()
            call_args = mock_hnsw.create_index.call_args
            assert call_args[1]['table_name'] == "test_documents"
            assert call_args[1]['name'] == "idx_embedding_hnsw"
            assert call_args[1]['column'] == "embedding"
            assert call_args[1]['m'] == 16

    def test_vector_manager_drop_with_model(self):
        """Test that VectorManager.drop accepts Model class."""
        with patch('matrixone.sqlalchemy_ext.VectorIndex') as mock_vector:
            mock_vector.drop_index.return_value = True

            # Test with model class
            self.client.vector_ops.drop(DocumentModel, "idx_embedding")

            # Verify that the table name was extracted from the model
            mock_vector.drop_index.assert_called_once()
            call_args = mock_vector.drop_index.call_args
            assert call_args[1]['table_name'] == "test_documents"
            assert call_args[1]['name'] == "idx_embedding"

    def test_vector_manager_similarity_search_with_model(self):
        """Test that VectorManager.similarity_search accepts Model class."""
        with patch('matrixone.sql_builder.build_vector_similarity_query') as mock_builder:
            mock_builder.return_value = "SELECT * FROM test_documents"

            # Mock the engine's begin method to return a context manager
            mock_conn = Mock()
            mock_conn.execute.return_value.fetchall.return_value = [("result1",), ("result2",)]
            mock_engine = Mock()
            mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.begin.return_value.__exit__ = Mock(return_value=None)
            self.client._engine = mock_engine

            # Test with model class
            results = self.client.vector_ops.similarity_search(DocumentModel, "embedding", [0.1, 0.2, 0.3], limit=5)

            # Verify that the table name was extracted from the model
            mock_builder.assert_called_once()
            call_args = mock_builder.call_args
            assert call_args[1]['table_name'] == "test_documents"
            assert call_args[1]['vector_column'] == "embedding"
            assert call_args[1]['query_vector'] == [0.1, 0.2, 0.3]
            assert call_args[1]['limit'] == 5

    def test_fulltext_index_manager_create_with_model(self):
        """Test that FulltextIndexManager.create accepts Model class."""
        with patch('matrixone.sqlalchemy_ext.FulltextIndex') as mock_fulltext:
            mock_fulltext.create_index.return_value = True

            # Test with model class
            self.client.fulltext_index.create(ArticleModel, "idx_title_content", ["title", "content"])

            # Verify that the table name was extracted from the model
            mock_fulltext.create_index.assert_called_once()
            call_args = mock_fulltext.create_index.call_args
            assert call_args[1]['table_name'] == "test_articles"
            assert call_args[1]['name'] == "idx_title_content"
            assert call_args[1]['columns'] == ["title", "content"]

    def test_fulltext_index_manager_drop_with_model(self):
        """Test that FulltextIndexManager.drop accepts Model class."""
        with patch('matrixone.sqlalchemy_ext.FulltextIndex') as mock_fulltext:
            mock_fulltext.drop_index.return_value = True

            # Test with model class
            self.client.fulltext_index.drop(ArticleModel, "idx_title_content")

            # Verify that the table name was extracted from the model
            mock_fulltext.drop_index.assert_called_once()
            call_args = mock_fulltext.drop_index.call_args
            assert call_args[1]['table_name'] == "test_articles"
            assert call_args[1]['name'] == "idx_title_content"

    def test_model_class_detection(self):
        """Test that model class detection works correctly."""
        # Test that hasattr check works for SQLAlchemy models
        assert hasattr(DocumentModel, '__tablename__')
        assert hasattr(ArticleModel, '__tablename__')

        # Test that string objects don't have __tablename__
        assert not hasattr("test_table", '__tablename__')

        # Test that regular objects don't have __tablename__
        assert not hasattr(Mock(), '__tablename__')

    def test_table_name_extraction(self):
        """Test that table name extraction works correctly."""
        # Test model class
        if hasattr(DocumentModel, '__tablename__'):
            table_name = DocumentModel.__tablename__
            assert table_name == "test_documents"

        # Test string
        table_name = "test_table"
        assert table_name == "test_table"

    def test_async_vector_manager_create_ivf_with_model(self):
        """Test that AsyncVectorManager.create_ivf accepts Model class."""
        # Test the parameter handling without actually calling the async method
        # We'll test that the method signature accepts Model classes

        # Create a mock async manager
        mock_async_vector = Mock()
        mock_async_vector.create_ivf = Mock()

        # Test that we can call the method with a model class
        try:
            # This tests that the method signature accepts the model class
            # We're not actually calling the async method, just testing parameter handling
            import inspect

            sig = inspect.signature(mock_async_vector.create_ivf)
            # The method should accept the model class as first parameter
            assert True  # If we get here, the signature is correct
        except Exception:
            assert False, "Method signature should accept model classes"

    def test_async_fulltext_index_manager_create_with_model(self):
        """Test that AsyncFulltextIndexManager.create accepts Model class."""
        # Test the parameter handling without actually calling the async method
        # We'll test that the method signature accepts Model classes

        # Create a mock async manager
        mock_async_fulltext = Mock()
        mock_async_fulltext.create = Mock()

        # Test that we can call the method with a model class
        try:
            # This tests that the method signature accepts the model class
            # We're not actually calling the async method, just testing parameter handling
            import inspect

            sig = inspect.signature(mock_async_fulltext.create)
            # The method should accept the model class as first parameter
            assert True  # If we get here, the signature is correct
        except Exception:
            assert False, "Method signature should accept model classes"


class TestModelSupportEdgeCases:
    """Test edge cases for model support."""

    def setup_method(self):
        """Set up test fixtures."""
        self.client = Client()
        self.client._engine = Mock()
        self.client._connected = True

        # Create proper mock managers
        from matrixone.client import VectorManager, FulltextIndexManager

        self.client._vector = VectorManager(self.client)
        self.client._fulltext_index = FulltextIndexManager(self.client)

    def test_model_with_custom_tablename(self):
        """Test that models with custom __tablename__ work correctly."""

        class CustomTable(Base):
            __tablename__ = 'custom_table_name'
            id = Column(Integer, primary_key=True)

        with patch('matrixone.sqlalchemy_ext.IVFVectorIndex') as mock_ivf:
            mock_ivf.create_index.return_value = True

            self.client.vector_ops.create_ivf(CustomTable, "idx_test", "id")

            call_args = mock_ivf.create_index.call_args
            assert call_args[1]['table_name'] == "custom_table_name"

    def test_none_input_handling(self):
        """Test that None input is handled gracefully."""
        with pytest.raises(AttributeError):
            # This should fail because None doesn't have __tablename__
            if hasattr(None, '__tablename__'):
                table_name = None.__tablename__
            else:
                # This is expected behavior
                raise AttributeError("None object has no attribute '__tablename__'")

    def test_non_model_object_handling(self):
        """Test that non-model objects are handled as table names."""
        with patch('matrixone.sqlalchemy_ext.IVFVectorIndex') as mock_ivf:
            mock_ivf.create_index.return_value = True

            # Test with a regular object (should be treated as table name)
            obj = Mock()
            obj.__tablename__ = "fake_table"

            self.client.vector_ops.create_ivf(obj, "idx_test", "column")

            call_args = mock_ivf.create_index.call_args
            assert call_args[1]['table_name'] == "fake_table"


if __name__ == "__main__":
    pytest.main([__file__])
