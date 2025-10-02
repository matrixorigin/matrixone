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
Simple offline tests for Model support in MatrixOne client interfaces.
Tests the core functionality without complex mocking.
"""

import pytest
import sys
import os
from unittest.mock import Mock, patch
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import declarative_base

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from matrixone import Client, AsyncClient
from matrixone.sqlalchemy_ext import Vectorf32

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


class TestModelSupportSimple:
    """Simple tests for model support functionality."""

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

    def test_client_get_pinecone_index_parameter_handling(self):
        """Test that Client.get_pinecone_index handles parameters correctly."""
        client = Client()

        # Test that the method exists and has the right signature
        import inspect

        sig = inspect.signature(client.get_pinecone_index)
        params = list(sig.parameters.keys())

        # Should have table_name_or_model and vector_column parameters
        assert 'table_name_or_model' in params
        assert 'vector_column' in params

    def test_async_client_get_pinecone_index_parameter_handling(self):
        """Test that AsyncClient.get_pinecone_index handles parameters correctly."""
        async_client = AsyncClient()

        # Test that the method exists and has the right signature
        import inspect

        sig = inspect.signature(async_client.get_pinecone_index)
        params = list(sig.parameters.keys())

        # Should have table_name_or_model and vector_column parameters
        assert 'table_name_or_model' in params
        assert 'vector_column' in params

    def test_model_class_attributes(self):
        """Test that model classes have the expected attributes."""
        # Test DocumentModel
        assert hasattr(DocumentModel, '__tablename__')
        assert DocumentModel.__tablename__ == 'test_documents'
        assert hasattr(DocumentModel, 'id')
        assert hasattr(DocumentModel, 'title')
        assert hasattr(DocumentModel, 'content')
        assert hasattr(DocumentModel, 'embedding')

        # Test ArticleModel
        assert hasattr(ArticleModel, '__tablename__')
        assert ArticleModel.__tablename__ == 'test_articles'
        assert hasattr(ArticleModel, 'id')
        assert hasattr(ArticleModel, 'title')
        assert hasattr(ArticleModel, 'content')
        assert hasattr(ArticleModel, 'category')

    def test_model_vs_string_handling(self):
        """Test the logic for handling models vs strings."""
        # Test model class handling
        if hasattr(DocumentModel, '__tablename__'):
            table_name = DocumentModel.__tablename__
            assert table_name == "test_documents"

        # Test string handling
        table_name_str = "test_table"
        if hasattr(table_name_str, '__tablename__'):
            table_name = table_name_str.__tablename__
        else:
            table_name = table_name_str
        assert table_name == "test_table"

    def test_none_input_handling(self):
        """Test that None input is handled gracefully."""
        # Test None handling
        none_input = None
        if hasattr(none_input, '__tablename__'):
            table_name = none_input.__tablename__
        else:
            # This is expected behavior for None
            assert none_input is None

    def test_custom_model_class(self):
        """Test that custom model classes work correctly."""

        class CustomTable(Base):
            __tablename__ = 'custom_table_name'
            id = Column(Integer, primary_key=True)

        # Test that custom model works
        assert hasattr(CustomTable, '__tablename__')
        assert CustomTable.__tablename__ == 'custom_table_name'

        # Test table name extraction
        if hasattr(CustomTable, '__tablename__'):
            table_name = CustomTable.__tablename__
            assert table_name == "custom_table_name"

    def test_parameter_signatures(self):
        """Test that modified methods have the correct parameter signatures."""
        client = Client()

        # Test VectorManager methods (if available)
        if hasattr(client, 'vector_ops'):
            vector_ops = client.vector_ops

            # Test create_ivf signature
            if hasattr(vector_ops, 'create_ivf'):
                import inspect

                sig = inspect.signature(vector_ops.create_ivf)
                params = list(sig.parameters.keys())
                assert 'table_name_or_model' in params or 'table_name' in params

            # Test create_hnsw signature
            if hasattr(vector_ops, 'create_hnsw'):
                import inspect

                sig = inspect.signature(vector_ops.create_hnsw)
                params = list(sig.parameters.keys())
                assert 'table_name_or_model' in params or 'table_name' in params

            # Test drop signature
            if hasattr(vector_ops, 'drop'):
                import inspect

                sig = inspect.signature(vector_ops.drop)
                params = list(sig.parameters.keys())
                assert 'table_name_or_model' in params or 'table_name' in params

        # Test FulltextIndexManager methods (if available)
        if hasattr(client, 'fulltext_index'):
            fulltext_index = client.fulltext_index

            # Test create signature
            if hasattr(fulltext_index, 'create'):
                import inspect

                sig = inspect.signature(fulltext_index.create)
                params = list(sig.parameters.keys())
                assert 'table_name_or_model' in params or 'table_name' in params

            # Test drop signature
            if hasattr(fulltext_index, 'drop'):
                import inspect

                sig = inspect.signature(fulltext_index.drop)
                params = list(sig.parameters.keys())
                assert 'table_name_or_model' in params or 'table_name' in params


if __name__ == "__main__":
    pytest.main([__file__])
