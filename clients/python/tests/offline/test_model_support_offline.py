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
Offline tests for SQLAlchemy Model classes with MatrixOne extensions.
Tests Model class definitions and table name extraction.
"""

import pytest
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import declarative_base

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


class ProductModel(Base):
    """Test model class with multiple vector columns."""

    __tablename__ = 'test_products'

    id = Column(Integer, primary_key=True)
    name = Column(String(200))
    description = Column(Text)
    embedding_32 = Column(Vectorf32(dimension=64))
    embedding_64 = Column(Vectorf64(dimension=128))


class TestModelDefinitions:
    """Test SQLAlchemy Model class definitions."""

    def test_document_model_has_tablename(self):
        """Test that DocumentModel has __tablename__ attribute"""
        assert hasattr(DocumentModel, '__tablename__')
        assert DocumentModel.__tablename__ == 'test_documents'

    def test_document_model_has_columns(self):
        """Test that DocumentModel has expected columns"""
        assert hasattr(DocumentModel, 'id')
        assert hasattr(DocumentModel, 'title')
        assert hasattr(DocumentModel, 'content')
        assert hasattr(DocumentModel, 'embedding')

    def test_article_model_has_tablename(self):
        """Test that ArticleModel has __tablename__ attribute"""
        assert hasattr(ArticleModel, '__tablename__')
        assert ArticleModel.__tablename__ == 'test_articles'

    def test_article_model_has_columns(self):
        """Test that ArticleModel has expected columns"""
        assert hasattr(ArticleModel, 'id')
        assert hasattr(ArticleModel, 'title')
        assert hasattr(ArticleModel, 'content')
        assert hasattr(ArticleModel, 'category')

    def test_product_model_has_tablename(self):
        """Test that ProductModel has __tablename__ attribute"""
        assert hasattr(ProductModel, '__tablename__')
        assert ProductModel.__tablename__ == 'test_products'

    def test_product_model_has_multiple_vectors(self):
        """Test that ProductModel has multiple vector columns"""
        assert hasattr(ProductModel, 'embedding_32')
        assert hasattr(ProductModel, 'embedding_64')


class TestTableNameExtraction:
    """Test table name extraction from Model classes."""

    def test_can_extract_tablename_from_document_model(self):
        """Test extracting table name from DocumentModel"""
        # This shows the correct way to use Model classes:
        # Extract __tablename__ before passing to manager methods
        table_name = DocumentModel.__tablename__
        assert table_name == 'test_documents'
        assert isinstance(table_name, str)

    def test_can_extract_tablename_from_article_model(self):
        """Test extracting table name from ArticleModel"""
        table_name = ArticleModel.__tablename__
        assert table_name == 'test_articles'
        assert isinstance(table_name, str)

    def test_can_extract_tablename_from_product_model(self):
        """Test extracting table name from ProductModel"""
        table_name = ProductModel.__tablename__
        assert table_name == 'test_products'
        assert isinstance(table_name, str)

    def test_tablename_extraction_works_for_operations(self):
        """Test that tablename extraction pattern works for all models"""
        models = [DocumentModel, ArticleModel, ProductModel]
        table_names = [model.__tablename__ for model in models]

        assert len(table_names) == 3
        assert all(isinstance(name, str) for name in table_names)
        assert 'test_documents' in table_names
        assert 'test_articles' in table_names
        assert 'test_products' in table_names


class TestVectorColumnTypes:
    """Test vector column type definitions."""

    def test_vectorf32_column_defined(self):
        """Test that Vectorf32 column is properly defined"""
        embedding_col = DocumentModel.__table__.columns['embedding']
        assert embedding_col is not None
        # Column type checking
        assert hasattr(embedding_col.type, 'dimension')

    def test_multiple_vector_types_in_model(self):
        """Test model with both Vectorf32 and Vectorf64"""
        embedding_32_col = ProductModel.__table__.columns['embedding_32']
        embedding_64_col = ProductModel.__table__.columns['embedding_64']

        assert embedding_32_col is not None
        assert embedding_64_col is not None
        assert hasattr(embedding_32_col.type, 'dimension')
        assert hasattr(embedding_64_col.type, 'dimension')


class TestVectorManagerModelSupport:
    """Test VectorManager with Model class arguments."""

    def setup_method(self):
        """Set up test fixtures."""
        from unittest.mock import Mock
        from matrixone.vector_manager import VectorManager

        self.mock_client = Mock()
        self.mock_client.execute = Mock(return_value=Mock(fetchall=Mock(return_value=[])))
        self.vector_manager = VectorManager(self.mock_client)

    def test_create_ivf_with_model(self):
        """Test create_ivf accepts Model class"""
        # Should not raise exception
        try:
            self.vector_manager.create_ivf(DocumentModel, "idx_test", "embedding", lists=10)
        except Exception as e:
            # Expected to fail on execute, but should extract table name first
            assert "test_documents" in str(e) or "idx_test" in str(e)

        # Verify execute was called (table name extracted)
        assert self.mock_client.execute.called

    def test_create_ivf_with_string(self):
        """Test create_ivf accepts string table name"""
        try:
            self.vector_manager.create_ivf("test_documents", "idx_test", "embedding", lists=10)
        except Exception as e:
            assert "test_documents" in str(e) or "idx_test" in str(e)

        assert self.mock_client.execute.called

    def test_create_hnsw_with_model(self):
        """Test create_hnsw accepts Model class"""
        try:
            self.vector_manager.create_hnsw(DocumentModel, "idx_test", "embedding", m=16)
        except Exception as e:
            assert "test_documents" in str(e) or "idx_test" in str(e)

        assert self.mock_client.execute.called

    def test_drop_with_model(self):
        """Test drop accepts Model class"""
        try:
            self.vector_manager.drop(DocumentModel, "idx_test")
        except Exception as e:
            assert "test_documents" in str(e) or "idx_test" in str(e)

        assert self.mock_client.execute.called

    def test_insert_with_model(self):
        """Test insert accepts Model class"""
        try:
            self.vector_manager.insert(DocumentModel, {"id": 1, "title": "Test"})
        except Exception as e:
            assert "test_documents" in str(e)

        assert self.mock_client.execute.called

    def test_similarity_search_with_model(self):
        """Test similarity_search accepts Model class"""
        from unittest.mock import Mock

        self.mock_client.execute.return_value = Mock(__iter__=Mock(return_value=iter([])))
        try:
            self.vector_manager.similarity_search(DocumentModel, "embedding", [0.1, 0.2], limit=5)
        except Exception as e:
            # May fail on result processing, but table name should be extracted
            assert "test_documents" in str(e) or "test_documents" in str(self.mock_client.execute.call_args)


class TestAsyncVectorManagerModelSupport:
    """Test AsyncVectorManager with Model class arguments."""

    def setup_method(self):
        """Set up test fixtures."""
        from unittest.mock import Mock, AsyncMock
        from matrixone.vector_manager import AsyncVectorManager

        self.mock_client = Mock()
        self.mock_client.execute = AsyncMock(return_value=Mock(fetchall=Mock(return_value=[])))
        self.async_vector_manager = AsyncVectorManager(self.mock_client)

    @pytest.mark.asyncio
    async def test_create_ivf_with_model(self):
        """Test async create_ivf accepts Model class"""
        try:
            await self.async_vector_manager.create_ivf(DocumentModel, "idx_test", "embedding", lists=10)
        except Exception as e:
            assert "test_documents" in str(e) or "idx_test" in str(e)

        assert self.mock_client.execute.called

    @pytest.mark.asyncio
    async def test_drop_with_model(self):
        """Test async drop accepts Model class"""
        try:
            await self.async_vector_manager.drop(DocumentModel, "idx_test")
        except Exception as e:
            assert "test_documents" in str(e) or "idx_test" in str(e)

        assert self.mock_client.execute.called


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
