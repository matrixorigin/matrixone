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
Offline tests for vector index functionality.
"""

import pytest
from unittest.mock import Mock, patch
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.schema import CreateTable

from matrixone.sqlalchemy_ext import (
    VectorIndex,
    VectorIndexType,
    VectorOpType,
    CreateVectorIndex,
    create_vector_index,
    create_ivfflat_index,
    vector_index_builder,
    create_vector_column,
    Vectorf32,
)


class TestVectorIndexOffline:
    """Test vector index functionality using mocks."""

    def test_vector_index_creation(self):
        """Test creating vector indexes."""
        # Test basic vector index
        index = VectorIndex(
            "idx_test",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=256,
            op_type=VectorOpType.VECTOR_L2_OPS,
        )

        assert index.name == "idx_test"
        assert index.index_type == VectorIndexType.IVFFLAT
        assert index.lists == 256
        assert index.op_type == VectorOpType.VECTOR_L2_OPS

    def test_create_vector_index_helper(self):
        """Test create_vector_index helper function."""
        index = create_vector_index(
            "idx_helper",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=128,
            op_type=VectorOpType.VECTOR_COSINE_OPS,
        )

        assert index.name == "idx_helper"
        assert index.index_type == VectorIndexType.IVFFLAT
        assert index.lists == 128
        assert index.op_type == VectorOpType.VECTOR_COSINE_OPS

    def test_create_ivfflat_index_helper(self):
        """Test create_ivfflat_index helper function."""
        index = create_ivfflat_index("idx_ivfflat", "embedding", lists=64, op_type=VectorOpType.VECTOR_L2_OPS)

        assert index.name == "idx_ivfflat"
        assert index.index_type == VectorIndexType.IVFFLAT
        assert index.lists == 64
        assert index.op_type == VectorOpType.VECTOR_L2_OPS

    def test_vector_index_sql_generation(self):
        """Test vector index SQL generation."""
        index = VectorIndex(
            "idx_sql_test",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=256,
            op_type=VectorOpType.VECTOR_L2_OPS,
        )

        # Mock table
        mock_table = Mock()
        mock_table.name = "test_table"
        index.table = mock_table

        # Test SQL generation
        sql = index._create_index_sql("test_table")
        expected_parts = [
            "CREATE INDEX idx_sql_test USING ivfflat ON test_table(embedding)",
            "lists = 256",
            "op_type 'vector_l2_ops'",
        ]

        for part in expected_parts:
            assert part in sql

    def test_vector_index_builder(self):
        """Test VectorIndexBuilder functionality."""
        builder = vector_index_builder("embedding")

        # Add multiple indexes
        indexes = (
            builder.l2_index("idx_l2", lists=256).cosine_index("idx_cosine", lists=128).ip_index("idx_ip", lists=192).build()
        )

        assert len(indexes) == 3

        # Check L2 index
        l2_idx = indexes[0]
        assert l2_idx.name == "idx_l2"
        assert l2_idx.lists == 256
        assert l2_idx.op_type == VectorOpType.VECTOR_L2_OPS

        # Check cosine index
        cosine_idx = indexes[1]
        assert cosine_idx.name == "idx_cosine"
        assert cosine_idx.lists == 128
        assert cosine_idx.op_type == VectorOpType.VECTOR_COSINE_OPS

        # Check IP index
        ip_idx = indexes[2]
        assert ip_idx.name == "idx_ip"
        assert ip_idx.lists == 192
        assert ip_idx.op_type == VectorOpType.VECTOR_IP_OPS

    def test_vector_index_with_table(self):
        """Test vector index integration with SQLAlchemy Table."""
        # Create metadata and table
        metadata = MetaData()
        table = Table(
            'vector_docs',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('embedding', Vectorf32(dimension=128)),
            Column('title', String(200)),
        )

        # Create vector index
        index = create_ivfflat_index("idx_embedding", "embedding", lists=256, op_type=VectorOpType.VECTOR_L2_OPS)

        # Add index to table
        index.table = table
        table.indexes.add(index)

        # Verify index is added
        assert len(table.indexes) == 1
        assert index in table.indexes

    def test_create_vector_index_ddl(self):
        """Test CreateVectorIndex DDL compilation."""
        index = VectorIndex(
            "idx_ddl_test",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=128,
            op_type=VectorOpType.VECTOR_COSINE_OPS,
        )

        # Mock table
        mock_table = Mock()
        mock_table.name = "test_table"
        index.table = mock_table

        # Create DDL element
        ddl = CreateVectorIndex(index)

        # Test that DDL element is created
        assert ddl.index == index
        assert ddl.if_not_exists is False

    def test_vector_index_types(self):
        """Test vector index type constants."""
        assert VectorIndexType.IVFFLAT == "ivfflat"
        assert VectorIndexType.HNSW == "hnsw"

        assert VectorOpType.VECTOR_L2_OPS == "vector_l2_ops"
        assert VectorOpType.VECTOR_COSINE_OPS == "vector_cosine_ops"
        assert VectorOpType.VECTOR_IP_OPS == "vector_ip_ops"

    def test_vector_index_parameters(self):
        """Test vector index with different parameters."""
        # Test without lists parameter
        index1 = VectorIndex(
            "idx_no_lists",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            op_type=VectorOpType.VECTOR_L2_OPS,
        )

        assert index1.lists is None

        # Test with standard parameters
        index2 = VectorIndex(
            "idx_standard",
            "embedding",
            index_type=VectorIndexType.IVFFLAT,
            lists=512,
            op_type=VectorOpType.VECTOR_COSINE_OPS,
        )

        assert index2.lists == 512
        assert index2.op_type == VectorOpType.VECTOR_COSINE_OPS

    def test_vector_index_builder_chain(self):
        """Test VectorIndexBuilder method chaining."""
        builder = vector_index_builder("embedding")

        # Test method chaining
        result = builder.l2_index("idx1", lists=100).cosine_index("idx2", lists=200).ip_index("idx3", lists=300)

        # Should return self for chaining
        assert result is builder

        # Build indexes
        indexes = builder.build()
        assert len(indexes) == 3

    def test_vector_index_with_column_object(self):
        """Test vector index with Column object instead of string."""
        # Create a vector column
        vector_col = create_vector_column(128, "f32")
        vector_col.name = "embedding"

        # Create index with column object
        index = create_ivfflat_index("idx_column_obj", vector_col, lists=256, op_type=VectorOpType.VECTOR_L2_OPS)

        assert index.name == "idx_column_obj"
        assert index.lists == 256
        assert index.op_type == VectorOpType.VECTOR_L2_OPS
