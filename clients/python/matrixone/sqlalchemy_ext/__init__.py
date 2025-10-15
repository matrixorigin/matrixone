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
SQLAlchemy extensions for MatrixOne Python client.

This module provides SQLAlchemy-specific extensions and utilities
for working with MatrixOne database features.
"""

from .dialect import MatrixOneDialect
from .fulltext_index import (
    FulltextAlgorithmType,
    FulltextIndex,
    FulltextModeType,
    FulltextParserType,
    FulltextSearchBuilder,
    create_fulltext_index,
    fulltext_search_builder,
)
from .fulltext_search import (
    FulltextFilter,
)
from .fulltext_search import FulltextIndexManager as FulltextSearchManager
from .fulltext_search import (
    FulltextSearchAlgorithm,
)
from .fulltext_search import FulltextSearchBuilder as AdvancedFulltextSearchBuilder
from .fulltext_search import (
    FulltextSearchMode,
    boolean_match,
    group,
    natural_match,
)
from .hnsw_config import (
    HNSWConfig,
    create_hnsw_config,
    disable_hnsw_indexing,
    enable_hnsw_indexing,
    get_hnsw_status,
)
from .ivf_config import (
    IVFConfig,
    create_ivf_config,
    disable_ivf_indexing,
    enable_ivf_indexing,
    get_ivf_status,
    set_probe_limit,
)
from .table_builder import (
    VectorTableBuilder,
    create_document_vector_table,
    create_product_vector_table,
    create_vector_index_table,
    create_vector_table,
)
from .vector_index import (
    CreateVectorIndex,
    HnswVectorIndex,
    IVFVectorIndex,
    VectorIndex,
    VectorIndexBuilder,
    VectorIndexType,
    VectorOpType,
    create_hnsw_index,
    create_ivfflat_index,
    create_vector_index,
    vector_index_builder,
)

# Import SQLAlchemy extensions
from .vector_type import (
    VectorColumn,
    Vectorf32,
    Vectorf64,
    VectorPrecision,
    VectorType,
    VectorTypeDecorator,
    cosine_distance,
    create_vector_column,
    inner_product,
    l2_distance,
    l2_distance_sq,
    most_similar,
    negative_inner_product,
    vector_distance_functions,
    vector_similarity_search,
    within_distance,
)

__all__ = [
    "VectorType",
    "Vectorf32",
    "Vectorf64",
    "VectorTypeDecorator",
    "VectorColumn",
    "VectorPrecision",
    "create_vector_column",
    "vector_distance_functions",
    "l2_distance",
    "l2_distance_sq",
    "cosine_distance",
    "inner_product",
    "negative_inner_product",
    "within_distance",
    "most_similar",
    "vector_similarity_search",
    "MatrixOneDialect",
    "VectorTableBuilder",
    "create_vector_table",
    "create_vector_index_table",
    "create_document_vector_table",
    "create_product_vector_table",
    "VectorIndex",
    "IVFVectorIndex",
    "HnswVectorIndex",
    "VectorIndexType",
    "VectorOpType",
    "CreateVectorIndex",
    "create_vector_index",
    "create_ivfflat_index",
    "create_hnsw_index",
    "VectorIndexBuilder",
    "vector_index_builder",
    "HNSWConfig",
    "create_hnsw_config",
    "enable_hnsw_indexing",
    "disable_hnsw_indexing",
    "get_hnsw_status",
    "IVFConfig",
    "create_ivf_config",
    "enable_ivf_indexing",
    "disable_ivf_indexing",
    "set_probe_limit",
    "get_ivf_status",
    "FulltextIndex",
    "FulltextAlgorithmType",
    "FulltextModeType",
    "FulltextParserType",
    "FulltextSearchBuilder",
    "create_fulltext_index",
    "fulltext_search_builder",
    "FulltextSearchMode",
    "FulltextSearchAlgorithm",
    "AdvancedFulltextSearchBuilder",
    "FulltextSearchManager",
    "FulltextFilter",
    "boolean_match",
    "natural_match",
    "group",
]
