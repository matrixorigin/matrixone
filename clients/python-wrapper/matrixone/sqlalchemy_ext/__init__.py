"""
SQLAlchemy extensions for MatrixOne Python client.

This module provides SQLAlchemy-specific extensions and utilities
for working with MatrixOne database features.
"""

from .dialect import MatrixOneDialect
from .ivf_config import (IVFConfig, create_ivf_config, disable_ivf_indexing,
                         enable_ivf_indexing, get_ivf_status, set_probe_limit)
from .table_builder import (VectorTableBuilder, create_document_vector_table,
                            create_product_vector_table,
                            create_vector_index_table, create_vector_table)
from .vector_index import (CreateVectorIndex, VectorIndex, VectorIndexBuilder,
                           VectorIndexType, VectorOpType, create_ivfflat_index,
                           create_vector_index, vector_index_builder)
# Import SQLAlchemy extensions
from .vector_type import (VectorColumn, Vectorf32, Vectorf64, VectorType,
                          VectorTypeDecorator, create_vector_column,
                          vector_distance_functions)

__all__ = [
    "VectorType",
    "Vectorf32",
    "Vectorf64",
    "VectorTypeDecorator",
    "VectorColumn",
    "create_vector_column",
    "vector_distance_functions",
    "MatrixOneDialect",
    "VectorTableBuilder",
    "create_vector_table",
    "create_vector_index_table",
    "create_document_vector_table",
    "create_product_vector_table",
    "VectorIndex",
    "VectorIndexType",
    "VectorOpType",
    "CreateVectorIndex",
    "create_vector_index",
    "create_ivfflat_index",
    "VectorIndexBuilder",
    "vector_index_builder",
    "IVFConfig",
    "create_ivf_config",
    "enable_ivf_indexing",
    "disable_ivf_indexing",
    "set_probe_limit",
    "get_ivf_status",
]
