"""
SQLAlchemy extensions for MatrixOne Python client.

This module provides SQLAlchemy-specific extensions and utilities
for working with MatrixOne database features.
"""

from .dialect import MatrixOneDialect
from .table_builder import (VectorTableBuilder, create_document_vector_table,
                            create_product_vector_table,
                            create_vector_index_table, create_vector_table)
# Import SQLAlchemy extensions
from .vector_type import Vectorf32, Vectorf64, VectorType, VectorTypeDecorator

__all__ = [
    "VectorType",
    "Vectorf32",
    "Vectorf64",
    "VectorTypeDecorator",
    "MatrixOneDialect",
    "VectorTableBuilder",
    "create_vector_table",
    "create_vector_index_table",
    "create_document_vector_table",
    "create_product_vector_table",
]
