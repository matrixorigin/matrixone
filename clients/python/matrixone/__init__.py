"""
MatrixOne Python SDK

A high-level Python SDK for MatrixOne that provides SQLAlchemy-like interface
for database operations, snapshot management, PITR, restore operations,
table cloning, and mo-ctl integration.
"""

from .account import Account, AccountManager, Grant, Role, TransactionAccountManager, User
from .async_client import AsyncClient, AsyncResultSet
from .client import Client
from .exceptions import (
    AccountError,
    CloneError,
    ConfigurationError,
    ConnectionError,
    MatrixOneError,
    MoCtlError,
    PitrError,
    PubSubError,
    QueryError,
    RestoreError,
    SnapshotError,
    VersionError,
)
from .moctl import MoCtlManager
from .pitr import Pitr, PitrManager
from .pubsub import Publication, PubSubManager, Subscription
from .restore import RestoreManager
from .snapshot import CloneManager, Snapshot, SnapshotLevel, SnapshotManager, SnapshotQueryBuilder

# Import SQLAlchemy extensions
from .sqlalchemy_ext import (
    FulltextAlgorithmType,
    FulltextIndex,
    FulltextModeType,
    FulltextSearchBuilder,
    MatrixOneDialect,
    Vectorf32,
    Vectorf64,
    VectorIndex,
    VectorIndexType,
    VectorOpType,
    VectorTableBuilder,
    VectorType,
    VectorTypeDecorator,
    create_document_vector_table,
    create_fulltext_index,
    create_hnsw_index,
    create_ivfflat_index,
    create_product_vector_table,
    create_vector_index_table,
    create_vector_table,
    enable_hnsw_indexing,
    enable_ivf_indexing,
    fulltext_search_builder,
)
from .version import FeatureRequirement, VersionInfo, VersionManager, requires_version

__version__ = "1.0.0"
__all__ = [
    "Client",
    "AsyncClient",
    "AsyncResultSet",
    "MatrixOneError",
    "ConnectionError",
    "QueryError",
    "ConfigurationError",
    "SnapshotError",
    "CloneError",
    "MoCtlError",
    "RestoreError",
    "PitrError",
    "PubSubError",
    "Snapshot",
    "SnapshotManager",
    "SnapshotQueryBuilder",
    "CloneManager",
    "SnapshotLevel",
    "MoCtlManager",
    "RestoreManager",
    "PitrManager",
    "Pitr",
    "PubSubManager",
    "Publication",
    "Subscription",
    "AccountError",
    "AccountManager",
    "Account",
    "User",
    "Role",
    "Grant",
    "TransactionAccountManager",
    "VersionError",
    "VersionManager",
    "VersionInfo",
    "FeatureRequirement",
    "requires_version",
    # SQLAlchemy extensions
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
    "VectorIndex",
    "VectorIndexType",
    "VectorOpType",
    "create_hnsw_index",
    "create_ivfflat_index",
    "enable_hnsw_indexing",
    "enable_ivf_indexing",
    "FulltextIndex",
    "FulltextAlgorithmType",
    "FulltextModeType",
    "FulltextSearchBuilder",
    "create_fulltext_index",
    "fulltext_search_builder",
]
