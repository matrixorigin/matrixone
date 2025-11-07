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
MatrixOne Python SDK

A high-level Python SDK for MatrixOne that provides SQLAlchemy-like interface
for database operations, snapshot management, PITR, restore operations,
table cloning, and mo-ctl integration.
"""

from .account import Account, AccountManager, Grant, Role, User
from .async_client import AsyncClient, AsyncResultSet
from .client import Client
from .session import Session, AsyncSession
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
from .load_data import (
    LoadDataManager,
    AsyncLoadDataManager,
    LoadDataFormat,
    CompressionFormat,
    JsonDataStructure,
)
from .export import (
    ExportManager,
    AsyncExportManager,
)
from .moctl import MoCtlManager
from .pitr import Pitr, PitrManager
from .pubsub import Publication, PubSubManager, Subscription
from .restore import RestoreManager
from .snapshot import Snapshot, SnapshotLevel, SnapshotManager
from .clone import CloneManager
from .stage import Stage, StageManager, AsyncStageManager
from .cdc import CDCTaskInfo, CDCWatermarkInfo, CDCManager, AsyncCDCManager

# Import SQLAlchemy extensions
from .sqlalchemy_ext import (
    FulltextAlgorithmType,
    FulltextFilter,
    FulltextIndex,
    FulltextModeType,
    FulltextParserType,
    FulltextSearchBuilder,
    MatrixOneDialect,
    Vectorf32,
    Vectorf64,
    VectorIndex,
    VectorIndexType,
    VectorOpType,
    VectorPrecision,
    VectorTableBuilder,
    VectorType,
    VectorTypeDecorator,
    boolean_match,
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
    group,
    natural_match,
)

from .version import FeatureRequirement, VersionInfo, VersionManager, requires_version

# Import SQLAlchemy 2.0 select support
from .sqlalchemy_select import select, compile_select_to_sql

__version__ = "1.0.0"
__all__ = [
    "Client",
    "Session",
    "AsyncClient",
    "AsyncSession",
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
    "LoadDataManager",
    "AsyncLoadDataManager",
    "LoadDataFormat",
    "CompressionFormat",
    "JsonDataStructure",
    "ExportManager",
    "AsyncExportManager",
    "VersionError",
    "VersionManager",
    "VersionInfo",
    "FeatureRequirement",
    "requires_version",
    "Stage",
    "StageManager",
    "AsyncStageManager",
    "CDCTaskInfo",
    "CDCWatermarkInfo",
    "CDCManager",
    "AsyncCDCManager",
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
    "VectorPrecision",
    "create_hnsw_index",
    "create_ivfflat_index",
    "enable_hnsw_indexing",
    "enable_ivf_indexing",
    "FulltextIndex",
    "FulltextAlgorithmType",
    "FulltextModeType",
    "FulltextParserType",
    "FulltextSearchBuilder",
    "FulltextFilter",
    "boolean_match",
    "natural_match",
    "group",
    "create_fulltext_index",
    "fulltext_search_builder",
    # SQLAlchemy 2.0 select support
    "select",
    "compile_select_to_sql",
]
