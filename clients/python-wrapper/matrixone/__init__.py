"""
MatrixOne Python SDK

A high-level Python SDK for MatrixOne that provides SQLAlchemy-like interface 
for database operations, snapshot management, PITR, restore operations, 
table cloning, and mo-ctl integration.
"""

from .client import Client
from .async_client import AsyncClient, AsyncResultSet
from .exceptions import (
    MatrixOneError,
    ConnectionError,
    QueryError,
    ConfigurationError,
    SnapshotError,
    CloneError,
    MoCtlError,
    RestoreError,
    PitrError
)
from .snapshot import Snapshot, SnapshotManager, SnapshotQueryBuilder, CloneManager, SnapshotLevel
from .moctl import MoCtlManager
from .restore import RestoreManager
from .pitr import PitrManager, Pitr

__version__ = "0.1.0"
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
    "Snapshot",
    "SnapshotManager",
    "SnapshotQueryBuilder",
    "CloneManager",
    "SnapshotLevel",
    "MoCtlManager",
    "RestoreManager",
    "PitrManager",
    "Pitr"
]
