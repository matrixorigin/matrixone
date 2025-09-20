"""
MatrixOne Python SDK

A high-level Python SDK for MatrixOne that provides SQLAlchemy-like interface
for database operations, snapshot management, PITR, restore operations,
table cloning, and mo-ctl integration.
"""

from .account import (Account, AccountManager, Grant, Role,
                      TransactionAccountManager, User)
from .async_client import AsyncClient, AsyncResultSet
from .client import Client
from .exceptions import (AccountError, CloneError, ConfigurationError,
                         ConnectionError, MatrixOneError, MoCtlError,
                         PitrError, PubSubError, QueryError, RestoreError,
                         SnapshotError, VersionError)
from .moctl import MoCtlManager
from .pitr import Pitr, PitrManager
from .pubsub import Publication, PubSubManager, Subscription
from .restore import RestoreManager
from .snapshot import (CloneManager, Snapshot, SnapshotLevel, SnapshotManager,
                       SnapshotQueryBuilder)
from .version import (FeatureRequirement, VersionInfo, VersionManager,
                      requires_version)

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
]
