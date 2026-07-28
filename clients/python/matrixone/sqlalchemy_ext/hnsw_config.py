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
HNSW index configuration management for MatrixOne.

This module provides utilities for managing HNSW index configuration
in MatrixOne, including enabling/disabling HNSW indexing and setting
search parameters.
"""

from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine


def _exec_sql_safe(connection, sql: str):
    """Execute SQL safely, bypassing bind parameter parsing when possible."""
    if hasattr(connection, 'exec_driver_sql'):
        # Escape % to %% for pymysql's format string handling
        escaped_sql = sql.replace('%', '%%')
        return connection.exec_driver_sql(escaped_sql)
    else:
        return connection.execute(text(sql))


class HNSWConfig:
    """
    Configuration manager for HNSW indexing in MatrixOne.

    Provides methods to enable/disable HNSW indexing and manage
    HNSW-specific parameters.
    """

    def __init__(self, engine: Engine):
        """
        Initialize HNSWConfig.

        Args:

            engine: SQLAlchemy engine instance
        """
        self.engine = engine

    def enable_hnsw_indexing(self, connection: Optional[Connection] = None) -> bool:
        """
        Enable HNSW indexing in MatrixOne.

        Args:

            connection: Optional existing database connection

        Returns:

            bool: True if successful, False otherwise
        """
        try:
            if connection:
                _exec_sql_safe(connection, "SET experimental_hnsw_index = 1")
            else:
                with self.engine.begin() as conn:
                    _exec_sql_safe(conn, "SET experimental_hnsw_index = 1")
            return True
        except Exception as e:
            print(f"Failed to enable HNSW indexing: {e}")
            return False

    def disable_hnsw_indexing(self, connection: Optional[Connection] = None) -> bool:
        """
        Disable HNSW indexing in MatrixOne.

        Args:

            connection: Optional existing database connection

        Returns:

            bool: True if successful, False otherwise
        """
        try:
            if connection:
                _exec_sql_safe(connection, "SET experimental_hnsw_index = 0")
            else:
                with self.engine.begin() as conn:
                    _exec_sql_safe(conn, "SET experimental_hnsw_index = 0")
            return True
        except Exception as e:
            print(f"Failed to disable HNSW indexing: {e}")
            return False

    def get_hnsw_status(self, connection: Optional[Connection] = None) -> Optional[bool]:
        """
        Get the current HNSW indexing status.

        Args:

            connection: Optional existing database connection

        Returns:

            bool: True if HNSW indexing is enabled, False if disabled, None if error
        """
        try:
            if connection:
                result = _exec_sql_safe(connection, "SHOW VARIABLES LIKE 'experimental_hnsw_index'")
            else:
                with self.engine.begin() as conn:
                    result = _exec_sql_safe(conn, "SHOW VARIABLES LIKE 'experimental_hnsw_index'")

            row = result.fetchone()
            if row:
                return row[1] in ("1", "on", "ON")
            return None
        except Exception as e:
            print(f"Failed to get HNSW status: {e}")
            return None


def create_hnsw_config(engine: Engine) -> HNSWConfig:
    """
    Create an HNSWConfig instance.

    Args:

        engine: SQLAlchemy engine instance

    Returns:

        HNSWConfig instance
    """
    return HNSWConfig(engine)


def enable_hnsw_indexing(engine: Engine, connection: Optional[Connection] = None) -> bool:
    """
    Enable HNSW indexing in MatrixOne.

    Args:

        engine: SQLAlchemy engine instance
        connection: Optional existing database connection

    Returns:

        bool: True if successful, False otherwise
    """
    config = create_hnsw_config(engine)
    return config.enable_hnsw_indexing(connection)


def disable_hnsw_indexing(engine: Engine, connection: Optional[Connection] = None) -> bool:
    """
    Disable HNSW indexing in MatrixOne.

    Args:

        engine: SQLAlchemy engine instance
        connection: Optional existing database connection

    Returns:

        bool: True if successful, False otherwise
    """
    config = create_hnsw_config(engine)
    return config.disable_hnsw_indexing(connection)


def get_hnsw_status(engine: Engine, connection: Optional[Connection] = None) -> Optional[bool]:
    """
    Get the current HNSW indexing status.

    Args:

        engine: SQLAlchemy engine instance
        connection: Optional existing database connection

    Returns:

        bool: True if HNSW indexing is enabled, False if disabled, None if error
    """
    config = create_hnsw_config(engine)
    return config.get_hnsw_status(connection)
