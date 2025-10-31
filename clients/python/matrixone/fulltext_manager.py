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
Fulltext Index Manager for MatrixOne.

This module provides fulltext index management functionality with:
- Base class for SQL generation
- Synchronous FulltextIndexManager
- Asynchronous AsyncFulltextIndexManager
- Executor pattern for Client/Session contexts
"""

from typing import Union, List, TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client
    from .async_client import AsyncClient


def _extract_table_name(table_name_or_model) -> str:
    """Extract table name from string or SQLAlchemy model."""
    if hasattr(table_name_or_model, '__tablename__'):
        return table_name_or_model.__tablename__
    return table_name_or_model


class _FulltextManagerBase:
    """
    Base class for fulltext index managers providing SQL generation logic.

    This class contains all SQL building logic as static methods,
    allowing sync and async managers to share the same SQL generation code.
    """

    @staticmethod
    def _build_create_sql(table_name: str, name: str, columns: Union[str, List[str]], algorithm: str) -> str:
        """Build SQL for creating fulltext index."""
        if isinstance(columns, str):
            columns = [columns]
        columns_str = ", ".join(columns)

        # For now, MatrixOne doesn't support algorithm specification in CREATE statement
        # The algorithm is handled by the FulltextIndex.create_index() call
        sql = f"CREATE FULLTEXT INDEX {name} ON {table_name} ({columns_str})"
        return sql

    @staticmethod
    def _build_drop_sql(table_name: str, name: str) -> str:
        """Build SQL for dropping fulltext index."""
        return f"DROP INDEX {name} ON {table_name}"

    @staticmethod
    def _build_enable_sql() -> str:
        """Build SQL for enabling fulltext indexing."""
        return "SET experimental_fulltext_index = 1"

    @staticmethod
    def _build_disable_sql() -> str:
        """Build SQL for disabling fulltext indexing."""
        return "SET experimental_fulltext_index = 0"


class FulltextIndexManager(_FulltextManagerBase):
    """
    Fulltext index manager for MatrixOne fulltext search operations.

    This class provides comprehensive fulltext indexing functionality for
    enabling fast text search capabilities in MatrixOne databases.

    Key Features:
    - Fulltext index creation and management
    - Support for multiple fulltext algorithms (TF-IDF, BM25)
    - Multi-column fulltext indexing
    - Index optimization and maintenance
    - Chain operations for efficient index management

    Supported Algorithms:
    - TF-IDF: Term Frequency-Inverse Document Frequency (default)
    - BM25: Best Matching 25 algorithm for improved relevance scoring

    Usage Examples::

        # Create fulltext index on single column
        client.fulltext_index.create("documents", "idx_content", "content", algorithm="BM25")

        # Create fulltext index on multiple columns
        client.fulltext_index.create("articles", "idx_title_content", ["title", "content"])

        # Enable fulltext indexing
        client.fulltext_index.enable_fulltext()

        # Drop fulltext index
        client.fulltext_index.drop("documents", "idx_content")

        # Use in Session context
        with client.session() as session:
            session.fulltext_index.create("docs", "idx_test", "content")
    """

    def __init__(self, client: "Client", executor=None):
        """
        Initialize fulltext index manager.

        Args:
            client: MatrixOne Client instance
            executor: Executor for SQL execution (Client or Session)
                     If None, uses client as executor (auto-commit mode)
        """
        self.client = client
        self.executor = executor if executor is not None else client

    def create(
        self, table_name_or_model, name: str, columns: Union[str, List[str]], algorithm: str = "TF-IDF"
    ) -> "FulltextIndexManager":
        """
        Create a fulltext index.

        Args:
            table_name_or_model: Table name (str) or SQLAlchemy model class
            name: Index name
            columns: Column(s) to index (str or List[str])
            algorithm: Fulltext algorithm type (TF-IDF or BM25)

        Returns:
            Self for chaining

        Example::

            # Auto-commit mode
            client.fulltext_index.create("articles", "idx_content", ["title", "content"])

            # Transaction context mode
            with client.session() as session:
                session.fulltext_index.create("articles", "idx_content", ["title", "content"])
                session.commit()
        """
        try:
            table_name = _extract_table_name(table_name_or_model)

            # Use FulltextIndex helper for proper index creation
            from .sqlalchemy_ext import FulltextIndex

            # Determine connection based on executor type
            if self.executor is self.client:
                # Auto-commit mode: create a temporary session internally
                with self.client.session() as session:
                    conn = session.connection()
                    success = FulltextIndex.create_index(
                        bind=conn,
                        table_name=table_name,
                        name=name,
                        columns=columns,
                        algorithm=algorithm,
                    )
                    session.commit()
            else:
                # Transaction context mode: use session's connection directly
                conn = self.executor.connection()
                success = FulltextIndex.create_index(
                    bind=conn,
                    table_name=table_name,
                    name=name,
                    columns=columns,
                    algorithm=algorithm,
                )

            if not success:
                raise Exception(f"Failed to create fulltext index {name} on table {table_name}")

            return self
        except Exception as e:
            if "Failed to create" not in str(e):
                raise Exception(f"Failed to create fulltext index {name} on table {table_name}: {e}")
            raise

    def drop(self, table_name_or_model, name: str) -> "FulltextIndexManager":
        """
        Drop a fulltext index.

        Args:
            table_name_or_model: Table name (str) or SQLAlchemy model class
            name: Index name

        Returns:
            Self for chaining

        Example::

            # Auto-commit mode
            client.fulltext_index.drop("articles", "idx_content")

            # Transaction context mode
            with client.session() as session:
                session.fulltext_index.drop("articles", "idx_content")
                session.commit()
        """
        try:
            table_name = _extract_table_name(table_name_or_model)

            # Use FulltextIndex helper for proper index drop
            from .sqlalchemy_ext import FulltextIndex

            # Determine connection based on executor type
            if self.executor is self.client:
                # Auto-commit mode: create a temporary session internally
                with self.client.session() as session:
                    conn = session.connection()
                    success = FulltextIndex.drop_index(bind=conn, table_name=table_name, name=name)
                    session.commit()
            else:
                # Transaction context mode: use session's connection directly
                conn = self.executor.connection()
                success = FulltextIndex.drop_index(bind=conn, table_name=table_name, name=name)

            if not success:
                raise Exception(f"Failed to drop fulltext index {name} from table {table_name}")

            return self
        except Exception as e:
            if "Failed to drop" not in str(e):
                raise Exception(f"Failed to drop fulltext index {name} from table {table_name}: {e}")
            raise

    def enable_fulltext(self) -> "FulltextIndexManager":
        """
        Enable fulltext indexing.

        Returns:
            Self for chaining

        Example::

            client.fulltext_index.enable_fulltext()
        """
        try:
            sql = self._build_enable_sql()
            self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to enable fulltext indexing: {e}")

    def disable_fulltext(self) -> "FulltextIndexManager":
        """
        Disable fulltext indexing.

        Returns:
            Self for chaining

        Example::

            client.fulltext_index.disable_fulltext()
        """
        try:
            sql = self._build_disable_sql()
            self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to disable fulltext indexing: {e}")


class AsyncFulltextIndexManager(_FulltextManagerBase):
    """
    Async fulltext index manager for MatrixOne fulltext search operations.

    This class provides asynchronous fulltext indexing functionality.

    Usage Examples::

        # Create fulltext index
        await async_client.fulltext_index.create("documents", "idx_content", "content", algorithm="BM25")

        # Enable fulltext indexing
        await async_client.fulltext_index.enable_fulltext()

        # Drop fulltext index
        await async_client.fulltext_index.drop("documents", "idx_content")

        # Use in AsyncSession context
        async with async_client.session() as session:
            await session.fulltext_index.create("docs", "idx_test", "content")
    """

    def __init__(self, client: "AsyncClient", executor=None):
        """
        Initialize async fulltext index manager.

        Args:
            client: MatrixOne AsyncClient instance
            executor: Executor for SQL execution (AsyncClient or AsyncSession)
                     If None, uses client as executor (auto-commit mode)
        """
        self.client = client
        self.executor = executor if executor is not None else client

    async def create(
        self, table_name_or_model, name: str, columns: Union[str, List[str]], algorithm: str = "TF-IDF"
    ) -> "AsyncFulltextIndexManager":
        """
        Create a fulltext index asynchronously.

        Args:
            table_name_or_model: Table name (str) or SQLAlchemy model class
            name: Index name
            columns: Column(s) to index (str or List[str])
            algorithm: Fulltext algorithm type (TF-IDF or BM25)

        Returns:
            Self for chaining

        Example::

            # Auto-commit mode
            await async_client.fulltext_index.create("articles", "idx_content", ["title", "content"])

            # Transaction context mode
            async with async_client.session() as session:
                await session.fulltext_index.create("articles", "idx_content", ["title", "content"])
                await session.commit()
        """
        try:
            table_name = _extract_table_name(table_name_or_model)

            # Use FulltextIndex helper for proper index creation
            from .sqlalchemy_ext import FulltextIndex

            # Determine connection based on executor type
            if self.executor is self.client:
                # Auto-commit mode: create a temporary session internally
                async with self.client.session() as session:
                    conn = await session.connection()
                    success = await conn.run_sync(
                        lambda sync_conn: FulltextIndex.create_index(
                            bind=sync_conn,
                            table_name=table_name,
                            name=name,
                            columns=columns,
                            algorithm=algorithm,
                        )
                    )
                    await session.commit()
            else:
                # Transaction context mode: use session's connection directly
                conn = await self.executor.connection()
                success = await conn.run_sync(
                    lambda sync_conn: FulltextIndex.create_index(
                        bind=sync_conn,
                        table_name=table_name,
                        name=name,
                        columns=columns,
                        algorithm=algorithm,
                    )
                )

            if not success:
                raise Exception(f"Failed to create fulltext index {name} on table {table_name}")

            return self
        except Exception as e:
            if "Failed to create" not in str(e):
                raise Exception(f"Failed to create fulltext index {name} on table {table_name}: {e}")
            raise

    async def drop(self, table_name_or_model, name: str) -> "AsyncFulltextIndexManager":
        """
        Drop a fulltext index asynchronously.

        Args:
            table_name_or_model: Table name (str) or SQLAlchemy model class
            name: Index name

        Returns:
            Self for chaining

        Example::

            # Auto-commit mode
            await async_client.fulltext_index.drop("articles", "idx_content")

            # Transaction context mode
            async with async_client.session() as session:
                await session.fulltext_index.drop("articles", "idx_content")
                await session.commit()
        """
        try:
            table_name = _extract_table_name(table_name_or_model)

            # Use FulltextIndex helper for proper index drop
            from .sqlalchemy_ext import FulltextIndex

            # Determine connection based on executor type
            if self.executor is self.client:
                # Auto-commit mode: create a temporary session internally
                async with self.client.session() as session:
                    conn = await session.connection()
                    success = await conn.run_sync(
                        lambda sync_conn: FulltextIndex.drop_index(bind=sync_conn, table_name=table_name, name=name)
                    )
                    await session.commit()
            else:
                # Transaction context mode: use session's connection directly
                conn = await self.executor.connection()
                success = await conn.run_sync(
                    lambda sync_conn: FulltextIndex.drop_index(bind=sync_conn, table_name=table_name, name=name)
                )

            if not success:
                raise Exception(f"Failed to drop fulltext index {name} from table {table_name}")

            return self
        except Exception as e:
            if "Failed to drop" not in str(e):
                raise Exception(f"Failed to drop fulltext index {name} from table {table_name}: {e}")
            raise

    async def enable_fulltext(self) -> "AsyncFulltextIndexManager":
        """
        Enable fulltext indexing asynchronously.

        Returns:
            Self for chaining

        Example::

            await async_client.fulltext_index.enable_fulltext()
        """
        try:
            sql = self._build_enable_sql()
            await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to enable fulltext indexing: {e}")

    async def disable_fulltext(self) -> "AsyncFulltextIndexManager":
        """
        Disable fulltext indexing asynchronously.

        Returns:
            Self for chaining

        Example::

            await async_client.fulltext_index.disable_fulltext()
        """
        try:
            sql = self._build_disable_sql()
            await self.executor.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to disable fulltext indexing: {e}")
