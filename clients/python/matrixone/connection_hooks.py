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
Connection hooks for MatrixOne clients
"""

from enum import Enum
from typing import Callable, List, Optional, Union

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine


class ConnectionAction(Enum):
    """Predefined connection actions that can be executed after connecting"""

    ENABLE_IVF = "enable_ivf"
    ENABLE_HNSW = "enable_hnsw"
    ENABLE_FULLTEXT = "enable_fulltext"
    ENABLE_VECTOR = "enable_vector"  # Enables both IVF and HNSW
    ENABLE_ALL = "enable_all"  # Enables all features


class ConnectionHook:
    """Connection hook that executes actions after successful connection"""

    def __init__(self, actions: Optional[List[Union[ConnectionAction, str]]] = None, custom_hook: Optional[Callable] = None):
        """
        Initialize connection hook

        Args:

            actions: List of predefined actions to execute
            custom_hook: Custom callback function to execute
        """
        self.actions = actions or []
        self.custom_hook = custom_hook
        self._action_handlers = {
            ConnectionAction.ENABLE_IVF: self._enable_ivf_with_connection,
            ConnectionAction.ENABLE_HNSW: self._enable_hnsw_with_connection,
            ConnectionAction.ENABLE_FULLTEXT: self._enable_fulltext_with_connection,
            ConnectionAction.ENABLE_VECTOR: self._enable_vector_with_connection,
            ConnectionAction.ENABLE_ALL: self._enable_all_with_connection,
        }
        self._client_ref = None  # Will be set when hook is attached to a client
        self._executed_connections = set()  # Track which connections have executed the hook

    def set_client(self, client):
        """Set the client reference for this hook"""
        self._client_ref = client

    def attach_to_engine(self, engine: Union[Engine, AsyncEngine]):
        """Attach this hook to a SQLAlchemy engine to listen for connection events"""
        if isinstance(engine, AsyncEngine):
            # For async engines, listen to both connect and before_cursor_execute events
            event.listen(engine.sync_engine, "connect", self._on_connect_sync)
            event.listen(engine.sync_engine, "before_cursor_execute", self._on_before_cursor_execute)
            if hasattr(self._client_ref, 'logger'):
                self._client_ref.logger.debug("Attached connection hook to async engine")
        else:
            # For sync engines, listen to both connect and before_cursor_execute events
            event.listen(engine, "connect", self._on_connect_sync)
            event.listen(engine, "before_cursor_execute", self._on_before_cursor_execute)
            if hasattr(self._client_ref, 'logger'):
                self._client_ref.logger.debug("Attached connection hook to sync engine")

    def _on_connect_sync(self, dbapi_connection, connection_record):
        """SQLAlchemy event handler for new connections (sync)"""
        if self._client_ref:
            # Get connection ID to track which connections have executed the hook
            conn_id = id(dbapi_connection)
            if conn_id not in self._executed_connections:
                try:
                    # Log that the hook is being executed
                    if hasattr(self._client_ref, 'logger'):
                        self._client_ref.logger.debug(f"Executing connection hook on new connection {conn_id}")
                    # Pass the connection to avoid creating new connections
                    self.execute_sync_with_connection(self._client_ref, dbapi_connection)
                    self._executed_connections.add(conn_id)
                except Exception as e:
                    # Log error but don't fail the connection
                    if hasattr(self._client_ref, 'logger'):
                        self._client_ref.logger.warning(f"Connection hook execution failed: {e}")

    def _on_before_cursor_execute(self, conn, cursor, statement, parameters, context, executemany):
        """SQLAlchemy event handler for before cursor execute"""
        if self._client_ref:
            # Get connection ID to track which connections have executed the hook
            conn_id = id(conn.connection)
            if conn_id not in self._executed_connections:
                try:
                    # Log that the hook is being executed
                    if hasattr(self._client_ref, 'logger'):
                        self._client_ref.logger.debug(f"Executing connection hook on connection {conn_id}")
                    # Use the connection to avoid creating new connections
                    self.execute_sync_with_connection(self._client_ref, conn.connection)
                    self._executed_connections.add(conn_id)
                except Exception as e:
                    # Log error but don't fail the query
                    if hasattr(self._client_ref, 'logger'):
                        self._client_ref.logger.warning(f"Connection hook execution failed: {e}")

    async def execute_async(self, client) -> None:
        """Execute hook actions asynchronously (for immediate execution)"""
        try:
            # For immediate execution, we need to get a connection from the client
            # This is a fallback for when we don't have a specific connection
            if hasattr(client, '_engine') and client._engine:
                async with client._engine.begin() as conn:
                    # For async connections, use get_raw_connection() method
                    raw_conn = await conn.get_raw_connection()
                    await self.execute_async_with_connection(client, raw_conn)
            else:
                client.logger.warning("No engine available for connection hook execution")

        except Exception as e:
            client.logger.warning(f"Connection hook execution failed: {e}")

    async def execute_async_with_connection(self, client, dbapi_connection) -> None:
        """Execute hook actions asynchronously using the provided connection"""
        try:
            # Execute predefined actions with connection
            for action in self.actions:
                if isinstance(action, str):
                    action = ConnectionAction(action)

                if action in self._action_handlers:
                    # For async, we still use the sync methods since they work with direct connection
                    self._action_handlers[action](client, dbapi_connection)
                else:
                    client.logger.warning(f"Unknown connection action: {action}")

            # Execute custom hook if provided
            if self.custom_hook:
                if hasattr(self.custom_hook, '__call__'):
                    # Check if it's an async function
                    if hasattr(self.custom_hook, '__code__') and self.custom_hook.__code__.co_flags & 0x80:  # Check if async
                        await self.custom_hook(client)
                    else:
                        # Try to call it as sync, but handle potential async functions
                        try:
                            result = self.custom_hook(client)
                            # If it returns a coroutine, await it
                            if hasattr(result, '__await__'):
                                await result
                        except TypeError as e:
                            if "object NoneType can't be used in 'await' expression" in str(e):
                                # This is likely an async function being called without await
                                client.logger.warning("Custom hook appears to be async but was called synchronously")
                            else:
                                raise
                else:
                    client.logger.warning("Custom hook is not callable")

        except Exception as e:
            client.logger.warning(f"Connection hook execution failed: {e}")

    def execute_sync(self, client) -> None:
        """Execute hook actions synchronously (for immediate execution)"""
        try:
            # For immediate execution, we need to get a connection from the client
            # This is a fallback for when we don't have a specific connection
            if hasattr(client, '_engine') and client._engine:
                with client._engine.connect() as conn:
                    self.execute_sync_with_connection(client, conn.connection)
            else:
                client.logger.warning("No engine available for connection hook execution")

        except Exception as e:
            client.logger.warning(f"Connection hook execution failed: {e}")

    def execute_sync_with_connection(self, client, dbapi_connection) -> None:
        """Execute hook actions synchronously using the provided connection"""
        try:
            # Execute predefined actions with connection
            for action in self.actions:
                if isinstance(action, str):
                    action = ConnectionAction(action)

                if action in self._action_handlers:
                    self._action_handlers[action](client, dbapi_connection)
                else:
                    client.logger.warning(f"Unknown connection action: {action}")

            # Execute custom hook if provided
            if self.custom_hook:
                if hasattr(self.custom_hook, '__call__'):
                    self.custom_hook(client)
                else:
                    client.logger.warning("Custom hook is not callable")

        except Exception as e:
            client.logger.warning(f"Connection hook execution failed: {e}")

    def _enable_ivf_with_connection(self, client, dbapi_connection) -> None:
        """Enable IVF vector operations using provided connection"""
        try:
            # Execute SQL directly on the connection to avoid creating new connections
            cursor = dbapi_connection.cursor()
            cursor.execute("SET experimental_ivf_index = 1")
            cursor.close()
            client.logger.debug("✓ Enabled IVF vector operations")
        except Exception as e:
            client.logger.warning(f"Failed to enable IVF: {e}")

    def _enable_hnsw_with_connection(self, client, dbapi_connection) -> None:
        """Enable HNSW vector operations using provided connection"""
        try:
            # Execute SQL directly on the connection to avoid creating new connections
            cursor = dbapi_connection.cursor()
            cursor.execute("SET experimental_hnsw_index = 1")
            cursor.close()
            client.logger.debug("✓ Enabled HNSW vector operations")
        except Exception as e:
            client.logger.warning(f"Failed to enable HNSW: {e}")

    def _enable_fulltext_with_connection(self, client, dbapi_connection) -> None:
        """Enable fulltext search operations using provided connection"""
        try:
            # Execute SQL directly on the connection to avoid creating new connections
            cursor = dbapi_connection.cursor()
            cursor.execute("SET experimental_fulltext_index = 1")
            cursor.close()
            client.logger.debug("✓ Enabled fulltext search operations")
        except Exception as e:
            client.logger.warning(f"Failed to enable fulltext: {e}")

    def _enable_vector_with_connection(self, client, dbapi_connection) -> None:
        """Enable both IVF and HNSW vector operations using provided connection"""
        self._enable_ivf_with_connection(client, dbapi_connection)
        self._enable_hnsw_with_connection(client, dbapi_connection)

    def _enable_all_with_connection(self, client, dbapi_connection) -> None:
        """Enable all available operations using provided connection"""
        self._enable_vector_with_connection(client, dbapi_connection)
        self._enable_fulltext_with_connection(client, dbapi_connection)


def create_connection_hook(
    actions: Optional[List[Union[ConnectionAction, str]]] = None, custom_hook: Optional[Callable] = None
) -> ConnectionHook:
    """
    Create a connection hook with predefined actions and/or custom callback

    Args:

        actions: List of predefined actions to execute
        custom_hook: Custom callback function to execute

    Returns:

        ConnectionHook: Configured connection hook

    Examples:

        # Enable all features
        hook = create_connection_hook([ConnectionAction.ENABLE_ALL])

        # Enable only vector operations
        hook = create_connection_hook([ConnectionAction.ENABLE_VECTOR])

        # Custom hook
        def my_hook(client):
            print(f"Connected to {client._connection_params['host']}")

        hook = create_connection_hook(custom_hook=my_hook)

        # Mixed actions
        hook = create_connection_hook(
            actions=[ConnectionAction.ENABLE_FULLTEXT],
            custom_hook=my_hook
        )
    """
    return ConnectionHook(actions, custom_hook)
