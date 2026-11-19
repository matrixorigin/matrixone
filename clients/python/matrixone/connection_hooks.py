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
from contextvars import ContextVar
import inspect
import threading
from typing import Callable, List, Optional, Union

from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

_initializing_clients = ContextVar('matrixone_initializing_clients', default=())
_settings_registration = threading.Lock()


def assert_not_in_hook(client):
    if any(owner is client for owner in _initializing_clients.get()):
        raise RuntimeError(
            'on_connect cannot reenter the same client pool; use ConnectionAction for session initialization'
        )


class ConnectionAction(Enum):
    """Predefined connection actions that can be executed after connecting"""

    ENABLE_IVF = "enable_ivf"
    ENABLE_HNSW = "enable_hnsw"
    ENABLE_FULLTEXT = "enable_fulltext"
    ENABLE_VECTOR = "enable_vector"  # Enables both IVF and HNSW
    ENABLE_ALL = "enable_all"  # Enables all features


class ConnectionHook:
    """Connection hook that executes actions after successful connection"""

    def __init__(
        self, actions: Optional[List[Union[ConnectionAction, str]]] = None, custom_hook: Optional[Callable] = None
    ):
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
        self._async_engine = False
        self._settings = {}
        self._settings_revision = 0
        self._settings_lock = threading.Lock()
        self._settings_publication_lock = threading.Lock()
        self._settings_token = object()

    def set_client(self, client):
        """Set the client reference for this hook"""
        self._client_ref = client

    def attach_to_engine(self, engine):
        """Initialize once per physical generation, including already-open adoption."""
        self._async_engine = isinstance(engine, AsyncEngine)
        target = engine.sync_engine if self._async_engine else engine
        event.listen(target, 'checkout', self._on_checkout)

    def _on_checkout(self, dbapi_connection, connection_record, connection_proxy):
        assert_not_in_hook(self._client_ref)
        # Checkout owns cleanup even on cancellation in SQLAlchemy 1.4 and 2.0.
        # Existing engine connections may predate hook attachment. Record.info
        # is cleared by SQLAlchemy when a physical connection is replaced.
        self._on_connect_sync(dbapi_connection, connection_record)
        self._apply_settings(dbapi_connection, connection_record)

    def _apply_settings(self, connection, record):
        if not self._settings:
            return
        with self._settings_lock:
            revision, settings = self._settings_revision, self._settings
        if record.info.get(self._settings_token) == revision:
            return
        cursor = connection.cursor()
        try:
            for name, value in settings.items():
                cursor.execute('SET ' + name + ' = %s', (value,))
        finally:
            cursor.close()
        record.info[self._settings_token] = revision

    def _on_connect_sync(self, dbapi_connection, connection_record):
        client = self._client_ref
        if client is None or connection_record.info.get(self):
            return
        assert_not_in_hook(client)
        connection_record.info[self] = True
        token = _initializing_clients.set(_initializing_clients.get() + (client,))
        try:
            for action in self.actions:
                action = ConnectionAction(action) if isinstance(action, str) else action
                self._action_handlers[action](client, dbapi_connection)
            if self.custom_hook is not None:
                result = self.custom_hook(client)
                if inspect.isawaitable(result):
                    if not self._async_engine:
                        if inspect.iscoroutine(result):
                            result.close()
                        raise TypeError('An asynchronous on_connect callback requires AsyncClient')
                    # The adapter's checkout event runs inside SQLAlchemy's
                    # greenlet, so this suspends admission until completion.
                    from sqlalchemy.util.concurrency import await_only

                    await_only(result)
        except Exception as error:
            client.logger.warning(f'Connection hook execution failed: {error}')
        finally:
            _initializing_clients.reset(token)

    async def execute_async(self, client) -> None:
        """Execute hook actions asynchronously (for immediate execution)"""
        try:
            # For immediate execution, we need to get a connection from the client
            # This is a fallback for when we don't have a specific connection
            if hasattr(client, '_engine') and client._engine:
                async with client._engine.connect() as conn:
                    # Execute actions directly on async connection
                    await self.execute_async_on_connection(client, conn)
            else:
                client.logger.warning("No engine available for connection hook execution")

        except Exception as e:
            client.logger.warning(f"Connection hook execution failed: {e}")

    async def execute_async_on_connection(self, client, async_connection) -> None:
        """Execute hook actions on an AsyncConnection (SQLAlchemy async connection)"""
        try:
            # Execute predefined actions
            for action in self.actions:
                if isinstance(action, str):
                    action = ConnectionAction(action)

                # Execute SQL directly on async connection
                if action == ConnectionAction.ENABLE_IVF:
                    await async_connection.execute(text("SET experimental_ivf_index = 1"))
                    client.logger.debug("✓ Enabled IVF vector operations")
                elif action == ConnectionAction.ENABLE_HNSW:
                    await async_connection.execute(text("SET experimental_hnsw_index = 1"))
                    client.logger.debug("✓ Enabled HNSW vector operations")
                elif action == ConnectionAction.ENABLE_FULLTEXT:
                    await async_connection.execute(text("SET experimental_fulltext_index = 1"))
                    client.logger.debug("✓ Enabled fulltext search operations")
                elif action == ConnectionAction.ENABLE_VECTOR:
                    await async_connection.execute(text("SET experimental_ivf_index = 1"))
                    await async_connection.execute(text("SET experimental_hnsw_index = 1"))
                    client.logger.debug("✓ Enabled vector operations")
                elif action == ConnectionAction.ENABLE_ALL:
                    await async_connection.execute(text("SET experimental_ivf_index = 1"))
                    await async_connection.execute(text("SET experimental_hnsw_index = 1"))
                    await async_connection.execute(text("SET experimental_fulltext_index = 1"))
                    client.logger.debug("✓ Enabled all operations")
                else:
                    client.logger.warning(f"Unknown connection action: {action}")

            # Execute custom hook if provided
            if self.custom_hook:
                if hasattr(self.custom_hook, '__call__'):
                    # Check if it's an async function
                    if hasattr(self.custom_hook, '__code__') and self.custom_hook.__code__.co_flags & 0x80:
                        await self.custom_hook(client)
                    else:
                        # Try to call it as sync
                        result = self.custom_hook(client)
                        # If it returns a coroutine, await it
                        if hasattr(result, '__await__'):
                            await result

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
                    if (
                        hasattr(self.custom_hook, '__code__') and self.custom_hook.__code__.co_flags & 0x80
                    ):  # Check if async
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
                    result = self.custom_hook(client)
                    if inspect.isawaitable(result):
                        if inspect.iscoroutine(result):
                            result.close()
                        raise TypeError('An asynchronous on_connect callback requires AsyncClient')
                else:
                    client.logger.warning("Custom hook is not callable")

        except Exception as e:
            client.logger.warning(f"Connection hook execution failed: {e}")

    def _enable_ivf_with_connection(self, client, dbapi_connection) -> None:
        """Enable IVF vector operations using provided connection"""
        try:
            # Execute SQL directly on the connection to avoid creating new connections
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute("SET experimental_ivf_index = 1")
            finally:
                cursor.close()
            client.logger.debug("✓ Enabled IVF vector operations")
        except Exception as e:
            client.logger.warning(f"Failed to enable IVF: {e}")

    def _enable_hnsw_with_connection(self, client, dbapi_connection) -> None:
        """Enable HNSW vector operations using provided connection"""
        try:
            # Execute SQL directly on the connection to avoid creating new connections
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute("SET experimental_hnsw_index = 1")
            finally:
                cursor.close()
            client.logger.debug("✓ Enabled HNSW vector operations")
        except Exception as e:
            client.logger.warning(f"Failed to enable HNSW: {e}")

    def _enable_fulltext_with_connection(self, client, dbapi_connection) -> None:
        """Enable fulltext search operations using provided connection"""
        try:
            # Execute SQL directly on the connection to avoid creating new connections
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute("SET experimental_fulltext_index = 1")
            finally:
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


def pool_settings_version(engine):
    """Identity of the existing checkout policy, for observation caches."""
    engine = getattr(engine, 'sync_engine', engine)
    hook = getattr(engine, '_matrixone_pool_settings', None)
    if hook is None:
        return None
    with hook._settings_lock:
        return hook, hook._settings_revision


def configure_pool_settings(engine, **values):
    """Apply desired settings on future checkouts, including recycled sessions.

    Connections already borrowed by another owner are deliberately untouched.
    Checkout owns failed-connection invalidation; no query is retried here.
    """
    allowed = {'experimental_ivf_index', 'experimental_hnsw_index', 'probe_limit'}
    if not values or not set(values) <= allowed:
        raise ValueError('Unsupported pool setting')
    if not isinstance(engine, Engine):
        raise TypeError('Pool settings require a synchronous SQLAlchemy Engine')
    with _settings_registration:
        hook = getattr(engine, '_matrixone_pool_settings', None)
        if hook is None:
            hook = ConnectionHook()
            hook.attach_to_engine(engine)
            engine._matrixone_pool_settings = hook
    with hook._settings_publication_lock:
        with hook._settings_lock:
            previous = hook._settings
            hook._settings = dict(previous, **values)
            hook._settings_revision += 1
            revision = hook._settings_revision
        try:
            # The checkout listener applies and validates the published generation.
            with engine.connect():
                pass
        except BaseException as primary:
            restored = False
            with hook._settings_lock:
                if hook._settings_revision == revision:
                    hook._settings = previous
                    hook._settings_revision += 1
                    restored = True
            if restored:
                # A concurrent checkout may have applied part of the rejected
                # generation. Retire idle sessions; borrowed sessions keep their
                # existing owner and will not reenter the replacement pool.
                try:
                    engine.dispose()
                except BaseException as cleanup_error:
                    primary.cleanup_error = cleanup_error
            raise
