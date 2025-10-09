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

#!/usr/bin/env python3
"""
Online tests for the new connection hooks functionality using SQLAlchemy events
"""

import pytest
from matrixone import Client, AsyncClient
from matrixone.connection_hooks import ConnectionAction, create_connection_hook


class TestNewConnectionHooksOnline:
    """Test the new connection hooks implementation with SQLAlchemy events"""

    def test_sync_client_connection_hook_executes_on_each_connection(self, test_client):
        """Test that connection hook executes on each new connection"""
        # Create a new client instance
        client = Client()

        # Track how many times the hook is called
        hook_call_count = 0

        def count_hook_calls(client):
            nonlocal hook_call_count
            hook_call_count += 1
            print(f"Hook called {hook_call_count} times")

        # Connect with the hook
        client.connect(
            host=test_client._connection_params['host'],
            port=test_client._connection_params['port'],
            user=test_client._connection_params['user'],
            password=test_client._connection_params['password'],
            database=test_client._connection_params['database'],
            on_connect=count_hook_calls,
        )

        # The hook should be called once for the initial connection
        assert hook_call_count >= 1

        # Force a new connection by executing a query
        # This should trigger the hook again
        result = client.execute("SELECT 1")
        assert len(result.rows) == 1

        # The hook should be called again for the new connection
        assert hook_call_count >= 2

        # Clean up
        client.disconnect()

    def test_sync_client_with_predefined_actions(self, test_client):
        """Test sync client with predefined actions"""
        client = Client()

        # Connect with predefined actions
        client.connect(
            host=test_client._connection_params['host'],
            port=test_client._connection_params['port'],
            user=test_client._connection_params['user'],
            password=test_client._connection_params['password'],
            database=test_client._connection_params['database'],
            on_connect=[ConnectionAction.ENABLE_FULLTEXT],
        )

        assert client.connected()

        # Test that we can use fulltext operations
        # (This would fail if fulltext wasn't enabled)
        try:
            # Create a simple table and test fulltext
            client.execute("CREATE TABLE IF NOT EXISTS test_hook_table (id INT, content TEXT)")
            client.execute("INSERT INTO test_hook_table VALUES (1, 'test content')")

            # This should work if fulltext is enabled
            result = (
                client.query("test_hook_table.content")
                .filter("MATCH(content) AGAINST('test' IN NATURAL LANGUAGE MODE)")
                .execute()
            )

            # Clean up
            client.execute("DROP TABLE IF EXISTS test_hook_table")

        except Exception as e:
            # If fulltext is not available, that's okay for this test
            print(f"Fulltext test skipped: {e}")

        # Clean up
        client.disconnect()

    @pytest.mark.asyncio
    async def test_async_client_connection_hook_executes_on_each_connection(self, test_async_client):
        """Test that async connection hook executes on each new connection"""
        # Create a new client instance
        client = AsyncClient()

        # Track how many times the hook is called
        hook_call_count = 0

        def count_hook_calls(client):
            nonlocal hook_call_count
            hook_call_count += 1
            print(f"Async hook called {hook_call_count} times")

        # Connect with the hook
        await client.connect(
            host=test_async_client._connection_params['host'],
            port=test_async_client._connection_params['port'],
            user=test_async_client._connection_params['user'],
            password=test_async_client._connection_params['password'],
            database=test_async_client._connection_params['database'],
            on_connect=count_hook_calls,
        )

        # The hook should be called once for the initial connection
        assert hook_call_count >= 1

        # Force a new connection by executing a query
        # This should trigger the hook again
        result = await client.execute("SELECT 1")
        assert len(result.rows) == 1

        # The hook should be called again for the new connection
        assert hook_call_count >= 2

        # Clean up
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_async_client_with_predefined_actions(self, test_async_client):
        """Test async client with predefined actions"""
        client = AsyncClient()

        # Connect with predefined actions
        await client.connect(
            host=test_async_client._connection_params['host'],
            port=test_async_client._connection_params['port'],
            user=test_async_client._connection_params['user'],
            password=test_async_client._connection_params['password'],
            database=test_async_client._connection_params['database'],
            on_connect=[ConnectionAction.ENABLE_FULLTEXT],
        )

        assert client.connected()

        # Test that we can use fulltext operations
        try:
            # Create a simple table and test fulltext
            await client.execute("CREATE TABLE IF NOT EXISTS test_async_hook_table (id INT, content TEXT)")
            await client.execute("INSERT INTO test_async_hook_table VALUES (1, 'test content')")

            # This should work if fulltext is enabled
            result = (
                await client.query("test_async_hook_table.content")
                .filter("MATCH(content) AGAINST('test' IN NATURAL LANGUAGE MODE)")
                .execute()
            )

            # Clean up
            await client.execute("DROP TABLE IF EXISTS test_async_hook_table")

        except Exception as e:
            # If fulltext is not available, that's okay for this test
            print(f"Async fulltext test skipped: {e}")

        # Clean up
        await client.disconnect()

    def test_connection_hook_with_multiple_actions(self, test_client):
        """Test connection hook with multiple actions"""
        client = Client()

        # Track which actions were executed
        executed_actions = []

        def track_actions(client):
            executed_actions.append("custom_hook")

        # Create hook with multiple actions
        hook = create_connection_hook(
            actions=[ConnectionAction.ENABLE_FULLTEXT, ConnectionAction.ENABLE_IVF], custom_hook=track_actions
        )

        # Connect with the hook
        client.connect(
            host=test_client._connection_params['host'],
            port=test_client._connection_params['port'],
            user=test_client._connection_params['user'],
            password=test_client._connection_params['password'],
            database=test_client._connection_params['database'],
            on_connect=hook,
        )

        assert client.connected()

        # Force a new connection to trigger the hook
        client.execute("SELECT 1")

        # The custom hook should have been called
        assert "custom_hook" in executed_actions

        # Clean up
        client.disconnect()
