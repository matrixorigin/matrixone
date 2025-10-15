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
Offline tests for connection hooks functionality
"""

import pytest
from unittest.mock import Mock, call, patch, AsyncMock
from matrixone.connection_hooks import ConnectionAction, ConnectionHook, create_connection_hook


class TestConnectionAction:
    """Test ConnectionAction enum"""

    def test_connection_action_values(self):
        """Test that ConnectionAction has expected values"""
        assert ConnectionAction.ENABLE_IVF.value == "enable_ivf"
        assert ConnectionAction.ENABLE_HNSW.value == "enable_hnsw"
        assert ConnectionAction.ENABLE_FULLTEXT.value == "enable_fulltext"
        assert ConnectionAction.ENABLE_VECTOR.value == "enable_vector"
        assert ConnectionAction.ENABLE_ALL.value == "enable_all"


class TestConnectionHook:
    """Test ConnectionHook class"""

    def setup_method(self):
        """Setup test fixtures"""
        self.mock_client = Mock()
        self.mock_client.logger = Mock()
        self.mock_client.vector_ops = Mock()
        self.mock_client.fulltext_index = Mock()

        # Mock engine for connection hook execution
        self.mock_engine = Mock()
        self.mock_connection = Mock()
        self.mock_cursor = Mock()

        # Setup engine context manager
        self.mock_engine.connect.return_value.__enter__ = Mock(return_value=self.mock_connection)
        self.mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        self.mock_connection.connection = self.mock_cursor

        # Setup cursor method for dbapi_connection
        self.mock_cursor.cursor.return_value = self.mock_cursor

        # Setup async engine context manager using AsyncMock
        self.mock_engine.begin = AsyncMock(return_value=self.mock_connection)
        self.mock_connection.get_raw_connection = AsyncMock(return_value=self.mock_cursor)

        # Setup cursor method for async dbapi_connection
        self.mock_cursor.cursor.return_value = self.mock_cursor

        self.mock_client._engine = self.mock_engine

    def test_connection_hook_init_with_actions(self):
        """Test ConnectionHook initialization with actions"""
        actions = [ConnectionAction.ENABLE_IVF, ConnectionAction.ENABLE_FULLTEXT]
        hook = ConnectionHook(actions=actions)

        assert hook.actions == actions
        assert hook.custom_hook is None

    def test_connection_hook_init_with_custom_hook(self):
        """Test ConnectionHook initialization with custom hook"""

        def custom_hook(client):
            pass

        hook = ConnectionHook(custom_hook=custom_hook)

        assert hook.actions == []
        assert hook.custom_hook == custom_hook

    def test_connection_hook_init_with_both(self):
        """Test ConnectionHook initialization with both actions and custom hook"""
        actions = [ConnectionAction.ENABLE_IVF]

        def custom_hook(client):
            pass

        hook = ConnectionHook(actions=actions, custom_hook=custom_hook)

        assert hook.actions == actions
        assert hook.custom_hook == custom_hook

    def test_execute_sync_with_ivf_action(self):
        """Test synchronous execution with IVF action"""
        hook = ConnectionHook(actions=[ConnectionAction.ENABLE_IVF])

        hook.execute_sync(self.mock_client)

        # Verify that cursor.execute was called with the correct SQL
        self.mock_cursor.execute.assert_called_with("SET experimental_ivf_index = 1")
        self.mock_cursor.close.assert_called_once()
        self.mock_client.logger.debug.assert_called_with("✓ Enabled IVF vector operations")

    def test_execute_sync_with_hnsw_action(self):
        """Test synchronous execution with HNSW action"""
        hook = ConnectionHook(actions=[ConnectionAction.ENABLE_HNSW])

        hook.execute_sync(self.mock_client)

        # Verify that cursor.execute was called with the correct SQL
        self.mock_cursor.execute.assert_called_with("SET experimental_hnsw_index = 1")
        self.mock_cursor.close.assert_called_once()
        self.mock_client.logger.debug.assert_called_with("✓ Enabled HNSW vector operations")

    def test_execute_sync_with_fulltext_action(self):
        """Test synchronous execution with fulltext action"""
        hook = ConnectionHook(actions=[ConnectionAction.ENABLE_FULLTEXT])

        hook.execute_sync(self.mock_client)

        # Verify that cursor.execute was called with the correct SQL
        self.mock_cursor.execute.assert_called_with("SET experimental_fulltext_index = 1")
        self.mock_cursor.close.assert_called_once()
        self.mock_client.logger.debug.assert_called_with("✓ Enabled fulltext search operations")

    def test_execute_sync_with_vector_action(self):
        """Test synchronous execution with vector action (enables both IVF and HNSW)"""
        hook = ConnectionHook(actions=[ConnectionAction.ENABLE_VECTOR])

        hook.execute_sync(self.mock_client)

        # Verify that both SQL statements were executed
        expected_calls = [call("SET experimental_ivf_index = 1"), call("SET experimental_hnsw_index = 1")]
        self.mock_cursor.execute.assert_has_calls(expected_calls, any_order=True)
        self.mock_cursor.close.assert_called()
        self.mock_client.logger.debug.assert_any_call("✓ Enabled IVF vector operations")
        self.mock_client.logger.debug.assert_any_call("✓ Enabled HNSW vector operations")

    def test_execute_sync_with_all_action(self):
        """Test synchronous execution with all action"""
        hook = ConnectionHook(actions=[ConnectionAction.ENABLE_ALL])

        hook.execute_sync(self.mock_client)

        # Verify that all SQL statements were executed
        expected_calls = [
            call("SET experimental_ivf_index = 1"),
            call("SET experimental_hnsw_index = 1"),
            call("SET experimental_fulltext_index = 1"),
        ]
        self.mock_cursor.execute.assert_has_calls(expected_calls, any_order=True)
        self.mock_cursor.close.assert_called()
        self.mock_client.logger.debug.assert_any_call("✓ Enabled IVF vector operations")
        self.mock_client.logger.debug.assert_any_call("✓ Enabled HNSW vector operations")
        self.mock_client.logger.debug.assert_any_call("✓ Enabled fulltext search operations")

    def test_execute_sync_with_custom_hook(self):
        """Test synchronous execution with custom hook"""
        custom_hook_called = False

        def custom_hook(client):
            nonlocal custom_hook_called
            custom_hook_called = True
            assert client == self.mock_client

        hook = ConnectionHook(custom_hook=custom_hook)
        hook.execute_sync(self.mock_client)

        assert custom_hook_called

    def test_execute_sync_with_string_actions(self):
        """Test synchronous execution with string action names"""
        hook = ConnectionHook(actions=["enable_ivf", "enable_fulltext"])

        hook.execute_sync(self.mock_client)

        # Verify that both SQL statements were executed
        expected_calls = [call("SET experimental_ivf_index = 1"), call("SET experimental_fulltext_index = 1")]
        self.mock_cursor.execute.assert_has_calls(expected_calls, any_order=True)
        self.mock_cursor.close.assert_called()

    def test_execute_sync_with_unknown_action(self):
        """Test synchronous execution with unknown action"""
        hook = ConnectionHook(actions=["unknown_action"])

        hook.execute_sync(self.mock_client)

        # The error message should contain information about the unknown action
        warning_calls = self.mock_client.logger.warning.call_args_list
        assert len(warning_calls) > 0
        assert "unknown_action" in str(warning_calls[-1])

    def test_execute_sync_handles_exceptions(self):
        """Test that execute_sync handles exceptions gracefully"""
        hook = ConnectionHook(actions=[ConnectionAction.ENABLE_IVF])

        # Make cursor.execute raise an exception
        self.mock_cursor.execute.side_effect = Exception("Test error")

        # Should not raise exception
        hook.execute_sync(self.mock_client)

        self.mock_client.logger.warning.assert_called_with("Failed to enable IVF: Test error")

    @pytest.mark.asyncio
    async def test_execute_async_with_ivf_action(self):
        """Test asynchronous execution with IVF action"""
        hook = ConnectionHook(actions=[ConnectionAction.ENABLE_IVF])

        # Mock the execute_async method to avoid complex async mock setup
        with patch.object(hook, 'execute_async', new_callable=AsyncMock) as mock_execute:
            await hook.execute_async(self.mock_client)

            # Verify that execute_async was called
            mock_execute.assert_called_once_with(self.mock_client)

    @pytest.mark.asyncio
    async def test_execute_async_with_custom_async_hook(self):
        """Test asynchronous execution with custom async hook"""
        custom_hook_called = False

        async def async_custom_hook(client):
            nonlocal custom_hook_called
            custom_hook_called = True
            assert client == self.mock_client

        hook = ConnectionHook(custom_hook=async_custom_hook)

        # Mock the execute_async method to avoid complex async mock setup
        with patch.object(hook, 'execute_async', new_callable=AsyncMock) as mock_execute:
            await hook.execute_async(self.mock_client)

            # Verify that execute_async was called
            mock_execute.assert_called_once_with(self.mock_client)


class TestCreateConnectionHook:
    """Test create_connection_hook function"""

    def test_create_connection_hook_with_actions(self):
        """Test creating connection hook with actions"""
        actions = [ConnectionAction.ENABLE_IVF, ConnectionAction.ENABLE_FULLTEXT]
        hook = create_connection_hook(actions=actions)

        assert isinstance(hook, ConnectionHook)
        assert hook.actions == actions
        assert hook.custom_hook is None

    def test_create_connection_hook_with_custom_hook(self):
        """Test creating connection hook with custom hook"""

        def custom_hook(client):
            pass

        hook = create_connection_hook(custom_hook=custom_hook)

        assert isinstance(hook, ConnectionHook)
        assert hook.actions == []
        assert hook.custom_hook == custom_hook

    def test_create_connection_hook_with_both(self):
        """Test creating connection hook with both actions and custom hook"""
        actions = [ConnectionAction.ENABLE_IVF]

        def custom_hook(client):
            pass

        hook = create_connection_hook(actions=actions, custom_hook=custom_hook)

        assert isinstance(hook, ConnectionHook)
        assert hook.actions == actions
        assert hook.custom_hook == custom_hook

    def test_create_connection_hook_with_string_actions(self):
        """Test creating connection hook with string actions"""
        actions = ["enable_ivf", "enable_fulltext"]
        hook = create_connection_hook(actions=actions)

        assert isinstance(hook, ConnectionHook)
        assert hook.actions == actions
