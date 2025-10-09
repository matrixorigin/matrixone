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
Offline tests for IVF configuration functionality.
"""

import pytest
from unittest.mock import Mock, patch
from sqlalchemy import text

from matrixone.sqlalchemy_ext import (
    IVFConfig,
    create_ivf_config,
    enable_ivf_indexing,
    disable_ivf_indexing,
    set_probe_limit,
    get_ivf_status,
)


class TestIVFConfigOffline:
    """Test IVF configuration functionality using mocks."""

    def _create_mock_engine_with_conn(self, mock_conn):
        """Helper to create a mock engine with connection."""
        # Ensure mock_conn has exec_driver_sql if not already set
        # This allows individual tests to customize the mock before calling this helper
        if not hasattr(mock_conn, 'exec_driver_sql') or mock_conn.exec_driver_sql is None:
            mock_conn.exec_driver_sql = Mock(return_value=Mock(returns_rows=False, rowcount=0))

        mock_engine = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_conn)
        mock_context.__exit__ = Mock(return_value=None)
        mock_engine.begin.return_value = mock_context
        return mock_engine

    def test_ivf_config_creation(self):
        """Test creating IVF configuration manager."""
        mock_engine = Mock()
        config = IVFConfig(mock_engine)

        assert config.engine == mock_engine

    def test_create_ivf_config_helper(self):
        """Test create_ivf_config helper function."""
        mock_engine = Mock()
        config = create_ivf_config(mock_engine)

        assert isinstance(config, IVFConfig)
        assert config.engine == mock_engine

    def test_enable_ivf_indexing(self):
        """Test enabling IVF indexing."""
        mock_conn = Mock()
        mock_engine = self._create_mock_engine_with_conn(mock_conn)

        config = IVFConfig(mock_engine)
        result = config.enable_ivf_indexing()

        assert result is True
        # Check that exec_driver_sql was called with the correct SQL
        mock_conn.exec_driver_sql.assert_called_once()
        call_args = mock_conn.exec_driver_sql.call_args[0][0]
        assert call_args == "SET experimental_ivf_index = 1"

    def test_disable_ivf_indexing(self):
        """Test disabling IVF indexing."""
        mock_conn = Mock()
        mock_engine = self._create_mock_engine_with_conn(mock_conn)

        config = IVFConfig(mock_engine)
        result = config.disable_ivf_indexing()

        assert result is True
        # Check that execute was called with the correct SQL
        mock_conn.exec_driver_sql.assert_called_once()
        call_args = mock_conn.exec_driver_sql.call_args[0][0]
        assert call_args == "SET experimental_ivf_index = 0"

    def test_set_probe_limit(self):
        """Test setting probe limit."""
        mock_conn = Mock()
        mock_engine = self._create_mock_engine_with_conn(mock_conn)

        config = IVFConfig(mock_engine)
        result = config.set_probe_limit(5)

        assert result is True
        # Check that execute was called with the correct SQL
        mock_conn.exec_driver_sql.assert_called_once()
        call_args = mock_conn.exec_driver_sql.call_args[0][0]
        assert call_args == "SET probe_limit = 5"

    def test_configure_ivf(self):
        """Test configuring IVF with multiple parameters."""
        mock_conn = Mock()
        mock_engine = self._create_mock_engine_with_conn(mock_conn)

        config = IVFConfig(mock_engine)
        result = config.configure_ivf(enabled=True, probe_limit=3)

        assert result is True
        # Should call both enable and set probe limit
        assert mock_conn.exec_driver_sql.call_count == 2

        # Check the SQL calls
        calls = [call[0][0] for call in mock_conn.exec_driver_sql.call_args_list]
        assert "SET experimental_ivf_index = 1" in calls
        assert "SET probe_limit = 3" in calls

    def test_get_ivf_status(self):
        """Test getting IVF status."""
        mock_conn = Mock()
        mock_engine = self._create_mock_engine_with_conn(mock_conn)

        # Mock the SHOW VARIABLES results
        mock_result1 = Mock()
        mock_result1.fetchone.return_value = ("experimental_ivf_index", "1")
        mock_result2 = Mock()
        mock_result2.fetchone.return_value = ("probe_limit", "5")

        mock_conn.exec_driver_sql.side_effect = [mock_result1, mock_result2]

        config = IVFConfig(mock_engine)
        status = config.get_ivf_status()

        assert status["ivf_enabled"] is True
        assert status["probe_limit"] == 5
        assert status["error"] is None

    def test_is_ivf_supported(self):
        """Test checking if IVF is supported."""
        mock_conn = Mock()
        mock_engine = self._create_mock_engine_with_conn(mock_conn)

        config = IVFConfig(mock_engine)
        result = config.is_ivf_supported()

        assert result is True
        # Check that execute was called with the correct SQL
        mock_conn.exec_driver_sql.assert_called_once()
        call_args = mock_conn.exec_driver_sql.call_args[0][0]
        assert call_args == "SHOW VARIABLES LIKE 'experimental_ivf_index'"

    def test_is_ivf_supported_failure(self):
        """Test checking if IVF is supported when it fails."""
        mock_engine = Mock()
        mock_engine.begin.side_effect = Exception("Not supported")

        config = IVFConfig(mock_engine)
        result = config.is_ivf_supported()

        assert result is False

    def test_enable_ivf_indexing_failure(self):
        """Test enabling IVF indexing when it fails."""
        mock_engine = Mock()
        mock_engine.begin.side_effect = Exception("Connection failed")

        config = IVFConfig(mock_engine)
        result = config.enable_ivf_indexing()

        assert result is False

    def test_disable_ivf_indexing_failure(self):
        """Test disabling IVF indexing when it fails."""
        mock_engine = Mock()
        mock_engine.begin.side_effect = Exception("Connection failed")

        config = IVFConfig(mock_engine)
        result = config.disable_ivf_indexing()

        assert result is False

    def test_set_probe_limit_failure(self):
        """Test setting probe limit when it fails."""
        mock_engine = Mock()
        mock_engine.begin.side_effect = Exception("Connection failed")

        config = IVFConfig(mock_engine)
        result = config.set_probe_limit(5)

        assert result is False

    def test_get_ivf_status_with_error(self):
        """Test getting IVF status when there's an error."""
        mock_engine = Mock()
        mock_engine.begin.side_effect = Exception("Connection failed")

        config = IVFConfig(mock_engine)
        status = config.get_ivf_status()

        assert status["ivf_enabled"] is None
        assert status["probe_limit"] is None
        assert status["error"] == "Connection failed"

    def test_configure_ivf_partial_failure(self):
        """Test configuring IVF when some operations fail."""
        mock_conn = Mock()
        mock_engine = self._create_mock_engine_with_conn(mock_conn)

        # First call succeeds, second call fails
        mock_conn.exec_driver_sql.side_effect = [None, Exception("Failed")]

        config = IVFConfig(mock_engine)
        result = config.configure_ivf(enabled=True, probe_limit=3)

        assert result is False  # Should be False because one operation failed

    def test_convenience_functions(self):
        """Test convenience functions."""
        mock_conn = Mock()
        mock_engine = self._create_mock_engine_with_conn(mock_conn)

        # Test enable_ivf_indexing
        result = enable_ivf_indexing(mock_engine)
        assert result is True

        # Test disable_ivf_indexing
        result = disable_ivf_indexing(mock_engine)
        assert result is True

        # Test set_probe_limit
        result = set_probe_limit(mock_engine, 10)
        assert result is True

        # Test get_ivf_status
        mock_result1 = Mock()
        mock_result1.fetchone.return_value = ("experimental_ivf_index", "0")
        mock_result2 = Mock()
        mock_result2.fetchone.return_value = ("probe_limit", "10")
        mock_conn.exec_driver_sql.side_effect = [mock_result1, mock_result2]

        status = get_ivf_status(mock_engine)
        assert status["ivf_enabled"] is False
        assert status["probe_limit"] == 10
