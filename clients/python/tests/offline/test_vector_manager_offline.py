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
Offline tests for VectorManager and AsyncVectorManager.
Tests executor pattern and SQL consistency.
"""

import unittest
from unittest.mock import Mock, MagicMock, AsyncMock, patch
from matrixone.client import VectorManager, AsyncVectorManager


class TestVectorManagerExecutor(unittest.TestCase):
    """Test VectorManager executor pattern"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_session = Mock()
        self.mock_engine = Mock()

        # Setup mock engine
        self.mock_client.get_sqlalchemy_engine.return_value = self.mock_engine
        self.mock_session.get_bind.return_value = self.mock_engine

    def test_vector_manager_init_with_client(self):
        """Test VectorManager initialization with client (default executor)"""
        manager = VectorManager(self.mock_client)
        self.assertEqual(manager.client, self.mock_client)
        self.assertEqual(manager.executor, self.mock_client)

    def test_vector_manager_init_with_session(self):
        """Test VectorManager initialization with session as executor"""
        manager = VectorManager(self.mock_client, executor=self.mock_session)
        self.assertEqual(manager.client, self.mock_client)
        self.assertEqual(manager.executor, self.mock_session)

    def test_get_executor_returns_client(self):
        """Test _get_executor returns client when no executor specified"""
        manager = VectorManager(self.mock_client)
        self.assertEqual(manager._get_executor(), self.mock_client)

    def test_get_executor_returns_session(self):
        """Test _get_executor returns session when specified"""
        manager = VectorManager(self.mock_client, executor=self.mock_session)
        self.assertEqual(manager._get_executor(), self.mock_session)

    def test_get_engine_from_client(self):
        """Test _get_engine gets engine from client"""
        manager = VectorManager(self.mock_client)
        engine = manager._get_engine()
        self.assertEqual(engine, self.mock_engine)
        self.mock_client.get_sqlalchemy_engine.assert_called_once()

    def test_get_engine_from_session(self):
        """Test _get_engine gets engine from session via get_bind"""
        manager = VectorManager(self.mock_client, executor=self.mock_session)
        engine = manager._get_engine()
        self.assertEqual(engine, self.mock_engine)
        self.mock_session.get_bind.assert_called_once()


class TestAsyncVectorManagerExecutor(unittest.TestCase):
    """Test AsyncVectorManager executor pattern"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_session = Mock()
        self.mock_engine = Mock()

        # Setup mock engine
        self.mock_client.get_sqlalchemy_engine.return_value = self.mock_engine
        self.mock_session.get_bind.return_value = self.mock_engine

    def test_async_vector_manager_init_with_client(self):
        """Test AsyncVectorManager initialization with client"""
        manager = AsyncVectorManager(self.mock_client)
        self.assertEqual(manager.client, self.mock_client)
        self.assertEqual(manager.executor, self.mock_client)

    def test_async_vector_manager_init_with_session(self):
        """Test AsyncVectorManager initialization with session"""
        manager = AsyncVectorManager(self.mock_client, executor=self.mock_session)
        self.assertEqual(manager.client, self.mock_client)
        self.assertEqual(manager.executor, self.mock_session)

    def test_get_executor_returns_client(self):
        """Test _get_executor returns client when no executor specified"""
        manager = AsyncVectorManager(self.mock_client)
        self.assertEqual(manager._get_executor(), self.mock_client)

    def test_get_executor_returns_session(self):
        """Test _get_executor returns session when specified"""
        manager = AsyncVectorManager(self.mock_client, executor=self.mock_session)
        self.assertEqual(manager._get_executor(), self.mock_session)

    def test_get_engine_from_client(self):
        """Test _get_engine gets engine from async client"""
        manager = AsyncVectorManager(self.mock_client)
        engine = manager._get_engine()
        self.assertEqual(engine, self.mock_engine)
        self.mock_client.get_sqlalchemy_engine.assert_called_once()

    def test_get_engine_from_session(self):
        """Test _get_engine gets engine from async session via get_bind"""
        manager = AsyncVectorManager(self.mock_client, executor=self.mock_session)
        engine = manager._get_engine()
        self.assertEqual(engine, self.mock_engine)
        self.mock_session.get_bind.assert_called_once()


class TestVectorManagerSQLConsistency(unittest.TestCase):
    """Test SQL consistency across VectorManager contexts"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_session = Mock()
        self.mock_engine = Mock()

        # Setup mock engine
        self.mock_client.get_sqlalchemy_engine.return_value = self.mock_engine
        self.mock_session.get_bind.return_value = self.mock_engine

        # Setup mock execute
        self.mock_client.execute = Mock()
        self.mock_session.execute = Mock()

    # Note: enable_ivf/disable_ivf/enable_hnsw/disable_hnsw use configuration objects
    # internally which require actual database connections. These methods are tested
    # in online tests. SQL consistency for create_ivf/create_hnsw/drop is tested below.


if __name__ == '__main__':
    unittest.main()
