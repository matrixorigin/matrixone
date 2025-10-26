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
Tests executor pattern initialization.
"""

import unittest
from unittest.mock import Mock
from matrixone.vector_manager import VectorManager, AsyncVectorManager


class TestVectorManagerExecutor(unittest.TestCase):
    """Test VectorManager executor pattern"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_session = Mock()

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


class TestAsyncVectorManagerExecutor(unittest.TestCase):
    """Test AsyncVectorManager executor pattern"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_session = Mock()

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


if __name__ == '__main__':
    unittest.main()
