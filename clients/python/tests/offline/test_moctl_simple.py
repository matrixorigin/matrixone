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
Test MoCtlManager core functionality (flush_table, increment_checkpoint, global_checkpoint only)
"""

import unittest
from unittest.mock import Mock, patch
import json
import sys
import os

# Store original modules to restore later
_original_modules = {}


def setup_sqlalchemy_mocks():
    """Setup SQLAlchemy mocks for this test class"""
    global _original_modules
    _original_modules['pymysql'] = sys.modules.get('pymysql')
    _original_modules['sqlalchemy'] = sys.modules.get('sqlalchemy')
    _original_modules['sqlalchemy.engine'] = sys.modules.get('sqlalchemy.engine')
    _original_modules['sqlalchemy.orm'] = sys.modules.get('sqlalchemy.orm')

    sys.modules['pymysql'] = Mock()
    sys.modules['sqlalchemy'] = Mock()
    sys.modules['sqlalchemy.engine'] = Mock()
    sys.modules['sqlalchemy.engine'].Engine = Mock()
    sys.modules['sqlalchemy.orm'] = Mock()
    sys.modules['sqlalchemy.orm'].sessionmaker = Mock()
    sys.modules['sqlalchemy.orm'].declarative_base = Mock()
    sys.modules['sqlalchemy'].create_engine = Mock()
    sys.modules['sqlalchemy'].text = Mock()
    sys.modules['sqlalchemy'].Column = Mock()
    sys.modules['sqlalchemy'].Integer = Mock()
    sys.modules['sqlalchemy'].String = Mock()
    sys.modules['sqlalchemy'].DateTime = Mock()


def teardown_sqlalchemy_mocks():
    """Restore original modules"""
    global _original_modules
    for module_name, original_module in _original_modules.items():
        if original_module is not None:
            sys.modules[module_name] = original_module
        elif module_name in sys.modules:
            del sys.modules[module_name]


# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone.moctl import MoCtlManager, MoCtlError
from matrixone.client import Client


class TestMoCtlManager(unittest.TestCase):
    """Test MoCtlManager core functionality"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        setup_sqlalchemy_mocks()

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        teardown_sqlalchemy_mocks()

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client.execute = Mock()
        self.moctl_manager = MoCtlManager(self.mock_client)

    def test_init(self):
        """Test MoCtlManager initialization"""
        self.assertEqual(self.moctl_manager.client, self.mock_client)

    def test_flush_table(self):
        """Test flush_table method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Flush", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result

        result = self.moctl_manager.flush_table('db1', 'users')

        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('dn', 'flush', 'db1.users')"
        self.mock_client.execute.assert_called_once_with(expected_sql)

        # Verify the result
        self.assertEqual(result['method'], 'Flush')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')

    def test_increment_checkpoint(self):
        """Test increment_checkpoint method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Checkpoint", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result

        result = self.moctl_manager.increment_checkpoint()

        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('dn', 'checkpoint', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)

        # Verify the result
        self.assertEqual(result['method'], 'Checkpoint')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')

    def test_global_checkpoint(self):
        """Test global_checkpoint method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "GlobalCheckpoint", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result

        result = self.moctl_manager.global_checkpoint()

        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('dn', 'globalcheckpoint', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)

        # Verify the result
        self.assertEqual(result['method'], 'GlobalCheckpoint')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')

    def test_flush_table_failure(self):
        """Test flush_table method with failure"""
        # Mock failure result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Flush", "result": [{"returnStr": "ERROR: Table not found"}]}',)]
        self.mock_client.execute.return_value = mock_result

        # Should raise MoCtlError
        with self.assertRaises(MoCtlError):
            self.moctl_manager.flush_table('db1', 'nonexistent')

    def test_increment_checkpoint_failure(self):
        """Test increment_checkpoint method with failure"""
        # Mock failure result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Checkpoint", "result": [{"returnStr": "ERROR: Checkpoint failed"}]}',)]
        self.mock_client.execute.return_value = mock_result

        # Should raise MoCtlError
        with self.assertRaises(MoCtlError):
            self.moctl_manager.increment_checkpoint()

    def test_global_checkpoint_failure(self):
        """Test global_checkpoint method with failure"""
        # Mock failure result
        mock_result = Mock()
        mock_result.rows = [
            ('{"method": "GlobalCheckpoint", "result": [{"returnStr": "ERROR: Global checkpoint failed"}]}',)
        ]
        self.mock_client.execute.return_value = mock_result

        # Should raise MoCtlError
        with self.assertRaises(MoCtlError):
            self.moctl_manager.global_checkpoint()


class TestMoCtlIntegration(unittest.TestCase):
    """Test MoCtlManager integration with Client"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        setup_sqlalchemy_mocks()

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        teardown_sqlalchemy_mocks()

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client.execute = Mock()
        self.moctl_manager = MoCtlManager(self.mock_client)

    def test_flush_table_integration(self):
        """Test flush_table integration"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Flush", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result

        result = self.moctl_manager.flush_table('production', 'orders')

        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('dn', 'flush', 'production.orders')"
        self.mock_client.execute.assert_called_once_with(expected_sql)

        # Verify the result
        self.assertEqual(result['method'], 'Flush')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')

    def test_increment_checkpoint_integration(self):
        """Test increment_checkpoint integration"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Checkpoint", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result

        result = self.moctl_manager.increment_checkpoint()

        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('dn', 'checkpoint', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)

        # Verify the result
        self.assertEqual(result['method'], 'Checkpoint')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')

    def test_global_checkpoint_integration(self):
        """Test global_checkpoint integration"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "GlobalCheckpoint", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result

        result = self.moctl_manager.global_checkpoint()

        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('dn', 'globalcheckpoint', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)

        # Verify the result
        self.assertEqual(result['method'], 'GlobalCheckpoint')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')


if __name__ == '__main__':
    unittest.main()
