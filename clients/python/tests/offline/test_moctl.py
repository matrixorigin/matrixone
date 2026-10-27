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
Test MoCtlManager functionality
"""

import unittest
from unittest.mock import Mock, patch
import json
import sys
import os

# Store original modules to restore later
_original_modules = {}


# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone.moctl import MoCtlManager, MoCtlError
from matrixone.client import Client


class TestMoCtlManager(unittest.TestCase):
    """Test MoCtlManager functionality"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        pass

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        pass

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client.execute = Mock()
        self.moctl_manager = MoCtlManager(self.mock_client)

    def test_init(self):
        """Test MoCtlManager initialization"""
        self.assertEqual(self.moctl_manager.client, self.mock_client)

    def test_execute_moctl_success(self):
        """Test successful mo_ctl execution"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Flush", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result

        result = self.moctl_manager._execute_moctl('dn', 'flush', 'db1.t')

        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('dn', 'flush', 'db1.t')"
        self.mock_client.execute.assert_called_once_with(expected_sql)

        # Verify the result was parsed correctly
        self.assertEqual(result['method'], 'Flush')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')

    def test_execute_moctl_no_params(self):
        """Test mo_ctl execution without parameters"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Checkpoint", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result

        result = self.moctl_manager._execute_moctl('dn', 'checkpoint', '')

        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('dn', 'checkpoint', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)

        # Verify the result was parsed correctly
        self.assertEqual(result['method'], 'Checkpoint')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')

    def test_execute_moctl_failure(self):
        """Test mo_ctl execution failure"""
        # Mock failure result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Flush", "result": [{"returnStr": "ERROR: Table not found"}]}',)]
        self.mock_client.execute.return_value = mock_result

        # Should raise MoCtlError
        with self.assertRaises(MoCtlError) as context:
            self.moctl_manager._execute_moctl('dn', 'flush', 'db1.nonexistent')

        self.assertIn("mo_ctl operation failed", str(context.exception))

    def test_execute_moctl_no_rows(self):
        """Test mo_ctl execution with no rows returned"""
        # Mock empty result
        mock_result = Mock()
        mock_result.rows = []
        self.mock_client.execute.return_value = mock_result

        # Should raise MoCtlError
        with self.assertRaises(MoCtlError) as context:
            self.moctl_manager._execute_moctl('dn', 'flush', 'db1.t')

        self.assertIn("mo_ctl command returned no results", str(context.exception))

    def test_execute_moctl_json_error(self):
        """Test mo_ctl execution with invalid JSON"""
        # Mock invalid JSON result
        mock_result = Mock()
        mock_result.rows = [('invalid json',)]
        self.mock_client.execute.return_value = mock_result

        # Should raise MoCtlError
        with self.assertRaises(MoCtlError) as context:
            self.moctl_manager._execute_moctl('dn', 'flush', 'db1.t')

        self.assertIn("Failed to parse mo_ctl result", str(context.exception))

    def test_execute_moctl_exception(self):
        """Test mo_ctl execution with exception"""
        # Mock exception
        self.mock_client.execute.side_effect = Exception("Connection failed")

        # Should raise MoCtlError
        with self.assertRaises(MoCtlError) as context:
            self.moctl_manager._execute_moctl('dn', 'flush', 'db1.t')

        self.assertIn("mo_ctl operation failed", str(context.exception))

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


class TestMoCtlIntegration(unittest.TestCase):
    """Test MoCtlManager integration with Client"""

    @classmethod
    def setUpClass(cls):
        """Setup mocks for the entire test class"""
        pass

    @classmethod
    def tearDownClass(cls):
        """Restore original modules after tests"""
        pass

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
