"""
Test MoCtlManager functionality
"""

import unittest
from unittest.mock import Mock, patch
import json
import sys
import os

# Mock the external dependencies
sys.modules['pymysql'] = Mock()
sys.modules['sqlalchemy'] = Mock()
sys.modules['sqlalchemy.engine'] = Mock()
sys.modules['sqlalchemy.engine'].Engine = Mock()
sys.modules['sqlalchemy.orm'] = Mock()
sys.modules['sqlalchemy.orm'].sessionmaker = Mock()
sys.modules['sqlalchemy.orm'].declarative_base = Mock()
sys.modules['sqlalchemy'] = Mock()
sys.modules['sqlalchemy'].create_engine = Mock()
sys.modules['sqlalchemy'].text = Mock()
sys.modules['sqlalchemy'].Column = Mock()
sys.modules['sqlalchemy'].Integer = Mock()
sys.modules['sqlalchemy'].String = Mock()
sys.modules['sqlalchemy'].DateTime = Mock()

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone.moctl import MoCtlManager, MoCtlError
from matrixone.client import Client


class TestMoCtlManager(unittest.TestCase):
    """Test MoCtlManager functionality"""
    
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
        
        with self.assertRaises(MoCtlError) as context:
            self.moctl_manager._execute_moctl('dn', 'flush', 'db1.nonexistent')
        
        self.assertIn("ERROR: Table not found", str(context.exception))
    
    def test_execute_moctl_no_results(self):
        """Test mo_ctl execution with no results"""
        # Mock empty result
        mock_result = Mock()
        mock_result.rows = []
        self.mock_client.execute.return_value = mock_result
        
        with self.assertRaises(MoCtlError) as context:
            self.moctl_manager._execute_moctl('dn', 'flush', 'db1.t')
        
        self.assertIn("mo_ctl command returned no results", str(context.exception))
    
    def test_execute_moctl_json_error(self):
        """Test mo_ctl execution with invalid JSON"""
        # Mock invalid JSON result
        mock_result = Mock()
        mock_result.rows = [('invalid json',)]
        self.mock_client.execute.return_value = mock_result
        
        with self.assertRaises(MoCtlError) as context:
            self.moctl_manager._execute_moctl('dn', 'flush', 'db1.t')
        
        self.assertIn("Failed to parse mo_ctl result", str(context.exception))
    
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
    
    
    def test_checkpoint(self):
        """Test checkpoint method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Checkpoint", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.checkpoint()
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('dn', 'checkpoint', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Checkpoint')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_incremental_checkpoint(self):
        """Test incremental_checkpoint method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Checkpoint", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.incremental_checkpoint()
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('dn', 'checkpoint', 'incremental')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Checkpoint')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_full_checkpoint(self):
        """Test full_checkpoint method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Checkpoint", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.full_checkpoint()
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('dn', 'checkpoint', 'full')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Checkpoint')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_log_level(self):
        """Test log_level method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Log", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.log_level('debug')
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('log', 'level', 'debug')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Log')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_log_enable(self):
        """Test log_enable method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Log", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.log_enable('sql')
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('log', 'enable', 'sql')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Log')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_log_disable(self):
        """Test log_disable method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Log", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.log_disable('sql')
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('log', 'disable', 'sql')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Log')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_cluster_status(self):
        """Test cluster_status method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Cluster", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.cluster_status()
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('cluster', 'status', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Cluster')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_cluster_info(self):
        """Test cluster_info method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Cluster", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.cluster_info()
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('cluster', 'info', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Cluster')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_node_status(self):
        """Test node_status method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Node", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.node_status('node1')
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('node', 'status', 'node1')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Node')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_node_status_all(self):
        """Test node_status method without node_id"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Node", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.node_status()
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('node', 'status', '')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Node')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_node_info(self):
        """Test node_info method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Node", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.node_info('node1')
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('node', 'info', 'node1')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Node')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_custom_ctl(self):
        """Test custom_ctl method"""
        # Mock successful result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Custom", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.custom_ctl('custom', 'operation', 'param1,param2')
        
        # Verify the SQL was called correctly
        expected_sql = "SELECT mo_ctl('custom', 'operation', 'param1,param2')"
        self.mock_client.execute.assert_called_once_with(expected_sql)
        
        # Verify the result
        self.assertEqual(result['method'], 'Custom')
        self.assertEqual(result['result'][0]['returnStr'], 'OK')
    
    def test_is_available_success(self):
        """Test is_available method when mo_ctl is available"""
        # Mock successful cluster_status result
        mock_result = Mock()
        mock_result.rows = [('{"method": "Cluster", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        result = self.moctl_manager.is_available()
        
        self.assertTrue(result)
    
    def test_is_available_failure(self):
        """Test is_available method when mo_ctl is not available"""
        # Mock failure
        self.mock_client.execute.side_effect = Exception("Connection failed")
        
        result = self.moctl_manager.is_available()
        
        self.assertFalse(result)
    
    def test_get_supported_operations(self):
        """Test get_supported_operations method"""
        operations = self.moctl_manager.get_supported_operations()
        
        self.assertIn("flush", operations)
        self.assertIn("checkpoint", operations)
        self.assertIn("log", operations)
        self.assertIn("cluster", operations)
        self.assertIn("node", operations)
        self.assertIn("custom", operations)
        
        self.assertIn("flush_table", operations["flush"])
        self.assertIn("flush_database", operations["flush"])
        self.assertIn("checkpoint", operations["checkpoint"])
        self.assertIn("log_level", operations["log"])
        self.assertIn("cluster_status", operations["cluster"])
        self.assertIn("node_status", operations["node"])
        self.assertIn("custom_ctl", operations["custom"])


class TestMoCtlIntegration(unittest.TestCase):
    """Test MoCtlManager integration with Client"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client._connection = Mock()
        self.mock_client._connection_params = {
            'user': 'root',
            'password': '111',
            'host': 'localhost',
            'port': 6001,
            'database': 'test'
        }
        self.mock_client.execute = Mock()
        self.mock_client._snapshots = Mock()
        self.mock_client._clone = Mock()
        self.mock_client._moctl = Mock()
    
    def test_client_moctl_property(self):
        """Test Client.moctl property"""
        # Create a real Client instance with mocked dependencies
        client = Client()
        client._connection = self.mock_client._connection
        client._connection_params = self.mock_client._connection_params
        client._snapshots = self.mock_client._snapshots
        client._clone = self.mock_client._clone
        client._moctl = self.mock_client._moctl
        
        # Test that moctl property returns the manager
        self.assertEqual(client.moctl, self.mock_client._moctl)
    
    def test_moctl_operations_chain(self):
        """Test chaining mo_ctl operations"""
        # Mock successful results
        mock_result = Mock()
        mock_result.rows = [('{"method": "Flush", "result": [{"returnStr": "OK"}]}',)]
        self.mock_client.execute.return_value = mock_result
        
        # Create MoCtlManager
        moctl_manager = MoCtlManager(self.mock_client)
        
        # Test multiple operations
        result1 = moctl_manager.flush_table('db1', 'users')
        result2 = moctl_manager.checkpoint()
        
        # Verify both operations were called
        self.assertEqual(self.mock_client.execute.call_count, 2)
        self.assertEqual(result1['method'], 'Flush')
        self.assertEqual(result2['method'], 'Flush')  # Same mock result


if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases using TestLoader
    loader = unittest.TestLoader()
    test_suite.addTests(loader.loadTestsFromTestCase(TestMoCtlManager))
    test_suite.addTests(loader.loadTestsFromTestCase(TestMoCtlIntegration))
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    if result.testsRun > 0:
        success_rate = ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100)
        print(f"Success rate: {success_rate:.1f}%")
    print(f"{'='*50}")
    
    # Exit with appropriate code
    if result.failures or result.errors:
        sys.exit(1)
    else:
        sys.exit(0)
