"""
Test MoCtlManager core functionality (flush_table and checkpoint only)
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
        
        # Test both operations
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
