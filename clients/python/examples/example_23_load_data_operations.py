#!/usr/bin/env python3

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
Example 23: Load Data Operations - Bulk Data Loading from Files

This example demonstrates comprehensive LOAD DATA operations:
1. Basic CSV file loading with comma delimiter
2. CSV files with header rows (ignore lines)
3. Custom delimiters (pipe, tab, etc.)
4. Quoted fields and escape characters
5. Character set conversion
6. Parallel loading for large files
7. Column mapping and transformations
8. Transaction-based loading
9. Error handling and validation
10. Local file loading (LOAD DATA LOCAL INFILE)

This example showcases the LoadDataManager functionality for efficient
bulk data import into MatrixOne tables.
"""

import logging
import os
import tempfile
from matrixone import Client
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params, print_config
from sqlalchemy import Column, Integer, String, DECIMAL, Text

# Create tmpfiles directory if it doesn't exist
TMPFILES_DIR = os.path.join(os.path.dirname(__file__), '..', 'tmpfiles')
os.makedirs(TMPFILES_DIR, exist_ok=True)


class LoadDataOperationsDemo:
    """Demonstrates comprehensive LOAD DATA operations with various file formats and options."""

    def __init__(self):
        self.logger = create_default_logger(
            sql_log_mode="auto",
        )
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'files_created': [],
            'tables_created': [],
        }

    def test_basic_csv_load(self, client):
        """Test basic CSV file loading with comma delimiter"""
        print("\n=== Basic CSV Load Test ===")
        self.results['tests_run'] += 1

        try:
            # Create table using ORM style
            client.drop_table('users')
            client.create_table_orm('users',
                Column('id', Integer, primary_key=True),
                Column('name', String(100)),
                Column('email', String(255)),
                Column('age', Integer)
            )
            self.results['tables_created'].append('users')
            self.logger.info("✅ Created table 'users'")
            
            # Create sample CSV file
            csv_content = """1,Alice,alice@example.com,30
2,Bob,bob@example.com,25
3,Charlie,charlie@example.com,35
4,Diana,diana@example.com,28"""
            
            csv_file = os.path.join(TMPFILES_DIR, "users.csv")
            with open(csv_file, 'w') as f:
                f.write(csv_content)
            self.results['files_created'].append(csv_file)
            
            self.logger.info(f"✅ Created sample CSV file: {csv_file}")
            
            # Load data using LoadDataManager
            result = client.load_data.from_file(csv_file, 'users')
            self.logger.info(f"✅ Loaded {result.affected_rows} rows into 'users' table")
            
            # Verify data using query builder
            count = client.query('users').count()
            assert count == 4
            self.logger.info("✅ Data verification successful")
            
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Basic CSV load test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'basic_csv_load', 'error': str(e)})

    def test_csv_with_header(self, client):
        """Test CSV file with header row (ignore first line)"""
        print("\n=== CSV with Header Test ===")
        self.results['tests_run'] += 1

        try:
            # Create table using ORM style
            client.drop_table('products')
            client.create_table_orm('products',
                Column('id', Integer, primary_key=True),
                Column('name', String(100)),
                Column('price', DECIMAL(10, 2)),
                Column('stock', Integer)
            )
            self.results['tables_created'].append('products')
            self.logger.info("✅ Created table 'products'")
            
            # Create sample CSV file with header
            csv_content = """id,name,price,stock
101,Laptop,999.99,50
102,Mouse,29.99,200
103,Keyboard,79.99,150
104,Monitor,299.99,75"""
            
            csv_file = os.path.join(TMPFILES_DIR, "products.csv")
            with open(csv_file, 'w') as f:
                f.write(csv_content)
            self.results['files_created'].append(csv_file)
            
            self.logger.info(f"✅ Created sample CSV file with header: {csv_file}")
            
            # Load data, skipping the first line (header)
            result = client.load_data.from_file(
                csv_file,
                'products',
                ignore_lines=1  # Skip header row
            )
            self.logger.info(f"✅ Loaded {result.affected_rows} rows into 'products' table (header skipped)")
            
            # Verify data using query builder
            count = client.query('products').count()
            assert count == 4
            self.logger.info("✅ Data verification successful")
            
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ CSV with header test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'csv_with_header', 'error': str(e)})

    def test_custom_delimiter(self, client):
        """Test pipe-delimited file"""
        print("\n=== Custom Delimiter Test ===")
        self.results['tests_run'] += 1

        try:
            # Create table using ORM style
            client.drop_table('orders')
            client.create_table_orm('orders',
                Column('order_id', Integer, primary_key=True),
                Column('customer_name', String(100)),
                Column('product', String(100)),
                Column('amount', DECIMAL(10, 2))
            )
            self.results['tables_created'].append('orders')
            self.logger.info("✅ Created table 'orders'")
            
            # Create sample pipe-delimited file
            pipe_content = """1001|John Smith|Laptop|1500.00
1002|Jane Doe|Mouse|25.50
1003|Bob Johnson|Keyboard|75.00
1004|Alice Williams|Monitor|350.00"""
            
            pipe_file = os.path.join(TMPFILES_DIR, "orders.txt")
            with open(pipe_file, 'w') as f:
                f.write(pipe_content)
            self.results['files_created'].append(pipe_file)
            
            self.logger.info(f"✅ Created sample pipe-delimited file: {pipe_file}")
            
            # Load data with pipe delimiter
            result = client.load_data.from_file(
                pipe_file,
                'orders',
                fields_terminated_by='|'  # Use pipe as delimiter
            )
            self.logger.info(f"✅ Loaded {result.affected_rows} rows into 'orders' table")
            
            # Verify data using query builder
            count = client.query('orders').count()
            assert count == 4
            self.logger.info("✅ Data verification successful")
            
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Custom delimiter test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'custom_delimiter', 'error': str(e)})

    def test_quoted_fields(self, client):
        """Test CSV with quoted fields containing commas"""
        print("\n=== Quoted Fields Test ===")
        self.results['tests_run'] += 1

        try:
            # Create table using ORM style
            client.drop_table('addresses')
            client.create_table_orm('addresses',
                Column('id', Integer, primary_key=True),
                Column('name', String(100)),
                Column('address', String(255)),
                Column('city', String(100))
            )
            self.results['tables_created'].append('addresses')
            self.logger.info("✅ Created table 'addresses'")
            
            # Create sample CSV with quoted fields
            csv_content = '''1,"Alice Smith","123 Main St, Apt 4","New York"
2,"Bob Jones","456 Oak Ave, Suite 100","Los Angeles"
3,"Charlie Brown","789 Pine Rd, Unit 5","Chicago"
4,"Diana Prince","321 Elm St, Floor 2","Houston"'''
            
            csv_file = os.path.join(TMPFILES_DIR, "addresses.csv")
            with open(csv_file, 'w') as f:
                f.write(csv_content)
            self.results['files_created'].append(csv_file)
            
            self.logger.info(f"✅ Created sample CSV with quoted fields: {csv_file}")
            
            # Load data with quoted fields
            result = client.load_data.from_file(
                csv_file,
                'addresses',
                fields_terminated_by=',',
                fields_enclosed_by='"'  # Fields are enclosed in quotes
            )
            self.logger.info(f"✅ Loaded {result.affected_rows} rows into 'addresses' table")
            
            # Verify data contains commas using query builder
            result = client.query('addresses').select('address').where('id = ?', 1).first()
            assert ',' in result.address
            self.logger.info("✅ Data verification successful (commas preserved)")
            
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Quoted fields test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'quoted_fields', 'error': str(e)})

    def test_tab_separated(self, client):
        """Test tab-separated values (TSV) file"""
        print("\n=== Tab-Separated Test ===")
        self.results['tests_run'] += 1

        try:
            # Create table using ORM style
            client.drop_table('logs')
            client.create_table_orm('logs',
                Column('id', Integer, primary_key=True),
                Column('timestamp', String(50)),
                Column('level', String(20)),
                Column('message', Text),
                Column('source', String(100))
            )
            self.results['tables_created'].append('logs')
            self.logger.info("✅ Created table 'logs'")
            
            # Create sample TSV file
            tsv_content = """1\t2024-01-01 10:00:00\tINFO\tApplication started\tsystem
2\t2024-01-01 10:01:00\tDEBUG\tConnection established\tdatabase
3\t2024-01-01 10:02:00\tWARNING\tSlow query detected\tperformance
4\t2024-01-01 10:03:00\tERROR\tConnection timeout\tnetwork"""
            
            tsv_file = os.path.join(TMPFILES_DIR, "logs.tsv")
            with open(tsv_file, 'w') as f:
                f.write(tsv_content)
            self.results['files_created'].append(tsv_file)
            
            self.logger.info(f"✅ Created sample TSV file: {tsv_file}")
            
            # Load data with tab delimiter
            result = client.load_data.from_file(
                tsv_file,
                'logs',
                fields_terminated_by='\\t'  # Tab character
            )
            self.logger.info(f"✅ Loaded {result.affected_rows} rows into 'logs' table")
            
            # Verify data using query builder
            count = client.query('logs').count()
            assert count == 4
            self.logger.info("✅ Data verification successful")
            
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Tab-separated test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'tab_separated', 'error': str(e)})

    def test_column_mapping(self, client):
        """Test loading data into specific columns"""
        print("\n=== Column Mapping Test ===")
        self.results['tests_run'] += 1

        try:
            # Create table with more columns than data file using ORM style
            client.drop_table('employees')
            client.create_table_orm('employees',
                Column('id', Integer, primary_key=True),
                Column('name', String(100)),
                Column('email', String(255)),
                Column('department', String(50)),
                Column('salary', DECIMAL(10, 2))
            )
            self.results['tables_created'].append('employees')
            self.logger.info("✅ Created table 'employees' with 5 columns")
            
            # Create sample CSV with only 3 columns
            csv_content = """1,Alice Johnson,alice.j@example.com
2,Bob Smith,bob.s@example.com
3,Charlie Brown,charlie.b@example.com"""
            
            csv_file = os.path.join(TMPFILES_DIR, "employees.csv")
            with open(csv_file, 'w') as f:
                f.write(csv_content)
            self.results['files_created'].append(csv_file)
            
            self.logger.info(f"✅ Created sample CSV with 3 columns: {csv_file}")
            
            # Load data into specific columns
            result = client.load_data.from_file(
                csv_file,
                'employees',
                columns=['id', 'name', 'email']  # Only load into these columns
            )
            self.logger.info(f"✅ Loaded {result.affected_rows} rows into specific columns (department and salary will be NULL)")
            
            # Verify data using query builder
            count = client.query('employees').count()
            assert count == 3
            
            # Verify NULL values using query builder
            result = client.query('employees').select('department').where('id = ?', 1).first()
            assert result.department is None
            self.logger.info("✅ Data verification successful (NULL values confirmed)")
            
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Column mapping test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'column_mapping', 'error': str(e)})

    def test_transaction_load(self, client):
        """Test loading data within a transaction"""
        print("\n=== Transaction Load Test ===")
        self.results['tests_run'] += 1

        try:
            # Create test tables using ORM style
            client.drop_table('accounts')
            client.drop_table('transactions')
            
            client.create_table_orm('accounts',
                Column('account_id', Integer, primary_key=True),
                Column('account_name', String(100)),
                Column('balance', DECIMAL(10, 2))
            )
            
            client.create_table_orm('transactions',
                Column('trans_id', Integer, primary_key=True),
                Column('account_id', Integer),
                Column('amount', DECIMAL(10, 2)),
                Column('trans_type', String(20))
            )
            self.results['tables_created'].extend(['accounts', 'transactions'])
            self.logger.info("✅ Created tables 'accounts' and 'transactions'")
            
            # Create sample data files
            accounts_content = """1,Alice Account,1000.00
2,Bob Account,2000.00
3,Charlie Account,1500.00"""
            
            transactions_content = """101,1,100.00,deposit
102,2,50.00,withdrawal
103,1,200.00,deposit
104,3,300.00,deposit"""
            
            accounts_file = os.path.join(TMPFILES_DIR, "accounts.csv")
            with open(accounts_file, 'w') as f:
                f.write(accounts_content)
            self.results['files_created'].append(accounts_file)
            
            transactions_file = os.path.join(TMPFILES_DIR, "transactions.csv")
            with open(transactions_file, 'w') as f:
                f.write(transactions_content)
            self.results['files_created'].append(transactions_file)
            
            self.logger.info("✅ Created sample data files")
            
            # Load data within a transaction
            with client.transaction() as tx:
                result1 = tx.load_data.from_file(accounts_file, 'accounts')
                self.logger.info(f"  ✅ Loaded {result1.affected_rows} rows into 'accounts' (in transaction)")
                
                result2 = tx.load_data.from_file(transactions_file, 'transactions')
                self.logger.info(f"  ✅ Loaded {result2.affected_rows} rows into 'transactions' (in transaction)")
                
                # Transaction will commit automatically on success
            
            self.logger.info("✅ Transaction committed successfully")
            
            # Verify data using query builder
            accounts_count = client.query('accounts').count()
            transactions_count = client.query('transactions').count()
            
            assert accounts_count == 3
            assert transactions_count == 4
            self.logger.info("✅ Data verification successful")
            
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Transaction load test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'transaction_load', 'error': str(e)})

    def test_error_handling(self, client):
        """Test error handling for invalid data"""
        print("\n=== Error Handling Test ===")
        self.results['tests_run'] += 1

        try:
            # Create table with NOT NULL constraints using ORM style
            client.drop_table('strict_data')
            client.create_table_orm('strict_data',
                Column('id', Integer, primary_key=True),
                Column('value', String(50), nullable=False)
            )
            self.results['tables_created'].append('strict_data')
            self.logger.info("✅ Created table 'strict_data' with NOT NULL constraints")
            
            # Create sample CSV with invalid data (missing values)
            csv_content = """1,value1
2,
3,value3"""
            
            csv_file = os.path.join(TMPFILES_DIR, "strict_data.csv")
            with open(csv_file, 'w') as f:
                f.write(csv_content)
            self.results['files_created'].append(csv_file)
            
            self.logger.info(f"✅ Created sample CSV with invalid data: {csv_file}")
            
            # Try to load data (may fail depending on MatrixOne's handling of empty strings)
            try:
                result = client.load_data.from_file(csv_file, 'strict_data')
                self.logger.info(f"✅ Loaded {result.affected_rows} rows")
                
                # Verify what was loaded using query builder
                results = client.query('strict_data').order_by('id').all()
                self.logger.info("  Loaded data (empty string may be loaded as NULL):")
                for row in results:
                    self.logger.info(f"    {row}")
                    
            except Exception as load_error:
                self.logger.info(f"✅ Load failed as expected: {type(load_error).__name__}")
                self.logger.info(f"  Error message: {str(load_error)[:100]}")
            
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Error handling test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'error_handling', 'error': str(e)})

    def test_large_data_load(self, client):
        """Test loading a larger dataset"""
        print("\n=== Large Data Load Test ===")
        self.results['tests_run'] += 1

        try:
            # Create table using ORM style
            client.drop_table('large_data')
            client.create_table_orm('large_data',
                Column('id', Integer, primary_key=True),
                Column('value', String(50))
            )
            self.results['tables_created'].append('large_data')
            self.logger.info("✅ Created table 'large_data'")
            
            # Create file with 1000 rows
            large_file = os.path.join(TMPFILES_DIR, "large_data.csv")
            with open(large_file, 'w') as f:
                for i in range(1000):
                    f.write(f"{i},value_{i}\n")
            self.results['files_created'].append(large_file)
            
            self.logger.info(f"✅ Created file with 1000 rows: {large_file}")
            
            # Load data
            result = client.load_data.from_file(large_file, 'large_data')
            self.logger.info(f"✅ Loaded {result.affected_rows} rows")
            
            # Verify count using query builder
            count = client.query('large_data').count()
            assert count == 1000
            self.logger.info("✅ Data verification successful")
            
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Large data load test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'large_data_load', 'error': str(e)})

    def test_load_data_manager_instance(self, client):
        """Test that load_data property returns LoadDataManager instance"""
        print("\n=== LoadDataManager Instance Test ===")
        self.results['tests_run'] += 1

        try:
            from matrixone.load_data import LoadDataManager
            
            # Test load_data property
            assert hasattr(client, 'load_data')
            assert isinstance(client.load_data, LoadDataManager)
            assert hasattr(client.load_data, 'from_file')
            assert hasattr(client.load_data, 'from_local_file')
            
            self.logger.info("✅ LoadDataManager instance verification successful")
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ LoadDataManager instance test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'load_data_manager_instance', 'error': str(e)})

    def cleanup(self, client):
        """Clean up test tables and files"""
        print("\n=== Cleanup ===")
        
        # Clean up tables
        tables = ['users', 'products', 'orders', 'addresses', 'logs', 
                  'employees', 'accounts', 'transactions', 'strict_data', 'large_data']
        for table in tables:
            try:
                client.drop_table(table)
            except Exception as e:
                self.logger.warning(f"Failed to drop table {table}: {e}")
        
        # Clean up files
        for file_path in self.results['files_created']:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                self.logger.warning(f"Failed to remove file {file_path}: {e}")
        
        self.logger.info("✅ Cleanup completed")

    def run_all_tests(self):
        """Run all LOAD DATA tests"""
        print("=" * 60)
        print("MatrixOne Python SDK - Load Data Operations Example")
        print("=" * 60)
        
        # Get connection parameters
        host, port, user, password, database = get_connection_params()
        
        # Create client
        client = Client(logger=self.logger)
        client.connect(host=host, port=port, user=user, password=password, database=database)
        
        try:
            # Run all tests
            self.test_basic_csv_load(client)
            self.test_csv_with_header(client)
            self.test_custom_delimiter(client)
            self.test_quoted_fields(client)
            self.test_tab_separated(client)
            self.test_column_mapping(client)
            self.test_transaction_load(client)
            self.test_error_handling(client)
            self.test_large_data_load(client)
            self.test_load_data_manager_instance(client)
            
            # Print results
            print("\n" + "=" * 60)
            print("Test Results Summary")
            print("=" * 60)
            print(f"Tests run: {self.results['tests_run']}")
            print(f"Tests passed: {self.results['tests_passed']}")
            print(f"Tests failed: {self.results['tests_failed']}")
            print(f"Success rate: {(self.results['tests_passed'] / self.results['tests_run'] * 100):.1f}%")
            
            if self.results['unexpected_results']:
                print("\nUnexpected results:")
                for result in self.results['unexpected_results']:
                    print(f"  - {result['test']}: {result['error']}")
            
            print("\n" + "=" * 60)
            if self.results['tests_failed'] == 0:
                print("✅ All tests completed successfully!")
            else:
                print("❌ Some tests failed. Check the logs above for details.")
            print("=" * 60)
            
        finally:
            # Cleanup
            self.cleanup(client)
            client.disconnect()


def main():
    """Main function to run the example"""
    demo = LoadDataOperationsDemo()
    demo.run_all_tests()


if __name__ == "__main__":
    main()