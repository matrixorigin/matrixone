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
Online tests for LOAD DATA operations

These tests are inspired by example_23_load_data_operations.py
"""

import pytest
import os
import tempfile
from matrixone import Client
from matrixone.load_data import LoadDataManager
from sqlalchemy import Column, Integer, String, DECIMAL, Text

# Create tmpfiles directory if it doesn't exist
TMPFILES_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'tmpfiles')
os.makedirs(TMPFILES_DIR, exist_ok=True)


@pytest.mark.online
class TestLoadDataOperations:
    """Test LOAD DATA functionality"""

    def test_basic_csv_load(self, test_client):
        """Test basic CSV file loading with comma delimiter"""
        # Create table using ORM style
        test_client.drop_table('test_load_users')
        test_client.create_table_orm('test_load_users',
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            Column('email', String(255)),
            Column('age', Integer)
        )
        
        # Create sample CSV file
        csv_content = """1,Alice,alice@example.com,30
2,Bob,bob@example.com,25
3,Charlie,charlie@example.com,35"""
        
        csv_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.csv")
        with open(csv_file, 'w') as f:
            f.write(csv_content)
        
        try:
            # Load data
            result = test_client.load_data.from_file(csv_file, 'test_load_users')
            assert result.affected_rows == 3
            
            # Verify data using query builder
            count = test_client.query('test_load_users').count()
            assert count == 3
            
        finally:
            os.unlink(csv_file)
            test_client.drop_table('test_load_users')

    def test_csv_with_header(self, test_client):
        """Test CSV file with header row (ignore first line)"""
        # Create table
        test_client.drop_table('test_load_products')
        test_client.create_table_orm('test_load_products',
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            Column('price', DECIMAL(10, 2))
        )
        
        # Create sample CSV with header
        csv_content = """id,name,price
101,Laptop,999.99
102,Mouse,29.99
103,Keyboard,79.99"""
        
        csv_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.csv")
        with open(csv_file, 'w') as f:
            f.write(csv_content)
        
        try:
            # Load data with ignore_lines=1
            result = test_client.load_data.from_file(
                csv_file,
                'test_load_products',
                ignore_lines=1
            )
            assert result.affected_rows == 3
            
            # Verify data using count
            count = test_client.query('test_load_products').count()
            assert count == 3
            
            # Verify first row data using query builder
            result = test_client.query('test_load_products').select('id').order_by('id').first()
            assert result.id == 101
            
        finally:
            os.unlink(csv_file)
            test_client.drop_table('test_load_products')

    def test_custom_delimiter(self, test_client):
        """Test pipe-delimited file"""
        # Create table
        test_client.drop_table('test_load_orders')
        test_client.create_table_orm('test_load_orders',
            Column('order_id', Integer, primary_key=True),
            Column('customer', String(100)),
            Column('amount', DECIMAL(10, 2))
        )
        
        # Create pipe-delimited file
        pipe_content = """1001|John Smith|1500.00
1002|Jane Doe|2500.00"""
        
        pipe_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.txt")
        with open(pipe_file, 'w') as f:
            f.write(pipe_content)
        
        try:
            # Load data with pipe delimiter
            result = test_client.load_data.from_file(
                pipe_file,
                'test_load_orders',
                fields_terminated_by='|'
            )
            assert result.affected_rows == 2
            
            # Verify data
            count = test_client.query('test_load_orders').count()
            assert count == 2
            
        finally:
            os.unlink(pipe_file)
            test_client.drop_table('test_load_orders')

    def test_quoted_fields(self, test_client):
        """Test CSV with quoted fields containing commas"""
        # Create table using ORM style
        test_client.drop_table('test_load_addresses')
        test_client.create_table_orm('test_load_addresses',
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            Column('address', String(255))
        )
        
        # Create CSV with quoted fields
        csv_content = '''1,"Alice Smith","123 Main St, Apt 4"
2,"Bob Jones","456 Oak Ave, Suite 100"'''
        
        csv_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.csv")
        with open(csv_file, 'w') as f:
            f.write(csv_content)
        
        try:
            # Load data with quoted fields
            result = test_client.load_data.from_file(
                csv_file,
                'test_load_addresses',
                fields_terminated_by=',',
                fields_enclosed_by='"'
            )
            assert result.affected_rows == 2
            
            # Verify data contains commas using query builder
            result = test_client.query('test_load_addresses').select('address').where('id = ?', 1).first()
            assert ',' in result.address
            
        finally:
            os.unlink(csv_file)
            test_client.drop_table('test_load_addresses')

    def test_tab_separated(self, test_client):
        """Test tab-separated values (TSV) file"""
        # Create table using ORM style
        test_client.drop_table('test_load_logs')
        test_client.create_table_orm('test_load_logs',
            Column('id', Integer, primary_key=True),
            Column('level', String(20)),
            Column('message', Text)
        )
        
        # Create TSV file
        tsv_content = """1\tINFO\tApplication started
2\tERROR\tConnection failed"""
        
        tsv_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.tsv")
        with open(tsv_file, 'w') as f:
            f.write(tsv_content)
        
        try:
            # Load data with tab delimiter
            result = test_client.load_data.from_file(
                tsv_file,
                'test_load_logs',
                fields_terminated_by='\\t'
            )
            assert result.affected_rows == 2
            
        finally:
            os.unlink(tsv_file)
            test_client.drop_table('test_load_logs')

    def test_column_mapping(self, test_client):
        """Test loading data into specific columns"""
        # Create table with more columns than data file using ORM style
        test_client.drop_table('test_load_employees')
        test_client.create_table_orm('test_load_employees',
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            Column('email', String(255)),
            Column('department', String(50))
        )
        
        # Create CSV with only 3 columns
        csv_content = """1,Alice,alice@example.com
2,Bob,bob@example.com"""
        
        csv_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.csv")
        with open(csv_file, 'w') as f:
            f.write(csv_content)
        
        try:
            # Load data into specific columns
            result = test_client.load_data.from_file(
                csv_file,
                'test_load_employees',
                columns=['id', 'name', 'email']
            )
            assert result.affected_rows == 2
            
            # Verify department is NULL using query builder
            result = test_client.query('test_load_employees').select('department').where('id = ?', 1).first()
            assert result.department is None
            
        finally:
            os.unlink(csv_file)
            test_client.drop_table('test_load_employees')

    def test_transaction_load(self, test_client):
        """Test loading data within a transaction"""
        # Create tables using ORM style
        test_client.drop_table('test_load_accounts')
        test_client.drop_table('test_load_transactions')
        
        test_client.create_table_orm('test_load_accounts',
            Column('account_id', Integer, primary_key=True),
            Column('balance', DECIMAL(10, 2))
        )
        
        test_client.create_table_orm('test_load_transactions',
            Column('trans_id', Integer, primary_key=True),
            Column('account_id', Integer),
            Column('amount', DECIMAL(10, 2))
        )
        
        # Create data files
        accounts_content = """1,1000.00
2,2000.00"""
        transactions_content = """101,1,100.00
102,2,200.00"""
        
        accounts_file = os.path.join(TMPFILES_DIR, f"test_accounts_{id(self)}.csv")
        with open(accounts_file, 'w') as f:
            f.write(accounts_content)
        
        transactions_file = os.path.join(TMPFILES_DIR, f"test_transactions_{id(self)}.csv")
        with open(transactions_file, 'w') as f:
            f.write(transactions_content)
        
        try:
            # Load data within transaction
            with test_client.transaction() as tx:
                result1 = tx.load_data.from_file(accounts_file, 'test_load_accounts')
                result2 = tx.load_data.from_file(transactions_file, 'test_load_transactions')
                
                assert result1.affected_rows == 2
                assert result2.affected_rows == 2
            
            # Verify data is committed
            accounts_count = test_client.query('test_load_accounts').count()
            transactions_count = test_client.query('test_load_transactions').count()
            
            assert accounts_count == 2
            assert transactions_count == 2
            
        finally:
            os.unlink(accounts_file)
            os.unlink(transactions_file)
            test_client.drop_table('test_load_accounts')
            test_client.drop_table('test_load_transactions')

    def test_empty_file(self, test_client):
        """Test loading from an empty file"""
        # Create table using ORM style
        test_client.drop_table('test_load_empty')
        test_client.create_table_orm('test_load_empty',
            Column('id', Integer, primary_key=True),
            Column('value', String(50))
        )
        
        # Create empty file
        empty_file = os.path.join(TMPFILES_DIR, f"test_empty_{id(self)}.csv")
        with open(empty_file, 'w') as f:
            # Write nothing
            pass
        
        try:
            # Load data from empty file
            result = test_client.load_data.from_file(empty_file, 'test_load_empty')
            assert result.affected_rows == 0
            
            # Verify table is empty
            count = test_client.query('test_load_empty').count()
            assert count == 0
            
        finally:
            os.unlink(empty_file)
            test_client.drop_table('test_load_empty')

    def test_large_data_load(self, test_client):
        """Test loading a larger dataset"""
        # Create table using ORM style
        test_client.drop_table('test_load_large')
        test_client.create_table_orm('test_load_large',
            Column('id', Integer, primary_key=True),
            Column('value', String(50))
        )
        
        # Create file with 1000 rows
        large_file = os.path.join(TMPFILES_DIR, f"test_large_{id(self)}.csv")
        with open(large_file, 'w') as f:
            for i in range(1000):
                f.write(f"{i},value_{i}\n")
        
        try:
            # Load data
            result = test_client.load_data.from_file(large_file, 'test_load_large')
            assert result.affected_rows == 1000
            
            # Verify count
            count = test_client.query('test_load_large').count()
            assert count == 1000
            
        finally:
            os.unlink(large_file)
            test_client.drop_table('test_load_large')

    def test_load_data_manager_instance(self, test_client):
        """Test that load_data property returns LoadDataManager instance"""
        assert hasattr(test_client, 'load_data')
        assert isinstance(test_client.load_data, LoadDataManager)
        assert hasattr(test_client.load_data, 'from_file')
        assert hasattr(test_client.load_data, 'from_local_file')


@pytest.mark.online
class TestLoadDataErrorHandling:
    """Test error handling in LOAD DATA operations"""

    def test_nonexistent_file(self, test_client):
        """Test loading from a non-existent file"""
        test_client.drop_table('test_load_error')
        test_client.create_table_orm('test_load_error',
            Column('id', Integer, primary_key=True)
        )
        
        try:
            with pytest.raises(Exception):
                test_client.load_data.from_file('/nonexistent/file.csv', 'test_load_error')
        finally:
            test_client.drop_table('test_load_error')

    def test_invalid_table_name(self, test_client):
        """Test loading into a non-existent table"""
        csv_file = os.path.join(TMPFILES_DIR, f"test_error_{id(self)}.csv")
        with open(csv_file, 'w') as f:
            f.write("1,value")
        
        try:
            with pytest.raises(Exception):
                test_client.load_data.from_file(csv_file, 'nonexistent_table')
        finally:
            os.unlink(csv_file)

    def test_invalid_parameters(self, test_client):
        """Test load_data with invalid parameters"""
        # Test empty file path
        with pytest.raises(ValueError):
            test_client.load_data.from_file('', 'test_table')
        
        # Test negative ignore_lines
        with pytest.raises(ValueError):
            test_client.load_data.from_file('/tmp/test.csv', 'test_table', ignore_lines=-1)

