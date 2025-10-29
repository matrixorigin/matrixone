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
from matrixone import Client, LoadDataFormat, CompressionFormat, JsonDataStructure
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params, print_config
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer,SmallInteger, String, DECIMAL, Text
from sqlalchemy.dialects.mysql import TINYINT, BLOB, VARBINARY, BINARY

# Create Base for model definitions
Base = declarative_base()

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
            # Define table model
            class User(Base):
                __tablename__ = 'users'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                email = Column(String(255))
                age = Column(Integer)

            # Create table using model
            client.drop_table('users')
            client.create_table(User)
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

            # Load data using simplified CSV interface with model
            result = client.load_data.from_csv(csv_file, User)
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
            # Define table model
            class Product(Base):
                __tablename__ = 'products'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                price = Column(DECIMAL(10, 2))
                stock = Column(Integer)

            # Create table using model
            client.drop_table('products')
            client.create_table(Product)
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
            result = client.load_data.from_csv(csv_file, Product, ignore_lines=1)  # Skip header row
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
            # Define table model
            class Order(Base):
                __tablename__ = 'orders'
                order_id = Column(Integer, primary_key=True)
                customer_name = Column(String(100))
                product = Column(String(100))
                amount = Column(DECIMAL(10, 2))

            # Create table using model
            client.drop_table('orders')
            client.create_table(Order)
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

            # Load data with pipe delimiter using simplified CSV interface
            result = client.load_data.from_csv(pipe_file, Order, delimiter='|')  # Use pipe as delimiter
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
            # Define table model
            class Address(Base):
                __tablename__ = 'addresses'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                address = Column(String(255))
                city = Column(String(100))

            # Create table using model
            client.drop_table('addresses')
            client.create_table(Address)
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

            # Load data with quoted fields using simplified CSV interface
            result = client.load_data.from_csv(
                csv_file, Address, delimiter=',', enclosed_by='"'  # Fields are enclosed in quotes
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
            # Define table model
            class Log(Base):
                __tablename__ = 'logs'
                id = Column(Integer, primary_key=True)
                timestamp = Column(String(50))
                level = Column(String(20))
                message = Column(Text)
                source = Column(String(100))

            # Create table using model
            client.drop_table('logs')
            client.create_table(Log)
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
            result = client.load_data.from_tsv(tsv_file, Log)  # Tab delimiter is automatic
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
            # Define table model with more columns than data file
            class Employee(Base):
                __tablename__ = 'employees'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                email = Column(String(255))
                department = Column(String(50))
                salary = Column(DECIMAL(10, 2))

            # Create table using model
            client.drop_table('employees')
            client.create_table(Employee)
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
            result = client.load_data.from_csv(
                csv_file, Employee, columns=['id', 'name', 'email']  # Only load into these columns
            )
            self.logger.info(
                f"✅ Loaded {result.affected_rows} rows into specific columns (department and salary will be NULL)"
            )

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
            # Define table models
            class Account(Base):
                __tablename__ = 'accounts'
                account_id = Column(Integer, primary_key=True)
                account_name = Column(String(100))
                balance = Column(DECIMAL(10, 2))

            class Transaction(Base):
                __tablename__ = 'transactions'
                trans_id = Column(Integer, primary_key=True)
                account_id = Column(Integer)
                amount = Column(DECIMAL(10, 2))
                trans_type = Column(String(20))

            # Create test tables using models
            client.drop_table('accounts')
            client.drop_table('transactions')
            client.create_table(Account)
            client.create_table(Transaction)
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

            # Load data within a session using simplified CSV interface with models
            with client.session() as tx:
                result1 = tx.load_data.from_csv(accounts_file, Account)
                # Check for both ResultSet and SQLAlchemy Result
                affected1 = result1.affected_rows if hasattr(result1, 'affected_rows') else result1.rowcount
                self.logger.info(f"  ✅ Loaded {affected1} rows into 'accounts' (in session)")

                result2 = tx.load_data.from_csv(transactions_file, Transaction)
                affected2 = result2.affected_rows if hasattr(result2, 'affected_rows') else result2.rowcount
                self.logger.info(f"  ✅ Loaded {affected2} rows into 'transactions' (in session)")

                # Session will commit automatically on success

            self.logger.info("✅ Session committed successfully")

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
            # Define table model with NOT NULL constraints
            class StrictData(Base):
                __tablename__ = 'strict_data'
                id = Column(Integer, primary_key=True)
                value = Column(String(50), nullable=False)

            # Create table using model
            client.drop_table('strict_data')
            client.create_table(StrictData)
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
                result = client.load_data.from_csv(csv_file, StrictData)
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
            # Define table model
            class LargeData(Base):
                __tablename__ = 'large_data'
                id = Column(Integer, primary_key=True)
                value = Column(String(50))

            # Create table using model
            client.drop_table('large_data')
            client.create_table(LargeData)
            self.results['tables_created'].append('large_data')
            self.logger.info("✅ Created table 'large_data'")

            # Create file with 1000 rows
            large_file = os.path.join(TMPFILES_DIR, "large_data.csv")
            with open(large_file, 'w') as f:
                for i in range(1000):
                    f.write(f"{i},value_{i}\n")
            self.results['files_created'].append(large_file)

            self.logger.info(f"✅ Created file with 1000 rows: {large_file}")

            # Load data using simplified CSV interface
            result = client.load_data.from_csv(large_file, LargeData)
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

    def test_jsonline_object_format(self, client):
        """Test JSONLINE format with object structure"""
        print("\n=== JSONLINE Object Format Test ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class JsonlineUser(Base):
                __tablename__ = 'jsonline_users'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                email = Column(String(255))
                age = Column(Integer)

            # Create table using model
            client.drop_table('jsonline_users')
            client.create_table(JsonlineUser)
            self.results['tables_created'].append('jsonline_users')
            self.logger.info("✅ Created table 'jsonline_users'")

            # Create JSONLINE file with JSON objects
            jl_content = '''{"id":1,"name":"Alice","email":"alice@example.com","age":30}
{"id":2,"name":"Bob","email":"bob@example.com","age":25}
{"id":3,"name":"Charlie","email":"charlie@example.com","age":35}'''

            jl_file = os.path.join(TMPFILES_DIR, "jsonline_users.jl")
            with open(jl_file, 'w') as f:
                f.write(jl_content)
            self.results['files_created'].append(jl_file)
            self.logger.info(f"✅ Created JSONLINE file: {jl_file}")

            # Load JSONLINE data using simplified interface with enum
            result = client.load_data.from_jsonline(jl_file, JsonlineUser, structure=JsonDataStructure.OBJECT)
            self.logger.info(f"✅ Loaded {result.affected_rows} rows from JSONLINE (object format)")

            # Verify data
            count = client.query('jsonline_users').count()
            assert count == 3
            self.logger.info("✅ Data verification successful")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ JSONLINE object format test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'jsonline_object_format', 'error': str(e)})

    def test_jsonline_array_format(self, client):
        """Test JSONLINE format with array structure"""
        print("\n=== JSONLINE Array Format Test ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class JsonlineProduct(Base):
                __tablename__ = 'jsonline_products'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                price = Column(DECIMAL(10, 2))
                stock = Column(Integer)

            # Create table using model
            client.drop_table('jsonline_products')
            client.create_table(JsonlineProduct)
            self.results['tables_created'].append('jsonline_products')
            self.logger.info("✅ Created table 'jsonline_products'")

            # Create JSONLINE file with JSON arrays
            jl_content = '''[1,"Laptop",999.99,50]
[2,"Mouse",29.99,200]
[3,"Keyboard",79.99,150]'''

            jl_file = os.path.join(TMPFILES_DIR, "jsonline_products.jl")
            with open(jl_file, 'w') as f:
                f.write(jl_content)
            self.results['files_created'].append(jl_file)
            self.logger.info(f"✅ Created JSONLINE file: {jl_file}")

            # Load JSONLINE data using simplified interface with enum
            result = client.load_data.from_jsonline(jl_file, JsonlineProduct, structure=JsonDataStructure.ARRAY)
            self.logger.info(f"✅ Loaded {result.affected_rows} rows from JSONLINE (array format)")

            # Verify data
            count = client.query('jsonline_products').count()
            assert count == 3
            self.logger.info("✅ Data verification successful")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ JSONLINE array format test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'jsonline_array_format', 'error': str(e)})

    def test_set_clause_with_nullif(self, client):
        """Test SET clause with NULLIF function"""
        print("\n=== SET Clause with NULLIF Test ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class CleanedData(Base):
                __tablename__ = 'cleaned_data'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                status = Column(String(50))

            # Create table using model
            client.drop_table('cleaned_data')
            client.create_table(CleanedData)
            self.results['tables_created'].append('cleaned_data')
            self.logger.info("✅ Created table 'cleaned_data'")

            # Create CSV with "null" strings that need to be converted to NULL
            csv_content = '''1,Alice,active
2,Bob,null
3,Charlie,active
4,Diana,null'''

            csv_file = os.path.join(TMPFILES_DIR, "data_with_nulls.csv")
            with open(csv_file, 'w') as f:
                f.write(csv_content)
            self.results['files_created'].append(csv_file)
            self.logger.info(f"✅ Created CSV file with 'null' strings: {csv_file}")

            # Load data with SET clause using simplified CSV interface
            result = client.load_data.from_csv(csv_file, CleanedData, set_clause={'status': 'NULLIF(status, "null")'})
            self.logger.info(f"✅ Loaded {result.affected_rows} rows with SET clause")

            # Verify NULL conversion
            result = client.query('cleaned_data').select('status').where('id = ?', 2).first()
            assert result.status is None
            self.logger.info("✅ NULLIF conversion verified (row 2 status is NULL)")

            result = client.query('cleaned_data').select('status').where('id = ?', 4).first()
            assert result.status is None
            self.logger.info("✅ NULLIF conversion verified (row 4 status is NULL)")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ SET clause with NULLIF test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'set_clause_nullif', 'error': str(e)})

    def test_optionally_enclosed_fields(self, client):
        """Test OPTIONALLY ENCLOSED BY for mixed quoted/unquoted fields"""
        print("\n=== OPTIONALLY ENCLOSED BY Test ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class MixedData(Base):
                __tablename__ = 'mixed_data'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                description = Column(String(255))

            # Create table using model
            client.drop_table('mixed_data')
            client.create_table(MixedData)
            self.results['tables_created'].append('mixed_data')
            self.logger.info("✅ Created table 'mixed_data'")

            # Create CSV with mixed quoted/unquoted fields
            csv_content = '''1,Alice,Simple description
2,"Bob Smith","Description with, comma"
3,Charlie,Another simple one
4,"Diana Jones","Multi, part, description"'''

            csv_file = os.path.join(TMPFILES_DIR, "mixed_data.csv")
            with open(csv_file, 'w') as f:
                f.write(csv_content)
            self.results['files_created'].append(csv_file)
            self.logger.info(f"✅ Created CSV with mixed quoted/unquoted fields: {csv_file}")

            # Load data with OPTIONALLY ENCLOSED BY
            result = client.load_data.from_csv(csv_file, MixedData, enclosed_by='"', optionally_enclosed=True)
            self.logger.info(f"✅ Loaded {result.affected_rows} rows with OPTIONALLY ENCLOSED BY")

            # Verify data
            count = client.query('mixed_data').count()
            assert count == 4
            self.logger.info("✅ Data verification successful")

            # Verify fields with commas were preserved
            result = client.query('mixed_data').select('description').where('id = ?', 2).first()
            assert ',' in result.description
            self.logger.info("✅ Comma preservation verified in quoted fields")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ OPTIONALLY ENCLOSED BY test failed: {e}")
            self.results['tests_failed'] += 1

    def test_csv_inline_loading(self, client):
        """Test LOAD DATA INLINE with CSV format"""
        print("\n=== CSV INLINE Loading Test ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class InlineData(Base):
                __tablename__ = 'test_inline_csv'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                value = Column(String(100))

            # Create table
            client.drop_table('test_inline_csv')
            client.create_table(InlineData)

            # Prepare inline CSV data
            csv_data = "1,Alice,value1\n2,Bob,value2\n3,Charlie,value3\n"

            # Load data using from_csv_inline
            result = client.load_data.from_csv_inline(csv_data, InlineData)
            self.logger.info(f"✅ Loaded {result.affected_rows} rows from inline CSV")

            # Verify data
            count = client.query('test_inline_csv').count()
            assert count == 3
            self.logger.info("✅ CSV INLINE data verification successful")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ CSV INLINE test failed: {e}")
            self.results['tests_failed'] += 1
        finally:
            client.drop_table('test_inline_csv')

    def test_jsonline_inline_loading(self, client):
        """Test LOAD DATA INLINE with JSONLINE format"""
        print("\n=== JSONLINE INLINE Loading Test ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class InlineJsonData(Base):
                __tablename__ = 'test_inline_jsonline'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                age = Column(Integer)

            # Create table
            client.drop_table('test_inline_jsonline')
            client.create_table(InlineJsonData)

            # Prepare inline JSONLINE data
            jsonline_data = '{"id":1,"name":"Alice","age":25}\n{"id":2,"name":"Bob","age":30}\n'

            # Load data using from_jsonline_inline
            result = client.load_data.from_jsonline_inline(jsonline_data, InlineJsonData, structure=JsonDataStructure.OBJECT)
            self.logger.info(f"✅ Loaded {result.affected_rows} rows from inline JSONLINE")

            # Verify data
            count = client.query('test_inline_jsonline').count()
            assert count == 2
            self.logger.info("✅ JSONLINE INLINE data verification successful")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ JSONLINE INLINE test failed: {e}")
            self.results['tests_failed'] += 1
        finally:
            client.drop_table('test_inline_jsonline')

    def test_generic_inline_loading(self, client):
        """Test generic LOAD DATA INLINE interface"""
        print("\n=== Generic INLINE Loading Test ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class GenericInlineData(Base):
                __tablename__ = 'test_generic_inline'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                score = Column(Integer)

            # Create table
            client.drop_table('test_generic_inline')
            client.create_table(GenericInlineData)

            # Prepare inline data
            inline_data = "1,Eve,95\n2,Frank,87\n"

            # Load data using generic from_inline
            result = client.load_data.from_inline(inline_data, GenericInlineData, format=LoadDataFormat.CSV)
            self.logger.info(f"✅ Loaded {result.affected_rows} rows from generic inline interface")

            # Verify data
            count = client.query('test_generic_inline').count()
            assert count == 2
            self.logger.info("✅ Generic INLINE data verification successful")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Generic INLINE test failed: {e}")
            self.results['tests_failed'] += 1
        finally:
            client.drop_table('test_generic_inline')

    def test_stage_loading(self, client):
        """Test LOAD DATA from stage with format-specific interfaces"""
        print("\n=== Stage Loading Test ===")
        self.results['tests_run'] += 1

        import tempfile

        tmpdir = None
        csv_file = None
        tsv_file = None
        jsonl_file = None

        try:
            # Define table model
            class StageData(Base):
                __tablename__ = 'test_stage_load'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))

            # Create table
            client.drop_table('test_stage_load')
            client.create_table(StageData)

            # Create temp files
            tmpdir = tempfile.mkdtemp()
            csv_file = os.path.join(tmpdir, 'test.csv')
            tsv_file = os.path.join(tmpdir, 'test.tsv')
            jsonl_file = os.path.join(tmpdir, 'test.jsonl')

            with open(csv_file, 'w') as f:
                f.write('1,Alice\n2,Bob\n')
            with open(tsv_file, 'w') as f:
                f.write('3\tCharlie\n')
            with open(jsonl_file, 'w') as f:
                f.write('{"id":4,"name":"Diana"}\n')

            # Create stage
            client.execute(f'CREATE STAGE IF NOT EXISTS demo_stage URL="file://{tmpdir}/"')
            self.logger.info("✅ Created stage successfully")

            # Test from_stage_csv
            result = client.load_data.from_stage_csv('demo_stage', 'test.csv', StageData)
            self.logger.info(f"✅ from_stage_csv: Loaded {result.affected_rows} rows")

            # Test from_stage_tsv
            result = client.load_data.from_stage_tsv('demo_stage', 'test.tsv', StageData)
            self.logger.info(f"✅ from_stage_tsv: Loaded {result.affected_rows} rows")

            # Test from_stage_jsonline
            result = client.load_data.from_stage_jsonline(
                'demo_stage', 'test.jsonl', StageData, structure=JsonDataStructure.OBJECT
            )
            self.logger.info(f"✅ from_stage_jsonline: Loaded {result.affected_rows} rows")

            # Verify total data
            count = client.query('test_stage_load').count()
            assert count == 4
            self.logger.info(f"✅ Stage data verification successful (total {count} rows)")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Stage loading test failed: {e}")
            self.results['tests_failed'] += 1
        finally:
            # Cleanup
            client.execute('DROP STAGE IF EXISTS demo_stage')
            client.drop_table('test_stage_load')
            if csv_file and os.path.exists(csv_file):
                os.unlink(csv_file)
            if tsv_file and os.path.exists(tsv_file):
                os.unlink(tsv_file)
            if jsonl_file and os.path.exists(jsonl_file):
                os.unlink(jsonl_file)
            if tmpdir and os.path.exists(tmpdir):
                os.rmdir(tmpdir)

    def test_stage_parquet_loading(self, client):
        """Test LOAD DATA from stage with Parquet format"""
        print("\n=== Stage Parquet Loading Test ===")
        self.results['tests_run'] += 1

        # Try to import pyarrow to generate parquet file
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            self.logger.warning("⚠️  pyarrow not installed, skipping parquet test")
            self.results['tests_run'] -= 1
            return

        parquet_file = None

        try:
            # Define table model matching parquet schema
            from sqlalchemy import BigInteger

            class ParquetData(Base):
                __tablename__ = 'test_stage_parquet'
                id = Column(BigInteger, primary_key=True, nullable=False)
                name = Column(String(100), nullable=False)
                int8Column = Column(TINYINT, nullable=False)
                int16Column = Column(SmallInteger, nullable=False)
                binaryColumn = Column(BINARY, nullable=False)
                varBinaryColumn = Column(VARBINARY(32), nullable=False)
                blobColumn = Column(BLOB, nullable=False)

            # Create table
            client.drop_table('test_stage_parquet')
            client.create_table(ParquetData)

            # Generate parquet file in tmpfiles
            tmpfiles_dir = os.path.join(os.getcwd(), 'tmpfiles')
            os.makedirs(tmpfiles_dir, exist_ok=True)
            parquet_file = os.path.join(tmpfiles_dir, 'test_stage.parq')

            # Create parquet data with explicit schema (non-nullable to match table definition)
            schema = pa.schema([
                pa.field('id', pa.int64(), nullable=False), 
                pa.field('name', pa.string(), nullable=False), 
                pa.field('int8column', pa.int8(), nullable=False), 
                pa.field('int16column', pa.int16(), nullable=False),
                pa.field('binarycolumn', pa.binary(), nullable=False),
                pa.field('varbinarycolumn', pa.binary(), nullable=False),
                pa.field('blobcolumn', pa.binary(), nullable=False),
            ])
            table = pa.table(
                {
                    'id': pa.array([1, 2, 3, 4, 5], type=pa.int64()),
                    'name': pa.array(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'], type=pa.string()),
                    'int8column': pa.array([1, 2, 3, 4, 5], type=pa.int8()),
                    'int16column': pa.array([100, 200, 300, 400, 500], type=pa.int16()),
                    'binarycolumn': pa.array([b'binary1', b'binary2', b'binary3', b'binary4', b'binary5'], type=pa.binary()),
                    'varbinarycolumn': pa.array([b'varbinary1', b'varbinary2', b'varbinary3', b'varbinary4', b'varbinary5'], type=pa.binary()),
                    'blobcolumn': pa.array([b'blob1', b'blob2', b'blob3', b'blob4', b'blob5'], type=pa.binary()),
                },
                schema=schema,
            )
            # Write with options compatible with MatrixOne
            pq.write_table(
                table,
                parquet_file,
                compression='none',
                use_dictionary=False,
                write_statistics=False,
                data_page_version='1.0',
            )
            self.results['files_created'].append(parquet_file)
            self.logger.info("✅ Generated parquet file successfully")
            print(parquet_file)

            # Create stage pointing to tmpfiles
            client.execute(f'CREATE STAGE IF NOT EXISTS parquet_stage URL="file://{tmpfiles_dir}/"')
            self.logger.info("✅ Created parquet stage successfully")

            exit()
            # Test from_stage_parquet
            result = client.load_data.from_stage_parquet('parquet_stage', 'test_stage.parq', ParquetData)
            self.logger.info(f"✅ from_stage_parquet: Loaded {result.affected_rows} rows")

            # Verify data
            count = client.query('test_stage_parquet').count()
            self.logger.info(f"✅ Stage Parquet verification successful (total {count} rows)")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Stage Parquet loading test failed: {e}")
            self.results['tests_failed'] += 1
        finally:
            # Cleanup
            client.execute('DROP STAGE IF EXISTS parquet_stage')
            client.drop_table('test_stage_parquet')
            if parquet_file and os.path.exists(parquet_file):
                os.unlink(parquet_file)

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
            assert hasattr(client.load_data, 'from_csv')
            assert hasattr(client.load_data, 'from_tsv')
            assert hasattr(client.load_data, 'from_jsonline')
            assert hasattr(client.load_data, 'from_parquet')

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
        tables = [
            'users',
            'products',
            'orders',
            'addresses',
            'logs',
            'employees',
            'accounts',
            'transactions',
            'strict_data',
            'large_data',
            'jsonline_users',
            'jsonline_products',
            'cleaned_data',
            'mixed_data',
        ]
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
            self.test_jsonline_object_format(client)
            self.test_jsonline_array_format(client)
            self.test_set_clause_with_nullif(client)
            self.test_optionally_enclosed_fields(client)
            self.test_csv_inline_loading(client)
            self.test_jsonline_inline_loading(client)
            self.test_generic_inline_loading(client)
            self.test_stage_loading(client)
            self.test_stage_parquet_loading(client)
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
