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
from matrixone import Client, AsyncClient, LoadDataManager, LoadDataFormat, CompressionFormat, JsonDataStructure
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL, Text

# Create Base for model definitions
Base = declarative_base()

# Create tmpfiles directory if it doesn't exist
TMPFILES_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'tmpfiles')
os.makedirs(TMPFILES_DIR, exist_ok=True)


@pytest.mark.online
class TestLoadDataOperations:
    """Test LOAD DATA functionality"""

    def test_basic_csv_load(self, test_client):
        """Test basic CSV file loading with comma delimiter"""

        # Define table model
        class User(Base):
            __tablename__ = 'test_load_users'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            email = Column(String(255))
            age = Column(Integer)

        # Create table using model
        test_client.drop_table('test_load_users')
        test_client.create_table(User)

        # Create sample CSV file
        csv_content = """1,Alice,alice@example.com,30
2,Bob,bob@example.com,25
3,Charlie,charlie@example.com,35"""

        csv_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.csv")
        with open(csv_file, 'w') as f:
            f.write(csv_content)

        try:
            # Load data using simplified CSV interface with model
            result = test_client.load_data.from_csv(csv_file, User)
            assert result.affected_rows == 3

            # Verify data using query builder
            count = test_client.query('test_load_users').count()
            assert count == 3

        finally:
            os.unlink(csv_file)
            test_client.drop_table('test_load_users')

    def test_csv_with_header(self, test_client):
        """Test CSV file with header row (ignore first line)"""

        # Define table model
        class Product(Base):
            __tablename__ = 'test_load_products'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            price = Column(DECIMAL(10, 2))

        # Create table using model
        test_client.drop_table('test_load_products')
        test_client.create_table(Product)

        # Create sample CSV with header
        csv_content = """id,name,price
101,Laptop,999.99
102,Mouse,29.99
103,Keyboard,79.99"""

        csv_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.csv")
        with open(csv_file, 'w') as f:
            f.write(csv_content)

        try:
            # Load data with header using simplified CSV interface with model
            result = test_client.load_data.from_csv(csv_file, Product, ignore_lines=1)
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

        # Define table model
        class Order(Base):
            __tablename__ = 'test_load_orders'
            order_id = Column(Integer, primary_key=True)
            customer = Column(String(100))
            amount = Column(DECIMAL(10, 2))

        # Create table using model
        test_client.drop_table('test_load_orders')
        test_client.create_table(Order)

        # Create pipe-delimited file
        pipe_content = """1001|John Smith|1500.00
1002|Jane Doe|2500.00"""

        pipe_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.txt")
        with open(pipe_file, 'w') as f:
            f.write(pipe_content)

        try:
            # Load data with pipe delimiter using simplified CSV interface with model
            result = test_client.load_data.from_csv(pipe_file, Order, delimiter='|')
            assert result.affected_rows == 2

            # Verify data
            count = test_client.query('test_load_orders').count()
            assert count == 2

        finally:
            os.unlink(pipe_file)
            test_client.drop_table('test_load_orders')

    def test_quoted_fields(self, test_client):
        """Test CSV with quoted fields containing commas"""

        # Define table model
        class Address(Base):
            __tablename__ = 'test_load_addresses'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            address = Column(String(255))

        # Create table using model
        test_client.drop_table('test_load_addresses')
        test_client.create_table(Address)

        # Create CSV with quoted fields
        csv_content = '''1,"Alice Smith","123 Main St, Apt 4"
2,"Bob Jones","456 Oak Ave, Suite 100"'''

        csv_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.csv")
        with open(csv_file, 'w') as f:
            f.write(csv_content)

        try:
            # Load data with quoted fields using simplified CSV interface with model
            result = test_client.load_data.from_csv(csv_file, Address, delimiter=',', enclosed_by='"')
            assert result.affected_rows == 2

            # Verify data contains commas using query builder
            result = test_client.query('test_load_addresses').select('address').where('id = ?', 1).first()
            assert ',' in result.address

        finally:
            os.unlink(csv_file)
            test_client.drop_table('test_load_addresses')

    def test_tab_separated(self, test_client):
        """Test tab-separated values (TSV) file"""

        # Define table model
        class Log(Base):
            __tablename__ = 'test_load_logs'
            id = Column(Integer, primary_key=True)
            level = Column(String(20))
            message = Column(Text)

        # Create table using model
        test_client.drop_table('test_load_logs')
        test_client.create_table(Log)

        # Create TSV file
        tsv_content = """1\tINFO\tApplication started
2\tERROR\tConnection failed"""

        tsv_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.tsv")
        with open(tsv_file, 'w') as f:
            f.write(tsv_content)

        try:
            # Load tab-separated data using simplified TSV interface with model
            result = test_client.load_data.from_tsv(tsv_file, Log)
            assert result.affected_rows == 2

        finally:
            os.unlink(tsv_file)
            test_client.drop_table('test_load_logs')

    def test_column_mapping(self, test_client):
        """Test loading data into specific columns"""

        # Define table model with more columns
        class Employee(Base):
            __tablename__ = 'test_load_employees'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            email = Column(String(255))
            department = Column(String(50))

        # Create table using model
        test_client.drop_table('test_load_employees')
        test_client.create_table(Employee)

        # Create CSV with only 3 columns
        csv_content = """1,Alice,alice@example.com
2,Bob,bob@example.com"""

        csv_file = os.path.join(TMPFILES_DIR, f"test_{self.__class__.__name__}_{id(self)}.csv")
        with open(csv_file, 'w') as f:
            f.write(csv_content)

        try:
            # Load data into specific columns using simplified CSV interface with model
            result = test_client.load_data.from_csv(csv_file, Employee, columns=['id', 'name', 'email'])
            assert result.affected_rows == 2

            # Verify department is NULL using query builder
            result = test_client.query('test_load_employees').select('department').where('id = ?', 1).first()
            assert result.department is None

        finally:
            os.unlink(csv_file)
            test_client.drop_table('test_load_employees')

    def test_transaction_load(self, test_client):
        """Test loading data within a transaction"""

        # Define table models
        class Account(Base):
            __tablename__ = 'test_load_accounts'
            account_id = Column(Integer, primary_key=True)
            balance = Column(DECIMAL(10, 2))

        class Transaction(Base):
            __tablename__ = 'test_load_transactions'
            trans_id = Column(Integer, primary_key=True)
            account_id = Column(Integer)
            amount = Column(DECIMAL(10, 2))

        # Create tables using models
        test_client.drop_table('test_load_accounts')
        test_client.drop_table('test_load_transactions')
        test_client.create_table(Account)
        test_client.create_table(Transaction)

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
                # Load data using simplified CSV interface within transaction with models
                result1 = tx.load_data.from_csv(accounts_file, Account)
                result2 = tx.load_data.from_csv(transactions_file, Transaction)

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

        # Define table model
        class EmptyTest(Base):
            __tablename__ = 'test_load_empty'
            id = Column(Integer, primary_key=True)
            value = Column(String(50))

        # Create table using model
        test_client.drop_table('test_load_empty')
        test_client.create_table(EmptyTest)

        # Create empty file
        empty_file = os.path.join(TMPFILES_DIR, f"test_empty_{id(self)}.csv")
        with open(empty_file, 'w') as f:
            # Write nothing
            pass

        try:
            # Load data from empty file using simplified CSV interface with model
            result = test_client.load_data.from_csv(empty_file, EmptyTest)
            assert result.affected_rows == 0

            # Verify table is empty
            count = test_client.query('test_load_empty').count()
            assert count == 0

        finally:
            os.unlink(empty_file)
            test_client.drop_table('test_load_empty')

    def test_large_data_load(self, test_client):
        """Test loading a larger dataset"""

        # Define table model
        class LargeData(Base):
            __tablename__ = 'test_load_large'
            id = Column(Integer, primary_key=True)
            value = Column(String(50))

        # Create table using model
        test_client.drop_table('test_load_large')
        test_client.create_table(LargeData)

        # Create file with 1000 rows
        large_file = os.path.join(TMPFILES_DIR, f"test_large_{id(self)}.csv")
        with open(large_file, 'w') as f:
            for i in range(1000):
                f.write(f"{i},value_{i}\n")

        try:
            # Load data using simplified CSV interface with model
            result = test_client.load_data.from_csv(large_file, LargeData)
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
        test_client.create_table_orm('test_load_error', Column('id', Integer, primary_key=True))

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

    def test_optionally_enclosed_by(self, test_client):
        """Test OPTIONALLY ENCLOSED BY for mixed quoted/unquoted fields"""

        # Define table model
        class MixedData(Base):
            __tablename__ = 'test_load_optional_enclosed'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            description = Column(String(255))

        # Create table using model
        test_client.drop_table('test_load_optional_enclosed')
        test_client.create_table(MixedData)

        # Create CSV with mixed quoted/unquoted fields
        csv_content = '''1,Alice,Simple description
2,"Bob Smith","Description with, comma"
3,Charlie,Another simple one
4,"Diana Jones","Multi, part, description"'''

        csv_file = os.path.join(TMPFILES_DIR, f"test_optional_{id(self)}.csv")
        with open(csv_file, 'w') as f:
            f.write(csv_content)

        try:
            # Load data with OPTIONALLY ENCLOSED BY
            result = test_client.load_data.from_csv(csv_file, MixedData, enclosed_by='"', optionally_enclosed=True)
            assert result.affected_rows == 4

            # Verify data loaded correctly
            count = test_client.query('test_load_optional_enclosed').count()
            assert count == 4

            # Verify fields with commas were preserved
            result = test_client.query('test_load_optional_enclosed').select('description').where('id = ?', 2).first()
            assert ',' in result.description

        finally:
            os.unlink(csv_file)
            test_client.drop_table('test_load_optional_enclosed')

    def test_csv_inline(self, test_client):
        """Test LOAD DATA INLINE with CSV format"""

        # Define table model
        class InlineData(Base):
            __tablename__ = 'test_inline_csv'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            value = Column(String(100))

        # Create table using model
        test_client.drop_table('test_inline_csv')
        test_client.create_table(InlineData)

        try:
            # Load data from inline CSV string
            csv_data = "1,Alice,value1\n2,Bob,value2\n3,Charlie,value3\n"
            result = test_client.load_data.from_csv_inline(csv_data, InlineData)
            assert result.affected_rows == 3

            # Verify data
            count = test_client.query('test_inline_csv').count()
            assert count == 3

        finally:
            test_client.drop_table('test_inline_csv')

    def test_jsonline_inline(self, test_client):
        """Test LOAD DATA INLINE with JSONLINE format"""

        # Define table model
        class InlineJsonData(Base):
            __tablename__ = 'test_inline_jsonline'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))

        # Create table using model
        test_client.drop_table('test_inline_jsonline')
        test_client.create_table(InlineJsonData)

        try:
            # Load data from inline JSONLINE string
            jsonline_data = '{"id":1,"name":"Alice"}\n{"id":2,"name":"Bob"}\n'
            result = test_client.load_data.from_jsonline_inline(
                jsonline_data, InlineJsonData, structure=JsonDataStructure.OBJECT
            )
            assert result.affected_rows == 2

            # Verify data
            count = test_client.query('test_inline_jsonline').count()
            assert count == 2

        finally:
            test_client.drop_table('test_inline_jsonline')

    def test_stage_loading(self, test_client):
        """Test LOAD DATA from stage with format-specific interfaces"""

        # Define table model
        class StageData(Base):
            __tablename__ = 'test_stage_load'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))

        # Create table using model
        test_client.drop_table('test_stage_load')
        test_client.create_table(StageData)

        # Create temp files
        import tempfile
        import os

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

        try:
            # Create stage
            test_client.execute(f'CREATE STAGE IF NOT EXISTS pytest_stage URL="file://{tmpdir}/"')

            # Test from_stage_csv
            result = test_client.load_data.from_stage_csv('pytest_stage', 'test.csv', StageData)
            assert result.affected_rows == 2

            # Test from_stage_tsv
            result = test_client.load_data.from_stage_tsv('pytest_stage', 'test.tsv', StageData)
            assert result.affected_rows == 1

            # Test from_stage_jsonline
            result = test_client.load_data.from_stage_jsonline(
                'pytest_stage', 'test.jsonl', StageData, structure=JsonDataStructure.OBJECT
            )
            assert result.affected_rows == 1

            # Verify data
            count = test_client.query('test_stage_load').count()
            assert count == 4

        finally:
            # Cleanup
            test_client.execute('DROP STAGE IF EXISTS pytest_stage')
            test_client.drop_table('test_stage_load')
            os.unlink(csv_file)
            os.unlink(tsv_file)
            os.unlink(jsonl_file)
            os.rmdir(tmpdir)

    def test_stage_parquet_loading(self, test_client):
        """Test LOAD DATA from stage with Parquet format"""
        # Try to import pyarrow to generate parquet file
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            pytest.skip("pyarrow not installed, skipping parquet test")

        from sqlalchemy import BigInteger
        import os

        class ParquetData(Base):
            __tablename__ = 'test_stage_parquet'
            id = Column(BigInteger, primary_key=True, nullable=False)
            name = Column(String(100), nullable=False)

        # Create table
        test_client.drop_table('test_stage_parquet')
        test_client.create_table(ParquetData)

        # Generate parquet file in tmpfiles
        tmpfiles_dir = os.path.join(os.getcwd(), 'tmpfiles')
        os.makedirs(tmpfiles_dir, exist_ok=True)
        parquet_file = os.path.join(tmpfiles_dir, 'test_stage.parq')

        # Create parquet data with explicit schema (non-nullable to match table definition)
        schema = pa.schema([pa.field('id', pa.int64(), nullable=False), pa.field('name', pa.string(), nullable=False)])
        table = pa.table(
            {
                'id': pa.array([1, 2, 3, 4, 5], type=pa.int64()),
                'name': pa.array(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'], type=pa.string()),
            },
            schema=schema,
        )
        # Write with options compatible with MatrixOne
        pq.write_table(
            table, parquet_file, compression='none', use_dictionary=False, write_statistics=False, data_page_version='1.0'
        )

        try:
            # Create stage pointing to tmpfiles
            test_client.execute(f'CREATE STAGE IF NOT EXISTS parquet_stage URL="file://{tmpfiles_dir}/"')

            # Test from_stage_parquet
            result = test_client.load_data.from_stage_parquet('parquet_stage', 'test_stage.parq', ParquetData)
            assert result.affected_rows == 5

            # Verify data
            count = test_client.query('test_stage_parquet').count()
            assert count == 5

        finally:
            # Cleanup
            test_client.execute('DROP STAGE IF EXISTS parquet_stage')
            test_client.drop_table('test_stage_parquet')
            if os.path.exists(parquet_file):
                os.unlink(parquet_file)

    def test_jsonline_object_format(self, test_client):
        """Test JSONLINE format with object structure"""

        # Define table model
        class JsonlineUser(Base):
            __tablename__ = 'test_load_jsonline_obj'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            email = Column(String(255))
            age = Column(Integer)

        # Create table using model
        test_client.drop_table('test_load_jsonline_obj')
        test_client.create_table(JsonlineUser)

        # Create JSONLINE file with objects
        jl_content = '''{"id":1,"name":"Alice","email":"alice@example.com","age":30}
{"id":2,"name":"Bob","email":"bob@example.com","age":25}
{"id":3,"name":"Charlie","email":"charlie@example.com","age":35}'''

        jl_file = os.path.join(TMPFILES_DIR, f"test_jsonline_obj_{id(self)}.jl")
        with open(jl_file, 'w') as f:
            f.write(jl_content)

        try:
            # Load JSONLINE data using simplified interface with model and enum
            result = test_client.load_data.from_jsonline(jl_file, JsonlineUser, structure=JsonDataStructure.OBJECT)
            assert result.affected_rows == 3

            # Verify data using query builder
            count = test_client.query('test_load_jsonline_obj').count()
            assert count == 3

        finally:
            os.unlink(jl_file)
            test_client.drop_table('test_load_jsonline_obj')

    def test_jsonline_array_format(self, test_client):
        """Test JSONLINE format with array structure"""

        # Define table model
        class JsonlineProduct(Base):
            __tablename__ = 'test_load_jsonline_arr'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            email = Column(String(255))
            age = Column(Integer)

        # Create table using model
        test_client.drop_table('test_load_jsonline_arr')
        test_client.create_table(JsonlineProduct)

        # Create JSONLINE file with arrays
        jl_content = '''[1,"Alice","alice@example.com",30]
[2,"Bob","bob@example.com",25]
[3,"Charlie","charlie@example.com",35]'''

        jl_file = os.path.join(TMPFILES_DIR, f"test_jsonline_arr_{id(self)}.jl")
        with open(jl_file, 'w') as f:
            f.write(jl_content)

        try:
            # Load JSONLINE data using simplified interface with model and enum
            result = test_client.load_data.from_jsonline(jl_file, JsonlineProduct, structure=JsonDataStructure.ARRAY)
            assert result.affected_rows == 3

            # Verify data using query builder
            count = test_client.query('test_load_jsonline_arr').count()
            assert count == 3

        finally:
            os.unlink(jl_file)
            test_client.drop_table('test_load_jsonline_arr')

    def test_set_clause_nullif(self, test_client):
        """Test SET clause with NULLIF function"""

        # Define table model
        class CleanedData(Base):
            __tablename__ = 'test_load_set_nullif'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            status = Column(String(50))

        # Create table using model
        test_client.drop_table('test_load_set_nullif')
        test_client.create_table(CleanedData)

        # Create CSV with "null" strings
        csv_content = '''1,Alice,active
2,Bob,null
3,Charlie,active'''

        csv_file = os.path.join(TMPFILES_DIR, f"test_set_nullif_{id(self)}.csv")
        with open(csv_file, 'w') as f:
            f.write(csv_content)

        try:
            # Load data with SET clause using simplified CSV interface with model
            result = test_client.load_data.from_csv(csv_file, CleanedData, set_clause={'status': 'NULLIF(status, "null")'})
            assert result.affected_rows == 3

            # Verify NULL conversion using query builder
            result = test_client.query('test_load_set_nullif').select('status').where('id = ?', 2).first()
            assert result.status is None

        finally:
            os.unlink(csv_file)
            test_client.drop_table('test_load_set_nullif')


@pytest.mark.online
@pytest.mark.asyncio
class TestAsyncLoadDataOperations:
    """Async tests for LoadDataManager - sample coverage"""

    @pytest.mark.asyncio
    async def test_async_basic_csv_load(self):
        """Test basic async CSV file loading"""
        client = AsyncClient()
        await client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')

        try:
            # Define table model
            class AsyncUser(Base):
                __tablename__ = 'test_async_load_users'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                email = Column(String(255))
                age = Column(Integer)

            # Create table
            await client.drop_table('test_async_load_users')
            await client.create_table(AsyncUser)

            # Create sample CSV file
            csv_content = """1,Alice,alice@example.com,30
2,Bob,bob@example.com,25
3,Charlie,charlie@example.com,35"""

            csv_file = os.path.join(TMPFILES_DIR, f"test_async_csv_{id(self)}.csv")
            with open(csv_file, 'w') as f:
                f.write(csv_content)

            try:
                # Load data using async interface
                result = await client.load_data.from_csv(csv_file, AsyncUser)
                assert result.affected_rows == 3

                # Verify data
                count_result = await client.execute('SELECT COUNT(*) FROM test_async_load_users')
                count = count_result.rows[0][0]
                assert count == 3

            finally:
                os.unlink(csv_file)
                await client.drop_table('test_async_load_users')

        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_async_transaction_load(self):
        """Test async loading data within transaction"""
        client = AsyncClient()
        await client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')

        try:
            # Define table models
            class AsyncAccount(Base):
                __tablename__ = 'test_async_accounts'
                account_id = Column(Integer, primary_key=True)
                balance = Column(DECIMAL(10, 2))

            class AsyncTransaction(Base):
                __tablename__ = 'test_async_transactions'
                trans_id = Column(Integer, primary_key=True)
                account_id = Column(Integer)
                amount = Column(DECIMAL(10, 2))

            # Create tables
            await client.drop_table('test_async_accounts')
            await client.drop_table('test_async_transactions')
            await client.create_table(AsyncAccount)
            await client.create_table(AsyncTransaction)

            # Create data files
            accounts_content = """1,1000.00
2,2000.00"""
            transactions_content = """101,1,100.00
102,2,200.00"""

            accounts_file = os.path.join(TMPFILES_DIR, f"test_async_accounts_{id(self)}.csv")
            with open(accounts_file, 'w') as f:
                f.write(accounts_content)

            transactions_file = os.path.join(TMPFILES_DIR, f"test_async_transactions_{id(self)}.csv")
            with open(transactions_file, 'w') as f:
                f.write(transactions_content)

            try:
                # Load data within async transaction
                async with client.transaction() as tx:
                    result1 = await tx.load_data.from_csv(accounts_file, AsyncAccount)
                    result2 = await tx.load_data.from_csv(transactions_file, AsyncTransaction)

                    assert result1.affected_rows == 2
                    assert result2.affected_rows == 2

                # Verify data is committed
                accounts_result = await client.execute('SELECT COUNT(*) FROM test_async_accounts')
                transactions_result = await client.execute('SELECT COUNT(*) FROM test_async_transactions')

                assert accounts_result.rows[0][0] == 2
                assert transactions_result.rows[0][0] == 2

            finally:
                os.unlink(accounts_file)
                os.unlink(transactions_file)
                await client.drop_table('test_async_accounts')
                await client.drop_table('test_async_transactions')

        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_async_jsonline_load(self):
        """Test async JSONLINE file loading"""
        client = AsyncClient()
        await client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')

        try:
            # Define table model
            class AsyncJsonData(Base):
                __tablename__ = 'test_async_jsonline'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                email = Column(String(255))

            # Create table
            await client.drop_table('test_async_jsonline')
            await client.create_table(AsyncJsonData)

            # Create JSONLINE file
            jl_content = '''{"id":1,"name":"Alice","email":"alice@example.com"}
{"id":2,"name":"Bob","email":"bob@example.com"}
{"id":3,"name":"Charlie","email":"charlie@example.com"}'''

            jl_file = os.path.join(TMPFILES_DIR, f"test_async_jsonline_{id(self)}.jl")
            with open(jl_file, 'w') as f:
                f.write(jl_content)

            try:
                # Load JSONLINE data using async interface
                result = await client.load_data.from_jsonline(jl_file, AsyncJsonData, structure=JsonDataStructure.OBJECT)
                assert result.affected_rows == 3

                # Verify data
                count_result = await client.execute('SELECT COUNT(*) FROM test_async_jsonline')
                count = count_result.rows[0][0]
                assert count == 3

            finally:
                os.unlink(jl_file)
                await client.drop_table('test_async_jsonline')

        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_async_stage_load(self):
        """Test async loading from stage"""
        client = AsyncClient()
        await client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')

        try:
            # Define table model
            class AsyncStageData(Base):
                __tablename__ = 'test_async_stage'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))

            # Create table
            await client.drop_table('test_async_stage')
            await client.create_table(AsyncStageData)

            # Create temp files
            tmpdir = tempfile.mkdtemp()
            csv_file = os.path.join(tmpdir, 'async_test.csv')

            with open(csv_file, 'w') as f:
                f.write('1,Alice\n2,Bob\n3,Charlie\n')

            try:
                # Create stage
                await client.execute(f'CREATE STAGE IF NOT EXISTS async_stage URL="file://{tmpdir}/"')

                # Load from stage using async interface
                result = await client.load_data.from_stage_csv('async_stage', 'async_test.csv', AsyncStageData)
                assert result.affected_rows == 3

                # Verify data
                count_result = await client.execute('SELECT COUNT(*) FROM test_async_stage')
                count = count_result.rows[0][0]
                assert count == 3

            finally:
                # Cleanup
                await client.execute('DROP STAGE IF EXISTS async_stage')
                os.unlink(csv_file)
                os.rmdir(tmpdir)
                await client.drop_table('test_async_stage')

        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_async_inline_load(self):
        """Test async inline data loading"""
        client = AsyncClient()
        await client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')

        try:
            # Define table model
            class AsyncInlineData(Base):
                __tablename__ = 'test_async_inline'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))

            # Create table
            await client.drop_table('test_async_inline')
            await client.create_table(AsyncInlineData)

            try:
                # Load inline CSV data using async interface
                csv_data = "1,Alice\n2,Bob\n3,Charlie\n"
                result = await client.load_data.from_csv_inline(csv_data, AsyncInlineData)
                assert result.affected_rows == 3

                # Verify data
                count_result = await client.execute('SELECT COUNT(*) FROM test_async_inline')
                count = count_result.rows[0][0]
                assert count == 3

            finally:
                await client.drop_table('test_async_inline')

        finally:
            await client.disconnect()
