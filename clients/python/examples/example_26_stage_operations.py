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
MatrixOne Python SDK - Stage Operations Example

This example demonstrates comprehensive stage management operations including:
- Creating stages (file system, S3, sub-stages)
- Modifying stage properties (URL, credentials, comment, enable/disable)
- Listing and querying stages
- Loading data from stages
- Using stage direct load methods (load_csv, load_json, load_files)
- Stage operations within transactions
"""

import os
import tempfile
from matrixone import Client, Stage, LoadDataFormat, JsonDataStructure
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params, print_config
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL

# Get database from declarative_base
Base = declarative_base()

# Temporary files directory
TMPFILES_DIR = os.path.join(os.getcwd(), 'tmpfiles')
os.makedirs(TMPFILES_DIR, exist_ok=True)


class StageOperationsDemo:
    """
    Comprehensive demonstration of MatrixOne Stage operations.

    A Stage is a named external storage location (filesystem, S3, cloud storage)
    that provides centralized, secure, and reusable access to data files.
    """

    def __init__(self):
        self.logger = create_default_logger()
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'stages_created': [],
            'tables_created': [],
            'files_created': [],
        }

    def test_create_local_stage(self, client):
        """Test creating a local stage with simplified path handling"""
        print("\n=== Creating Local Stage (Simplified) ===")
        self.results['tests_run'] += 1

        try:
            # Create local stage with relative path
            stage = client.stage.create_local(
                'demo_local_stage', './tmpfiles/', comment='Demo local stage with auto path handling'
            )
            self.results['stages_created'].append('demo_local_stage')

            self.logger.info(f"✅ Created local stage: {stage.name}")
            self.logger.info(f"  Input path: ./tmpfiles/")
            self.logger.info(f"  Auto URL: {stage.url}")
            self.logger.info(f"  Status: {stage.status}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Create local stage test failed: {e}")
            self.results['tests_failed'] += 1

    def test_create_file_stage(self, client):
        """Test creating a file system stage"""
        print("\n=== Creating File System Stage ===")
        self.results['tests_run'] += 1

        try:
            # Create a file system stage
            stage = client.stage.create('demo_file_stage', f'file://{TMPFILES_DIR}/', comment='Demo file system stage')
            self.results['stages_created'].append('demo_file_stage')

            self.logger.info(f"✅ Created file system stage: {stage.name}")
            self.logger.info(f"  URL: {stage.url}")
            self.logger.info(f"  Status: {stage.status}")
            self.logger.info(f"  Comment: {stage.comment}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Create file stage test failed: {e}")
            self.results['tests_failed'] += 1

    def test_create_stage_with_if_not_exists(self, client):
        """Test CREATE STAGE IF NOT EXISTS"""
        print("\n=== CREATE STAGE IF NOT EXISTS ===")
        self.results['tests_run'] += 1

        try:
            # Create stage with IF NOT EXISTS (should succeed even if exists)
            stage = client.stage.create('demo_file_stage', f'file://{TMPFILES_DIR}/', if_not_exists=True)

            self.logger.info(f"✅ CREATE STAGE IF NOT EXISTS succeeded")
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ IF NOT EXISTS test failed: {e}")
            self.results['tests_failed'] += 1

    def test_list_and_show_stages(self, client):
        """Test listing and showing stages"""
        print("\n=== List and Show Stages ===")
        self.results['tests_run'] += 1

        try:
            # List all stages
            stages = client.stage.list()
            self.logger.info(f"✅ Listed {len(stages)} stage(s) from mo_catalog.mo_stages")

            for stage in stages:
                self.logger.info(f"  - {stage.name}: {stage.url} [{stage.status}]")

            # Show stages
            stages = client.stage.show()
            self.logger.info(f"✅ SHOW STAGES returned {len(stages)} stage(s)")

            # Show with LIKE pattern
            stages = client.stage.show(like_pattern='demo%')
            self.logger.info(f"✅ SHOW STAGES LIKE 'demo%' returned {len(stages)} stage(s)")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ List/show stages test failed: {e}")
            self.results['tests_failed'] += 1

    def test_get_and_exists(self, client):
        """Test getting specific stage and checking existence"""
        print("\n=== Get Stage and Check Existence ===")
        self.results['tests_run'] += 1

        try:
            # Check if stage exists
            exists = client.stage.exists('demo_file_stage')
            assert exists, "Stage should exist"
            self.logger.info("✅ Stage exists check passed")

            # Get specific stage
            stage = client.stage.get('demo_file_stage')
            self.logger.info(f"✅ Retrieved stage: {stage.name}")
            self.logger.info(f"  URL: {stage.url}")
            self.logger.info(f"  Status: {stage.status}")

            # Check non-existent stage
            exists = client.stage.exists('nonexistent_stage')
            assert not exists, "Non-existent stage should return False"
            self.logger.info("✅ Non-existent stage check passed")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Get/exists test failed: {e}")
            self.results['tests_failed'] += 1

    def test_alter_stage(self, client):
        """Test modifying stage properties"""
        print("\n=== Alter Stage Properties ===")
        self.results['tests_run'] += 1

        try:
            # Alter comment
            client.stage.alter('demo_file_stage', comment='Updated comment')
            stage = client.stage.get('demo_file_stage')
            self.logger.info(f"✅ Updated comment: {stage.comment}")

            # Alter enable status
            client.stage.alter('demo_file_stage', enable=False)
            self.logger.info("✅ Disabled stage")

            client.stage.alter('demo_file_stage', enable=True)
            self.logger.info("✅ Re-enabled stage")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Alter stage test failed: {e}")
            self.results['tests_failed'] += 1

    def test_load_data_from_stage(self, client):
        """Test loading data from stage"""
        print("\n=== Load Data from Stage ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class StageUser(Base):
                __tablename__ = 'stage_demo_users'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                email = Column(String(255))

            # Create table
            client.drop_table('stage_demo_users')
            client.create_table(StageUser)
            self.results['tables_created'].append('stage_demo_users')

            # Create sample CSV file in tmpfiles (which is our stage location)
            csv_file = os.path.join(TMPFILES_DIR, 'stage_users.csv')
            with open(csv_file, 'w') as f:
                f.write('1,Alice,alice@example.com\n')
                f.write('2,Bob,bob@example.com\n')
                f.write('3,Charlie,charlie@example.com\n')
            self.results['files_created'].append(csv_file)

            # Load data from stage using client.load_data
            result = client.load_data.read_csv_stage('demo_file_stage', 'stage_users.csv', StageUser)
            self.logger.info(f"✅ Loaded {result.affected_rows} rows from stage")

            # Verify data
            count = client.query('stage_demo_users').count()
            assert count == 3
            self.logger.info(f"✅ Verified: {count} rows in table")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Load from stage test failed: {e}")
            self.results['tests_failed'] += 1

    def test_stage_direct_load_api(self, client):
        """Test using stage direct load methods"""
        print("\n=== Stage Direct Load API ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class StageProduct(Base):
                __tablename__ = 'stage_demo_products'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                price = Column(DECIMAL(10, 2))

            # Create table
            client.drop_table('stage_demo_products')
            client.create_table(StageProduct)
            self.results['tables_created'].append('stage_demo_products')

            # Create sample CSV file
            csv_file = os.path.join(TMPFILES_DIR, 'products.csv')
            with open(csv_file, 'w') as f:
                f.write('1,Laptop,999.99\n')
                f.write('2,Mouse,29.99\n')
                f.write('3,Keyboard,79.99\n')
            self.results['files_created'].append(csv_file)

            # Get stage object
            stage = client.stage.get('demo_file_stage')

            # Use stage.load_csv (automatically passes stage name)
            result = stage.load_csv('products.csv', StageProduct)
            self.logger.info(f"✅ Loaded {result.affected_rows} rows using stage.load_csv()")

            # Verify
            count = client.query('stage_demo_products').count()
            assert count == 3
            self.logger.info(f"✅ Verified: {count} rows in table")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Stage.load_data API test failed: {e}")
            self.results['tests_failed'] += 1

    def test_stage_with_jsonline(self, client):
        """Test loading JSONLINE data from stage"""
        print("\n=== Load JSONLINE from Stage ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class StageEvent(Base):
                __tablename__ = 'stage_demo_events'
                id = Column(Integer, primary_key=True)
                event_type = Column(String(50))
                user_id = Column(Integer)

            # Create table
            client.drop_table('stage_demo_events')
            client.create_table(StageEvent)
            self.results['tables_created'].append('stage_demo_events')

            # Create JSONLINE file
            jsonl_file = os.path.join(TMPFILES_DIR, 'events.jsonl')
            with open(jsonl_file, 'w') as f:
                f.write('{"id":1,"event_type":"login","user_id":100}\n')
                f.write('{"id":2,"event_type":"purchase","user_id":101}\n')
                f.write('{"id":3,"event_type":"logout","user_id":100}\n')
            self.results['files_created'].append(jsonl_file)

            # Load JSONLINE from stage
            stage = client.stage.get('demo_file_stage')
            result = stage.load_json('events.jsonl', StageEvent, structure=JsonDataStructure.OBJECT)
            self.logger.info(f"✅ Loaded {result.affected_rows} rows from JSONLINE in stage")

            # Verify
            count = client.query('stage_demo_events').count()
            assert count == 3
            self.logger.info(f"✅ Verified: {count} events")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ JSONLINE from stage test failed: {e}")
            self.results['tests_failed'] += 1

    def test_multiple_stages(self, client):
        """Test managing multiple stages"""
        print("\n=== Multiple Stages Management ===")
        self.results['tests_run'] += 1

        try:
            # Create multiple stages
            tmpdir1 = tempfile.mkdtemp()
            tmpdir2 = tempfile.mkdtemp()

            stage1 = client.stage.create(
                'stage_demo_1', f'file://{tmpdir1}/', comment='First demo stage', if_not_exists=True
            )
            self.results['stages_created'].append('stage_demo_1')

            stage2 = client.stage.create(
                'stage_demo_2', f'file://{tmpdir2}/', comment='Second demo stage', if_not_exists=True
            )
            self.results['stages_created'].append('stage_demo_2')

            self.logger.info(f"✅ Created 2 stages: {stage1.name}, {stage2.name}")

            # List all stages with demo prefix
            stages = client.stage.show(like_pattern='stage_demo_%')
            self.logger.info(f"✅ Found {len(stages)} stages matching 'stage_demo_%'")

            # Cleanup temp dirs
            os.rmdir(tmpdir1)
            os.rmdir(tmpdir2)

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Multiple stages test failed: {e}")
            self.results['tests_failed'] += 1

    def test_stage_in_transaction(self, client):
        """Test stage operations within transaction"""
        print("\n=== Stage Operations in Transaction ===")
        self.results['tests_run'] += 1

        # Note: Stage creation must happen outside transaction as it's a DDL operation
        # and may not be visible within the same transaction
        client.stage.create('tx_temp_stage', f'file://{TMPFILES_DIR}/', if_not_exists=True)
        self.results['stages_created'].append('tx_temp_stage')

        try:
            # Define table model
            class TxStageData(Base):
                __tablename__ = 'tx_stage_demo'
                id = Column(Integer, primary_key=True)
                value = Column(String(100))

            # Create table
            client.drop_table('tx_stage_demo')
            client.create_table(TxStageData)
            self.results['tables_created'].append('tx_stage_demo')

            # Create data file
            csv_file = os.path.join(TMPFILES_DIR, 'tx_data.csv')
            with open(csv_file, 'w') as f:
                f.write('1,value1\n2,value2\n3,value3\n')
            self.results['files_created'].append(csv_file)

            # Load data from stage within session
            with client.session() as tx:
                # Load data from pre-existing stage in session
                result = tx.load_data.read_csv_stage('tx_temp_stage', 'tx_data.csv', TxStageData)
                # Check for both ResultSet and SQLAlchemy Result
                affected = result.affected_rows if hasattr(result, 'affected_rows') else result.rowcount
                self.logger.info(f"✅ Loaded {affected} rows in session")

            # Verify session committed
            count = client.query('tx_stage_demo').count()
            assert count == 3
            self.logger.info(f"✅ Session committed: {count} rows in table")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Transaction stage test failed: {e}")
            self.results['tests_failed'] += 1

    def test_stage_convenience_methods(self, client):
        """Test stage convenience methods (load_csv, load_json, load_files)"""
        print("\n=== Stage Convenience Methods ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class ConvenienceData(Base):
                __tablename__ = 'stage_convenience_demo'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))

            # Create table
            client.drop_table('stage_convenience_demo')
            client.create_table(ConvenienceData)
            self.results['tables_created'].append('stage_convenience_demo')

            # Create test files
            csv_file = os.path.join(TMPFILES_DIR, 'convenience.csv')
            with open(csv_file, 'w') as f:
                f.write('1,Alice\n2,Bob\n')
            self.results['files_created'].append(csv_file)

            jsonl_file = os.path.join(TMPFILES_DIR, 'convenience.jsonl')
            with open(jsonl_file, 'w') as f:
                f.write('{"id":3,"name":"Charlie"}\n{"id":4,"name":"Diana"}\n')
            self.results['files_created'].append(jsonl_file)

            # Get stage
            stage = client.stage.get('demo_file_stage')

            # Test load_csv convenience method (direct on Stage object)
            result = stage.load_csv('convenience.csv', ConvenienceData)
            self.logger.info(f"✅ stage.load_csv(): Loaded {result.affected_rows} rows")

            # Test load_json convenience method (direct on Stage object)
            result = stage.load_json('convenience.jsonl', ConvenienceData)
            self.logger.info(f"✅ stage.load_json(): Loaded {result.affected_rows} rows")

            # Test load_files batch method (direct on Stage object)
            client.execute('TRUNCATE TABLE stage_convenience_demo')
            results = stage.load_files({'convenience.csv': ConvenienceData, 'convenience.jsonl': ConvenienceData})

            total_rows = sum(r.affected_rows for r in results.values() if hasattr(r, 'affected_rows'))
            self.logger.info(f"✅ load_files(): Batch loaded {total_rows} rows from {len(results)} files")

            # Verify
            count = client.query('stage_convenience_demo').count()
            self.logger.info(f"✅ Verified: {count} rows in table")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Convenience methods test failed: {e}")
            self.results['tests_failed'] += 1

    def test_stage_formats(self, client):
        """Test loading different file formats from stage"""
        print("\n=== Multiple File Formats from Stage ===")
        self.results['tests_run'] += 1

        try:
            # Define table model
            class FormatData(Base):
                __tablename__ = 'stage_format_demo'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))

            # Create table
            client.drop_table('stage_format_demo')
            client.create_table(FormatData)
            self.results['tables_created'].append('stage_format_demo')

            # Get stage
            stage = client.stage.get('demo_file_stage')

            # Test CSV
            csv_file = os.path.join(TMPFILES_DIR, 'format_test.csv')
            with open(csv_file, 'w') as f:
                f.write('1,Alice\n2,Bob\n')
            self.results['files_created'].append(csv_file)

            result = stage.load_csv('format_test.csv', FormatData)
            self.logger.info(f"✅ CSV: Loaded {result.affected_rows} rows")

            # Test TSV
            client.execute('TRUNCATE TABLE stage_format_demo')
            tsv_file = os.path.join(TMPFILES_DIR, 'format_test.tsv')
            with open(tsv_file, 'w') as f:
                f.write('3\tCharlie\n4\tDiana\n')
            self.results['files_created'].append(tsv_file)

            result = stage.load_tsv('format_test.tsv', FormatData)
            self.logger.info(f"✅ TSV: Loaded {result.affected_rows} rows")

            # Test JSONLINE
            client.execute('TRUNCATE TABLE stage_format_demo')
            jsonl_file = os.path.join(TMPFILES_DIR, 'format_test.jsonl')
            with open(jsonl_file, 'w') as f:
                f.write('{"id":5,"name":"Eve"}\n{"id":6,"name":"Frank"}\n')
            self.results['files_created'].append(jsonl_file)

            result = stage.load_json('format_test.jsonl', FormatData, structure=JsonDataStructure.OBJECT)
            self.logger.info(f"✅ JSONLINE: Loaded {result.affected_rows} rows")

            # Verify total
            count = client.query('stage_format_demo').count()
            self.logger.info(f"✅ All formats tested successfully (total {count} rows)")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Multiple formats test failed: {e}")
            self.results['tests_failed'] += 1

    def cleanup(self, client):
        """Clean up test resources"""
        print("\n=== Cleanup ===")

        # Drop tables
        for table in self.results['tables_created']:
            try:
                client.drop_table(table)
            except Exception as e:
                self.logger.warning(f"Failed to drop table {table}: {e}")

        # Drop stages
        for stage_name in self.results['stages_created']:
            try:
                client.stage.drop(stage_name, if_exists=True)
            except Exception as e:
                self.logger.warning(f"Failed to drop stage {stage_name}: {e}")

        # Remove files
        for file_path in self.results['files_created']:
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
            except Exception as e:
                self.logger.warning(f"Failed to remove file {file_path}: {e}")

        self.logger.info("✅ Cleanup completed")

    def test_real_world_ecommerce_pipeline(self, client):
        """
        Real-world example: E-commerce Order Data Pipeline

        Scenario:
        An e-commerce company receives daily order files from multiple sources:
        - customers.csv: Customer dimension data
        - products.jsonline: Product catalog updates (JSON format)
        - orders.csv: Daily order transactions
        - shipping.tsv: Shipping logistics data

        This pipeline demonstrates:
        1. Creating a centralized data stage for multi-source ingestion
        2. Loading data from multiple file formats (CSV, JSON, TSV)
        3. Building a star schema (dimensions + facts)
        4. Business intelligence queries
        5. Incremental data loading (delta loads)

        """
        print("\n" + "=" * 60)
        print("REAL-WORLD EXAMPLE: E-commerce Data Pipeline")
        print("=" * 60)
        self.results['tests_run'] += 1

        try:
            # Step 1: Create centralized data stage
            print("\n📦 Step 1: Setting up E-commerce Data Stage")
            print("-" * 60)

            stage_path = os.path.join(TMPFILES_DIR, 'ecommerce_data')
            os.makedirs(stage_path, exist_ok=True)

            stage = client.stage.create_local(
                'ecommerce_data_stage', stage_path, comment='Centralized stage for e-commerce data ingestion'
            )
            self.results['stages_created'].append('ecommerce_data_stage')

            print(f"✅ Stage created: {stage.name}")
            print(f"   Purpose: Centralized data ingestion point")
            print(f"   Location: {stage.url}")

            # Step 2: Create data warehouse tables (star schema)
            print("\n📊 Step 2: Creating Star Schema Tables")
            print("-" * 60)

            # Dimension: Customers
            class Customer(Base):
                __tablename__ = 'dim_customers'
                customer_id = Column(Integer, primary_key=True)
                customer_name = Column(String(100))
                email = Column(String(100))
                country = Column(String(50))
                signup_date = Column(String(20))

            # Dimension: Products
            class Product(Base):
                __tablename__ = 'dim_products'
                product_id = Column(Integer, primary_key=True)
                product_name = Column(String(200))
                category = Column(String(50))
                price = Column(DECIMAL(10, 2))
                stock_quantity = Column(Integer)

            # Fact: Orders
            class Order(Base):
                __tablename__ = 'fact_orders'
                order_id = Column(Integer, primary_key=True)
                customer_id = Column(Integer)
                product_id = Column(Integer)
                quantity = Column(Integer)
                order_amount = Column(DECIMAL(12, 2))
                order_date = Column(String(20))
                status = Column(String(20))

            # Fact: Shipping
            class Shipping(Base):
                __tablename__ = 'fact_shipping'
                shipping_id = Column(Integer, primary_key=True)
                order_id = Column(Integer)
                carrier = Column(String(50))
                tracking_number = Column(String(100))
                ship_date = Column(String(20))
                delivery_date = Column(String(20))

            # Create all tables
            for table_class in [Customer, Product, Order, Shipping]:
                try:
                    client.drop_table(table_class)
                except:
                    pass
                client.create_table(table_class)
                self.results['tables_created'].append(table_class.__tablename__)
                print(f"✅ Created table: {table_class.__tablename__}")

            # Step 3: Generate sample data files
            print("\n📝 Step 3: Generating Sample Data Files")
            print("-" * 60)

            # customers.csv
            customers_file = os.path.join(stage_path, 'customers.csv')
            with open(customers_file, 'w') as f:
                f.write('customer_id,customer_name,email,country,signup_date\n')
                f.write('1001,Alice Johnson,alice@email.com,USA,2024-01-15\n')
                f.write('1002,Bob Smith,bob@email.com,UK,2024-01-20\n')
                f.write('1003,Carol Wang,carol@email.com,China,2024-02-01\n')
                f.write('1004,David Brown,david@email.com,Canada,2024-02-15\n')
            self.results['files_created'].append(customers_file)
            print(f"✅ Generated: customers.csv (4 records)")

            # products.jsonline
            products_file = os.path.join(stage_path, 'products.jsonline')
            with open(products_file, 'w') as f:
                f.write(
                    '{"product_id":2001,"product_name":"Laptop Pro 15","category":"Electronics","price":1299.99,"stock_quantity":50}\n'
                )
                f.write(
                    '{"product_id":2002,"product_name":"Wireless Mouse","category":"Accessories","price":29.99,"stock_quantity":200}\n'
                )
                f.write(
                    '{"product_id":2003,"product_name":"USB-C Cable","category":"Accessories","price":19.99,"stock_quantity":500}\n'
                )
                f.write(
                    '{"product_id":2004,"product_name":"Monitor 27inch","category":"Electronics","price":399.99,"stock_quantity":75}\n'
                )
            self.results['files_created'].append(products_file)
            print(f"✅ Generated: products.jsonline (4 records)")

            # orders.csv
            orders_file = os.path.join(stage_path, 'orders.csv')
            with open(orders_file, 'w') as f:
                f.write('order_id,customer_id,product_id,quantity,order_amount,order_date,status\n')
                f.write('5001,1001,2001,1,1299.99,2024-10-20,completed\n')
                f.write('5002,1002,2002,2,59.98,2024-10-21,completed\n')
                f.write('5003,1003,2003,3,59.97,2024-10-22,processing\n')
                f.write('5004,1001,2004,1,399.99,2024-10-23,shipped\n')
                f.write('5005,1004,2001,1,1299.99,2024-10-24,processing\n')
            self.results['files_created'].append(orders_file)
            print(f"✅ Generated: orders.csv (5 records)")

            # shipping.tsv
            shipping_file = os.path.join(stage_path, 'shipping.tsv')
            with open(shipping_file, 'w') as f:
                f.write('shipping_id\torder_id\tcarrier\ttracking_number\tship_date\tdelivery_date\n')
                f.write('7001\t5001\tFedEx\tFDX123456789\t2024-10-21\t2024-10-23\n')
                f.write('7002\t5002\tUPS\tUPS987654321\t2024-10-22\t2024-10-25\n')
                f.write('7003\t5004\tDHL\tDHL456789123\t2024-10-24\t2024-10-27\n')
            self.results['files_created'].append(shipping_file)
            print(f"✅ Generated: shipping.tsv (3 records)")

            # Step 4: Load data from stage
            print("\n🚀 Step 4: Loading Data from Stage")
            print("-" * 60)

            # Load customers
            print("Loading customers dimension...")
            stage.load_csv('customers.csv', Customer, skiprows=1)
            count = client.query(Customer).count()
            print(f"✅ Loaded {count} customers")

            # Load products (JSON) - jsondata is auto-defaulted to OBJECT
            print("Loading products dimension...")
            stage.load_json('products.jsonline', Product)
            count = client.query(Product).count()
            print(f"✅ Loaded {count} products")

            # Load orders
            print("Loading orders fact table...")
            stage.load_csv('orders.csv', Order, skiprows=1)
            count = client.query(Order).count()
            print(f"✅ Loaded {count} orders")

            # Load shipping (TSV)
            print("Loading shipping logistics...")
            stage.load_tsv('shipping.tsv', Shipping, skiprows=1)
            count = client.query(Shipping).count()
            print(f"✅ Loaded {count} shipping records")

            # Step 5: Business Intelligence Queries (using client.query ORM)
            print("\n📊 Step 5: Business Intelligence Analysis")
            print("-" * 60)

            from sqlalchemy import func

            # Revenue by customer using client.query() ORM with SQLAlchemy expressions
            print("\n💰 Revenue Analysis:")
            revenue_results = (
                client.query(
                    Order,
                    Customer.customer_name,
                    Customer.country,
                    func.sum(Order.order_amount).label('total_revenue'),
                    func.count(Order.order_id).label('order_count'),
                )
                .join(Customer, Customer.customer_id == Order.customer_id)
                .group_by(Customer.customer_name, Customer.country)
                .order_by(func.sum(Order.order_amount).desc())
                .all()
            )
            for row in revenue_results:
                print(f"  {row.customer_name} ({row.country}): ${row.total_revenue} from {row.order_count} orders")

            # Top products using client.query() ORM with SQLAlchemy expressions
            print("\n🔥 Top Selling Products:")
            products_results = (
                client.query(
                    Order,
                    Product.product_name,
                    Product.category,
                    func.sum(Order.quantity).label('units_sold'),
                    func.sum(Order.order_amount).label('revenue'),
                )
                .join(Product, Product.product_id == Order.product_id)
                .group_by(Product.product_name, Product.category)
                .order_by(func.sum(Order.quantity).desc())
                .all()
            )
            for row in products_results:
                print(f"  {row.product_name} ({row.category}): {row.units_sold} units, ${row.revenue} revenue")

            # Order status using client.query() ORM (simple aggregation, no join)
            print("\n📦 Order Fulfillment Status:")
            status_results = (
                client.query(Order.status, func.count().label('count'), func.sum(Order.order_amount).label('value'))
                .group_by(Order.status)
                .order_by(func.count().desc())
                .all()
            )
            for row in status_results:
                print(f"  {row.status}: {row.count} orders, ${row.value} value")

            # Step 6: Incremental Loading (Delta)
            print("\n🔄 Step 6: Incremental Data Loading")
            print("-" * 60)

            # New orders delta file
            delta_file = os.path.join(stage_path, 'orders_delta.csv')
            with open(delta_file, 'w') as f:
                f.write('order_id,customer_id,product_id,quantity,order_amount,order_date,status\n')
                f.write('5006,1002,2003,5,99.95,2024-10-25,processing\n')
                f.write('5007,1003,2002,1,29.99,2024-10-25,completed\n')
            self.results['files_created'].append(delta_file)

            print("📥 Loading incremental orders...")
            stage.load_csv('orders_delta.csv', Order, skiprows=1)
            count = client.query(Order).count()
            print(f"✅ Total orders now: {count} (+2 new)")

            # Summary
            print("\n" + "=" * 60)
            print("📊 Pipeline Summary")
            print("=" * 60)
            print(f"✅ Stage: {stage.name}")
            print(f"✅ Tables: 4 (2 dimensions, 2 facts)")
            print(f"✅ Files: 5 (CSV, JSONLINE, TSV)")
            print(f"✅ Records: ~20+")
            print(f"✅ BI Queries: 3")

            print("\n💡 Key Takeaways:")
            print("  • Centralized stage simplifies multi-source data ingestion")
            print("  • Supports multiple formats (CSV, JSON, TSV, Parquet)")
            print("  • Direct load methods provide clean, intuitive API")
            print("  • Enables real-world ETL/ELT patterns")
            print("  • Perfect for data warehouse and analytics workloads")
            print("  • Mirrors Snowflake/cloud data warehouse Stage concepts")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"❌ E-commerce pipeline test failed: {e}")
            import traceback

            traceback.print_exc()
            self.results['tests_failed'] += 1

    def run(self):
        """Run all stage operation demos"""
        print("=" * 60)
        print("MatrixOne Python SDK - Stage Operations Example")
        print("=" * 60)

        # Get connection parameters
        host, port, user, password, database = get_connection_params()

        # Create client
        client = Client(logger=self.logger)
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            self.test_create_local_stage(client)
            self.test_create_file_stage(client)
            self.test_create_stage_with_if_not_exists(client)
            self.test_list_and_show_stages(client)
            self.test_get_and_exists(client)
            self.test_alter_stage(client)
            self.test_load_data_from_stage(client)
            self.test_stage_direct_load_api(client)
            self.test_stage_convenience_methods(client)
            self.test_stage_formats(client)
            self.test_multiple_stages(client)
            self.test_stage_in_transaction(client)
            self.test_real_world_ecommerce_pipeline(client)

            # Print results
            print("\n" + "=" * 60)
            print("Test Results:")
            print("=" * 60)
            print(f"Tests run: {self.results['tests_run']}")
            print(f"Tests passed: {self.results['tests_passed']}")
            print(f"Tests failed: {self.results['tests_failed']}")
            print(f"Success rate: {(self.results['tests_passed'] / self.results['tests_run'] * 100):.1f}%")

            print("\n" + "=" * 60)
            if self.results['tests_failed'] == 0:
                print("✅ All tests passed successfully!")
            else:
                print("❌ Some tests failed. Check the logs above for details.")
            print("=" * 60)

        finally:
            # Cleanup
            self.cleanup(client)
            client.disconnect()


def main():
    """Main entry point"""
    demo = StageOperationsDemo()
    demo.run()


if __name__ == "__main__":
    main()
