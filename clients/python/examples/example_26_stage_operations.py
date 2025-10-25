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
            result = client.load_data.from_stage_csv('demo_file_stage', 'stage_users.csv', StageUser)
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

            # Load data from stage within transaction
            with client.transaction() as tx:
                # Load data from pre-existing stage in transaction
                result = tx.load_data.from_stage_csv('tx_temp_stage', 'tx_data.csv', TxStageData)
                self.logger.info(f"✅ Loaded {result.affected_rows} rows in transaction")

            # Verify transaction committed
            count = client.query('tx_stage_demo').count()
            assert count == 3
            self.logger.info(f"✅ Transaction committed: {count} rows in table")

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
            # Run all tests
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
