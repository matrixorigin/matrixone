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
Online tests for Export operations (INTO OUTFILE, INTO STAGE)

These tests are inspired by example_27_export_operations.py
"""

import pytest
import os
import tempfile
from matrixone import Client, ExportFormat
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL, func

# Create Base for model definitions
Base = declarative_base()

# Create tmpfiles directory if it doesn't exist
TMPFILES_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'tmpfiles')
os.makedirs(TMPFILES_DIR, exist_ok=True)


# Define table model for export tests
class ExportTestData(Base):
    __tablename__ = 'export_test_data'
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    value = Column(Integer)


@pytest.fixture
def test_client():
    """Fixture for test client"""
    client = Client()
    client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')
    yield client
    client.disconnect()


@pytest.mark.online
class TestExportToFile:
    """Test SELECT ... INTO OUTFILE functionality"""

    def test_export_to_csv_file(self, test_client):
        """Test basic export to CSV file"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})
        test_client.insert(ExportTestData, {'id': 2, 'name': 'Bob', 'value': 200})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'test_export.csv')

        try:
            # Export using low-level API
            result = test_client.export.to_file(
                query="SELECT * FROM export_test_data", filepath=export_file, format=ExportFormat.CSV
            )

            assert result is not None
            assert os.path.exists(export_file)

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_export_with_custom_delimiter(self, test_client):
        """Test exporting with custom field delimiter"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'test_custom_delim.csv')

        try:
            # Export with custom delimiter (pipe-separated)
            result = test_client.export.to_file(
                query="SELECT * FROM export_test_data",
                filepath=export_file,
                format=ExportFormat.CSV,
                fields_terminated_by='|',
            )

            assert result is not None

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_export_with_field_enclosure(self, test_client):
        """Test exporting with field enclosure (quotes)"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Data,WithComma', 'value': 100})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'test_enclosed.csv')

        try:
            # Export with field enclosure only
            result = test_client.export.to_file(
                query="SELECT * FROM export_test_data", filepath=export_file, format=ExportFormat.CSV, fields_enclosed_by='"'
            )

            assert result is not None

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')


@pytest.mark.online
class TestExportToStage:
    """Test SELECT ... INTO STAGE functionality (using stage:// protocol in INTO OUTFILE)"""

    def test_export_to_stage_csv(self, test_client):
        """Test basic export to stage as CSV"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})
        test_client.insert(ExportTestData, {'id': 2, 'name': 'Bob', 'value': 200})

        # Create temporary stage
        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        stage_name = 'pytest_export_stage'

        try:
            # Create stage
            test_client.stage.create(stage_name, f"file://{tmpdir}/", if_not_exists=True)

            # Export using low-level API
            result = test_client.export.to_stage(
                query="SELECT * FROM export_test_data",
                stage_name=stage_name,
                filename='test_export.csv',
                format=ExportFormat.CSV,
            )

            assert result is not None

        finally:
            test_client.stage.drop(stage_name, if_exists=True)
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_stage_export_to_convenience_method(self, test_client):
        """Test stage.export_to() convenience method"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})

        # Create temporary stage
        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        stage_name = 'pytest_export_stage_conv'

        try:
            # Create stage and get stage object
            test_client.stage.create(stage_name, f"file://{tmpdir}/", if_not_exists=True)
            stage = test_client.stage.get(stage_name)

            # Export using stage convenience method
            result = stage.export_to(query="SELECT * FROM export_test_data", filename='test_export_conv.csv')

            assert result is not None

        finally:
            test_client.stage.drop(stage_name, if_exists=True)
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_export_aggregated_query(self, test_client):
        """Test exporting aggregated query results"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})
        test_client.insert(ExportTestData, {'id': 2, 'name': 'Bob', 'value': 200})
        test_client.insert(ExportTestData, {'id': 3, 'name': 'Alice', 'value': 150})

        # Create temporary stage
        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        stage_name = 'pytest_export_stage_agg'

        try:
            # Create stage
            test_client.stage.create(stage_name, f"file://{tmpdir}/", if_not_exists=True)

            # Export aggregated data using ORM
            result = (
                test_client.query(ExportTestData.name, func.sum(ExportTestData.value).label('total'))
                .group_by(ExportTestData.name)
                .export_to_stage(stage_name, 'aggregated_export.csv')
            )

            assert result is not None

        finally:
            test_client.stage.drop(stage_name, if_exists=True)
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')


@pytest.mark.online
class TestExportFormats:
    """Test different export formats"""

    def test_export_jsonline_format(self, test_client):
        """Test exporting as JSONLINE format"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})

        # Create temporary stage
        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        stage_name = 'pytest_export_stage_json'

        try:
            # Create stage
            test_client.stage.create(stage_name, f"file://{tmpdir}/", if_not_exists=True)

            # Export as JSONLINE
            result = test_client.export.to_stage(
                query="SELECT * FROM export_test_data",
                stage_name=stage_name,
                filename='test_export.jsonl',
                format=ExportFormat.JSONLINE,
            )

            assert result is not None

        finally:
            test_client.stage.drop(stage_name, if_exists=True)
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')


@pytest.mark.online
class TestExportEdgeCases:
    """Test export edge cases and special scenarios"""

    def test_export_empty_result_set(self, test_client):
        """Test exporting empty query results"""
        # Create empty test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)

        # Create temporary stage
        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        stage_name = 'pytest_export_stage_empty'

        try:
            # Create stage
            test_client.stage.create(stage_name, f"file://{tmpdir}/", if_not_exists=True)

            # Export empty results
            result = test_client.export.to_stage(
                query="SELECT * FROM export_test_data WHERE id > 1000",
                stage_name=stage_name,
                filename='empty_export.csv',
                format=ExportFormat.CSV,
            )

            assert result is not None

        finally:
            test_client.stage.drop(stage_name, if_exists=True)
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_export_with_special_characters(self, test_client):
        """Test exporting data with special characters"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Test"Quote', 'value': 100})

        # Create temporary stage
        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        stage_name = 'pytest_export_stage_special'

        try:
            # Create stage
            test_client.stage.create(stage_name, f"file://{tmpdir}/", if_not_exists=True)

            # Export with field enclosure to handle special characters
            result = test_client.export.to_stage(
                query="SELECT * FROM export_test_data",
                stage_name=stage_name,
                filename='special_chars.csv',
                format=ExportFormat.CSV,
                fields_enclosed_by='"',
            )

            assert result is not None

        finally:
            test_client.stage.drop(stage_name, if_exists=True)
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')
