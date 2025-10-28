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
Online tests for Export operations (pandas-style interface)

These tests verify the pandas-style to_csv() and to_jsonl() methods.
"""

import pytest
import os
import tempfile
from matrixone import Client
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, select, func

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
class TestExportToCSV:
    """Test pandas-style to_csv() functionality"""

    def test_to_csv_basic(self, test_client):
        """Test basic to_csv export with defaults"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})
        test_client.insert(ExportTestData, {'id': 2, 'name': 'Bob', 'value': 200})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'test_basic.csv')

        try:
            # Export using pandas-style API
            result = test_client.export.to_csv(export_file, "SELECT * FROM export_test_data")

            assert result is not None
            assert os.path.exists(export_file)

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_to_csv_with_custom_sep(self, test_client):
        """Test to_csv with custom separator (TSV)"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'test_tsv.tsv')

        try:
            # Export with tab separator (pandas-style)
            result = test_client.export.to_csv(export_file, "SELECT * FROM export_test_data", sep='\t')

            assert result is not None

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_to_csv_with_pipe_delimiter(self, test_client):
        """Test to_csv with pipe delimiter"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'test_pipe.csv')

        try:
            # Export with pipe separator (pandas-style)
            result = test_client.export.to_csv(export_file, "SELECT * FROM export_test_data", sep='|')

            assert result is not None

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_to_csv_with_quotechar(self, test_client):
        """Test to_csv with field enclosure (quotechar)"""
        # Create and populate test table with special characters
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Data,WithComma', 'value': 100})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'test_quoted.csv')

        try:
            # Export with quotechar (pandas-style)
            result = test_client.export.to_csv(export_file, "SELECT * FROM export_test_data", quotechar='"')

            assert result is not None

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_to_csv_with_sqlalchemy_select(self, test_client):
        """Test to_csv with SQLAlchemy select() statement"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})
        test_client.insert(ExportTestData, {'id': 2, 'name': 'Bob', 'value': 200})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'test_sqlalchemy.csv')

        try:
            # Use SQLAlchemy select() statement
            stmt = select(ExportTestData).where(ExportTestData.value > 100)

            result = test_client.export.to_csv(export_file, stmt, sep=',')

            assert result is not None

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_to_csv_with_matrixone_query(self, test_client):
        """Test to_csv with MatrixOne query builder"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})
        test_client.insert(ExportTestData, {'id': 2, 'name': 'Bob', 'value': 200})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'test_moquery.csv')

        try:
            # Use MatrixOne query builder
            query = test_client.query(ExportTestData).filter(ExportTestData.value > 100)

            result = test_client.export.to_csv(export_file, query)

            assert result is not None

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')


@pytest.mark.online
class TestExportToJSONL:
    """Test pandas-style to_jsonl() functionality"""

    def test_to_jsonl_basic(self, test_client):
        """Test basic to_jsonl export"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'test_basic.jsonl')

        try:
            # Export using pandas-style API
            result = test_client.export.to_jsonl(export_file, "SELECT * FROM export_test_data")

            assert result is not None
            assert os.path.exists(export_file)

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_to_jsonl_with_sqlalchemy(self, test_client):
        """Test to_jsonl with SQLAlchemy select()"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})
        test_client.insert(ExportTestData, {'id': 2, 'name': 'Bob', 'value': 200})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'test_sqlalchemy.jsonl')

        try:
            # Use SQLAlchemy select() statement
            stmt = select(ExportTestData).where(ExportTestData.id > 0)

            result = test_client.export.to_jsonl(export_file, stmt)

            assert result is not None

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')


@pytest.mark.online
class TestExportToStage:
    """Test exporting to stages using stage:// protocol"""

    def test_to_csv_stage_path(self, test_client):
        """Test to_csv with stage:// path"""
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

            # Export using stage:// path (pandas-style)
            result = test_client.export.to_csv(f'stage://{stage_name}/test_export.csv', "SELECT * FROM export_test_data")

            assert result is not None

        finally:
            test_client.stage.drop(stage_name, if_exists=True)
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_to_jsonl_stage_path(self, test_client):
        """Test to_jsonl with stage:// path"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})

        # Create temporary stage
        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        stage_name = 'pytest_jsonl_stage'

        try:
            # Create stage
            test_client.stage.create(stage_name, f"file://{tmpdir}/", if_not_exists=True)

            # Export using stage:// path (pandas-style)
            result = test_client.export.to_jsonl(f'stage://{stage_name}/test_export.jsonl', "SELECT * FROM export_test_data")

            assert result is not None

        finally:
            test_client.stage.drop(stage_name, if_exists=True)
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_to_csv_aggregated_query(self, test_client):
        """Test exporting aggregated query results"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})
        test_client.insert(ExportTestData, {'id': 2, 'name': 'Bob', 'value': 200})
        test_client.insert(ExportTestData, {'id': 3, 'name': 'Alice', 'value': 150})

        # Create temporary stage
        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        stage_name = 'pytest_agg_stage'

        try:
            # Create stage
            test_client.stage.create(stage_name, f"file://{tmpdir}/", if_not_exists=True)

            # Export aggregated data using SQLAlchemy
            stmt = select(ExportTestData.name, func.sum(ExportTestData.value).label('total')).group_by(ExportTestData.name)

            result = test_client.export.to_csv(f'stage://{stage_name}/aggregated.csv', stmt)

            assert result is not None

        finally:
            test_client.stage.drop(stage_name, if_exists=True)
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')


@pytest.mark.online
class TestExportEdgeCases:
    """Test export edge cases and error handling"""

    def test_to_csv_empty_result(self, test_client):
        """Test exporting empty query results"""
        # Create empty test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'empty.csv')

        try:
            # Export empty results
            result = test_client.export.to_csv(export_file, "SELECT * FROM export_test_data WHERE id > 1000")

            assert result is not None

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_to_csv_special_characters(self, test_client):
        """Test exporting data with special characters"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Test"Quote', 'value': 100})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'special_chars.csv')

        try:
            # Export with quotechar to handle special characters
            result = test_client.export.to_csv(export_file, "SELECT * FROM export_test_data", quotechar='"')

            assert result is not None

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')

    def test_to_csv_with_session(self, test_client):
        """Test to_csv using session"""
        # Create and populate test table
        test_client.drop_table('export_test_data')
        test_client.create_table(ExportTestData)
        test_client.insert(ExportTestData, {'id': 1, 'name': 'Alice', 'value': 100})

        tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        export_file = os.path.join(tmpdir, 'session_export.csv')

        try:
            # Export using session
            with test_client.session() as session:
                stmt = select(ExportTestData).where(ExportTestData.id > 0)
                result = session.export.to_csv(export_file, stmt)
                assert result is not None

        finally:
            import shutil

            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
            test_client.drop_table('export_test_data')
