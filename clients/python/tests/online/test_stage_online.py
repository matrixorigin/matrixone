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
Online tests for Stage operations
"""

import pytest
import os
import tempfile
from matrixone import Client, AsyncClient, Stage, JsonDataStructure
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL

# Create Base for model definitions
Base = declarative_base()

# Temporary files directory
TMPFILES_DIR = os.path.join(os.getcwd(), 'tmpfiles')
os.makedirs(TMPFILES_DIR, exist_ok=True)


@pytest.fixture
def test_client():
    """Fixture for test client"""
    client = Client()
    client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')
    yield client
    client.disconnect()


@pytest.mark.online
class TestStageOperations:
    """Tests for Stage management operations"""
    
    def test_create_local_stage(self, test_client):
        """Test creating a local stage with simplified path handling"""
        try:
            # Test with relative path
            stage = test_client.stage.create_local(
                'pytest_local_relative',
                './tmpfiles/',
                comment='Relative path test',
                if_not_exists=True
            )
            
            assert stage.name == 'pytest_local_relative'
            assert 'file://' in stage.url
            assert 'tmpfiles' in stage.url
            
            # Test with absolute path
            import os
            abs_path = os.path.abspath('./tmpfiles/')
            stage2 = test_client.stage.create_local(
                'pytest_local_absolute',
                abs_path,
                if_not_exists=True
            )
            assert 'file://' in stage2.url
            
        finally:
            test_client.stage.drop('pytest_local_relative', if_exists=True)
            test_client.stage.drop('pytest_local_absolute', if_exists=True)
    
    def test_create_file_stage(self, test_client):
        """Test creating a file system stage"""
        try:
            stage = test_client.stage.create(
                'pytest_file_stage',
                f'file://{TMPFILES_DIR}/',
                comment='Pytest file stage',
                if_not_exists=True
            )
            
            assert stage.name == 'pytest_file_stage'
            assert TMPFILES_DIR in stage.url
            assert stage.comment == 'Pytest file stage'
            
        finally:
            test_client.stage.drop('pytest_file_stage', if_exists=True)
    
    def test_list_stages(self, test_client):
        """Test listing stages"""
        # Create a test stage
        test_client.stage.create(
            'pytest_list_test',
            f'file://{TMPFILES_DIR}/',
            if_not_exists=True
        )
        
        try:
            # List from mo_catalog
            stages = test_client.stage.list()
            assert len(stages) > 0
            
            # Show stages
            stages = test_client.stage.show()
            assert len(stages) > 0
            
            # Show with pattern
            stages = test_client.stage.show(like_pattern='pytest%')
            assert any(s.name == 'pytest_list_test' for s in stages)
            
        finally:
            test_client.stage.drop('pytest_list_test', if_exists=True)
    
    def test_get_and_exists(self, test_client):
        """Test get and exists methods"""
        test_client.stage.create(
            'pytest_get_test',
            f'file://{TMPFILES_DIR}/',
            if_not_exists=True
        )
        
        try:
            # Test exists
            assert test_client.stage.exists('pytest_get_test') == True
            assert test_client.stage.exists('nonexistent_stage') == False
            
            # Test get
            stage = test_client.stage.get('pytest_get_test')
            assert stage.name == 'pytest_get_test'
            assert stage.url is not None
            
        finally:
            test_client.stage.drop('pytest_get_test', if_exists=True)
    
    def test_alter_stage(self, test_client):
        """Test modifying stage properties"""
        test_client.stage.create(
            'pytest_alter_test',
            f'file://{TMPFILES_DIR}/',
            if_not_exists=True
        )
        
        try:
            # Alter comment
            test_client.stage.alter('pytest_alter_test', comment='Updated comment')
            stage = test_client.stage.get('pytest_alter_test')
            assert stage.comment == 'Updated comment'
            
            # Alter enable status
            test_client.stage.alter('pytest_alter_test', enable=False)
            test_client.stage.alter('pytest_alter_test', enable=True)
            
        finally:
            test_client.stage.drop('pytest_alter_test', if_exists=True)
    
    def test_load_from_stage(self, test_client):
        """Test loading data from stage"""
        # Create stage
        test_client.stage.create(
            'pytest_load_stage',
            f'file://{TMPFILES_DIR}/',
            if_not_exists=True
        )
        
        try:
            # Define table
            class StageTestUser(Base):
                __tablename__ = 'pytest_stage_users'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
            
            test_client.drop_table('pytest_stage_users')
            test_client.create_table(StageTestUser)
            
            # Create data file
            csv_file = os.path.join(TMPFILES_DIR, 'pytest_users.csv')
            with open(csv_file, 'w') as f:
                f.write('1,Alice\n2,Bob\n3,Charlie\n')
            
            try:
                # Load from stage
                result = test_client.load_data.from_stage_csv(
                    'pytest_load_stage',
                    'pytest_users.csv',
                    StageTestUser
                )
                assert result.affected_rows == 3
                
                # Verify
                count = test_client.query('pytest_stage_users').count()
                assert count == 3
                
            finally:
                os.unlink(csv_file)
                test_client.drop_table('pytest_stage_users')
        
        finally:
            test_client.stage.drop('pytest_load_stage', if_exists=True)
    
    def test_stage_load_data_api(self, test_client):
        """Test stage.load_data convenience API"""
        # Create stage
        test_client.stage.create(
            'pytest_api_stage',
            f'file://{TMPFILES_DIR}/',
            if_not_exists=True
        )
        
        try:
            # Define table
            class StageTestProduct(Base):
                __tablename__ = 'pytest_stage_products'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                price = Column(DECIMAL(10, 2))
            
            test_client.drop_table('pytest_stage_products')
            test_client.create_table(StageTestProduct)
            
            # Create data file
            csv_file = os.path.join(TMPFILES_DIR, 'pytest_products.csv')
            with open(csv_file, 'w') as f:
                f.write('1,Laptop,999.99\n2,Mouse,29.99\n')
            
            try:
                # Get stage and use direct load method
                stage = test_client.stage.get('pytest_api_stage')
                result = stage.load_csv('pytest_products.csv', StageTestProduct)
                assert result.affected_rows == 2
                
                # Verify
                count = test_client.query('pytest_stage_products').count()
                assert count == 2
                
            finally:
                os.unlink(csv_file)
                test_client.drop_table('pytest_stage_products')
        
        finally:
            test_client.stage.drop('pytest_api_stage', if_exists=True)
    
    def test_stage_convenience_methods(self, test_client):
        """Test stage.load_csv/load_json/load_files convenience methods"""
        # Create stage using create_local
        stage = test_client.stage.create_local(
            'pytest_convenience',
            TMPFILES_DIR,
            if_not_exists=True
        )
        
        try:
            # Define table
            class ConvenienceData(Base):
                __tablename__ = 'pytest_convenience_data'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
            
            test_client.drop_table('pytest_convenience_data')
            test_client.create_table(ConvenienceData)
            
            # Create CSV file
            csv_file = os.path.join(TMPFILES_DIR, 'convenience.csv')
            with open(csv_file, 'w') as f:
                f.write('1,Alice\n2,Bob\n')
            
            # Create JSONLINE file
            jsonl_file = os.path.join(TMPFILES_DIR, 'convenience.jsonl')
            with open(jsonl_file, 'w') as f:
                f.write('{"id":3,"name":"Charlie"}\n{"id":4,"name":"Diana"}\n')
            
            try:
                # Test load_csv (direct method on Stage object)
                result = stage.load_csv('convenience.csv', ConvenienceData)
                assert result.affected_rows == 2
                
                # Test load_json (direct method on Stage object)
                result = stage.load_json('convenience.jsonl', ConvenienceData)
                assert result.affected_rows == 2
                
                # Verify total
                count = test_client.query('pytest_convenience_data').count()
                assert count == 4
                
                # Test load_files (batch - direct method on Stage object)
                test_client.execute('TRUNCATE TABLE pytest_convenience_data')
                results = stage.load_files({
                    'convenience.csv': ConvenienceData,
                    'convenience.jsonl': ConvenienceData
                })
                
                assert len(results) == 2
                assert results['convenience.csv'].affected_rows == 2
                assert results['convenience.jsonl'].affected_rows == 2
                
            finally:
                os.unlink(csv_file)
                os.unlink(jsonl_file)
                test_client.drop_table('pytest_convenience_data')
        
        finally:
            test_client.stage.drop('pytest_convenience', if_exists=True)
    
    def test_stage_transaction(self, test_client):
        """Test stage operations within transaction"""
        # Note: Stage creation must happen outside transaction as it's a DDL operation
        # and may not be visible within the same transaction
        test_client.stage.create(
            'pytest_tx_stage',
            f'file://{TMPFILES_DIR}/',
            if_not_exists=True
        )
        
        try:
            # Define table
            class TxStageData(Base):
                __tablename__ = 'pytest_tx_stage_data'
                id = Column(Integer, primary_key=True)
                value = Column(String(100))
            
            test_client.drop_table('pytest_tx_stage_data')
            test_client.create_table(TxStageData)
            
            # Create data file
            csv_file = os.path.join(TMPFILES_DIR, 'pytest_tx_data.csv')
            with open(csv_file, 'w') as f:
                f.write('1,value1\n2,value2\n')
            
            try:
                # Load data from stage in transaction
                with test_client.transaction() as tx:
                    result = tx.load_data.from_stage_csv(
                        'pytest_tx_stage',
                        'pytest_tx_data.csv',
                        TxStageData
                    )
                    assert result.affected_rows == 2
                
                # Verify
                count = test_client.query('pytest_tx_stage_data').count()
                assert count == 2
                
            finally:
                os.unlink(csv_file)
                test_client.drop_table('pytest_tx_stage_data')
        
        finally:
            test_client.stage.drop('pytest_tx_stage', if_exists=True)


@pytest.mark.online
@pytest.mark.asyncio
class TestAsyncStageOperations:
    """Async tests for Stage operations"""
    
    @pytest.mark.asyncio
    async def test_async_create_and_list(self):
        """Test async stage creation and listing"""
        client = AsyncClient()
        await client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')
        
        try:
            # Create stage
            stage = await client.stage.create(
                'pytest_async_stage',
                f'file://{TMPFILES_DIR}/',
                comment='Async test stage',
                if_not_exists=True
            )
            
            assert stage.name == 'pytest_async_stage'
            
            # List stages
            stages = await client.stage.list()
            assert len(stages) > 0
            
            # Check exists
            exists = await client.stage.exists('pytest_async_stage')
            assert exists == True
            
        finally:
            await client.stage.drop('pytest_async_stage', if_exists=True)
            await client.disconnect()
    
    @pytest.mark.asyncio
    async def test_async_load_from_stage(self):
        """Test async loading data from stage"""
        client = AsyncClient()
        await client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')
        
        try:
            # Create stage
            await client.stage.create(
                'pytest_async_load',
                f'file://{TMPFILES_DIR}/',
                if_not_exists=True
            )
            
            # Define table
            class AsyncStageUser(Base):
                __tablename__ = 'pytest_async_stage_users'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
            
            await client.drop_table('pytest_async_stage_users')
            await client.create_table(AsyncStageUser)
            
            # Create data file
            csv_file = os.path.join(TMPFILES_DIR, 'pytest_async_users.csv')
            with open(csv_file, 'w') as f:
                f.write('1,Alice\n2,Bob\n3,Charlie\n')
            
            try:
                # Load from stage
                result = await client.load_data.from_stage_csv(
                    'pytest_async_load',
                    'pytest_async_users.csv',
                    AsyncStageUser
                )
                assert result.affected_rows == 3
                
                # Verify
                count_result = await client.execute('SELECT COUNT(*) FROM pytest_async_stage_users')
                count = count_result.rows[0][0]
                assert count == 3
                
            finally:
                os.unlink(csv_file)
                await client.drop_table('pytest_async_stage_users')
        
        finally:
            await client.stage.drop('pytest_async_load', if_exists=True)
            await client.disconnect()

