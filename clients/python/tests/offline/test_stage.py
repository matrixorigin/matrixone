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
Unit tests for MatrixOne StageManager functionality - SQL consistency tests
"""

import unittest
from unittest.mock import Mock, AsyncMock
from matrixone.stage import StageManager, AsyncStageManager


class TestStageSQLConsistency(unittest.TestCase):
    """Test SQL generation consistency across sync/async/session interfaces"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = Mock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"

        self.session = Mock()
        self.session.client = self.client
        self.session._escape_identifier = lambda x: f"`{x}`"
        self.session._escape_string = lambda x: f"'{x}'"

        # Create managers
        self.sync_manager = StageManager(self.client)
        self.session_manager = StageManager(self.client, executor=self.session)

    def test_create_stage_basic_sql_consistency(self):
        """Test that create generates identical SQL across client and session"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        # Test basic stage creation
        self.sync_manager.create('test_stage', 'file:///tmp/data/')
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.create('test_stage', 'file:///tmp/data/')
        session_sql = self.session.execute.call_args[0][0]

        # Verify SQL is identical
        expected_sql = "CREATE STAGE test_stage URL='file:///tmp/data/'"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)
        self.assertEqual(sync_sql, session_sql)

    def test_create_stage_with_comment_sql_consistency(self):
        """Test create stage with comment"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        self.sync_manager.create('test_stage', 'file:///tmp/data/', comment='Test comment')
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.create('test_stage', 'file:///tmp/data/', comment='Test comment')
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "CREATE STAGE test_stage URL='file:///tmp/data/' COMMENT='Test comment'"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_create_stage_with_credentials_sql_consistency(self):
        """Test create stage with credentials (e.g., S3)"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        credentials = {'AWS_KEY_ID': 'key123', 'AWS_SECRET_KEY': 'secret456'}
        self.sync_manager.create('s3_stage', 's3://bucket/path/', credentials=credentials)
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.create('s3_stage', 's3://bucket/path/', credentials=credentials)
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = (
            "CREATE STAGE s3_stage URL='s3://bucket/path/' CREDENTIALS={'AWS_KEY_ID'='key123', 'AWS_SECRET_KEY'='secret456'}"
        )
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_drop_stage_sql_consistency(self):
        """Test that drop generates identical SQL"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        self.sync_manager.drop('test_stage')
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.drop('test_stage')
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "DROP STAGE test_stage"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_drop_stage_if_exists_sql_consistency(self):
        """Test DROP STAGE IF EXISTS SQL"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        self.sync_manager.drop('test_stage', if_exists=True)
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.drop('test_stage', if_exists=True)
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "DROP STAGE IF EXISTS test_stage"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_alter_stage_url_sql_consistency(self):
        """Test ALTER STAGE SET URL"""
        # Mock to return stage for get() call
        mock_result = Mock()
        mock_result.rows = [['test_stage', 'file:///new/path/', 'enabled', None]]
        self.client.execute = Mock(return_value=mock_result)
        self.session.execute = Mock(return_value=mock_result)

        self.sync_manager.alter('test_stage', url='file:///new/path/')
        sync_sql = self.client.execute.call_args_list[0][0][0]  # First call is ALTER

        self.session_manager.alter('test_stage', url='file:///new/path/')
        session_sql = self.session.execute.call_args_list[0][0][0]  # First call is ALTER

        expected_sql = "ALTER STAGE test_stage SET URL='file:///new/path/'"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_alter_stage_enable_sql_consistency(self):
        """Test ALTER STAGE SET ENABLE"""
        # Mock to return stage for get() call
        mock_result = Mock()
        mock_result.rows = [['test_stage', 'file:///tmp/', 'disabled', None]]
        self.client.execute = Mock(return_value=mock_result)
        self.session.execute = Mock(return_value=mock_result)

        self.sync_manager.alter('test_stage', enable=False)
        sync_sql = self.client.execute.call_args_list[0][0][0]  # First call is ALTER

        self.session_manager.alter('test_stage', enable=False)
        session_sql = self.session.execute.call_args_list[0][0][0]  # First call is ALTER

        expected_sql = "ALTER STAGE test_stage SET ENABLE=FALSE"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_alter_stage_multiple_sets_sql_consistency(self):
        """Test ALTER STAGE with multiple SET clauses"""
        # Mock to return stage for get() call
        mock_result = Mock()
        mock_result.rows = [['test_stage', 'file:///new/path/', 'enabled', 'Updated']]
        self.client.execute = Mock(return_value=mock_result)
        self.session.execute = Mock(return_value=mock_result)

        self.sync_manager.alter('test_stage', url='file:///new/path/', enable=True, comment='Updated')
        sync_sql = self.client.execute.call_args_list[0][0][0]  # First call is ALTER

        self.session_manager.alter('test_stage', url='file:///new/path/', enable=True, comment='Updated')
        session_sql = self.session.execute.call_args_list[0][0][0]  # First call is ALTER

        expected_sql = "ALTER STAGE test_stage SET URL='file:///new/path/' COMMENT='Updated' ENABLE=TRUE"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_list_stages_sql_consistency(self):
        """Test that list generates identical SQL"""
        self.client.execute = Mock(return_value=Mock(rows=[]))
        self.session.execute = Mock(return_value=Mock(rows=[]))

        self.sync_manager.list()
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.list()
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "SELECT stage_name, url, stage_status, comment, created_time FROM mo_catalog.mo_stages"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_get_stage_sql_consistency(self):
        """Test that get generates identical SQL"""
        self.client.execute = Mock(return_value=Mock(rows=[]))
        self.session.execute = Mock(return_value=Mock(rows=[]))

        try:
            self.sync_manager.get('test_stage')
        except Exception:
            pass
        sync_sql = self.client.execute.call_args[0][0]

        try:
            self.session_manager.get('test_stage')
        except Exception:
            pass
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "SELECT stage_name, url, stage_status, comment, created_time FROM mo_catalog.mo_stages WHERE stage_name = 'test_stage'"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)


class TestAsyncStageSQLConsistency(unittest.IsolatedAsyncioTestCase):
    """Test SQL generation consistency for async versions"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = AsyncMock()
        self.client._escape_identifier = lambda x: f"`{x}`"
        self.client._escape_string = lambda x: f"'{x}'"

        self.session = AsyncMock()
        self.session.client = self.client
        self.session._escape_identifier = lambda x: f"`{x}`"
        self.session._escape_string = lambda x: f"'{x}'"

        self.async_manager = AsyncStageManager(self.client)
        self.async_session_manager = AsyncStageManager(self.client, executor=self.session)

    async def test_async_create_stage_sql_consistency(self):
        """Test async create stage SQL"""
        self.client.execute = AsyncMock()
        self.session.execute = AsyncMock()

        await self.async_manager.create('test_stage', 'file:///tmp/data/')
        async_sql = self.client.execute.call_args[0][0]

        await self.async_session_manager.create('test_stage', 'file:///tmp/data/')
        async_session_sql = self.session.execute.call_args[0][0]

        expected_sql = "CREATE STAGE test_stage URL='file:///tmp/data/'"
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)

    async def test_async_create_stage_with_credentials_sql_consistency(self):
        """Test async create stage with credentials"""
        self.client.execute = AsyncMock()
        self.session.execute = AsyncMock()

        credentials = {'AWS_KEY_ID': 'key123', 'AWS_SECRET_KEY': 'secret456', 'AWS_REGION': 'us-east-1'}
        await self.async_manager.create('s3_stage', 's3://bucket/path/', credentials=credentials)
        async_sql = self.client.execute.call_args[0][0]

        await self.async_session_manager.create('s3_stage', 's3://bucket/path/', credentials=credentials)
        async_session_sql = self.session.execute.call_args[0][0]

        expected_sql = (
            "CREATE STAGE s3_stage URL='s3://bucket/path/' "
            "CREDENTIALS={'AWS_KEY_ID'='key123', 'AWS_SECRET_KEY'='secret456', 'AWS_REGION'='us-east-1'}"
        )
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)

    async def test_async_drop_stage_sql_consistency(self):
        """Test async drop stage SQL"""
        self.client.execute = AsyncMock()
        self.session.execute = AsyncMock()

        await self.async_manager.drop('test_stage')
        async_sql = self.client.execute.call_args[0][0]

        await self.async_session_manager.drop('test_stage')
        async_session_sql = self.session.execute.call_args[0][0]

        expected_sql = "DROP STAGE test_stage"
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)

    async def test_async_drop_stage_if_exists_sql_consistency(self):
        """Test async DROP STAGE IF EXISTS SQL"""
        self.client.execute = AsyncMock()
        self.session.execute = AsyncMock()

        await self.async_manager.drop('test_stage', if_exists=True)
        async_sql = self.client.execute.call_args[0][0]

        await self.async_session_manager.drop('test_stage', if_exists=True)
        async_session_sql = self.session.execute.call_args[0][0]

        expected_sql = "DROP STAGE IF EXISTS test_stage"
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)

    async def test_async_alter_stage_sql_consistency(self):
        """Test async ALTER STAGE SQL"""
        # Mock to return stage for get() call
        mock_result = AsyncMock()
        mock_result.rows = [['test_stage', 'file:///new/path/', 'disabled', None, None]]
        self.client.execute = AsyncMock(return_value=mock_result)
        self.session.execute = AsyncMock(return_value=mock_result)

        await self.async_manager.alter('test_stage', url='file:///new/path/', enable=False)
        async_sql = self.client.execute.call_args_list[0][0][0]  # First call is ALTER

        await self.async_session_manager.alter('test_stage', url='file:///new/path/', enable=False)
        async_session_sql = self.session.execute.call_args_list[0][0][0]  # First call is ALTER

        expected_sql = "ALTER STAGE test_stage SET URL='file:///new/path/' ENABLE=FALSE"
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)

    async def test_async_list_stages_sql_consistency(self):
        """Test async list stages SQL"""
        self.client.execute = AsyncMock(return_value=AsyncMock(rows=[]))
        self.session.execute = AsyncMock(return_value=AsyncMock(rows=[]))

        await self.async_manager.list()
        async_sql = self.client.execute.call_args[0][0]

        await self.async_session_manager.list()
        async_session_sql = self.session.execute.call_args[0][0]

        expected_sql = "SELECT stage_name, url, stage_status, comment, created_time FROM mo_catalog.mo_stages"
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)


if __name__ == '__main__':
    unittest.main()
