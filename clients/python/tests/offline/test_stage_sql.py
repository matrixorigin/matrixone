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
Offline tests for Stage SQL generation.

These tests verify that the Stage module generates correct SQL statements
without requiring a database connection.
"""

import pytest
from unittest.mock import Mock, MagicMock
from matrixone.stage import StageManager, Stage


class TestStageSQLGeneration:
    """Test SQL generation for Stage operations"""

    def setup_method(self):
        """Setup mock client for each test"""
        self.mock_client = Mock()
        self.mock_client.execute = Mock()
        self.stage_manager = StageManager(self.mock_client)

    def test_create_stage_basic(self):
        """Test CREATE STAGE basic SQL generation"""
        self.stage_manager.create('my_stage', 'file:///data/')

        expected_sql = "CREATE STAGE my_stage URL='file:///data/'"
        self.mock_client.execute.assert_called_once()
        actual_sql = self.mock_client.execute.call_args[0][0]
        assert actual_sql == expected_sql

    def test_create_stage_if_not_exists(self):
        """Test CREATE STAGE IF NOT EXISTS"""
        self.stage_manager.create('my_stage', 'file:///data/', if_not_exists=True)

        expected_sql = "CREATE STAGE IF NOT EXISTS my_stage URL='file:///data/'"
        actual_sql = self.mock_client.execute.call_args[0][0]
        assert actual_sql == expected_sql

    def test_create_stage_with_comment(self):
        """Test CREATE STAGE with COMMENT"""
        self.stage_manager.create('my_stage', 'file:///data/', comment='test stage')

        expected_sql = "CREATE STAGE my_stage URL='file:///data/' COMMENT='test stage'"
        actual_sql = self.mock_client.execute.call_args[0][0]
        assert actual_sql == expected_sql

    def test_create_stage_with_credentials(self):
        """Test CREATE STAGE with CREDENTIALS"""
        self.stage_manager.create(
            'aws_stage',
            's3://bucket/path/',
            credentials={'AWS_KEY_ID': 'test_key', 'AWS_SECRET_KEY': 'test_secret', 'AWS_REGION': 'us-east-1'},
        )

        actual_sql = self.mock_client.execute.call_args[0][0]
        assert actual_sql.startswith("CREATE STAGE aws_stage URL='s3://bucket/path/'")
        assert "CREDENTIALS={" in actual_sql
        assert "'AWS_KEY_ID'='test_key'" in actual_sql
        assert "'AWS_SECRET_KEY'='test_secret'" in actual_sql
        assert "'AWS_REGION'='us-east-1'" in actual_sql

    def test_create_stage_full(self):
        """Test CREATE STAGE with all options"""
        self.stage_manager.create(
            'full_stage', 's3://bucket/path/', credentials={'AWS_KEY_ID': 'key'}, comment='full test', if_not_exists=True
        )

        actual_sql = self.mock_client.execute.call_args[0][0]
        assert "CREATE STAGE IF NOT EXISTS full_stage" in actual_sql
        assert "URL='s3://bucket/path/'" in actual_sql
        assert "CREDENTIALS={" in actual_sql
        assert "COMMENT='full test'" in actual_sql

    def test_alter_stage_url(self):
        """Test ALTER STAGE SET URL"""
        # Mock get() to return a stage
        mock_stage = Stage(name='my_stage', url='file:///old/', _client=self.mock_client)
        self.mock_client.execute.return_value = Mock(rows=[['my_stage', 'file:///new/', 'enabled', None, None]])

        self.stage_manager.alter('my_stage', url='file:///new/')

        # First call is ALTER, second is SELECT (from get())
        alter_sql = self.mock_client.execute.call_args_list[0][0][0]
        assert alter_sql == "ALTER STAGE my_stage SET URL='file:///new/'"

    def test_alter_stage_comment(self):
        """Test ALTER STAGE SET COMMENT"""
        self.mock_client.execute.return_value = Mock(rows=[['my_stage', 'file:///data/', 'enabled', 'new comment', None]])

        self.stage_manager.alter('my_stage', comment='new comment')

        alter_sql = self.mock_client.execute.call_args_list[0][0][0]
        assert alter_sql == "ALTER STAGE my_stage SET COMMENT='new comment'"

    def test_alter_stage_enable(self):
        """Test ALTER STAGE SET ENABLE"""
        self.mock_client.execute.return_value = Mock(rows=[['my_stage', 'file:///data/', 'disabled', None, None]])

        self.stage_manager.alter('my_stage', enable=False)

        alter_sql = self.mock_client.execute.call_args_list[0][0][0]
        assert alter_sql == "ALTER STAGE my_stage SET ENABLE=FALSE"

    def test_alter_stage_credentials(self):
        """Test ALTER STAGE SET CREDENTIALS"""
        self.mock_client.execute.return_value = Mock(rows=[['my_stage', 's3://bucket/', 'enabled', None, None]])

        self.stage_manager.alter('my_stage', credentials={'AWS_KEY_ID': 'new_key'})

        alter_sql = self.mock_client.execute.call_args_list[0][0][0]
        assert "ALTER STAGE my_stage SET" in alter_sql
        assert "CREDENTIALS={" in alter_sql
        assert "'AWS_KEY_ID'='new_key'" in alter_sql

    def test_alter_stage_multiple_options(self):
        """Test ALTER STAGE with multiple SET clauses"""
        self.mock_client.execute.return_value = Mock(rows=[['my_stage', 'file:///new/', 'enabled', 'updated', None]])

        self.stage_manager.alter('my_stage', url='file:///new/', comment='updated', enable=True)

        alter_sql = self.mock_client.execute.call_args_list[0][0][0]
        assert "ALTER STAGE my_stage SET" in alter_sql
        assert "URL='file:///new/'" in alter_sql
        assert "COMMENT='updated'" in alter_sql
        assert "ENABLE=TRUE" in alter_sql

    def test_alter_stage_if_exists(self):
        """Test ALTER STAGE IF EXISTS"""
        self.mock_client.execute.return_value = Mock(rows=[['my_stage', 'file:///data/', 'enabled', None, None]])

        self.stage_manager.alter('my_stage', comment='test', if_exists=True)

        alter_sql = self.mock_client.execute.call_args_list[0][0][0]
        assert alter_sql == "ALTER STAGE IF EXISTS my_stage SET COMMENT='test'"

    def test_drop_stage_basic(self):
        """Test DROP STAGE basic SQL"""
        self.stage_manager.drop('my_stage')

        expected_sql = "DROP STAGE my_stage"
        actual_sql = self.mock_client.execute.call_args[0][0]
        assert actual_sql == expected_sql

    def test_drop_stage_if_exists(self):
        """Test DROP STAGE IF EXISTS"""
        self.stage_manager.drop('my_stage', if_exists=True)

        expected_sql = "DROP STAGE IF EXISTS my_stage"
        actual_sql = self.mock_client.execute.call_args[0][0]
        assert actual_sql == expected_sql

    def test_show_stages_basic(self):
        """Test SHOW STAGES basic SQL"""
        self.mock_client.execute.return_value = Mock(rows=[])

        self.stage_manager.show()

        expected_sql = "SHOW STAGES"
        actual_sql = self.mock_client.execute.call_args[0][0]
        assert actual_sql == expected_sql

    def test_show_stages_like(self):
        """Test SHOW STAGES LIKE pattern"""
        self.mock_client.execute.return_value = Mock(rows=[])

        self.stage_manager.show(like_pattern='my_stage%')

        expected_sql = "SHOW STAGES LIKE 'my_stage%'"
        actual_sql = self.mock_client.execute.call_args[0][0]
        assert actual_sql == expected_sql

    def test_list_stages_sql(self):
        """Test LIST stages SQL query"""
        self.mock_client.execute.return_value = Mock(rows=[])

        self.stage_manager.list()

        expected_sql = "SELECT stage_name, url, stage_status, comment, created_time FROM mo_catalog.mo_stages"
        actual_sql = self.mock_client.execute.call_args[0][0]
        assert actual_sql == expected_sql

    def test_get_stage_sql(self):
        """Test GET stage SQL query"""
        self.mock_client.execute.return_value = Mock(rows=[['my_stage', 'file:///data/', 'enabled', 'test', None]])

        self.stage_manager.get('my_stage')

        expected_sql = "SELECT stage_name, url, stage_status, comment, created_time FROM mo_catalog.mo_stages WHERE stage_name = 'my_stage'"
        actual_sql = self.mock_client.execute.call_args[0][0]
        assert actual_sql == expected_sql

    def test_create_local_path_expansion(self):
        """Test create_local path expansion and URL generation"""
        import os

        # Test relative path
        self.stage_manager.create_local('local_stage', './data/')
        actual_sql = self.mock_client.execute.call_args[0][0]

        # Should contain absolute path with file:// prefix
        assert actual_sql.startswith("CREATE STAGE local_stage URL='file://")
        assert actual_sql.endswith("/data/'")

    def test_create_local_home_expansion(self):
        """Test create_local with ~ expansion"""
        self.stage_manager.create_local('home_stage', '~/data/')
        actual_sql = self.mock_client.execute.call_args[0][0]

        # Should expand ~ to home directory
        assert actual_sql.startswith("CREATE STAGE home_stage URL='file://")
        assert '~' not in actual_sql  # ~ should be expanded

    def test_create_s3_url_generation(self):
        """Test create_s3 URL generation"""
        self.stage_manager.create_s3('s3_stage', 'my-bucket/data/', aws_key='key', aws_secret='secret')

        actual_sql = self.mock_client.execute.call_args[0][0]

        # Should add s3:// prefix
        assert "URL='s3://my-bucket/data/'" in actual_sql
        assert "CREDENTIALS={" in actual_sql
        assert "'AWS_KEY_ID'='key'" in actual_sql
        assert "'AWS_SECRET_KEY'='secret'" in actual_sql

    def test_create_s3_with_region(self):
        """Test create_s3 with AWS region"""
        self.stage_manager.create_s3(
            's3_stage', 's3://bucket/path/', aws_key='key', aws_secret='secret', aws_region='us-west-2'
        )

        actual_sql = self.mock_client.execute.call_args[0][0]
        assert "'AWS_REGION'='us-west-2'" in actual_sql

    def test_create_s3_skip_prefix_if_present(self):
        """Test create_s3 doesn't double-add s3:// prefix"""
        self.stage_manager.create_s3('s3_stage', 's3://my-bucket/data/', aws_key='key', aws_secret='secret')

        actual_sql = self.mock_client.execute.call_args[0][0]
        # Should not be s3://s3://
        assert "URL='s3://my-bucket/data/'" in actual_sql
        assert "s3://s3://" not in actual_sql


class TestStageObjectMethods:
    """Test Stage object's direct load methods"""

    def test_stage_has_load_methods(self):
        """Test Stage object has all load methods"""
        stage = Stage(name='test_stage', url='file:///data/')

        # Verify all load methods exist
        assert hasattr(stage, 'load_csv')
        assert hasattr(stage, 'load_tsv')
        assert hasattr(stage, 'load_json')
        assert hasattr(stage, 'load_parquet')
        assert hasattr(stage, 'load_files')

        # Verify they are callable
        assert callable(stage.load_csv)
        assert callable(stage.load_tsv)
        assert callable(stage.load_json)
        assert callable(stage.load_parquet)
        assert callable(stage.load_files)

    def test_load_csv_without_client(self):
        """Test load_csv raises error when no client associated"""
        stage = Stage(name='orphan', url='file:///data/')

        with pytest.raises(ValueError, match="must be associated with a client"):
            stage.load_csv('file.csv', 'table')

    def test_load_files_without_client(self):
        """Test load_files raises error when no client associated"""
        stage = Stage(name='orphan', url='file:///data/')

        with pytest.raises(ValueError, match="must be associated with a client"):
            stage.load_files({'file.csv': 'table'})


class TestStageManagerConvenienceMethods:
    """Test StageManager convenience methods"""

    def setup_method(self):
        """Setup mock client"""
        self.mock_client = Mock()
        self.mock_client.execute = Mock()
        self.stage_manager = StageManager(self.mock_client)

    def test_create_local_relative_path(self):
        """Test create_local with relative path"""
        import os

        self.stage_manager.create_local('local', './data/')

        actual_sql = self.mock_client.execute.call_args[0][0]

        # Should convert to absolute path
        assert actual_sql.startswith("CREATE STAGE local URL='file://")
        assert "/data/'" in actual_sql
        # Should not contain ./
        assert "'file://./data/'" not in actual_sql

    def test_create_local_home_expansion(self):
        """Test create_local expands ~ to home directory"""
        import os

        self.stage_manager.create_local('home', '~/mydata/')

        actual_sql = self.mock_client.execute.call_args[0][0]

        # Should expand ~
        home_dir = os.path.expanduser('~')
        assert f"URL='file://{home_dir}/mydata/'" in actual_sql

    def test_create_local_adds_trailing_slash(self):
        """Test create_local adds trailing slash if missing"""
        self.stage_manager.create_local('local', '/data')

        actual_sql = self.mock_client.execute.call_args[0][0]
        assert "URL='file:///data/'" in actual_sql  # Note the trailing /

    def test_create_s3_adds_prefix(self):
        """Test create_s3 adds s3:// prefix if missing"""
        self.stage_manager.create_s3('s3', 'my-bucket/path/', aws_key='key', aws_secret='secret')

        actual_sql = self.mock_client.execute.call_args[0][0]
        assert "URL='s3://my-bucket/path/'" in actual_sql

    def test_create_s3_preserves_prefix(self):
        """Test create_s3 doesn't double-add s3:// prefix"""
        self.stage_manager.create_s3('s3', 's3://my-bucket/path/', aws_key='key', aws_secret='secret')

        actual_sql = self.mock_client.execute.call_args[0][0]
        assert "URL='s3://my-bucket/path/'" in actual_sql
        assert "s3://s3://" not in actual_sql

    def test_create_s3_env_fallback(self):
        """Test create_s3 can work without explicit credentials"""
        import os

        # Set environment variables
        old_key = os.environ.get('AWS_ACCESS_KEY_ID')
        old_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')

        try:
            os.environ['AWS_ACCESS_KEY_ID'] = 'env_key'
            os.environ['AWS_SECRET_ACCESS_KEY'] = 'env_secret'

            self.stage_manager.create_s3('s3', 'my-bucket/path/')

            actual_sql = self.mock_client.execute.call_args[0][0]
            assert "'AWS_KEY_ID'='env_key'" in actual_sql
            assert "'AWS_SECRET_KEY'='env_secret'" in actual_sql
        finally:
            # Restore environment
            if old_key:
                os.environ['AWS_ACCESS_KEY_ID'] = old_key
            else:
                os.environ.pop('AWS_ACCESS_KEY_ID', None)
            if old_secret:
                os.environ['AWS_SECRET_ACCESS_KEY'] = old_secret
            else:
                os.environ.pop('AWS_SECRET_ACCESS_KEY', None)
