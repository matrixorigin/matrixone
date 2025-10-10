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
Test configuration validation for online tests
"""

import pytest
import os
from .test_config import online_config


class TestConfigValidation:
    """Test that the configuration system works correctly"""

    def test_config_has_all_required_attributes(self):
        """Test that config has all required attributes"""
        assert hasattr(online_config, 'host')
        assert hasattr(online_config, 'port')
        assert hasattr(online_config, 'user')
        assert hasattr(online_config, 'password')
        assert hasattr(online_config, 'database')
        assert hasattr(online_config, 'test_database')
        assert hasattr(online_config, 'sys_database')
        assert hasattr(online_config, 'test_account')
        assert hasattr(online_config, 'table_prefix')
        assert hasattr(online_config, 'snapshot_prefix')
        assert hasattr(online_config, 'vector_dimensions')
        assert hasattr(online_config, 'vector_test_data_size')

    def test_config_methods_work(self):
        """Test that config methods work correctly"""
        # Test connection params
        host, port, user, password, database = online_config.get_connection_params()
        assert isinstance(host, str)
        assert isinstance(port, int)
        assert isinstance(user, str)
        assert isinstance(password, str)
        assert isinstance(database, str)

        # Test database names
        test_db = online_config.get_test_database()
        sys_db = online_config.get_sys_database()
        assert isinstance(test_db, str)
        assert isinstance(sys_db, str)

        # Test account
        account = online_config.get_test_account()
        assert isinstance(account, str)

        # Test table name generation
        table_name = online_config.get_table_name("test_table")
        assert table_name == f"{online_config.table_prefix}test_table"

        # Test snapshot name generation
        snapshot_name = online_config.get_snapshot_name("test_snapshot")
        assert snapshot_name == f"{online_config.snapshot_prefix}_test_snapshot"

        # Test vector settings
        dimensions = online_config.get_vector_dimensions()
        data_size = online_config.get_vector_test_data_size()
        assert isinstance(dimensions, int)
        assert isinstance(data_size, int)

    def test_config_respects_environment_variables(self):
        """Test that config respects environment variables"""
        # Save original values
        original_host = online_config.host
        original_port = online_config.port
        original_user = online_config.user
        original_password = online_config.password
        original_database = online_config.database

        try:
            # Set test environment variables
            os.environ['MATRIXONE_HOST'] = 'test_host'
            os.environ['MATRIXONE_PORT'] = '9999'
            os.environ['MATRIXONE_USER'] = 'test_user'
            os.environ['MATRIXONE_PASSWORD'] = 'test_password'
            os.environ['MATRIXONE_DATABASE'] = 'test_db'

            # Create new config instance to test env vars
            from .test_config import OnlineTestConfig

            test_config = OnlineTestConfig()

            assert test_config.host == 'test_host'
            assert test_config.port == 9999
            assert test_config.user == 'test_user'
            assert test_config.password == 'test_password'
            assert test_config.database == 'test_db'

        finally:
            # Clean up environment variables
            for key in [
                'MATRIXONE_HOST',
                'MATRIXONE_PORT',
                'MATRIXONE_USER',
                'MATRIXONE_PASSWORD',
                'MATRIXONE_DATABASE',
            ]:
                if key in os.environ:
                    del os.environ[key]

    def test_config_string_representation(self):
        """Test config string representation"""
        config_str = str(online_config)
        assert 'OnlineTestConfig' in config_str
        assert online_config.host in config_str
        assert str(online_config.port) in config_str
        assert online_config.user in config_str
        assert online_config.database in config_str
