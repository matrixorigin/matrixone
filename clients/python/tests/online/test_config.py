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
Configuration for online tests
"""

import os
from typing import Optional, Tuple


class OnlineTestConfig:
    """Configuration for online tests"""

    def __init__(self):
        # Connection parameters
        self.host = os.getenv('MATRIXONE_HOST', '127.0.0.1')
        self.port = int(os.getenv('MATRIXONE_PORT', '6001'))
        self.user = os.getenv('MATRIXONE_USER', 'root')
        self.password = os.getenv('MATRIXONE_PASSWORD', '111')
        self.database = os.getenv('MATRIXONE_DATABASE', 'test')

        # Test database names (can be overridden for different test environments)
        self.test_database = os.getenv('MATRIXONE_TEST_DATABASE', self.database)
        self.sys_database = os.getenv('MATRIXONE_SYS_DATABASE', 'sys')

        # Test account (for multi-tenant scenarios)
        self.test_account = os.getenv('MATRIXONE_TEST_ACCOUNT', 'sys')

        # Test table prefixes (to avoid conflicts)
        self.table_prefix = os.getenv('MATRIXONE_TABLE_PREFIX', 'test_')

        # Snapshot and backup settings
        self.snapshot_prefix = os.getenv('MATRIXONE_SNAPSHOT_PREFIX', 'testsnapshot')

        # Vector test settings
        self.vector_dimensions = int(os.getenv('MATRIXONE_VECTOR_DIMENSIONS', '64'))
        self.vector_test_data_size = int(os.getenv('MATRIXONE_VECTOR_TEST_SIZE', '100'))

    def get_connection_params(self) -> Tuple[str, int, str, str, str]:
        """Get connection parameters as tuple"""
        return self.host, self.port, self.user, self.password, self.database

    def get_test_database(self) -> str:
        """Get test database name"""
        return self.test_database

    def get_sys_database(self) -> str:
        """Get system database name"""
        return self.sys_database

    def get_test_account(self) -> str:
        """Get test account name"""
        return self.test_account

    def get_table_name(self, base_name: str) -> str:
        """Get full table name with prefix"""
        return f"{self.table_prefix}{base_name}"

    def get_snapshot_name(self, base_name: str = None) -> str:
        """Get snapshot name with prefix"""
        if base_name:
            return f"{self.snapshot_prefix}_{base_name}"
        return self.snapshot_prefix

    def get_vector_dimensions(self) -> int:
        """Get vector dimensions for tests"""
        return self.vector_dimensions

    def get_vector_test_data_size(self) -> int:
        """Get vector test data size"""
        return self.vector_test_data_size

    def __str__(self):
        return f"OnlineTestConfig(host={self.host}, port={self.port}, user={self.user}, database={self.database}, test_database={self.test_database})"


# Global configuration instance
online_config = OnlineTestConfig()
