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
Unit tests for MatrixOne LoadDataManager functionality - SQL consistency tests
"""

import unittest
from unittest.mock import Mock, AsyncMock
from matrixone.load_data import LoadDataManager, AsyncLoadDataManager


class TestLoadDataSQLConsistency(unittest.TestCase):
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
        self.sync_manager = LoadDataManager(self.client)
        self.session_manager = LoadDataManager(self.client, executor=self.session)

    def test_from_csv_basic_sql_consistency(self):
        """Test that from_csv generates identical SQL across client and session"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        # Test basic CSV load
        self.sync_manager.from_csv('/path/to/data.csv', 'test_table')
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.from_csv('/path/to/data.csv', 'test_table')
        session_sql = self.session.execute.call_args[0][0]

        # Verify SQL is identical
        expected_sql = "LOAD DATA INFILE '/path/to/data.csv' INTO TABLE test_table FIELDS TERMINATED BY ','"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)
        self.assertEqual(sync_sql, session_sql)

    def test_from_csv_with_delimiter_sql_consistency(self):
        """Test from_csv with custom delimiter"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        self.sync_manager.from_csv('/path/to/data.txt', 'test_table', delimiter='|')
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.from_csv('/path/to/data.txt', 'test_table', delimiter='|')
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "LOAD DATA INFILE '/path/to/data.txt' INTO TABLE test_table FIELDS TERMINATED BY '|'"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_from_csv_with_enclosed_by_sql_consistency(self):
        """Test from_csv with enclosed_by option"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        self.sync_manager.from_csv('/path/to/data.csv', 'test_table', enclosed_by='"')
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.from_csv('/path/to/data.csv', 'test_table', enclosed_by='"')
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "LOAD DATA INFILE '/path/to/data.csv' INTO TABLE test_table FIELDS TERMINATED BY ',' ENCLOSED BY '\"'"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_from_csv_with_ignore_lines_sql_consistency(self):
        """Test from_csv with ignore_lines (header skip)"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        self.sync_manager.from_csv('/path/to/data.csv', 'test_table', ignore_lines=1)
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.from_csv('/path/to/data.csv', 'test_table', ignore_lines=1)
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "LOAD DATA INFILE '/path/to/data.csv' INTO TABLE test_table FIELDS TERMINATED BY ',' IGNORE 1 LINES"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_from_csv_with_columns_sql_consistency(self):
        """Test from_csv with specific columns"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        columns = ['id', 'name', 'email']
        self.sync_manager.from_csv('/path/to/data.csv', 'test_table', columns=columns)
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.from_csv('/path/to/data.csv', 'test_table', columns=columns)
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = (
            "LOAD DATA INFILE '/path/to/data.csv' INTO TABLE test_table FIELDS TERMINATED BY ',' (id, name, email)"
        )
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)

    def test_from_csv_with_parallel_sql_consistency(self):
        """Test from_csv with parallel loading"""
        self.client.execute = Mock()
        self.session.execute = Mock()

        self.sync_manager.from_csv('/path/to/data.csv', 'test_table', parallel=True)
        sync_sql = self.client.execute.call_args[0][0]

        self.session_manager.from_csv('/path/to/data.csv', 'test_table', parallel=True)
        session_sql = self.session.execute.call_args[0][0]

        expected_sql = "LOAD DATA INFILE '/path/to/data.csv' INTO TABLE test_table FIELDS TERMINATED BY ',' PARALLEL 'true'"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)


class TestAsyncLoadDataSQLConsistency(unittest.IsolatedAsyncioTestCase):
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

        self.async_manager = AsyncLoadDataManager(self.client)
        self.async_session_manager = AsyncLoadDataManager(self.client, executor=self.session)

    async def test_async_from_file_sql_consistency(self):
        """Test async from_file SQL"""
        self.client.execute = AsyncMock()
        self.session.execute = AsyncMock()

        await self.async_manager.from_file('/path/to/data.csv', 'test_table')
        async_sql = self.client.execute.call_args[0][0]

        await self.async_session_manager.from_file('/path/to/data.csv', 'test_table')
        async_session_sql = self.session.execute.call_args[0][0]

        expected_sql = "LOAD DATA INFILE '/path/to/data.csv' INTO TABLE test_table FIELDS TERMINATED BY ','"
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)

    async def test_async_from_file_with_options_sql_consistency(self):
        """Test async from_file with options"""
        self.client.execute = AsyncMock()
        self.session.execute = AsyncMock()

        await self.async_manager.from_file(
            '/path/to/data.txt', 'test_table', fields_terminated_by='|', fields_enclosed_by='"', ignore_lines=1
        )
        async_sql = self.client.execute.call_args[0][0]

        await self.async_session_manager.from_file(
            '/path/to/data.txt', 'test_table', fields_terminated_by='|', fields_enclosed_by='"', ignore_lines=1
        )
        async_session_sql = self.session.execute.call_args[0][0]

        expected_sql = "LOAD DATA INFILE '/path/to/data.txt' INTO TABLE test_table FIELDS TERMINATED BY '|' ENCLOSED BY '\"' IGNORE 1 LINES"
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)


if __name__ == '__main__':
    unittest.main()
