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
Offline tests for BranchManager
"""

import pytest
from matrixone.branch import BranchManager, AsyncBranchManager
from matrixone.exceptions import BranchError, ConnectionError


class MockClient:
    """Mock client for testing"""

    def __init__(self):
        self._engine = True
        self.executed_sql = []

    def execute(self, sql):
        self.executed_sql.append(sql)
        return MockResultSet()


class MockAsyncClient:
    """Mock async client for testing"""

    def __init__(self):
        self._engine = True
        self.executed_sql = []

    async def execute(self, sql):
        self.executed_sql.append(sql)
        return MockAsyncResultSet()


class MockResultSet:
    """Mock result set"""

    def __init__(self):
        self.rows = []

    def fetchall(self):
        return self.rows


class MockAsyncResultSet:
    """Mock async result set"""

    def __init__(self):
        self.rows = []

    def fetchall(self):
        return self.rows


class MockExecutor:
    """Mock executor"""

    def __init__(self):
        self.executed_sql = []

    def execute(self, sql):
        self.executed_sql.append(sql)
        return MockResultSet()


class MockAsyncExecutor:
    """Mock async executor"""

    def __init__(self):
        self.executed_sql = []

    async def execute(self, sql):
        self.executed_sql.append(sql)
        return MockAsyncResultSet()


class TestBranchManager:
    """Test BranchManager SQL generation"""

    def test_create_table_branch_without_snapshot(self):
        """Test creating table branch without snapshot"""
        manager = BranchManager(MockClient())
        sql = manager._build_create_table_branch_sql('branch_tbl', 'base_tbl')
        assert sql == "data branch create table branch_tbl from base_tbl"

    def test_create_table_branch_with_snapshot(self):
        """Test creating table branch with snapshot"""
        manager = BranchManager(MockClient())
        sql = manager._build_create_table_branch_sql('branch_tbl', 'base_tbl', 'snapshot_1')
        assert sql == 'data branch create table branch_tbl from base_tbl{snapshot="snapshot_1"}'

    def test_delete_table_branch(self):
        """Test deleting table branch"""
        manager = BranchManager(MockClient())
        sql = manager._build_delete_table_branch_sql('branch_tbl')
        assert sql == "data branch delete table branch_tbl"

    def test_create_database_branch(self):
        """Test creating database branch"""
        manager = BranchManager(MockClient())
        sql = manager._build_create_database_branch_sql('dst_db', 'src_db')
        assert sql == "data branch create database dst_db from src_db"

    def test_delete_database_branch(self):
        """Test deleting database branch"""
        manager = BranchManager(MockClient())
        sql = manager._build_delete_database_branch_sql('dst_db')
        assert sql == "data branch delete database dst_db"

    def test_diff_table_without_snapshot(self):
        """Test diffing tables without snapshot"""
        manager = BranchManager(MockClient())
        sql = manager._build_diff_table_sql('t2', 't1')
        assert sql == 'data branch diff t2 against t1'

    def test_diff_table_with_snapshot(self):
        """Test diffing tables with snapshot"""
        manager = BranchManager(MockClient())
        sql = manager._build_diff_table_sql('t2', 't1', 'snapshot_1')
        assert sql == 'data branch diff t2 against t1{snapshot="snapshot_1"}'

    def test_diff_table_with_count_output(self):
        """Test diffing tables with count output"""
        manager = BranchManager(MockClient())
        sql = manager._build_diff_table_sql('t2', 't1', output='count')
        assert sql == 'data branch diff t2 against t1 output count'

    def test_merge_table_with_skip(self):
        """Test merging tables with skip strategy"""
        manager = BranchManager(MockClient())
        sql = manager._build_merge_table_sql('t2', 't1', 'skip')
        assert sql == "data branch merge t2 into t1 when conflict skip"

    def test_merge_table_with_accept(self):
        """Test merging tables with accept strategy"""
        manager = BranchManager(MockClient())
        sql = manager._build_merge_table_sql('t2', 't1', 'accept')
        assert sql == "data branch merge t2 into t1 when conflict accept"


class TestAsyncBranchManager:
    """Test AsyncBranchManager SQL generation"""

    def test_create_table_branch_without_snapshot(self):
        """Test creating table branch without snapshot"""
        manager = AsyncBranchManager(MockAsyncClient())
        sql = manager._build_create_table_branch_sql('branch_tbl', 'base_tbl')
        assert sql == "data branch create table branch_tbl from base_tbl"

    def test_create_table_branch_with_snapshot(self):
        """Test creating table branch with snapshot"""
        manager = AsyncBranchManager(MockAsyncClient())
        sql = manager._build_create_table_branch_sql('branch_tbl', 'base_tbl', 'snapshot_1')
        assert sql == 'data branch create table branch_tbl from base_tbl{snapshot="snapshot_1"}'

    def test_delete_table_branch(self):
        """Test deleting table branch"""
        manager = AsyncBranchManager(MockAsyncClient())
        sql = manager._build_delete_table_branch_sql('branch_tbl')
        assert sql == "data branch delete table branch_tbl"

    def test_create_database_branch(self):
        """Test creating database branch"""
        manager = AsyncBranchManager(MockAsyncClient())
        sql = manager._build_create_database_branch_sql('dst_db', 'src_db')
        assert sql == "data branch create database dst_db from src_db"

    def test_delete_database_branch(self):
        """Test deleting database branch"""
        manager = AsyncBranchManager(MockAsyncClient())
        sql = manager._build_delete_database_branch_sql('dst_db')
        assert sql == "data branch delete database dst_db"

    def test_diff_table_without_snapshot(self):
        """Test diffing tables without snapshot"""
        manager = AsyncBranchManager(MockAsyncClient())
        sql = manager._build_diff_table_sql('t2', 't1')
        assert sql == 'data branch diff t2 against t1'

    def test_diff_table_with_snapshot(self):
        """Test diffing tables with snapshot"""
        manager = AsyncBranchManager(MockAsyncClient())
        sql = manager._build_diff_table_sql('t2', 't1', 'snapshot_1')
        assert sql == 'data branch diff t2 against t1{snapshot="snapshot_1"}'

    def test_diff_table_with_count_output(self):
        """Test diffing tables with count output"""
        manager = AsyncBranchManager(MockAsyncClient())
        sql = manager._build_diff_table_sql('t2', 't1', output='count')
        assert sql == 'data branch diff t2 against t1 output count'

    def test_merge_table_with_skip(self):
        """Test merging tables with skip strategy"""
        manager = AsyncBranchManager(MockAsyncClient())
        sql = manager._build_merge_table_sql('t2', 't1', 'skip')
        assert sql == "data branch merge t2 into t1 when conflict skip"

    def test_merge_table_with_accept(self):
        """Test merging tables with accept strategy"""
        manager = AsyncBranchManager(MockAsyncClient())
        sql = manager._build_merge_table_sql('t2', 't1', 'accept')
        assert sql == "data branch merge t2 into t1 when conflict accept"


class TestBranchManagerValidation:
    """Test parameter validation"""

    def test_invalid_target_table_empty(self):
        """Test empty target_table raises error"""
        manager = BranchManager(MockClient())
        with pytest.raises(BranchError, match="target_table must be a non-empty string"):
            manager._build_create_table_branch_sql('', 'source')

    def test_invalid_target_table_none(self):
        """Test None target_table raises error"""
        manager = BranchManager(MockClient())
        with pytest.raises(BranchError, match="target_table must be a non-empty string"):
            manager._build_create_table_branch_sql(None, 'source')

    def test_invalid_source_table_empty(self):
        """Test empty source_table raises error"""
        manager = BranchManager(MockClient())
        with pytest.raises(BranchError, match="source_table must be a non-empty string"):
            manager._build_create_table_branch_sql('target', '')

    def test_invalid_conflict_strategy(self):
        """Test invalid conflict_strategy raises error"""
        manager = BranchManager(MockClient())
        with pytest.raises(BranchError, match="on_conflict must be 'skip' or 'accept'"):
            manager._build_merge_table_sql('source', 'target', 'invalid')

    def test_invalid_database_name_empty(self):
        """Test empty database name raises error"""
        manager = BranchManager(MockClient())
        with pytest.raises(BranchError, match="target_database must be a non-empty string"):
            manager._build_create_database_branch_sql('', 'source_db')

    def test_invalid_delete_table_empty(self):
        """Test empty table name for delete raises error"""
        manager = BranchManager(MockClient())
        with pytest.raises(BranchError, match="table must be a non-empty string"):
            manager._build_delete_table_branch_sql('')

    def test_invalid_snapshot_name_type(self):
        """Test non-string snapshot_name raises error"""
        manager = BranchManager(MockClient())
        with pytest.raises(BranchError, match="snapshot_name must be a string or None"):
            manager._build_create_table_branch_sql('target', 'source', 123)
