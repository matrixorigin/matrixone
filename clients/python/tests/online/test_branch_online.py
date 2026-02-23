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
Online tests for Branch operations
"""

import pytest
import time


class TestBranchOnline:
    """Test branch operations with real database"""

    def test_create_table_branch(self, test_client):
        """Test creating table branch"""
        test_client.execute("DROP TABLE IF EXISTS branch_src")
        test_client.execute("CREATE TABLE branch_src (id INT PRIMARY KEY, name VARCHAR(50))")
        test_client.execute("INSERT INTO branch_src VALUES (1, 'test')")

        branch_name = f"branch_{int(time.time())}"
        test_client.branch.create_table_branch(branch_name, "branch_src")

        result = test_client.execute(f"SELECT * FROM {branch_name}")
        assert len(result.rows) == 1

        test_client.execute(f"DROP TABLE {branch_name}")
        test_client.execute("DROP TABLE branch_src")

    def test_create_table_branch_with_snapshot(self, test_client):
        """Test creating table branch from snapshot"""
        test_client.execute("DROP TABLE IF EXISTS branch_src_snap")
        test_client.execute("CREATE TABLE branch_src_snap (id INT PRIMARY KEY)")
        test_client.execute("INSERT INTO branch_src_snap VALUES (1)")

        snap_name = f"snap_{int(time.time())}"
        test_client.execute(f"CREATE SNAPSHOT {snap_name} FOR TABLE test branch_src_snap")

        test_client.execute("INSERT INTO branch_src_snap VALUES (2)")

        branch_name = f"branch_{int(time.time())}"
        test_client.branch.create_table_branch(branch_name, "branch_src_snap", snap_name)

        result = test_client.execute(f"SELECT * FROM {branch_name}")
        assert len(result.rows) == 1

        test_client.execute(f"DROP TABLE {branch_name}")
        test_client.execute("DROP TABLE branch_src_snap")
        test_client.execute(f"DROP SNAPSHOT {snap_name}")

    def test_delete_table_branch(self, test_client):
        """Test deleting table branch"""
        test_client.execute("DROP TABLE IF EXISTS branch_src_del")
        test_client.execute("CREATE TABLE branch_src_del (id INT PRIMARY KEY)")
        test_client.execute("INSERT INTO branch_src_del VALUES (1)")

        branch_name = f"branch_{int(time.time())}"
        test_client.branch.create_table_branch(branch_name, "branch_src_del")

        test_client.branch.delete_table_branch(branch_name)

        result = test_client.execute(f"SHOW TABLES LIKE '{branch_name}'")
        assert len(result.rows) == 0

        test_client.execute("DROP TABLE branch_src_del")

    def test_diff_table(self, test_client):
        """Test diffing tables"""
        test_client.execute("DROP TABLE IF EXISTS diff_t1")
        test_client.execute("DROP TABLE IF EXISTS diff_t2")
        test_client.execute("CREATE TABLE diff_t1 (id INT PRIMARY KEY, val INT)")
        test_client.execute("CREATE TABLE diff_t2 (id INT PRIMARY KEY, val INT)")
        test_client.execute("INSERT INTO diff_t1 VALUES (1, 100), (2, 200)")
        test_client.execute("INSERT INTO diff_t2 VALUES (1, 100), (3, 300)")

        diffs = test_client.branch.diff_table("diff_t2", "diff_t1")

        test_client.execute("DROP TABLE diff_t1")
        test_client.execute("DROP TABLE diff_t2")

    def test_merge_table_skip(self, test_client):
        """Test merging tables with skip strategy"""
        test_client.execute("DROP TABLE IF EXISTS merge_t1")
        test_client.execute("DROP TABLE IF EXISTS merge_t2")
        test_client.execute("CREATE TABLE merge_t1 (id INT PRIMARY KEY, val INT)")
        test_client.execute("CREATE TABLE merge_t2 (id INT PRIMARY KEY, val INT)")
        test_client.execute("INSERT INTO merge_t1 VALUES (1, 100)")
        test_client.execute("INSERT INTO merge_t2 VALUES (2, 200)")

        test_client.branch.merge_table("merge_t2", "merge_t1", "skip")

        result = test_client.execute("SELECT * FROM merge_t1 ORDER BY id")
        assert len(result.rows) == 2

        test_client.execute("DROP TABLE merge_t1")
        test_client.execute("DROP TABLE merge_t2")

    def test_merge_table_accept(self, test_client):
        """Test merging tables with accept strategy"""
        test_client.execute("DROP TABLE IF EXISTS merge_t3")
        test_client.execute("DROP TABLE IF EXISTS merge_t4")
        test_client.execute("CREATE TABLE merge_t3 (id INT PRIMARY KEY, val INT)")
        test_client.execute("CREATE TABLE merge_t4 (id INT PRIMARY KEY, val INT)")
        test_client.execute("INSERT INTO merge_t3 VALUES (1, 100)")
        test_client.execute("INSERT INTO merge_t4 VALUES (2, 200)")

        test_client.branch.merge_table("merge_t4", "merge_t3", "accept")

        result = test_client.execute("SELECT * FROM merge_t3 ORDER BY id")
        assert len(result.rows) == 2

        test_client.execute("DROP TABLE merge_t3")
        test_client.execute("DROP TABLE merge_t4")

    def test_diff_table_count_output(self, test_client):
        """Test diff with output='count' parameter"""
        test_client.execute("DROP TABLE IF EXISTS diff_count_t1")
        test_client.execute("DROP TABLE IF EXISTS diff_count_t2")
        test_client.execute("CREATE TABLE diff_count_t1 (id INT PRIMARY KEY, val INT)")
        test_client.execute("CREATE TABLE diff_count_t2 (id INT PRIMARY KEY, val INT)")
        test_client.execute("INSERT INTO diff_count_t1 VALUES (1, 100), (2, 200)")
        test_client.execute("INSERT INTO diff_count_t2 VALUES (1, 100), (3, 300)")

        count = test_client.branch.diff("diff_count_t2", "diff_count_t1", output='count')
        assert isinstance(count, list)
        count_value = count[0][0] if count else 0
        assert count_value >= 0

        test_client.execute("DROP TABLE diff_count_t1")
        test_client.execute("DROP TABLE diff_count_t2")

    def test_create_database_branch(self, test_client):
        """Test creating database branch"""
        src_db = f"branch_src_db_{int(time.time())}"
        dst_db = f"branch_dst_db_{int(time.time())}"

        test_client.execute(f"DROP DATABASE IF EXISTS {src_db}")
        test_client.execute(f"CREATE DATABASE {src_db}")
        test_client.execute(f"USE {src_db}")
        test_client.execute("CREATE TABLE t1 (id INT PRIMARY KEY)")
        test_client.execute("INSERT INTO t1 VALUES (1)")

        test_client.branch.create_database_branch(dst_db, src_db)

        result = test_client.execute(f"SELECT * FROM {dst_db}.t1")
        assert len(result.rows) == 1

        test_client.execute(f"DROP DATABASE {src_db}")
        test_client.execute(f"DROP DATABASE {dst_db}")
        test_client.execute("USE test")

    def test_delete_database_branch(self, test_client):
        """Test deleting database branch"""
        src_db = f"branch_src_db_{int(time.time())}"
        dst_db = f"branch_dst_db_{int(time.time())}"

        test_client.execute(f"DROP DATABASE IF EXISTS {src_db}")
        test_client.execute(f"CREATE DATABASE {src_db}")
        test_client.branch.create_database_branch(dst_db, src_db)

        test_client.branch.delete_database_branch(dst_db)

        result = test_client.execute(f"SHOW DATABASES LIKE '{dst_db}'")
        assert len(result.rows) == 0

        test_client.execute(f"DROP DATABASE {src_db}")


class TestAsyncBranchOnline:
    """Test async branch operations"""

    @pytest.mark.asyncio
    async def test_async_create_table_branch(self, test_async_client):
        """Test async table branch creation"""
        await test_async_client.execute("DROP TABLE IF EXISTS async_branch_src")
        await test_async_client.execute("CREATE TABLE async_branch_src (id INT PRIMARY KEY)")
        await test_async_client.execute("INSERT INTO async_branch_src VALUES (1)")

        branch_name = f"async_branch_{int(time.time())}"
        await test_async_client.branch.create_table_branch(branch_name, "async_branch_src")

        result = await test_async_client.execute(f"SELECT * FROM {branch_name}")
        assert len(result.rows) == 1

        await test_async_client.execute(f"DROP TABLE {branch_name}")
        await test_async_client.execute("DROP TABLE async_branch_src")

    @pytest.mark.asyncio
    async def test_async_diff_table(self, test_async_client):
        """Test async table diff"""
        await test_async_client.execute("DROP TABLE IF EXISTS async_diff_t1")
        await test_async_client.execute("DROP TABLE IF EXISTS async_diff_t2")
        await test_async_client.execute("CREATE TABLE async_diff_t1 (id INT PRIMARY KEY)")
        await test_async_client.execute("CREATE TABLE async_diff_t2 (id INT PRIMARY KEY)")
        await test_async_client.execute("INSERT INTO async_diff_t1 VALUES (1)")
        await test_async_client.execute("INSERT INTO async_diff_t2 VALUES (2)")

        diffs = await test_async_client.branch.diff_table("async_diff_t2", "async_diff_t1")

        await test_async_client.execute("DROP TABLE async_diff_t1")
        await test_async_client.execute("DROP TABLE async_diff_t2")

    @pytest.mark.asyncio
    async def test_async_merge_table(self, test_async_client):
        """Test async table merge"""
        await test_async_client.execute("DROP TABLE IF EXISTS async_merge_t1")
        await test_async_client.execute("DROP TABLE IF EXISTS async_merge_t2")
        await test_async_client.execute("CREATE TABLE async_merge_t1 (id INT PRIMARY KEY)")
        await test_async_client.execute("CREATE TABLE async_merge_t2 (id INT PRIMARY KEY)")
        await test_async_client.execute("INSERT INTO async_merge_t1 VALUES (1)")
        await test_async_client.execute("INSERT INTO async_merge_t2 VALUES (2)")

        await test_async_client.branch.merge_table("async_merge_t2", "async_merge_t1", "skip")

        result = await test_async_client.execute("SELECT * FROM async_merge_t1 ORDER BY id")
        assert len(result.rows) == 2

        await test_async_client.execute("DROP TABLE async_merge_t1")
        await test_async_client.execute("DROP TABLE async_merge_t2")

    @pytest.mark.asyncio
    async def test_async_diff_count_output(self, test_async_client):
        """Test async diff with output='count' parameter"""
        await test_async_client.execute("DROP TABLE IF EXISTS async_diff_count_t1")
        await test_async_client.execute("DROP TABLE IF EXISTS async_diff_count_t2")
        await test_async_client.execute("CREATE TABLE async_diff_count_t1 (id INT PRIMARY KEY)")
        await test_async_client.execute("CREATE TABLE async_diff_count_t2 (id INT PRIMARY KEY)")
        await test_async_client.execute("INSERT INTO async_diff_count_t1 VALUES (1)")
        await test_async_client.execute("INSERT INTO async_diff_count_t2 VALUES (2)")

        count = await test_async_client.branch.diff("async_diff_count_t2", "async_diff_count_t1", output='count')
        assert isinstance(count, list)
        count_value = count[0][0] if count else 0
        assert count_value >= 0

        await test_async_client.execute("DROP TABLE async_diff_count_t1")
        await test_async_client.execute("DROP TABLE async_diff_count_t2")
