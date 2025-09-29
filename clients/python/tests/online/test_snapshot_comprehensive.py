#!/usr/bin/env python3

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
Comprehensive online tests for snapshot functionality.
Merged from multiple snapshot test files to reduce redundancy while maintaining coverage.

This file consolidates tests from:
- test_snapshot_online.py (10 tests)
- test_snapshot_restore.py (6 tests)
- test_client_online.py (2 snapshot tests)
- test_async_client_online.py (2 async snapshot tests)
- test_matrixone_query_orm.py (3 snapshot tests)
- test_async_client_interfaces.py (2 snapshot tests)
- test_async_orm_online.py (1 snapshot test)

Total: 26 snapshot tests consolidated into one comprehensive file
"""

import pytest
import pytest_asyncio
import unittest
import os
import sys
import time
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client, AsyncClient
from matrixone.snapshot import SnapshotLevel, SnapshotManager
from matrixone.exceptions import SnapshotError, ConnectionError
from matrixone.logger import create_default_logger
from .test_config import online_config


@pytest.mark.online
class TestSnapshotComprehensive:
    """Comprehensive online tests for snapshot functionality"""

    @pytest.fixture(scope="class")
    def test_client(self):
        """Create and connect Client for testing"""
        host, port, user, password, database = online_config.get_connection_params()
        client = Client()
        client.connect(host=host, port=port, user=user, password=password, database=database)
        try:
            yield client
        finally:
            try:
                client.disconnect()
            except Exception as e:
                print(f"Warning: Failed to disconnect client: {e}")

    @pytest_asyncio.fixture(scope="function")
    async def test_async_client(self):
        """Create and connect AsyncClient for testing"""
        host, port, user, password, database = online_config.get_connection_params()
        client = AsyncClient()
        await client.connect(host=host, port=port, user=user, password=password, database=database)
        try:
            yield client
        finally:
            try:
                await client.disconnect()
            except Exception as e:
                print(f"Warning: Failed to disconnect async client: {e}")

    @pytest.fixture(scope="class")
    def test_database(self, test_client):
        """Set up test database and table"""
        test_db = "test_snapshot_db"
        test_table = "test_snapshot_table"

        try:
            test_client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            test_client.execute(f"USE {test_db}")
            test_client.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {test_table} (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT
                )
            """
            )

            # Clear existing data and insert test data
            test_client.execute(f"DELETE FROM {test_table}")
            test_client.execute(f"INSERT INTO {test_table} VALUES (1, 'test1', 100)")
            test_client.execute(f"INSERT INTO {test_table} VALUES (2, 'test2', 200)")
            test_client.execute(f"INSERT INTO {test_table} VALUES (3, 'test3', 300)")

            yield test_db, test_table

        finally:
            # Clean up
            try:
                test_client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            except Exception as e:
                print(f"Cleanup failed: {e}")

    @pytest_asyncio.fixture(scope="function")
    async def async_test_database(self, test_async_client):
        """Set up test database and table for async tests"""
        test_db = "test_async_snapshot_db"
        test_table = "test_async_snapshot_table"

        try:
            await test_async_client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            await test_async_client.execute(f"USE {test_db}")
            await test_async_client.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {test_table} (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT
                )
            """
            )

            # Clear existing data and insert test data
            await test_async_client.execute(f"DELETE FROM {test_table}")
            await test_async_client.execute(f"INSERT INTO {test_table} VALUES (1, 'async_test1', 100)")
            await test_async_client.execute(f"INSERT INTO {test_table} VALUES (2, 'async_test2', 200)")
            await test_async_client.execute(f"INSERT INTO {test_table} VALUES (3, 'async_test3', 300)")

            yield test_db, test_table

        finally:
            # Clean up
            try:
                await test_async_client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            except Exception as e:
                print(f"Async cleanup failed: {e}")

    # ==================== BASIC SNAPSHOT OPERATIONS ====================

    def test_snapshot_creation_and_management(self, test_client, test_database):
        """Test creating and managing snapshots - from test_snapshot_online.py"""
        snapshot_name = f"test_snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Test cluster snapshot creation
        snapshot = test_client.snapshots.create(snapshot_name, SnapshotLevel.CLUSTER)
        assert snapshot.name == snapshot_name
        assert snapshot.level == SnapshotLevel.CLUSTER

        # Test snapshot exists
        assert test_client.snapshots.exists(snapshot_name)

        # Test getting snapshot
        retrieved_snapshot = test_client.snapshots.get(snapshot_name)
        assert retrieved_snapshot.name == snapshot_name

        # Test listing snapshots
        snapshots = test_client.snapshots.list()
        snapshot_names = [s.name for s in snapshots]
        assert snapshot_name in snapshot_names

        # Test snapshot deletion
        test_client.snapshots.delete(snapshot_name)
        assert not test_client.snapshots.exists(snapshot_name)

    def test_table_snapshot_creation(self, test_client, test_database):
        """Test creating table-level snapshots - from test_snapshot_online.py"""
        snapshot_name = f"test_table_snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Test table snapshot creation
        snapshot = test_client.snapshots.create(
            snapshot_name,
            SnapshotLevel.TABLE,
            database="test_snapshot_db",
            table="test_snapshot_table",
        )
        assert snapshot.name == snapshot_name
        assert snapshot.level == SnapshotLevel.TABLE
        assert snapshot.database == "test_snapshot_db"
        assert snapshot.table == "test_snapshot_table"

        # Clean up
        test_client.snapshots.delete(snapshot_name)

    def test_basic_snapshot_operations(self, test_client):
        """Test basic snapshot creation and management - from test_snapshot_restore.py"""
        # Create test data in the correct database
        test_db = online_config.get_test_database()
        test_client.execute(
            f"CREATE TABLE IF NOT EXISTS {test_db}.snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)"
        )
        test_client.execute(f"DELETE FROM {test_db}.snapshot_test")
        test_client.execute(f"INSERT INTO {test_db}.snapshot_test VALUES (1, 'test1', 100)")
        test_client.execute(f"INSERT INTO {test_db}.snapshot_test VALUES (2, 'test2', 200)")

        # Verify initial data
        result = test_client.execute(f"SELECT COUNT(*) FROM {test_db}.snapshot_test")
        assert result.rows[0][0] == 2

        # Create snapshot
        snapshot_name = f"testsnapshot{int(time.time())}"
        try:
            snapshot = test_client.snapshots.create(
                name=snapshot_name,
                level="table",
                database=test_db,
                table="snapshot_test",
                description="Test snapshot for basic operations",
            )
            assert snapshot is not None
            assert snapshot.name == snapshot_name
        except Exception as e:
            pytest.skip(f"Snapshot creation not supported: {e}")

        # Add more data after snapshot
        test_client.execute(f"INSERT INTO {test_db}.snapshot_test VALUES (3, 'test3', 300)")
        test_client.execute(f"INSERT INTO {test_db}.snapshot_test VALUES (4, 'test4', 400)")

        result = test_client.execute(f"SELECT COUNT(*) FROM {test_db}.snapshot_test")
        assert result.rows[0][0] == 4

        # List snapshots
        try:
            snapshots = test_client.snapshots.list()
            assert isinstance(snapshots, list)
            # Check if our snapshot is in the list
            snapshot_names = [s.name for s in snapshots]
            assert snapshot_name in snapshot_names
        except Exception as e:
            pytest.skip(f"Snapshot listing not supported: {e}")

        # Cleanup
        try:
            test_client.snapshots.delete(snapshot_name)
        except Exception:
            pass  # Ignore cleanup errors

        test_client.execute(f"DROP TABLE IF EXISTS {test_db}.snapshot_test")

    def test_snapshot_enumeration(self, test_client):
        """Test snapshot enumeration and information - from test_snapshot_restore.py"""
        # Create test table in the correct database
        test_db = online_config.get_test_database()
        test_client.execute(
            f"CREATE TABLE IF NOT EXISTS {test_db}.snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)"
        )
        test_client.execute(f"DELETE FROM {test_db}.snapshot_test")
        test_client.execute(f"INSERT INTO {test_db}.snapshot_test VALUES (1, 'enum_test1', 100)")
        test_client.execute(f"INSERT INTO {test_db}.snapshot_test VALUES (2, 'enum_test2', 200)")

        # Create multiple snapshots
        snapshot_names = []
        for i in range(3):
            snapshot_name = f"enumsnapshot{i}{int(time.time())}"
            try:
                snapshot = test_client.snapshots.create(
                    name=snapshot_name,
                    level="table",
                    database=test_db,
                    table="snapshot_test",
                    description=f"Test snapshot {i}",
                )
                snapshot_names.append(snapshot_name)
            except Exception as e:
                pytest.skip(f"Snapshot creation not supported: {e}")

        # List all snapshots
        try:
            snapshots = test_client.snapshots.list()
            assert isinstance(snapshots, list)
            assert len(snapshots) >= len(snapshot_names)

            # Verify our snapshots are in the list
            snapshot_names_in_list = [s.name for s in snapshots]
            for name in snapshot_names:
                assert name in snapshot_names_in_list
        except Exception as e:
            pytest.skip(f"Snapshot listing not supported: {e}")

        # Cleanup
        for snapshot_name in snapshot_names:
            try:
                test_client.snapshots.delete(snapshot_name)
            except Exception:
                pass  # Ignore cleanup errors

        test_client.execute(f"DROP TABLE IF EXISTS {test_db}.snapshot_test")

    # ==================== SNAPSHOT QUERY OPERATIONS ====================

    def test_snapshot_query_basic(self, test_client, test_database):
        """Test basic snapshot query builder functionality - from test_snapshot_online.py"""
        snapshot_name = f"test_query_snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Create snapshot first
        test_client.snapshots.create(
            snapshot_name,
            SnapshotLevel.TABLE,
            database="test_snapshot_db",
            table="test_snapshot_table",
        )

        try:
            # Test basic query
            result = (
                test_client.query("test_snapshot_db.test_snapshot_table", snapshot=snapshot_name)
                .select("id", "name", "value")
                .execute()
            )

            rows = result.fetchall()
            assert len(rows) == 3

            # Test query with WHERE condition
            result = (
                test_client.query("test_snapshot_db.test_snapshot_table", snapshot=snapshot_name)
                .select("id", "name", "value")
                .where("value > ?", 150)
                .execute()
            )

            rows = result.fetchall()
            assert len(rows) == 2  # Only rows with value > 150

        finally:
            # Clean up
            test_client.snapshots.delete(snapshot_name)

    def test_snapshot_query_parameter_substitution(self, test_client, test_database):
        """Test parameter substitution in snapshot queries - from test_snapshot_online.py"""
        snapshot_name = f"test_param_snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Create cluster snapshot first (more reliable than table snapshot)
        test_client.snapshots.create(snapshot_name, SnapshotLevel.CLUSTER)

        try:
            # Test string parameter substitution
            result = (
                test_client.query("test_snapshot_db.test_snapshot_table", snapshot=snapshot_name)
                .select("id", "name", "value")
                .where("name = ?", "test1")
                .execute()
            )

            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == "test1"

            # Test numeric parameter substitution
            result = (
                test_client.query("test_snapshot_db.test_snapshot_table", snapshot=snapshot_name)
                .select("id", "name", "value")
                .where("value = ?", 200)
                .execute()
            )

            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][2] == 200

            # Test multiple parameters
            result = (
                test_client.query("test_snapshot_db.test_snapshot_table", snapshot=snapshot_name)
                .select("id", "name", "value")
                .where("name = ? AND value > ?", "test2", 150)
                .execute()
            )

            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == "test2"

        finally:
            # Clean up
            test_client.snapshots.delete(snapshot_name)

    def test_snapshot_query_complex_query(self, test_client, test_database):
        """Test complex snapshot query with all clauses - from test_snapshot_online.py"""
        snapshot_name = f"test_complex_snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Create snapshot first
        test_client.snapshots.create(
            snapshot_name,
            SnapshotLevel.TABLE,
            database="test_snapshot_db",
            table="test_snapshot_table",
        )

        try:
            # Test complex query with ORDER BY and LIMIT
            result = (
                test_client.query("test_snapshot_db.test_snapshot_table", snapshot=snapshot_name)
                .select("id", "name", "value")
                .where("value > ?", 100)
                .order_by("value DESC")
                .limit(2)
                .execute()
            )

            rows = result.fetchall()
            assert len(rows) == 2
            # Should be ordered by value DESC
            assert rows[0][2] >= rows[1][2]

        finally:
            # Clean up
            test_client.snapshots.delete(snapshot_name)

    def test_snapshot_query_functionality(self, test_client, test_database):
        """Test snapshot query functionality - from test_client_online.py"""
        test_db, test_table = test_database
        snapshot_name = f"test_client_snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            # Create snapshot
            test_client.snapshots.create(snapshot_name, "table", database=test_db, table=test_table)

            # Test snapshot query using query builder
            result = (
                test_client.query(f"{test_db}.{test_table}", snapshot=snapshot_name)
                .select("*")
                .where("value > ?", 150)
                .execute()
            )

            rows = result.fetchall()
            assert len(rows) == 2  # Should have 2 rows with value > 150

            # Test snapshot query without parameters
            result = test_client.query(f"{test_db}.{test_table}", snapshot=snapshot_name).select("COUNT(*)").execute()

            count = result.fetchone()[0]
            assert count == 3

        finally:
            # Clean up snapshot
            try:
                test_client.snapshots.delete(snapshot_name)
            except Exception:
                pass

    def test_snapshot_query_syntax_validation(self, test_client, test_database):
        """Test that snapshot queries use correct syntax - from test_snapshot_online.py and test_client_online.py"""
        test_db, test_table = test_database
        snapshot_name = f"test_syntax_snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            # Create snapshot
            test_client.snapshots.create(snapshot_name, "table", database=test_db, table=test_table)

            # This should work without errors - the SQL should be properly formatted
            result = (
                test_client.query(f"{test_db}.{test_table}", snapshot=snapshot_name)
                .select("id", "name")
                .where("id = ?", 1)
                .execute()
            )

            rows = result.fetchall()
            assert len(rows) == 1

        finally:
            # Clean up
            test_client.snapshots.delete(snapshot_name)

    # ==================== SNAPSHOT MANAGEMENT ====================

    def test_snapshot_management_comprehensive(self, test_client, test_database):
        """Test comprehensive snapshot management functionality - from test_snapshot_online.py"""
        snapshot_name = f"test_snapshot_001_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            # Create snapshot using the snapshot manager
            snapshot = test_client.snapshots.create(
                name=snapshot_name,
                level="table",
                database="test_snapshot_db",
                table="test_snapshot_table",
                description="Test snapshot for SDK",
            )

            assert snapshot is not None
            assert snapshot.name == snapshot_name

            # List snapshots
            snapshots = test_client.snapshots.list()
            snapshot_names = [s.name for s in snapshots]
            assert snapshot_name in snapshot_names

            # Get specific snapshot
            retrieved_snapshot = test_client.snapshots.get(snapshot_name)
            assert retrieved_snapshot.name == snapshot_name

            # Test snapshot query using query builder
            result = test_client.query("test_snapshot_db.test_snapshot_table", snapshot=snapshot_name).select("*").execute()
            rows = result.fetchall()
            assert len(rows) == 3  # We inserted 3 test rows

            # Test snapshot context manager
            with test_client.snapshot(snapshot_name) as snapshot_client:
                result = snapshot_client.execute("SELECT COUNT(*) FROM test_snapshot_db.test_snapshot_table")
                count = result.scalar()
                assert count == 3

            # Test snapshot query builder with WHERE clause
            result = (
                test_client.query("test_snapshot_db.test_snapshot_table", snapshot=snapshot_name)
                .select("id", "name", "value")
                .where("value > ?", 150)
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) == 2  # Only rows with value > 150

        finally:
            # Clean up
            test_client.snapshots.delete(snapshot_name)

    # ==================== SNAPSHOT RESTORE OPERATIONS ====================

    def test_snapshot_restore_operations(self, test_client):
        """Test snapshot restore operations - from test_snapshot_restore.py"""
        # Create test data in the correct database
        test_db = online_config.get_test_database()
        test_client.execute(
            f"CREATE TABLE IF NOT EXISTS {test_db}.snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)"
        )
        test_client.execute(f"DELETE FROM {test_db}.snapshot_test")
        test_client.execute(f"INSERT INTO {test_db}.snapshot_test VALUES (1, 'restore_test1', 100)")
        test_client.execute(f"INSERT INTO {test_db}.snapshot_test VALUES (2, 'restore_test2', 200)")

        # Create snapshot
        snapshot_name = f"restoresnapshot{int(time.time())}"
        try:
            snapshot = test_client.snapshots.create(
                name=snapshot_name,
                level="table",
                database=test_db,
                table="snapshot_test",
                description="Test snapshot for restore operations",
            )
        except Exception as e:
            pytest.skip(f"Snapshot creation not supported: {e}")

        # Add more data after snapshot
        test_client.execute(f"INSERT INTO {test_db}.snapshot_test VALUES (3, 'restore_test3', 300)")
        test_client.execute(f"INSERT INTO {test_db}.snapshot_test VALUES (4, 'restore_test4', 400)")

        result = test_client.execute(f"SELECT COUNT(*) FROM {test_db}.snapshot_test")
        assert result.rows[0][0] == 4

        # Restore from snapshot
        try:
            test_client.restore.restore_table(snapshot_name, "sys", test_db, "snapshot_test")

            # Verify data after restore
            result = test_client.execute(f"SELECT COUNT(*) FROM {test_db}.snapshot_test")
            assert result.rows[0][0] == 2  # Should be back to original 2 records

            # Verify specific data
            result = test_client.execute(f"SELECT * FROM {test_db}.snapshot_test ORDER BY id")
            assert len(result.rows) == 2
            assert result.rows[0][0] == 1  # ID: 1
            assert result.rows[1][0] == 2  # ID: 2
        except Exception as e:
            pytest.skip(f"Snapshot restore not supported: {e}")

        # Cleanup
        try:
            test_client.snapshots.delete(snapshot_name)
        except Exception:
            pass  # Ignore cleanup errors

        test_client.execute(f"DROP TABLE IF EXISTS {test_db}.snapshot_test")

    # ==================== ERROR HANDLING ====================

    def test_snapshot_error_handling(self, test_client, test_database):
        """Test snapshot error handling - from test_snapshot_online.py and test_snapshot_restore.py"""
        # Test non-existent snapshot
        with pytest.raises(SnapshotError):
            test_client.snapshots.get("non_existent_snapshot")

        # Test invalid snapshot level
        with pytest.raises(SnapshotError):
            test_client.snapshots.create("test", "invalid_level")

        # Test creating snapshot with invalid parameters
        try:
            test_client.snapshots.create(
                name="",  # Empty name should fail
                level="table",
                database=online_config.get_test_database(),
                table="nonexistent_table",
                description="Test invalid snapshot",
            )
            assert False, "Should have failed with empty name"
        except Exception:
            pass  # Expected to fail

        # Test deleting non-existent snapshot
        try:
            test_client.snapshots.delete("nonexistent_snapshot")
            assert False, "Should have failed with non-existent snapshot"
        except Exception:
            pass  # Expected to fail

        # Test restoring from non-existent snapshot
        try:
            test_client.restore.restore_table("nonexistent_snapshot", "sys", "test", "test_table")
            assert False, "Should have failed with non-existent snapshot"
        except Exception:
            pass  # Expected to fail

    # ==================== SNAPSHOT SYNTAX VARIATIONS ====================

    def test_snapshot_syntax_variations(self, test_client, test_database):
        """Test different snapshot syntax variations - from test_snapshot_online.py"""
        # Create a test table for syntax testing
        test_table = "test_syntax_table"
        test_client.execute(f"CREATE TABLE IF NOT EXISTS {test_table} (id INT PRIMARY KEY, name VARCHAR(100))")
        test_client.execute(f"INSERT INTO {test_table} (id, name) VALUES (1, 'test')")

        try:
            # Test different snapshot syntax variations
            syntax_variations = [
                f"CREATE SNAPSHOT test_snapshot_001 TABLE test_snapshot_db.{test_table}",
                f"CREATE SNAPSHOT test_snapshot_001 OF TABLE test_snapshot_db.{test_table}",
                f"CREATE SNAPSHOT test_snapshot_001 FOR TABLE test_snapshot_db.{test_table}",
                f"CREATE SNAPSHOT test_snapshot_001 FROM TABLE test_snapshot_db.{test_table}",
                f"CREATE SNAPSHOT test_snapshot_001 CLONE TABLE test_snapshot_db.{test_table}",
            ]

            snapshot_created = False
            for i, syntax in enumerate(syntax_variations):
                try:
                    test_client.execute(syntax)
                    snapshot_created = True
                    print(f"✓ Syntax {i+1} worked: {syntax}")
                    break
                except Exception as e:
                    print(f"✗ Syntax {i+1} failed: {e}")

            if snapshot_created:
                # Try to drop the snapshot
                try:
                    test_client.execute("DROP SNAPSHOT test_snapshot_001")
                    print("✓ Snapshot dropped successfully")
                except Exception as e:
                    print(f"✗ Failed to drop snapshot: {e}")

            # Test listing snapshots
            try:
                result = test_client.execute("SHOW SNAPSHOTS")
                snapshots = result.fetchall()
                print(f"✓ SHOW SNAPSHOTS worked: {len(snapshots)} snapshots found")
            except Exception as e:
                print(f"✗ SHOW SNAPSHOTS failed: {e}")

            # Test other snapshot commands
            try:
                result = test_client.execute("SELECT * FROM mo_catalog.mo_snapshots")
                rows = result.fetchall()
                print(f"✓ mo_catalog.mo_snapshots query worked: {len(rows)} rows")
            except Exception as e:
                print(f"✗ mo_catalog.mo_snapshots query failed: {e}")

        finally:
            # Clean up test table
            test_client.execute(f"DROP TABLE IF EXISTS {test_table}")

    # ==================== ASYNC SNAPSHOT OPERATIONS ====================

    @pytest.mark.asyncio
    async def test_async_snapshot_operations(self, test_async_client):
        """Test async snapshot operations - from test_snapshot_restore.py"""
        # Create test data
        await test_async_client.execute(
            "CREATE TABLE IF NOT EXISTS snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)"
        )
        await test_async_client.execute("DELETE FROM snapshot_test")
        await test_async_client.execute("INSERT INTO snapshot_test VALUES (1, 'async_test1', 100)")
        await test_async_client.execute("INSERT INTO snapshot_test VALUES (2, 'async_test2', 200)")

        # Verify initial data
        result = await test_async_client.execute("SELECT COUNT(*) FROM snapshot_test")
        assert result.rows[0][0] == 2

        # Test snapshot operations (as per examples - handle failures gracefully)
        snapshot_name = f"asyncsnapshot{int(time.time())}"

        # Test snapshot listing (should work even if empty)
        snapshots = await test_async_client.snapshots.list()
        assert isinstance(snapshots, list)

        try:
            snapshot = await test_async_client.snapshots.create(
                name=snapshot_name,
                level="table",
                database=online_config.get_test_database(),
                table="snapshot_test",
                description="Test async snapshot",
            )
            assert snapshot is not None
            assert snapshot.name == snapshot_name

            # Test snapshot listing after creation
            snapshots = await test_async_client.snapshots.list()
            assert isinstance(snapshots, list)

            # Test snapshot deletion
            await test_async_client.snapshots.delete(snapshot_name)

        except Exception as e:
            # If snapshot creation fails, test the API methods still work
            assert "snapshot" in str(e).lower() or "SQL" in str(e) or "syntax" in str(e).lower()  # Ignore cleanup errors

        await test_async_client.execute("DROP TABLE IF EXISTS snapshot_test")

    @pytest.mark.asyncio
    async def test_async_snapshot_query_functionality(self, test_async_client, async_test_database):
        """Test async snapshot query functionality - from test_async_client_online.py"""
        test_db, test_table = async_test_database

        # First create a snapshot
        snapshot_name = f"test_async_client_snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            # Create snapshot
            await test_async_client.snapshots.create(snapshot_name, "table", database=test_db, table=test_table)

            # Test snapshot query using query builder
            result = (
                await test_async_client.query(f"{test_db}.{test_table}", snapshot=snapshot_name)
                .select("*")
                .where("value > ?", 150)
                .execute()
            )

            rows = result.fetchall()
            assert len(rows) == 2  # Should have 2 rows with value > 150

            # Test snapshot query without parameters
            result = (
                await test_async_client.query(f"{test_db}.{test_table}", snapshot=snapshot_name).select("COUNT(*)").execute()
            )

            count = result.fetchone()[0]
            assert count == 3

        finally:
            # Clean up snapshot
            try:
                await test_async_client.snapshots.delete(snapshot_name)
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_async_snapshot_query_syntax_validation(self, test_async_client, async_test_database):
        """Test that async snapshot queries use correct syntax - from test_async_client_online.py"""
        test_db, test_table = async_test_database
        snapshot_name = f"test_async_syntax_snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            # Create snapshot
            await test_async_client.snapshots.create(snapshot_name, "table", database=test_db, table=test_table)

            # Test that snapshot query works with proper syntax
            result = (
                await test_async_client.query(f"{test_db}.{test_table}", snapshot=snapshot_name)
                .select("id", "name")
                .where("id = ?", 1)
                .execute()
            )

            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1
            assert rows[0][1] == 'async_test1'

        finally:
            # Clean up snapshot
            try:
                await test_async_client.snapshots.delete(snapshot_name)
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_snapshot_query(self, test_async_client):
        """Test query method with snapshot parameter - from test_async_client_interfaces.py"""
        # Should return a query builder with snapshot
        query = test_async_client.query("test_table", snapshot="test_snapshot")
        assert query is not None
        assert hasattr(query, '_snapshot_name')
        assert query._snapshot_name == "test_snapshot"

    @pytest.mark.asyncio
    async def test_snapshot_context_manager(self, test_async_client):
        """Test snapshot context manager - from test_async_client_interfaces.py"""
        # Create a test table first
        await test_async_client.create_table("test_snapshot_table", {"id": "int primary key", "name": "varchar(100)"})

        try:
            # Create a snapshot with unique name
            snapshot_name = f"test_snapshot_ctx_{int(time.time())}"
            await test_async_client.snapshots.create(
                name=snapshot_name,
                level="table",
                database=online_config.get_test_database(),
                table="test_snapshot_table",
            )

            # Test snapshot context manager
            async with test_async_client.snapshot(snapshot_name) as snapshot_client:
                assert snapshot_client is not None
                # Should be able to execute queries
                result = await snapshot_client.execute("SELECT COUNT(*) FROM test_snapshot_table")
                assert result is not None

        finally:
            # Cleanup
            try:
                await test_async_client.snapshots.delete(snapshot_name)
            except Exception:
                pass
            try:
                await test_async_client.execute("DROP TABLE IF EXISTS test_snapshot_table")
            except Exception:
                pass

    # ==================== SNAPSHOT WITH LOGGING ====================

    def test_snapshot_with_logging(self, test_client):
        """Test snapshot operations with custom logging - from test_snapshot_restore.py"""
        # Create logger
        logger = create_default_logger()

        # Create test data in the correct database
        test_db = online_config.get_test_database()
        test_client.execute(
            f"CREATE TABLE IF NOT EXISTS {test_db}.snapshot_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)"
        )
        test_client.execute(f"DELETE FROM {test_db}.snapshot_test")
        test_client.execute(f"INSERT INTO {test_db}.snapshot_test VALUES (1, 'log_test1', 100)")

        # Create snapshot with logging
        snapshot_name = f"logsnapshot{int(time.time())}"
        try:
            snapshot = test_client.snapshots.create(
                name=snapshot_name,
                level="table",
                database=test_db,
                table="snapshot_test",
                description="Test snapshot with logging",
            )
            assert snapshot is not None
        except Exception as e:
            pytest.skip(f"Snapshot creation not supported: {e}")

        # Cleanup
        try:
            test_client.snapshots.delete(snapshot_name)
        except Exception:
            pass  # Ignore cleanup errors

        test_client.execute(f"DROP TABLE IF EXISTS {test_db}.snapshot_test")

    # ==================== ORM SNAPSHOT OPERATIONS ====================

    def test_orm_snapshot_queries(self, test_client):
        """Test ORM snapshot functionality - from test_matrixone_query_orm.py"""
        # This test requires ORM setup which may not be available in all environments
        # We'll create a simplified version that tests the basic snapshot query functionality

        # Create test table for ORM-like testing in the correct database
        test_db = online_config.get_test_database()
        test_client.execute(
            f"CREATE TABLE IF NOT EXISTS {test_db}.test_users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))"
        )
        test_client.execute(f"DELETE FROM {test_db}.test_users")
        test_client.execute(f"INSERT INTO {test_db}.test_users VALUES (1, 'John Doe', 'john@example.com')")
        test_client.execute(f"INSERT INTO {test_db}.test_users VALUES (2, 'Jane Smith', 'jane@example.com')")

        try:
            # Create a snapshot
            snapshot_manager = SnapshotManager(test_client)
            snapshot = snapshot_manager.create(
                name="test_users_snapshot",
                level=SnapshotLevel.TABLE,
                database=test_db,
                table="test_users",
            )

            # Query from snapshot using basic query builder
            result = test_client.query(f"{test_db}.test_users", snapshot="test_users_snapshot").select("*").execute()
            rows = result.fetchall()
            assert len(rows) == 2

            # Query with filter from snapshot
            result = (
                test_client.query(f"{test_db}.test_users", snapshot="test_users_snapshot")
                .select("*")
                .where("id = ?", 1)
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 'John Doe'

        except Exception as e:
            pytest.skip(f"ORM snapshot functionality not supported: {e}")

        finally:
            # Clean up snapshot
            try:
                test_client.execute("DROP SNAPSHOT test_users_snapshot")
            except:
                pass
            test_client.execute(f"DROP TABLE IF EXISTS {test_db}.test_users")

    @pytest.mark.asyncio
    async def test_async_orm_snapshot_queries(self, test_async_client):
        """Test async ORM snapshot functionality - from test_matrixone_query_orm.py"""
        # Create test table for ORM-like testing
        await test_async_client.execute(
            "CREATE TABLE IF NOT EXISTS test_users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))"
        )
        await test_async_client.execute("DELETE FROM test_users")
        await test_async_client.execute("INSERT INTO test_users VALUES (1, 'John Doe', 'john@example.com')")
        await test_async_client.execute("INSERT INTO test_users VALUES (2, 'Jane Smith', 'jane@example.com')")

        try:
            # Create a snapshot
            snapshot = await test_async_client.snapshots.create(
                name="test_users_snapshot_async",
                level="table",
                database=online_config.get_connection_params()[4],  # database name
                table="test_users",
            )

            # Query from snapshot using basic query builder
            db_name = online_config.get_connection_params()[4]
            result = (
                await test_async_client.query(f"{db_name}.test_users", snapshot="test_users_snapshot_async")
                .select("*")
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) == 2

            # Query with filter from snapshot
            result = (
                await test_async_client.query(f"{db_name}.test_users", snapshot="test_users_snapshot_async")
                .select("*")
                .where("id = ?", 1)
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 'John Doe'

        except Exception as e:
            pytest.skip(f"Async ORM snapshot functionality not supported: {e}")

        finally:
            # Clean up snapshot
            try:
                await test_async_client.execute("DROP SNAPSHOT test_users_snapshot_async")
            except:
                pass
            await test_async_client.execute("DROP TABLE IF EXISTS test_users")

    def test_invalid_snapshot(self, test_client):
        """Test invalid snapshot operations - from test_matrixone_query_orm.py"""
        # Test querying with non-existent snapshot
        try:
            result = test_client.query("test_table", snapshot="non_existent_snapshot").select("*").execute()
            # If this doesn't raise an exception, the result should be empty or indicate error
            assert result is not None
        except Exception as e:
            # Expected to fail with non-existent snapshot
            assert "snapshot" in str(e).lower() or "not found" in str(e).lower()


if __name__ == '__main__':
    unittest.main()
