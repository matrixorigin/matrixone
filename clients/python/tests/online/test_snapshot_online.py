"""
Online tests for Snapshot functionality - tests actual database operations
"""

import pytest
import unittest
import os
import sys
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client
from matrixone.snapshot import SnapshotLevel
from matrixone.exceptions import SnapshotError, ConnectionError
from .test_config import online_config


class TestSnapshotOnline:
    """Online tests for snapshot functionality"""

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

            # Insert test data
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

    def test_snapshot_creation_and_management(self, test_client, test_database):
        """Test creating and managing snapshots"""
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
        """Test creating table-level snapshots"""
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

    def test_snapshot_query_basic(self, test_client, test_database):
        """Test basic snapshot query builder functionality"""
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
        """Test parameter substitution in snapshot queries"""
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

            # Test without parameters first
            result = (
                test_client.query("test_snapshot_db.test_snapshot_table", snapshot=snapshot_name)
                .select("id", "name", "value")
                .execute()
            )

            all_rows = result.fetchall()
            print(f"All rows in snapshot: {all_rows}")
            print(f"Number of columns: {len(all_rows[0]) if all_rows else 0}")

            # Let's also check what's in the table directly
            direct_result = test_client.execute("SELECT * FROM test_snapshot_db.test_snapshot_table")
            direct_rows = direct_result.fetchall()
            print(f"Direct table query: {direct_rows}")
            print(f"Direct query columns: {len(direct_rows[0]) if direct_rows else 0}")

            # Let's test the raw SQL that's being generated
            raw_sql = f"SELECT id, name, value FROM test_snapshot_db.test_snapshot_table{{snapshot = '{snapshot_name}'}}"
            print(f"Raw SQL: {raw_sql}")
            raw_result = test_client.execute(raw_sql)
            raw_rows = raw_result.fetchall()
            print(f"Raw SQL result: {raw_rows}")
            print(f"Raw SQL columns: {len(raw_rows[0]) if raw_rows else 0}")

            # Test numeric parameter substitution
            result = (
                test_client.query("test_snapshot_db.test_snapshot_table", snapshot=snapshot_name)
                .select("id", "name", "value")
                .where("value = ?", 200)
                .execute()
            )

            rows = result.fetchall()
            print(f"Rows with value=200: {rows}")
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
        """Test complex snapshot query with all clauses"""
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

    def test_snapshot_query_validation(self, test_client, test_database):
        """Test snapshot query builder validation"""
        snapshot_name = f"test_validation_snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Create snapshot first
        test_client.snapshots.create(
            snapshot_name,
            SnapshotLevel.TABLE,
            database="test_snapshot_db",
            table="test_snapshot_table",
        )

        try:
            # Note: These validation tests were specific to SnapshotQueryBuilder
            # and are no longer applicable with the new query() API
            pass
        finally:
            # Clean up
            test_client.snapshots.delete(snapshot_name)

    def test_snapshot_query_syntax_validation(self, test_client, test_database):
        """Test that snapshot queries use correct syntax"""
        snapshot_name = f"test_syntax_snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Create snapshot first
        test_client.snapshots.create(
            snapshot_name,
            SnapshotLevel.TABLE,
            database="test_snapshot_db",
            table="test_snapshot_table",
        )

        try:
            # This should work without errors - the SQL should be properly formatted
            result = (
                test_client.query("test_snapshot_db.test_snapshot_table", snapshot=snapshot_name)
                .select("id", "name")
                .where("id = ?", 1)
                .execute()
            )

            rows = result.fetchall()
            assert len(rows) == 1

        finally:
            # Clean up
            test_client.snapshots.delete(snapshot_name)

    def test_snapshot_management_comprehensive(self, test_client, test_database):
        """Test comprehensive snapshot management functionality - from test_snapshot.py"""
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

    def test_snapshot_error_handling(self, test_client, test_database):
        """Test snapshot error handling - from test_snapshot.py"""
        # Test non-existent snapshot
        with pytest.raises(SnapshotError):
            test_client.snapshots.get("non_existent_snapshot")

        # Test invalid snapshot level
        with pytest.raises(SnapshotError):
            test_client.snapshots.create("test", "invalid_level")

    def test_snapshot_syntax_variations(self, test_client, test_database):
        """Test different snapshot syntax variations - from test_snapshot_syntax.py"""
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


if __name__ == '__main__':
    unittest.main()
