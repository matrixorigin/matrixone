"""
Online tests for branch statement builders with real database.

Uses test_client fixture from conftest.py. All table/database names use
unique prefix 'bb_' (branch builder) with timestamps to avoid collisions.
Cleanup is done in finally blocks to ensure resources are released on failure.
"""

import pytest
import time
from matrixone.branch_builder import (
    create_table_branch,
    create_database_branch,
    delete_table_branch,
    delete_database_branch,
    diff_table_branch,
    merge_table_branch,
)


def _uid():
    """Generate unique suffix for test resource names."""
    return str(int(time.time() * 1000))[-8:]


class TestCreateTableBranchOnline:

    def test_basic(self, test_client):
        """Branch copies data from source table."""
        src = f"bb_src_{_uid()}"
        tgt = f"bb_tgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, name VARCHAR(50))")
            test_client.execute(f"INSERT INTO {src} VALUES (1, 'alice'), (2, 'bob')")

            stmt = create_table_branch(tgt).from_table(src)
            test_client.execute(str(stmt))

            rows = test_client.execute(f"SELECT * FROM {tgt} ORDER BY id").fetchall()
            assert len(rows) == 2
            assert rows[0][1] == 'alice'
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")

    def test_with_snapshot(self, test_client):
        """Branch from snapshot captures point-in-time data."""
        src = f"bb_snap_src_{_uid()}"
        tgt = f"bb_snap_tgt_{_uid()}"
        snap = f"bb_snap_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, val INT)")
            test_client.execute(f"INSERT INTO {src} VALUES (1, 100)")

            test_client.execute(f"CREATE SNAPSHOT {snap} FOR TABLE test {src}")

            # Insert after snapshot — should NOT appear in branch
            test_client.execute(f"INSERT INTO {src} VALUES (2, 200)")

            stmt = create_table_branch(tgt).from_table(src, snapshot=snap)
            test_client.execute(str(stmt))

            rows = test_client.execute(f"SELECT * FROM {tgt}").fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 100
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")
            test_client.execute(f"DROP SNAPSHOT IF EXISTS {snap}")

    def test_data_isolation(self, test_client):
        """Modifications to branch do not affect source."""
        src = f"bb_iso_src_{_uid()}"
        tgt = f"bb_iso_tgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, val INT)")
            test_client.execute(f"INSERT INTO {src} VALUES (1, 100)")

            test_client.execute(str(create_table_branch(tgt).from_table(src)))

            test_client.execute(f"UPDATE {tgt} SET val = 999 WHERE id = 1")
            test_client.execute(f"INSERT INTO {tgt} VALUES (2, 200)")

            src_rows = test_client.execute(f"SELECT * FROM {src}").fetchall()
            tgt_rows = test_client.execute(f"SELECT * FROM {tgt}").fetchall()

            assert len(src_rows) == 1
            assert src_rows[0][1] == 100
            assert len(tgt_rows) == 2
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")


class TestCreateDatabaseBranchOnline:

    def test_basic(self, test_client):
        """Branch copies entire database."""
        src_db = f"bb_dbsrc_{_uid()}"
        tgt_db = f"bb_dbtgt_{_uid()}"
        try:
            test_client.execute(f"DROP DATABASE IF EXISTS {src_db}")
            test_client.execute(f"DROP DATABASE IF EXISTS {tgt_db}")
            test_client.execute(f"CREATE DATABASE {src_db}")
            test_client.execute(f"CREATE TABLE {src_db}.t1 (id INT PRIMARY KEY)")
            test_client.execute(f"INSERT INTO {src_db}.t1 VALUES (1), (2)")

            test_client.execute(str(create_database_branch(tgt_db).from_database(src_db)))

            rows = test_client.execute(f"SELECT * FROM {tgt_db}.t1").fetchall()
            assert len(rows) == 2
        finally:
            test_client.execute(f"DROP DATABASE IF EXISTS {tgt_db}")
            test_client.execute(f"DROP DATABASE IF EXISTS {src_db}")
            test_client.execute("USE test")


class TestDeleteBranchOnline:

    def test_delete_table_branch(self, test_client):
        """Deleted branch table is no longer accessible."""
        src = f"bb_delsrc_{_uid()}"
        tgt = f"bb_deltgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT)")
            test_client.execute(f"INSERT INTO {src} VALUES (1)")
            test_client.execute(str(create_table_branch(tgt).from_table(src)))

            test_client.execute(str(delete_table_branch(tgt)))

            result = test_client.execute(f"SHOW TABLES LIKE '{tgt}'")
            assert len(result.rows) == 0
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")

    def test_delete_database_branch(self, test_client):
        """Deleted branch database is no longer accessible."""
        src_db = f"bb_deldbsrc_{_uid()}"
        tgt_db = f"bb_deldbtgt_{_uid()}"
        try:
            test_client.execute(f"DROP DATABASE IF EXISTS {src_db}")
            test_client.execute(f"DROP DATABASE IF EXISTS {tgt_db}")
            test_client.execute(f"CREATE DATABASE {src_db}")
            test_client.execute(str(create_database_branch(tgt_db).from_database(src_db)))

            test_client.execute(str(delete_database_branch(tgt_db)))

            result = test_client.execute(f"SHOW DATABASES LIKE '{tgt_db}'")
            assert len(result.rows) == 0
        finally:
            test_client.execute(f"DROP DATABASE IF EXISTS {tgt_db}")
            test_client.execute(f"DROP DATABASE IF EXISTS {src_db}")
            test_client.execute("USE test")


class TestDiffBranchOnline:

    def test_diff_detects_insert(self, test_client):
        """Diff detects rows inserted into branch."""
        src = f"bb_diffsrc_{_uid()}"
        tgt = f"bb_difftgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, val INT)")
            test_client.execute(f"INSERT INTO {src} VALUES (1, 100)")
            test_client.execute(str(create_table_branch(tgt).from_table(src)))

            test_client.execute(f"INSERT INTO {tgt} VALUES (2, 200)")

            sql = diff_table_branch(tgt).against(src).compile()
            rows = test_client.execute(sql).fetchall()
            assert len(rows) >= 1
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")

    def test_diff_output_count(self, test_client):
        """OUTPUT COUNT returns a single count value."""
        src = f"bb_cntsrc_{_uid()}"
        tgt = f"bb_cnttgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY)")
            test_client.execute(f"INSERT INTO {src} VALUES (1), (2), (3)")
            test_client.execute(str(create_table_branch(tgt).from_table(src)))

            test_client.execute(f"INSERT INTO {tgt} VALUES (4), (5)")

            sql = diff_table_branch(tgt).against(src).output_count().compile()
            result = test_client.execute(sql).fetchone()
            assert result[0] == 2
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")

    def test_diff_output_limit(self, test_client):
        """OUTPUT LIMIT caps the number of returned rows."""
        src = f"bb_limsrc_{_uid()}"
        tgt = f"bb_limtgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY)")
            test_client.execute(f"INSERT INTO {src} SELECT result FROM generate_series(1, 10) g")
            test_client.execute(str(create_table_branch(tgt).from_table(src)))
            test_client.execute(f"INSERT INTO {tgt} SELECT result FROM generate_series(11, 30) g")

            sql = diff_table_branch(tgt).against(src).output_limit(5).compile()
            rows = test_client.execute(sql).fetchall()
            assert len(rows) == 5
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")

    def test_diff_detects_update(self, test_client):
        """Diff detects updated rows in branch."""
        src = f"bb_updsrc_{_uid()}"
        tgt = f"bb_updtgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, val INT)")
            test_client.execute(f"INSERT INTO {src} VALUES (1, 100), (2, 200)")
            test_client.execute(str(create_table_branch(tgt).from_table(src)))

            test_client.execute(f"UPDATE {tgt} SET val = 999 WHERE id = 1")

            sql = diff_table_branch(tgt).against(src).compile()
            rows = test_client.execute(sql).fetchall()
            # Should detect the update
            assert len(rows) >= 1
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")

    def test_diff_detects_delete(self, test_client):
        """Diff detects deleted rows in branch."""
        src = f"bb_deldiffsrc_{_uid()}"
        tgt = f"bb_deldifftgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, val INT)")
            test_client.execute(f"INSERT INTO {src} VALUES (1, 100), (2, 200)")
            test_client.execute(str(create_table_branch(tgt).from_table(src)))

            test_client.execute(f"DELETE FROM {tgt} WHERE id = 2")

            sql = diff_table_branch(tgt).against(src).output_count().compile()
            count = test_client.execute(sql).fetchone()[0]
            assert count >= 1
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")

    def test_diff_no_changes_returns_empty(self, test_client):
        """Diff with no changes returns zero count."""
        src = f"bb_nodiffsrc_{_uid()}"
        tgt = f"bb_nodifftgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY)")
            test_client.execute(f"INSERT INTO {src} VALUES (1)")
            test_client.execute(str(create_table_branch(tgt).from_table(src)))

            sql = diff_table_branch(tgt).against(src).output_count().compile()
            count = test_client.execute(sql).fetchone()[0]
            assert count == 0
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")


class TestMergeBranchOnline:

    def test_merge_skip(self, test_client):
        """Merge with skip adds non-conflicting rows."""
        src = f"bb_mskipsrc_{_uid()}"
        tgt = f"bb_mskiptgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, val INT)")
            test_client.execute(f"INSERT INTO {src} VALUES (1, 100)")
            test_client.execute(str(create_table_branch(tgt).from_table(src)))

            test_client.execute(f"INSERT INTO {tgt} VALUES (2, 200)")

            test_client.execute(str(merge_table_branch(tgt).into(src).when_conflict('skip')))

            rows = test_client.execute(f"SELECT * FROM {src} ORDER BY id").fetchall()
            assert len(rows) == 2
            assert rows[1][0] == 2
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")

    def test_merge_accept(self, test_client):
        """Merge with accept overwrites conflicting rows."""
        src = f"bb_maccsrc_{_uid()}"
        tgt = f"bb_macctgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, val INT)")
            test_client.execute(f"INSERT INTO {src} VALUES (1, 100)")
            test_client.execute(str(create_table_branch(tgt).from_table(src)))

            test_client.execute(f"UPDATE {src} SET val = 150 WHERE id = 1")
            test_client.execute(f"UPDATE {tgt} SET val = 999 WHERE id = 1")

            test_client.execute(str(merge_table_branch(tgt).into(src).when_conflict('accept')))

            val = test_client.execute(f"SELECT val FROM {src} WHERE id = 1").fetchone()[0]
            assert val == 999
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")


class TestComplexWorkflowOnline:

    def test_multi_level_branching(self, test_client):
        """Branches can be created from other branches."""
        base = f"bb_mlbase_{_uid()}"
        l1 = f"bb_mll1_{_uid()}"
        l2 = f"bb_mll2_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {base} (id INT PRIMARY KEY)")
            test_client.execute(f"INSERT INTO {base} VALUES (1)")

            test_client.execute(str(create_table_branch(l1).from_table(base)))
            test_client.execute(f"INSERT INTO {l1} VALUES (2)")

            test_client.execute(str(create_table_branch(l2).from_table(l1)))
            test_client.execute(f"INSERT INTO {l2} VALUES (3)")

            assert test_client.execute(f"SELECT COUNT(*) FROM {base}").fetchone()[0] == 1
            assert test_client.execute(f"SELECT COUNT(*) FROM {l1}").fetchone()[0] == 2
            assert test_client.execute(f"SELECT COUNT(*) FROM {l2}").fetchone()[0] == 3
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {l2}")
            test_client.execute(f"DROP TABLE IF EXISTS {l1}")
            test_client.execute(f"DROP TABLE IF EXISTS {base}")

    def test_branch_diff_merge_workflow(self, test_client):
        """Full workflow: branch → modify → diff → merge → verify."""
        src = f"bb_wfsrc_{_uid()}"
        tgt = f"bb_wftgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, name VARCHAR(50))")
            test_client.execute(f"INSERT INTO {src} VALUES (1, 'alice'), (2, 'bob')")

            # Branch
            test_client.execute(str(create_table_branch(tgt).from_table(src)))

            # Modify branch
            test_client.execute(f"INSERT INTO {tgt} VALUES (3, 'charlie')")
            test_client.execute(f"UPDATE {tgt} SET name = 'alice_v2' WHERE id = 1")

            # Diff count
            count_sql = diff_table_branch(tgt).against(src).output_count().compile()
            diff_count = test_client.execute(count_sql).fetchone()[0]
            assert diff_count > 0

            # Merge
            test_client.execute(str(merge_table_branch(tgt).into(src).when_conflict('accept')))

            # Verify
            rows = test_client.execute(f"SELECT * FROM {src} ORDER BY id").fetchall()
            assert len(rows) == 3
            assert rows[0][1] == 'alice_v2'
            assert rows[2][1] == 'charlie'
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")


class TestWithSessionOnline:

    def test_branch_in_session(self, test_client):
        """Branch operations work within session context."""
        src = f"bb_txsrc_{_uid()}"
        tgt = f"bb_txtgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY)")
            test_client.execute(f"INSERT INTO {src} VALUES (1)")

            with test_client.session() as session:
                session.execute(str(create_table_branch(tgt).from_table(src)))
                result = session.execute(f"SELECT COUNT(*) FROM {tgt}").fetchone()
                assert result[0] == 1

            # Verify persisted after session commit
            assert test_client.execute(f"SELECT COUNT(*) FROM {tgt}").fetchone()[0] == 1
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")

    def test_multiple_branches_in_session(self, test_client):
        """Multiple branch operations in single session."""
        src = f"bb_multisrc_{_uid()}"
        b1 = f"bb_multib1_{_uid()}"
        b2 = f"bb_multib2_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY)")
            test_client.execute(f"INSERT INTO {src} VALUES (1), (2)")

            with test_client.session() as session:
                session.execute(str(create_table_branch(b1).from_table(src)))
                session.execute(str(create_table_branch(b2).from_table(src)))

                r1 = session.execute(f"SELECT COUNT(*) FROM {b1}").fetchone()[0]
                r2 = session.execute(f"SELECT COUNT(*) FROM {b2}").fetchone()[0]
                assert r1 == 2
                assert r2 == 2
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {b2}")
            test_client.execute(f"DROP TABLE IF EXISTS {b1}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")


class TestORMModelOnline:
    """Test branch builders with ORM model parameters."""

    def test_create_and_diff_with_orm_model(self, test_client):
        """ORM model can be used as source/target in branch builders."""
        from sqlalchemy import Column, Integer, String
        from matrixone.orm import declarative_base

        Base = declarative_base()

        class BbOrmTest(Base):
            __tablename__ = f"bb_orm_{_uid()}"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        tgt = f"bb_orm_tgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {BbOrmTest.__tablename__} (id INT PRIMARY KEY, name VARCHAR(50))")
            test_client.execute(f"INSERT INTO {BbOrmTest.__tablename__} VALUES (1, 'alice')")

            # create branch using ORM model as source
            stmt = create_table_branch(tgt).from_table(BbOrmTest)
            test_client.execute(str(stmt))

            rows = test_client.execute(f"SELECT * FROM {tgt}").fetchall()
            assert len(rows) == 1

            # diff using ORM model as base
            stmt = diff_table_branch(tgt).against(BbOrmTest).output_count()
            result = test_client.execute(str(stmt)).fetchone()
            assert result[0] == 0  # no differences

            # merge using ORM model as target
            test_client.execute(f"INSERT INTO {tgt} VALUES (2, 'bob')")
            stmt = merge_table_branch(tgt).into(BbOrmTest).when_conflict('skip')
            test_client.execute(str(stmt))

            count = test_client.execute(f"SELECT COUNT(*) FROM {BbOrmTest.__tablename__}").fetchone()[0]
            assert count == 2
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {BbOrmTest.__tablename__}")
