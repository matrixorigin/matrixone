"""
Online tests for clone statement builders with real database.

Uses test_client fixture from conftest.py. All resource names use
unique prefix 'cb_' (clone builder) with timestamps to avoid collisions.
"""

import pytest
import time
from matrixone.clone_builder import clone_table, clone_database


def _uid():
    return str(int(time.time() * 1000))[-8:]


class TestCloneTableOnline:

    def test_basic(self, test_client):
        src = f"cb_src_{_uid()}"
        tgt = f"cb_tgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, name VARCHAR(50))")
            test_client.execute(f"INSERT INTO {src} VALUES (1, 'alice'), (2, 'bob')")

            stmt = clone_table(tgt).from_table(src)
            test_client.execute(str(stmt))

            rows = test_client.execute(f"SELECT * FROM {tgt} ORDER BY id").fetchall()
            assert len(rows) == 2
            assert rows[0][1] == 'alice'
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")

    def test_if_not_exists(self, test_client):
        src = f"cb_ine_src_{_uid()}"
        tgt = f"cb_ine_tgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY)")
            test_client.execute(f"INSERT INTO {src} VALUES (1)")

            stmt = clone_table(tgt).if_not_exists().from_table(src)
            assert "IF NOT EXISTS" in str(stmt)
            test_client.execute(str(stmt))

            count = test_client.execute(f"SELECT COUNT(*) FROM {tgt}").fetchone()[0]
            assert count == 1
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")

    def test_with_snapshot(self, test_client):
        src = f"cb_snap_src_{_uid()}"
        tgt = f"cb_snap_tgt_{_uid()}"
        snap = f"cb_snap_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, val INT)")
            test_client.execute(f"INSERT INTO {src} VALUES (1, 100)")

            test_client.execute(f"CREATE SNAPSHOT {snap} FOR TABLE test {src}")

            # Insert after snapshot — should NOT appear in clone
            test_client.execute(f"INSERT INTO {src} VALUES (2, 200)")

            stmt = clone_table(tgt).from_table(src, snapshot=snap)
            test_client.execute(str(stmt))

            rows = test_client.execute(f"SELECT * FROM {tgt}").fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 100
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")
            try:
                test_client.execute(f"DROP SNAPSHOT IF EXISTS {snap}")
            except Exception:
                pass

    def test_cross_database(self, test_client):
        """Clone using 'db.table' qualified names."""
        src_db = f"cb_xdb_src_{_uid()}"
        tgt_db = f"cb_xdb_tgt_{_uid()}"
        try:
            test_client.execute(f"CREATE DATABASE {src_db}")
            test_client.execute(f"CREATE TABLE {src_db}.t1 (id INT PRIMARY KEY, val INT)")
            test_client.execute(f"INSERT INTO {src_db}.t1 VALUES (1, 42)")

            test_client.execute(f"CREATE DATABASE {tgt_db}")

            stmt = clone_table(f'{tgt_db}.t1').from_table(f'{src_db}.t1')
            test_client.execute(str(stmt))

            rows = test_client.execute(f"SELECT * FROM {tgt_db}.t1").fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 42
        finally:
            test_client.execute(f"DROP DATABASE IF EXISTS {tgt_db}")
            test_client.execute(f"DROP DATABASE IF EXISTS {src_db}")

    def test_clone_nonexistent_source_fails(self, test_client):
        """Cloning from a non-existent table should raise an error."""
        tgt = f"cb_err_tgt_{_uid()}"
        try:
            stmt = clone_table(tgt).from_table('no_such_table_xyz')
            with pytest.raises(Exception):
                test_client.execute(str(stmt))
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")

    def test_clone_duplicate_target_fails(self, test_client):
        """Cloning to an existing table without IF NOT EXISTS should fail."""
        src = f"cb_dup_src_{_uid()}"
        tgt = f"cb_dup_tgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY)")
            test_client.execute(f"CREATE TABLE {tgt} (id INT PRIMARY KEY)")

            stmt = clone_table(tgt).from_table(src)
            with pytest.raises(Exception):
                test_client.execute(str(stmt))
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")


class TestCloneDatabaseOnline:

    def test_basic(self, test_client):
        src_db = f"cb_srcdb_{_uid()}"
        tgt_db = f"cb_tgtdb_{_uid()}"
        try:
            test_client.execute(f"CREATE DATABASE {src_db}")
            test_client.execute(f"CREATE TABLE {src_db}.t1 (id INT PRIMARY KEY)")
            test_client.execute(f"INSERT INTO {src_db}.t1 VALUES (1)")

            stmt = clone_database(tgt_db).from_database(src_db)
            test_client.execute(str(stmt))

            count = test_client.execute(f"SELECT COUNT(*) FROM {tgt_db}.t1").fetchone()[0]
            assert count == 1
        finally:
            test_client.execute(f"DROP DATABASE IF EXISTS {tgt_db}")
            test_client.execute(f"DROP DATABASE IF EXISTS {src_db}")

    def test_if_not_exists(self, test_client):
        src_db = f"cb_dine_src_{_uid()}"
        tgt_db = f"cb_dine_tgt_{_uid()}"
        try:
            test_client.execute(f"CREATE DATABASE {src_db}")
            test_client.execute(f"CREATE TABLE {src_db}.t1 (id INT PRIMARY KEY)")

            stmt = clone_database(tgt_db).if_not_exists().from_database(src_db)
            # Verify IF NOT EXISTS appears in SQL
            assert "IF NOT EXISTS" in str(stmt)
            test_client.execute(str(stmt))

            # Verify clone succeeded
            count = test_client.execute(f"SELECT COUNT(*) FROM {tgt_db}.t1").fetchone()[0]
            assert count == 0  # empty table cloned
        finally:
            test_client.execute(f"DROP DATABASE IF EXISTS {tgt_db}")
            test_client.execute(f"DROP DATABASE IF EXISTS {src_db}")

    def test_clone_nonexistent_source_fails(self, test_client):
        """Cloning from a non-existent database should raise an error."""
        tgt_db = f"cb_err_db_{_uid()}"
        try:
            stmt = clone_database(tgt_db).from_database('no_such_db_xyz')
            with pytest.raises(Exception):
                test_client.execute(str(stmt))
        finally:
            test_client.execute(f"DROP DATABASE IF EXISTS {tgt_db}")


class TestCloneWithSessionOnline:

    def test_clone_in_session(self, test_client):
        src = f"cb_txsrc_{_uid()}"
        tgt = f"cb_txtgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY)")
            test_client.execute(f"INSERT INTO {src} VALUES (1)")

            # Clone DDL must run outside session (not transactional)
            test_client.execute(str(clone_table(tgt).from_table(src)))

            with test_client.session() as session:
                count = session.execute(f"SELECT COUNT(*) FROM {tgt}").fetchone()[0]
                assert count == 1
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {src}")


class TestCloneWithORMModelOnline:

    def test_clone_with_orm_model(self, test_client):
        """ORM model as source, then verify data."""
        from sqlalchemy import Column, Integer, String
        from matrixone.orm import declarative_base

        Base = declarative_base()

        class CbOrmTest(Base):
            __tablename__ = f"cb_orm_{_uid()}"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        tgt = f"cb_orm_tgt_{_uid()}"
        try:
            test_client.execute(f"CREATE TABLE {CbOrmTest.__tablename__} (id INT PRIMARY KEY, name VARCHAR(50))")
            test_client.execute(f"INSERT INTO {CbOrmTest.__tablename__} VALUES (1, 'alice')")

            stmt = clone_table(tgt).from_table(CbOrmTest)
            test_client.execute(str(stmt))

            rows = test_client.execute(f"SELECT * FROM {tgt}").fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 'alice'
        finally:
            test_client.execute(f"DROP TABLE IF EXISTS {tgt}")
            test_client.execute(f"DROP TABLE IF EXISTS {CbOrmTest.__tablename__}")
