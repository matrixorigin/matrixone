"""Offline tests for clone statement builders."""

import pytest
from matrixone.clone_builder import (
    clone_table,
    clone_database,
)


class FakeModel:
    __tablename__ = 'fake_table'


class FakeModelWithSchema:
    __tablename__ = 'fake_table'
    __table_args__ = {'schema': 'mydb'}


class NoTableModel:
    pass


# ---------------------------------------------------------------------------
# CloneTable
# ---------------------------------------------------------------------------


class TestCloneTable:

    def test_basic(self):
        sql = clone_table('tgt').from_table('src').compile()
        assert sql == "CREATE TABLE tgt CLONE src"

    def test_with_snapshot(self):
        sql = clone_table('tgt').from_table('src', snapshot='snap1').compile()
        assert sql == 'CREATE TABLE tgt CLONE src {snapshot = "snap1"}'

    def test_if_not_exists(self):
        sql = clone_table('tgt').if_not_exists().from_table('src').compile()
        assert sql == "CREATE TABLE IF NOT EXISTS tgt CLONE src"

    def test_to_account(self):
        sql = clone_table('tgt').from_table('src', snapshot='s1').to_account('acc1').compile()
        assert 'TO ACCOUNT acc1' in sql

    def test_all_options(self):
        sql = clone_table('tgt').if_not_exists().from_table('src', snapshot='snap1').to_account('tenant1').compile()
        assert sql == 'CREATE TABLE IF NOT EXISTS tgt CLONE src {snapshot = "snap1"} TO ACCOUNT tenant1'

    def test_qualified_names(self):
        sql = clone_table('db2.tgt').from_table('db1.src').compile()
        assert sql == "CREATE TABLE db2.tgt CLONE db1.src"

    def test_orm_model_as_source(self):
        sql = clone_table('tgt').from_table(FakeModel).compile()
        assert sql == "CREATE TABLE tgt CLONE fake_table"

    def test_orm_model_as_target(self):
        sql = clone_table(FakeModel).from_table('src').compile()
        assert sql == "CREATE TABLE fake_table CLONE src"

    def test_orm_model_with_schema(self):
        sql = clone_table('tgt').from_table(FakeModelWithSchema).compile()
        assert sql == "CREATE TABLE tgt CLONE mydb.fake_table"

    def test_orm_model_with_schema_as_target(self):
        sql = clone_table(FakeModelWithSchema).from_table('src').compile()
        assert sql == "CREATE TABLE mydb.fake_table CLONE src"

    def test_str(self):
        stmt = clone_table('tgt').from_table('src')
        assert str(stmt) == "CREATE TABLE tgt CLONE src"

    def test_repr(self):
        stmt = clone_table('tgt').from_table('src')
        assert 'CloneTable' in repr(stmt)

    # --- Validation errors ---

    def test_empty_target_raises(self):
        with pytest.raises(ValueError, match="target_table"):
            clone_table('')

    def test_empty_source_raises(self):
        with pytest.raises(ValueError, match="source_table"):
            clone_table('tgt').from_table('')

    def test_missing_source_raises(self):
        with pytest.raises(ValueError, match="source_table must be set"):
            clone_table('tgt').compile()

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Invalid table parameter"):
            clone_table(12345)

    def test_no_tablename_model_raises(self):
        with pytest.raises(ValueError, match="Invalid table parameter"):
            clone_table(NoTableModel)

    def test_empty_account_raises(self):
        with pytest.raises(ValueError, match="account"):
            clone_table('tgt').from_table('src').to_account('')


# ---------------------------------------------------------------------------
# CloneDatabase
# ---------------------------------------------------------------------------


class TestCloneDatabase:

    def test_basic(self):
        sql = clone_database('tgt_db').from_database('src_db').compile()
        assert sql == "CREATE DATABASE tgt_db CLONE src_db"

    def test_with_snapshot(self):
        sql = clone_database('tgt_db').from_database('src_db', snapshot='snap1').compile()
        assert sql == 'CREATE DATABASE tgt_db CLONE src_db {snapshot = "snap1"}'

    def test_if_not_exists(self):
        sql = clone_database('tgt_db').if_not_exists().from_database('src_db').compile()
        assert sql == "CREATE DATABASE IF NOT EXISTS tgt_db CLONE src_db"

    def test_to_account(self):
        sql = clone_database('tgt_db').from_database('src_db', snapshot='s1').to_account('acc1').compile()
        assert 'TO ACCOUNT acc1' in sql

    def test_all_options(self):
        sql = (
            clone_database('tgt_db')
            .if_not_exists()
            .from_database('src_db', snapshot='snap1')
            .to_account('tenant1')
            .compile()
        )
        assert sql == 'CREATE DATABASE IF NOT EXISTS tgt_db CLONE src_db {snapshot = "snap1"} TO ACCOUNT tenant1'

    def test_str(self):
        stmt = clone_database('tgt').from_database('src')
        assert str(stmt) == "CREATE DATABASE tgt CLONE src"

    def test_repr(self):
        stmt = clone_database('tgt').from_database('src')
        assert 'CloneDatabase' in repr(stmt)

    # --- Validation errors ---

    def test_empty_target_raises(self):
        with pytest.raises(ValueError, match="target_database"):
            clone_database('')

    def test_empty_source_raises(self):
        with pytest.raises(ValueError, match="source_database"):
            clone_database('tgt').from_database('')

    def test_missing_source_raises(self):
        with pytest.raises(ValueError, match="source_database must be set"):
            clone_database('tgt').compile()

    def test_empty_account_raises(self):
        with pytest.raises(ValueError, match="account"):
            clone_database('tgt').from_database('src').to_account('')
