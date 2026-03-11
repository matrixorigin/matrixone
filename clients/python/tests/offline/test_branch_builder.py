"""
Offline tests for branch statement builders.
"""

import pytest
from matrixone.branch_builder import (
    create_table_branch,
    create_database_branch,
    delete_table_branch,
    delete_database_branch,
    diff_table_branch,
    merge_table_branch,
    DiffOutputOption,
    BranchStatement,
)
from matrixone.branch import MergeConflictStrategy

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeModel:
    """Fake ORM model for testing."""

    __tablename__ = 'fake_table'


class FakeModelWithSchema:
    """Fake ORM model with schema (db.table)."""

    __tablename__ = 'fake_table'
    __table_args__ = {'schema': 'mydb'}


class NoTableModel:
    """Object without __tablename__."""

    pass


# ---------------------------------------------------------------------------
# CREATE TABLE BRANCH
# ---------------------------------------------------------------------------


class TestCreateTableBranch:

    def test_basic(self):
        sql = create_table_branch('tgt').from_table('src').compile()
        assert sql == "data branch create table tgt from src"

    def test_with_snapshot(self):
        sql = create_table_branch('tgt').from_table('src', snapshot='snap1').compile()
        assert sql == 'data branch create table tgt from src{snapshot="snap1"}'

    def test_with_account(self):
        sql = create_table_branch('tgt').from_table('src').to_account('tenant1').compile()
        assert sql == "data branch create table tgt from src to account tenant1"

    def test_all_params(self):
        sql = create_table_branch('tgt').from_table('src', snapshot='snap1').to_account('tenant1').compile()
        assert sql == 'data branch create table tgt from src{snapshot="snap1"} to account tenant1'

    def test_qualified_names(self):
        sql = create_table_branch('db1.tgt').from_table('db2.src').compile()
        assert sql == "data branch create table db1.tgt from db2.src"

    def test_orm_model_as_source(self):
        sql = create_table_branch('tgt').from_table(FakeModel).compile()
        assert sql == "data branch create table tgt from fake_table"

    def test_orm_model_as_target(self):
        sql = create_table_branch(FakeModel).from_table('src').compile()
        assert sql == "data branch create table fake_table from src"

    def test_orm_model_with_schema(self):
        sql = create_table_branch('tgt').from_table(FakeModelWithSchema).compile()
        assert sql == "data branch create table tgt from mydb.fake_table"

    def test_orm_model_with_schema_as_target(self):
        sql = create_table_branch(FakeModelWithSchema).from_table('src').compile()
        assert sql == "data branch create table mydb.fake_table from src"

    # --- Validation errors ---

    def test_empty_target_raises(self):
        with pytest.raises(ValueError, match="target_table"):
            create_table_branch('')

    def test_empty_source_raises(self):
        with pytest.raises(ValueError, match="source_table"):
            create_table_branch('tgt').from_table('')

    def test_missing_source_raises(self):
        with pytest.raises(ValueError, match="source_table must be set"):
            create_table_branch('tgt').compile()

    def test_invalid_type_target_raises(self):
        with pytest.raises(ValueError, match="Invalid table parameter"):
            create_table_branch(12345)

    def test_invalid_type_source_raises(self):
        with pytest.raises(ValueError, match="Invalid table parameter"):
            create_table_branch('tgt').from_table(12345)

    def test_no_tablename_model_raises(self):
        with pytest.raises(ValueError, match="Invalid table parameter"):
            create_table_branch(NoTableModel)

    def test_empty_account_raises(self):
        with pytest.raises(ValueError, match="account"):
            create_table_branch('tgt').from_table('src').to_account('')


# ---------------------------------------------------------------------------
# CREATE DATABASE BRANCH
# ---------------------------------------------------------------------------


class TestCreateDatabaseBranch:

    def test_basic(self):
        sql = create_database_branch('dev').from_database('prod').compile()
        assert sql == "data branch create database dev from prod"

    def test_with_snapshot(self):
        sql = create_database_branch('dev').from_database('prod', snapshot='snap1').compile()
        assert sql == 'data branch create database dev from prod{snapshot="snap1"}'

    def test_with_account(self):
        sql = create_database_branch('dev').from_database('prod').to_account('t1').compile()
        assert sql == "data branch create database dev from prod to account t1"

    def test_empty_target_raises(self):
        with pytest.raises(ValueError, match="target_database"):
            create_database_branch('')

    def test_empty_source_raises(self):
        with pytest.raises(ValueError, match="source_database"):
            create_database_branch('dev').from_database('')

    def test_missing_source_raises(self):
        with pytest.raises(ValueError, match="source_database must be set"):
            create_database_branch('dev').compile()


# ---------------------------------------------------------------------------
# DELETE TABLE BRANCH
# ---------------------------------------------------------------------------


class TestDeleteTableBranch:

    def test_basic(self):
        assert delete_table_branch('tbl').compile() == "data branch delete table tbl"

    def test_qualified_name(self):
        assert delete_table_branch('db.tbl').compile() == "data branch delete table db.tbl"

    def test_orm_model(self):
        assert delete_table_branch(FakeModel).compile() == "data branch delete table fake_table"

    def test_orm_model_with_schema(self):
        assert delete_table_branch(FakeModelWithSchema).compile() == "data branch delete table mydb.fake_table"

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="table"):
            delete_table_branch('')


# ---------------------------------------------------------------------------
# DELETE DATABASE BRANCH
# ---------------------------------------------------------------------------


class TestDeleteDatabaseBranch:

    def test_basic(self):
        assert delete_database_branch('db1').compile() == "data branch delete database db1"

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="database"):
            delete_database_branch('')


# ---------------------------------------------------------------------------
# DIFF TABLE BRANCH
# ---------------------------------------------------------------------------


class TestDiffTableBranch:

    def test_basic(self):
        sql = diff_table_branch('t1').against('t2').compile()
        assert sql == "data branch diff t1 against t2"

    def test_target_snapshot(self):
        sql = diff_table_branch('t1').snapshot('s1').against('t2').compile()
        assert sql == 'data branch diff t1{snapshot="s1"} against t2'

    def test_base_snapshot(self):
        sql = diff_table_branch('t1').against('t2', snapshot='s2').compile()
        assert sql == 'data branch diff t1 against t2{snapshot="s2"}'

    def test_both_snapshots(self):
        sql = diff_table_branch('t1').snapshot('s1').against('t2', snapshot='s2').compile()
        assert sql == 'data branch diff t1{snapshot="s1"} against t2{snapshot="s2"}'

    def test_output_count(self):
        sql = diff_table_branch('t1').against('t2').output_count().compile()
        assert sql == "data branch diff t1 against t2 output count"

    def test_output_limit(self):
        sql = diff_table_branch('t1').against('t2').output_limit(10).compile()
        assert sql == "data branch diff t1 against t2 output limit 10"

    def test_output_limit_zero(self):
        sql = diff_table_branch('t1').against('t2').output_limit(0).compile()
        assert sql == "data branch diff t1 against t2 output limit 0"

    def test_output_file_local(self):
        sql = diff_table_branch('t1').against('t2').output_file('/tmp/diff.sql').compile()
        assert sql == "data branch diff t1 against t2 output file '/tmp/diff.sql'"

    def test_output_file_stage(self):
        sql = diff_table_branch('t1').against('t2').output_file('stage://my_stage/').compile()
        assert sql == "data branch diff t1 against t2 output file 'stage://my_stage/'"

    def test_output_as(self):
        sql = diff_table_branch('t1').against('t2').output_as('result').compile()
        assert sql == "data branch diff t1 against t2 output as result"

    def test_qualified_names(self):
        sql = diff_table_branch('db1.t1').against('db2.t2').compile()
        assert 'db1.t1' in sql and 'db2.t2' in sql

    def test_orm_model_with_schema(self):
        sql = diff_table_branch(FakeModelWithSchema).against('t2').output_count().compile()
        assert sql == "data branch diff mydb.fake_table against t2 output count"

    def test_orm_model_with_schema_as_base(self):
        sql = diff_table_branch('t1').against(FakeModelWithSchema).compile()
        assert sql == "data branch diff t1 against mydb.fake_table"

    def test_all_params_combined(self):
        sql = diff_table_branch('t1').snapshot('s1').against('t2', snapshot='s2').output_limit(50).compile()
        assert 'snapshot="s1"' in sql
        assert 'snapshot="s2"' in sql
        assert 'output limit 50' in sql

    # --- Output override: last one wins ---

    def test_output_override_count_then_limit(self):
        sql = diff_table_branch('t1').against('t2').output_count().output_limit(5).compile()
        assert 'output limit 5' in sql
        assert 'output count' not in sql

    def test_output_override_limit_then_count(self):
        sql = diff_table_branch('t1').against('t2').output_limit(5).output_count().compile()
        assert 'output count' in sql
        assert 'output limit' not in sql

    # --- Validation errors ---

    def test_missing_against_raises(self):
        with pytest.raises(ValueError, match="base_table must be set"):
            diff_table_branch('t1').compile()

    def test_empty_target_raises(self):
        with pytest.raises(ValueError, match="target_table"):
            diff_table_branch('')

    def test_empty_base_raises(self):
        with pytest.raises(ValueError, match="base_table"):
            diff_table_branch('t1').against('')

    def test_empty_snapshot_raises(self):
        with pytest.raises(ValueError, match="snapshot_name"):
            diff_table_branch('t1').snapshot('')

    def test_negative_limit_raises(self):
        with pytest.raises(ValueError, match="non-negative integer"):
            diff_table_branch('t1').against('t2').output_limit(-1)

    def test_non_int_limit_raises(self):
        with pytest.raises(ValueError, match="non-negative integer"):
            diff_table_branch('t1').against('t2').output_limit('abc')

    def test_empty_file_path_raises(self):
        with pytest.raises(ValueError, match="path"):
            diff_table_branch('t1').against('t2').output_file('')

    def test_empty_output_as_raises(self):
        with pytest.raises(ValueError, match="table_name"):
            diff_table_branch('t1').against('t2').output_as('')


# ---------------------------------------------------------------------------
# MERGE TABLE BRANCH
# ---------------------------------------------------------------------------


class TestMergeTableBranch:

    def test_default_skip(self):
        sql = merge_table_branch('src').into('tgt').compile()
        assert sql == "data branch merge src into tgt when conflict skip"

    def test_explicit_skip(self):
        sql = merge_table_branch('src').into('tgt').when_conflict('skip').compile()
        assert sql == "data branch merge src into tgt when conflict skip"

    def test_accept(self):
        sql = merge_table_branch('src').into('tgt').when_conflict('accept').compile()
        assert sql == "data branch merge src into tgt when conflict accept"

    def test_enum_strategy(self):
        sql = merge_table_branch('src').into('tgt').when_conflict(MergeConflictStrategy.ACCEPT).compile()
        assert sql == "data branch merge src into tgt when conflict accept"

    def test_qualified_names(self):
        sql = merge_table_branch('db1.src').into('db2.tgt').compile()
        assert 'db1.src' in sql and 'db2.tgt' in sql

    def test_orm_model_with_schema(self):
        sql = merge_table_branch(FakeModelWithSchema).into('tgt').compile()
        assert "data branch merge mydb.fake_table into tgt" in sql

    def test_orm_model_with_schema_as_target(self):
        sql = merge_table_branch('src').into(FakeModelWithSchema).compile()
        assert "data branch merge src into mydb.fake_table" in sql

    def test_strategy_override(self):
        sql = merge_table_branch('src').into('tgt').when_conflict('skip').when_conflict('accept').compile()
        assert 'when conflict accept' in sql

    # --- Validation errors ---

    def test_missing_into_raises(self):
        with pytest.raises(ValueError, match="target_table must be set"):
            merge_table_branch('src').compile()

    def test_empty_source_raises(self):
        with pytest.raises(ValueError, match="source_table"):
            merge_table_branch('')

    def test_empty_target_raises(self):
        with pytest.raises(ValueError, match="target_table"):
            merge_table_branch('src').into('')

    def test_invalid_strategy_raises(self):
        with pytest.raises(ValueError, match="Invalid conflict strategy"):
            merge_table_branch('src').into('tgt').when_conflict('invalid')


# ---------------------------------------------------------------------------
# str / repr
# ---------------------------------------------------------------------------


class TestStringRepresentation:

    def test_str(self):
        stmt = create_table_branch('tgt').from_table('src')
        assert str(stmt) == "data branch create table tgt from src"

    def test_repr(self):
        stmt = delete_table_branch('tbl')
        r = repr(stmt)
        assert 'DeleteTableBranch' in r
        assert 'data branch delete table tbl' in r

    def test_str_complex(self):
        stmt = diff_table_branch('t1').snapshot('s1').against('t2').output_count()
        assert 'output count' in str(stmt)
