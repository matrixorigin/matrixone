"""
Branch statement builders - SQLAlchemy style API for branch operations.

Similar to select(), insert(), delete(), update() from SQLAlchemy.
These builders produce SQL strings via compile() and are executed
by passing them to client.execute() or session.execute() as strings.
"""

from typing import Optional, Union, Type
from enum import Enum

from .branch import MergeConflictStrategy
from ._utils import get_table_name as _get_table_name, require_non_empty as _require_non_empty


class DiffOutputOption(str, Enum):
    """Diff output format options"""

    COUNT = 'count'
    LIMIT = 'limit'
    FILE = 'file'
    AS = 'as'


class BranchStatement:
    """Base class for branch statements.

    These are NOT SQLAlchemy ClauseElements. They produce raw SQL strings
    via compile() and should be executed as: client.execute(str(stmt))
    """

    def compile(self) -> str:
        """Compile to SQL string."""
        raise NotImplementedError

    def __str__(self) -> str:
        return self.compile()

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.compile()}>"


class CreateTableBranch(BranchStatement):
    """CREATE TABLE BRANCH statement builder."""

    def __init__(self, target_table: Union[str, Type]):
        name = _get_table_name(target_table)
        _require_non_empty(name, "target_table")
        self._target = name
        self._source = None
        self._snapshot = None
        self._account = None

    def from_table(self, source: Union[str, Type], snapshot: Optional[str] = None) -> 'CreateTableBranch':
        """Set source table and optional snapshot."""
        name = _get_table_name(source)
        _require_non_empty(name, "source_table")
        self._source = name
        self._snapshot = snapshot
        return self

    def to_account(self, account: str) -> 'CreateTableBranch':
        """Set target account for cross-tenant branching (sys tenant only)."""
        _require_non_empty(account, "account")
        self._account = account
        return self

    def compile(self) -> str:
        if not self._source:
            raise ValueError("source_table must be set via from_table()")
        sql = f"data branch create table {self._target} from {self._source}"
        if self._snapshot:
            sql += f'{{snapshot="{self._snapshot}"}}'
        if self._account:
            sql += f" to account {self._account}"
        return sql


class CreateDatabaseBranch(BranchStatement):
    """CREATE DATABASE BRANCH statement builder."""

    def __init__(self, target_db: str):
        _require_non_empty(target_db, "target_database")
        self._target = target_db
        self._source = None
        self._snapshot = None
        self._account = None

    def from_database(self, source: str, snapshot: Optional[str] = None) -> 'CreateDatabaseBranch':
        """Set source database and optional snapshot."""
        _require_non_empty(source, "source_database")
        self._source = source
        self._snapshot = snapshot
        return self

    def to_account(self, account: str) -> 'CreateDatabaseBranch':
        """Set target account for cross-tenant branching (sys tenant only)."""
        _require_non_empty(account, "account")
        self._account = account
        return self

    def compile(self) -> str:
        if not self._source:
            raise ValueError("source_database must be set via from_database()")
        sql = f"data branch create database {self._target} from {self._source}"
        if self._snapshot:
            sql += f'{{snapshot="{self._snapshot}"}}'
        if self._account:
            sql += f" to account {self._account}"
        return sql


class DeleteTableBranch(BranchStatement):
    """DELETE TABLE BRANCH statement builder."""

    def __init__(self, table: Union[str, Type]):
        name = _get_table_name(table)
        _require_non_empty(name, "table")
        self._table = name

    def compile(self) -> str:
        return f"data branch delete table {self._table}"


class DeleteDatabaseBranch(BranchStatement):
    """DELETE DATABASE BRANCH statement builder."""

    def __init__(self, database: str):
        _require_non_empty(database, "database")
        self._database = database

    def compile(self) -> str:
        return f"data branch delete database {self._database}"


class DiffTableBranch(BranchStatement):
    """DIFF TABLE BRANCH statement builder."""

    def __init__(self, target_table: Union[str, Type]):
        name = _get_table_name(target_table)
        _require_non_empty(name, "target_table")
        self._target = name
        self._target_snapshot = None
        self._base = None
        self._base_snapshot = None
        self._output_type = None
        self._output_value = None

    def snapshot(self, name: str) -> 'DiffTableBranch':
        """Set snapshot for target table."""
        _require_non_empty(name, "snapshot_name")
        self._target_snapshot = name
        return self

    def against(self, base: Union[str, Type], snapshot: Optional[str] = None) -> 'DiffTableBranch':
        """Set base table to compare against."""
        name = _get_table_name(base)
        _require_non_empty(name, "base_table")
        self._base = name
        self._base_snapshot = snapshot
        return self

    def output_count(self) -> 'DiffTableBranch':
        """Output only count of differences."""
        self._output_type = DiffOutputOption.COUNT
        self._output_value = None
        return self

    def output_limit(self, limit: int) -> 'DiffTableBranch':
        """Limit returned difference rows."""
        if not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        self._output_type = DiffOutputOption.LIMIT
        self._output_value = limit
        return self

    def output_file(self, path: str) -> 'DiffTableBranch':
        """Export differences to file (local path or stage:// URL)."""
        _require_non_empty(path, "path")
        self._output_type = DiffOutputOption.FILE
        self._output_value = path
        return self

    def output_as(self, table_name: str) -> 'DiffTableBranch':
        """Save differences to table (not yet supported by MatrixOne)."""
        _require_non_empty(table_name, "table_name")
        self._output_type = DiffOutputOption.AS
        self._output_value = table_name
        return self

    def compile(self) -> str:
        if not self._base:
            raise ValueError("base_table must be set via against()")
        sql = f"data branch diff {self._target}"
        if self._target_snapshot:
            sql += f'{{snapshot="{self._target_snapshot}"}}'
        sql += f" against {self._base}"
        if self._base_snapshot:
            sql += f'{{snapshot="{self._base_snapshot}"}}'
        if self._output_type == DiffOutputOption.COUNT:
            sql += " output count"
        elif self._output_type == DiffOutputOption.LIMIT:
            sql += f" output limit {self._output_value}"
        elif self._output_type == DiffOutputOption.FILE:
            sql += f" output file '{self._output_value}'"
        elif self._output_type == DiffOutputOption.AS:
            sql += f" output as {self._output_value}"
        return sql


class MergeTableBranch(BranchStatement):
    """MERGE TABLE BRANCH statement builder."""

    def __init__(self, source_table: Union[str, Type]):
        name = _get_table_name(source_table)
        _require_non_empty(name, "source_table")
        self._source = name
        self._target = None
        self._strategy = MergeConflictStrategy.SKIP

    def into(self, target: Union[str, Type]) -> 'MergeTableBranch':
        """Set target table to merge into."""
        name = _get_table_name(target)
        _require_non_empty(name, "target_table")
        self._target = name
        return self

    def when_conflict(self, strategy: Union[str, MergeConflictStrategy]) -> 'MergeTableBranch':
        """Set conflict resolution strategy: 'skip' or 'accept'."""
        if isinstance(strategy, str):
            try:
                strategy = MergeConflictStrategy(strategy)
            except ValueError:
                raise ValueError(f"Invalid conflict strategy: '{strategy}'. Must be 'skip' or 'accept'.")
        self._strategy = strategy
        return self

    def compile(self) -> str:
        if not self._target:
            raise ValueError("target_table must be set via into()")
        return f"data branch merge {self._source} into {self._target} " f"when conflict {self._strategy.value}"


# ---------------------------------------------------------------------------
# Top-level builder functions (like select(), insert(), etc.)
# ---------------------------------------------------------------------------


def create_table_branch(target_table: Union[str, Type]) -> CreateTableBranch:
    """Build a CREATE TABLE BRANCH statement.

    Example::

        stmt = create_table_branch('dev').from_table('prod', snapshot='snap1')
        client.execute(str(stmt))
    """
    return CreateTableBranch(target_table)


def create_database_branch(target_db: str) -> CreateDatabaseBranch:
    """Build a CREATE DATABASE BRANCH statement.

    Example::

        stmt = create_database_branch('dev_db').from_database('prod_db')
        client.execute(str(stmt))
    """
    return CreateDatabaseBranch(target_db)


def delete_table_branch(table: Union[str, Type]) -> DeleteTableBranch:
    """Build a DELETE TABLE BRANCH statement.

    Example::

        stmt = delete_table_branch('branch_table')
        client.execute(str(stmt))
    """
    return DeleteTableBranch(table)


def delete_database_branch(database: str) -> DeleteDatabaseBranch:
    """Build a DELETE DATABASE BRANCH statement.

    Example::

        stmt = delete_database_branch('branch_db')
        client.execute(str(stmt))
    """
    return DeleteDatabaseBranch(database)


def diff_table_branch(target_table: Union[str, Type]) -> DiffTableBranch:
    """Build a DIFF TABLE BRANCH statement.

    Example::

        stmt = diff_table_branch('t1').against('t2').output_count()
        client.execute(str(stmt))
    """
    return DiffTableBranch(target_table)


def merge_table_branch(source_table: Union[str, Type]) -> MergeTableBranch:
    """Build a MERGE TABLE BRANCH statement.

    Example::

        stmt = merge_table_branch('source').into('target').when_conflict('accept')
        client.execute(str(stmt))
    """
    return MergeTableBranch(source_table)
