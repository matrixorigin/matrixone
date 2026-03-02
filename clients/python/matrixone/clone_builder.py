"""
Clone statement builders - SQLAlchemy style API for clone operations.

These builders produce SQL strings via compile() and are executed
by passing them to client.execute() or session.execute() as strings.
"""

from typing import Optional, Union, Type

from ._utils import get_table_name as _get_table_name, require_non_empty as _require_non_empty


class CloneStatement:
    """Base class for clone statements.

    These produce raw SQL strings via compile() and should be
    executed as: client.execute(str(stmt))
    """

    def compile(self) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        return self.compile()

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.compile()}>"


class CloneTable(CloneStatement):
    """CREATE TABLE ... CLONE statement builder."""

    def __init__(self, target_table: Union[str, Type]):
        name = _get_table_name(target_table)
        _require_non_empty(name, "target_table")
        self._target = name
        self._source = None
        self._snapshot = None
        self._account = None
        self._if_not_exists = False

    def from_table(self, source: Union[str, Type], snapshot: Optional[str] = None) -> 'CloneTable':
        """Set source table and optional snapshot."""
        name = _get_table_name(source)
        _require_non_empty(name, "source_table")
        self._source = name
        self._snapshot = snapshot
        return self

    def if_not_exists(self) -> 'CloneTable':
        """Add IF NOT EXISTS clause."""
        self._if_not_exists = True
        return self

    def to_account(self, account: str) -> 'CloneTable':
        """Set target account for cross-tenant cloning (sys tenant only)."""
        _require_non_empty(account, "account")
        self._account = account
        return self

    def compile(self) -> str:
        if not self._source:
            raise ValueError("source_table must be set via from_table()")
        ine = "IF NOT EXISTS " if self._if_not_exists else ""
        sql = f"CREATE TABLE {ine}{self._target} CLONE {self._source}"
        if self._snapshot:
            sql += f' {{snapshot = "{self._snapshot}"}}'
        if self._account:
            sql += f" TO ACCOUNT {self._account}"
        return sql


class CloneDatabase(CloneStatement):
    """CREATE DATABASE ... CLONE statement builder."""

    def __init__(self, target_db: str):
        _require_non_empty(target_db, "target_database")
        self._target = target_db
        self._source = None
        self._snapshot = None
        self._account = None
        self._if_not_exists = False

    def from_database(self, source: str, snapshot: Optional[str] = None) -> 'CloneDatabase':
        """Set source database and optional snapshot."""
        _require_non_empty(source, "source_database")
        self._source = source
        self._snapshot = snapshot
        return self

    def if_not_exists(self) -> 'CloneDatabase':
        """Add IF NOT EXISTS clause."""
        self._if_not_exists = True
        return self

    def to_account(self, account: str) -> 'CloneDatabase':
        """Set target account for cross-tenant cloning (sys tenant only)."""
        _require_non_empty(account, "account")
        self._account = account
        return self

    def compile(self) -> str:
        if not self._source:
            raise ValueError("source_database must be set via from_database()")
        ine = "IF NOT EXISTS " if self._if_not_exists else ""
        sql = f"CREATE DATABASE {ine}{self._target} CLONE {self._source}"
        if self._snapshot:
            sql += f' {{snapshot = "{self._snapshot}"}}'
        if self._account:
            sql += f" TO ACCOUNT {self._account}"
        return sql


# ---------------------------------------------------------------------------
# Top-level builder functions (like select(), insert(), etc.)
# ---------------------------------------------------------------------------


def clone_table(target_table: Union[str, Type]) -> CloneTable:
    """Build a CREATE TABLE ... CLONE statement.

    Example::

        stmt = clone_table('users_copy').from_table('users')
        client.execute(str(stmt))
    """
    return CloneTable(target_table)


def clone_database(target_db: str) -> CloneDatabase:
    """Build a CREATE DATABASE ... CLONE statement.

    Example::

        stmt = clone_database('dev_db').from_database('prod_db')
        client.execute(str(stmt))
    """
    return CloneDatabase(target_db)
