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
Snapshot support for SQLAlchemy select() statements

This module provides MatrixOne-specific snapshot query capabilities
integrated with SQLAlchemy's select() API.
"""

from sqlalchemy import select as sqlalchemy_select
from sqlalchemy.sql.selectable import Select


# Extend Select class to add with_snapshot method
def _with_snapshot(self, snapshot_name: str):
    """
    Add snapshot hint to this select statement.

    This method enables point-in-time queries by attaching a snapshot name
    to the select statement. The snapshot hint will be injected into the
    compiled SQL.

    Args:
        snapshot_name: Name of the snapshot to query

    Returns:
        A new Select; the original remains unchanged

    Examples::

        # Chain with other query methods
        stmt = (select(User)
                .where(User.age > 25)
                .with_snapshot('backup_20241025')
                .order_by(User.name))

        # Combine with fulltext search
        stmt = (select(Article)
                .where(boolean_match("title", "content").must("python"))
                .with_snapshot('yesterday_backup')
                .limit(10))
    """
    from .._sql_literals import sql_string_token

    if not isinstance(snapshot_name, str) or not snapshot_name:
        raise ValueError('snapshot must be a nonempty string')
    sql_string_token(snapshot_name, context='Snapshot')
    return self._annotate({'matrixone_snapshot': snapshot_name})


# Monkey-patch the Select class to add with_snapshot method
Select.with_snapshot = _with_snapshot


def select(*entities, snapshot=None):
    """
    Create a SQLAlchemy select statement with optional MatrixOne snapshot support.

    This is a drop-in replacement for SQLAlchemy's select() that adds
    MatrixOne's snapshot query capability for point-in-time queries.

    Args:
        *entities: Tables, columns, or expressions to select (same as SQLAlchemy select())
        snapshot: Optional snapshot name for querying historical data

    Returns:
        SQLAlchemy Select statement with snapshot metadata attached and with_snapshot() method

    Examples::

        from matrixone.sqlalchemy_ext import select
        from matrixone import Client

        client = Client(...)

        # Method 1: Using snapshot parameter
        stmt = select(User, snapshot='backup_20241025').where(User.age > 25)

        # Method 2: Using with_snapshot() chain (recommended)
        stmt = (select(User)
                .where(User.age > 25)
                .with_snapshot('backup_20241025'))

        # Both methods are equivalent and can be used interchangeably

        # With fulltext search
        from matrixone.sqlalchemy_ext import boolean_match
        stmt = (select(Article)
                .where(boolean_match("title", "content").must("python"))
                .with_snapshot('yesterday_backup')
                .order_by(Article.created_at.desc()))

        # Complex query
        stmt = (select(User.department, func.count(User.id))
                .group_by(User.department)
                .having(func.count(User.id) > 5)
                .with_snapshot('year_end_backup')
                .order_by(func.count(User.id).desc()))

        # Execute with client
        sql = compile_select(stmt)
        results = client.execute(sql)

    Note:
        - This function is 100% compatible with SQLAlchemy's select()
        - The snapshot parameter is optional and MatrixOne-specific
        - When no snapshot is specified, behaves exactly like sqlalchemy.select()
        - The with_snapshot() method is available on all select statements
    """
    stmt = sqlalchemy_select(*entities)

    if snapshot is not None:
        stmt = _with_snapshot(stmt, snapshot)

    return stmt


def compile_select(stmt, engine=None):
    """Render typed parameters and all supported snapshot sources consistently.

    Snapshot compilation requires the MatrixOne dialect. An explicitly supplied
    foreign engine remains usable for ordinary statements; it cannot silently
    ignore snapshot metadata.
    """
    from sqlalchemy.sql import visitors

    from .._query_compilation import compile_expression
    from .dialect import MatrixOneDialect

    dialect = engine.dialect if engine is not None else None
    if dialect is not None and not isinstance(dialect, MatrixOneDialect):
        if any(item._annotations.get('matrixone_snapshot') is not None for item in visitors.iterate(stmt)):
            raise ValueError('Snapshot compilation requires a MatrixOne engine')
    return compile_expression(stmt, compiler_dialect=dialect)


# Retain the private import for existing callers, with one rewriting authority.
from .._snapshot_sql import rewrite_snapshot as _inject_snapshot_hint

__all__ = ['select', 'compile_select']
