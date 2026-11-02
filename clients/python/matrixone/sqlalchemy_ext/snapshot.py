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


def with_snapshot(statement, snapshot_name: str):
    """Return a snapshot-annotated Select without modifying SQLAlchemy globally.

    The source statement remains unchanged. Compile/execute through the
    MatrixOne dialect; unsupported sources fail rather than read live data.
    """
    from .._sql_literals import sql_string_token

    if not isinstance(statement, Select):
        raise TypeError('Snapshot queries require a SQLAlchemy Select statement')
    if not isinstance(snapshot_name, str) or not snapshot_name:
        raise ValueError('snapshot must be a nonempty string')
    sql_string_token(snapshot_name, context='Snapshot')
    return statement._annotate({'matrixone_snapshot': snapshot_name})


# Preserve the private callable for existing imports, without installing it.
_with_snapshot = with_snapshot


def enable_snapshot_method():
    """Opt in to Select.with_snapshot() at application startup.

    This process-wide compatibility method is never installed on import.
    Repeated calls are harmless; a third-party attribute is never overwritten.
    Configure before other threads modify SQLAlchemy's Select class.
    """
    from inspect import getattr_static

    missing = object()
    current = getattr_static(Select, 'with_snapshot', missing)
    if current is with_snapshot:
        return
    if current is not missing:
        raise RuntimeError('Select.with_snapshot is already defined by another owner')
    Select.with_snapshot = with_snapshot


def select(*entities, snapshot=None):
    """Build an ordinary Select, optionally annotated with a MatrixOne snapshot.

    Use ``select(User, snapshot='nightly')`` or
    ``with_snapshot(sqlalchemy.select(User), 'nightly')``. Neither changes
    SQLAlchemy classes. Applications retaining ``stmt.with_snapshot(name)``
    must explicitly call ``enable_snapshot_method()`` once at startup.
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

__all__ = ['select', 'compile_select', 'with_snapshot', 'enable_snapshot_method']
