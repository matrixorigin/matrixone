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

import re
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
        Self for method chaining
    
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
    self._matrixone_snapshot = snapshot_name
    return self


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
    
    if snapshot:
        # Attach snapshot name as custom attribute
        stmt._matrixone_snapshot = snapshot
    
    return stmt


def compile_select(stmt, engine=None):
    """
    Compile a SQLAlchemy select statement to SQL with MatrixOne snapshot support.
    
    This function compiles a select statement and injects MatrixOne snapshot hints
    if the statement has snapshot metadata attached.
    
    Args:
        stmt: SQLAlchemy Select statement (possibly created with snapshot parameter)
        engine: Optional SQLAlchemy engine for dialect-specific compilation
    
    Returns:
        SQL string with snapshot hint injected if applicable
    
    Examples::
    
        from matrixone.sqlalchemy_ext import select, compile_select
        
        # Create statement with snapshot
        stmt = select(User, snapshot='backup').where(User.age > 25)
        
        # Compile to SQL
        sql = compile_select(stmt)
        # Result: "SELECT users.id, users.name FROM users {snapshot = 'backup'} WHERE users.age > 25"
        
        # Use with client
        results = client.execute(sql)
    
    Note:
        The snapshot hint is injected after the first table name in the FROM clause
        using MatrixOne's snapshot query syntax: {snapshot = 'snapshot_name'}
    """
    # Compile to standard SQL
    if engine:
        compiled = stmt.compile(bind=engine, compile_kwargs={"literal_binds": True})
    else:
        compiled = stmt.compile(compile_kwargs={"literal_binds": True})
    
    sql = str(compiled)
    
    # Inject snapshot hint if present
    if hasattr(stmt, '_matrixone_snapshot'):
        sql = _inject_snapshot_hint(sql, stmt._matrixone_snapshot)
    
    return sql


def _inject_snapshot_hint(sql: str, snapshot_name: str) -> str:
    """
    Inject MatrixOne snapshot hint into SQL query.
    
    Finds the first table after FROM clause and injects the snapshot hint.
    
    Args:
        sql: SQL query string
        snapshot_name: Name of the snapshot
    
    Returns:
        SQL with snapshot hint injected
    
    Example:
        Input:  "SELECT * FROM users WHERE age > 25"
        Output: "SELECT * FROM users {snapshot = 'backup'} WHERE age > 25"
    """
    # Pattern to find first table after FROM
    pattern = r'(\bFROM\s+)(\w+)(\s|,|$)'
    
    def replace_func(match):
        return f"{match.group(1)}{match.group(2)} {{snapshot = '{snapshot_name}'}}{match.group(3)}"
    
    # Inject snapshot hint after first table
    return re.sub(pattern, replace_func, sql, count=1)


__all__ = [
    'select',
    'compile_select',
]

