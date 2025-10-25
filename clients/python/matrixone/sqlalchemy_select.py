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
SQLAlchemy 2.0 Select Support for MatrixOne

This module provides utilities and examples for using SQLAlchemy 2.0's select()
with MatrixOne-specific features like fulltext search and vector search.

The approach is simple: use SQLAlchemy's native select() directly, and MatrixOne
features work seamlessly because they're implemented as SQLAlchemy expressions.

Examples::

    from sqlalchemy import select, func
    from matrixone.sqlalchemy_ext import boolean_match
    
    # Basic select
    stmt = select(User).where(User.age > 25)
    
    # With fulltext search (MatrixOne-specific)
    stmt = select(Article).where(
        boolean_match("title", "content").must("python").encourage("tutorial")
    )
    
    # With vector search (MatrixOne-specific)
    query_vector = [0.1, 0.2, ...]
    stmt = (select(Document, Document.embedding.l2_distance(query_vector).label('distance'))
           .order_by(Document.embedding.l2_distance(query_vector))
           .limit(10))
    
    # Execute with session or engine
    results = session.execute(stmt).scalars().all()
    
    # Or export to stage
    client.export.to_stage(query=stmt, stage_name="my_stage", filename="export.csv")
"""

from typing import Any, Optional
from sqlalchemy import select as sqlalchemy_select


# Re-export SQLAlchemy's select for convenience
# This makes it clear we're using the standard SQLAlchemy select
select = sqlalchemy_select


def compile_select_to_sql(stmt, engine=None) -> str:
    """
    Compile a SQLAlchemy select statement to SQL string.
    
    This is useful when you need to pass the SQL to APIs that expect
    a string (like export APIs).
    
    Args:
        stmt: SQLAlchemy select statement
        engine: Optional SQLAlchemy engine for dialect-specific compilation
        
    Returns:
        SQL string
        
    Examples::
    
        from sqlalchemy import select
        from matrixone.sqlalchemy_ext import boolean_match
        
        stmt = select(Article).where(
            boolean_match("title", "content").must("python")
        )
        
        # Compile to SQL
        sql = compile_select_to_sql(stmt)
        
        # Use with export
        client.export.to_stage(query=sql, stage_name="my_stage", filename="export.csv")
    """
    if engine:
        compiled = stmt.compile(bind=engine, compile_kwargs={"literal_binds": True})
    else:
        compiled = stmt.compile(compile_kwargs={"literal_binds": True})
    
    return str(compiled)


__all__ = [
    "select",
    "compile_select_to_sql",
]

