# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Keep rendered SDK literals on the authoritative Session execution path."""

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import TextClause


class _LiteralSQL(TextClause):
    inherit_cache = False

    def __init__(self, sql):
        super().__init__('')
        self.text = sql

    def _execute_on_connection(self, connection, *args):
        try:
            # Session has already applied its defaults. Connection dispatch in
            # both 1.4 and 2.x places per-call execution options last.
            effective = self._execution_options.union(connection.get_execution_options()).union(args[-1])
            sql = self.text
            if connection.dialect.paramstyle in ('format', 'pyformat') and not effective.get('no_parameters', False):
                sql = sql.replace('%', '%%')
            return connection.exec_driver_sql(sql, execution_options=effective)
        except BaseException:
            # Only legacy autocommit connections are result-owned. Preparation
            # can fail before the driver gives that owner control.
            if getattr(connection, 'should_close_with_result', False) and not connection.closed:
                connection.close()
            raise


@compiles(_LiteralSQL)
def _compile_literal_sql(statement, compiler, **kwargs):
    # Hooks can wrap this literal in a compiled statement. Protect standalone
    # bind-free representation without changing the outer compiler's binds or
    # result processing when this is a child expression.
    if compiler.statement is statement:
        compiler.isplaintext = True
        compiler.positional = False
        compiler._numeric_binds = False
    return compiler.post_process_text(statement.text)


def execute_session_sql(parent_execute, sql, *, bind_arguments=None, execution_options=None, **kwargs):
    return parent_execute(
        _LiteralSQL(sql),
        bind_arguments=dict(bind_arguments) if bind_arguments is not None else None,
        execution_options=dict(execution_options or {}),
        **kwargs,
    )
