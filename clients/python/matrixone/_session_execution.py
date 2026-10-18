# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Keep rendered SDK literals on the authoritative Session execution path."""

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import TextClause


class _LiteralSQL(TextClause):
    inherit_cache = False

    def __init__(self, sql, client=None, params=None):
        super().__init__('')
        self.text = sql
        self._literal_client = client
        self._literal_params = params

    def _render_on_connection(self, connection):
        if self._literal_client is None:
            return self
        from ._sql_literals import connection_sql_mode

        return _LiteralSQL(
            self._literal_client._substitute_parameters(
                self.text, self._literal_params, sql_mode=connection_sql_mode(connection)
            )
        )

    def _execute_on_connection(self, connection, *args):
        try:
            # Session has already applied its defaults. Connection dispatch in
            # both 1.4 and 2.x places per-call execution options last.
            effective = self._execution_options.union(connection.get_execution_options()).union(args[-1])
            sql = self._render_on_connection(connection).text
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
    if statement._literal_client is not None:
        raise ValueError('Mode-sensitive SDK SQL must be executed on a connection before compilation')
    # Hooks can wrap this literal in a compiled statement. Protect standalone
    # bind-free representation without changing the outer compiler's binds or
    # result processing when this is a child expression.
    if compiler.statement is statement:
        compiler.isplaintext = True
        compiler.positional = False
        compiler._numeric_binds = False
    return compiler.post_process_text(statement.text)


@event.listens_for(Engine, 'before_execute', retval=True)
def _render_wrapped_literals(connection, statement, multiparams, params, execution_options):
    # A do_orm_execute hook may turn the SDK literal into a TextualSelect or
    # embed it in a larger expression. This connection event runs after all
    # Session routing hooks, including invoke_statement(), on sync/async engines.
    # Only our deferred leaves are replaced; ordinary SQLAlchemy statements and
    # their bind parameters are returned unchanged.
    deferred = [
        node
        for node in visitors.iterate(statement)
        if isinstance(node, _LiteralSQL) and node._literal_client is not None
    ]
    if not deferred:
        return statement, multiparams, params
    try:
        rendered = {id(node): node._render_on_connection(connection) for node in deferred}
        statement = visitors.replacement_traverse(statement, {}, lambda node: rendered.get(id(node)))
        return statement, multiparams, params
    except BaseException:
        if getattr(connection, 'should_close_with_result', False) and not connection.closed:
            connection.close()
        raise


def execute_session_sql(
    parent_execute, sql, *, bind_arguments=None, execution_options=None, client=None, params=None, **kwargs
):
    if client is not None and params is not None and '\\' in sql:
        # Keep mode-sensitive SQL unrendered until the parent has selected its
        # actual connection. No eager Session.connection() or duplicate routing.
        statement = _LiteralSQL(sql, client, params)
    else:
        statement = _LiteralSQL(client._substitute_parameters(sql, params) if client is not None else sql)
    return parent_execute(
        statement,
        bind_arguments=dict(bind_arguments) if bind_arguments is not None else None,
        execution_options=dict(execution_options or {}),
        **kwargs,
    )
