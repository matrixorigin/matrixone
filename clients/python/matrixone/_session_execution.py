# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Keep rendered SDK literals on the authoritative Session execution path."""

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import TextClause


class _LegacyResultBind:
    """Let the parent retain its result-owned 1.4 connection without branching."""

    def __init__(self, connection):
        self.connection = connection

    def connect(self, **kwargs):
        return self.connection

    def __getattr__(self, name):
        return getattr(self.connection.engine, name)


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


async def _execute_async_with_mode(parent_execute, client, sql, params, bind_arguments, execution_options, kwargs):
    from ._sql_literals import connection_sql_mode

    route = dict(bind_arguments or {})
    route.update(kwargs)
    connection = await parent_execute.__self__.connection(bind_arguments=route)
    mode = await connection.run_sync(connection_sql_mode)
    rendered = client._substitute_parameters(sql, params, sql_mode=mode)
    return await execute_session_sql(
        parent_execute, rendered, bind_arguments=bind_arguments, execution_options=execution_options, **kwargs
    )


def execute_session_sql(
    parent_execute, sql, *, bind_arguments=None, execution_options=None, client=None, params=None, **kwargs
):
    if client is not None:
        if params is not None and '\\' in sql:
            from inspect import iscoroutinefunction

            from ._sql_literals import connection_sql_mode

            if iscoroutinefunction(parent_execute):
                return _execute_async_with_mode(
                    parent_execute, client, sql, params, bind_arguments, execution_options, kwargs
                )
            # Borrow the authoritative Session connection. Normal calls retain
            # parent autoflush/routing; mode discovery is limited to raw SQL
            # with a backslash, never generated parameter payloads.
            session = parent_execute.__self__
            route = dict(bind_arguments or {})
            route.update(kwargs)
            legacy = getattr(session, 'autocommit', False)
            options = {'close_with_result': True} if legacy else {}
            connection = session.connection(bind_arguments=route, **options)
            try:
                rendered = client._substitute_parameters(sql, params, sql_mode=connection_sql_mode(connection))
                if legacy:
                    # Keep the same legacy result-owned connection through the
                    # real execution; the existing result lifecycle closes it.
                    bind = _LegacyResultBind(connection)
                    bind_arguments = dict(bind_arguments or {}, bind=bind)
                    kwargs = dict(kwargs)
                    if 'bind' in kwargs:
                        kwargs['bind'] = bind
                return execute_session_sql(
                    parent_execute,
                    rendered,
                    bind_arguments=bind_arguments,
                    execution_options=execution_options,
                    **kwargs,
                )
            except BaseException:
                if legacy and not connection.closed:
                    connection.close()
                raise
        sql = client._substitute_parameters(sql, params)
    return parent_execute(
        _LiteralSQL(sql),
        bind_arguments=dict(bind_arguments) if bind_arguments is not None else None,
        execution_options=dict(execution_options or {}),
        **kwargs,
    )
