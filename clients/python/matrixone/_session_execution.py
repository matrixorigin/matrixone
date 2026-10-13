# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Execute rendered SDK SQL on the caller's Session transaction."""


def _prepare_driver_sql(session, connection, sql, execution_options):
    # SQLAlchemy 2.1 adds Session defaults; earlier versions have none.
    options = dict(getattr(session, 'execution_options', {}))
    options.update(execution_options or {})
    # Match 1.4 Session.execute: transactional results use Row, while legacy
    # autocommit results must retain their single-use connection cleanup.
    if hasattr(session, 'autocommit'):
        options['future_result'] = not session.autocommit
    effective = dict(connection.get_execution_options())
    effective.update(options)
    # format drivers interpolate even an empty parameter tuple. With
    # no_parameters they receive None and must see the original percent signs.
    if connection.dialect.paramstyle in ('format', 'pyformat') and not effective.get('no_parameters', False):
        sql = sql.replace('%', '%%')
    return sql, options


def execute_session_sql(session, sql, *, bind_arguments=None, execution_options=None):
    connection_options: dict = {'bind_arguments': dict(bind_arguments) if bind_arguments is not None else None}
    if getattr(session, 'autocommit', False):
        connection_options['close_with_result'] = True
    connection = session.connection(**connection_options)
    try:
        sql, options = _prepare_driver_sql(session, connection, sql, execution_options)
        return connection.exec_driver_sql(sql, execution_options=options)
    except BaseException:
        # Driver errors already close single-use connections. Preparation can
        # fail before the driver gets control; release only that same owner.
        if getattr(connection, 'should_close_with_result', False) and not connection.closed:
            connection.close()
        raise


async def execute_async_session_sql(session, sql, *, bind_arguments=None, execution_options=None):
    connection = await session.connection(bind_arguments=dict(bind_arguments) if bind_arguments is not None else None)
    sql, options = _prepare_driver_sql(session.sync_session, connection.sync_connection, sql, execution_options)
    return await connection.exec_driver_sql(sql, execution_options=options)
