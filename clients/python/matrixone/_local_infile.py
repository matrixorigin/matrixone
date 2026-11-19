# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Statement-scoped LOCAL file authority on SDK-owned physical connections."""

import os
import weakref
from contextlib import contextmanager

from pymysql.constants import CLIENT
from pymysql.err import OperationalError


def connection_options(option):
    if option is None:
        # Negotiate the protocol capability without enabling driver file opens,
        # including the driver's own initialization queries before return.
        return {'local_infile': False, 'client_flag': CLIENT.LOCAL_FILES}
    return {'local_infile': option}


def _abort_driver(driver):
    # PyMySQL.close sends COM_QUIT; a peer waiting for LOCAL bytes could read
    # that packet as file data. Abort the socket without writing anything.
    force_close = getattr(driver, '_force_close', None)
    if force_close is not None:
        force_close()
    else:
        driver.close()  # aiomysql.close aborts its transport synchronously.


def _physical_driver(dbapi_connection):
    driver = getattr(dbapi_connection, 'driver_connection', None)
    if driver is not None:
        return driver
    # SQLAlchemy 1.4's aiomysql DBAPI adapter exposes the native object here;
    # the pool's public driver_connection accessor is not available until the
    # adapter has been returned from do_connect and registered with the pool.
    return getattr(dbapi_connection, '_connection', dbapi_connection)


class _LocalFileGuard:
    def __init__(self, driver):
        self.driver = weakref.proxy(driver)
        self.path = None
        self.consumed = False
        self.reading = False

    def reject(self):
        _abort_driver(self.driver)
        raise OperationalError(2013, 'Unapproved LOCAL file request; connection closed')

    def check(self, packet):
        if packet.is_load_local_packet():
            if self.path is None or self.consumed or packet._data[1:] != self.path:
                self.reject()
            self.consumed = True
            self.driver._local_infile = True
        return packet


def _install_guard(dbapi_connection):
    driver = _physical_driver(dbapi_connection)
    if getattr(driver, '_matrixone_local_guard', None) is not None:
        return
    if not all(hasattr(driver, name) for name in ('_read_query_result', '_read_packet')):
        raise TypeError('Driver cannot provide statement-scoped LOCAL file imports')
    if not hasattr(driver, '_local_infile'):
        from aiomysql.connection import Connection

        if not isinstance(driver, Connection):
            raise TypeError('Driver cannot provide statement-scoped LOCAL file imports')
    driver._local_infile = False
    # Validate both instance-method mutation points before SQLAlchemy owns the
    # connection; unsupported driver shapes must fail closed during setup.
    packet_was_instance = '_read_packet' in vars(driver)
    packet_value = vars(driver).get('_read_packet')
    driver._read_packet = driver._read_packet
    if packet_was_instance:
        driver._read_packet = packet_value
    else:
        del driver._read_packet
    guard = _LocalFileGuard(driver)
    driver_ref = weakref.ref(driver)
    original = driver._read_query_result
    bound = getattr(original, '__self__', None) is driver
    read_function = original.__func__ if bound else original
    import inspect

    if inspect.iscoroutinefunction(read_function):

        async def read(*args, **kwargs):
            driver = driver_ref()
            if driver is None:
                raise ReferenceError('Connection has been released')
            read_result = read_function.__get__(driver, type(driver)) if bound else read_function
            if guard.reading:
                guard.reject()
            guard.reading = True
            packet_was_instance = '_read_packet' in vars(driver)
            packet_value = vars(driver).get('_read_packet')
            read_packet = driver._read_packet
            first = True

            async def packet(*packet_args, **packet_kwargs):
                nonlocal first
                result = await read_packet(*packet_args, **packet_kwargs)
                if first:
                    first = False
                    guard.check(result)
                return result

            driver._read_packet = packet
            try:
                return await read_result(*args, **kwargs)
            finally:
                if packet_was_instance:
                    driver._read_packet = packet_value
                else:
                    del driver._read_packet
                driver._local_infile = False
                guard.reading = False

    else:

        def read(*args, **kwargs):
            driver = driver_ref()
            if driver is None:
                raise ReferenceError('Connection has been released')
            read_result = read_function.__get__(driver, type(driver)) if bound else read_function
            if guard.reading:
                guard.reject()
            guard.reading = True
            packet_was_instance = '_read_packet' in vars(driver)
            packet_value = vars(driver).get('_read_packet')
            read_packet = driver._read_packet
            first = True

            def packet(*packet_args, **packet_kwargs):
                nonlocal first
                result = read_packet(*packet_args, **packet_kwargs)
                if first:
                    first = False
                    guard.check(result)
                return result

            driver._read_packet = packet
            try:
                return read_result(*args, **kwargs)
            finally:
                if packet_was_instance:
                    driver._read_packet = packet_value
                else:
                    del driver._read_packet
                driver._local_infile = False
                guard.reading = False

    driver._read_query_result = read
    driver._matrixone_local_guard = guard


def install_guard(dbapi_connection):
    try:
        _install_guard(dbapi_connection)
    except BaseException as error:
        try:
            _abort_driver(_physical_driver(dbapi_connection))
        except BaseException as cleanup_error:
            error.cleanup_error = cleanup_error
        raise


def configure_engine(engine, option, required_tls=False):
    asynchronous = engine.dialect.driver == 'aiomysql'
    if option is not None and not required_tls and not asynchronous:
        return
    from sqlalchemy import event
    from ._connection_options import connect_required_tls

    def connect(dialect, record, args, kwargs):
        if asynchronous and option is not True:
            from aiomysql.connection import Connection
            from sqlalchemy.dialects.mysql.aiomysql import AsyncAdapt_aiomysql_connection
            from sqlalchemy.util.concurrency import await_only

            async def establish():
                # aiomysql 0.2 has no file-open switch. Install the response
                # guard before its first post-authentication setup query.
                raw = Connection(*args, **kwargs)
                try:
                    install_guard(raw)
                    await raw._connect()
                    return raw
                except BaseException as error:
                    try:
                        _abort_driver(raw)
                    except BaseException as cleanup_error:
                        error.cleanup_error = cleanup_error
                    raise

            return AsyncAdapt_aiomysql_connection(dialect.dbapi, await_only(establish()))
        connection = (
            connect_required_tls(dialect, record, args, kwargs) if required_tls else dialect.connect(*args, **kwargs)
        )
        if option is None:
            install_guard(connection)
        elif asynchronous and option is True:
            # aiomysql 0.2 does not expose the flag, but this connection was
            # explicitly created with its legacy unrestricted LOCAL opt-in.
            _physical_driver(connection)._local_infile = True
        return connection

    event.listen(getattr(engine, 'sync_engine', engine), 'do_connect', connect)


@contextmanager
def local_file_authority(connection, path):
    if path is None:
        yield
        return
    raw = connection.connection.driver_connection
    guard = getattr(raw, '_matrixone_local_guard', None)
    if guard is None:
        # Explicit True retains the driver's original unrestricted opt-in.
        if not getattr(raw, '_local_infile', False):
            raise ValueError('LOCAL file transfer was not negotiated on this connection')
        yield
        return
    if guard.path is not None or guard.reading:
        raise RuntimeError('A LOCAL file statement already owns this connection')
    guard.path, guard.consumed = os.fsencode(path), False
    try:
        yield
    finally:
        guard.path, guard.consumed = None, False
        raw._local_infile = False
