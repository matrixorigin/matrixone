# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Native async file relations and Arrow batches over the client's aiomysql pool."""

import asyncio
import sys
import uuid
from contextlib import asynccontextmanager
from functools import wraps
from pathlib import Path
from types import SimpleNamespace

from ._telemetry import observe, phase, pool_wait
from .exceptions import annotate_execution_error
from ._analytics_workers import AnalyticsWorkers
from .analytics import (
    AnalyticsCleanupError,
    AnalyticsCommitUnknownError,
    _arrow,
    _ddl_columns,
    _identifier,
    _plan_files,
    _protocol_schema,
    _qualified_table,
    _rows_to_batch,
    _server_staging_path,
    _stage_arrow,
    _table_name,
    _validate_upload,
)

_CLEANUP_TIMEOUT = 5


def _abort(connection):
    """Stop the sole transport before any await can be canceled again."""
    if connection is not None:
        # Reading .connection after invalidation may revalidate it. Only abort
        # the already-owned DBAPI object; never acquire a replacement here.
        raw = connection.sync_connection._dbapi_connection
        driver = getattr(raw, 'driver_connection', None)
        if driver is not None:
            driver.close()


def _tracked(function):
    @wraps(function)
    async def run(owner, *args, **kwargs):
        manager = getattr(owner, '_manager', owner)
        task = asyncio.current_task()
        already_tracked = task in manager._tasks
        manager._tasks.add(task)
        failure = []

        async def execute():
            try:
                return await function(owner, *args, **kwargs)
            except BaseException as error:
                failure.append(error)
                raise

        try:
            return await asyncio.wait_for(execute(), manager.client.query_timeout)
        except asyncio.TimeoutError as error:
            if failure and failure[0] is not error:
                # wait_for translates cancellation into TimeoutError. Retain
                # the write outcome and owned cleanup handles from that task.
                error.__dict__.update(failure[0].__dict__)
                raise error from failure[0]
            raise
        finally:
            if not already_tracked:
                manager._tasks.discard(task)

    return run


@asynccontextmanager
async def _write_transaction_impl(manager, rows=None):
    pool_wait()
    connection = await manager.engine.connect()
    transaction = None
    terminal = True
    execution_phase = 'setup'
    commit_started = False
    committed = False
    old = None
    changed_auto = False
    try:
        async with connection.begin():
            result = await connection.exec_driver_sql(
                'SELECT @@time_zone, @@mo_pk_check_by_dn, @@sql_mode, @@autocommit'
            )
            try:
                old = result.one()
            finally:
                result.close()
        auto = str(old[3]).strip().lower()
        if auto not in ('0', '1', 'false', 'true', 'off', 'on'):
            raise ValueError('Cannot verify server autocommit mode')
        changed_auto = auto in ('1', 'true', 'on')
        transaction = await connection.begin()
        terminal = False
        if changed_auto:
            (await connection.exec_driver_sql('SET autocommit = 0')).close()
            result = await connection.exec_driver_sql('SELECT @@autocommit')
            try:
                if str(result.scalar()).strip().lower() not in ('0', 'false', 'off'):
                    raise ValueError('Server did not establish an autocommit-off transaction')
            finally:
                result.close()
        (await connection.exec_driver_sql("SET time_zone = '+00:00'")).close()
        (await connection.exec_driver_sql('SET mo_pk_check_by_dn = 1')).close()
        modes = [mode for mode in old[2].split(',') if mode]
        if 'STRICT_ALL_TABLES' not in modes:
            modes.append('STRICT_ALL_TABLES')
        (await connection.exec_driver_sql('SET sql_mode = %s', (','.join(modes),))).close()
        execution_phase = 'execute'
        yield connection
        commit_started = True
        try:
            with phase('commit'):
                await transaction.commit()
        except asyncio.CancelledError:
            committed = None
            raise
        except Exception as error:
            committed = None
            raise AnalyticsCommitUnknownError(
                'COMMIT did not return acknowledgement; reconcile before retrying', rows=rows
            ) from error
        committed = True
        terminal = True
    except BaseException as primary:
        annotate_execution_error(
            primary,
            phase='commit' if commit_started else execution_phase,
            rows=rows,
            committed=None if commit_started else False,
        )
        if isinstance(primary, asyncio.CancelledError) or commit_started:
            _abort(connection)
        elif transaction is not None and transaction.is_active:
            try:
                (await connection.exec_driver_sql('ROLLBACK')).close()
                await transaction.rollback()
                terminal = True
            except BaseException as rollback_error:
                primary.rollback_error = rollback_error
                _abort(connection)
        raise
    finally:
        primary = sys.exc_info()[1]
        if not terminal:
            _abort(connection)

        async def cleanup():
            try:
                if terminal and old is not None and not connection.invalidated:
                    try:
                        async with connection.begin():
                            for setting, value in zip(('time_zone', 'mo_pk_check_by_dn', 'sql_mode'), old[:3]):
                                (await connection.exec_driver_sql('SET ' + setting + ' = %s', (value,))).close()
                            if changed_auto:
                                (await connection.exec_driver_sql('SET autocommit = 1')).close()
                    except BaseException as error:
                        _abort(connection)
                        await connection.invalidate()
                        raise AnalyticsCleanupError(
                            'Session cleanup failed after transaction; committed=%s' % committed,
                            committed=committed,
                            rows=rows,
                        ) from error
                elif not terminal:
                    await connection.invalidate()
            finally:
                await connection.close()

        try:
            await manager._cleanup(cleanup(), primary)
        except BaseException as error:
            error.committed = committed
            raise


@asynccontextmanager
async def _write_transaction(manager, rows=None):
    try:
        async with _write_transaction_impl(manager, rows) as connection:
            yield connection
    except BaseException as error:
        annotate_execution_error(error, phase='setup', rows=rows, committed=getattr(error, 'committed', False))
        raise


class AsyncArrowStream:
    """Single-consumer async iterator; enter before fetching, close on early exit."""

    def __init__(
        self, manager, sql, params=None, schema=None, batch_size=65536, network_timeout=None, source_check=None
    ):
        _validate_upload(batch_size, 1)
        if network_timeout is not None:
            from ._connection_options import positive_timeout

            positive_timeout(network_timeout, 'network_timeout')
        self._manager = manager
        self._engine = manager.engine
        if self._engine.dialect.driver != 'aiomysql':
            raise TypeError('Async Arrow streaming requires the aiomysql driver')
        self._sql, self._params, self.schema = sql, params, schema
        self.batch_size = batch_size
        self._timeout = network_timeout if network_timeout is not None else manager.client.query_timeout
        self._source_check = source_check
        self._connection = self._driver = self._cursor = self._lease = None
        self._complete = self._closed = self._entered = self._fetching = False

    async def _io(self, awaitable):
        return await asyncio.wait_for(awaitable, self._timeout)

    @observe('arrow_open')
    @_tracked
    async def __aenter__(self):
        if self._entered or self._closed:
            raise RuntimeError('Arrow stream cannot be entered again')
        self._entered = True
        self._manager._streams.add(self)
        try:
            self._lease = await self._manager._workers.workers.acquire()
            # Optional dependency validation precedes any query/DB side effect;
            # the first PyArrow import can itself perform blocking work.
            await self._lease.run(_arrow)
            if self._manager.engine is not self._engine:
                raise RuntimeError('Client connection changed before stream entry')
            # Reserve CPU capacity before the DB slot. A staged upload never
            # retains CPU capacity while waiting for this stream's connection.
            pool_wait()
            self._connection = await self._engine.connect()
            if self._closed:
                # A driver may finish checkout after shutdown's bounded wait.
                # It must never revive the stream or retain the late DB slot.
                async def close_late_checkout():
                    try:
                        await self._connection.invalidate()
                    finally:
                        await self._connection.close()

                await self._manager._cleanup(close_late_checkout())
                raise RuntimeError('Arrow stream closed during connection checkout')
            raw = await self._connection.get_raw_connection()
            self._driver = raw.driver_connection
            cursor = await self._driver.cursor()
            try:
                await self._io(cursor.execute('SELECT @@time_zone, @@sql_mode'))
                self._old_timezone, self._old_mode = await self._io(cursor.fetchone())
                await self._io(cursor.execute("SET time_zone = '+00:00'"))
                modes = [mode for mode in self._old_mode.split(',') if mode]
                if 'STRICT_ALL_TABLES' not in modes:
                    modes.append('STRICT_ALL_TABLES')
                await self._io(cursor.execute('SET sql_mode = %s', (','.join(modes),)))
            finally:
                await cursor.close()
            await self._check_sources()
            from aiomysql import SSCursor

            self._cursor = await self._driver.cursor(SSCursor)
            with phase('execute'):
                await self._io(self._cursor.execute(self._sql, self._params))
            if self._cursor.description is None:
                raise ValueError('Arrow query must return rows')
            fields = tuple(
                SimpleNamespace(
                    **{
                        key: getattr(field, key)
                        for key in ('name', 'type_code', 'length', 'scale', 'flags', 'charsetnr')
                    }
                )
                for field in self._cursor._result.fields
            )
            # The worker sees copied protocol fields, never the live cursor.
            metadata = SimpleNamespace(_result=SimpleNamespace(fields=fields))
            self._source_schema = await self._lease.run(_protocol_schema, metadata)
            if self.schema is None:
                self.schema = self._source_schema
            if [name.casefold() for name in self.schema.names] != [d[0].casefold() for d in self._cursor.description]:
                raise ValueError('Explicit schema names/order must match query columns')
            return self
        except BaseException as primary:
            await self._close(primary)
            raise

    async def _check_sources(self):
        if self._source_check is not None:
            await self._source_check(self._driver, self._lease, self._io)

    def __aiter__(self):
        return self

    @observe('arrow_fetch')
    @_tracked
    async def __anext__(self):
        if not self._entered:
            raise RuntimeError('Enter the Arrow stream before fetching')
        if self._closed:
            raise StopAsyncIteration
        if self._fetching:
            raise RuntimeError('Arrow stream has a single consumer')
        self._fetching = True
        try:
            with phase('fetch'):
                rows = await self._io(self._cursor.fetchmany(self.batch_size))
            if not rows:
                self._complete = True
                await self._check_sources()
                await self.close()
                raise StopAsyncIteration
            return await self._lease.run(_rows_to_batch, rows, self._source_schema, self.schema)
        except StopAsyncIteration:
            raise
        except BaseException as primary:
            await self._close(primary)
            raise
        finally:
            self._fetching = False

    @observe('arrow_materialize')
    async def to_arrow(self):
        batches = [batch async for batch in self]
        return _arrow().Table.from_batches(batches, schema=self.schema)

    async def _close(self, primary=None):
        if self._closed:
            return
        self._closed = True
        # Synchronous transport abort precedes cursor.close(), which otherwise
        # drains unread rows. Conversion may continue but owns detached rows only.
        if self._driver is not None and not self._complete:
            self._driver.close()
            if self._cursor is not None and self._cursor._result is not None:
                # An unfinished aiomysql result retains its connection, while
                # the connection retains the result. The transport is closed;
                # sever that cycle without draining unread rows.
                self._cursor._result.unbuffered_active = False
                self._cursor._result.connection = None
        if self._lease is not None:
            self._lease.release()
        self._manager._streams.discard(self)

        async def cleanup():
            if self._connection is None:
                return
            try:
                if self._complete:
                    await self._cursor.close()
                    await self._driver.rollback()
                    cursor = await self._driver.cursor()
                    try:
                        await cursor.execute('SET time_zone = %s', (self._old_timezone,))
                        await cursor.execute('SET sql_mode = %s', (self._old_mode,))
                    finally:
                        await cursor.close()
                    await self._driver.rollback()
                else:
                    await self._connection.invalidate()
            except BaseException:
                _abort(self._connection)
                await self._connection.invalidate()
                raise
            finally:
                await self._connection.close()

        await self._manager._cleanup(cleanup(), primary)

    async def close(self):
        await self._close()

    async def __aexit__(self, exc_type, exc, traceback):
        await self._close(exc)


class AsyncFileRelation:
    """Owned ordinary external tables; close is retryable after a DROP failure."""

    def __init__(self, manager, plan):
        self._manager, self._engine, self._plan = manager, manager.engine, plan
        self.schema = plan.schema
        self._tables = []
        self._sql = ''
        self._closed = False
        self._close_task = None

    @property
    def sources(self):
        """Stable, expanded client-side paths; no rediscovery or database I/O."""
        return self._plan.sources

    def _check_open(self):
        if self._closed or self._close_task is not None:
            raise RuntimeError('File relation is closing or closed')

    async def _sources(self, lease=None):
        self._check_open()
        for check in self._plan.checks:
            if lease is None:
                await self._manager._offload(check)
            else:
                await lease.run(check)

    async def _server_sources(self, execute):
        for source in self._plan.server_sources:
            (await execute('SELECT length(load_file(CAST(%s AS DATALINK)))', (source,))).close()

    @_tracked
    async def sql(self):
        """Await validation of client/server sources before obtaining SQL."""
        if self._manager.engine is not self._engine:
            raise RuntimeError('Client connection changed; recreate the relation')
        await self._sources()
        if self._manager.engine is not self._engine:
            raise RuntimeError('Client connection changed during source validation')
        async with self._engine.connect() as connection:
            await self._server_sources(connection.exec_driver_sql)
        if self._manager.engine is not self._engine:
            raise RuntimeError('Client connection changed during source validation')
        return self._sql

    def query(self, sql=None, params=None, schema=None, batch_size=65536, network_timeout=None):
        self._check_open()

        async def source_check(driver, lease, io):
            await self._sources(lease)
            cursor = await driver.cursor()
            try:
                for source in self._plan.server_sources:
                    await io(cursor.execute('SELECT length(load_file(CAST(%s AS DATALINK)))', (source,)))
            finally:
                await cursor.close()

        stream = AsyncArrowStream(
            self._manager,
            sql or 'SELECT * FROM ' + self._sql + ' AS files',
            params,
            self.schema if sql is None and schema is None else schema,
            batch_size,
            network_timeout,
            source_check,
        )
        stream._engine = self._engine
        return stream

    @_tracked
    @observe('file_import')
    async def import_into(self, table):
        await self._sources()
        async with _write_transaction(self._manager) as connection:
            if connection.engine is not self._engine:
                raise RuntimeError('Client connection changed; recreate the relation')
            await self._server_sources(connection.exec_driver_sql)
            result = await connection.exec_driver_sql(
                (
                    'INSERT INTO %s (%s) SELECT * FROM %s AS files'
                    % (_table_name(table), ', '.join(map(_identifier, self.schema.names)), self._sql)
                ).replace('%', '%%')
            )
            rows = result.rowcount
            result.close()
            await self._server_sources(connection.exec_driver_sql)
            return rows

    @_tracked
    async def close(self):
        await self._close()

    async def _close(self, primary=None):
        if self._closed:
            return
        # A timed-out or canceled caller leaves the same DROP owner retained.
        # Retry joins it until completion; only a finished failure starts anew.
        if self._close_task is None or self._close_task.done():
            self._close_task = asyncio.create_task(self._drop_tables())
        await self._manager._cleanup(self._close_task, primary)

    async def _drop_tables(self):
        first_error = None
        for identity in reversed(self._tables[:]):
            try:
                async with self._engine.begin() as connection:
                    (
                        await connection.exec_driver_sql(
                            'DROP TABLE IF EXISTS ' + _qualified_table(*identity).replace('%', '%%')
                        )
                    ).close()
                self._tables.remove(identity)
            except Exception as error:
                if first_error is None:
                    first_error = error
        self._closed = not self._tables
        if first_error is not None:
            raise first_error

    async def __aenter__(self):
        self._check_open()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self._close(exc)


class AsyncAnalyticsManager:
    def __init__(self, client):
        self.client = client
        self._workers = AnalyticsWorkers()
        self._streams = set()
        self._tasks = set()
        self._finalizers = set()

    @property
    def engine(self):
        if self.client._engine is None:
            raise RuntimeError('Connect the MatrixOne client first')
        return self.client._engine

    async def _cleanup(self, coroutine, primary=None):
        task = asyncio.ensure_future(coroutine)

        def completed(done):
            self._finalizers.discard(done)
            if not done.cancelled():
                done.exception()

        if task not in self._finalizers:
            self._finalizers.add(task)
            task.add_done_callback(completed)
        try:
            await asyncio.wait_for(asyncio.shield(task), _CLEANUP_TIMEOUT)
        except BaseException as error:
            if primary is not None:
                primary.cleanup_error = error
                primary.cleanup_pending = not task.done()
            else:
                raise

    async def _offload(self, function, *args, **kwargs):
        lease = await self._workers.workers.acquire()
        try:
            return await lease.run(function, *args, **kwargs)
        finally:
            lease.release()

    def query(self, sql, params=None, schema=None, batch_size=65536, network_timeout=None):
        return AsyncArrowStream(self, sql, params, schema, batch_size, network_timeout)

    async def read_parquet(self, paths, **kwargs):
        return await self.read_files(paths, format='parquet', **kwargs)

    async def read_csv(self, paths, **kwargs):
        return await self.read_files(paths, format='csv', **kwargs)

    async def read_json(self, paths, lines=True, orient='records', **kwargs):
        if lines is not True or orient != 'records':
            raise ValueError('File relations support JSON Lines objects: lines=True, orient=records')
        return await self.read_files(paths, format='jsonline', **kwargs)

    @_tracked
    @observe('file_read')
    async def read_files(self, paths, format, **kwargs):
        engine = self.engine
        plan = await self._workers.run(_plan_files, paths, format, **kwargs)
        if self.engine is not engine:
            raise RuntimeError('Client connection changed during file planning')
        relation = AsyncFileRelation(self, plan)
        selects = []
        try:
            for scan in plan.scans:
                identity = None
                try:
                    async with relation._engine.begin() as connection:
                        if not selects:
                            await relation._server_sources(connection.exec_driver_sql)
                        result = await connection.exec_driver_sql('SELECT DATABASE()')
                        try:
                            identity = result.scalar(), 'mo_py_files_' + uuid.uuid4().hex
                        finally:
                            result.close()
                        relation._tables.append(identity)
                        qualified = _qualified_table(*identity)
                        (await connection.exec_driver_sql(scan.ddl(qualified), scan.params)).close()
                except Exception as error:
                    from sqlalchemy.exc import DBAPIError

                    if (
                        isinstance(error, DBAPIError)
                        and not error.connection_invalidated
                        and getattr(error.orig, 'args', ())
                        and error.orig.args[0] == 1050
                        and identity in relation._tables
                    ):
                        relation._tables.remove(identity)
                    raise
                selects.append('SELECT %s FROM %s' % (scan.projection, qualified))
            relation._sql = '(' + ' UNION ALL '.join(selects) + ')'
            return relation
        except BaseException as primary:
            primary.file_relation = relation
            primary.attempted_tables = tuple(_qualified_table(*identity) for identity in relation._tables)
            await relation._close(primary)
            primary.owned_tables = tuple(_qualified_table(*identity) for identity in relation._tables)
            raise

    @_tracked
    @observe('arrow_upload')
    async def upload_arrow(
        self,
        table,
        data,
        staging_dir,
        server_staging_dir=None,
        schema=None,
        create=True,
        batch_size=65536,
        max_staging_bytes=1024 * 1024 * 1024,
    ):
        target = _table_name(table).replace('%', '%%')
        _validate_upload(batch_size, max_staging_bytes)
        from .capabilities import require_arrow_override, check_arrow_load_async

        require_arrow_override(self.client)
        operation = await self._workers.upload()
        rows = None
        committed = False
        operation_phase = 'staging'
        try:
            staged = await operation.prepare(_stage_arrow, data, staging_dir, schema, batch_size, max_staging_bytes)
            rows = staged.rows
            server_path = _server_staging_path(server_staging_dir or Path(staged.path).parent, Path(staged.path).name)
            async with _write_transaction(self, rows) as connection:
                await check_arrow_load_async(connection)
                operation_phase = 'ddl_table'
                if create:
                    (
                        await connection.exec_driver_sql('CREATE TABLE %s (%s)' % (target, _ddl_columns(staged.schema)))
                    ).close()
                operation_phase = 'load'
                (
                    await connection.exec_driver_sql(
                        "LOAD DATA INFILE {'filepath'=%s, 'format'='arrow'} INTO TABLE " + target, (server_path,)
                    )
                ).close()
            committed = True
            return rows
        except BaseException as primary:
            committed = getattr(primary, 'committed', False)
            primary.operation_id = operation.id
            if getattr(primary, 'phase', None) == 'execute':
                primary.phase = operation_phase
            annotate_execution_error(primary, phase=operation_phase, rows=rows, committed=committed)
            raise
        finally:
            primary = sys.exc_info()[1]
            operation.finish()
            # A canceled producer can still be blocked in foreign I/O. Its own
            # concurrent-future callback performs unlink after it really exits.
            if not operation.status()['worker_running']:
                try:
                    await self._cleanup(operation.wait_cleanup())
                except BaseException as error:
                    if primary is not None:
                        primary.staging_cleanup_error = error
                    else:
                        cleanup = AnalyticsCleanupError(
                            'Staging cleanup failed; committed=%s' % committed, committed=committed, rows=rows
                        )
                        cleanup.operation_id = operation.id
                        raise cleanup from error

    async def upload_dataframe(self, table, dataframe, staging_dir, **kwargs):
        return await self.upload_arrow(table, dataframe, staging_dir, **kwargs)

    def pending_operations(self):
        return self._workers.pending()

    @_tracked
    async def retry_cleanup(self, operation_id):
        await self._workers.retry_cleanup(operation_id)

    async def disconnect(self):
        self._workers.set_open(False)
        tasks = set(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=5)
        for stream in tuple(self._streams):
            await stream.close()
        tasks = self._tasks | self._finalizers
        if tasks:
            await asyncio.wait(tasks, timeout=5)

    def reconnect(self):
        if self._tasks or self._finalizers or self._streams:
            raise RuntimeError('Previous async analytics database work is still closing')
        self._workers.set_open(True)
