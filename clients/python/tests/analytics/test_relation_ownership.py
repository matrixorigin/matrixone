# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import re
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_adversarial import hard_client

from matrixone.analytics import AnalyticsManager


def qualified(identity):
    return '.'.join('`' + part.replace('`', '``') + '`' for part in identity)


class FaultEngine:
    def __init__(self, phase, cleanup_failure, databases=('a', 'b')):
        self.phase = phase
        self.cleanup_failure = cleanup_failure
        self.catalog = set()
        self.operations = []
        self.connections = 0
        self.databases = databases
        self.primary = ConnectionError('CREATE acknowledgement unavailable')
        self.cleanup = PermissionError('DROP unavailable')

    def connect(self):
        return self.begin()

    @contextmanager
    def begin(self):
        self.connections += 1
        if self.phase == 'acquire' and self.connections == 1:
            raise self.primary
        connection_id = self.connections
        operation = []

        def execute(sql, params=None):
            if sql.startswith('SELECT length(load_file('):
                return SimpleNamespace(scalar=lambda: 1)
            if sql == 'SELECT DATABASE()':
                self.operations.append(('DATABASE', None, connection_id))
                if self.phase == 'database' or (self.phase == 'database_second' and connection_id == 2):
                    raise self.primary
                database = self.databases[min(connection_id - 1, len(self.databases) - 1)]
                return SimpleNamespace(scalar=lambda: database)
            target = sql.split(' (', 1)[0] if sql.startswith('CREATE') else sql
            name = tuple(s.replace('``', '`').replace('%%', '%') for s in re.findall(r'`((?:``|[^`])*)`', target))
            assert len(name) == 2, sql
            action = 'CREATE' if sql.startswith('CREATE') else 'DROP'
            operation.append(action)
            self.operations.append((action, name, connection_id))
            if action == 'CREATE':
                if self.phase == 'execute':
                    raise self.primary
                self.catalog.add(name)
            else:
                if self.cleanup_failure is True:
                    raise self.cleanup
                self.catalog.discard(name)

        yield SimpleNamespace(exec_driver_sql=execute)
        if operation and operation[0] == 'CREATE' and self.phase == 'commit':
            raise self.primary
        if operation and operation[0] == 'DROP' and self.cleanup_failure == 'commit':
            raise self.cleanup
        if operation and operation[0] == 'CREATE' and self.phase == 'collision':
            from pymysql.err import OperationalError as DriverError
            from sqlalchemy.exc import OperationalError

            raise OperationalError('COMMIT', None, DriverError(1050, 'Concurrent table already exists'))


@pytest.mark.parametrize(
    'phase,cleanup_failure',
    [
        ('normal', False),
        ('acquire', False),
        ('database', False),
        ('execute', False),
        ('commit', False),
        ('execute', True),
        ('commit', True),
        ('commit', 'commit'),
    ],
)
def test_relation_construction_ownership_faults(tmp_path, phase, cleanup_failure):
    path = tmp_path / 'input.parquet'
    pq.write_table(pa.table({'id': [1]}), path)
    engine = FaultEngine(phase, cleanup_failure)
    logger = Mock()
    logger.warning.side_effect = RuntimeError('logging unavailable')
    client = SimpleNamespace(_engine=engine, logger=logger)
    manager = AnalyticsManager(client)
    if phase == 'normal':
        relation = manager.read_parquet(path)
        assert len(engine.catalog) == 1
        engine.cleanup_failure = False
        relation.close()
        assert not engine.catalog
        assert [op[0] for op in engine.operations] == ['DATABASE', 'CREATE', 'DROP']
        return
    with pytest.raises(ConnectionError) as caught:
        manager.read_parquet(path)
    error = caught.value
    assert error is engine.primary
    attempted = error.attempted_tables
    if phase in ('acquire', 'database'):
        assert attempted == error.owned_tables == ()
        assert all(op[0] == 'DATABASE' for op in engine.operations) and error.file_relation._closed
        return
    assert len(attempted) == 1
    operations = [op for op in engine.operations if op[0] != 'DATABASE']
    assert [op[0] for op in operations] == ['CREATE', 'DROP']
    assert operations[0][1] == operations[1][1]
    assert qualified(operations[0][1]) == attempted[0]
    assert operations[0][2] != operations[1][2]
    if cleanup_failure:
        assert error.cleanup_error is engine.cleanup
        assert error.owned_tables == attempted
        replacement = FaultEngine('normal', False)
        pending = set(engine.catalog)
        replacement.catalog.update(pending)
        client._engine = replacement
        engine.cleanup_failure = False
        error.file_relation.close()
        assert engine.operations[-1][0] == 'DROP'
        assert replacement.catalog == pending
        assert replacement.operations == []
    else:
        assert error.owned_tables == ()
        assert error.file_relation._closed
    assert not engine.catalog


def test_commit_collision_does_not_drop_foreign_table(tmp_path):
    from sqlalchemy.exc import OperationalError

    path = tmp_path / 'input.parquet'
    pq.write_table(pa.table({'id': [1]}), path)
    engine = FaultEngine('collision', False)
    manager = AnalyticsManager(SimpleNamespace(_engine=engine, logger=Mock()))
    with pytest.raises(OperationalError) as caught:
        manager.read_parquet(path)
    assert caught.value.owned_tables == ()
    assert len(engine.catalog) == 1
    assert [operation[0] for operation in engine.operations] == ['DATABASE', 'CREATE']


@pytest.mark.parametrize('database', [None, '', 'x' * 65])
def test_invalid_current_database_does_not_reserve_or_create(tmp_path, database):
    path = tmp_path / 'input.parquet'
    pq.write_table(pa.table({'id': [1]}), path)
    engine = FaultEngine('normal', False, (database,))
    manager = AnalyticsManager(SimpleNamespace(_engine=engine, logger=Mock()))
    with pytest.raises(ValueError) as caught:
        manager.read_parquet(path)
    assert caught.value.attempted_tables == caught.value.owned_tables == ()
    assert caught.value.file_relation._closed
    assert [op[0] for op in engine.operations] == ['DATABASE']
    assert not engine.catalog


def test_each_source_captures_its_connection_database(tmp_path):
    paths = [tmp_path / name for name in ['first.parquet', 'second.parquet']]
    for path in paths:
        pq.write_table(pa.table({'id': [1]}), path)
    databases = ('first.db%`quoted', 'second')
    engine = FaultEngine('normal', False, databases)
    manager = AnalyticsManager(SimpleNamespace(_engine=engine, logger=Mock()))
    relation = manager.read_parquet(paths)
    try:
        identities = set(engine.catalog)
        assert {identity[0] for identity in identities} == set(databases)
        assert all(qualified(identity) in relation.sql for identity in identities)
        # A foreign same-name table in another database is never owned.
        foreign = {('foreign', identity[1]) for identity in identities}
        engine.catalog.update(foreign)
        relation.close()
        assert engine.catalog == foreign
    finally:
        engine.cleanup_failure = False
        relation.close()


@pytest.mark.parametrize('lookup_error', [False, True])
def test_failed_second_database_lookup_cleans_prior_identity(tmp_path, lookup_error):
    paths = [tmp_path / name for name in ['first.parquet', 'second.parquet']]
    for path in paths:
        pq.write_table(pa.table({'id': [1]}), path)
    engine = FaultEngine('database_second' if lookup_error else 'normal', False, ('a', None))
    manager = AnalyticsManager(SimpleNamespace(_engine=engine, logger=Mock()))
    with pytest.raises(ConnectionError if lookup_error else ValueError) as caught:
        manager.read_parquet(paths)
    assert len(caught.value.attempted_tables) == 1
    assert caught.value.owned_tables == () and caught.value.file_relation._closed
    assert [op[0] for op in engine.operations] == ['DATABASE', 'CREATE', 'DATABASE', 'DROP']
    assert engine.operations[1][1] == engine.operations[-1][1]
    assert not engine.catalog


@pytest.mark.parametrize(
    'body_error,logger_error', [(False, None), (True, None), (True, RuntimeError), (True, KeyboardInterrupt)]
)
def test_context_cleanup_preserves_primary_and_retry(tmp_path, body_error, logger_error):
    path = tmp_path / 'input.parquet'
    pq.write_table(pa.table({'id': [1]}), path)
    engine = FaultEngine('normal', True)
    logger = Mock()
    if logger_error:
        logger.warning.side_effect = logger_error('logging unavailable')
    relation = AnalyticsManager(SimpleNamespace(_engine=engine, logger=logger)).read_parquet(path)
    primary = ValueError('query failed')
    try:
        with pytest.raises(ValueError if body_error else PermissionError) as caught:
            with relation:
                if body_error:
                    raise primary
        assert caught.value is (primary if body_error else engine.cleanup)
        if body_error:
            assert primary.cleanup_error is engine.cleanup
        assert not relation._closed and len(engine.catalog) == 1
        engine.cleanup_failure = False
        relation.close()
        assert relation._closed and not engine.catalog
    finally:
        engine.cleanup_failure = False
        relation.close()


def test_relation_operations_keep_original_engine(monkeypatch):
    import matrixone.analytics as module

    original, replacement = object(), object()
    client = SimpleNamespace(_engine=original, query_timeout=7, logger=Mock())
    relation = module.FileRelation(AnalyticsManager(client), pa.schema([('id', pa.int64())]), 'source', [])
    client._engine = replacement
    stream = Mock()
    monkeypatch.setattr(module, 'ArrowStream', stream)
    relation.query()
    assert stream.call_args.args[0] is original and stream.call_args.args[-1] == 7
    relation.query(network_timeout=2)
    assert stream.call_args.args[0] is original and stream.call_args.args[-1] == 2
    writes = []

    @contextmanager
    def transaction(engine):
        assert engine is original
        yield SimpleNamespace(exec_driver_sql=lambda sql: writes.append(sql) or SimpleNamespace(rowcount=2))

    monkeypatch.setattr(module, '_write_transaction', transaction)
    assert relation.import_into('target') == 2
    assert writes == ['INSERT INTO `target` (`id`) SELECT * FROM source AS files']
    relation.close()


@pytest.mark.parametrize('switch_before_create', [False, True])
def test_public_client_use_preserves_relation_and_foreign_table(hard_client, tmp_path, switch_before_create):
    import os
    import uuid

    from matrixone import Client

    databases = ['ownership_' + uuid.uuid4().hex[:16] for _ in range(2)]
    created = []
    client = None
    relation = None
    path = tmp_path / 'input.parquet'
    pq.write_table(pa.table({'id': [1, 2]}), path)
    try:
        for database in databases:
            hard_client.execute('CREATE DATABASE ' + qualified((database,)))
            created.append(database)
        client = Client(
            host=os.environ.get('MO_HOST', '127.0.0.1'),
            port=int(os.environ.get('MO_PORT', '6001')),
            user=os.environ.get('MO_USER', 'root'),
            password=os.environ.get('MO_PASSWORD', '111'),
            database=databases[0],
            pool_size=1,
            max_overflow=0,
            sql_log_mode='off',
        )
        owner, foreign = databases[::-1] if switch_before_create else databases
        if switch_before_create:
            client.execute('USE ' + qualified((owner,)))
        relation = client.analytics.read_parquet(path)
        tables = hard_client.execute('SHOW TABLES FROM ' + qualified((owner,))).rows
        assert len(tables) == 1
        table = tables[0][0]
        foreign_table = qualified((foreign, table))
        hard_client.execute('CREATE TABLE ' + foreign_table + ' (id BIGINT)')
        hard_client.execute('INSERT INTO ' + foreign_table + ' VALUES (42)')
        client.execute('USE ' + qualified((foreign,)))
        with relation.query() as stream:
            assert stream.to_arrow()['id'].to_pylist() == [1, 2]
        relation.close()
        assert hard_client.execute('SHOW TABLES FROM ' + qualified((owner,))).rows == []
        assert hard_client.execute('SELECT id FROM ' + foreign_table).rows == [(42,)]
        assert client._engine.pool.checkedout() == 0
    finally:
        if relation is not None:
            relation.close()
        if client is not None:
            client.disconnect()
        for database in reversed(created):
            hard_client.execute('DROP DATABASE IF EXISTS ' + qualified((database,)))


def test_stream_source_error_keeps_precedence_over_cleanup():
    from matrixone.analytics import ArrowStream

    primary = PermissionError('source no longer readable')
    cleanup = ConnectionError('connection reset failed')
    stream = ArrowStream.__new__(ArrowStream)
    stream._closed = False
    stream._complete = False
    stream.batch_size = 1
    stream._cursor = SimpleNamespace(fetchmany=lambda size: [])
    stream._check_sources = Mock(side_effect=primary)
    stream.close = Mock(side_effect=cleanup)
    with pytest.raises(PermissionError) as caught:
        next(stream)
    assert caught.value is primary
    assert primary.cleanup_error is cleanup
    stream.close.assert_called_once()


def test_stream_validation_cursor_close_keeps_primary_error():
    from matrixone.analytics import ArrowStream

    primary = PermissionError('source denied')
    cleanup = ConnectionError('validation cursor close failed')
    cursor = SimpleNamespace(execute=Mock(), close=Mock(side_effect=cleanup))
    stream = ArrowStream.__new__(ArrowStream)
    stream._raw = SimpleNamespace(cursor=lambda: cursor)
    stream._source_check = Mock(side_effect=primary)
    with pytest.raises(PermissionError) as caught:
        stream._check_sources()
    assert caught.value is primary
    assert primary.cursor_cleanup_error is cleanup
    cursor.close.assert_called_once()
