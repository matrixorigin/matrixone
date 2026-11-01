# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Exercise the SQLAlchemy boundaries rather than replacing context owners."""

import traceback
from unittest.mock import Mock

import pymysql
import pytest
from sqlalchemy import Column, Index, Integer, create_engine, event, inspect
from sqlalchemy.orm import declarative_base

from matrixone import Client
from matrixone.async_client import AsyncClient
from matrixone.exceptions import ConnectionError, QueryError
from matrixone.sqlalchemy_ext import FulltextIndex, VectorIndex


def probe_model(index_kind=None):
    base = declarative_base()

    class Probe(base):
        __tablename__ = 'create_table_boundary'
        id = Column(Integer, primary_key=True)
        payload = Column(Integer)

    if index_kind == 'fulltext':
        FulltextIndex('boundary_index', 'payload')._set_parent(Probe.__table__)
    elif index_kind == 'vector':
        VectorIndex('boundary_index', Probe.payload)
    elif index_kind == 'regular':
        Index('boundary_index', Probe.payload)
    return Probe


def assert_redacted(error, secret, expect_mask=True):
    assert secret not in str(error)
    if expect_mask:
        assert '***' in str(error)
    assert error.__suppress_context__
    assert secret not in ''.join(traceback.format_exception(type(error), error, error.__traceback__))


@pytest.mark.parametrize('secret', ['synthetic-password-boundary', "synthetic-'\\界-boundary"])
@pytest.mark.parametrize('code', [1045, 1049])
def test_create_table_real_lazy_connection_failure_redacted(secret, code):
    attempted = []

    def creator():
        attempted.append(True)
        raise pymysql.err.OperationalError(code, 'driver echoed ' + secret)

    engine = create_engine('mysql+pymysql://', creator=creator)
    client = Client(logger=Mock())
    client._engine = engine
    client._connection_params = {'password': secret, 'database': 'probe'}
    try:
        # begin() is lazy: the real DBAPI failure occurs only on __enter__.
        assert not attempted
        with pytest.raises(ConnectionError) as raised:
            client.create_table(probe_model())
        assert attempted
        assert_redacted(raised.value, secret, expect_mask=code != 1049)
        if code == 1049:
            assert "Database 'probe' does not exist" in str(raised.value)
            assert '-p ' in str(raised.value)
        assert secret not in str(client.logger.mock_calls)
    finally:
        client.disconnect()


@pytest.mark.parametrize('secret', ['synthetic-password-boundary', "synthetic-'\\界-boundary"])
@pytest.mark.parametrize('phase', ['table', 'regular', 'fulltext', 'vector'])
def test_create_table_real_sql_failure_redacted(secret, phase):
    engine = create_engine('sqlite://')
    statements, returned, rolled_back = [], [], []

    @event.listens_for(engine, 'before_cursor_execute')
    def fail_sql(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)
        if phase == 'table' or 'INDEX' in statement.upper():
            raise RuntimeError('driver echoed ' + secret)

    event.listen(engine, 'checkin', lambda *args: returned.append(True))
    event.listen(engine, 'rollback', lambda *args: rolled_back.append(True))
    client = Client(logger=Mock())
    client._engine = engine
    client._connection_params = {'password': secret}
    try:
        model = probe_model(None if phase == 'table' else phase)
        if phase == 'table':
            with pytest.raises(QueryError) as raised:
                client.create_table(model)
            assert_redacted(raised.value, secret)
            assert '***' in str(client.logger.log_error.call_args)
        else:
            # A missing declared index is a failed operation, even after the
            # table DDL was acknowledged. Preserve that partial outcome.
            with pytest.raises(QueryError) as raised:
                client.create_table(model)
            assert_redacted(raised.value, secret)
            assert raised.value.statement_acknowledged is True
            assert getattr(raised.value, 'table_created', None) is None
            assert rolled_back
            assert raised.value.phase == 'ddl_index'
            assert isinstance(raised.value.orig, RuntimeError)
            assert any('INDEX' in sql.upper() for sql in statements)
            assert inspect(engine).has_table(model.__tablename__)
        assert returned, 'the real SQLAlchemy context must return its connection'
        assert secret not in str(client.logger.mock_calls)
    finally:
        client.disconnect()


def test_create_table_real_context_exit_failure_redacted():
    secret = 'synthetic-commit-password-boundary'
    engine = create_engine('sqlite://')
    returned = []

    @event.listens_for(engine, 'commit')
    def fail_commit(conn):
        raise RuntimeError('commit echoed ' + secret)

    event.listen(engine, 'checkin', lambda *args: returned.append(True))
    client = Client(logger=Mock())
    client._engine = engine
    client._connection_params = {'password': secret}
    try:
        with pytest.raises(QueryError) as raised:
            client.create_table(probe_model())
        assert_redacted(raised.value, secret)
        assert returned
        assert secret not in str(client.logger.mock_calls)
    finally:
        client.disconnect()


def test_create_table_logging_failure_redacted(capsys):
    secret = 'synthetic-logger-password-boundary'
    engine = create_engine('sqlite://')

    @event.listens_for(engine, 'before_cursor_execute')
    def fail_sql(*args):
        raise RuntimeError('driver echoed ' + secret)

    client = Client(logger=Mock())
    client._engine = engine
    client._connection_params = {'password': secret}
    client.logger.log_error.side_effect = RuntimeError('logger echoed ' + secret)
    try:
        with pytest.raises(QueryError) as raised:
            client.create_table(probe_model())
        assert_redacted(raised.value, secret)
        assert secret not in capsys.readouterr().err
    finally:
        client.disconnect()


def test_real_sync_callback_is_best_effort(monkeypatch):
    client = Client(logger=Mock())
    engine = create_engine('sqlite://')
    monkeypatch.setattr(client, '_create_engine', lambda: engine)
    monkeypatch.setattr(client, '_detect_backend_version', lambda: None)
    called = []

    def callback(connected_client):
        called.append(connected_client)
        raise RuntimeError('deliberate callback failure')

    try:
        client.connect(database='test', on_connect=callback)
        assert called and all(item is client for item in called)
        assert client.connected()
        assert 'deliberate callback failure' in str(client.logger.warning.mock_calls)
        assert [call.kwargs['success'] for call in client.logger.log_connection.call_args_list] == [True]
    finally:
        client.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('async_callback', [False, True])
async def test_real_async_callback_is_best_effort(monkeypatch, async_callback):
    from sqlalchemy.ext.asyncio import create_async_engine

    client = AsyncClient(logger=Mock())
    engine = create_async_engine('sqlite+aiosqlite://')
    monkeypatch.setattr(client, '_create_async_engine', lambda: engine)
    called = []

    def callback(connected_client):
        called.append(connected_client)
        raise RuntimeError('deliberate callback failure')

    async def callback_async(connected_client):
        callback(connected_client)

    try:
        await client.connect(database='test', on_connect=callback_async if async_callback else callback)
        assert called and all(item is client for item in called)
        assert client.connected()
        assert 'deliberate callback failure' in str(client.logger.warning.mock_calls)
        assert [call.kwargs['success'] for call in client.logger.log_connection.call_args_list] == [True]
    finally:
        await client.disconnect()
