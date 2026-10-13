# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import json
import os
import uuid
from unittest.mock import Mock

import pytest
from sqlalchemy import event

from matrixone import Client
from matrixone.async_client import AsyncClient
from matrixone.exceptions import QueryError
from matrixone.vector_manager import AsyncVectorManager, VectorManager

pytestmark = pytest.mark.skipif(os.environ.get('MO_SDK_ONLINE') != '1', reason='isolated server required')


def params():
    return {
        'host': os.environ.get('MO_HOST', '127.0.0.1'),
        'port': int(os.environ.get('MO_PORT', '6001')),
        'user': os.environ.get('MO_USER', 'root'),
        'password': os.environ.get('MO_PASSWORD', '111'),
        'database': os.environ.get('MO_DATABASE', 'mo_sdk_regression'),
    }


@pytest.fixture
def client():
    c = Client(**params(), logger=Mock())
    try:
        yield c
        assert c._engine.pool.checkedout() == 0
    finally:
        c.disconnect()


@pytest.fixture
def table(client):
    name = 'sdk_batch_' + uuid.uuid4().hex[:12]
    client.execute('CREATE TABLE ' + name + '(id INT PRIMARY KEY, embedding VECF32(2))')
    try:
        yield name
    finally:
        client.execute('DROP TABLE ' + name)


def rows(n):
    return [{'id': i, 'embedding': [float(i), float(i + 1)]} for i in range(n)]


def test_native_batch_values_and_roundtrip_count(client, table):
    inserts = []

    def capture(conn, cursor, sql, args, context, many):
        if sql.startswith('INSERT INTO ' + table):
            inserts.append(sql)

    event.listen(client._engine, 'before_cursor_execute', capture)
    VectorManager(client).batch_insert(table, rows(101), batch_size=16)
    assert len(inserts) == 7
    result = client.execute('SELECT id,embedding FROM ' + table + ' ORDER BY id').rows
    assert [r[0] for r in result] == list(range(101))
    assert [json.loads(r[1]) for r in result] == [r['embedding'] for r in rows(101)]


def test_native_late_duplicate_rolls_back_all_chunks(client, table):
    data = rows(10) + [{'id': 1, 'embedding': [1, 2]}]
    with pytest.raises(QueryError):
        VectorManager(client).batch_insert(table, data, batch_size=3)
    assert client.execute('SELECT count(*) FROM ' + table).rows == [(0,)]


def test_native_existing_session_retains_rollback(client, table):
    with pytest.raises(RuntimeError):
        with client.session() as session:
            VectorManager(client, executor=session).batch_insert(table, rows(10), batch_size=3)
            raise RuntimeError('caller rollback')
    assert client.execute('SELECT count(*) FROM ' + table).rows == [(0,)]


@pytest.mark.asyncio
async def test_async_native_batch_and_late_rollback(client, table):
    c = AsyncClient(logger=Mock())
    await c.connect(**params(), on_connect=[])
    try:
        manager = AsyncVectorManager(c)
        await manager.batch_insert(table, rows(101), batch_size=16)
        assert (await c.execute('SELECT count(*) FROM ' + table)).rows == [(101,)]
        await c.execute('DELETE FROM ' + table)
        with pytest.raises(QueryError):
            await manager.batch_insert(table, rows(10) + [{'id': 1, 'embedding': [1, 2]}], batch_size=3)
        assert (await c.execute('SELECT count(*) FROM ' + table)).rows == [(0,)]
    finally:
        await c.disconnect()


@pytest.mark.asyncio
async def test_native_literal_payloads_sync_and_async(client, table):
    client.execute('ALTER TABLE ' + table + ' ADD COLUMN label VARCHAR(200)')
    labels = [':tag', 'prefix :tag', '{"a":1,"b":":tag"}', '% %% %s %(tag)s', "it's 雪"]
    data = [dict(row, label=label) for row, label in zip(rows(len(labels)), labels)]
    VectorManager(client).batch_insert(table, data, batch_size=2)
    assert client.execute('SELECT label FROM ' + table + ' ORDER BY id').rows == [(label,) for label in labels]
    client.execute('DELETE FROM ' + table)
    c = AsyncClient(logger=Mock())
    await c.connect(**params(), on_connect=[])
    try:
        await AsyncVectorManager(c).batch_insert(table, data, batch_size=2)
        assert (await c.execute('SELECT label FROM ' + table + ' ORDER BY id')).rows == [(label,) for label in labels]
    finally:
        await c.disconnect()
