# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import os
import uuid
from decimal import Decimal
from unittest.mock import Mock

import numpy as np
import pytest

from matrixone import Client

pytestmark = pytest.mark.skipif(os.environ.get('MO_SDK_ONLINE') != '1', reason='isolated server required')


@pytest.fixture
def client():
    c = Client(
        host=os.environ.get('MO_HOST', '127.0.0.1'),
        port=int(os.environ.get('MO_PORT', '6001')),
        user=os.environ.get('MO_USER', 'root'),
        password=os.environ.get('MO_PASSWORD', '111'),
        database=os.environ.get('MO_DATABASE', 'mo_sdk_regression'),
        logger=Mock(),
    )
    yield c
    assert c._engine.pool.checkedout() == 0
    c.disconnect()


@pytest.mark.parametrize('mode', ['', 'NO_BACKSLASH_ESCAPES'])
@pytest.mark.parametrize(
    'value',
    [
        "O'Reilly",
        '中文😀',
        'C:\\new\\path',
        "x\\' OR 1=1 --",
        '\x00data',
        '?:id%s',
        'value:label',
        {'key': "O'Reilly"},
        np.int64(7),
        np.uint64(2**64 - 1),
        np.float16(1.25),
        np.float32(1.25),
        np.bool_(True),
        np.bool_(False),
    ],
)
def test_native_session_parameter_roundtrip(client, mode, value):
    import json

    expected = json.dumps(value) if isinstance(value, dict) else value
    if isinstance(value, np.generic):
        expected = value.item()
    with client.session() as session:
        session.execute("SET sql_mode = ?", (mode,))
        result = session.execute("SELECT '?' AS marker, ? AS value", (value,)).fetchone()
        assert tuple(result) == ('?', expected)
        session.execute("SET sql_mode = ''")


def test_named_prefix_and_decimal_native(client):
    assert client.execute('SELECT :id AS a, :id2 AS b', {'id': 1, 'id2': 2}).rows == [(1, 2)]
    assert client.execute('SELECT ? AS amount', (Decimal('1.20'),)).rows == [(Decimal('1.20'),)]


def test_base_and_vector_insert_quote_roundtrip(client):
    table = 'sdk_sql_' + uuid.uuid4().hex[:12]
    client.execute('CREATE TABLE ' + table + '(id INT, value TEXT)')
    try:
        client.execute(client._build_insert_sql(table, {'id': 1, 'value': "O'Reilly"}))
        client.vector_ops.insert(table, {'id': 2, 'value': 'C:\\new\\path'})
        assert client.execute('SELECT value FROM ' + table + ' ORDER BY id').rows == [("O'Reilly",), ('C:\\new\\path',)]
    finally:
        client.execute('DROP TABLE ' + table)


@pytest.mark.parametrize('mode', ['client_insert', 'client_batch', 'vector_insert'])
def test_native_numpy_scalar_insert_compatibility(client, mode):
    table = 'sdk_numpy_' + uuid.uuid4().hex[:12]
    client.execute('CREATE TABLE ' + table + '(id INT, value BIGINT, flag BOOLEAN, label VARCHAR(40))')
    try:
        row = {'id': np.uint32(1), 'value': np.int64(7), 'flag': np.bool_(True), 'label': np.float32(1.25)}
        if mode == 'client_insert':
            client.insert(table, row)
        elif mode == 'client_batch':
            client.batch_insert(table, [row])
        else:
            client.vector_ops.insert(table, row)
        assert client.execute('SELECT value,flag,label FROM ' + table).rows == [(7, True, '1.25')]
    finally:
        client.execute('DROP TABLE ' + table)
