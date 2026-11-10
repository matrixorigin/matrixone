# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import json
import os
import uuid
from decimal import Decimal
from unittest.mock import Mock

import numpy as np
import pytest
from sqlalchemy import Integer, String, bindparam, event, literal, select, text
from sqlalchemy.types import TypeDecorator

from matrixone import Client
from matrixone.async_client import AsyncClient
from matrixone.client import Session as ClientSession
from matrixone.exceptions import QueryError
from matrixone.session import Session as ExportedSession
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


def literal_query():
    return "SELECT ':tag', '% %% %s %(tag)s'"


def scalar_rows():
    values = [True, False, 1.0, Decimal('1.0'), np.int64(1), np.float32(1.0), np.bool_(True), np.bool_(False)]
    return [
        dict(row, scalar=value, flag=value, label=value, precise=Decimal('12345678901234567890.125'))
        for row, value in zip(rows(len(values)), values)
    ]


def assert_scalar_values(result):
    assert [tuple(row) for row in result] == [
        (count, flag, label, Decimal('12345678901234567890.125'))
        for count, flag, label in [
            (1, True, '1'),
            (0, False, '0'),
            (1, True, '1.0'),
            (1, True, '1.0'),
            (1, True, '1'),
            (1, True, '1.0'),
            (1, True, '1'),
            (0, False, '0'),
        ]
    ]


def scalar_schema(table):
    return (
        'ALTER TABLE ' + table + ' ADD COLUMN scalar INT, ADD COLUMN flag BOOLEAN, '
        'ADD COLUMN label VARCHAR(40), ADD COLUMN precise DECIMAL(30,10)'
    )


@pytest.mark.parametrize('mode', ['vector_single', 'vector_batch', 'client_batch', 'session_batch'])
def test_native_scalar_types_across_public_consumers(client, table, mode):
    client.execute(scalar_schema(table))
    data = scalar_rows()
    if mode == 'vector_single':
        for row in data:
            client.vector_ops.insert(table, row)
    elif mode == 'vector_batch':
        client.vector_ops.batch_insert(table, data, batch_size=2)
    elif mode == 'client_batch':
        client.batch_insert(table, data)
    else:
        with client.session() as session:
            session.batch_insert(table, data)
    assert_scalar_values(client.execute('SELECT scalar,flag,label,precise FROM ' + table + ' ORDER BY id').rows)


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['vector_single', 'vector_batch', 'client_batch', 'session_batch'])
async def test_async_native_scalar_types_across_public_consumers(client, table, mode):
    client.execute(scalar_schema(table))
    c = AsyncClient(logger=Mock())
    await c.connect(**params(), on_connect=[])
    try:
        data = scalar_rows()
        if mode == 'vector_single':
            for row in data:
                await c.vector_ops.insert(table, row)
        elif mode == 'vector_batch':
            await c.vector_ops.batch_insert(table, data, batch_size=2)
        elif mode == 'client_batch':
            await c.batch_insert(table, data)
        else:
            async with c.session() as session:
                await session.batch_insert(table, data)
        assert_scalar_values((await c.execute('SELECT scalar,flag,label,precise FROM ' + table + ' ORDER BY id')).rows)
    finally:
        await c.disconnect()


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
    with client.session() as session:
        for no_parameters in [False, True]:
            assert tuple(
                session.execute(literal_query(), execution_options={'no_parameters': no_parameters}).one()
            ) == (
                ':tag',
                '% %% %s %(tag)s',
            )
    client.execute('DELETE FROM ' + table)
    c = AsyncClient(logger=Mock())
    await c.connect(**params(), on_connect=[])
    try:
        await AsyncVectorManager(c).batch_insert(table, data, batch_size=2)
        assert (await c.execute('SELECT label FROM ' + table + ' ORDER BY id')).rows == [(label,) for label in labels]
        async with c.session() as session:
            for no_parameters in [False, True]:
                assert tuple(
                    (await session.execute(literal_query(), execution_options={'no_parameters': no_parameters})).one()
                ) == (':tag', '% %% %s %(tag)s')
    finally:
        await c.disconnect()


class TypedLabel(TypeDecorator):
    impl = String
    cache_ok = True

    def process_result_value(self, value, dialect):
        return value + '|typed'


def transform_literal(state, composed):
    typed = state.statement.columns(label=TypedLabel())
    if not composed:
        state.statement = typed
        return
    source = typed.subquery()
    state.statement = (
        select(source.c.label, literal(7).label('bound_value'))
        .where(literal(2).in_(bindparam('allowed', [1, 2], expanding=True)))
        .where(literal(3) == bindparam('guard', 3, type_=Integer, literal_execute=True))
    )


def transformed_expected(value, composed, no_parameters):
    # Parent compilation escapes percent before bind-free no_parameters dispatch.
    if no_parameters and not composed:
        value = value.replace('%', '%%')
    return (value + '|typed', 7) if composed else (value + '|typed',)


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
@pytest.mark.parametrize('composed', [False, True])
@pytest.mark.parametrize('no_parameters', [False, True])
def test_native_transformed_literals_and_outer_binds(client, cls, composed, no_parameters):
    with cls(bind=client._engine, client=client) as session:
        event.listen(session, 'do_orm_execute', lambda state: transform_literal(state, composed))
        for value in ['ordinary', '100%', '%s']:
            sql = "SELECT '" + value + "' AS label"
            expected = transformed_expected(value, composed, no_parameters)
            for statement in [text(sql), sql]:
                assert (
                    tuple(session.execute(statement, execution_options={'no_parameters': no_parameters}).one())
                    == expected
                )
        assert tuple(session.execute("SELECT ':tag' AS label").one()) == (
            (':tag|typed', 7) if composed else (':tag|typed',)
        )
        value = '%(tag)s'
        outcomes = []
        for statement in [text("SELECT '" + value + "' AS label"), "SELECT '" + value + "' AS label"]:
            try:
                observed = tuple(session.execute(statement, execution_options={'no_parameters': no_parameters}).one())
            except QueryError as error:
                assert "(builtins.KeyError) 'tag'" in str(error)
                observed = 'parent positional bind error'
            else:
                assert observed == transformed_expected(value, composed, no_parameters)
            outcomes.append(observed)
        assert outcomes[0] == outcomes[1]


@pytest.mark.asyncio
@pytest.mark.parametrize('composed', [False, True])
@pytest.mark.parametrize('no_parameters', [False, True])
async def test_async_native_transformed_literals_and_outer_binds(client, composed, no_parameters):
    c = AsyncClient(logger=Mock())
    await c.connect(**params(), on_connect=[])
    try:
        async with c.session() as session:
            event.listen(session.sync_session, 'do_orm_execute', lambda state: transform_literal(state, composed))
            for value in ['ordinary', '100%', '%s']:
                sql = "SELECT '" + value + "' AS label"
                expected = transformed_expected(value, composed, no_parameters)
                for statement in [text(sql), sql]:
                    assert (
                        tuple(
                            (await session.execute(statement, execution_options={'no_parameters': no_parameters})).one()
                        )
                        == expected
                    )
            assert tuple((await session.execute("SELECT ':tag' AS label")).one()) == (
                (':tag|typed', 7) if composed else (':tag|typed',)
            )
            value = '%(tag)s'
            outcomes = []
            for statement in [text("SELECT '" + value + "' AS label"), "SELECT '" + value + "' AS label"]:
                try:
                    observed = tuple(
                        (await session.execute(statement, execution_options={'no_parameters': no_parameters})).one()
                    )
                except QueryError as error:
                    assert "(builtins.KeyError) 'tag'" in str(error)
                    observed = 'parent positional bind error'
                else:
                    assert observed == transformed_expected(value, composed, no_parameters)
                outcomes.append(observed)
            assert outcomes[0] == outcomes[1]
    finally:
        await c.disconnect()
