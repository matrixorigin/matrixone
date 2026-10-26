# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import os
import uuid
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import Mock
from uuid import UUID

import numpy as np
import pytest

from matrixone import Client
from matrixone.async_client import AsyncClient

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


@pytest.mark.parametrize('mode', ['', 'NO_BACKSLASH_ESCAPES'])
def test_native_raw_sql_lexical_boundaries(client, mode):
    with client.session() as session:
        session.execute('SET sql_mode = ?', (mode,))
        assert tuple(session.execute('SELECT 1 AS `tail\\`, ?', (7,)).one()) == (1, 7)
        if mode:
            assert tuple(session.execute("SELECT '\\', ?", (8,)).one()) == ('\\', 8)
        assert session.execute('SELECT /*!50000 ? */', (9,)).scalar() == 9
        assert session.execute('SELECT /*! :id */ /* :ignored */', {'id': 10}).scalar() == 10
        session.execute("SET sql_mode = ''")


def test_native_numpy_vector_elements(client):
    import json

    table = 'sdk_vector_' + uuid.uuid4().hex[:12]
    client.execute('CREATE TABLE ' + table + '(id INT, embedding VECF32(2))')
    try:
        for i, dtype in enumerate([np.float32, np.int64, np.float64]):
            client.vector_ops.insert(table, {'id': i, 'embedding': list(np.array([1, 2], dtype=dtype))})
        assert [
            json.loads(row[0]) for row in client.execute('SELECT embedding FROM ' + table + ' ORDER BY id').rows
        ] == [[1, 2], [1, 2], [1, 2]]
    finally:
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
async def test_async_native_vectors_and_sql_mode(client):
    c = AsyncClient(logger=Mock())
    await c.connect(
        host=os.environ.get('MO_HOST', '127.0.0.1'),
        port=int(os.environ.get('MO_PORT', '6001')),
        user=os.environ.get('MO_USER', 'root'),
        password=os.environ.get('MO_PASSWORD', '111'),
        database=os.environ.get('MO_DATABASE', 'mo_sdk_regression'),
        on_connect=[],
    )
    table = 'sdk_vector_' + uuid.uuid4().hex[:12]
    client.execute('CREATE TABLE ' + table + '(id INT, embedding VECF32(2))')
    try:
        for i, dtype in enumerate([np.float32, np.int64, np.float64]):
            await c.vector_ops.insert(table, {'id': i, 'embedding': list(np.array([1, 2], dtype=dtype))})
        assert (await c.execute('SELECT count(*) FROM ' + table)).rows == [(3,)]
        async with c.session() as session:
            await session.execute("SET sql_mode = 'NO_BACKSLASH_ESCAPES'")
            assert tuple((await session.execute("SELECT '\\', ?", (7,))).one()) == ('\\', 7)
            assert tuple((await session.execute('SELECT 1 AS `tail\\`, ?', (8,))).one()) == (1, 8)
            assert (await session.execute('SELECT /*!50000 ? */', (9,))).scalar() == 9
            await session.execute("SET sql_mode = ''")
    finally:
        try:
            await c.disconnect()
        finally:
            client.execute('DROP TABLE ' + table)


@pytest.mark.parametrize('exported', [False, True])
@pytest.mark.parametrize('transformed', [False, True])
def test_native_mode_follows_unbound_session_hook(client, exported, transformed):
    from sqlalchemy import create_engine, event

    from matrixone.client import Session as ClientSession
    from matrixone.session import Session as ExportedSession

    routed = create_engine(client._engine.url, connect_args={'init_command': "SET sql_mode='NO_BACKSLASH_ESCAPES'"})
    default_probes, routed_probes = [], []

    def record(target):
        def observe(c, cur, sql, *args):
            if sql == 'SELECT @@SESSION.sql_mode':
                target.append(c)

        return observe

    default_observer = record(default_probes)
    event.listen(client._engine, 'before_cursor_execute', default_observer)
    event.listen(routed, 'before_cursor_execute', record(routed_probes))

    def route(state):
        state.bind_arguments['bind'] = routed
        if transformed:
            state.statement = state.statement.columns()

    cls = ExportedSession if exported else ClientSession
    try:
        with cls(client=client) as session:
            event.listen(session, 'do_orm_execute', route)
            assert tuple(session.execute("SELECT '\\', ?", (7,)).one()) == ('\\', 7)
        assert default_probes == []
        assert len(routed_probes) == 1
        assert routed.pool.checkedout() == 0
    finally:
        event.remove(client._engine, 'before_cursor_execute', default_observer)
        routed.dispose()


def test_native_snapshot_mode_is_owned_by_connection(client):
    from matrixone.client import SnapshotClient

    with client._engine.begin() as connection:
        connection.exec_driver_sql("SET sql_mode='NO_BACKSLASH_ESCAPES'")
    try:
        assert SnapshotClient(client, 'unused_without_from').execute("SELECT '\\', ?", (7,)).rows == [('\\', 7)]
    finally:
        with client._engine.begin() as connection:
            connection.exec_driver_sql("SET sql_mode=''")


@pytest.mark.asyncio
@pytest.mark.parametrize('transformed', [False, True])
async def test_native_async_mode_follows_unbound_session_hook(client, transformed):
    from sqlalchemy import event
    from sqlalchemy.ext.asyncio import create_async_engine

    from matrixone.session import AsyncSession

    routed = create_async_engine(
        client._engine.url.set(drivername='mysql+aiomysql'),
        connect_args={'init_command': "SET sql_mode='NO_BACKSLASH_ESCAPES'"},
        pool_pre_ping=False,
    )
    probes = []
    event.listen(routed.sync_engine, 'before_cursor_execute', lambda c, cur, sql, *a: probes.append(sql))
    async_client = AsyncClient(logger=Mock())

    def route(state):
        state.bind_arguments['bind'] = routed.sync_engine
        if transformed:
            state.statement = state.statement.columns()

    try:
        async with AsyncSession(client=async_client) as session:
            event.listen(session.sync_session, 'do_orm_execute', route)
            assert tuple((await session.execute("SELECT '\\', ?", (7,))).one()) == ('\\', 7)
        assert probes == ['SELECT @@SESSION.sql_mode', "SELECT '\\', 7"]
        assert routed.sync_engine.pool.checkedout() == 0
    finally:
        await routed.dispose()


_NATIVE_TOKEN_CASES = [
    ('SELECT ? // ? :ignored %s', (7,), (7,)),
    ('SELECT :v // :ignored', {'v': 7}, (7,)),
    ('SELECT ? // ?\n, ?', (7, 8), (7, 8)),
    ("SELECT $$O'Reilly ? :ignored %s$$, ?", (7,), ("O'Reilly ? :ignored %s", 7)),
    ("SELECT $txt$O'Reilly$txt$, :v", {'v': 7}, ("O'Reilly", 7)),
    ('SELECT $9$? /* :ignored */$9$, ?', (7,), ('? /* :ignored */', 7)),
    ('SELECT 1 AS name$tag$, ?', (7,), (1, 7)),
    ('SELECT /*!50000$$?$$*/, ?', (7,), ('?', 7)),
    ('SELECT 1.0$$?$$, ?', (7,), (Decimal('1.0'), 7)),
    ('SELECT .1$tag$? :ignored$tag$, :v', {'v': 7}, (Decimal('0.1'), 7)),
    ('SELECT 1e+2$$?$$, ?', (7,), (100, 7)),
    ('SELECT 1.2e3$$?$$, ?', (7,), (1200, 7)),
]


@pytest.mark.parametrize('sql,params,expected', _NATIVE_TOKEN_CASES)
@pytest.mark.parametrize('surface', ['client', 'embedded_session', 'exported_session'])
def test_native_public_token_boundaries(client, surface, sql, params, expected):
    from matrixone.client import Session as ClientSession
    from matrixone.session import Session as ExportedSession

    if surface == 'client':
        assert client.execute(sql, params).rows == [expected]
    else:
        cls = ClientSession if surface == 'embedded_session' else ExportedSession
        with cls(bind=client._engine, client=client) as session:
            assert tuple(session.execute(sql, params).one()) == expected


@pytest.mark.asyncio
async def test_async_native_public_token_boundaries(client):
    c = AsyncClient(logger=Mock())
    await c.connect(
        host=os.environ.get('MO_HOST', '127.0.0.1'),
        port=int(os.environ.get('MO_PORT', '6001')),
        user=os.environ.get('MO_USER', 'root'),
        password=os.environ.get('MO_PASSWORD', '111'),
        database=os.environ.get('MO_DATABASE', 'mo_sdk_regression'),
        on_connect=[],
    )
    try:
        for sql, params, expected in _NATIVE_TOKEN_CASES:
            assert (await c.execute(sql, params)).rows == [expected]
            async with c.session() as session:
                assert tuple((await session.execute(sql, params)).one()) == expected
        assert c._engine.sync_engine.pool.checkedout() == 0
    finally:
        await c.disconnect()


@pytest.mark.parametrize('keyword', ['window', 'integer', 'rows'])
def test_native_keyword_builders(client, keyword):
    table = 'sdk_keyword_' + uuid.uuid4().hex[:12]
    client.execute('CREATE TABLE ' + table + '(`' + keyword + '` INT)')
    try:
        client.insert(table, {keyword: 1})
        client.batch_insert(table, [{keyword: 2}])
        client.vector_ops.insert(table, {keyword: 3})
        assert client.execute('SELECT `' + keyword + '` FROM ' + table + ' ORDER BY 1').rows == [(1,), (2,), (3,)]
    finally:
        client.execute('DROP TABLE ' + table)


@pytest.mark.parametrize('sql', ['SELECT $tag$x$other$', 'SELECT $$unterminated'])
def test_native_invalid_dollar_controls_and_pool_reuse(client, sql):
    from matrixone.exceptions import QueryError

    # params=None leaves the malformed SQL to the actual server scanner.
    with pytest.raises(QueryError):
        client.execute(sql)
    with pytest.raises(QueryError, match='SQL dollar string'):
        client.execute(sql, ())
    assert client._engine.pool.checkedout() == 0
    assert client.execute('SELECT ?', (9,)).rows == [(9,)]


@pytest.mark.asyncio
async def test_native_decoded_time_and_uuid_share_all_insert_paths(client):
    from sqlalchemy import event

    from matrixone.exceptions import QueryError

    table = 'sdk_scalars_' + uuid.uuid4().hex[:12]
    token = UUID('12345678-1234-5678-1234-567812345678')
    day = date(2026, 10, 25)
    c = AsyncClient(logger=Mock())
    client.execute('CREATE TABLE ' + table + '(id INT PRIMARY KEY, duration TIME(6), token UUID, day DATE)')
    try:
        client.execute(
            'INSERT INTO ' + table + " VALUES (0, '1:02:03', '12345678-1234-5678-1234-567812345678', '2026-10-25')"
        )
        duration = client.execute('SELECT duration FROM ' + table + ' WHERE id=0').rows[0][0]
        assert isinstance(duration, timedelta) and duration == timedelta(seconds=3723)
        await c.connect(
            host=os.environ.get('MO_HOST', '127.0.0.1'),
            port=int(os.environ.get('MO_PORT', '6001')),
            user=os.environ.get('MO_USER', 'root'),
            password=os.environ.get('MO_PASSWORD', '111'),
            database=os.environ.get('MO_DATABASE', 'mo_sdk_regression'),
            on_connect=[],
        )
        inserts = []

        def observe(conn, cursor, sql, *args):
            if sql.upper().startswith('INSERT'):
                inserts.append(sql)

        event.listen(client._engine, 'before_cursor_execute', observe)
        event.listen(c._engine.sync_engine, 'before_cursor_execute', observe)
        ident = 0
        for actor in [client, c]:
            for vector in [False, True]:
                target = actor.vector_ops if vector else actor
                for batch in [False, True]:
                    ident += 1
                    row = {'id': ident, 'duration': duration, 'token': token, 'day': day}
                    method = target.batch_insert if batch else target.insert
                    result = method(table, [row] if batch else row)
                    if actor is c:
                        await result
                    assert client.execute(
                        'SELECT duration,CAST(token AS CHAR),day FROM ' + table + ' WHERE id=?', (ident,)
                    ).rows == [(duration, str(token), day)], (actor.__class__.__name__, vector, batch)

                    inserts.clear()
                    invalid = dict(row, id=1000 + ident, token=object())
                    with pytest.raises(QueryError if vector else TypeError, match='Unsupported SQL parameter type'):
                        result = method(table, [dict(row, id=2000 + ident), invalid] if batch else invalid)
                        if actor is c:
                            await result
                    # Both batch rows fit the first vector chunk; later-chunk
                    # rollback remains covered by the transaction tests.
                    assert inserts == []
                    assert client._engine.pool.checkedout() == c._engine.sync_engine.pool.checkedout() == 0

        for value in [
            timedelta(),
            timedelta(microseconds=-1),
            timedelta(minutes=-30),
            timedelta(hours=26, seconds=3, microseconds=4),
        ]:
            ident += 1
            client.insert(table, {'id': ident, 'duration': value, 'token': token, 'day': day})
            assert client.execute('SELECT duration FROM ' + table + ' WHERE id=?', (ident,)).rows == [(value,)]
        with pytest.raises(QueryError):
            client.insert(table, {'id': 9999, 'duration': timedelta.max, 'token': token, 'day': day})
        assert client.execute('SELECT count(*) FROM ' + table).rows == [(ident + 1,)]
        assert client._engine.pool.checkedout() == c._engine.sync_engine.pool.checkedout() == 0
    finally:
        try:
            await c.disconnect()
        finally:
            client.execute('DROP TABLE ' + table)


def test_native_builder_qmark_parameters_preserve_modulo_and_typed_literals(client):
    from matrixone.sql_builder import MatrixOneSQLBuilder

    table = 'sdk_builder_' + uuid.uuid4().hex[:12]
    client.execute(
        'CREATE TABLE ' + table + '(value INT, size INT, s INT, marker VARCHAR(10), day DATE, payload VARBINARY(4))'
    )
    try:
        client.execute(
            'INSERT INTO ' + table + " VALUES (7,3,4,'?','2026-10-26',X'00ff'),(8,4,5,'?','2026-10-26',X'0001')"
        )
        for expression, expected in [('value%size', [1, 0]), ('value%s', [3, 3]), ('value % size', [1, 0])]:
            for bound in [False, True]:
                builder = MatrixOneSQLBuilder().select(expression).from_table(table).order_by('value')
                if bound:
                    builder.where('value = ?', 7)
                assert client.execute(builder.build_with_parameter_substitution()).rows == [
                    (value,) for value in (expected[:1] if bound else expected)
                ]
        query = (
            MatrixOneSQLBuilder()
            .select('value')
            .from_table(table)
            .where("marker = '?' /* ? %s */")
            .where('day = ?', date(2026, 10, 26))
            .where('payload = ?', b'\x00\xff')
            .build_with_parameter_substitution()
        )
        assert client.execute(query).rows == [(7,)]
        # Client APIs retain their separate legacy format-placeholder contract.
        assert client.execute('SELECT %s', (9,)).rows == [(9,)]
    finally:
        client.execute('DROP TABLE ' + table)
