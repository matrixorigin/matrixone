# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Required small public-contract suite for explicitly selected server targets."""

import csv
import json
import os
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import declarative_base

from matrixone import Client, AsyncClient
from matrixone.exceptions import QueryError

pytestmark = pytest.mark.skipif(os.environ.get('MO_SDK_ONLINE') != '1', reason='explicit isolated target required')


@pytest.fixture(scope='module')
def server(record_testsuite_property):
    config = dict(
        host=os.environ['MO_HOST'],
        port=int(os.environ['MO_PORT']),
        user=os.environ['MO_USER'],
        password=os.environ['MO_PASSWORD'],
        database='mo_catalog',
        on_connect=[],
    )
    admin = Client(sql_log_mode='off')
    admin.connect(**config)
    name = 'sdk_quality_' + uuid.uuid4().hex[:12]
    try:
        version, revision = admin.execute('SELECT version(),git_version()').rows[0]
        expected = os.environ['MO_SDK_EXPECT_VERSION']
        assert expected in version, (version, expected)
        expected_revision = os.environ.get('MO_SDK_EXPECT_REVISION')
        if expected_revision:
            assert revision.startswith(expected_revision), (revision, expected_revision)
        print('SDK_SERVER_IDENTITY', version, revision)
        record_testsuite_property('matrixone.version', version)
        record_testsuite_property('matrixone.revision', revision)
        admin.execute('CREATE DATABASE ' + name)
        config['database'] = name
        yield config
    finally:
        admin.execute('DROP DATABASE IF EXISTS ' + name)
        assert (
            admin.execute('SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=?', (name,)).scalar() == 0
        )
        admin.disconnect()


@pytest.fixture
def client(server):
    c = Client(sql_log_mode='off')
    c.connect(**server)
    try:
        yield c
    finally:
        assert c._engine.pool.checkedout() == 0
        c.disconnect()


@pytest_asyncio.fixture
async def async_client(server):
    c = AsyncClient(sql_log_mode='off')
    await c.connect(**server)
    try:
        yield c
    finally:
        assert c._engine.pool.checkedout() == 0
        await c.disconnect()


@pytest.fixture
def tables(client):
    root = 'rows_' + uuid.uuid4().hex[:10]
    other = root + '_b'
    client.execute(f'CREATE TABLE {root}(id INT PRIMARY KEY,v DECIMAL(10,0),label VARCHAR(100))')
    try:
        client.execute(f'CREATE TABLE {other}(id INT PRIMARY KEY,ref INT)')
        client.execute(f"INSERT INTO {root} VALUES(1,2,'red'),(2,1,'blue'),(3,NULL,NULL)")
        client.execute(f'INSERT INTO {other} VALUES(10,1),(20,8)')
        base = declarative_base()
        model = type(
            'Item',
            (base,),
            dict(
                __tablename__=root,
                id=sa.Column(sa.Integer, primary_key=True),
                v=sa.Column(sa.Numeric),
                label=sa.Column(sa.String(100)),
            ),
        )
        yield model, other
    finally:
        client.execute('DROP TABLE IF EXISTS ' + other)
        client.execute('DROP TABLE ' + root)


def test_public_query_contracts(client, tables):
    model, other = tables
    assert [r.id for r in client.query(model).filter_by(v=Decimal('2')).filter('id=?', 1).all()] == [1]
    assert [r.id for r in client.query(model).filter_by(v=None).all()] == [3]
    assert [r.id for r in client.query(model.__tablename__).filter_by(label='red').all()] == [1]
    assert client.query(model.__tablename__).filter_by(id=99).first() is None
    with pytest.raises(ValueError):
        client.query(model).filter_by(typo=1)
    q = client.query(model).join(other, f'{model.__tablename__}.id={other}.ref').filter(model.id == 1)
    assert q.select(model.id).group_by(model.id).having(sa.func.count(model.id) == 1).all()[0][0] == 1
    q = client.query(model).rightjoin(other, f'{model.__tablename__}.id={other}.ref').select(model.id, other + '.id')
    assert [tuple(r) for r in q.order_by(other + '.id').all()] == [(1, 10), (None, 20)]
    with client.session() as session:
        assert [r.id for r in session.query(model).filter_by(v=None).all()] == [3]


@pytest.mark.asyncio
async def test_async_public_contracts_and_pre_ping(async_client, tables):
    model, other = tables
    for _ in range(3):
        assert (await async_client.execute('SELECT 1')).scalar() == 1
    assert [r.id for r in await async_client.query(model).filter_by(v=Decimal('2')).filter('id=?', 1).all()] == [1]
    assert [r.id for r in await async_client.query(model).filter_by(v=None).all()] == [3]
    assert (await async_client.query(model.__tablename__).filter_by(id=1).first()).label == 'red'
    q = (
        async_client.query(model)
        .rightjoin(other, f'{model.__tablename__}.id={other}.ref')
        .select(model.id, other + '.id')
    )
    assert [tuple(r) for r in await q.order_by(other + '.id').all()] == [(1, 10), (None, 20)]
    assert (await async_client.get_capabilities())['right_join'].state == 'supported'


def test_type_metadata_and_values(client):
    name = 'types_' + uuid.uuid4().hex[:10]
    table = sa.Table(
        name,
        sa.MetaData(),
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('u', mysql.BIGINT(unsigned=True)),
        sa.Column('ts', mysql.TIMESTAMP(fsp=6)),
        sa.Column('dt', mysql.DATETIME(fsp=6)),
        sa.Column('tm', mysql.TIME(fsp=6)),
    )
    with client._engine.begin() as connection:
        table.create(connection)
    control = name + "_native"
    try:
        client.execute(f"CREATE TABLE {control}(ts TIMESTAMP(6))")
        value = datetime(2026, 1, 2, 3, 4, 5, 123456)
        client.execute(
            f'INSERT INTO {name} VALUES(?,?,?,?,?)',
            (1, 2**64 - 1, value, value, timedelta(seconds=5, microseconds=123456)),
        )
        row = client.execute(f'SELECT u,ts,dt,tm FROM {name}').rows[0]
        assert row == (2**64 - 1, value, value, timedelta(seconds=5, microseconds=123456))
        metadata = dict(
            client.execute(
                'SELECT column_name,column_type FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=?',
                (name,),
            ).rows
        )
        assert 'unsigned' in metadata['u'].lower()
        precision = client.execute(
            'SELECT datetime_precision FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name=?',
            (name, 'ts'),
        ).scalar()
        native_precision = client.execute(
            'SELECT datetime_precision FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name=?',
            (control, 'ts'),
        ).scalar()
        # 4.2.5 omits DATETIME_PRECISION even for native DDL. Compare native
        # metadata and assert schema + exact values rather than inventing it.
        assert precision == native_precision
        assert 'timestamp(6)' in client.execute('SHOW CREATE TABLE ' + name).rows[0][1].lower()
        with pytest.raises(QueryError):
            client.execute(f'INSERT INTO {name}(id) VALUES(1)')
        assert client.execute(f'SELECT count(*) FROM {name}').scalar() == 1
        assert client.execute('SELECT 1').scalar() == 1
    finally:
        client.execute('DROP TABLE IF EXISTS ' + control)
        client.execute('DROP TABLE ' + name)


@pytest.mark.asyncio
async def test_snapshot_same_time_all_tables(client, async_client, tables):
    model, other = tables
    root = model.__tablename__
    snapshot = 'sp_' + uuid.uuid4().hex[:12]
    client.execute(f'CREATE SNAPSHOT {snapshot} FOR DATABASE {client._connection_params["database"]}')
    try:
        client.execute(f'UPDATE {other} SET ref=2 WHERE id=10')
        direct = f"SELECT a.id FROM {root}{{snapshot='{snapshot}'}} AS a JOIN {other}{{snapshot='{snapshot}'}} AS b ON a.id=b.ref"
        assert client.execute(direct).rows == [(1,)]
        sql = f'SELECT a.id FROM {root} AS a JOIN {other} AS b ON a.id=b.ref'
        with client.snapshot(snapshot) as snap:
            assert snap.execute(sql).rows == [(1,)]
        async with async_client.snapshot(snapshot) as snap:
            assert (await snap.execute(sql)).rows == [(1,)]
        q = client.query(root, snapshot=snapshot).alias('a').join(other, f'a.id={other}.ref').select('a.id')
        assert q.execute().rows == [(1,)]
        database = client._connection_params['database']
        with client.snapshot(snapshot) as snap:
            assert snap.execute(f'SELECT ref FROM {database}.{other} WHERE id=10').rows == [(1,)]
        assert client.execute(f'SELECT ref FROM {other} WHERE id=10').rows == [(2,)]
    finally:
        client.execute('DROP SNAPSHOT ' + snapshot)


@pytest.mark.parametrize('suffix', ['jsonl', 'txt'])
def test_export_content(client, tables, suffix):
    model, _ = tables
    filename = 'sdk_' + uuid.uuid4().hex + '.' + suffix
    server_path = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + filename
    local_path = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / filename
    try:
        client.export.to_jsonl(server_path, f'SELECT id,label FROM {model.__tablename__} ORDER BY id')
        rows = [json.loads(line) for line in local_path.read_text().splitlines()]
        assert rows == [{'id': 1, 'label': 'red'}, {'id': 2, 'label': 'blue'}, {'id': 3, 'label': None}]
    finally:
        local_path.unlink(missing_ok=True)


def test_capabilities_are_observations(client):
    before = client.execute("SHOW VARIABLES LIKE 'experimental_hnsw_index'").rows
    caps = client.get_capabilities(refresh=True)
    assert caps['right_join'].state == 'supported'
    assert caps['snapshot'].state == 'unknown'
    assert caps['arrow_load'].state == 'unknown'
    assert client.execute("SHOW VARIABLES LIKE 'experimental_hnsw_index'").rows == before
    assert caps['hnsw_index'].state == ('supported' if str(before[0][1]).lower() in ('on', '1') else 'disabled')


def test_legacy_query_native_execution(client, tables):
    from matrixone.orm import Query

    model, _ = tables
    assert Query(model, client).count() == 3
    assert Query(model, client).filter('id=?', 1).first().label == 'red'
    Query(model, client).insert(id=4, v=8, label='new').execute()
    Query(model, client).filter('id=?', 4).update(v=9).execute()
    assert Query(model, client).filter('id=?', 4).all()[0].v == 9
    Query(model, client).filter('id=?', 4).delete()
    assert Query(model, client).count() == 3
    base = declarative_base()
    renamed = type(
        'Renamed',
        (base,),
        dict(
            __tablename__=model.__tablename__,
            key=sa.Column('id', sa.Integer, primary_key=True),
            amount=sa.Column('v', sa.Integer),
            display=sa.Column('label', sa.String),
        ),
    )
    assert Query(renamed, client).filter('id=?', 1).first().display == 'red'
    row = Query(renamed, client).select('label', 'id').filter('id=?', 1).first()
    assert (row.display, row.key) == ('red', 1)


@pytest.mark.asyncio
async def test_native_aliased_dml_and_recursive_cte(client, async_client, tables):
    from matrixone.orm import CTE

    model, _ = tables
    for c in (client, async_client):
        q = c.query(model).filter(model.id == 1).update(v=model.v + 1).alias('a')
        if c is client:
            q.execute()
        else:
            await q.execute()
        recursive = CTE('r', 'SELECT 1 AS n UNION ALL SELECT n+1 FROM r WHERE n<3', recursive=True)
        q = c.query(recursive).select('n').order_by('n')
        rows = q.all() if c is client else await q.all()
        assert [r.n for r in rows] == [1, 2, 3]
    assert client.execute(f'SELECT v FROM {model.__tablename__} WHERE id=1').scalar() == 4
    for c in (client, async_client):
        joined = c.query(model).join(tables[1], f'{model.__tablename__}.id={tables[1]}.ref')
        with pytest.raises(ValueError, match='Joined UPDATE/DELETE'):
            if c is client:
                joined.update(v=99).execute()
            else:
                await joined.update(v=99).execute()
        with pytest.raises(ValueError, match='Joined UPDATE/DELETE'):
            if c is client:
                joined.delete()
            else:
                await joined.delete()
    assert client.execute(f'SELECT id,v FROM {model.__tablename__} ORDER BY id').rows == [(1, 4), (2, 1), (3, None)]
    await async_client.query(model).filter(model.id == 2).alias('a').delete()
    client.query(model).filter(model.id == 3).alias('a').delete()
    assert client.execute(f'SELECT id FROM {model.__tablename__}').rows == [(1,)]


@pytest.mark.asyncio
@pytest.mark.parametrize('owner', ['client', 'session'])
async def test_async_explicit_database_mutations(client, async_client, tables, owner):
    model, _ = tables
    database = 'sdk_route_' + uuid.uuid4().hex[:12]
    table = model.__tablename__
    client.execute('CREATE DATABASE ' + database)
    try:
        client.execute(f'CREATE TABLE {database}.{table}(id INT PRIMARY KEY,v INT)')
        client.execute(f'INSERT INTO {database}.{table} VALUES(1,100),(2,200)')

        async def mutate(executor):
            q = executor.query(table)
            q.database = database
            q.filter('id=?', 1).update(v=101)
            await q.execute()
            assert (await q.first()).v == 101
            await q.delete()
            q.database = 'sdk_nonexistent_' + uuid.uuid4().hex[:8]
            with pytest.raises(QueryError):
                await q.update(v=0).execute()

        if owner == 'client':
            await mutate(async_client)
        else:
            async with async_client.session() as session:
                await mutate(session)
        assert client.execute(f'SELECT id,v FROM {database}.{table}').rows == [(2, 200)]
        base = declarative_base()
        mapped = type(
            'Routed',
            (base,),
            dict(
                __tablename__=table,
                __table_args__={'schema': database},
                id=sa.Column(sa.Integer, primary_key=True),
                v=sa.Column(sa.Integer),
            ),
        )
        from matrixone.orm import Query

        assert Query(mapped, client).first().v == 200
        routed = async_client.query(mapped).filter(mapped.id == 2).update(v=mapped.v + 1)
        await routed.execute()
        assert (await async_client.query(mapped).first()).v == 201
        routed.database = 'different'
        with pytest.raises(ValueError, match='conflicts'):
            await routed.execute()
        assert client.execute(f'SELECT id,v FROM {table} ORDER BY id').rows == [(1, 2), (2, 1), (3, None)]
        assert (await async_client.execute('SELECT 42')).scalar() == 42
    finally:
        client.execute('DROP DATABASE ' + database)


@pytest.mark.asyncio
@pytest.mark.parametrize('format', ['csv', 'jsonl'])
async def test_async_public_export(client, async_client, tables, format):
    model, _ = tables
    filename = 'async_export_' + uuid.uuid4().hex + '.' + format
    server_path = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + filename
    local_path = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / filename
    query = async_client.query(model).select('id', 'label').order_by('id')
    try:
        if format == 'jsonl':
            await async_client.export.to_jsonl(server_path, query)
            rows = [json.loads(line) for line in local_path.read_text().splitlines()]
            assert rows == [{'id': 1, 'label': 'red'}, {'id': 2, 'label': 'blue'}, {'id': 3, 'label': None}]
        else:
            await async_client.export.to_csv(server_path, query)
            assert list(csv.reader(local_path.read_text().splitlines())) == [
                ['id', 'label'],
                ['1', 'red'],
                ['2', 'blue'],
                ['3', r'\N'],
            ]
        with pytest.raises(QueryError):
            # A missing directory may be created; an existing file cannot be a parent.
            await async_client.export.to_jsonl(server_path + '/fail.jsonl', query)
        assert client.execute(f'SELECT COUNT(*) FROM {model.__tablename__}').scalar() == 3
        assert (await async_client.execute('SELECT 42')).scalar() == 42
    finally:
        local_path.unlink(missing_ok=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize(
    'sep,quotechar',
    [(sep, quotechar) for sep in (',', ';', "'") for quotechar in ('"', '~', "'") if sep != quotechar],
)
async def test_csv_separator_and_quotechar_round_trip(client, async_client, tables, kind, sep, quotechar):
    model, _ = tables
    label = 'comma,semicolon;and\nnewline'
    client.execute(f'UPDATE {model.__tablename__} SET label=? WHERE id=1', (label,))
    filename = 'csv_options_' + uuid.uuid4().hex + '.csv'
    server_path = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + filename
    local_path = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / filename
    query = f'SELECT id,label FROM {model.__tablename__} ORDER BY id'
    try:
        if kind == 'async':
            await async_client.export.to_csv(server_path, query, sep=sep, quotechar=quotechar)
        else:
            client.export.to_csv(server_path, query, sep=sep, quotechar=quotechar)
        with local_path.open(newline='') as exported:
            assert list(csv.reader(exported, delimiter=sep, quotechar=quotechar)) == [
                ['id', 'label'],
                ['1', label],
                ['2', 'blue'],
                ['3', r'\N'],
            ]
    finally:
        local_path.unlink(missing_ok=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize(
    'scale,text,neighbor_text',
    [
        (18, '12345678901234567890.123456789012345678', '12345678901234567890.123456789012345679'),
        (27, '0.000000001123456789012345678', '0.000000001123456789012345679'),
    ],
)
async def test_decimal_binding_preserves_exact_mutation_scope(client, async_client, kind, scale, text, neighbor_text):
    from .test_sdk_manager_contracts import resolve

    c = client if kind == 'sync' else async_client
    table = 'decimal_' + uuid.uuid4().hex[:10]
    base = declarative_base()
    amount = type(
        'Amount',
        (base,),
        dict(
            __tablename__=table,
            id=sa.Column(sa.Integer, primary_key=True, autoincrement=False),
            value=sa.Column(sa.Numeric(38, scale)),
        ),
    )
    value = Decimal(text)
    neighbor = Decimal(neighbor_text)
    negative = Decimal('-' + text)
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,value DECIMAL(38,{scale}))')
    try:
        assert c._engine.dialect.supports_native_decimal
        if kind == 'sync':
            with c.session() as session:
                session.add(amount(id=1, value=value))
        else:
            async with c.session() as session:
                session.add(amount(id=1, value=value))
        # Independent native-SQL adjacent and negative controls.
        client.execute(f'INSERT INTO {table} VALUES(2,?),(3,?)', (neighbor, negative))
        expected = [(1, value), (2, neighbor), (3, negative)]
        assert client.execute(f'SELECT id,value FROM {table} ORDER BY id').rows == expected
        assert [r.id for r in await resolve(c.query(amount).filter(amount.value == value).all())] == [1]
        assert [r.id for r in await resolve(c.query(amount).filter_by(value=value).all())] == [1]
        assert [r.id for r in await resolve(c.query(amount).filter(amount.value.in_([neighbor])).all())] == [2]
        with pytest.raises(QueryError):
            await resolve(c.execute(f'INSERT INTO {table} VALUES(2,?)', (value,)))
        assert client.execute(f'SELECT id,value FROM {table} ORDER BY id').rows == expected

        async def mutate(session):
            selected = await resolve(session.execute(sa.select(amount.id).where(amount.value == value)))
            assert selected.scalars().all() == [1]
            await resolve(session.execute(sa.update(amount).where(amount.value == value).values(value=Decimal('0.5'))))
            await resolve(session.execute(sa.delete(amount).where(amount.value == negative)))

        if kind == 'sync':
            with c.session() as session:
                await mutate(session)
        else:
            async with c.session() as session:
                await mutate(session)
        assert client.execute(f'SELECT id,value FROM {table} ORDER BY id').rows == [(1, Decimal('0.5')), (2, neighbor)]
        await resolve(c.query(amount).filter(amount.value == neighbor).update(value=negative).execute())
        assert client.execute(f'SELECT id,value FROM {table} ORDER BY id').rows == [(1, Decimal('0.5')), (2, negative)]
        await resolve(c.query(amount).filter_by(value=negative).delete())
        assert client.execute(f'SELECT id,value FROM {table}').rows == [(1, Decimal('0.5'))]
        assert (await resolve(c.execute('SELECT 42'))).scalar() == 42
    finally:
        client.execute('DROP TABLE ' + table)
