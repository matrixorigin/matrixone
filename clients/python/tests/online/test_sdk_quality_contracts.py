# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Required small public-contract suite for explicitly selected server targets."""

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
def server():
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
