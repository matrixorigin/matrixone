# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Version-qualified manager inputs, catalog identities and transaction outcomes."""

import asyncio
import gzip
import inspect
import os
import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa

from matrixone import Client, SnapshotLevel
from matrixone.exceptions import BranchError, CloneError, PubSubError, SnapshotError
from .test_sdk_quality_contracts import server, client, async_client  # noqa: F401

pytestmark = pytest.mark.skipif(os.environ.get('MO_SDK_ONLINE') != '1', reason='explicit isolated target required')


async def resolve(value):
    return await value if inspect.isawaitable(value) else value


def name(prefix):
    return prefix + uuid.uuid4().hex[:10]


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_catalog_names_and_publication_all(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    db = client._connection_params['database']
    snap, pub, tabpub, table = name('MixedSnap'), name('MixedPub'), name('TablePub'), name('pub_src_')
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY)')
    try:
        saved = await resolve(c.snapshots.create(snap, SnapshotLevel.DATABASE, database=db))
        assert saved.name == snap.lower()
        assert (await resolve(c.snapshots.get(snap))).name == saved.name
        assert await resolve(c.snapshots.exists(snap))
        with pytest.raises(SnapshotError):
            await resolve(c.snapshots.create(snap, SnapshotLevel.DATABASE, database=db))
        assert (await resolve(c.snapshots.get(snap))).name == saved.name
        p = await resolve(c.pubsub.create_database_publication(pub, db, 'aLl'))
        assert p.name == pub.lower() and p.sub_account == '*'
        p = await resolve(c.pubsub.create_table_publication(tabpub, db, table, 'ALL'))
        assert p.tables == table and p.sub_account == '*'
        assert (await resolve(c.pubsub.get_publication(pub))).name == pub.lower()
        with pytest.raises(PubSubError):
            await resolve(c.pubsub.get_publication(name('missing_')))
        assert (await resolve(c.execute('SELECT 42'))).scalar() == 42
    finally:
        for publication in (pub, tabpub):
            client.execute('DROP PUBLICATION IF EXISTS ' + publication)
        client.execute('DROP SNAPSHOT IF EXISTS ' + snap)
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_branch_uses_snapshot_without_version_veto(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    source, target, invalid = name('branch_src_'), name('branch_dst_'), name('branch_bad_')
    snap = name('branch_snap_')
    client.execute('CREATE DATABASE ' + source)
    try:
        client.execute(f'CREATE TABLE {source}.t(id INT PRIMARY KEY)')
        client.execute(f'INSERT INTO {source}.t VALUES(1)')
        client.execute(f'CREATE SNAPSHOT {snap} FOR DATABASE {source}')
        client.execute(f'INSERT INTO {source}.t VALUES(2)')
        c._version_manager.set_backend_version('1.3.0')
        await resolve(c.branch.create(target, source, snapshot=snap, database=True))
        assert client.execute(f'SELECT id FROM {target}.t ORDER BY id').rows == [(1,)]
        assert client.execute(f'SELECT id FROM {source}.t ORDER BY id').rows == [(1,), (2,)]
        await resolve(c.branch.create(source + '.historical', source + '.t', snapshot=snap))
        assert client.execute(f'SELECT id FROM {source}.historical').rows == [(1,)]
        with pytest.raises(BranchError):
            await resolve(c.branch.create(invalid, source, snapshot=name('absent_'), database=True))
        assert (
            client.execute('SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=?', (invalid,)).scalar()
            == 0
        )
    finally:
        for database in (target, invalid, source):
            client.execute('DROP DATABASE IF EXISTS ' + database)
        client.execute('DROP SNAPSHOT IF EXISTS ' + snap)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_load_preserves_format_options(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    table = name('load_')
    local = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR'])
    remote = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/')
    files = []

    def source(suffix, data):
        path = local / (name('load_') + suffix)
        path.write_bytes(data)
        files.append(path)
        return remote + '/' + path.name

    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,s VARCHAR(100),flag INT DEFAULT 9)')

    async def rows():
        result = (await resolve(c.execute(f'SELECT id,s,flag FROM {table} ORDER BY id'))).rows
        await resolve(c.execute('DELETE FROM ' + table))
        return result

    try:
        path = source('.payload', gzip.compress(b'1,alpha,0\n2,beta,0\n'))
        await resolve(c.load_data.from_file(path, table, compression='gzip'))
        assert await rows() == [(1, 'alpha', 0), (2, 'beta', 0)]
        path = source('.jsonline', b'HEADER\n{"id":1,"s":"alpha","flag":0}\n')
        await resolve(
            c.load_data.from_file(
                path, table, format='jsonline', jsondata='object', ignore_lines=1, set_clause={'flag': 'NULLIF(flag,0)'}
            )
        )
        assert await rows() == [(1, 'alpha', None)]
        path = source('.jsonline', b'["alpha",1]\n')
        await resolve(c.load_data.from_file(path, table, format='jsonline', jsondata='array', columns=['s', 'id']))
        assert await rows() == [(1, 'alpha', 9)]
        import pyarrow as pa
        import pyarrow.parquet as pq

        path = local / (name('load_') + '.parquet')
        files.append(path)
        pq.write_table(pa.table({'id': [1], 's': ['alpha']}), path)
        await resolve(c.load_data.read_parquet(remote + '/' + path.name, table, columns=['id', 's']))
        assert await rows() == [(1, 'alpha', 9)]
        await resolve(
            c.load_data.read_csv(
                's,id\nalpha!,beta,1\n', table=table, inline=True, skiprows=1, names=['s', 'id'], escapechar='!'
            )
        )
        assert await rows() == [(1, 'alpha,beta', 9)]
        await resolve(c.load_data.read_csv("1,it's 雪 $mo$,0\n", table=table, inline=True, escapechar='!'))
        assert await rows() == [(1, "it's 雪 $mo$", 0)]
        with pytest.raises(ValueError, match='backslashes'):
            await resolve(c.load_data.read_csv("1,a\\path,0\n", table=table, inline=True))
        assert await rows() == []
        with pytest.raises(ValueError):
            await resolve(
                c.load_data.read_csv('1,x,0', table=table, inline=True, set_clause={'flag': 'NULLIF(flag,0)'})
            )
        assert await rows() == []
        assert (await resolve(c.execute('SELECT 42'))).scalar() == 42
    finally:
        client.execute('DROP TABLE ' + table)
        for path in files:
            path.unlink(missing_ok=True)


def test_dialect_keeps_initialized_driver_owner(client):
    table, view = name('reflect_'), name('view_')
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,v VECF32(2))')
    client.execute(f'CREATE VIEW {view} AS SELECT id FROM {table}')
    try:
        engines = [client._engine]
        for initialized in (False, True):
            engine = sa.create_engine(client._engine.url)
            engines.append(engine)
            original = engine.dialect
            if initialized:
                with engine.connect() as connection:
                    connection.exec_driver_sql('SELECT 1')
            Client.from_engine(engine)
            assert engine.dialect is original
        for engine in engines:
            assert view in sa.inspect(engine).get_view_names()
            metadata = sa.MetaData()
            metadata.reflect(bind=engine, views=True, only=[view, table])
            assert view in metadata.tables
            with engine.connect() as connection:
                native = connection.exec_driver_sql('SELECT @@transaction_isolation').scalar()
                assert connection.get_isolation_level() == native.replace('-', ' ').upper()
                assert connection.exec_driver_sql('SELECT 42').scalar() == 42
    finally:
        for engine in locals().get('engines', [])[1:]:
            engine.dispose()
        client.execute('DROP VIEW ' + view)
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_session_clone_commit_rollback_and_reuse(client, async_client, kind):
    source, target = name('clone_src_'), name('clone_dst_')
    client.execute(f'CREATE TABLE {source}(id INT PRIMARY KEY)')
    client.execute(f'INSERT INTO {source} VALUES(1)')
    try:
        if kind == 'sync':
            with client.session() as session:
                session.clone.clone_table(target, source)
            client.execute('DROP TABLE ' + target)
            with pytest.raises(CloneError):
                with client.session() as session:
                    session.execute(f'INSERT INTO {source} VALUES(2)')
                    session.clone.clone_table(target, source)
                    session.clone.clone_table(name('clone_bad_'), name('absent_'))
        else:
            async with async_client.session() as session:
                await session.clone.clone_table(target, source)
            client.execute('DROP TABLE ' + target)
            with pytest.raises(CloneError):
                async with async_client.session() as session:
                    await session.execute(f'INSERT INTO {source} VALUES(2)')
                    await session.clone.clone_table(target, source)
                    await session.clone.clone_table(name('clone_bad_'), name('absent_'))
        assert client.execute('SHOW TABLES LIKE ?', (target,)).rows == []
        assert client.execute(f'SELECT id FROM {source}').rows == [(1,)]
        assert client.execute('SELECT 42').scalar() == 42
    finally:
        client.execute('DROP TABLE IF EXISTS ' + target)
        client.execute('DROP TABLE ' + source)
