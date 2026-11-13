# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Version-qualified manager inputs, catalog identities and transaction outcomes."""

import asyncio
import gzip
import inspect
import os
import uuid
from pathlib import Path
from decimal import Decimal

import pytest
import sqlalchemy as sa

from matrixone import Client, SnapshotLevel
from matrixone.exceptions import BranchError, CloneError, PubSubError, QueryError, SnapshotError
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
        path = source('.csv', b'1|alpha|0;2|beta|0;')
        await resolve(c.load_data.read_csv(path, table, sep='|', lines_terminated_by=';'))
        assert await rows() == [(1, 'alpha', 0), (2, 'beta', 0)]
        with pytest.raises(ValueError, match='sep'):
            await resolve(c.load_data.read_csv(path, table, fields_terminated_by='|'))
        with pytest.raises(ValueError, match='NULLIF'):
            await resolve(c.load_data.read_csv(path, table, set_clause={'flag': '1'}))
        assert await rows() == []
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


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('scope', ['database', 'table'])
async def test_snapshot_restore_rolls_back_real_data(client, async_client, kind, scope):
    from matrixone.exceptions import RestoreError

    c = client if kind == 'sync' else async_client
    db, snap = name('restore_db_'), name('restore_sp_')
    client.execute('CREATE DATABASE ' + db)
    try:
        client.execute(f'CREATE TABLE {db}.t(id INT PRIMARY KEY, amount DECIMAL(20,6))')
        client.execute(f'INSERT INTO {db}.t VALUES (1, 1.250000)')
        client.execute(f'CREATE VIEW {db}.v AS SELECT id, amount FROM {db}.t')
        target = db if scope == 'database' else db + ' t'
        client.execute(f'CREATE SNAPSHOT {snap} FOR {scope.upper()} {target}')
        client.execute(f'INSERT INTO {db}.t VALUES (2, 2.500000)')
        method = c.restore.restore_database if scope == 'database' else c.restore.restore_table
        args = (snap, 'sys', db) if scope == 'database' else (snap, 'sys', db, 't')
        with pytest.raises(RestoreError):
            await resolve(method(name('missing_'), *args[1:]))
        assert client.execute(f'SELECT id FROM {db}.t ORDER BY id').rows == [(1,), (2,)]
        assert await resolve(method(*args))
        assert client.execute(f'SELECT id, amount FROM {db}.v ORDER BY id').rows == [(1, Decimal('1.250000'))]
        assert (await resolve(c.execute('SELECT 42'))).scalar() == 42
    finally:
        client.execute('DROP SNAPSHOT IF EXISTS ' + snap)
        client.execute('DROP DATABASE IF EXISTS ' + db)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_fulltext_embedded_backtick_column(client, async_client, kind):
    from matrixone.sqlalchemy_ext.fulltext_search import boolean_match

    c = client if kind == 'sync' else async_client
    table = name('ft_ident_')
    client.execute(f'CREATE TABLE {table}(id BIGINT PRIMARY KEY, `tick``col` TEXT)')
    try:
        client.execute(f"INSERT INTO {table} VALUES (1, 'alpha beta'), (2, 'gamma')")
        if os.environ.get('MO_SDK_TARGET') == '4.2.5':
            # This release predates the server fix for generated identifier SQL.
            with pytest.raises(QueryError, match='syntax error'):
                client.execute(f'CREATE FULLTEXT INDEX ft_idx ON {table} (`tick``col`)')
            assert client.execute(f'SELECT id, `tick``col` FROM {table} ORDER BY id').rows == [
                (1, 'alpha beta'),
                (2, 'gamma'),
            ]
            return
        client.execute(f'CREATE FULLTEXT INDEX ft_idx ON {table} (`tick``col`)')
        predicate = boolean_match('tick`col').must('alpha').compile()
        assert (await resolve(c.execute(f'SELECT id FROM {table} WHERE {predicate}'))).rows == [(1,)]
        client.execute(f"INSERT INTO {table} VALUES (3, 'alpha delta')")
        assert (await resolve(c.execute(f'SELECT id FROM {table} WHERE {predicate} ORDER BY id'))).rows == [(1,), (3,)]
        client.execute(f"UPDATE {table} SET `tick``col`='gamma' WHERE id=1")
        assert (await resolve(c.execute(f'SELECT id FROM {table} WHERE {predicate} ORDER BY id'))).rows == [(3,)]
        client.execute(f'DELETE FROM {table} WHERE id=3')
        assert (await resolve(c.execute(f'SELECT id FROM {table} WHERE {predicate} ORDER BY id'))).rows == []
        assert client.execute(f'SELECT id, `tick``col` FROM {table} ORDER BY id').rows == [
            (1, 'gamma'),
            (2, 'gamma'),
        ]
    finally:
        client.execute('DROP TABLE IF EXISTS ' + table)


@pytest.mark.asyncio
async def test_async_fulltext_special_identifiers_catch_up(client):
    table = name('ft_async_ident_')
    parser_table = name('ft_async_parser_')
    try:
        client.execute(
            f'CREATE TABLE {table} (`key``id` BIGINT PRIMARY KEY, `tick``col` TEXT, '
            '`order` TEXT, FULLTEXT ft_idx (`tick``col`, `order`) ASYNC)'
        )
        client.execute(f"INSERT INTO {table} VALUES (1, 'uniquestarttoken', 'reservedtoken')")
        if os.environ.get('MO_SDK_TARGET') == '4.2.5':
            # This release does not reliably catch up this index; check the
            # underlying rows while the sync prerequisite is tested above.
            assert client.execute(f'SELECT * FROM {table}').rows == [(1, 'uniquestarttoken', 'reservedtoken')]
            return

        async def wait_for(token, expected):
            predicate = f"MATCH(`tick``col`, `order`) AGAINST('{token}')"
            for _ in range(120):
                rows = client.execute(f'SELECT `key``id` FROM {table} WHERE {predicate} ORDER BY `key``id`').rows
                if rows == expected:
                    return
                await asyncio.sleep(0.5)
            pytest.fail(f'Async fulltext index did not catch up for {token}: {rows!r}')

        await wait_for('uniquestarttoken', [(1,)])
        await wait_for('reservedtoken', [(1,)])
        client.execute(f"UPDATE {table} SET `tick``col`='uniquenewtoken' WHERE `key``id`=1")
        await wait_for('uniquenewtoken', [(1,)])
        await wait_for('uniquestarttoken', [])
        client.execute(f'DELETE FROM {table} WHERE `key``id`=1')
        await wait_for('uniquenewtoken', [])
        assert client.execute(f'SELECT * FROM {table}').rows == []

        client.execute(
            f'CREATE TABLE {parser_table} (id BIGINT PRIMARY KEY, body TEXT, '
            'FULLTEXT ft_idx(body) WITH PARSER gojieba ASYNC)'
        )
        client.execute(f"INSERT INTO {parser_table} VALUES (1, '苹果香蕉')")
        for _ in range(120):
            rows = client.execute(f"SELECT id FROM {parser_table} WHERE MATCH(body) AGAINST('苹果香蕉')").rows
            if rows == [(1,)]:
                break
            await asyncio.sleep(0.5)
        assert rows == [(1,)], 'Async fulltext parser configuration did not reach the writer'
    finally:
        client.execute('DROP TABLE IF EXISTS ' + parser_table)
        client.execute('DROP TABLE IF EXISTS ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_branch_lineage_ddl_uses_client_autocommit(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    source, branch, ordinary = name('ddl_src_'), name('ddl_branch_'), name('ddl_plain_')
    try:
        client.execute(f'CREATE TABLE {source}(id INT PRIMARY KEY)')
        client.execute(f'INSERT INTO {source} VALUES (1), (2)')
        client.execute(f'CREATE TABLE {ordinary}(id INT PRIMARY KEY)')
        await resolve(c.branch.create(branch, source))
        with pytest.raises(QueryError):
            if kind == 'sync':
                with c.session() as session:
                    session.execute(f'ALTER TABLE {branch} ADD COLUMN extra INT DEFAULT 9')
            else:
                async with c.session() as session:
                    await session.execute(f'ALTER TABLE {branch} ADD COLUMN extra INT DEFAULT 9')
        assert client.execute(f'SELECT id FROM {branch} ORDER BY id').rows == [(1,), (2,)]
        await resolve(c.execute(f'ALTER TABLE {branch} ADD COLUMN extra INT DEFAULT 9'))
        assert client.execute(f'SELECT id,extra FROM {branch} ORDER BY id').rows == [(1, 9), (2, 9)]
        await resolve(c.execute(f'ALTER TABLE {ordinary} ADD COLUMN extra INT DEFAULT 9'))
        await resolve(c.execute(f'TRUNCATE TABLE {branch}'))
        assert client.execute(f'SELECT id FROM {branch}').rows == []
        assert client.execute(f'SELECT id FROM {source} ORDER BY id').rows == [(1,), (2,)]
        assert (await resolve(c.execute('SELECT 42'))).scalar() == 42
        pool = c._engine.pool if kind == 'sync' else c._engine.sync_engine.pool
        assert pool.checkedout() == 0
    finally:
        for table in (branch, ordinary, source):
            client.execute('DROP TABLE IF EXISTS ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_column_projection_join_keeps_mapped_sources(client, async_client, kind):
    from sqlalchemy import Column, Integer, String
    from sqlalchemy.orm import aliased, declarative_base

    c = client if kind == 'sync' else async_client
    other_db, fact, dim = name('join_db_'), name('fact_'), name('dim_')
    client.execute('CREATE DATABASE ' + other_db)
    try:
        client.execute(f'CREATE TABLE {fact}(id INT PRIMARY KEY)')
        client.execute(f'CREATE TABLE {other_db}.{dim}(id INT PRIMARY KEY,label VARCHAR(20))')
        client.execute(f'CREATE TABLE {dim}(id INT PRIMARY KEY,label VARCHAR(20))')
        client.execute(f'INSERT INTO {fact} VALUES (17),(18)')
        client.execute(f"INSERT INTO {other_db}.{dim} VALUES (17,'remote')")
        client.execute(f"INSERT INTO {dim} VALUES (17,'local')")
        base = declarative_base()
        Fact = type('Fact', (base,), {'__tablename__': fact, 'id': Column(Integer, primary_key=True)})
        Dim = type(
            'Dim',
            (base,),
            {
                '__tablename__': dim,
                '__table_args__': {'schema': other_db},
                'id': Column(Integer, primary_key=True),
                'label': Column(String(20)),
            },
        )
        query = c.query(Fact.id, Dim.label)
        with pytest.raises(ValueError, match='JOIN'):
            await resolve(query.all())
        with pytest.raises(ValueError, match='JOIN'):
            await resolve(c.query(Fact.id, Dim.label).update(id=999).execute())
        with pytest.raises(ValueError, match='JOIN'):
            await resolve(c.query(Fact.id, Dim.label).delete())
        assert client.execute(f'SELECT id FROM {fact} ORDER BY id').rows == [(17,), (18,)]
        rows = await resolve(query.join(Dim, Fact.id == Dim.id).all())
        assert [tuple(row) for row in rows] == [(17, 'remote')]
        peer = aliased(Fact, name='peer')
        rows = await resolve(c.query(Fact.id, peer.id.label('peer_id')).join(peer, peer.id == Fact.id + 1).all())
        assert [tuple(row) for row in rows] == [(17, 18)]
    finally:
        client.execute('DROP TABLE IF EXISTS ' + fact)
        client.execute('DROP TABLE IF EXISTS ' + dim)
        client.execute('DROP DATABASE IF EXISTS ' + other_db)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('scope', ['account', 'cluster'])
async def test_disposable_snapshot_restore_account_and_cluster(client, async_client, kind, scope):
    if os.environ.get('MO_SDK_DISPOSABLE_SERVER') != '1':
        pytest.fail('Account and cluster restore require an exclusively owned disposable server')
    c = client if kind == 'sync' else async_client
    db, snap = name('restore_global_db_'), name('restore_global_sp_')
    client.execute('CREATE DATABASE ' + db)
    try:
        client.execute(f'CREATE TABLE {db}.t(id INT PRIMARY KEY)')
        client.execute(f'INSERT INTO {db}.t VALUES (1)')
        client.execute(f'CREATE SNAPSHOT {snap} FOR {scope.upper()}')
        client.execute(f'INSERT INTO {db}.t VALUES (2)')
        if scope == 'account':
            assert await resolve(c.restore.restore_tenant(snap, 'sys'))
        else:
            assert await resolve(c.restore.restore_cluster(snap))
        assert client.execute(f'SELECT id FROM {db}.t ORDER BY id').rows == [(1,)]
        assert (await resolve(c.execute('SELECT 42'))).scalar() == 42
    finally:
        try:
            client.execute('DROP SNAPSHOT IF EXISTS ' + snap)
        finally:
            client.execute('DROP DATABASE IF EXISTS ' + db)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_disposable_restore_account_to_another_account(client, async_client, kind):
    if os.environ.get('MO_SDK_DISPOSABLE_SERVER') != '1':
        pytest.fail('Cross-account restore requires an exclusively owned disposable server')
    import secrets

    c = client if kind == 'sync' else async_client
    source, destination, snapshot = name('restore_src_'), name('restore_dst_'), name('restore_sp_')
    password = secrets.token_hex(16)
    source_client, destination_client = Client(sql_log_mode='off'), Client(sql_log_mode='off')
    try:
        try:
            client.account.create_account(source, 'restore_admin', password)
            client.account.create_account(destination, 'restore_admin', password)
        except Exception:
            raise RuntimeError('Could not create disposable account fixtures') from None
        options = dict(
            host=os.environ['MO_HOST'], port=int(os.environ['MO_PORT']), password=password, database='mo_catalog'
        )
        source_client.connect(user=source + ':restore_admin', **options)
        source_client.execute('CREATE DATABASE restore_data')
        source_client.execute('CREATE TABLE restore_data.t(id INT PRIMARY KEY)')
        source_client.execute('INSERT INTO restore_data.t VALUES (1)')
        client.execute(f'CREATE SNAPSHOT {snapshot} FOR ACCOUNT {source}')
        source_client.execute('INSERT INTO restore_data.t VALUES (2)')
        assert await resolve(c.restore.restore_tenant(snapshot, source, destination))
        destination_client.connect(user=destination + ':restore_admin', **options)
        assert destination_client.execute('SELECT id FROM restore_data.t ORDER BY id').rows == [(1,)]
        assert source_client.execute('SELECT id FROM restore_data.t ORDER BY id').rows == [(1,), (2,)]
    finally:
        source_client.disconnect()
        destination_client.disconnect()
        try:
            client.execute('DROP SNAPSHOT IF EXISTS ' + snapshot)
        finally:
            for account in (destination, source):
                client.execute('DROP ACCOUNT IF EXISTS ' + account)
