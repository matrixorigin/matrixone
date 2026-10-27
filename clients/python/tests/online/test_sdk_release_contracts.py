"""Release qualification through public SDK entry points and exact native controls."""

import json
import os
import uuid
from pathlib import Path

import pymysql
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base

from matrixone import Client, AsyncClient, select, compile_select_to_sql
from matrixone.exceptions import CloneError, QueryError
from matrixone.session import Session
from matrixone.sqlalchemy_ext.fulltext_index import FulltextSearchBuilder as IndexSearch
from matrixone.sqlalchemy_ext.fulltext_search import FulltextQueryBuilder, FulltextSearchBuilder
from .test_sdk_quality_contracts import server, client, async_client  # noqa: F401
from .test_sdk_manager_contracts import resolve, name

pytestmark = pytest.mark.skipif(os.environ.get('MO_SDK_ONLINE') != '1', reason='explicit isolated target required')


def rows(result):
    return result.rows if hasattr(result, 'rows') else result.fetchall()


@pytest.fixture
def historical_join(client, server):
    a_name, b_name = name('release_a_'), name('release_b_')
    snapshots = []
    database = server['database']
    try:
        client.execute(f'CREATE TABLE {a_name}(id INT PRIMARY KEY)')
        client.execute(f'CREATE TABLE {b_name}(ref INT)')
        client.execute(f'INSERT INTO {a_name} VALUES(1),(2),(3)')
        client.execute(f'INSERT INTO {b_name} VALUES(1)')
        snapshot_base = name('release_sp_')
        for ref in (1, 2):
            client.execute(f'UPDATE {b_name} SET ref={ref}')
            # Collapsing %% to % would select the other existing snapshot.
            snapshot = snapshot_base + '%' * ref + 'name'
            client.snapshots.create(snapshot, 'database', database=database)
            snapshots.append(snapshot)
        client.execute(f'UPDATE {b_name} SET ref=3')
        base = declarative_base()
        model = type(
            'Historical',
            (base,),
            dict(__tablename__=a_name, __table_args__={'schema': database}, id=sa.Column(sa.Integer, primary_key=True)),
        )
        b = sa.Table(b_name, sa.MetaData(), sa.Column('ref', sa.Integer), schema=database)
        statement = select(model.id).join(b, model.id == b.c.ref).where(model.id >= sa.bindparam('minimum', 1))
        yield model, b, statement, snapshots
    finally:
        for snapshot in reversed(snapshots):
            client.snapshots.delete(snapshot)
        client.execute('DROP TABLE IF EXISTS ' + b_name)
        client.execute('DROP TABLE IF EXISTS ' + a_name)


@pytest.mark.asyncio
@pytest.mark.parametrize('owner', ['client', 'async_client', 'session', 'exported_session', 'async_session'])
async def test_cached_snapshot_select_public_owners(client, async_client, historical_join, owner):
    model, b, live, snapshots = historical_join
    a, second = [live.with_snapshot(snapshot) for snapshot in snapshots]

    async def check(executor):
        for statement, expected in ((live, 3), (a, 1), (second, 2), (a, 1), (live, 3)):
            assert [tuple(row) for row in rows(await resolve(executor.execute(statement, {'minimum': 1})))] == [
                (expected,)
            ]
        if owner in ('session', 'exported_session', 'async_session'):
            statement = select(model).join(b, model.id == b.c.ref).with_snapshot(snapshots[0])
            result = await resolve(executor.execute(statement))
            objects = result.scalars().all()
            assert len(objects) == 1 and isinstance(objects[0], model) and objects[0].id == 1

    if owner == 'client':
        await check(client)
    elif owner == 'async_client':
        await check(async_client)
    elif owner == 'session':
        with client.session() as session:
            await check(session)
    elif owner == 'exported_session':
        with Session(bind=client._engine, client=client) as session:
            await check(session)
    else:
        async with async_client.session() as session:
            await check(session)
    assert client.execute(compile_select_to_sql(a)).rows == [(1,)]


def test_snapshot_statement_export_preserves_history(client, historical_join):
    _, _, live, snapshots = historical_join
    filename = name('snapshot_export_') + '.jsonl'
    local = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / filename
    try:
        client.export.to_jsonl(
            os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + filename, live.with_snapshot(snapshots[0])
        )
        assert [json.loads(line) for line in local.read_text().splitlines()] == [{'id': 1}]
    finally:
        local.unlink(missing_ok=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_admin_name_and_password_roundtrip(client, async_client, server, kind):
    c = client if kind == 'sync' else async_client
    stage, user = name('stage_') + '-dash', name('user_')
    password = "quoted'Password7!"
    try:
        created = await resolve(c.stage.create(stage, 'file:///tmp/release-data', comment='initial'))
        assert (created.name, created.comment) == (stage, 'initial')
        assert (await resolve(c.stage.alter(stage, comment=''))).comment == ''
        await resolve(c.account.create_user(user, password))
        connection = pymysql.connect(
            host=server['host'], port=server['port'], user=user, password=password, connect_timeout=5, read_timeout=5
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute('SELECT 1')
                assert cursor.fetchall() == ((1,),)
        finally:
            connection.close()
    finally:
        await resolve(c.stage.drop(stage, if_exists=True))
        await resolve(c.account.drop_user(user, if_exists=True))
    assert client.execute('SELECT count(*) FROM mo_catalog.mo_stages WHERE stage_name=?', (stage,)).scalar() == 0
    assert client.execute('SELECT count(*) FROM mo_catalog.mo_user WHERE user_name=?', (user,)).scalar() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_stage_comment_native_outcome_is_preserved(client, async_client, kind, record_property):
    c = client if kind == 'sync' else async_client
    native, sdk = name('native_stage_'), name('sdk_stage_')
    try:
        try:
            await resolve(c.execute(f"CREATE STAGE {native} URL='file:///tmp/data' COMMENT='O''Reilly'"))
        except QueryError as error:
            # These releases reserialize the accepted comment into catalog SQL.
            assert error.errno in (1064, 1105)
            assert 'syntax error' in str(error).lower()
            with pytest.raises(QueryError) as raised:
                await resolve(c.stage.create(sdk, 'file:///tmp/data', comment="O'Reilly"))
            assert raised.value.errno == error.errno
            assert raised.value.orig is raised.value.__cause__
            assert (
                client.execute(
                    'SELECT count(*) FROM mo_catalog.mo_stages WHERE stage_name IN (?,?)', (native, sdk)
                ).scalar()
                == 0
            )
            record_property('matrixone.feature.stage_apostrophe_comment', 'native-limitation:' + str(error.errno))
        else:
            assert (await resolve(c.stage.get(native))).comment == "O'Reilly"
            assert (await resolve(c.stage.create(sdk, 'file:///tmp/data', comment="O'Reilly"))).comment == "O'Reilly"
            record_property('matrixone.feature.stage_apostrophe_comment', 'supported')
    finally:
        await resolve(c.stage.drop(native, if_exists=True))
        await resolve(c.stage.drop(sdk, if_exists=True))
    assert (await resolve(c.execute('SELECT 42'))).scalar() == 42


@pytest.mark.asyncio
async def test_native_metadata_boolean_matches_structured(client, async_client, server, record_property):
    table = name('hidden_')
    client.execute(f'CREATE TABLE {table}(v INT)')
    try:
        client.execute(f'INSERT INTO {table} VALUES(1),(2)')
        client.moctl.increment_checkpoint()
        await async_client.moctl.increment_checkpoint()
        native = client.execute(
            f"SELECT col_name,object_name,is_hidden FROM metadata_scan('{server['database']}.{table}','*') g"
        ).rows
        assert native
        expected = {(row[0], row[1]): row[2] in (True, 1, 'true', 'TRUE', '1') for row in native}
        for c in (client, async_client):
            actual = await resolve(c.metadata.scan(server['database'], table, columns='*'))
            assert {(row.col_name, row.object_name): row.is_hidden for row in actual} == expected
        record_property(
            'matrixone.feature.metadata_boolean_types', ','.join(sorted({type(row[2]).__name__ for row in native}))
        )
    finally:
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
async def test_fulltext_zero_limit_native_results(client, async_client):
    table = name('fulltext_zero_')
    client.execute('SET experimental_fulltext_index=1')
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,body TEXT)')
    try:
        client.execute(f"INSERT INTO {table} VALUES(1,'hello world'),(2,'other data')")
        client.execute(f'CREATE FULLTEXT INDEX ft ON {table}(body)')
        for limit in (None, 0, 2):
            advanced = FulltextSearchBuilder(client).table(table).columns(['body']).query('hello')
            indexed = IndexSearch(table, ['body']).search('hello')
            if limit is not None:
                advanced.limit(limit)
                indexed.limit(limit)
            sqls = [
                FulltextQueryBuilder().must('hello').as_sql(table, ['body'], limit=limit),
                advanced.explain(),
                indexed.build_sql(),
            ]
            for c in (client, async_client):
                for sql in sqls:
                    result = (await resolve(c.execute(sql))).rows
                    assert [row[0] for row in result] == ([] if limit == 0 else [1])
    finally:
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_clone_verified_success_rolls_back_with_later_error(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    source, target, native_target = name('verified_src_'), name('verified_dst_'), name('native_dst_')
    client.execute(f'CREATE TABLE {source}(id INT PRIMARY KEY)')
    try:
        client.execute(f'INSERT INTO {source} VALUES(1)')

        # Independent native transaction defines source visibility for this release.
        with client._engine.connect() as connection:
            transaction = connection.begin()
            try:
                connection.exec_driver_sql('BEGIN')
                connection.exec_driver_sql(f'INSERT INTO {source} VALUES(2)')
                connection.exec_driver_sql(f'CREATE TABLE {native_target} CLONE {source}')
                native_rows = [
                    tuple(row) for row in connection.exec_driver_sql(f'SELECT id FROM {native_target} ORDER BY id')
                ]
            finally:
                transaction.rollback()
        assert client.execute('SHOW TABLES LIKE ?', (native_target,)).rows == []

        async def mutate(session):
            await resolve(session.execute(f'INSERT INTO {source} VALUES(2)'))
            await resolve(session.clone.clone_table(target, source))
            # Match native visibility; pending source writes remain transaction-owned.
            assert [
                tuple(row) for row in rows(await resolve(session.execute(f'SELECT id FROM {target} ORDER BY id')))
            ] == native_rows
            await resolve(session.execute(f'INSERT INTO {source} VALUES(1)'))

        with pytest.raises(QueryError):
            if kind == 'sync':
                with c.session() as session:
                    await mutate(session)
            else:
                async with c.session() as session:
                    await mutate(session)
        assert client.execute(f'SELECT id FROM {source}').rows == [(1,)]
        assert client.execute('SHOW TABLES LIKE ?', (target,)).rows == []
        await resolve(c.clone.clone_table(target, source))
        await resolve(c.clone.clone_table(target, source, if_not_exists=True))
        assert client.execute(f'SELECT id FROM {target}').rows == [(1,)]
    finally:
        client.execute('DROP TABLE IF EXISTS ' + native_target)
        client.execute('DROP TABLE IF EXISTS ' + target)
        client.execute('DROP TABLE ' + source)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_query_failure_retains_source_and_pool_reuse(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    with pytest.raises(QueryError) as raised:
        await resolve(c.execute('SELECT * FROM ' + name('absent_')))
    assert isinstance(raised.value.orig, sa.exc.DBAPIError)
    assert raised.value.__cause__ is raised.value.orig
    assert isinstance(raised.value.errno, int)
    assert (await resolve(c.execute('SELECT 42'))).rows == [(42,)]


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_external_cache_warmed_before_adoption(client, server, historical_join, kind):
    from sqlalchemy.ext.asyncio import create_async_engine

    _, _, live, snapshots = historical_join
    historical = live.with_snapshot(snapshots[0])
    nested = select(historical.subquery())
    cache = {}
    url = sa.engine.URL.create(
        'mysql+aiomysql' if kind == 'async' else 'mysql+pymysql',
        username=server['user'],
        password=server['password'],
        host=server['host'],
        port=server['port'],
        database=server['database'],
    )
    engine = (create_async_engine if kind == 'async' else sa.create_engine)(url)
    engine = engine.execution_options(compiled_cache=cache)
    dialect, pool = engine.dialect, engine.pool
    try:
        if kind == 'async':
            async with engine.connect() as connection:
                assert (await connection.execute(historical)).fetchall() == [(3,)]
                assert (await connection.execute(nested)).fetchall() == [(3,)]
        else:
            with engine.connect() as connection:
                assert connection.execute(historical).fetchall() == [(3,)]
                assert connection.execute(nested).fetchall() == [(3,)]
        warmed = dict(cache)
        adopted = (AsyncClient if kind == 'async' else Client).from_engine(engine, sql_log_mode='off')
        assert engine.dialect is dialect and engine.pool is pool
        for statement, expected in (
            (historical, 1),
            (live, 3),
            (live.with_snapshot(snapshots[1]), 2),
            (historical, 1),
            (historical.where(sa.literal(0) == 1), None),
        ):
            result = await resolve(adopted.execute(statement))
            assert [tuple(row) for row in result.rows] == ([] if expected is None else [(expected,)])
        with pytest.raises(QueryError, match='Snapshot|snapshot') as rejected:
            await resolve(adopted.execute(nested))
        assert isinstance(rejected.value.orig, ValueError)
        assert all(cache[key] is value for key, value in warmed.items())
        before = len(cache)
        assert (await resolve(adopted.execute(historical))).rows == [(1,)]
        assert len(cache) == before
    finally:
        await resolve(engine.dispose())


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('owner', ['client', 'session'])
@pytest.mark.parametrize('object_type', ['TABLE', 'DATABASE'])
async def test_clone_existing_target_is_noop(client, async_client, server, kind, owner, object_type):
    c = client if kind == 'sync' else async_client
    database = name('clone_db_')
    table = name('Mixed_Target_') + '-Dash'
    target = '`' + table + '`' if object_type == 'TABLE' else database
    location = target if object_type == 'TABLE' else database + '.contents'
    try:
        if object_type == 'DATABASE':
            client.execute('CREATE DATABASE ' + database)
        client.execute('CREATE TABLE ' + location + '(id INT PRIMARY KEY)')
        client.execute('INSERT INTO ' + location + ' VALUES(99)')

        async def check(executor):
            for _ in range(2):
                method = executor.clone.clone_table if object_type == 'TABLE' else executor.clone.clone_database
                await resolve(method(target, 'missing_source', if_not_exists=True))
                assert [tuple(row) for row in rows(await resolve(executor.execute('SELECT id FROM ' + location)))] == [
                    (99,)
                ]

        if owner == 'client':
            await check(c)
        elif kind == 'sync':
            with c.session() as session:
                await check(session)
        else:
            async with c.session() as session:
                await check(session)
        assert client.execute('SELECT id FROM ' + location).rows == [(99,)]
    finally:
        if object_type == 'DATABASE':
            client.execute('DROP DATABASE IF EXISTS ' + database)
        else:
            client.execute('DROP TABLE IF EXISTS ' + target)


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', [0, 1, 2])
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('owner', ['client', 'session'])
@pytest.mark.parametrize('object_type', ['TABLE', 'QUALIFIED_TABLE', 'DATABASE'])
async def test_clone_target_case_matches_native_mode(server, mode, kind, owner, object_type):
    # Global changes are safe only on an isolated server. New physical connections
    # inherit the new value; existing pooled connections do not.
    if os.environ.get('MO_SDK_DISPOSABLE_SERVER') != '1':
        pytest.fail('Set MO_SDK_DISPOSABLE_SERVER=1 only for an exclusive disposable server or tenant')
    admin = pymysql.connect(
        host=server['host'], port=server['port'], user=server['user'], password=server['password'], autocommit=True
    )
    c = None
    original_mode = None
    target = 'Case_' + uuid.uuid4().hex[:12]
    lower = target.lower()
    fresh = 'fresh_' + uuid.uuid4().hex[:12]
    source = 'source_' + uuid.uuid4().hex[:12]
    database = server['database']
    is_database = object_type == 'DATABASE'
    try:
        with admin.cursor() as cursor:
            cursor.execute('SELECT @@global.lower_case_table_names')
            original_mode = int(cursor.fetchone()[0])
            cursor.execute('SET GLOBAL lower_case_table_names = %s', (mode,))
        c = (AsyncClient if kind == 'async' else Client)(sql_log_mode='off')
        await resolve(c.connect(**server))
        assert int((await resolve(c.execute('SELECT @@lower_case_table_names'))).scalar()) == mode
        await resolve(c.execute('CREATE TABLE ' + source + '(id INT)'))
        await resolve(c.execute('INSERT INTO ' + source + ' VALUES(42)'))
        if is_database:
            await resolve(c.execute('CREATE DATABASE ' + lower))
            await resolve(c.execute('CREATE TABLE ' + lower + '.contents(id INT)'))
            await resolve(c.execute('INSERT INTO ' + lower + '.contents VALUES(99)'))
            target_arg, source_arg = target, database
        else:
            await resolve(c.execute('CREATE TABLE ' + lower + '(id INT)'))
            await resolve(c.execute('INSERT INTO ' + lower + ' VALUES(99)'))
            prefix = database + '.' if object_type == 'QUALIFIED_TABLE' else ''
            target_arg, source_arg = prefix + target, prefix + source

        async def check(executor):
            assert int((await resolve(executor.execute('SELECT @@lower_case_table_names'))).scalar()) == mode
            method = executor.clone.clone_database if is_database else executor.clone.clone_table
            if mode == 0:
                # Released servers execute CLONE using a lowercasing background
                # executor even in mode 0. Surface that native conflict rather
                # than reporting a successful no-op for the absent exact name.
                exact = target if is_database else target_arg
                try:
                    await resolve(method(target_arg, source_arg, if_not_exists=True))
                except CloneError as error:
                    assert 'already exists' in str(error).lower() or 'acknowledged' in str(error).lower()
                    with pytest.raises(QueryError):
                        await resolve(
                            executor.execute('SHOW CREATE ' + ('DATABASE ' if is_database else 'TABLE ') + exact)
                        )
                    if is_database:
                        await resolve(executor.execute('CREATE DATABASE ' + target))
                    else:
                        await resolve(executor.execute('CREATE TABLE ' + exact + '(id INT)'))
                else:
                    location = target + '.' + source if is_database else exact
                    assert [
                        tuple(row) for row in rows(await resolve(executor.execute('SELECT id FROM ' + location)))
                    ] == [(42,)]
                await resolve(method(target_arg, 'missing_source', if_not_exists=True))
                fresh_arg = database + '.' + fresh if object_type == 'QUALIFIED_TABLE' else fresh
                await resolve(method(fresh_arg, source_arg, if_not_exists=True))
                fresh_location = fresh + '.' + source if is_database else fresh_arg
                assert [
                    tuple(row) for row in rows(await resolve(executor.execute('SELECT id FROM ' + fresh_location)))
                ] == [(42,)]
            else:
                await resolve(method(target_arg, 'missing_source', if_not_exists=True))
                if mode == 1:
                    mixed_fresh = 'F' + fresh[1:]
                    fresh_arg = database + '.' + mixed_fresh if object_type == 'QUALIFIED_TABLE' else mixed_fresh
                    await resolve(method(fresh_arg, source_arg, if_not_exists=True))
                    fresh_location = mixed_fresh + '.' + source if is_database else fresh_arg
                    assert [
                        tuple(row) for row in rows(await resolve(executor.execute('SELECT id FROM ' + fresh_location)))
                    ] == [(42,)]
            location = lower + '.contents' if is_database else lower
            assert [tuple(row) for row in rows(await resolve(executor.execute('SELECT id FROM ' + location)))] == [
                (99,)
            ]
            if mode == 0 and object_type == 'QUALIFIED_TABLE':
                # The differently-cased schema must not alias this database.
                with pytest.raises(CloneError):
                    await resolve(method(database.upper() + '.' + lower, database + '.' + source, if_not_exists=True))

        if owner == 'client':
            await check(c)
        elif kind == 'sync':
            with c.session() as session:
                await check(session)
        else:
            async with c.session() as session:
                await check(session)
    finally:
        try:
            if c is not None:
                try:
                    if is_database:
                        await resolve(c.execute('DROP DATABASE IF EXISTS ' + target))
                        await resolve(c.execute('DROP DATABASE IF EXISTS ' + lower))
                        await resolve(c.execute('DROP DATABASE IF EXISTS ' + fresh))
                    else:
                        await resolve(c.execute('DROP TABLE IF EXISTS ' + target))
                        await resolve(c.execute('DROP TABLE IF EXISTS ' + lower))
                        await resolve(c.execute('DROP TABLE IF EXISTS ' + fresh))
                    await resolve(c.execute('DROP TABLE IF EXISTS ' + source))
                finally:
                    await resolve(c.disconnect())
        finally:
            try:
                if original_mode is not None:
                    with admin.cursor() as cursor:
                        cursor.execute('SET GLOBAL lower_case_table_names = %s', (original_mode,))
            finally:
                admin.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_clone_snapshot_convenience_single_connection_pool(client, server, kind):
    from sqlalchemy.ext.asyncio import create_async_engine

    source, target, snapshot = name('single_src_'), name('single_dst_'), name('single_snapshot_')
    client.execute(f'CREATE TABLE {source}(id INT PRIMARY KEY)')
    engine = None
    try:
        client.execute(f'INSERT INTO {source} VALUES(1)')
        client.snapshots.create(snapshot, 'database', database=server['database'])
        url = sa.engine.URL.create(
            'mysql+aiomysql' if kind == 'async' else 'mysql+pymysql',
            username=server['user'],
            password=server['password'],
            host=server['host'],
            port=server['port'],
            database=server['database'],
        )
        engine = (create_async_engine if kind == 'async' else sa.create_engine)(
            url, pool_size=1, max_overflow=0, pool_timeout=0.5
        )
        c = (AsyncClient if kind == 'async' else Client).from_engine(engine, query_timeout=5, sql_log_mode='off')

        async def check(session):
            await resolve(session.clone.clone_table_with_snapshot(target, source, snapshot))
            assert [tuple(row) for row in rows(await resolve(session.execute('SELECT id FROM ' + target)))] == [(1,)]
            await resolve(
                session.clone.clone_table_with_snapshot(
                    target, 'missing_source', 'missing_snapshot', if_not_exists=True
                )
            )
            assert engine.pool.checkedout() == 1
            raise RuntimeError('caller requests rollback')

        with pytest.raises(RuntimeError, match='caller requests rollback'):
            if kind == 'async':
                async with c.session() as session:
                    await check(session)
            else:
                with c.session() as session:
                    await check(session)
        assert engine.pool.checkedout() == 0
        assert client.execute('SHOW TABLES LIKE ?', (target,)).rows == []
        assert (await resolve(c.execute('SELECT 42'))).scalar() == 42
    finally:
        if engine is not None:
            await resolve(engine.dispose())
        client.snapshots.delete(snapshot)
        client.execute('DROP TABLE IF EXISTS ' + target)
        client.execute('DROP TABLE ' + source)
