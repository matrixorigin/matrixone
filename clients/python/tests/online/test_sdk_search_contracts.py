# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Small real-server oracles for scoring, metadata and physical connection lifetime."""

import asyncio
import os
import warnings

import pytest
from matrixone import Client, AsyncClient
from matrixone.exceptions import QueryError
from matrixone.sqlalchemy_ext import AdvancedFulltextSearchBuilder, FulltextSearchMode
from .test_sdk_quality_contracts import server, client, async_client  # noqa: F401
from .test_sdk_manager_contracts import resolve, name

pytestmark = pytest.mark.skipif(os.environ.get('MO_SDK_ONLINE') != '1', reason='explicit isolated target required')


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('metric,function', [('cosine', 'cosine_distance'), ('ip', 'inner_product')])
async def test_vector_metric_filters_and_live_schema(client, async_client, kind, metric, function):
    c = client if kind == 'sync' else async_client
    table = name('vectors_')
    client.execute('SET experimental_ivf_index=1')
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,price INT,v VECF32(2),other VECF32(2))')
    try:
        client.execute(
            f"INSERT INTO {table} VALUES(1,50,'[10,0]','[10,0]'),(2,150,'[1,1]','[1,1]'),(3,550,'[0,1]','[0,1]')"
        )
        client.execute(f"CREATE INDEX idx_v USING ivfflat ON {table}(v) LISTS 1 op_type 'vector_{metric}_ops'")
        client.execute(f"CREATE INDEX idx_other USING ivfflat ON {table}(other) LISTS 1 op_type 'vector_l2_ops'")
        handle = c.get_pinecone_index(table, vector_column='v')
        query = handle.query if kind == 'sync' else handle.query_async
        result = await resolve(query([1, 0], top_k=3, include_metadata=False))
        native = client.execute(f"SELECT id,{function}(v,'[1,0]') FROM {table} ORDER BY {function}(v,'[1,0]')").rows
        assert [m.id for m in result.matches] == [str(r[0]) for r in native]
        assert [m.score for m in result.matches] == pytest.approx([r[1] for r in native], abs=1e-6)
        result = await resolve(query([1, 0], top_k=3, filter={'price': {'$gte': 100, '$lt': 500}}))
        assert [m.id for m in result.matches] == ['2']
        other = c.get_pinecone_index(table, vector_column='other')
        info = await resolve(other._get_index_info() if kind == 'sync' else other._get_index_info_async())
        assert info['metric'] == 'l2'
        client.execute(f"ALTER TABLE {table} ADD COLUMN tag VARCHAR(20) DEFAULT 'new'")
        assert all(m.metadata['tag'] == 'new' for m in (await resolve(query([1, 0], top_k=3))).matches)
        client.execute(f'ALTER TABLE {table} DROP COLUMN tag')
        assert all('tag' not in m.metadata for m in (await resolve(query([1, 0], top_k=3))).matches)
        with pytest.raises(ValueError):
            await resolve(query([1, 0], filter={'price': {'$gte': 0, '$unknown': 10}}))
        assert client.execute(f'SELECT COUNT(*) FROM {table}').scalar() == 3
    finally:
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_fulltext_algorithm_restores_owned_connection(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    table = name('fulltext_')
    client.execute('SET experimental_fulltext_index=1')
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,content TEXT)')
    try:
        client.execute(
            f"INSERT INTO {table} VALUES(1,'orange banana'),(2,'orange orange orange orange banana'),(3,'orange apple grape lemon peach pear banana'),(4,'banana grape lemon peach pear')"
        )
        client.execute(f'CREATE FULLTEXT INDEX idx_ft ON {table}(content)')
        builder = (
            AdvancedFulltextSearchBuilder(c)
            .table(table)
            .columns(['content'])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .query('orange')
            .with_score()
            .select(['id'])
            .order_by('id', 'ASC')
        )
        await resolve(c.execute("SET ft_relevancy_algorithm='BM25'"))
        native = (await resolve(c.execute(builder.explain()))).rows
        await resolve(c.execute("SET ft_relevancy_algorithm='TF-IDF'"))
        tfidf = (await resolve(builder.execute())).rows
        assert [r[1] for r in native] != [r[1] for r in tfidf]
        rows = (await resolve(builder.with_algorithm('BM25').execute())).rows
        assert [r[0] for r in rows] == [r[0] for r in native]
        assert [r[1] for r in rows] == pytest.approx([r[1] for r in native], abs=1e-6)
        assert (await resolve(c.execute('SELECT @@ft_relevancy_algorithm'))).scalar() == 'TF-IDF'
        builder.table(name('missing_'))
        with pytest.raises(QueryError):
            await resolve(builder.execute())
        assert (await resolve(c.execute('SELECT @@ft_relevancy_algorithm'))).scalar() == 'TF-IDF'
    finally:
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
async def test_metadata_distinct_nonempty_objects(client, async_client):
    table = name('objects_')
    db = client._connection_params['database']
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,v VARCHAR(20))')
    try:
        client.execute(f"INSERT INTO {table} VALUES(1,'x'),(2,NULL)")
        client.moctl.increment_checkpoint()
        native = {
            r[0] for r in client.execute(f"SELECT DISTINCT object_name FROM metadata_scan('{db}.{table}','*') g").rows
        }
        assert native
        for c in (client, async_client):
            result = await resolve(c.metadata.scan(db, table, distinct_object_name=True))
            assert {r[0] for r in result.rows} == native
            assert result.columns == ['object_name']
            selected = await resolve(c.metadata.scan(db, table, distinct_object_name=True, columns=['object_name']))
            assert {r['object_name'] for r in selected} == native
            with pytest.raises(ValueError):
                await resolve(c.metadata.scan(db, table, distinct_object_name=True, columns='*'))
        assert client.execute(f'SELECT id,v FROM {table} ORDER BY id').rows == [(1, 'x'), (2, None)]
    finally:
        client.execute('DROP TABLE ' + table)


def test_sync_hook_runs_once_per_physical_generation(server):
    calls = []
    c = Client(sql_log_mode='off')

    def hook(owner):
        with pytest.raises(RuntimeError, match='cannot reenter'):
            owner.execute('SELECT 1')
        calls.append(1)

    config = dict(server, on_connect=hook)
    c.connect(**config)
    try:
        first = c.execute('SELECT CONNECTION_ID()').scalar()
        assert len(calls) == 1
        assert c.execute('SELECT CONNECTION_ID()').scalar() == first
        with c._engine.connect() as held:
            assert held.exec_driver_sql('SELECT CONNECTION_ID()').scalar() == first
            assert c.execute('SELECT CONNECTION_ID()').scalar() != first
            assert len(calls) == 2
        c._engine.dispose()
        assert c.execute('SELECT 42').scalar() == 42
        assert len(calls) == 3 and c._engine.pool.checkedout() == 0
    finally:
        c.disconnect()


@pytest.mark.asyncio
async def test_async_hook_admission_reuse_and_cancellation(server):
    calls = []
    c = AsyncClient(sql_log_mode='off')

    async def setup(owner):
        await asyncio.sleep(0)  # Force an await; no timing or race assertion.
        with pytest.raises(RuntimeError, match='cannot reenter'):
            await owner.execute('SELECT 1')
        calls.append(1)

    # A synchronous callable returning an awaitable must also be awaited.
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        await c.connect(**dict(server, on_connect=lambda owner: setup(owner)))
        try:
            first = (await c.execute('SELECT CONNECTION_ID()')).scalar()
            assert len(calls) == 1
            async with c._engine.connect() as held:
                assert (await held.exec_driver_sql('SELECT CONNECTION_ID()')).scalar() == first
                assert (await c.execute('SELECT CONNECTION_ID()')).scalar() != first
                assert len(calls) == 2
            assert c._engine.pool.checkedout() == 0
            await c._engine.dispose()
            assert (await c.execute('SELECT 42')).scalar() == 42
            assert len(calls) == 3
        finally:
            await c.disconnect()
        assert not any('never awaited' in str(w.message) for w in seen)
    entered = asyncio.Event()

    async def blocked(owner):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(c.connect(**dict(server, on_connect=blocked)))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert c._engine is None
    await c.connect(**server)
    try:
        assert (await c.execute('SELECT 42')).scalar() == 42
    finally:
        await c.disconnect()


@pytest.mark.asyncio
async def test_cancelled_clone_session_rolls_back(client, async_client):
    source, target = name('cancel_src_'), name('cancel_dst_')
    entered = asyncio.Event()
    client.execute(f'CREATE TABLE {source}(id INT PRIMARY KEY)')
    client.execute(f'INSERT INTO {source} VALUES(1)')

    async def work():
        async with async_client.session() as session:
            await session.execute(f'INSERT INTO {source} VALUES(2)')
            await session.clone.clone_table(target, source)
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(work())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.execute('SHOW TABLES LIKE ?', (target,)).rows == []
        assert client.execute(f'SELECT id FROM {source}').rows == [(1,)]
        assert async_client._engine.pool.checkedout() == 0
        assert (await async_client.execute('SELECT 42')).scalar() == 42
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        client.execute('DROP TABLE IF EXISTS ' + target)
        client.execute('DROP TABLE ' + source)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_ivf_stats_follow_session_use(client, async_client, server, kind):
    other = name('ivf_db_')
    table = name('ivf_')
    original = server['database']
    client.execute('CREATE DATABASE ' + other)
    try:
        client.execute('SET experimental_ivf_index=1')
        for database, count in [(original, 2), (other, 3)]:
            client.execute(f'CREATE TABLE {database}.{table}(id INT PRIMARY KEY,v VECF32(2))')
            values = ','.join(f"({i},'[{i},1]')" for i in range(1, count + 1))
            client.execute(f'INSERT INTO {database}.{table} VALUES {values}')
            client.execute(f"CREATE INDEX idx_v USING ivfflat ON {database}.{table}(v) LISTS 1 op_type 'vector_l2_ops'")

        def check(stats):
            assert stats['database'] == other
            assert sum(stats['distribution']['centroid_count']) == 3

        if kind == 'sync':
            with client.session() as session:
                try:
                    session.execute('USE ' + other)
                    check(session.vector_ops.get_ivf_stats(table))
                    explicit = session.vector_ops.get_ivf_stats(table, database=original)
                    assert sum(explicit['distribution']['centroid_count']) == 2
                finally:
                    session.execute('USE ' + original)
        else:
            async with async_client.session() as session:
                try:
                    await session.execute('USE ' + other)
                    check(await session.vector_ops.get_ivf_stats(table))
                    explicit = await session.vector_ops.get_ivf_stats(table, database=original)
                    assert sum(explicit['distribution']['centroid_count']) == 2
                finally:
                    await session.execute('USE ' + original)
    finally:
        client.execute(f'DROP TABLE IF EXISTS {original}.{table}')
        client.execute('DROP DATABASE IF EXISTS ' + other)
