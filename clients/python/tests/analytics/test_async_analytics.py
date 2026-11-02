# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Native async parity and ownership checks on the configured SDK test service."""

import asyncio
import os
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from test_adversarial import hard_client, trial  # noqa: F401

from matrixone import AsyncClient
from matrixone.exceptions import QueryError

pa = pytest.importorskip('pyarrow', minversion='18.0.0')


@pytest_asyncio.fixture
async def async_client(trial):
    url = trial.client._engine.url.set(drivername='mysql+aiomysql')
    engine = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=5, pool_reset_on_return='commit')
    client = AsyncClient.from_engine(engine, query_timeout=5, sql_log_mode='off')
    try:
        yield client
        assert engine.pool.checkedout() == 0
        assert client.analytics.pending_operations() == ()
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_async_stream_exact_types_empty_result_and_early_close(trial, async_client):
    table = trial.table()
    trial.client.execute('CREATE TABLE ' + table + '(id BIGINT, amount DECIMAL(30,6), txt VARCHAR(20))')
    trial.client.execute('INSERT INTO ' + table + " VALUES (1,9007199254740993.125000,'a'),(2,NULL,''),(3,2.5,NULL)")
    async with async_client.analytics.query('SELECT * FROM ' + table + ' ORDER BY id', batch_size=2) as stream:
        batches = [batch async for batch in stream]
        assert [batch.num_rows for batch in batches] == [2, 1]
        actual = pa.Table.from_batches(batches)
    assert actual['amount'].to_pylist() == [Decimal('9007199254740993.125000'), None, Decimal('2.500000')]
    assert actual['txt'].to_pylist() == ['a', '', None]
    async with async_client.analytics.query('SELECT id FROM ' + table, batch_size=1) as stream:
        assert (await stream.__anext__()).num_rows == 1
    assert async_client._engine.pool.checkedout() == 0
    async with async_client.analytics.query('SELECT id FROM ' + table + ' WHERE 1=0') as stream:
        empty = await stream.to_arrow()
    assert empty.num_rows == 0 and empty.schema.field('id').type == pa.int64()
    assert (await async_client.execute_driver('SELECT %s', ("literal ? :name 100% O'Reilly",))).scalar() == (
        "literal ? :name 100% O'Reilly"
    )


@pytest.mark.asyncio
async def test_async_file_relation_checks_and_imports_without_catalog_leaks(trial, async_client):
    path = trial.directory / 'async.csv'
    path.write_text('id,txt\n1,a\n2,b\n')
    table = trial.table()
    trial.client.execute('CREATE TABLE ' + table + '(id INT,txt VARCHAR(20))')
    relation = await async_client.analytics.read_csv(path)
    async with relation:
        assert 'mo_py_files_' in await relation.sql()
        async with relation.query(batch_size=1) as stream:
            actual = await stream.to_arrow()
        assert sorted(actual['id'].to_pylist()) == [1, 2]
        await relation.import_into(table)
    assert sorted(trial.client.execute('SELECT * FROM ' + table).rows) == [(1, 'a'), (2, 'b')]
    with pytest.raises(RuntimeError, match='closed'):
        await relation.sql()


@pytest.mark.asyncio
async def test_async_upload_capability_and_complete_values(trial, async_client):
    table = trial.table()
    data = pa.table({'id': [1, 2, 3], 'txt': ['a', '', None]})
    capability = (await async_client.get_capabilities())['arrow_load']
    if capability.state in ('unsupported', 'disabled'):
        with pytest.raises(QueryError) as caught:
            await async_client.analytics.upload_arrow(table, data, trial.directory)
        assert caught.value.phase == 'capability' and caught.value.committed is False
        assert not trial.exists(table)
        return  # Explicit negative server qualification, not a skipped roundtrip.
    assert await async_client.analytics.upload_arrow(table, data, trial.directory, batch_size=1) == 3
    async with async_client.analytics.query('SELECT * FROM ' + table + ' ORDER BY id') as stream:
        actual = await stream.to_arrow()
    assert actual.equals(data)
    assert not list(trial.directory.glob('mo_py_arrow_*'))


@pytest.mark.asyncio
async def test_pool_wait_cancellation_preserves_borrowed_owner(async_client):
    held = await async_client._engine.connect()
    stream = async_client.analytics.query('SELECT 7 AS n')
    task = asyncio.create_task(stream.__aenter__())
    try:
        await asyncio.sleep(0)  # let entry reach admission/pool wait, no elapsed-time oracle
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert async_client._engine.pool.checkedout() == 1
    finally:
        await held.close()
    async with async_client.analytics.query('SELECT 42 AS n') as stream:
        assert (await stream.to_arrow())['n'].to_pylist() == [42]


@pytest.mark.asyncio
async def test_one_pool_slot_stream_and_two_staged_uploads_make_progress(trial, async_client):
    capability = (await async_client.get_capabilities())['arrow_load']
    if capability.state in ('unsupported', 'disabled'):
        # Query streaming remains available on servers without Arrow LOAD.
        async with async_client.analytics.query('SELECT 7 AS n') as stream:
            assert (await stream.to_arrow())['n'].to_pylist() == [7]
        return
    stream = async_client.analytics.query('SELECT 7 AS n', batch_size=1)
    await stream.__aenter__()
    uploads = [
        asyncio.create_task(
            async_client.analytics.upload_arrow(trial.table(), pa.table({'id': [index, index + 10]}), trial.directory)
        )
        for index in range(2)
    ]

    async def staged():
        while True:
            pending = async_client.analytics.pending_operations()
            if len(pending) == 2 and all(item['path'] and not item['worker_running'] for item in pending):
                return
            await asyncio.sleep(0.001)  # wait for observable producer completion

    try:
        await asyncio.wait_for(staged(), 5)
        assert async_client._engine.pool.checkedout() == 1
        assert len(list(trial.directory.glob('mo_py_arrow_*'))) == 2
        assert (await stream.to_arrow())['n'].to_pylist() == [7]
        assert await asyncio.gather(*uploads) == [2, 2]
    finally:
        await stream.close()
        for task in uploads:
            if not task.done():
                task.cancel()
        await asyncio.gather(*uploads, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['read', 'load', 'commit'])
async def test_async_deadline_network_stall_preserves_outcome_and_pool_reuse(trial, fault):
    from sqlalchemy import event
    from mysql_fault_proxy import FaultProxy

    capability = trial.client.get_capabilities()['arrow_load']
    if fault != 'read' and capability.state in ('unsupported', 'disabled'):
        with pytest.raises(QueryError):
            await _unsupported_upload_control(trial)
        return
    port = trial.client._engine.url.port
    table = trial.table()
    with FaultProxy(
        'stall_result', target_port=port, stall_sql='SELECT 123 AS N' if fault == 'read' else None
    ) as proxy:
        engine = create_async_engine(
            trial.client._engine.url.set(drivername='mysql+aiomysql', port=proxy.port),
            pool_size=1,
            max_overflow=0,
            pool_timeout=2,
        )
        client = AsyncClient.from_engine(engine, query_timeout=10, sql_log_mode='off')
        try:
            await client.get_capabilities()
            # Apply the short deadline to the armed fault, not cold dialect setup.
            client.query_timeout = 1
            if fault != 'read':

                @event.listens_for(engine.sync_engine, 'before_cursor_execute')
                def arm(conn, cursor, statement, parameters, context, many):
                    if statement.startswith('LOAD DATA'):
                        proxy.stall_sql = (
                            'COMMIT' if fault == 'commit' else cursor._cursor.mogrify(statement, parameters)
                        )

            with pytest.raises(asyncio.TimeoutError) as caught:
                if fault == 'read':
                    async with client.analytics.query('SELECT 123 AS n') as stream:
                        await stream.to_arrow()
                else:
                    await client.analytics.upload_arrow(table, pa.table({'id': [1, 2]}), trial.directory)
            assert proxy.triggered.is_set()
            if fault != 'read':
                assert caught.value.committed is (None if fault == 'commit' else False)
                assert caught.value.phase == ('commit' if fault == 'commit' else 'load')
                assert caught.value.rows == 2
                assert client.analytics.pending_operations() == ()
                assert not list(trial.directory.glob('mo_py_arrow_*'))
                if fault == 'commit':
                    assert trial.client.execute('SELECT count(*), sum(id) FROM ' + table).rows == [(2, 3)]
                else:
                    assert not trial.exists(table)
            assert engine.pool.checkedout() == 0
            client.query_timeout = 10  # Recovery may initialize a replacement physical connection.
            async with client.analytics.query('SELECT 321 AS n') as stream:
                assert (await stream.to_arrow())['n'].to_pylist() == [321]
        finally:
            await client.disconnect()


async def _unsupported_upload_control(trial):
    engine = create_async_engine(trial.client._engine.url.set(drivername='mysql+aiomysql'))
    client = AsyncClient.from_engine(engine)
    try:
        await client.analytics.upload_arrow(trial.table(), pa.table({'id': [1]}), trial.directory)
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_async_cross_cn_upload_and_file_relation_visibility(trial):
    import os
    from decimal import Decimal

    port = os.environ.get('MO_SECOND_CN_PORT')
    if not port:
        pytest.skip('second CN required for the cross-CN qualification')
    engine = create_async_engine(trial.client._engine.url.set(drivername='mysql+aiomysql', port=int(port)))
    client = AsyncClient.from_engine(engine)
    table = trial.table()
    try:
        data = pa.table({'id': [1, 2, 3]})
        assert await client.analytics.upload_arrow(table, data, trial.directory) == 3
        assert trial.client.execute('SELECT count(*), sum(id) FROM ' + table).rows == [(3, Decimal(6))]
        path = trial.directory / 'cross-cn-async.csv'
        path.write_text('id\n4\n5\n')
        relation = await client.analytics.read_csv(path)
        identities = list(relation._tables)
        async with relation:
            sql = await relation.sql()
            with trial.analytics.query('SELECT sum(id) AS total FROM ' + sql + ' AS files') as stream:
                assert stream.to_arrow()['total'].to_pylist() == [9]
            assert await relation.import_into(table) == 2
        assert trial.client.execute('SELECT count(*), sum(id) FROM ' + table).rows == [(5, Decimal(15))]
        for database, name in identities:
            assert not trial.client.execute('SHOW TABLES LIKE ?', (name,)).rows
    finally:
        await client.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('suffix', [" 'q' {space}", '/part', '%2F', 'ÉCOLE'])
async def test_stage_literal_catalog_and_file_consumers(trial, async_client, kind, suffix):
    import inspect
    import uuid
    from urllib.parse import quote

    async def resolve(value):
        return await value if inspect.isawaitable(value) else value

    client = trial.client if kind == 'sync' else async_client
    name = 'sdk_' + uuid.uuid4().hex[:10] + suffix
    nested_name = 'sdk_nested_' + uuid.uuid4().hex[:10]
    directory = trial.directory / "quoted'directory"
    directory.mkdir()
    source = directory / 'input.csv'
    source.write_text('id,txt\n1,a\n2,b\n')
    url = 'file://' + str(directory) + '/'
    credential = "one'two=three"
    comment = "qa 'quoted' comment 界"
    owned = False
    nested = False
    try:
        if os.environ.get('MO_EXPECT_STAGE_LITERAL_SUPPORT', '1') == '0':
            with pytest.raises(QueryError):
                await resolve(client.stage.create(name, url, comment=comment))
            return  # Explicit negative qualification of an older native server.
        canonical = name.lower()
        try:
            await resolve(
                client.stage.create(
                    name, url, credentials={'AWS_SECRET_KEY': credential}, comment=comment, enable=False
                )
            )
        except BaseException as error:
            owned = bool(getattr(error, 'statement_acknowledged', False))
            raise
        owned = True
        row = trial.client.execute(
            'SELECT url,stage_credentials,stage_status,comment FROM mo_catalog.mo_stages WHERE stage_name=?',
            (canonical,),
        ).rows[0]
        assert row == (url, 'AWS_SECRET_KEY=' + credential, 'disabled', comment)
        stage = await resolve(client.stage.get(name))
        assert (stage.name, stage.url, stage.comment) == (canonical, url, comment)
        await resolve(client.stage.alter(name, enable=True, comment="changed 'comment'"))
        assert (await resolve(client.stage.get(name))).comment == "changed 'comment'"
        # A rejected credential cannot leave an earlier ALTER URL mutation.
        with pytest.raises(QueryError):
            await resolve(
                client.stage.alter(
                    name, url='file:///must-not-change/', credentials={'AWS_SECRET_KEY': 'bad,delimiter'}
                )
            )
        assert (await resolve(client.stage.get(name))).url == url
        uri = 'stage:///' + quote(name, safe='') + '/input.csv'
        relation = await resolve(client.analytics.read_csv(source, server_paths=[uri]))
        if kind == 'sync':
            with relation:
                assert relation.sources == (str(source.resolve()),)
                with relation.query() as stream:
                    assert stream.to_arrow()['id'].to_pylist() == [1, 2]
        else:
            async with relation:
                assert relation.sources == (str(source.resolve()),)
                async with relation.query() as stream:
                    assert (await stream.to_arrow())['id'].to_pylist() == [1, 2]
        table = trial.table()
        trial.client.execute('CREATE TABLE ' + table + '(id BIGINT,txt VARCHAR(20))')
        await resolve(client.load_data.read_csv_stage(name, 'input.csv', table, skiprows=1))
        assert trial.client.execute('SELECT * FROM ' + table + ' ORDER BY id').rows == [(1, 'a'), (2, 'b')]
        await resolve(client.export.to_csv_stage(name, 'output.csv', 'SELECT 7 AS id'))
        assert (directory / 'output.csv').read_text().splitlines() in (['7'], ['id', '7'])
        await resolve(client.stage.create(nested_name, 'stage:///' + quote(name, safe='') + '/'))
        nested = True
        table = trial.table()
        trial.client.execute('CREATE TABLE ' + table + '(id BIGINT,txt VARCHAR(20))')
        await resolve(client.load_data.read_csv_stage(nested_name, 'input.csv', table, skiprows=1))
        assert trial.client.execute('SELECT count(*) FROM ' + table).scalar() == 2
    finally:
        if nested:
            await resolve(client.stage.drop(nested_name))
            assert not trial.client.execute(
                'SELECT stage_name FROM mo_catalog.mo_stages WHERE stage_name=?', (nested_name,)
            ).rows
        if owned:
            await resolve(client.stage.drop(name))
            assert not trial.client.execute(
                'SELECT stage_name FROM mo_catalog.mo_stages WHERE stage_name=?', (canonical,)
            ).rows
