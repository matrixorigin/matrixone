# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Real driver/pool rollback and explicit SQLAlchemy autocommit boundaries."""

from contextlib import closing
import os
from uuid import uuid4

import pytest

from .test_sdk_quality_contracts import server, client, async_client  # noqa: F401

pytestmark = pytest.mark.skipif(os.environ.get('MO_SDK_ONLINE') != '1', reason='isolated server required')


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('completion', ['abandon', 'rollback', 'commit'])
async def test_untracked_driver_transaction_is_reset_before_pool_reuse(client, async_client, kind, completion):
    table = 'pool_reset_' + uuid4().hex[:12]
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY)')
    try:
        if kind == 'sync':
            with closing(client._engine.raw_connection()) as raw:
                raw.autocommit(False)
                with closing(raw.cursor()) as cursor:
                    cursor.execute(f'INSERT INTO {table} VALUES(7)')
                if completion != 'abandon':
                    getattr(raw, completion)()
            # Observe from another physical driver, before this pool is reused.
            rows = (await async_client.execute(f'SELECT id FROM {table}')).rows
            assert client._engine.pool.checkedout() == 0
        else:
            async with async_client._engine.connect() as connection:
                raw = await connection.get_raw_connection()
                driver = raw.driver_connection
                await driver.autocommit(False)
                async with driver.cursor() as cursor:
                    await cursor.execute(f'INSERT INTO {table} VALUES(7)')
                if completion != 'abandon':
                    await getattr(driver, completion)()
            rows = client.execute(f'SELECT id FROM {table}').rows
            assert async_client._engine.sync_engine.pool.checkedout() == 0
        expected = [(7,)] if completion == 'commit' else []
        assert rows == expected
        assert client.execute(f'SELECT id FROM {table}').rows == expected
        assert (await async_client.execute(f'SELECT id FROM {table}')).rows == expected
        client.execute(f'INSERT INTO {table} VALUES(9)')
        assert (await async_client.execute(f'SELECT id FROM {table} ORDER BY id')).rows == expected + [(9,)]
    finally:
        client.execute('DROP TABLE IF EXISTS ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_explicit_sqlalchemy_autocommit_commits_before_close(client, async_client, kind):
    table = 'explicit_auto_' + uuid4().hex[:12]
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY)')
    try:
        if kind == 'sync':
            with client._engine.connect().execution_options(isolation_level='AUTOCOMMIT') as connection:
                assert str(connection.exec_driver_sql('SELECT @@autocommit').scalar()) == '1'
                connection.exec_driver_sql(f'INSERT INTO {table} VALUES(7)')
                assert (await async_client.execute(f'SELECT id FROM {table}')).rows == [(7,)]
            with closing(client._engine.raw_connection()) as raw:
                assert raw.get_autocommit()
        else:
            async with async_client._engine.connect() as connection:
                await connection.execution_options(isolation_level='AUTOCOMMIT')
                assert str((await connection.exec_driver_sql('SELECT @@autocommit')).scalar()) == '1'
                await connection.exec_driver_sql(f'INSERT INTO {table} VALUES(7)')
                assert client.execute(f'SELECT id FROM {table}').rows == [(7,)]
            async with async_client._engine.connect() as connection:
                raw = await connection.get_raw_connection()
                assert raw.driver_connection.get_autocommit()
        assert client.execute(f'SELECT id FROM {table}').rows == [(7,)]
        assert (await async_client.execute(f'SELECT id FROM {table}')).rows == [(7,)]
    finally:
        client.execute('DROP TABLE IF EXISTS ' + table)
