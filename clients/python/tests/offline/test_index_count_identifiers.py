# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Index count verification must preserve literal table identifiers."""

from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from matrixone import Client, AsyncClient


def seed(connection, main, index, mismatch):
    for name, count in [(main, 2), (index, 1 if mismatch else 2)]:
        quoted = '`' + name.replace('`', '``') + '`'
        connection.exec_driver_sql(f'CREATE TABLE {quoted} (id INTEGER)')
        for value in range(count):
            connection.exec_driver_sql(f'INSERT INTO {quoted} VALUES ({value})')


@pytest.mark.parametrize('main,index', [('plain', 'idx'), ('a`b', 'idx'), ('plain', 'i`x')])
@pytest.mark.parametrize('mismatch', [False, True])
def test_sync_verify_identifiers(main, index, mismatch, monkeypatch):
    client = Client(logger=Mock())
    client._engine = create_engine('sqlite:///:memory:')
    monkeypatch.setattr(
        'matrixone.index_utils.read_index_metadata',
        lambda connection, table: ('main', main, [(1, 'ix', index, 'MULTIPLE', 'id', 1, '', '')]),
    )
    try:
        with client._engine.begin() as conn:
            seed(conn, main, index, mismatch)
        if mismatch:
            with pytest.raises(ValueError, match='Index count verification failed'):
                client.verify_table_index_counts(main)
        else:
            assert client.verify_table_index_counts(main) == 2
    finally:
        client.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('main,index', [('plain', 'idx'), ('a`b', 'idx'), ('plain', 'i`x')])
@pytest.mark.parametrize('mismatch', [False, True])
async def test_async_verify_identifiers(main, index, mismatch, monkeypatch):
    client = AsyncClient(logger=Mock())
    client._engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    monkeypatch.setattr(
        'matrixone.index_utils.read_index_metadata',
        lambda connection, table: ('main', main, [(1, 'ix', index, 'MULTIPLE', 'id', 1, '', '')]),
    )
    try:
        async with client._engine.begin() as conn:
            await conn.run_sync(lambda sync: seed(sync, main, index, mismatch))
        if mismatch:
            with pytest.raises(ValueError, match='Index count verification failed'):
                await client.verify_table_index_counts(main)
        else:
            assert await client.verify_table_index_counts(main) == 2
    finally:
        await client.disconnect()
