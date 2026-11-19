# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import os
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest
from test_adversarial import hard_client, trial

from matrixone import Client

pa = pytest.importorskip('pyarrow', minversion='18.0.0')
pq = pytest.importorskip('pyarrow.parquet')


@pytest.fixture
def cn2():
    port = os.environ.get('MO_SECOND_CN_PORT')
    if not port:
        pytest.skip('second CN required')
    c = Client(
        host='127.0.0.1',
        port=int(port),
        user='root',
        password='111',
        database=os.environ.get('MO_DATABASE', 'mo_analytics_test'),
        query_timeout=10,
        sql_log_mode='off',
    )
    yield c
    assert c._engine.pool.checkedout() == 0
    c.disconnect()


def rows(c, sql):
    with c.analytics.query(sql) as out:
        return out.to_arrow().to_pylist()


def test_cross_cn_arrow_visibility(trial, cn2):
    table = trial.table()
    trial.client.execute('CREATE TABLE ' + table + '(id BIGINT PRIMARY KEY)')
    trial.analytics.upload_arrow(table, pa.table({'id': range(1000)}), trial.directory, create=False)
    assert rows(cn2, 'SELECT count(*) n,sum(id) s FROM ' + table) == [{'n': 1000, 's': Decimal(499500)}]
    cn2.analytics.upload_arrow(table, pa.table({'id': range(1000, 2000)}), trial.directory, create=False)
    assert trial.query('SELECT count(*) n,sum(id) s FROM ' + table).to_pylist() == [{'n': 2000, 's': Decimal(1999000)}]


def test_cross_cn_pk_conflict_is_atomic(trial, cn2):
    table = trial.table()
    trial.client.execute('CREATE TABLE ' + table + '(id BIGINT PRIMARY KEY)')

    def write(i):
        c = cn2 if i % 2 else trial.client
        try:
            c.analytics.upload_arrow(table, pa.table({'id': [0, i + 1]}), trial.directory, create=False)
            return i + 1
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=12) as pool:
        winners = [r for r in pool.map(write, range(16)) if r is not None]
    assert len(winners) == 1
    assert sorted(trial.rows(table)['id'].to_pylist()) == [0, winners[0]]
    assert sorted(r['id'] for r in rows(cn2, 'SELECT id FROM ' + table)) == [0, winners[0]]


def test_cross_cn_external_and_import(trial, cn2):
    for i in range(8):
        pq.write_table(pa.table({'id': range(i * 100, (i + 1) * 100)}), trial.directory / ('%02d.parquet' % i))
    table = trial.table()
    trial.client.execute('CREATE TABLE ' + table + '(id BIGINT PRIMARY KEY)')
    with cn2.analytics.read_parquet(str(trial.directory / '*.parquet')) as rel:
        assert rel.import_into(table) == 800
    assert trial.query('SELECT count(*) n,sum(id) s FROM ' + table).to_pylist() == [{'n': 800, 's': Decimal(319600)}]


def test_cross_cn_read_after_failed_append(trial, cn2):
    table = trial.table()
    trial.client.execute('CREATE TABLE ' + table + '(id BIGINT PRIMARY KEY)')
    trial.analytics.upload_arrow(table, pa.table({'id': [1]}), trial.directory, create=False)
    with pytest.raises(Exception):
        cn2.analytics.upload_arrow(table, pa.table({'id': [2, 1, 3]}), trial.directory, create=False, batch_size=1)
    assert trial.rows(table)['id'].to_pylist() == [1]
    assert rows(cn2, 'SELECT * FROM ' + table) == [{'id': 1}]
