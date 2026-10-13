# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import os
import uuid

import pytest

from matrixone import Client
from test_adversarial import hard_client

pa = pytest.importorskip('pyarrow', minversion='18.0.0')
pq = pytest.importorskip('pyarrow.parquet')


@pytest.fixture
def tenants(hard_client):
    names = ['advacc_' + uuid.uuid4().hex[:16] for _ in range(2)]
    clients = []
    try:
        for name in names:
            hard_client.execute("CREATE ACCOUNT `" + name + "` ADMIN_NAME 'admin' IDENTIFIED BY '111'")
            c = Client(
                host='127.0.0.1',
                port=int(os.environ.get('MO_PORT', '6001')),
                account=name,
                user='admin',
                password='111',
                sql_log_mode='off',
            )
            c.connect(
                host='127.0.0.1',
                port=int(os.environ.get('MO_PORT', '6001')),
                account=name,
                user='admin',
                password='111',
                database=None,
            )
            c.execute('CREATE DATABASE analytics_tenant')
            c.disconnect()
            c = Client(
                host='127.0.0.1',
                port=int(os.environ.get('MO_PORT', '6001')),
                account=name,
                user='admin',
                password='111',
                database='analytics_tenant',
                sql_log_mode='off',
            )
            clients.append(c)
        yield names, clients
    finally:
        for c in clients:
            c.disconnect()
        for name in names:
            hard_client.execute('DROP ACCOUNT IF EXISTS `' + name + '`')


def test_arrow_tables_are_account_isolated(tenants, tmp_path):
    names, (a, b) = tenants
    a.analytics.upload_arrow('same_name', pa.table({'id': [1]}), tmp_path)
    b.analytics.upload_arrow('same_name', pa.table({'id': [2]}), tmp_path)
    for c, expected in [(a, 1), (b, 2)]:
        with c.analytics.query('SELECT * FROM same_name') as out:
            assert out.to_arrow()['id'].to_pylist() == [expected]
    assert a._engine.pool.checkedout() == 0 and b._engine.pool.checkedout() == 0


def test_external_catalog_is_account_isolated(tenants, tmp_path):
    _, (a, b) = tenants
    p = tmp_path / 'input.parquet'
    pq.write_table(pa.table({'id': [1]}), p)
    with a.analytics.read_parquet(p) as rel:
        with pytest.raises(Exception):
            b.analytics.query('SELECT * FROM ' + rel.sql + ' AS f')
        with rel.query() as out:
            assert out.to_arrow()['id'].to_pylist() == [1]
    assert not [r for r in a.execute('SHOW TABLES').rows if r[0].startswith('mo_py_files_')]


def test_read_only_user_cannot_create_or_append(tenants, tmp_path):
    names, (admin, _) = tenants
    admin.analytics.upload_arrow('protected', pa.table({'id': [1]}), tmp_path)
    admin.execute('CREATE ROLE reader')
    admin.execute('GRANT SELECT ON TABLE analytics_tenant.protected TO reader')
    admin.execute("CREATE USER viewer IDENTIFIED BY '111' DEFAULT ROLE reader")
    admin.execute('GRANT reader TO viewer')
    c = Client(
        host='127.0.0.1',
        port=int(os.environ.get('MO_PORT', '6001')),
        account=names[0],
        user='viewer',
        password='111',
        database='analytics_tenant',
        sql_log_mode='off',
    )
    try:
        with c.analytics.query('SELECT * FROM protected') as out:
            assert out.to_arrow()['id'].to_pylist() == [1]
        with pytest.raises(Exception):
            c.analytics.upload_arrow('forbidden', pa.table({'id': [2]}), tmp_path)
        with pytest.raises(Exception):
            c.analytics.upload_arrow('protected', pa.table({'id': [2]}), tmp_path, create=False)
        p = tmp_path / 'input.parquet'
        pq.write_table(pa.table({'id': [1]}), p)
        with pytest.raises(Exception):
            c.analytics.read_parquet(p)
        with admin.analytics.query('SELECT * FROM protected') as out:
            assert out.to_arrow()['id'].to_pylist() == [1]
        assert not list(tmp_path.glob('mo_py_arrow_*')) and c._engine.pool.checkedout() == 0
    finally:
        c.disconnect()
