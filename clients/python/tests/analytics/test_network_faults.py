# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import os
import time

import pytest
from sqlalchemy import event

from matrixone import Client
from matrixone.analytics import AnalyticsCleanupError, AnalyticsCommitUnknownError
from mysql_fault_proxy import FaultProxy
from test_adversarial import hard_client, trial

pa = pytest.importorskip("pyarrow", minversion="18.0.0")


def test_relation_create_lost_commit_acknowledgement(trial):
    import pyarrow.parquet as pq

    path = trial.directory / 'relation.parquet'
    pq.write_table(pa.table({'id': [1, 2]}), path)
    unrelated = trial.table()
    trial.client.execute('CREATE TABLE ' + unrelated + '(id INT)')
    with FaultProxy('drop_create_commit_ack', target_port=int(os.environ.get('MO_PORT', '6001'))) as proxy:
        client = proxied(proxy)
        try:
            with pytest.raises(Exception) as caught:
                client.analytics.read_parquet(path)
            assert proxy.triggered.is_set() and proxy.commit_ack_seen
            assert proxy.create_requests == 1
            assert len(caught.value.attempted_tables) == 1
            assert caught.value.owned_tables == ()
            assert caught.value.file_relation._closed
            assert client._engine.pool.checkedout() == 0
        finally:
            client.disconnect()
    catalog = {row[0] for row in trial.client.execute('SHOW TABLES').rows}
    assert not set(caught.value.attempted_tables) & catalog
    assert unrelated in catalog


def test_relation_name_collision_preserves_existing_table(trial, monkeypatch):
    import uuid
    from types import SimpleNamespace

    import pyarrow.parquet as pq
    import matrixone.analytics as module

    identifier = uuid.uuid4()
    table = 'mo_py_files_' + identifier.hex
    path = trial.directory / 'collision.parquet'
    pq.write_table(pa.table({'id': [1]}), path)
    trial.client.execute('CREATE TABLE ' + table + '(id INT)')
    trial.client.execute('INSERT INTO ' + table + ' VALUES (42)')
    try:
        with monkeypatch.context() as patch:
            patch.setattr(module, 'uuid', SimpleNamespace(uuid4=lambda: identifier))
            with pytest.raises(Exception) as caught:
                trial.analytics.read_parquet(path)
        assert caught.value.orig.args[0] == 1050
        assert caught.value.owned_tables == ()
        assert trial.client.execute('SELECT id FROM ' + table).rows == [(42,)]
    finally:
        trial.client.execute('DROP TABLE IF EXISTS ' + table)


def proxied(proxy):
    return Client(
        host="127.0.0.1",
        port=proxy.port,
        ssl_mode="disabled",  # This local packet proxy deliberately inspects plaintext protocol frames.
        user="root",
        password="111",
        database="mo_analytics_test",
        query_timeout=3,
        connection_timeout=3,
        sql_log_mode="off",
    )


def test_lost_commit_acknowledgement_is_unknown_not_rollback(trial):
    table = trial.table()
    with FaultProxy("drop_commit_ack") as proxy:
        client = proxied(proxy)
        try:
            with pytest.raises(AnalyticsCommitUnknownError) as caught:
                client.analytics.upload_arrow(table, pa.table({"id": range(1000)}), trial.directory)
            assert proxy.triggered.is_set() and proxy.commit_ack_seen
            assert caught.value.committed is None
            assert caught.value.rows == 1000
            assert client._engine.pool.checkedout() == 0
        finally:
            client.disconnect()
    result = trial.rows(table)
    assert result.num_rows == 1000 and sum(result['id'].to_pylist()) == 499500


def test_disconnect_before_commit_leaves_no_partial_create(trial):
    table = trial.table()
    with FaultProxy("disconnect_before_commit") as proxy:
        client = proxied(proxy)
        try:
            with pytest.raises(AnalyticsCommitUnknownError):
                client.analytics.upload_arrow(table, pa.table({"id": range(1000)}), trial.directory)
            assert proxy.triggered.is_set()
        finally:
            client.disconnect()
    deadline = time.monotonic() + 5
    while trial.exists(table) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not trial.exists(table)


def test_stalled_network_times_out_and_releases_pool(trial):
    sql = "SELECT 123 AS n"
    with FaultProxy("stall_result", stall_sql=sql) as proxy:
        client = proxied(proxy)
        try:
            start = time.monotonic()
            with pytest.raises(Exception):
                client.analytics.query(sql, network_timeout=0.25)
            elapsed = time.monotonic() - start
            assert proxy.triggered.is_set() and elapsed < 2
            assert client._engine.pool.checkedout() == 0
            with client.analytics.query('SELECT 321 AS n') as result:
                assert result.to_arrow()['n'].to_pylist() == [321]
        finally:
            client.disconnect()


def test_corrupt_staging_file_rolls_back_target(trial):
    table = trial.table()

    def corrupt(conn, cursor, sql, params, context, many):
        if sql.startswith("LOAD DATA INFILE"):
            with open(params[0], 'r+b') as f:
                f.truncate(20)

    event.listen(trial.client._engine, 'before_cursor_execute', corrupt)
    try:
        with pytest.raises(Exception):
            trial.analytics.upload_arrow(table, pa.table({'id': [1, 2]}), trial.directory)
    finally:
        event.remove(trial.client._engine, 'before_cursor_execute', corrupt)
    assert not trial.exists(table)


def test_staging_unlink_failure_preserves_commit_evidence(trial, monkeypatch):
    import matrixone.analytics as module

    table = trial.table()
    original = os.unlink

    def fail(path, *args, **kwargs):
        if str(path).startswith(str(trial.directory)) and 'mo_py_arrow_' in str(path):
            raise PermissionError('Injected unlink failure')
        return original(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(module.os, 'unlink', fail)
        with pytest.raises(AnalyticsCleanupError) as caught:
            trial.analytics.upload_arrow(table, pa.table({'id': [1, 2]}), trial.directory)
    assert caught.value.committed is True and caught.value.rows == 2
    assert trial.rows(table).num_rows == 2
    for p in trial.directory.glob('mo_py_arrow_*'):
        p.unlink()


def test_cleanup_failure_does_not_mask_original_reader_error(trial, monkeypatch):
    import matrixone.analytics as module

    table = trial.table()
    original = os.unlink

    def batches():
        yield pa.record_batch({'id': [1]})
        raise RuntimeError('original input failure')

    def fail(path, *args, **kwargs):
        if str(path).startswith(str(trial.directory)) and 'mo_py_arrow_' in str(path):
            raise PermissionError('cleanup failure')
        return original(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(module.os, 'unlink', fail)
        reader = pa.RecordBatchReader.from_batches(pa.schema([('id', pa.int64())]), batches())
        with pytest.raises(RuntimeError, match='original input failure') as caught:
            trial.analytics.upload_arrow(table, reader, trial.directory)
    assert isinstance(caught.value.staging_cleanup_error, PermissionError)
    assert not trial.exists(table)
    for p in trial.directory.glob('mo_py_arrow_*'):
        p.unlink()
