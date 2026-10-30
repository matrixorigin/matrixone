# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Exercise crash/pressure cleanup against independently queried SQLite catalogs."""

import json
import sqlite3
from types import SimpleNamespace

import pytest
import run_chaos
import run_soak


def test_crash_identity_json_roundtrip_preserves_foreign_table():
    db = sqlite3.connect(':memory:')
    database = 'owned.db%`quoted'
    quoted = '`owned.db%``quoted`'
    name = 'mo_py_files_' + 'a' * 32
    db.execute('ATTACH DATABASE ":memory:" AS ' + quoted)
    db.execute('CREATE TABLE ' + quoted + '.' + name + '(id INTEGER)')
    db.execute('CREATE TABLE ' + name + '(id INTEGER)')
    db.execute('INSERT INTO ' + name + ' VALUES (42)')
    operations = []

    def execute(sql):
        operations.append(sql)
        if sql.startswith('SHOW TABLES FROM '):
            schema = sql[len('SHOW TABLES FROM ') :]
            rows = db.execute('SELECT name FROM ' + schema + '.sqlite_master WHERE type="table"').fetchall()
        else:
            rows = db.execute(sql).fetchall()
        return SimpleNamespace(rows=rows)

    try:
        barrier = json.loads(json.dumps({'tables': [(database, name)]}))
        run_chaos.cleanup_external_tables(SimpleNamespace(execute=execute), barrier['tables'])
        assert db.execute('SELECT id FROM ' + name).fetchall() == [(42,)]
        assert db.execute('SELECT name FROM ' + quoted + '.sqlite_master').fetchall() == []
        assert operations == ['SHOW TABLES FROM ' + quoted, 'DROP TABLE ' + quoted + '.`' + name + '`']
    finally:
        db.close()


@pytest.mark.parametrize('collision', [False, True])
@pytest.mark.parametrize('worker_failure', [False, True])
@pytest.mark.parametrize('entry', ['memory_pressure', 'memory_only'])
def test_pressure_cleanup_requires_upload_acknowledgement(tmp_path, monkeypatch, collision, worker_failure, entry):
    db = sqlite3.connect(':memory:')
    table = 'soak_memory_' + 'b' * 32
    if collision:
        db.execute('CREATE TABLE ' + table + '(id INTEGER)')
        db.execute('INSERT INTO ' + table + ' VALUES (42)')
    disconnected = []
    drops = []

    def upload(name, *args, **kwargs):
        db.execute('CREATE TABLE ' + name + '(id INTEGER)')

    def execute(sql):
        drops.append(sql)
        db.execute(sql)

    c = SimpleNamespace(
        analytics=SimpleNamespace(upload_arrow=upload),
        execute=execute,
        disconnect=lambda: disconnected.append(True),
    )

    def worker(argv, **kwargs):
        if worker_failure:
            raise RuntimeError('worker failed after upload')
        mode = argv[argv.index('--mode') + 1]
        return json.dumps(
            {'rows': 3, 'checksum': 3, 'error': None if mode == 'streamed' else {'classes': ['MemoryError']}}
        )

    monkeypatch.setattr(run_soak, 'uuid', SimpleNamespace(uuid4=lambda: SimpleNamespace(hex='b' * 32)))
    monkeypatch.setattr(run_soak, 'client', lambda: c)
    monkeypatch.setattr(run_soak.subprocess, 'check_output', worker)
    output = tmp_path / 'report.json'
    output.write_text('{"soak": {}}')
    args = SimpleNamespace(rows=3, staging_dir=str(tmp_path), output=str(output))
    try:

        def invoke():
            if entry == 'memory_pressure':
                return run_soak.memory_pressure(c, args, tmp_path)
            return run_soak.memory_only(args)

        if collision or worker_failure:
            with pytest.raises(sqlite3.OperationalError if collision else RuntimeError):
                invoke()
        else:
            invoke()
        if collision:
            assert db.execute('SELECT id FROM ' + table).fetchall() == [(42,)]
            assert drops == []
        else:
            assert not db.execute('SELECT name FROM sqlite_master WHERE name=?', (table,)).fetchall()
            assert len(drops) == 1
        assert bool(disconnected) == (entry == 'memory_only')
    finally:
        db.close()


def test_soak_setup_failure_releases_client_and_source(tmp_path, monkeypatch):
    disconnected = []
    monkeypatch.setattr(run_soak, 'client', lambda: SimpleNamespace(disconnect=lambda: disconnected.append(True)))

    def fail(*args):
        raise RuntimeError('initial query failed')

    monkeypatch.setattr(run_soak, 'read', fail)
    with pytest.raises(RuntimeError, match='initial query failed'):
        run_soak.main(SimpleNamespace(staging_dir=str(tmp_path)))
    assert disconnected == [True]
    assert list(tmp_path.glob('soak_*.parquet')) == []
