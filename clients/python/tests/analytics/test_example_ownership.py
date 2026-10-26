# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Run the actual CLIs with real SQLite CREATE/DROP ownership boundaries."""

import importlib.util
import sqlite3
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest


@pytest.mark.parametrize('script', ['example', 'benchmark'])
@pytest.mark.parametrize('collision', [False, True])
@pytest.mark.parametrize('query_failure', [False, True])
def test_cli_drops_only_acknowledged_owned_table(tmp_path, monkeypatch, script, collision, query_failure):
    root = Path(__file__).resolve().parents[2]
    path = root / (
        'examples/example_35_arrow_analytics.py' if script == 'example' else 'tests/analytics/benchmark_analytics.py'
    )
    spec = importlib.util.spec_from_file_location('ownership_cli', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    prefix = 'analytics_example' if script == 'example' else 'analytics_benchmark'
    target = prefix + '_forcedcollision'
    db = sqlite3.connect(':memory:')
    for name in [prefix, target] if collision else [prefix]:
        db.execute('CREATE TABLE ' + name + '(id INTEGER)')
        db.execute('INSERT INTO ' + name + ' VALUES (42)')
    dropped, worker_tables = [], []
    result = pa.table({'id': [1, 2, 3]})

    class Analytics:
        def read_parquet(self, *args):
            return nullcontext(
                SimpleNamespace(sql='files', query=lambda *a: nullcontext(SimpleNamespace(to_arrow=lambda: result)))
            )

        def upload_arrow(self, name, *args):
            db.execute('CREATE TABLE `' + name + '` (id INTEGER)')

        upload_dataframe = upload_arrow

        def query(self, *args, **kwargs):
            if query_failure:
                raise RuntimeError('read failed after upload')
            return nullcontext([result.to_batches()[0]])

    class Client:
        analytics = Analytics()

        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql):
            if sql.startswith('DROP'):
                dropped.append(sql)
            return db.execute(sql)

        def disconnect(self):
            pass

    monkeypatch.setattr(
        module, 'uuid', SimpleNamespace(uuid4=lambda: SimpleNamespace(hex='forcedcollision')), raising=False
    )
    if script == 'example':
        monkeypatch.setattr(module, 'Client', Client)
    else:
        monkeypatch.setattr(module, 'client', Client)

        def run_worker(argv, **kwargs):
            worker_tables.append(argv[argv.index('--table') + 1])
            if query_failure:
                raise RuntimeError('read failed after upload')
            return '{"rows":3,"checksum":3}'

        monkeypatch.setattr(module.subprocess, 'check_output', run_worker)
    monkeypatch.setattr(
        sys,
        'argv',
        [str(path), '--staging-dir', str(tmp_path)]
        + (['--database', 'test'] if script == 'example' else ['--rows', '3', '--repeats', '1']),
    )
    try:
        if collision or query_failure:
            with pytest.raises(sqlite3.OperationalError if collision else RuntimeError):
                module.main()
        else:
            module.main()
        assert db.execute('SELECT id FROM ' + prefix).fetchall() == [(42,)]
        if collision:
            assert db.execute('SELECT id FROM ' + target).fetchall() == [(42,)]
            assert dropped == []
        else:
            assert not db.execute('SELECT name FROM sqlite_master WHERE name=?', (target,)).fetchall()
            assert len(dropped) == 1
            if script == 'benchmark':
                assert worker_tables and set(worker_tables) == {target}
    finally:
        db.close()
