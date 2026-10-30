# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Desired session settings follow real QueuePool physical generations."""

import sqlite3

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

from matrixone.sqlalchemy_ext.ivf_config import IVFConfig
from matrixone.sqlalchemy_ext.hnsw_config import HNSWConfig


@pytest.fixture
def setting_engine():
    failure = set()

    class Cursor(sqlite3.Cursor):
        def execute(self, statement, parameters=()):
            if statement.startswith('SET '):
                name = statement.split()[1]
                if name in failure:
                    raise RuntimeError('setting rejected')
                self.connection.settings[name] = parameters[0]
                return self
            return super().execute(statement, parameters)

    class Physical(sqlite3.Connection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.settings = {'experimental_ivf_index': 0, 'experimental_hnsw_index': 0}

        def cursor(self, *args, **kwargs):
            return super().cursor(factory=Cursor)

    engine = create_engine(
        'sqlite://',
        creator=lambda: sqlite3.connect(':memory:', factory=Physical),
        poolclass=QueuePool,
        pool_size=2,
        max_overflow=0,
    )
    try:
        yield engine, failure
    finally:
        engine.dispose()


def settings(connection):
    return connection.connection.driver_connection.settings


@pytest.mark.parametrize(
    'config_type,flag,prefix',
    [(IVFConfig, 'experimental_ivf_index', 'ivf'), (HNSWConfig, 'experimental_hnsw_index', 'hnsw')],
)
def test_configuration_follows_checkout_return_and_dispose(setting_engine, config_type, flag, prefix):
    engine, _ = setting_engine
    config = config_type(engine)
    with engine.connect() as borrowed:
        assert getattr(config, 'enable_' + prefix + '_indexing')()
        assert settings(borrowed)[flag] == 0  # Already borrowed: preserve owner.
        with engine.connect() as fresh:
            assert settings(fresh)[flag] == 1
    with engine.connect() as reused:
        assert settings(reused)[flag] == 1
    engine.dispose()
    with engine.connect() as recycled:
        assert settings(recycled)[flag] == 1
    assert getattr(config, 'disable_' + prefix + '_indexing')()
    with engine.connect() as disabled:
        assert settings(disabled)[flag] == 0
    assert engine.pool.checkedout() == 0


def test_failed_multi_setting_generation_keeps_previous_policy(setting_engine):
    engine, failure = setting_engine
    config = IVFConfig(engine)
    assert config.enable_ivf_indexing()
    failure.add('probe_limit')
    assert config.configure_ivf(enabled=False, probe_limit=3) is False
    failure.clear()
    with engine.connect() as recovered:
        assert settings(recovered)['experimental_ivf_index'] == 1
        assert 'probe_limit' not in settings(recovered)
    assert engine.pool.checkedout() == 0


@pytest.mark.parametrize('value', [0, -1, True, 1.5])
def test_invalid_probe_limit_never_checks_out(setting_engine, value):
    engine, _ = setting_engine
    with pytest.raises(ValueError):
        IVFConfig(engine).set_probe_limit(value)
    assert not hasattr(engine, '_matrixone_pool_settings')


def test_failed_publication_cannot_be_inherited_by_concurrent_success(monkeypatch):
    import threading
    from matrixone.connection_hooks import ConnectionHook, configure_pool_settings

    engine = create_engine('sqlite://', poolclass=QueuePool, connect_args={'check_same_thread': False})
    first_entered, release_first, second_waiting = threading.Event(), threading.Event(), threading.Event()
    outcomes = {}

    def apply(hook, connection, record):
        if threading.current_thread().name == 'failed-ivf':
            first_entered.set()
            assert release_first.wait(5), 'publication barrier was not released'
            raise OSError('first publication rejected')
        # SQLite has no MO SET grammar; this test checks the real pool's
        # publication lifecycle, while the other fixture checks applied values.
        record.info['observed_policy'] = dict(hook._settings)

    monkeypatch.setattr(ConnectionHook, '_apply_settings', apply)

    def publish(name, **settings):
        try:
            configure_pool_settings(engine, **settings)
            outcomes[name] = 'success'
        except OSError:
            outcomes[name] = 'failed'

    first = threading.Thread(target=publish, args=('ivf',), kwargs={'experimental_ivf_index': 1}, name='failed-ivf')
    first.start()
    try:
        assert first_entered.wait(5)
        hook = engine._matrixone_pool_settings
        publication_lock = hook._settings_publication_lock

        class ObservedLock:
            def __enter__(self):
                second_waiting.set()
                publication_lock.acquire()

            def __exit__(self, *args):
                publication_lock.release()

        hook._settings_publication_lock = ObservedLock()
        second = threading.Thread(target=publish, args=('hnsw',), kwargs={'experimental_hnsw_index': 1})
        second.start()
        assert second_waiting.wait(5)
        assert hook._settings == {'experimental_ivf_index': 1}
        release_first.set()
        first.join(5)
        second.join(5)
        assert not first.is_alive() and not second.is_alive()
        assert outcomes == {'ivf': 'failed', 'hnsw': 'success'}
        assert hook._settings == {'experimental_hnsw_index': 1}
        with engine.connect() as connection:
            assert connection.info['observed_policy'] == {'experimental_hnsw_index': 1}
        assert engine.pool.checkedout() == 0
    finally:
        release_first.set()
        first.join(5)
        engine.dispose()
