# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Returning a raw checkout must not commit its abandoned transaction."""

import sqlite3
from contextlib import closing

import pytest
import sqlalchemy as sa
from sqlalchemy.pool import QueuePool

from matrixone._connection_options import preserve_transactions_with_autocommit


@pytest.fixture
def autocommit_pool(tmp_path):
    database = str(tmp_path / 'reset.sqlite')
    with closing(sqlite3.connect(database)) as setup:
        setup.execute('CREATE TABLE pending (value INTEGER)')
        setup.commit()
    drivers = []

    class Driver:
        def __init__(self):
            self.connection = sqlite3.connect(database, isolation_level=None)
            self.fail_rollback = False
            self.fail_restore = False

        def __getattr__(self, name):
            return getattr(self.connection, name)

        @property
        def isolation_level(self):
            return self.connection.isolation_level

        @isolation_level.setter
        def isolation_level(self, value):
            self.connection.isolation_level = value

        def autocommit(self, enabled):
            if enabled and self.fail_restore:
                raise sqlite3.OperationalError('mode restoration failed')
            # Like MatrixOne, enabling autocommit commits a pending write.
            self.connection.isolation_level = None if enabled else ''

        def rollback(self):
            if self.fail_rollback:
                raise sqlite3.OperationalError('rollback failed')
            self.connection.rollback()

    def create():
        driver = Driver()
        drivers.append(driver)
        return driver

    engine = sa.create_engine('sqlite://', creator=create, poolclass=QueuePool, pool_size=1, max_overflow=0)
    preserve_transactions_with_autocommit(engine)
    try:
        yield engine, database, drivers
    finally:
        engine.dispose()


def _stored_rows(database):
    with closing(sqlite3.connect(database)) as observer:
        with closing(observer.execute('SELECT value FROM pending')) as rows:
            return rows.fetchall()


def test_explicit_sqlalchemy_autocommit_is_not_suspended(autocommit_pool):
    engine, database, _ = autocommit_pool
    with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as connection:
        connection.exec_driver_sql('INSERT INTO pending VALUES (7)')
        assert _stored_rows(database) == [(7,)]
    assert _stored_rows(database) == [(7,)]
    # SQLAlchemy restores its default isolation after the reset callback. The
    # next raw borrower must still receive the SDK's requested autocommit mode.
    with closing(engine.raw_connection()) as raw:
        assert raw.isolation_level is None


@pytest.mark.parametrize('completion, expected', [('abandon', []), ('rollback', []), ('commit', [(7,)])])
def test_raw_checkout_return_keeps_explicit_commit_semantics(autocommit_pool, completion, expected):
    engine, database, _ = autocommit_pool
    raw = engine.raw_connection()
    try:
        raw.autocommit(False)
        cursor = raw.cursor()
        try:
            cursor.execute('INSERT INTO pending VALUES (7)')
        finally:
            cursor.close()
        if completion != 'abandon':
            getattr(raw, completion)()
    finally:
        raw.close()
    assert _stored_rows(database) == expected


@pytest.mark.parametrize('failure', ['fail_rollback', 'fail_restore'])
def test_failed_pool_reset_discards_connection_without_committing(autocommit_pool, failure):
    engine, database, drivers = autocommit_pool
    raw = engine.raw_connection()
    driver = drivers[-1]
    try:
        raw.autocommit(False)
        cursor = raw.cursor()
        try:
            cursor.execute('INSERT INTO pending VALUES (7)')
        finally:
            cursor.close()
        setattr(driver, failure, True)
    finally:
        raw.close()
    assert _stored_rows(database) == []
    # Failed cleanup must not turn into a reusable, wrongly configured session.
    next_raw = engine.raw_connection()
    try:
        assert drivers[-1] is not driver
    finally:
        next_raw.close()
