# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Test-only profile and exact SDK limitation oracles; never runtime switches."""

import os

from matrixone.exceptions import QueryError

RELEASE_TARGETS = {
    '4.2.8': ('8.0.30-MatrixOne-v4.2.8', '10d204b'),
    '4.2.8-hotfix-20261114': ('8.0.30-MatrixOne-v4.2.8-hotfix-20261114', 'c660300'),
}


def selected_release_target():
    target = os.environ.get('MO_SDK_TARGET')
    if target == 'current-source' and os.environ.get('MO_SDK_EXPECT_VERSION') == '4.2.8-hotfix':
        revision = os.environ.get('MO_SDK_EXPECT_REVISION')
        assert revision, 'Release-branch source qualification requires an exact revision'
        return '4.2.8-hotfix', ('8.0.30-MatrixOne-v4.2.8-hotfix', revision)
    return target, RELEASE_TARGETS.get(target)


def release_428(executor):
    _, target = selected_release_target()
    if target is None:
        return False
    if hasattr(executor, 'dialect'):
        with executor.connect() as conn:
            version, revision = conn.exec_driver_sql('SELECT version(),git_version()').one()
    else:
        version, revision = executor.execute('SELECT version(),git_version()').rows[0]
    assert version == target[0] and revision.startswith(target[1])
    return True


def assert_release_error(error, feature, native_errno=None):
    version, _ = selected_release_target()
    version = version or '4.2.8'
    current, seen = error, set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, QueryError) and getattr(current, 'compatibility_feature', None) == feature:
            assert current.backend_version == version and current.compatibility_hint
            assert 'MatrixOne v' + version + ' compatibility:' in str(current)
            if native_errno is None:
                assert current.phase in ('compatibility', 'metadata_reflection') and current.committed is False
                assert current.statement_acknowledged is (None if current.phase == 'metadata_reflection' else False)
                assert current.errno is None and current.orig is None
            else:
                assert current.errno == native_errno and current.orig is not None
            return current
        current = getattr(current, 'orig', None) or current.__cause__
    raise AssertionError('Missing exact SDK compatibility diagnostic: ' + feature)


def native_execute(client, sql):
    """Independent native control bypasses SDK listeners and never retries."""
    raw = client.get_sqlalchemy_engine().raw_connection()
    try:
        cursor = raw.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()
    finally:
        try:
            raw.rollback()
        finally:
            raw.close()
