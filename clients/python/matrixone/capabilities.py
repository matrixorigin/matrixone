# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Bounded, read-only observations, never inferred from a version number."""

from dataclasses import dataclass
from typing import Tuple
import re
import uuid

from sqlalchemy.exc import DBAPIError

from .connection_hooks import pool_settings_version


@dataclass(frozen=True)
class Capability:
    state: str
    reason: str
    source: str
    server_identity: Tuple[str, str]


PROBES = {
    'right_join': 'SELECT a.n,b.n FROM (SELECT 1 n) a RIGHT JOIN (SELECT 2 n) b ON a.n=b.n',
    'ivf_index': "SHOW VARIABLES LIKE 'experimental_ivf_index'",
    'hnsw_index': "SHOW VARIABLES LIKE 'experimental_hnsw_index'",
    'fulltext_index': "SHOW VARIABLES LIKE 'experimental_fulltext_index'",
    'arrow_load': "EXPLAIN LOAD DATA INFILE {'filepath'='SDK_PROBE_PATH','format'='arrow'} INTO TABLE mo_catalog.mo_database",
}
NAMES = frozenset(PROBES) | {'snapshot', 'export_jsonline'}
STATES = frozenset(('supported', 'disabled', 'unsupported', 'unknown'))


def validate_overrides(overrides):
    result = dict(overrides or {})
    if any(key not in NAMES or value not in STATES for key, value in result.items()):
        raise ValueError('Unknown capability name or state')
    return result


def initial_capabilities(identity, overrides):
    result = {
        name: Capability(
            'unknown', 'No safe read-only probe; qualify the operation on this server', 'unprobed', identity
        )
        for name in NAMES
    }
    for name, state in overrides.items():
        result[name] = Capability(state, 'Explicit caller assertion; not SDK certification', 'override', identity)
    return result


def observation(name, rows, identity):
    if name == 'arrow_load':
        return Capability('unknown', 'EXPLAIN does not certify native Arrow LOAD execution', 'probe', identity)
    if name == 'right_join':
        supported = [tuple(row) for row in rows] == [(None, 2)]
        return Capability(
            'supported' if supported else 'unknown',
            'Constant-table RIGHT JOIN semantics observed' if supported else 'Unexpected probe rows',
            'probe',
            identity,
        )
    if not rows:
        return Capability('unsupported', 'Experimental setting is absent', 'probe', identity)
    value = str(rows[0][1]).lower()
    state = 'supported' if value in ('on', '1', 'true') else 'disabled' if value in ('off', '0', 'false') else 'unknown'
    return Capability(
        state,
        'Observed session setting only; index semantics and other pool sessions are unqualified',
        'probe',
        identity,
    )


def failed_observation(error, identity, name=None):
    # Preserve original driver errors in their owning executor. Do not expose
    # server text, which can contain credentials or caller SQL, in diagnostics.
    code = error.orig.args[0] if getattr(error.orig, 'args', ()) else None
    if name == 'arrow_load' and code == 20300:
        message = str(error.orig.args[1]) if len(error.orig.args) > 1 else ''
        if re.search(r"\bformat\s+['\"]?arrow['\"]?\s+is\s+not\s+supported\b", message, re.IGNORECASE):
            return Capability(
                'unsupported', 'Server rejects the native Arrow LOAD format (code 20300)', 'probe', identity
            )
    state = 'unsupported' if name != 'arrow_load' and code in (1064, 1193, 1235) else 'unknown'
    return Capability(
        state, 'Database probe failed (code %s); check syntax, permissions and connectivity' % code, 'probe', identity
    )


def _probe_statement(name, statement):
    if name == 'arrow_load':
        return statement.replace('SDK_PROBE_PATH', '/__matrixone_sdk_readonly_probe_' + uuid.uuid4().hex + '.arrow')
    return statement


def require_arrow_override(client):
    state = client._capability_overrides.get('arrow_load')
    if state in ('unsupported', 'disabled'):
        from .exceptions import QueryError

        raise QueryError(
            'Native Arrow LOAD is %s by explicit caller assertion' % state, phase='capability', committed=False
        )


def _raise_arrow_rejection(error):
    value = failed_observation(error, (), 'arrow_load')
    if value.state == 'unsupported':
        from .exceptions import QueryError

        raise QueryError(
            'Native Arrow LOAD is unsupported: ' + value.reason, phase='capability', committed=False, orig=error
        ) from error
    if error.connection_invalidated:
        raise error


def check_arrow_load(connection):
    """Qualify only this selected write checkout; pooled observations are hints."""
    try:
        result = connection.exec_driver_sql(_probe_statement('arrow_load', PROBES['arrow_load']))
        result.close()
    except DBAPIError as error:
        _raise_arrow_rejection(error)


async def check_arrow_load_async(connection):
    try:
        result = await connection.exec_driver_sql(_probe_statement('arrow_load', PROBES['arrow_load']))
        result.close()
    except DBAPIError as error:
        _raise_arrow_rejection(error)


def get_capabilities(client, refresh=False):
    engine = client._engine
    if engine is None:
        from .exceptions import ConnectionError

        raise ConnectionError('Client must be connected before capability discovery')
    generation = client._compatibility_generation
    policy = pool_settings_version(engine)
    with engine.connect() as connection:
        result = connection.exec_driver_sql('SELECT VERSION(), git_version()')
        try:
            identity = tuple(str(v) for v in result.fetchone())
        finally:
            result.close()
        if (
            generation != client._compatibility_generation
            or client._engine is not engine
            or pool_settings_version(engine) != policy
        ):
            raise RuntimeError('Connection changed during capability discovery; retry explicitly')
        cached = client._capability_cache
        cache_key = identity, policy, engine, generation
        if not refresh and cached is not None and cached[0] == cache_key:
            return dict(cached[1])
        values = initial_capabilities(identity, client._capability_overrides)
        for name, sql in PROBES.items():
            if name in client._capability_overrides:
                continue
            try:
                result = connection.exec_driver_sql(_probe_statement(name, sql))
                try:
                    values[name] = observation(name, result.fetchall(), identity)
                finally:
                    result.close()
            except DBAPIError as error:
                values[name] = failed_observation(error, identity, name)
                if error.connection_invalidated:
                    raise
    if (
        generation != client._compatibility_generation
        or client._engine is not engine
        or pool_settings_version(engine) != policy
    ):
        raise RuntimeError('Connection changed during capability discovery; retry explicitly')
    client._capability_cache = (cache_key, values)
    return dict(values)


async def get_capabilities_async(client, refresh=False):
    engine = client._engine
    if engine is None:
        from .exceptions import ConnectionError

        raise ConnectionError('Client must be connected before capability discovery')
    generation = client._compatibility_generation
    policy = pool_settings_version(engine)
    async with engine.connect() as connection:
        result = await connection.exec_driver_sql('SELECT VERSION(), git_version()')
        try:
            identity = tuple(str(v) for v in result.fetchone())
        finally:
            result.close()
        if (
            generation != client._compatibility_generation
            or client._engine is not engine
            or pool_settings_version(engine) != policy
        ):
            raise RuntimeError('Connection changed during capability discovery; retry explicitly')
        cached = client._capability_cache
        cache_key = identity, policy, engine, generation
        if not refresh and cached is not None and cached[0] == cache_key:
            return dict(cached[1])
        values = initial_capabilities(identity, client._capability_overrides)
        for name, sql in PROBES.items():
            if name in client._capability_overrides:
                continue
            try:
                result = await connection.exec_driver_sql(_probe_statement(name, sql))
                try:
                    values[name] = observation(name, result.fetchall(), identity)
                finally:
                    result.close()
            except DBAPIError as error:
                values[name] = failed_observation(error, identity, name)
                if error.connection_invalidated:
                    raise
    if (
        generation != client._compatibility_generation
        or client._engine is not engine
        or pool_settings_version(engine) != policy
    ):
        raise RuntimeError('Connection changed during capability discovery; retry explicitly')
    client._capability_cache = (cache_key, values)
    return dict(values)
