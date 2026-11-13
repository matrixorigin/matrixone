# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Explicit connection security and timeout options shared by SDK clients."""

import json
import math
import ssl

import pymysql
from pymysql.constants import CLIENT


def positive_timeout(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError('%s must be positive and finite' % name)
    return value


def resolve_auto_commit(value, inherited):
    """Resolve an explicit connection override without losing ``False``."""
    resolved = inherited if value is None else value
    if not isinstance(resolved, bool):
        raise TypeError('auto_commit must be a boolean')
    return resolved


def _driver_connection(connection):
    """Resolve the DBAPI connection behind a SQLAlchemy sync or adapted connection."""
    fairy = connection.connection
    return getattr(fairy, 'dbapi_connection', None) or getattr(fairy, 'connection', fairy)


def _suspend_autocommit(connection):
    """Enter SQLAlchemy's transaction with driver autocommit suspended.

    MatrixOne rejects administrative statements such as ``CREATE STAGE`` inside
    an explicit transaction, so emitting ``BEGIN`` would break them. Disabling
    driver autocommit reproduces the non-autocommit session that
    ``Client.execute`` already expects, and keeps ``commit``/``rollback``
    effective. The requested mode is restored when the connection is reused.
    """
    if connection.get_execution_options().get('isolation_level') == 'AUTOCOMMIT':
        return
    driver = _driver_connection(connection)
    setter = getattr(driver, 'autocommit', None)
    if callable(setter):
        setter(False)
    else:
        connection.exec_driver_sql('SET autocommit = 0')


def _restore_autocommit(dbapi_connection, *args):
    # SQLAlchemy 2.x supplies a reset state; 1.4 only supplies the pool record.
    reset_state = args[1] if len(args) > 1 else None
    if getattr(reset_state, 'terminate_only', False):
        return
    setter = getattr(dbapi_connection, 'autocommit', None)
    if not callable(setter):
        return
    # Reset listeners precede the pool's rollback. Enabling autocommit first
    # would commit a transaction abandoned through raw_connection(). Failures
    # must reach the pool so it discards the unusable physical connection.
    if not getattr(reset_state, 'transaction_was_reset', False):
        dbapi_connection.rollback()
    setter(True)


def _checkout_autocommit(dbapi_connection, connection_record, connection_proxy):
    # SQLAlchemy's isolation finalizer runs after reset and can disable
    # autocommit again. Restore the requested baseline before publishing a lease.
    setter = getattr(dbapi_connection, 'autocommit', None)
    if callable(setter):
        setter(True)


def preserve_transactions_with_autocommit(engine):
    """Keep SQLAlchemy transaction boundaries effective on autocommit DBAPI connections."""
    from sqlalchemy import event

    event.listen(engine, 'begin', _suspend_autocommit)
    event.listen(engine, 'reset', _restore_autocommit)
    event.listen(engine, 'checkout', _checkout_autocommit)


def redact_error(error, password):
    message = str(error)
    if not password:
        return message
    # DBAPI errors format their args with repr(), and SQLAlchemy embeds that
    # representation in its own message. Match escaped forms before the raw
    # password so replacing a raw substring cannot break the escaped match.
    quoted = repr(password)
    # Remove only the enclosing quote's escape, preserving escaped backslashes
    # and printable Unicode. The surrounding error can choose either quote.
    escaped = quoted[1:-1].replace('\\' + quoted[0], quoted[0])
    representations = {
        password,
        repr(password)[1:-1],
        ascii(password)[1:-1],
        escaped,
        escaped.replace("'", "\\'"),
        escaped.replace('"', '\\"'),
        json.dumps(password)[1:-1],
        json.dumps(password, ensure_ascii=False)[1:-1],
    }
    for value in sorted(representations, key=len, reverse=True):
        message = message.replace(value, '***')
    return message


def tls_options(params):
    mode = params.get('ssl_mode', 'preferred')
    if mode not in ('disabled', 'preferred', 'required'):
        raise ValueError('ssl_mode must be disabled, preferred or required')
    if mode == 'disabled':
        return {'ssl_disabled': True}
    if mode == 'required' or params.get('ssl_ca'):
        context = ssl.create_default_context(cafile=params.get('ssl_ca'))
        # Explicit context preserves hostname checking when system roots are
        # used; some driver versions disable it when no ca path is supplied.
        if params.get('ssl_cert'):
            context.load_cert_chain(params['ssl_cert'], params.get('ssl_key'))
        return {'ssl': context}
    if params.get('ssl_cert') or params.get('ssl_key'):
        raise ValueError('A client certificate requires ssl_ca or ssl_mode=required')
    return {}


class TLSRequiredConnection(pymysql.connections.Connection):
    """Fail before authentication when a peer cannot negotiate required TLS.

    Older supported PyMySQL versions otherwise silently fall back to plaintext.
    Certificate and hostname verification are handled by the SSLContext.
    """

    def _request_authentication(self):
        if not self.server_capabilities & CLIENT.SSL:
            raise pymysql.err.OperationalError(2026, 'TLS is required but the server does not advertise TLS')
        return super()._request_authentication()


def connect_required_tls(dialect, record, args, params):
    return TLSRequiredConnection(*args, **params)
