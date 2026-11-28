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


# Execution option for checkouts that must run statements in server autocommit
# (standalone DDL, MERGE/PICK): no server BEGIN is sent for them.
STATEMENT_AUTOCOMMIT = 'matrixone_statement_autocommit'


def install_server_transaction_begin(engine, autocommit):
    """Give each logical SQLAlchemy transaction a real server transaction.

    With physical DBAPI autocommit on, SQLAlchemy's logical begin sends nothing
    to the server, so every statement commits immediately and a later rollback
    (``engine.begin()`` failure, ``client.session()`` error after a manual
    commit) cannot undo it. Issue ``BEGIN`` on each logical begin instead,
    keeping the selected physical mode for statements run outside a
    transaction. Connections explicitly set to ``AUTOCOMMIT`` isolation keep
    statement autocommit, as do checkouts marked with ``STATEMENT_AUTOCOMMIT``.
    With physical autocommit off nothing is installed.
    """
    if not autocommit:
        return
    from sqlalchemy import event

    target = getattr(engine, 'sync_engine', engine)

    def begin(connection):
        options = connection.get_execution_options()
        if options.get('isolation_level') == 'AUTOCOMMIT' or options.get(STATEMENT_AUTOCOMMIT):
            return
        connection.exec_driver_sql('BEGIN')

    event.listen(target, 'begin', begin)
    target._matrixone_server_begin = True


def server_begin_installed(engine):
    return getattr(getattr(engine, 'sync_engine', engine), '_matrixone_server_begin', False)
