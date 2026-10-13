# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Explicit connection security and timeout options shared by SDK clients."""

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
    return message.replace(password, '***') if password else message


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
