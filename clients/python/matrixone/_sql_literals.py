# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Literal rendering for legacy SQL APIs; independent of backslash SQL modes.

New execution APIs should use driver-bound parameters. Legacy builders return a
complete SQL string, so substitutions must respect SQL lexical boundaries.
"""

import json
import math
import re
from datetime import date, datetime, time
from decimal import Decimal

import numpy as np

_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')


_SIMPLE_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_RESERVED = {
    'select',
    'from',
    'where',
    'group',
    'order',
    'by',
    'table',
    'index',
    'key',
    'primary',
    'insert',
    'update',
    'delete',
    'join',
    'limit',
    'values',
    'into',
    'null',
    'default',
}


def sql_identifier(name, qualified=False):
    if not isinstance(name, str):
        raise TypeError('SQL identifier must be a string')
    parts = name.split('.') if qualified else [name]
    quoted = []
    for part in parts:
        if not part or len(part) > 64 or '\x00' in part:
            raise ValueError('SQL identifiers require 1..64 characters and no NUL')
        if _SIMPLE_IDENTIFIER.fullmatch(part) and part.casefold() not in _RESERVED:
            quoted.append(part)  # Preserve ordinary legacy SQL rendering.
        else:
            quoted.append('`' + part.replace('`', '``') + '`')
    return '.'.join(quoted)


def sql_literal(value):
    if value is None:
        return 'NULL'
    if isinstance(value, (bool, np.bool_)):
        return '1' if value else '0'
    if isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        if isinstance(value, (float, Decimal, np.floating)):
            finite = np.isfinite(value) if isinstance(value, np.floating) else math.isfinite(value)
            if not finite:
                raise ValueError('Non-finite SQL numeric parameters are unsupported')
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "X'%s'" % bytes(value).hex()
    if isinstance(value, dict):
        value = json.dumps(value)
    elif isinstance(value, (list, tuple)):
        value = json.dumps(value, separators=(',', ':'))
    elif isinstance(value, (date, datetime, time)):
        value = str(value)
    if not isinstance(value, str):
        raise TypeError('Unsupported SQL parameter type: %s' % type(value).__name__)
    # A quoted backslash has different meanings across NO_BACKSLASH_ESCAPES.
    # Hex text also keeps NUL out of SQL/driver strings and works in both modes.
    if '\\' in value or '\x00' in value:
        return "CONVERT(X'%s' USING utf8mb4)" % value.encode('utf-8').hex()
    return "'%s'" % value.replace("'", "''")


def substitute_parameters(sql, params):
    if params is None:
        return sql
    named = isinstance(params, dict)
    if not named and not isinstance(params, (tuple, list)):
        raise TypeError('SQL parameters must be a mapping, tuple or list')
    values = iter(params) if not named else None
    used = set()
    output = []
    i = 0
    while i < len(sql):
        char = sql[i]
        if char in ("'", '"', '`'):
            start, quote = i, char
            i += 1
            while i < len(sql):
                if sql[i] == '\\':
                    i += 2
                elif sql[i] == quote:
                    i += 1
                    if i < len(sql) and sql[i] == quote:
                        i += 1
                    else:
                        break
                else:
                    i += 1
            output.append(sql[start:i])
            continue
        if char == '#' or (sql.startswith('--', i) and (i + 2 == len(sql) or sql[i + 2].isspace())):
            end = sql.find('\n', i)
            end = len(sql) if end < 0 else end
            output.append(sql[i:end])
            i = end
            continue
        if sql.startswith('/*', i):
            end = sql.find('*/', i + 2)
            end = len(sql) if end < 0 else end + 2
            output.append(sql[i:end])
            i = end
            continue
        if named and char == ':' and (i == 0 or sql[i - 1] != ':'):
            match = _NAME.match(sql, i + 1)
            if match:
                key = match.group()
                if key not in params:
                    raise ValueError('Missing named SQL parameter: %s' % key)
                output.append(sql_literal(params[key]))
                used.add(key)
                i = match.end()
                continue
        if not named and (char == '?' or sql.startswith('%s', i)):
            try:
                value = next(values)
            except StopIteration as error:
                raise ValueError('Not enough SQL parameters') from error
            output.append(sql_literal(value))
            i += 1 if char == '?' else 2
            continue
        output.append(char)
        i += 1
    if named:
        if set(params) != used:
            raise ValueError('Unused named SQL parameters: %s' % sorted(set(params) - used))
    else:
        try:
            next(values)
        except StopIteration:
            pass
        else:
            raise ValueError('Too many SQL parameters')
    return ''.join(output)
