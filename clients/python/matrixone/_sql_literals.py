# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Literal rendering for legacy SQL APIs; independent of backslash SQL modes.

New execution APIs should use driver-bound parameters. Legacy builders return a
complete SQL string, so substitutions must respect SQL lexical boundaries.
"""

import json
import math
import re
from datetime import date, time, timedelta
from decimal import Decimal
from uuid import UUID
from typing import cast

import numpy as np
from sqlalchemy.dialects.mysql.base import MySQLIdentifierPreparer

_MAX_DECIMAL_LITERAL_CHARS = 4096

_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
# Native unquoted identifiers allow digits, dollar signs and BMP characters.
_WORD = re.compile(r'[A-Za-z0-9_$\u0080-\uffff]+')
_DOLLAR_TAG = re.compile(r'\$[A-Za-z0-9_\u0080-\uffff]*\$')
# Decimal points and signed exponents end numeric tokens before identifiers.
# Native integer/unsigned-exponent tokens can instead promote to identifiers.
_NUMBER = re.compile(r'(?:[0-9]+\.[0-9]*|\.[0-9]+)(?:[eE](?:[+-][0-9]*|[0-9]+))?|[0-9]+[eE][+-][0-9]*')


def _json_scalar(value):
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    raise TypeError('Unsupported JSON parameter type: %s' % type(value).__name__)


def connection_sql_mode(connection):
    # Only raw SQL containing a backslash needs mode discovery. Read from the
    # connection that will execute it; engine defaults can be stale after SET.
    if connection.dialect.name == 'sqlite':
        return 'NO_BACKSLASH_ESCAPES'
    closes_with_result = getattr(connection, 'should_close_with_result', False)
    if closes_with_result:
        connection.should_close_with_result = False
    try:
        result = connection.exec_driver_sql('SELECT @@SESSION.sql_mode')
        try:
            return result.scalar() or ''
        finally:
            result.close()
    finally:
        if closes_with_result:
            connection.should_close_with_result = True


_SIMPLE_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
# Reuse the keyword authority of the SDK's MySQL-derived dialect.
_RESERVED = MySQLIdentifierPreparer.reserved_words


def sql_identifier(name, qualified=False, *, force=False) -> str:
    if not isinstance(name, str):
        raise TypeError('SQL identifier must be a string')
    parts = name.split('.') if qualified else [name]
    quoted = []
    for part in parts:
        if not part or len(part) > 64 or '\x00' in part:
            raise ValueError('SQL identifiers require 1..64 characters and no NUL')
        if not force and _SIMPLE_IDENTIFIER.fullmatch(part) and part.casefold() not in _RESERVED:
            quoted.append(part)  # Preserve ordinary legacy SQL rendering.
        else:
            quoted.append('`' + part.replace('`', '``') + '`')
    return '.'.join(quoted)


def identifier_parts(name, qualified=False):
    """Normalize complete legacy quoted names, never trailing SQL fragments."""
    if not isinstance(name, str):
        raise TypeError('SQL identifier must be a string')
    if not name or len(name) > (261 if qualified else 130):
        raise ValueError('Invalid SQL object name')
    parts = []
    offset = 0
    while offset < len(name):
        if name[offset] in ('`', '"'):
            quote = name[offset]
            match = re.match(quote + r'((?:' + quote * 2 + '|[^' + quote + r'])*)' + quote, name[offset:])
            if match is None:
                raise ValueError('Unterminated quoted SQL identifier')
            part = match[1].replace(quote * 2, quote)
            offset += match.end()
            if offset < len(name) and name[offset] != '.':
                raise ValueError('Unexpected SQL after quoted identifier')
        else:
            end = name.find('.', offset) if qualified else -1
            if end < 0:
                end = len(name)
            part = name[offset:end]
            offset = end
        sql_identifier(part)  # Apply the same length/NUL contract to decoded parts.
        parts.append(part)
        if offset < len(name):
            if not qualified or len(parts) >= 2 or offset + 1 == len(name):
                raise ValueError('Expected an atomic name or database.table reference')
            offset += 1
    return tuple(parts)


def sql_object_name(name, qualified=False):
    """Render a manager name, including complete legacy backtick quoting."""
    return '.'.join(sql_identifier(part) for part in identifier_parts(name, qualified))


def sql_literal(value):
    if value is None:
        return 'NULL'
    if isinstance(value, (bool, np.bool_)):
        return '1' if value else '0'
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError('Non-finite SQL numeric parameters are unsupported')
        parts = value.as_tuple()
        exponent = cast(int, parts.exponent)
        digits = len(parts.digits)
        length = parts.sign + (digits + exponent if exponent >= 0 else max(digits + exponent, 1) + 1 - exponent)
        if length > _MAX_DECIMAL_LITERAL_CHARS:
            raise ValueError('Decimal SQL literal exceeds the 4096-character expansion budget')
        # Scientific notation is a floating-point token in MatrixOne SQL.
        # Preserve exact decimal comparison/storage without context rounding.
        return format(value, 'f')
    if isinstance(value, (int, float, np.integer, np.floating)):
        if isinstance(value, (float, np.floating)):
            finite = np.isfinite(value) if isinstance(value, np.floating) else math.isfinite(value)
            if not finite:
                raise ValueError('Non-finite SQL numeric parameters are unsupported')
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "X'%s'" % bytes(value).hex()
    if isinstance(value, timedelta):
        # Use signed microseconds: normalized negative days and fractions must
        # not become complemented fields, rounded floats, or wrapped hours.
        total_us = (value.days * 86400 + value.seconds) * 1_000_000 + value.microseconds
        sign = '-' if total_us < 0 else ''
        seconds, micros = divmod(abs(total_us), 1_000_000)
        hours, seconds = divmod(seconds, 3600)
        minutes, seconds = divmod(seconds, 60)
        value = f'{sign}{hours}:{minutes:02d}:{seconds:02d}'
        if micros:
            value += f'.{micros:06d}'
    elif isinstance(value, dict):
        value = json.dumps(value, default=_json_scalar, allow_nan=False)
    elif isinstance(value, (list, tuple)):
        value = json.dumps(value, separators=(',', ':'), default=_json_scalar, allow_nan=False)
    elif isinstance(value, (date, time, UUID)):
        value = str(value)
    if not isinstance(value, str):
        raise TypeError('Unsupported SQL parameter type: %s' % type(value).__name__)
    # A quoted backslash has different meanings across NO_BACKSLASH_ESCAPES.
    # Hex text also keeps NUL out of SQL/driver strings and works in both modes.
    if '\\' in value or '\x00' in value:
        return "CONVERT(X'%s' USING utf8mb4)" % value.encode('utf-8').hex()
    return "'%s'" % value.replace("'", "''")


def substitute_parameters(sql, params, *, sql_mode=''):
    if params is None:
        return sql
    named = isinstance(params, dict)
    if not named and not isinstance(params, (tuple, list)):
        raise TypeError('SQL parameters must be a mapping, tuple or list')
    values = iter(params) if not named else None
    used = set()
    modes = {mode.strip().upper() for mode in sql_mode.split(',')}
    output = []
    i = 0
    while i < len(sql):
        char = sql[i]
        if char == '$':
            opener = _DOLLAR_TAG.match(sql, i)
            if opener:
                delimiter = opener.group()
                end = sql.find('$', opener.end())
                # MatrixOne ends the body at the first dollar, then validates
                # its tag. Searching for a later matching delimiter differs.
                if end < 0 or not sql.startswith(delimiter, end):
                    raise ValueError('Unterminated or mismatched SQL dollar string')
                end += len(delimiter)
                output.append(sql[i:end])
                i = end
                continue
        if char in ("'", '"', '`'):
            start, quote = i, char
            i += 1
            while i < len(sql):
                identifier = quote == '`' or (quote == '"' and 'ANSI_QUOTES' in modes)
                if sql[i] == '\\' and not identifier and 'NO_BACKSLASH_ESCAPES' not in modes:
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
        if (
            char == '#'
            or (char == '/' and sql.startswith('//', i))
            or (char == '-' and sql.startswith('--', i) and (i + 2 == len(sql) or sql[i + 2].isspace()))
        ):
            end = sql.find('\n', i)
            end = len(sql) if end < 0 else end
            output.append(sql[i:end])
            i = end
            continue
        if char == '/' and sql.startswith('/*!', i):
            # MatrixOne executes MySQL version-comment bodies. Preserve the
            # delimiter/version bytes, but scan their SQL like the outer SQL.
            output.append('/*!')
            i += 3
            if i < len(sql) and '0' <= sql[i] <= '9':
                version = sql[i : i + 5]
                if len(version) != 5 or any(ch < '0' or ch > '9' for ch in version):
                    raise ValueError('Invalid SQL executable-comment version')
                output.append(version)
                i += 5
            continue
        if char == '/' and sql.startswith('/*', i):
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
        if not named and (char == '?' or (char == '%' and sql.startswith('%s', i))):
            try:
                value = next(values)
            except StopIteration as error:
                raise ValueError('Not enough SQL parameters') from error
            output.append(sql_literal(value))
            i += 1 if char == '?' else 2
            continue
        if char == '.' or '0' <= char <= '9':
            number = _NUMBER.match(sql, i)
            if number:
                output.append(number.group())
                i = number.end()
                continue
        # Consume identifiers as tokens, so name$tag$ never starts a string.
        word = _WORD.match(sql, i)
        if word:
            output.append(word.group())
            i = word.end()
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


def sql_string_token(value, *, context='LOAD') -> str:
    """Render a grammar STRING, rejecting SQL-mode-dependent escapes.

    LOAD's STRING positions cannot use CAST/hex or dollar-quoted expressions.
    Inline data containing backslashes must use a caller-owned input file;
    paths/options themselves must not contain backslashes or NUL bytes.
    """
    if not isinstance(value, str) or '\x00' in value or '\\' in value:
        advice = '; use a file for such inline data' if context == 'LOAD' else ''
        raise ValueError(context + ' string tokens require text without NUL or backslashes' + advice)
    return "'" + value.replace("'", "''") + "'"


def fulltext_term_token(value):
    """Render AGAINST's STRING grammar, independent of sql_mode.

    NUL and combined backslash/dollar input cannot be represented safely by
    the supported server lexer and are rejected before query execution.
    """
    if not isinstance(value, str):
        raise TypeError('Fulltext search term must be a string')
    if '\x00' in value or ('\\' in value and '$' in value):
        raise ValueError('Fulltext terms cannot contain NUL or both backslash and dollar')
    if '\\' in value:
        return '$sdk$' + value + '$sdk$'
    return sql_string_token(value)
