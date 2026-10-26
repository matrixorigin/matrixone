# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Complete-or-reject snapshot rewriting for a bounded SELECT grammar.

This is deliberately not a general SQL parser. Unsupported table-source forms
must use native SQL with explicit snapshot hints; no partial query is dispatched.
"""

import re
from typing import List, NamedTuple

from ._sql_literals import sql_string_token


class _Token(NamedTuple):
    text: str
    start: int
    end: int
    kind: str
    depth: int


_WORD = re.compile(r'[\w$]+', re.UNICODE)
_FORBIDDEN = frozenset(
    (
        'WITH UNION INTERSECT EXCEPT MINUS INTO OUTFILE DUMPFILE FOR LOCK '
        'PARTITION USE FORCE IGNORE LATERAL NATURAL UPDATE DELETE INSERT REPLACE TABLE VALUES'
    ).split()
)
_BOUNDARIES = frozenset('JOIN INNER LEFT RIGHT FULL CROSS OUTER ON USING WHERE GROUP HAVING ORDER LIMIT OFFSET'.split())


def _tokens(sql):
    if '\\' in sql or '$' in sql:
        raise ValueError('Snapshot SQL cannot contain mode-ambiguous backslashes or dollar tokens')
    i, depth = 0, 0
    while i < len(sql):
        char = sql[i]
        if char.isspace():
            i += 1
            continue
        if sql.startswith('/*!', i):
            raise ValueError('Executable comments are unsupported in snapshot SQL')
        if sql.startswith('/*', i):
            end = sql.find('*/', i + 2)
            if end < 0:
                raise ValueError('Unterminated SQL comment')
            i = end + 2
            continue
        if (
            char == '#'
            or sql.startswith('//', i)
            or (sql.startswith('--', i) and (i + 2 == len(sql) or sql[i + 2].isspace()))
        ):
            end = sql.find('\n', i)
            i = end if end >= 0 else len(sql)
            continue
        start = i
        if char in "'\"`":
            i += 1
            while i < len(sql):
                if sql[i] == char:
                    i += 1
                    if i < len(sql) and sql[i] == char:
                        i += 1
                        continue
                    break
                i += 1
            else:
                raise ValueError('Unterminated SQL quote')
            yield _Token(sql[start:i], start, i, 'identifier' if char == '`' else 'literal', depth)
            continue
        match = _WORD.match(sql, i)
        if match:
            i = match.end()
            yield _Token(match.group(), start, i, 'word', depth)
            continue
        if char == ')':
            depth -= 1
            if depth < 0:
                raise ValueError('Unbalanced SQL parentheses')
        yield _Token(char, start, start + 1, 'symbol', depth)
        if char == '(':
            depth += 1
            if depth > 128:
                raise ValueError('Snapshot SQL nesting exceeds 128')
        i += 1
    if depth:
        raise ValueError('Unbalanced SQL parentheses')


def rewrite_snapshot(sql, snapshot):
    if not isinstance(sql, str) or not isinstance(snapshot, str) or not snapshot:
        raise ValueError('Snapshot queries require SQL text and a nonempty snapshot name')
    tokens = list(_tokens(sql))
    if not tokens or tokens[0].text.upper() != 'SELECT' or tokens[0].kind != 'word':
        raise ValueError('Snapshot wrapper accepts one SELECT statement')
    inserts = []
    source_region = False
    for index, token in enumerate(tokens):
        word = token.text.upper() if token.kind == 'word' else ''
        if word in _FORBIDDEN or (word == 'SELECT' and index):
            raise ValueError('Unsupported snapshot query construct: ' + word)
        if token.text in ('{', '}') and token.kind == 'symbol':
            raise ValueError('Existing time-travel hints are unsupported in snapshot wrapper')
        if token.text == ';' and (index != len(tokens) - 1 or token.depth):
            raise ValueError('Snapshot wrapper accepts one statement')
        if token.depth:
            continue
        if word in ('WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET'):
            source_region = False
        if source_region and token.text == ',' and token.kind == 'symbol':
            raise ValueError('Comma table sources are unsupported in snapshot wrapper')
        if word not in ('FROM', 'JOIN'):
            continue
        source_region = True
        j = index + 1
        if j >= len(tokens) or tokens[j].kind not in ('word', 'identifier'):
            raise ValueError('Snapshot requires a simple table reference')
        if tokens[j].kind == 'word' and tokens[j].text.upper() in _BOUNDARIES | _FORBIDDEN:
            raise ValueError('Invalid snapshot table reference')
        end = tokens[j].end
        j += 1
        if j < len(tokens) and tokens[j].text == '.':
            j += 1
            if j >= len(tokens) or tokens[j].kind not in ('word', 'identifier'):
                raise ValueError('Invalid qualified table reference')
            end = tokens[j].end
            j += 1
        # Consume at most one alias, then require a classified SQL boundary.
        if j < len(tokens) and tokens[j].kind == 'word' and tokens[j].text.upper() == 'AS':
            j += 1
            if j >= len(tokens) or tokens[j].kind not in ('word', 'identifier'):
                raise ValueError('Invalid table alias')
            j += 1
        elif j < len(tokens) and tokens[j].kind in ('word', 'identifier'):
            if tokens[j].text.upper() not in _BOUNDARIES | _FORBIDDEN:
                j += 1
        if j < len(tokens):
            suffix = tokens[j]
            if not (suffix.kind == 'word' and suffix.text.upper() in _BOUNDARIES) and suffix.text != ';':
                raise ValueError('Unsupported snapshot table-source suffix')
        inserts.append(end)
    hint = '{snapshot = ' + sql_string_token(snapshot, context='Snapshot') + '}'
    parts: List[str] = []
    previous = 0
    for end in inserts:
        parts.extend((sql[previous:end], hint))
        previous = end
    parts.append(sql[previous:])
    return ''.join(parts)
