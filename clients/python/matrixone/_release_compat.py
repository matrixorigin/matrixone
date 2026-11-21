# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Equivalent release SQL and actionable limitations on the actual checkout.

Only qualified release-family banners select these profiles. Other source/custom/new
releases require their own qualification; test environment/version overrides
cannot authorize it. See docs/design/released-server-compatibility.md.
"""

import re

from sqlalchemy import event

from ._local_infile import _physical_driver
from ._sql_literals import _DOLLAR_TAG, substitute_parameters
from ._utils import _read_word, _skip_space_and_comments
from .exceptions import QueryError

_BANNER = '8.0.30-MatrixOne-v4.2.8'
_RELEASE_PROFILES = {
    _BANNER: '4.2.8',
    '8.0.30-MatrixOne-v4.2.8-hotfix-20261114': '4.2.8-hotfix-20261114',
    # Release-branch source builds retain the 4.2.8 SQL contract. Qualification
    # pins their actual git revision separately; they are not published images.
    '8.0.30-MatrixOne-v4.2.8-hotfix': '4.2.8-hotfix',
}
_LIMITATIONS = {
    'fulltext_identifiers': (
        'The released server cannot safely index these FULLTEXT identifiers',
        'Use ordinary nonreserved identifiers, or upgrade to a release containing the quoted-identifier fix (#32723).',
    ),
    'async_fulltext_parser': (
        'The released server does not preserve the requested ASYNC FULLTEXT parser',
        'Explicitly choose synchronous indexing if acceptable, or upgrade to a release containing #32723.',
    ),
    'jieba_dictionary': (
        'The server installation is missing the dictionaries required by gojieba',
        'Install the matching server release dictionaries in the sibling dict directory '
        '(or set MO_JIEBA_DICT_DIR to it), then restart affected CNs. Do not change the requested parser.',
    ),
    'stage_literals': (
        'The released server cannot preserve apostrophes in Stage names or catalog values',
        'Percent-encode URI path characters where applicable; quoted credentials/comments require a fixed release (#32723).',
    ),
    'stage_create_disabled': (
        'The released server ignores ENABLE=FALSE during CREATE STAGE',
        'Use ALTER STAGE after creation only if an initially enabled Stage is acceptable, or upgrade to a fixed release (#32723).',
    ),
    'vecf4': ('The released server does not implement VECF4', 'Choose a supported vector precision explicitly.'),
    'vecf8': ('The released server does not implement VECF8', 'Choose a supported vector precision explicitly.'),
    'branch_database_identity': (
        'The released server could not establish the database branch identity',
        'Empty branches require a release containing #32723. Verify the target; the SDK never substitutes DROP DATABASE.',
    ),
    'schema_serialization': (
        'The released server cannot serialize this schema for SHOW CREATE/reflection replay',
        'Use SDK-adopted reflection for ordinary types; these ENUM/SET definitions require a fixed release (#32723).',
    ),
    'view_enum_set_provenance': (
        'The released view metadata does not retain a usable ENUM/SET domain',
        'Declare columns from the original source model explicitly, or recreate the view on a fixed release (#32723).',
    ),
    'parquet_float32_infinity': (
        'The released Parquet reader rejects FLOAT32 infinity',
        'Explicitly cast input/schema to FLOAT64 if acceptable, or upgrade to a fixed release (#32723). No type is widened automatically.',
    ),
}


def release_for_banner(banner):
    return _RELEASE_PROFILES.get(banner)


def _release_version(connection):
    # Never reconnect from handle_error: a primary transport error stays primary.
    if connection is None or connection.closed or connection.invalidated:
        return None
    try:
        driver = _physical_driver(connection.connection)
        return release_for_banner(getattr(driver, 'server_version', None))
    except Exception:
        return None


def _qualified_release(connection):
    return _release_version(connection) is not None


def _error(feature, *, original=None, errno=None, metadata=False, version='4.2.8'):
    problem, alternative = _LIMITATIONS[feature]
    error = QueryError(
        'MatrixOne v' + version + ' compatibility: ' + problem + '. ' + alternative,
        orig=original,
        errno=errno,
        phase='metadata_reflection' if metadata else 'compatibility' if original is None else 'execute',
        committed=False if original is None else None,
        statement_acknowledged=False if original is None and not metadata else None,
    )
    error.compatibility_feature = feature
    error.compatibility_hint = alternative
    error.backend_version = version
    return error


def _tokens(sql, mode=''):
    """Only DDL inspection: retain identifiers and decoded literal boundaries."""
    modes = {part.strip().upper() for part in mode.split(',')}
    out, pos = [], 0
    while pos < len(sql):
        pos = _skip_space_and_comments(sql, pos)
        if pos == len(sql):
            break
        char = sql[pos]
        if char in ("'", '"', '`'):
            identifier = char == '`' or (char == '"' and 'ANSI_QUOTES' in modes)
            value, pos = [], pos + 1
            while pos < len(sql):
                current = sql[pos]
                pos += 1
                if current == '\\' and not identifier and 'NO_BACKSLASH_ESCAPES' not in modes and pos < len(sql):
                    value.append(sql[pos])
                    pos += 1
                elif current == char:
                    if pos < len(sql) and sql[pos] == char:
                        value.append(char)
                        pos += 1
                    else:
                        break
                else:
                    value.append(current)
            else:
                return None
            out.append(('identifier' if identifier else 'literal', ''.join(value)))
            continue
        if char == '$':
            tag = _DOLLAR_TAG.match(sql, pos)
            if tag:
                end = sql.find('$', tag.end())
                if end < 0 or not sql.startswith(tag.group(), end):
                    return None
                out.append(('literal', sql[tag.end() : end]))
                pos = end + len(tag.group())
                continue
        word, end = _read_word(sql, pos)
        if end > pos:
            out.append(('word', word))
            pos = end
        else:
            out.append(('symbol', char))
            pos += 1
    return out


def _unsupported_ddl(tokens):
    if not tokens:
        return None
    words = [value.upper() if kind == 'word' else '' for kind, value in tokens]
    if words[:2] in (['CREATE', 'STAGE'], ['ALTER', 'STAGE']):
        if any("'" in value for kind, value in tokens if kind in ('identifier', 'literal')):
            return 'stage_literals'
        if words[0] == 'CREATE' and any(
            words[i] == 'ENABLE' and tokens[i + 1] == ('symbol', '=') and words[i + 2] in ('FALSE', '0')
            for i in range(len(tokens) - 2)
        ):
            return 'stage_create_disabled'
    if 'FULLTEXT' in words:
        # Compute spans once, including malformed DDL: repeated tokens or nested
        # parentheses cannot cause repeated scans of the remaining statement.
        size = len(tokens)
        next_open, fields = [size] * (size + 1), []
        closing, stack, field = {}, [], 0
        async_options, gojieba_options = {}, {}
        involved, indexed = [0] * (size + 1), [0] * (size + 1)
        for index, token in enumerate(tokens):
            fields.append(field)
            # Nonreserved ASYNC can name a column, index or parser. Options
            # must follow the column list and belong to this index's field.
            if words[index] == 'ASYNC' and words[max(0, index - 2) : index] != ['WITH', 'PARSER']:
                async_options[field] = index
            if (
                words[index : index + 2] == ['WITH', 'PARSER']
                and index + 2 < size
                and tokens[index + 2][1].upper() == 'GOJIEBA'
            ):
                gojieba_options[field] = index
            if token == ('symbol', '('):
                stack.append((index, field))
                field = index + 1
            elif token == ('symbol', ')') and stack:
                opening, field = stack.pop()
                closing[opening] = index
            elif token == ('symbol', ','):
                field = index + 1
        for index in range(size - 1, -1, -1):
            next_open[index] = index if tokens[index] == ('symbol', '(') else next_open[index + 1]
        async_parser = False
        for index, word in enumerate(words):
            if word != 'FULLTEXT':
                continue
            start = next_open[index + 1]
            if start < size:
                end = closing.get(start, size)
                for mask in (involved, indexed):
                    mask[start + 1] += 1
                    mask[end] -= 1
                field = fields[index]
                if async_options.get(field, -1) > end and gojieba_options.get(field, -1) > end:
                    async_parser = True
        # Unrelated quoted columns remain supported. Inspect only the indexed
        # columns, table and primary key used by the server's generated SQL.
        for index, word in enumerate(words):
            if word in ('TABLE', 'ON'):
                involved[index + 1] += 1
                involved[next_open[index + 1]] -= 1
                break
        for index in range(len(tokens) - 2):
            if words[index : index + 2] != ['PRIMARY', 'KEY']:
                continue
            if tokens[index + 2] == ('symbol', '('):
                involved[index + 3] += 1
                involved[closing.get(index + 2, size)] -= 1
            else:
                involved[fields[index]] += 1
                involved[fields[index] + 1] -= 1
        used, column = 0, 0
        for index, (kind, value) in enumerate(tokens):
            used += involved[index]
            column += indexed[index]
            if kind in ('word', 'identifier') and (
                (used and '`' in value) or (column and value.lower() in ('order', 'fulltext1'))
            ):
                return 'fulltext_identifiers'
        if async_parser:
            return 'async_fulltext_parser'
    for index, word in enumerate(words[:-1]):
        if word in ('VECF4', 'VECF8') and tokens[index + 1] == ('symbol', '('):
            return word.lower()
    return None


def _before_cursor_execute(connection, cursor, statement, parameters, context, executemany):
    if not isinstance(statement, str):
        return statement, parameters
    version = _release_version(connection)
    if version is None:
        return statement, parameters
    pos = _skip_space_and_comments(statement, 0)
    head, _ = _read_word(statement, pos)
    ddl = head.upper() in ('CREATE', 'ALTER') and any(
        keyword in statement.upper() for keyword in ('STAGE', 'FULLTEXT', 'VECF4', 'VECF8')
    )
    options = getattr(context, 'execution_options', {})
    numeric_alias = (
        not options.get('_matrixone_preserve_driver_sql', False)
        and '$' in statement
        and ('.' in statement or 'e+' in statement.lower() or 'e-' in statement.lower())
    )
    if not ddl and not numeric_alias:
        return statement, parameters
    # Even SHOW VARIABLES starts a transaction on this release. Inspect at most
    # four quote interpretations and act only on agreement, without issuing SQL.
    modes = ['', 'NO_BACKSLASH_ESCAPES'] if '\\' in statement else ['']
    if '"' in statement:
        modes += [mode + ',ANSI_QUOTES' for mode in modes]
    if ddl:
        decisions = []
        for mode in modes:
            tokens = _tokens(statement, mode)
            feature = _unsupported_ddl(tokens)
            if (
                not feature
                and tokens
                and [value.upper() if kind == 'word' else '' for kind, value in tokens[:2]]
                in (['CREATE', 'STAGE'], ['ALTER', 'STAGE'])
            ):
                values = parameters.values() if isinstance(parameters, dict) else parameters or ()
                if any(isinstance(value, str) and "'" in value for value in values):
                    feature = 'stage_literals'
            decisions.append(feature)
        feature = decisions[0] if all(value == decisions[0] for value in decisions) else None
        if feature:
            raise _error(feature, version=version) from None
    if numeric_alias:
        normalized = []
        for mode in modes:
            try:
                normalized.append(substitute_parameters(statement, None, sql_mode=mode, _portable_numeric_aliases=True))
            except ValueError:
                normalized.append(statement)
        if all(value == normalized[0] for value in normalized):
            statement = normalized[0]
    return statement, parameters


def _handle_error(context):
    return classify_release_error(
        context.connection, context.statement, context.original_exception, context.sqlalchemy_exception
    )


def classify_release_error(connection, statement, original_exception, sqlalchemy_exception):
    version = _release_version(connection)
    if version is None:
        return None
    args = getattr(original_exception, 'args', ())
    if len(args) < 2 or not isinstance(args[1], str):
        return None
    code, message = args[:2]
    if not isinstance(statement, str):
        return None
    prefix = statement.lstrip().upper()
    feature = None
    if (
        code == 20101
        and prefix.startswith('DATA BRANCH DELETE DATABASE ')
        and re.fullmatch(r'internal error: DATA BRANCH DELETE target .+ is not an active branch database', message)
    ):
        feature = 'branch_database_identity'
    elif (
        code == 1105
        and prefix.startswith('SHOW CREATE TABLE ')
        and message.startswith('You have an error in your SQL syntax;')
        and 'syntax error' in message.lower()
    ):
        feature = 'schema_serialization'
    elif code == 20101 and message.startswith('internal error: jieba dictionary not available: '):
        feature = 'jieba_dictionary'
    elif code == 20301 and re.fullmatch(r'invalid input: parquet value [+-]?Inf overflows FLOAT', message):
        feature = 'parquet_float32_infinity'
    if feature:
        return _error(feature, original=sqlalchemy_exception or original_exception, errno=code, version=version)
    return None


def normalize_described_columns(connection, rows):
    # Native DATE(0) is a display width, not a Date constructor argument.
    # Change metadata only; the stored type and values remain DATE.
    version = _release_version(connection)
    if version is None:
        return rows
    normalized = []
    for row in rows:
        spelling = row[1]
        if re.fullmatch(r'(?:ENUM|SET)\([0-9]+\)', spelling, re.IGNORECASE):
            raise _error('view_enum_set_provenance', metadata=True, version=version) from None
        # MySQL's parser recognizes lowercase type modifiers such as unsigned.
        # Quoted domains must retain their original member bytes and case.
        if not any(quote in spelling for quote in ("'", '"', '`')):
            spelling = spelling.lower()
            # Released DESCRIBE places integer display width after UNSIGNED;
            # MySQL reflection expects the width before that modifier.
            spelling = re.sub(
                r'^(tinyint|smallint|mediumint|int|integer|bigint) unsigned(\([0-9]+\))$',
                r'\1\2 unsigned',
                spelling,
            )
        if spelling == 'date(0)':
            spelling = 'date'
        normalized.append(tuple(row[:1]) + (spelling,) + tuple(row[2:]))
    return normalized


def restore_set_members(connection, columns, table_name, schema):
    """Recover release SHOW CREATE case loss from the same tenant's catalog."""
    from sqlalchemy import text
    from sqlalchemy.dialects.mysql import SET

    selected = [column for column in columns if isinstance(column['type'], SET)]
    if not selected or not _qualified_release(connection):
        return columns
    result = connection.execute(
        text(
            'SELECT attname,attr_enum FROM mo_catalog.mo_columns '
            'WHERE account_id=current_account_id() '
            'AND BINARY att_database=BINARY (CASE WHEN @@lower_case_table_names=0 '
            'THEN COALESCE(:schema,DATABASE()) ELSE LOWER(COALESCE(:schema,DATABASE())) END) '
            'AND BINARY att_relname=BINARY (CASE WHEN @@lower_case_table_names=0 '
            'THEN :table ELSE LOWER(:table) END)'
        ),
        {'schema': schema, 'table': table_name},
    )
    try:
        domains = {}
        for name, domain in result.all():
            domains[name] = None if name in domains else domain
    except BaseException as primary:
        try:
            result.close()
        except Exception as cleanup_error:
            primary.metadata_cleanup_error = cleanup_error
        raise
    else:
        result.close()
    for column in selected:
        domain = domains.get(column['name'])
        if not isinstance(domain, str):
            raise QueryError(
                'SET reflection could not read the native column domain', phase='metadata_reflection'
            ) from None
        original = column['type']
        options = {
            key: getattr(original, key) for key in ('charset', 'collation', 'ascii', 'unicode', 'binary', 'national')
        }
        column['type'] = SET(*domain.split(','), retrieve_as_bitwise=original.retrieve_as_bitwise, **options)
    return columns


def install_release_compatibility(engine):
    if not event.contains(engine, 'before_cursor_execute', _before_cursor_execute):
        event.listen(engine, 'before_cursor_execute', _before_cursor_execute, retval=True)
    if not event.contains(engine, 'handle_error', _handle_error):
        event.listen(engine, 'handle_error', _handle_error, retval=True)
