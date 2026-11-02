# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""JSON string results retain string operator semantics in SQLAlchemy."""

import json

import pytest
from sqlalchemy import bindparam, create_engine, select

from matrixone.sqlalchemy_ext import JSON
from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect
from matrixone.sqlalchemy_ext.json_functions import json_extract_string, json_unquote


@pytest.mark.parametrize('kind', ['extract', 'astext', 'unquote'])
@pytest.mark.parametrize(
    'left, right, expected',
    [('Ada', 'Lovelace', 'AdaLovelace'), ('12', '34', '1234'), ('你', '好', '你好'), (None, 'suffix', None)],
)
def test_json_strings_concatenate(kind, left, right, expected):
    if kind == 'unquote':
        first = json_unquote(json.dumps(left) if left is not None else None)
        second = json_unquote(json.dumps(right))
    else:
        source = bindparam('document', {'left': left, 'right': right}, type_=JSON)
        if kind == 'astext':
            first, second = source['left'].astext, source['right'].astext
        else:
            first = json_extract_string(source, '$.left')
            second = json_extract_string(source, '$.right')
    statement = select(first + second)

    # SQLite callbacks model only these functions' string/NULL return values;
    # SQLAlchemy still chooses and executes the concatenation operator itself.
    def extract(document, path):
        key = json.loads(path[2:]) if path[2:].startswith('"') else path[2:]
        value = json.loads(document).get(key)
        return value if isinstance(value, str) else None

    engine = create_engine('sqlite:///:memory:')
    try:
        with engine.connect() as connection:
            connection.connection.create_function('json_extract_string', 2, extract)
            connection.connection.create_function(
                'json_unquote', 1, lambda value: json.loads(value) if value is not None else None
            )
            assert connection.execute(statement).scalar_one() == expected
    finally:
        engine.dispose()
    assert 'concat(' in str(statement.compile(dialect=MatrixOneDialect())).lower()
