"""Public complete-SQL builders must use the same lexical boundary as execution."""

import sqlite3
from datetime import date

import numpy as np
import pytest
from matrixone.sql_builder import (
    MatrixOneSQLBuilder,
    build_select_query,
    build_insert_query,
    build_update_query,
    build_delete_query,
    build_vector_similarity_query,
)


@pytest.fixture
def database():
    connection = sqlite3.connect(':memory:')
    try:
        connection.execute('CREATE TABLE docs(id INTEGER PRIMARY KEY, day TEXT, payload BLOB, note TEXT)')
        connection.execute('INSERT INTO docs VALUES(?,?,?,?)', (1, '2024-01-02', b'abc', "O'Reilly ? :id2"))
        yield connection
    finally:
        connection.close()


@pytest.mark.parametrize(
    'columns,condition,params',
    [
        (["'?' AS marker", 'id'], 'id=?', [1]),
        (['id'], 'day=?', [date(2024, 1, 2)]),
        (['id'], 'payload=?', [b'abc']),
        (["'? :id' AS marker", 'id /* ? ignored */'], 'note=? -- ? ignored\n', ["O'Reilly ? :id2"]),
    ],
)
def test_builder_alias_and_select_helper_match_bound_values(database, columns, condition, params):
    builder = MatrixOneSQLBuilder().select(*columns).from_table('docs').where(condition, *params)
    parameterized = builder.build()
    expected = database.execute(*parameterized).fetchall()
    assert expected
    for sql in (
        builder.build_with_parameter_substitution(),
        str(builder),
        build_select_query('docs', columns, [condition], params),
    ):
        assert database.execute(sql).fetchall() == expected
    assert builder.build() == parameterized
    assert build_select_query('docs', columns, [condition], params, use_parameter_substitution=False) == parameterized


@pytest.mark.parametrize('params', [[], [1], [1, 2, 3]])
def test_builder_rejects_wrong_parameter_count_before_execution(params):
    builder = MatrixOneSQLBuilder().select('*').from_table('docs').where('id=? AND id=?', *params)
    for render in (
        builder.build_with_parameter_substitution,
        lambda: str(builder),
        lambda: build_select_query('docs', where_conditions=['id=? AND id=?'], where_params=params),
    ):
        with pytest.raises(ValueError, match='SQL parameters'):
            render()


def test_builder_keeps_native_comments_and_dollar_literals_out_of_bind_namespace():
    sql = (
        MatrixOneSQLBuilder()
        .select("$tag$ ? ' :id $tag$ AS marker", 'id /* ? */')
        .from_table('docs')
        .where('id=? // ? ignored\n', 7)
        .build_with_parameter_substitution()
    )
    assert "$tag$ ? ' :id $tag$ AS marker" in sql
    assert 'id /* ? */' in sql and 'id=7 // ? ignored\n' in sql


def test_insert_update_delete_helpers_preserve_values_and_scope(database):
    value = "O'Reilly ? :id2"
    database.execute(
        build_insert_query(
            'docs',
            {'id': 2, 'note': value, 'payload': b'xyz', 'day': date(2024, 1, 2)},
            use_parameter_substitution=True,
        )
    )
    assert database.execute('SELECT note,payload,day FROM docs WHERE id=2').fetchone() == (value, b'xyz', '2024-01-02')
    database.execute(
        build_update_query(
            'docs', {'note': 'changed ?'}, ['note=? AND id=?'], [value, 2], use_parameter_substitution=True
        )
    )
    assert database.execute('SELECT id,note FROM docs ORDER BY id').fetchall() == [(1, value), (2, 'changed ?')]
    database.execute(build_delete_query('docs', ['note=? AND id=?'], ['changed ?', 2], use_parameter_substitution=True))
    assert database.execute('SELECT id FROM docs').fetchall() == [(1,)]


@pytest.mark.parametrize('substitute', [False, True])
@pytest.mark.parametrize('condition_kind', ['none', 'empty_list', 'empty_iterator'])
@pytest.mark.parametrize('operation', ['select', 'update', 'delete', 'vector'])
def test_helper_never_drops_filter_values_without_conditions(database, substitute, condition_kind, operation):
    conditions = {'none': None, 'empty_list': [], 'empty_iterator': iter(())}[condition_kind]
    args = dict(where_conditions=conditions, where_params=[1], use_parameter_substitution=substitute)
    with pytest.raises(ValueError, match='where_params requires where_conditions'):
        if operation == 'select':
            sql = build_select_query('docs', **args)
        elif operation == 'update':
            sql = build_update_query('docs', {'note': 'wrong'}, **args)
        elif operation == 'delete':
            sql = build_delete_query('docs', **args)
        else:
            sql = build_vector_similarity_query('docs', 'v', [1, 2], **args)
        database.execute(sql)  # Unreachable: a builder rejection precedes any mutation.
    assert database.execute('SELECT id,note FROM docs').fetchall() == [(1, "O'Reilly ? :id2")]


@pytest.mark.parametrize('vector', [[1, 2], [np.float32(1.5), np.int64(2)], ["1]') AS injected FROM docs -- "]])
def test_vector_value_cannot_escape_its_literal(database, vector):
    captured = []
    database.create_function('l2_distance_sq', 2, lambda column, value: captured.append(value) or 0)
    sql = (
        MatrixOneSQLBuilder()
        .select('id')
        .add_vector_distance('id', vector)
        .from_table('docs')
        .build_with_parameter_substitution()
    )
    cursor = database.execute(sql)
    assert cursor.fetchall() == [(1, 0)]
    assert [column[0] for column in cursor.description] == ['id', 'distance']
    assert captured == ['[' + ','.join(map(str, vector)) + ']']


@pytest.mark.parametrize('column', ['s', 'scol'])
@pytest.mark.parametrize('bound', [False, True])
def test_builder_preserves_modulo_while_shared_default_keeps_format_binds(database, column, bound):
    from matrixone._sql_literals import substitute_parameters

    builder = (
        MatrixOneSQLBuilder().select('7%' + column + ' AS remainder').from_table('(SELECT 3 AS s,3 AS scol) AS docs')
    )
    if bound:
        builder.where('1=?', 1)
    sql, params = builder.build()
    expected = database.execute(sql, params).fetchall()
    assert expected == [(1,)]
    assert database.execute(builder.build_with_parameter_substitution()).fetchall() == expected
    assert database.execute(substitute_parameters(sql, params, qmark_only=True)).fetchall() == expected
    assert substitute_parameters('SELECT %s', [7]) == 'SELECT 7'


@pytest.mark.parametrize('substitute', [False, True])
@pytest.mark.parametrize('operation', ['select', 'update', 'delete', 'vector'])
def test_helper_consumes_condition_and_parameter_iterators_once(database, substitute, operation):
    database.execute("INSERT INTO docs(id,note) VALUES(2,'untouched')")
    kwargs = dict(where_conditions=iter(['id=?']), where_params=iter([1]), use_parameter_substitution=substitute)
    if operation == 'select':
        sql = build_select_query('docs', select_columns=['id'], **kwargs)
    elif operation == 'update':
        sql = build_update_query('docs', {'note': 'updated'}, **kwargs)
    elif operation == 'delete':
        sql = build_delete_query('docs', **kwargs)
    else:
        database.create_function('l2_distance_sq', 2, lambda a, b: 0)
        sql = build_vector_similarity_query('docs', 'id', [1], **kwargs)
    result = database.execute(sql) if substitute else database.execute(*sql)
    if operation in ('select', 'vector'):
        assert [row[0] for row in result.fetchall()] == [1]
    elif operation == 'update':
        assert database.execute('SELECT note FROM docs WHERE id=1').fetchone() == ('updated',)
    else:
        assert database.execute('SELECT id FROM docs WHERE id=1').fetchall() == []
    assert database.execute('SELECT note FROM docs WHERE id=2').fetchone() == ('untouched',)
