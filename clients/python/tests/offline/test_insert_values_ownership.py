# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""INSERT builders own their column and row containers."""

from contextlib import closing
import sqlite3
from types import MappingProxyType

import pytest

from matrixone.sql_builder import MatrixOneSQLBuilder


def execute_rows(builder):
    with closing(sqlite3.connect(':memory:')) as connection:
        connection.execute('CREATE TABLE items (id INTEGER, label TEXT)')
        sql, params = builder.build()
        connection.execute(sql, params)
        return connection.execute('SELECT id, label FROM items ORDER BY id').fetchall()


def test_reusing_input_rows_keeps_builders_independent():
    columns = ['id', 'label']
    rows = [[1, 'first']]
    first = MatrixOneSQLBuilder().insert_into('items').insert_values(columns, rows)
    second = MatrixOneSQLBuilder().insert_into('items').insert_values(columns, rows)
    first.values(id=2, label='second')
    assert execute_rows(first) == [(1, 'first'), (2, 'second')]
    assert execute_rows(second) == [(1, 'first')]
    assert rows == [[1, 'first']]
    assert columns == ['id', 'label']


def test_input_container_changes_do_not_rewrite_pending_insert():
    columns = ['id', 'label']
    rows = [[1, 'first']]
    builder = MatrixOneSQLBuilder().insert_into('items').insert_values(columns, rows)
    columns.reverse()
    rows[0][1] = 'changed'
    rows.append([2, 'extra'])
    assert execute_rows(builder) == [(1, 'first')]


@pytest.mark.parametrize('row', [{'id': 9, 'label': 'mapped'}, MappingProxyType({'id': 9, 'label': 'mapped'})])
def test_mapping_rows_are_rejected_without_replacing_pending_values(row):
    builder = MatrixOneSQLBuilder().insert_into('items').insert_values(['id', 'label'], [[1, 'first']])
    before = builder.build()

    with pytest.raises(TypeError, match=r'insert_values\(\) rows must be positional, not mappings'):
        builder.insert_values(['id', 'label'], [row])

    assert builder.build() == before


def test_outer_generator_rows_are_not_consumed_by_mapping_validation():
    rows = ([1, 'first'], [2, 'second'])
    builder = MatrixOneSQLBuilder().insert_into('items').insert_values(['id', 'label'], (row for row in rows))

    assert execute_rows(builder) == [(1, 'first'), (2, 'second')]


def test_outer_generator_can_reuse_a_mutable_row_buffer():
    def reused_buffer_rows():
        row = [None, None]
        for values in ((1, 'first'), (2, 'second')):
            row[:] = values
            yield row

    builder = MatrixOneSQLBuilder().insert_into('items').insert_values(['id', 'label'], reused_buffer_rows())

    assert execute_rows(builder) == [(1, 'first'), (2, 'second')]


@pytest.mark.parametrize('first_call', ['values', 'bulk'])
def test_bulk_insert_appends_without_losing_earlier_rows(first_call):
    builder = MatrixOneSQLBuilder().insert_into('items')
    if first_call == 'values':
        builder.values(id=1, label='first')
    else:
        builder.insert_values(['id', 'label'], [[1, 'first']])

    builder.insert_values(['id', 'label'], [(2, 'second')])
    builder.values(id=3)
    assert execute_rows(builder) == [(1, 'first'), (2, 'second'), (3, None)]


def test_empty_bulk_insert_preserves_pending_rows():
    builder = MatrixOneSQLBuilder().insert_into('items').values(id=1, label='first')
    builder.insert_values(['id', 'label'], [])
    assert execute_rows(builder) == [(1, 'first')]


@pytest.mark.parametrize('columns', [['label', 'id'], ['id', 'other'], ['id']])
def test_bulk_insert_rejects_changed_columns_without_dropping_rows(columns):
    builder = MatrixOneSQLBuilder().insert_into('items').values(id=1, label='first')
    before = builder.build()
    with pytest.raises(ValueError, match='INSERT columns'):
        builder.insert_values(columns, [[2] * len(columns)])
    assert builder.build() == before
    builder.insert_values(['id', 'label'], [[2, 'second']])
    assert execute_rows(builder) == [(1, 'first'), (2, 'second')]


@pytest.mark.parametrize(
    'bad_row,error',
    [({3, 'third'}, TypeError), ('xy', TypeError),
     ([3], ValueError), ([3, 'third', 4], ValueError)],
)
def test_bulk_insert_rejects_bad_rows_atomically(bad_row, error):
    builder = MatrixOneSQLBuilder().insert_into('items').values(id=1, label='first')
    before = builder.build()
    with pytest.raises(error):
        builder.insert_values(['id', 'label'], [[2, 'second'], bad_row])
    assert builder.build() == before
    builder.insert_values(['id', 'label'], [[4, 'fourth']])
    assert execute_rows(builder) == [(1, 'first'), (4, 'fourth')]


@pytest.mark.parametrize('first_call', ['values', 'bulk'])
def test_new_insert_target_does_not_inherit_pending_rows(first_call):
    builder = MatrixOneSQLBuilder().insert_into('first')
    if first_call == 'values':
        builder.values(id=1, label='first')
    else:
        builder.insert_values(['id', 'label'], [[1, 'first']])
    first_sql, first_params = builder.build()
    second_sql, second_params = builder.insert_into('second').insert_values(
        ['id', 'label'], [[2, 'second']]
    ).build()
    with closing(sqlite3.connect(':memory:')) as connection:
        connection.execute('CREATE TABLE first (id INTEGER, label TEXT)')
        connection.execute('CREATE TABLE second (id INTEGER, label TEXT)')
        connection.execute(first_sql, first_params)
        connection.execute(second_sql, second_params)
        assert connection.execute('SELECT id, label FROM first').fetchall() == [(1, 'first')]
        assert connection.execute('SELECT id, label FROM second').fetchall() == [(2, 'second')]


def test_insert_into_starts_a_new_statement_for_the_same_table():
    builder = MatrixOneSQLBuilder().insert_into('items').values(id=1, label='first')
    builder.insert_into('items').insert_values(['id', 'label'], [[2, 'second']])
    assert execute_rows(builder) == [(2, 'second')]


def test_empty_first_insert_row_keeps_default_insert_and_rejects_new_columns():
    builder = MatrixOneSQLBuilder().insert_into('items')
    builder.values().values()
    before = builder.build()
    assert before == ('INSERT INTO items () VALUES (), ()', [])
    with pytest.raises(ValueError, match='unexpected INSERT columns'):
        builder.values(id=7)
    with pytest.raises(ValueError, match='INSERT columns'):
        builder.insert_values(['id'], [[7]])
    assert builder.build() == before


def test_bulk_insert_preserves_default_rows_without_columns():
    builder = MatrixOneSQLBuilder().insert_into('items')
    builder.insert_values([], [[]]).values()
    before = builder.build()
    assert before == ('INSERT INTO items () VALUES (), ()', [])
    with pytest.raises(ValueError, match='INSERT columns'):
        builder.insert_values(['id', 'label'], [[7, 'valid']])
    assert builder.build() == before
    builder.insert_into('items').insert_values(['id', 'label'], [[7, 'valid']])
    assert execute_rows(builder) == [(7, 'valid')]


@pytest.mark.parametrize('columns', [{'id', 'label'}, {'id': 0, 'label': 1}, 'id'])
def test_bulk_insert_rejects_unordered_or_string_columns(columns):
    builder = MatrixOneSQLBuilder().insert_into('items')
    with pytest.raises(TypeError, match='INSERT columns'):
        builder.insert_values(columns, [[1, 'first']])
    builder.insert_values(['id', 'label'], [[2, 'second']])
    assert execute_rows(builder) == [(2, 'second')]
