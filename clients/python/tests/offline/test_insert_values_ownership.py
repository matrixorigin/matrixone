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
