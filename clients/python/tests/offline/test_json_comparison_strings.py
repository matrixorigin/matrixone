# Copyright 2026 Matrix Origin
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

"""JSON comparison binding contracts; no database server is involved."""

import json
import operator

import pytest
from sqlalchemy import column, select
from sqlalchemy.dialects import mysql

from matrixone.sqlalchemy_ext import JSON


@pytest.mark.parametrize('compare', [operator.eq, operator.ne], ids=['eq', 'ne'])
@pytest.mark.parametrize('value', ['plain', '中文', 'a"b', r'a\b', 'a\nb', 'a\tb', 'a\x00b'])
def test_json_comparison_string_roundtrip(compare, value):
    predicate = compare(column('payload', JSON)['name'], value)
    compiled = select(predicate).compile()
    # Decode the value actually bound by the public SQLAlchemy expression.
    encoded = compiled.params[compiled.bind_names[predicate.right]]
    assert json.loads(encoded) == value


@pytest.mark.parametrize('compare', [operator.eq, operator.ne], ids=['eq', 'ne'])
@pytest.mark.parametrize('value', ['"true"', '"false"', '"null"', '"already encoded"'])
def test_json_comparison_preserves_explicit_json(compare, value):
    predicate = compare(column('payload', JSON)['name'], value)
    compiled = select(predicate).compile()
    assert compiled.params[compiled.bind_names[predicate.right]] == value


@pytest.mark.parametrize('compare', [operator.eq, operator.ne], ids=['eq', 'ne'])
@pytest.mark.parametrize('value', ['true', 'false', 'null'])
def test_json_special_strings_compare_in_string_domain(compare, value):
    predicate = compare(column('payload', JSON)['name'], value)
    sql = str(select(predicate).compile(dialect=mysql.dialect(), compile_kwargs={'literal_binds': True}))
    assert "json_type(json_extract(" in sql
    assert "'STRING'" in sql
    assert "json_extract_string(" in sql
    assert (" = " if compare is operator.eq else " != ") + f"'{value}'" in sql
    assert (" AND " if compare is operator.eq else " OR ") in sql


@pytest.mark.parametrize('compare', [operator.eq, operator.ne], ids=['eq', 'ne'])
@pytest.mark.parametrize('value', [True, False], ids=['true', 'false'])
def test_json_boolean_comparison_avoids_json_to_bool_cast(compare, value):
    predicate = compare(column('payload', JSON)['name'], value)
    sql = str(select(predicate).compile(dialect=mysql.dialect(), compile_kwargs={'literal_binds': True}))
    assert "json_type(json_extract(" in sql
    assert "'BOOLEAN'" in sql
    assert "CAST(json_extract(" in sql
    assert " AS CHAR)" in sql
    assert (" = " if compare is operator.eq else " != ") + f"'{str(value).lower()}'" in sql
    assert (" AND " if compare is operator.eq else " OR ") in sql
