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

"""Pagination contracts for the three public fulltext SQL builders."""

import pytest
from matrixone.sqlalchemy_ext.fulltext_index import FulltextSearchBuilder as IndexSearch
from matrixone.sqlalchemy_ext.fulltext_search import FulltextQueryBuilder, FulltextSearchBuilder


@pytest.mark.parametrize('limit', [None, 0, 2])
@pytest.mark.parametrize('entry', ['query', 'client', 'index'])
def test_fulltext_limit_preserves_zero(entry, limit):
    if entry == 'query':
        sql = FulltextQueryBuilder().must('hello').as_sql('docs', ['body'], limit=limit)
    elif entry == 'client':
        builder = FulltextSearchBuilder(None).table('docs').columns(['body']).query('hello')
        if limit is not None:
            builder.limit(limit)
        sql = builder.explain()
    else:
        builder = IndexSearch('docs', ['body']).search('hello')
        if limit is not None:
            builder.limit(limit)
        sql = builder.build_sql()
    if limit is None:
        assert 'LIMIT' not in sql
    else:
        assert sql.endswith(f'LIMIT {limit}')
