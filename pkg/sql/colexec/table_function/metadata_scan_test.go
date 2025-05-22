// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type mock struct {
	name               string
	source             *vector.Vector
	col                *vector.Vector
	expected_db        string
	expected_tbl       string
	expected_idx       string
	expected_col       string
	expected_tombstone bool
	expected_err       error
}

func TestHandleDataSource(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mp := mpool.MustNewZero()

	testCases := []mock{
		{
			name:               "invalid_source_length",
			source:             createStringVector(t, mp, []string{"db", "table"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_tombstone: false,
			expected_err:       moerr.NewInternalErrorNoCtx("wrong input len"),
		},
		{
			name:               "invalid_col_length",
			source:             createStringVector(t, mp, []string{"db.table"}),
			col:                createStringVector(t, mp, []string{"col1", "col2"}),
			expected_tombstone: false,
			expected_err:       moerr.NewInternalErrorNoCtx("wrong input len"),
		},
		{
			name:               "valid_old_format",
			source:             createStringVector(t, mp, []string{"db1.table1"}),
			col:                createStringVector(t, mp, []string{"col1"}),
			expected_db:        "db1",
			expected_tbl:       "table1",
			expected_col:       "col1",
			expected_tombstone: false,
		},
		{
			name:               "valid_new_format",
			source:             createStringVector(t, mp, []string{"db2.table2.?index1"}),
			col:                createStringVector(t, mp, []string{"col2"}),
			expected_db:        "db2",
			expected_tbl:       "table2",
			expected_idx:       "index1",
			expected_col:       "col2",
			expected_tombstone: false,
		},
		{
			name:               "invalid_new_format_index1",
			source:             createStringVector(t, mp, []string{"db.table.index"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_tombstone: false,
			expected_err:       moerr.NewInternalErrorNoCtx("index name must start with ? and follow identifier rules"),
		},
		{
			name:               "double?",
			source:             createStringVector(t, mp, []string{"db.table.??index"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_db:        "db",
			expected_tbl:       "table",
			expected_idx:       "?index",
			expected_col:       "col",
			expected_tombstone: false,
		},
		{
			name:               "invalid_new_format_identifier",
			source:             createStringVector(t, mp, []string{"db.table.#index"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_tombstone: false,
			expected_err:       moerr.NewInternalErrorNoCtx("index name must start with ? and follow identifier rules"),
		},
		{
			name:               "invalid_format_1_part",
			source:             createStringVector(t, mp, []string{"justdb"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_tombstone: false,
			expected_err:       moerr.NewInternalErrorNoCtx("source must be in db_name.table_name or db_name.table_name.?index_name or db_name.table_name.# or db_name.table_name.?index_name.# format"),
		},
		{
			name:               "invalid_format_4_parts",
			source:             createStringVector(t, mp, []string{"db.table.extra.?index"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_tombstone: false,
			expected_err:       moerr.NewInternalErrorNoCtx("invalid tombstone identifier: must be #"),
		},
		{
			name:               "empty_dbname",
			source:             createStringVector(t, mp, []string{".table.?index"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_db:        "",
			expected_tbl:       "table",
			expected_idx:       "index",
			expected_col:       "col",
			expected_tombstone: false,
		},
		{
			name:               "whitespace_in_source",
			source:             createStringVector(t, mp, []string{"db.table.? index"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_db:        "db",
			expected_tbl:       "table",
			expected_idx:       " index",
			expected_col:       "col",
			expected_tombstone: false,
		},
		{
			name:               "underscore_identifier",
			source:             createStringVector(t, mp, []string{"_db.table.?_index"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_db:        "_db",
			expected_tbl:       "table",
			expected_idx:       "_index",
			expected_col:       "col",
			expected_tombstone: false,
		},
		{
			name:               "valid_tombstone_format",
			source:             createStringVector(t, mp, []string{"db3.table3.#"}),
			col:                createStringVector(t, mp, []string{"col3"}),
			expected_db:        "db3",
			expected_tbl:       "table3",
			expected_col:       "col3",
			expected_tombstone: true,
			expected_idx:       "",
		},
		{
			name:               "invalid_tombstone_with_extra_char",
			source:             createStringVector(t, mp, []string{"db.table.##"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_err:       moerr.NewInternalErrorNoCtx("index name must start with ? and follow identifier rules"),
			expected_tombstone: false,
		},
		{
			name:               "mixed_tombstone_and_index",
			source:             createStringVector(t, mp, []string{"db.table.#?index"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_err:       moerr.NewInternalErrorNoCtx("index name must start with ? and follow identifier rules"),
			expected_tombstone: false,
		},
		{
			name:               "valid_new_four_format",
			source:             createStringVector(t, mp, []string{"db.table.?index.#"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_db:        "db",
			expected_tbl:       "table",
			expected_idx:       "index",
			expected_col:       "col",
			expected_tombstone: true,
		},
		{
			name:               "invalid_new_four_format",
			source:             createStringVector(t, mp, []string{"db.table.index.#"}),
			col:                createStringVector(t, mp, []string{"col"}),
			expected_err:       moerr.NewInternalErrorNoCtx("index name must start with ? and follow identifier rules"),
			expected_tombstone: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if tc.source != nil {
					tc.source.Free(mp)
				}
				if tc.col != nil {
					tc.col.Free(mp)
				}
			}()

			db, tbl, idx, col, visitTombstone, err := handleDataSource(tc.source, tc.col)

			if tc.expected_err != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expected_err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected_db, db)
				require.Equal(t, tc.expected_tbl, tbl)
				require.Equal(t, tc.expected_idx, idx)
				require.Equal(t, tc.expected_col, col)
				require.Equal(t, tc.expected_tombstone, visitTombstone)
			}
		})
	}
}

func createStringVector(t *testing.T, mp *mpool.MPool, values []string) *vector.Vector {
	vec := vector.NewVec(types.T_varchar.ToType())
	for _, v := range values {
		err := vector.AppendBytes(vec, []byte(v), false, mp)
		require.NoError(t, err)
	}
	return vec
}
