// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestCreateTableLikePreservesCheckAcrossSQLModes(t *testing.T) {
	testCases := []struct {
		name       string
		sourceMode string
		likeMode   string
		createSQL  string
	}{
		{
			name:       "no backslash escapes to default",
			sourceMode: "NO_BACKSLASH_ESCAPES",
			likeMode:   "",
			createSQL:  `create table source_t(s varchar(10), check (s = 'a\nb'))`,
		},
		{
			name:       "default to no backslash escapes",
			sourceMode: "",
			likeMode:   "NO_BACKSLASH_ESCAPES",
			createSQL:  `create table source_t(s varchar(10), check (s = 'a\\nb'))`,
		},
		{
			name:       "no backslash escapes trailing backslash",
			sourceMode: "NO_BACKSLASH_ESCAPES",
			likeMode:   "",
			createSQL:  `create table source_t(s varchar(20), check (s = 'a\'))`,
		},
	}

	build := func(t *testing.T, mock *MockOptimizer, sql, mode string) *plan.TableDef {
		t.Helper()
		mock.ctxt.SetSqlModeOverride(mode)
		stmts, err := mysql.ParseWithSQLMode(t.Context(), sql, 1, mode)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		defer stmts[0].Free()
		built, err := BuildPlan(mock.CurrentContext(), stmts[0], false)
		require.NoError(t, err)
		return built.GetDdl().GetCreateTable().GetTableDef()
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			source := build(t, mock, tc.createSQL, tc.sourceMode)
			require.Len(t, source.Checks, 1)
			mock.ctxt.tables["source_t"] = source

			clone := build(t, mock, "create table clone_t like source_t", tc.likeMode)
			require.Len(t, clone.Checks, 1)
			require.Equal(t, source.Checks[0].Check, clone.Checks[0].Check)
			require.Equal(t, source.Checks[0].OriginSql, clone.Checks[0].OriginSql)
			require.NotSame(t, source.Checks[0].Check, clone.Checks[0].Check)
		})
	}
}
