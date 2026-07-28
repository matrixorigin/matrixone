// Copyright 2021 - 2026 Matrix Origin
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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

// TestCTASFullTextNoBackslashEscapes: the CTAS follow-up INSERT ... SELECT is
// re-parsed under the SESSION's sql_mode, so the formatter must receive
// NO_BACKSLASH_ESCAPES from the session (#24823 review follow-up). Under that
// mode a MATCH pattern stores backslashes literally; without the propagation
// the deparse doubled them and the follow-up query searched a different
// pattern.
func TestCTASFullTextNoBackslashEscapes(t *testing.T) {
	sql := `CREATE TABLE ctas_ft AS SELECT N_NAME FROM NATION WHERE MATCH(N_NAME) AGAINST('a\nb' IN BOOLEAN MODE)`

	buildWithMode := func(t *testing.T, mode string) string {
		t.Helper()
		mock := NewMockOptimizer(false)
		mock.ctxt.SetSqlModeOverride(mode)
		ctx := mock.CurrentContext()
		stmts, err := mysql.ParseWithSQLMode(ctx.GetContext(), sql, 1, mode)
		require.NoError(t, err)
		logicPlan, err := BuildPlan(ctx, stmts[0], false)
		require.NoError(t, err)
		createTable := logicPlan.GetDdl().GetCreateTable()
		require.NotNil(t, createTable)
		return createTable.GetCreateAsSelectSql()
	}

	// Under NO_BACKSLASH_ESCAPES the stored pattern is literally a\nb: the
	// generated SQL must keep the single backslash (no re-escaping).
	out := buildWithMode(t, "NO_BACKSLASH_ESCAPES")
	require.Contains(t, out, `AGAINST ('a\nb'`,
		"pattern must keep its literal single backslash, got: "+out)
	require.NotContains(t, out, `a\\nb`, "backslash must not be doubled: "+out)

	// Default mode: \n parses to a newline; the deparse re-escapes it so the
	// generated SQL re-parses (under default mode) to the same newline.
	outDefault := buildWithMode(t, "")
	require.Contains(t, outDefault, `AGAINST ('a\nb'`,
		"newline must be re-escaped on the default path, got: "+outDefault)
}
