// Copyright 2026 Matrix Origin
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
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

// SHOW CREATE TABLE must print a string default so that replaying the
// output stores the same default (#31066).
func TestShowCreateTableStringDefaultRoundTrip(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	const sourceSQL = "create table t_default_quote (" +
		"a varchar(10) default 'it''s', " +
		"b varchar(10) default 'x`y', " +
		"c varchar(10) default \"q\"\"r\", " +
		"d varchar(10) default 'a\\\\b', " +
		"e varchar(10) default '')"

	sourceDef, err := buildTestCreateTableStmt(mock, sourceSQL)
	require.NoError(t, err)

	showSQL, parsed, err := ConstructCreateTableSQL(&mock.ctxt, sourceDef, nil, false, nil)
	require.NoError(t, err)
	if parsed != nil {
		parsed.Free()
	}
	for _, want := range []string{
		"`a` varchar(10) DEFAULT 'it''s'",
		"`b` varchar(10) DEFAULT 'x`y'",
		"`c` varchar(10) DEFAULT 'q\"r'",
		"`d` varchar(10) DEFAULT 'a\\\\b'",
		"`e` varchar(10) DEFAULT ''",
	} {
		require.True(t, strings.Contains(showSQL, want), "missing %q in\n%s", want, showSQL)
	}

	before := proto.Clone(sourceDef)
	for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
		t.Run(mode, func(t *testing.T) {
			mock.ctxt.SetSqlModeOverride(mode)
			current := sourceDef
			for generation := 0; generation < 2; generation++ {
				ddl, parsed, err := ConstructCreateTableSQL(&mock.ctxt, current, nil, false, nil)
				require.NoError(t, err)
				if parsed != nil {
					parsed.Free()
				}
				stmt, err := mysql.ParseOneWithSQLMode(t.Context(), ddl, 1, mode)
				require.NoError(t, err)
				rebuilt, err := BuildPlan(&mock.ctxt, stmt, false)
				stmt.Free()
				require.NoError(t, err)
				next := rebuilt.GetDdl().GetCreateTable().GetTableDef()
				for i, col := range sourceDef.Cols {
					require.Equal(t, col.Default.OriginString, next.Cols[i].Default.OriginString, col.Name)
					require.True(t, proto.Equal(col.Default.Expr, next.Cols[i].Default.Expr), col.Name)
				}
				current = next
			}
			_, err := ConstructCreateTableSQLForTableDumpHash(sourceDef)
			require.NoError(t, err)
			require.True(t, proto.Equal(before, sourceDef), "rendering must not mutate shared metadata")
		})
	}
}

func TestDefaultWithoutBackslashEscapesRejectsInvalidScalar(t *testing.T) {
	for _, origin := range []string{"'unterminated", "'a\\b', 2", "'a\\b' as alias", "'a\\b' from t", "'a\\b' where true", "'a\\b' limit 1"} {
		t.Run(origin, func(t *testing.T) {
			_, err := formatDefaultWithoutBackslashEscapes(origin)
			require.Error(t, err)
		})
	}
}

func TestAlterCopyDefaultUsesInternalParserMode(t *testing.T) {
	for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
		t.Run(mode, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, `create table mode_copy(id int primary key, v varchar(32) default 'old')`)
			source := mock.ctxt.tables["mode_copy"]
			before := proto.Clone(source)
			mock.ctxt.SetSqlModeOverride(mode)
			literal := `'a\\b'`
			if strings.Contains(mode, "NO_BACKSLASH_ESCAPES") {
				literal = `'a\b'`
			}
			for _, clause := range []string{"add added varchar(32) default " + literal, "modify v varchar(32) default " + literal} {
				stmt, err := mysql.ParseOneWithSQLMode(t.Context(), "alter table mode_copy "+clause+", algorithm=copy", 1, mode)
				require.NoError(t, err)
				built, err := BuildPlan(&mock.ctxt, stmt, false)
				stmt.Free()
				require.NoError(t, err)
				alter := built.GetDdl().GetAlterTable()
				// Match the ordinary-mode parser used by the internal SQL executor.
				replay, err := mysql.ParseOne(t.Context(), alter.CreateTmpTableSql, 1)
				require.NoError(t, err, alter.CreateTmpTableSql)
				rebuilt, err := BuildPlan(&mock.ctxt, replay, false)
				replay.Free()
				require.NoError(t, err)
				for _, col := range alter.CopyTableDef.Cols {
					after := FindColumn(rebuilt.GetDdl().GetCreateTable().TableDef.Cols, col.Name)
					require.NotNil(t, after, col.Name)
					require.Equal(t, col.Default.OriginString, after.Default.OriginString, col.Name)
					require.True(t, proto.Equal(col.Default.Expr, after.Default.Expr), col.Name)
				}
				require.True(t, proto.Equal(before, source), "COPY planning must not rewrite source defaults")
			}
		})
	}
}
