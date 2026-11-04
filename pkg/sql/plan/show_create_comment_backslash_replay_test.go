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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// buildTestCreateTableStmtForMode mirrors buildTestCreateTableStmt but parses
// under an explicit SQL mode. SHOW CREATE TABLE output is replayed under the
// session mode that displayed it, so both parses must use that mode.
func buildTestCreateTableStmtForMode(t testing.TB, opt Optimizer, sql, sqlMode string) (*TableDef, error) {
	stmt, err := mysql.ParseOneWithSQLMode(opt.CurrentContext().GetContext(), sql, 1, sqlMode)
	if err != nil {
		return nil, err
	}
	t.Cleanup(stmt.Free)
	ddlPlan, err := BuildPlan(opt.CurrentContext(), stmt, false)
	if err != nil {
		return nil, err
	}
	planDdl := ddlPlan.Plan.(*plan.Plan_Ddl)
	definition := planDdl.Ddl.Definition.(*plan.DataDefinition_CreateTable)
	definition.CreateTable.TableDef.TableType = catalog.SystemOrdinaryRel
	return definition.CreateTable.TableDef, nil
}

// testTableComment returns the stored table comment of def.
func testTableComment(def *TableDef) string {
	for _, defType := range def.Defs {
		if properties := defType.GetProperties(); properties != nil {
			for _, property := range properties.Properties {
				if property.Key == catalog.SystemRelAttr_Comment {
					return property.Value
				}
			}
		}
	}
	return ""
}

// buildAndShow parses createSQL under sqlMode, builds its TableDef, and renders
// it through ConstructCreateTableSQL with the same sqlMode in scope.
func buildAndShow(t testing.TB, opt Optimizer, sqlMode string, createSQL string) (*TableDef, string) {
	t.Helper()
	def, err := buildTestCreateTableStmtForMode(t, opt, createSQL, sqlMode)
	require.NoError(t, err, sqlMode)
	showSQL, _, err := ConstructCreateTableSQL(opt.CurrentContext(), def, nil, false, nil)
	require.NoError(t, err)
	return def, showSQL
}

func storedColumnComment(def *TableDef, name string) string {
	for _, col := range def.Cols {
		if col.Name == name {
			return col.Comment
		}
	}
	return ""
}

// SHOW CREATE TABLE must print column and table comments so that replaying its
// output stores the same comments (#31068). The parser stores those comments
// with single quotes already doubled (util.DealCommentString) and backslashes
// raw, so the renderer must double backslashes in the default SQL mode and
// leave the comment verbatim under NO_BACKSLASH_ESCAPES.
func TestShowCreateTableCommentBackslashStoredByteRoundTrip(t *testing.T) {
	// wantStoredComment pins the catalog-storage comment of every named column
	// of tc.createSQL: backslashes stay single, quotes stay doubled. Both
	// subtests' source literals decode to the same raw comments.
	wantStored := map[string]string{
		"a": `C:\dir\new`,
		"b": `it''s`,
		"c": `plain`,
		"d": `end\`,
		"e": `pre\''post`,
		"f": "",
	}
	const wantTableComment = `x\ty''z`

	for _, tc := range []struct {
		name         string
		sqlMode      string
		createSQL    string
		showContains []string
		notContains  []string
	}{
		{
			name:    "default",
			sqlMode: "",
			createSQL: `CREATE TABLE comment_roundtrip_src (
				a INT COMMENT 'C:\\dir\\new',
				b INT COMMENT 'it''s',
				c INT COMMENT 'plain',
				d INT COMMENT 'end\\',
				e INT COMMENT 'pre\\''post',
				f INT
			) COMMENT 'x\\ty''z'`,
			showContains: []string{
				`COMMENT 'C:\\dir\\new'`,
				`COMMENT 'it''s'`,
				`COMMENT 'plain'`,
				`COMMENT 'end\\'`,
				`COMMENT 'pre\\''post'`,
				`COMMENT='x\\ty''z'`,
			},
			// A comment without backslashes must not gain escaping.
			notContains: []string{`COMMENT 'it''''s'`},
		},
		{
			// Under NO_BACKSLASH_ESCAPES a backslash is an ordinary character,
			// so the rendered comment must stay verbatim, matching the index
			// comment contract of formatStrLitForSQLMode.
			name:    "no_backslash_escapes",
			sqlMode: "NO_BACKSLASH_ESCAPES",
			createSQL: `CREATE TABLE comment_roundtrip_src (
				a INT COMMENT 'C:\dir\new',
				b INT COMMENT 'it''s',
				c INT COMMENT 'plain',
				d INT COMMENT 'end\',
				e INT COMMENT 'pre\''post',
				f INT
			) COMMENT 'x\ty''z'`,
			showContains: []string{
				`COMMENT 'C:\dir\new'`,
				`COMMENT 'it''s'`,
				`COMMENT 'end\'`,
				`COMMENT 'pre\''post'`,
				`COMMENT='x\ty''z'`,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			if tc.sqlMode != "" {
				mock.ctxt.SetSqlModeOverride(tc.sqlMode)
			}

			sourceDef, showSQL := buildAndShow(t, mock, tc.sqlMode, tc.createSQL)
			for name, want := range wantStored {
				require.Equal(t, want, storedColumnComment(sourceDef, name),
					"source column %s", name)
			}
			require.Equal(t, wantTableComment, testTableComment(sourceDef))

			// Rendering must escape backslashes (default mode) without
			// re-escaping the already doubled quotes.
			for _, fragment := range tc.showContains {
				require.Contains(t, showSQL, fragment)
			}
			for _, fragment := range tc.notContains {
				require.NotContains(t, showSQL, fragment)
			}

			// Replay the rendered DDL under the same SQL mode and require the
			// catalog-storage comments to survive the round trip unchanged.
			replayedDef, err := buildTestCreateTableStmtForMode(t, mock, showSQL, tc.sqlMode)
			require.NoError(t, err)
			for name, want := range wantStored {
				require.Equal(t, want, storedColumnComment(replayedDef, name),
					"replayed column %s from %s", name, showSQL)
			}
			require.Equal(t, wantTableComment, testTableComment(replayedDef))

			// The replayed definition must render to identical DDL, so
			// repeated SHOW CREATE TABLE cycles stay stable.
			_, showSQLAgain := buildAndShow(t, mock, tc.sqlMode, showSQL)
			require.Equal(t, showSQL, showSQLAgain)
		})
	}
}

// TestShowCreateTableCommentIsNotMutated guards the renderer against mutating
// the TableDef it renders, the same contract index comments already pin.
func TestShowCreateTableCommentIsNotMutated(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	def, showSQL := buildAndShow(t, mock, "", `CREATE TABLE comment_mutation_src (
		a INT COMMENT 'C:\\dir\\new it''s',
		b INT
	) COMMENT 'x\\ty''z'`)
	require.Contains(t, showSQL, `COMMENT 'C:\\dir\\new it''s'`)
	require.Contains(t, showSQL, `COMMENT='x\\ty''z'`)
	require.Equal(t, `C:\dir\new it''s`, def.Cols[0].Comment)
	require.Equal(t, `x\ty''z`, testTableComment(def))
}

// The rendered DDL must survive transport encoding as a single parseable
// literal, including the escaped comments.
func TestShowCreateTableCommentTransport(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	_, showSQL := buildAndShow(t, mock, "", `CREATE TABLE comment_transport_src (
		a INT COMMENT 'C:\\dir\\new',
		b INT
	) COMMENT 'x\\ty'`)
	require.Contains(t, showSQL, `COMMENT 'C:\\dir\\new'`)

	statements, err := mysql.Parse(mock.ctxt.GetContext(),
		`SELECT `+sqlquote.String(showSQL), 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	selectStmt, ok := statements[0].(*tree.Select)
	require.True(t, ok)
	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	require.True(t, ok)
	require.Len(t, selectClause.Exprs, 1)
	value, ok := selectClause.Exprs[0].Expr.(*tree.NumVal)
	require.True(t, ok)
	require.Equal(t, showSQL, value.String())
}
