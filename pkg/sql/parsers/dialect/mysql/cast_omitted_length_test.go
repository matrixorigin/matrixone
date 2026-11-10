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

package mysql

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// castTargetOf binds a SQL expression and returns the parsed CAST target type.
func castTargetOf(t *testing.T, sql string) *tree.T {
	t.Helper()
	stmt, err := ParseOne(t.Context(), sql, 1)
	require.NoError(t, err)
	require.IsType(t, &tree.Select{}, stmt)
	sel := stmt.(*tree.Select)
	clause, ok := sel.Select.(*tree.SelectClause)
	require.True(t, ok, "expected a plain select clause in %q", sql)
	cast, ok := clause.Exprs[0].Expr.(*tree.CastExpr)
	require.True(t, ok, "expected a cast expression in %q", sql)
	target, ok := cast.Type.(*tree.T)
	require.True(t, ok)
	return target
}

// TestCastOmittedCharLengthTargetIsUnbounded verifies that every CAST syntax
// recognizing an omitted CHAR length materializes the unbounded string
// (TEXT) target instead of the VARCHAR(65535) default, so the cast never
// truncates large values.
func TestCastOmittedCharLengthTargetIsUnbounded(t *testing.T) {
	for _, sql := range []string{
		"select cast(a as char)",
		"select cast('a' as char)",
		"select a::char",
		"select convert(a, char)",
	} {
		target := castTargetOf(t, sql)
		require.Equal(t, "text", strings.ToLower(target.InternalType.FamilyString), sql)
		require.Equal(t, uint32(defines.MYSQL_TYPE_TEXT), target.InternalType.Oid, sql)
		require.Equal(t, int32(0), target.InternalType.DisplayWith, sql)
		require.Equal(t, tree.StringFamily, target.InternalType.Family, sql)
	}

	// SERIAL_EXTRACT shares the cast-target grammar rule.
	stmt, err := ParseOne(t.Context(), "select serial_extract(s, 0 as char)", 1)
	require.NoError(t, err)
	sel := stmt.(*tree.Select)
	clause := sel.Select.(*tree.SelectClause)
	serial := clause.Exprs[0].Expr.(*tree.SerialExtractExpr)
	target, ok := serial.ResultType.(*tree.T)
	require.True(t, ok)
	require.Equal(t, "text", strings.ToLower(target.InternalType.FamilyString))
	require.Equal(t, uint32(defines.MYSQL_TYPE_TEXT), target.InternalType.Oid)
}

// TestCastOmittedCharLengthRendersReparseableText pins the CTAS contract at
// the grammar layer: CTAS rebuilds its INSERT ... SELECT by formatting the
// AsSource select and re-parsing the produced SQL, so the canonical rendering
// of an omitted-length CHAR cast must be exactly the rendering of a TEXT
// cast target (a bare "text", never "text(0)"), and that rendering must
// parse back to the same unbounded TEXT target.
func TestCastOmittedCharLengthRendersReparseableText(t *testing.T) {
	render := func(sql string) string {
		t.Helper()
		st, err := ParseOne(t.Context(), sql, 1)
		require.NoError(t, err)
		return tree.String(st, dialect.MYSQL)
	}

	rendered := render("select cast(a as char)")
	require.Equal(t, render("select cast(a as text)"), rendered)
	require.Equal(t, "select cast(a as text)", rendered)

	// The rendered SQL re-parses to the same unbounded TEXT target.
	target := castTargetOf(t, rendered)
	require.Equal(t, "text", strings.ToLower(target.InternalType.FamilyString))
	require.Equal(t, uint32(defines.MYSQL_TYPE_TEXT), target.InternalType.Oid)
	require.Equal(t, int32(0), target.InternalType.DisplayWith)

	// CONVERT(expr, CHAR) parses to the same cast expression and must render
	// the same re-parseable unbounded TEXT target.
	require.Equal(t, rendered, render("select convert(a, char)"))
}

// TestCastExplicitCharLengthTargetKeepsWidth verifies that an explicit
// CHAR(N) still builds the bounded CHAR target: width enforcement and
// truncation for explicit lengths are unchanged.
func TestCastExplicitCharLengthTargetKeepsWidth(t *testing.T) {
	for _, sql := range []string{
		"select cast(a as char(3))",
		"select convert(a, char(3))",
	} {
		target := castTargetOf(t, sql)
		require.Equal(t, "char", strings.ToLower(target.InternalType.FamilyString), sql)
		require.Equal(t, uint32(defines.MYSQL_TYPE_STRING), target.InternalType.Oid, sql)
		require.Equal(t, int32(3), target.InternalType.DisplayWith, sql)
	}
}

// TestCastOmittedVarcharLengthTargetUnchanged verifies the fix stays scoped
// to CHAR: an omitted-length VARCHAR cast keeps the VARCHAR(65535) default.
func TestCastOmittedVarcharLengthTargetUnchanged(t *testing.T) {
	target := castTargetOf(t, "select cast(a as varchar)")
	require.Equal(t, "varchar", strings.ToLower(target.InternalType.FamilyString))
	require.Equal(t, uint32(defines.MYSQL_TYPE_VARCHAR), target.InternalType.Oid)
	require.Equal(t, int32(-1), target.InternalType.DisplayWith)
}
