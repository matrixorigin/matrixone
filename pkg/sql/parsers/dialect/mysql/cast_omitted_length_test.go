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
// truncates large values. The omitted-length signature (family "char",
// DisplayWith -1) stays visible on the tree so the canonical rendering
// prints "char" instead of an explicit TEXT cast.
func TestCastOmittedCharLengthTargetIsUnbounded(t *testing.T) {
	for _, sql := range []string{
		"select cast(a as char)",
		"select cast('a' as char)",
		"select a::char",
		"select convert(a, char)",
	} {
		target := castTargetOf(t, sql)
		require.Equal(t, "char", strings.ToLower(target.InternalType.FamilyString), sql)
		require.Equal(t, uint32(defines.MYSQL_TYPE_TEXT), target.InternalType.Oid, sql)
		require.Equal(t, int32(-1), target.InternalType.DisplayWith, sql)
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
	require.Equal(t, "char", strings.ToLower(target.InternalType.FamilyString))
	require.Equal(t, uint32(defines.MYSQL_TYPE_TEXT), target.InternalType.Oid)
}

// TestCastOmittedCharLengthRendersReparseableChar pins the CTAS contract at
// the grammar layer: CTAS rebuilds its INSERT ... SELECT by formatting the
// AsSource select and re-parsing the produced SQL, so the canonical rendering
// of an omitted-length CHAR cast must be the omitted "char" spelling itself.
// It re-parses through the same omitted-length action and keeps the
// StringFamily TEXT target that base_binder.go distinguishes from explicit
// TEXT casts; a "text" rendering would silently turn the CTAS source into an
// explicit TEXT cast and lose the omitted-CHAR provenance (PR #32438 review).
func TestCastOmittedCharLengthRendersReparseableChar(t *testing.T) {
	render := func(sql string) string {
		t.Helper()
		st, err := ParseOne(t.Context(), sql, 1)
		require.NoError(t, err)
		return tree.String(st, dialect.MYSQL)
	}

	rendered := render("select cast(a as char)")
	require.Equal(t, "select cast(a as char)", rendered)

	// The rendered SQL re-parses to the same omitted-length signature.
	target := castTargetOf(t, rendered)
	require.Equal(t, "char", strings.ToLower(target.InternalType.FamilyString))
	require.Equal(t, uint32(defines.MYSQL_TYPE_TEXT), target.InternalType.Oid)
	require.Equal(t, int32(-1), target.InternalType.DisplayWith)
	require.Equal(t, tree.StringFamily, target.InternalType.Family)

	// An explicit TEXT cast keeps its explicit spelling and the BlobFamily
	// TIM signature: the two remain distinguishable after a round trip.
	textTarget := castTargetOf(t, render("select cast(a as text)"))
	require.Equal(t, "text", strings.ToLower(textTarget.InternalType.FamilyString))
	require.Equal(t, uint32(defines.MYSQL_TYPE_TEXT), textTarget.InternalType.Oid)
	require.Equal(t, tree.BlobFamily, textTarget.InternalType.Family)

	// CONVERT(expr, CHAR) parses to the same cast expression and must render
	// the same re-parseable omitted-length "char" spelling.
	require.Equal(t, rendered, render("select convert(a, char)"))

	// Explicit CHAR(N) keeps its width in the rendering.
	require.Equal(t, "select cast(a as char(3))", render("select cast(a as char(3))"))
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
