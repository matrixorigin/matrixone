// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInspectExpressionSyntax(t *testing.T) {
	literal := NewNumVal("@x", "@x", false, P_char)
	variable := NewVarExpr("x", false, false, nil)
	function := &FuncExpr{Func: FuncName2ResolvableFunctionReference(NewUnresolvedColName("abs")), FuncName: NewCStr("abs", 1), Exprs: Exprs{variable}}
	subquery := &Subquery{Select: &Select{Select: &SelectClause{Exprs: SelectExprs{{Expr: literal}}, From: &From{}}}}
	for _, tc := range []struct {
		name       string
		expr       Expr
		restricted bool
		functions  int
	}{
		{"literal is not a variable", literal, false, 0},
		{"quoted column is not a variable", NewUnresolvedColName("@x"), false, 0},
		{"user variable", variable, true, 0},
		{"system variable", NewVarExpr("sql_mode", true, false, nil), true, 0},
		{"nested function variable", &CollateExpr{Expr: &ParenExpr{Expr: function}, Collation: "utf8mb4_bin"}, true, 1},
		{"subquery without variable", subquery, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := String(tc.expr, dialect.MYSQL)
			syntax := InspectExpressionSyntax(tc.expr)
			require.Equal(t, tc.restricted, syntax.VariableOrSubquery)
			require.Len(t, syntax.Functions, tc.functions)
			require.Equal(t, before, String(tc.expr, dialect.MYSQL))
			if tc.functions == 1 {
				require.Same(t, function, syntax.Functions[0])
			}
			require.Equal(t, ExpressionSyntax{}, InspectExpressionSyntax(literal))
		})
	}
}
