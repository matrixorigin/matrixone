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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCollateSelectIntoValidationWalkers(t *testing.T) {
	for _, tc := range []struct {
		name       string
		variables  []*VarExpr
		export     *ExportParam
		nestedInto bool
		userInto   bool
	}{
		{name: "ordinary subquery"},
		{name: "user variable", variables: []*VarExpr{{Name: "inner"}}, nestedInto: true, userInto: true},
		{name: "outfile", export: &ExportParam{}, nestedInto: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subquery := &Subquery{Select: &Select{Select: &SelectClause{}, IntoVars: tc.variables, Ep: tc.export}}
			for _, expr := range []Expr{
				subquery,
				&CollateExpr{Expr: subquery, Collation: "utf8mb4_bin"},
				&CollateExpr{Expr: &ParenExpr{Expr: &CollateExpr{Expr: subquery, Collation: "utf8mb4_bin"}}, Collation: "utf8mb4_bin"},
			} {
				require.Equal(t, tc.nestedInto, exprHasNestedInto(expr))
				require.Equal(t, tc.userInto, exprHasUserVariableInto(expr))
			}
		})
	}
}
