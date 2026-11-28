// Copyright 2023 Matrix Origin
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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestParseSQLUdfBody(t *testing.T) {
	const mode = "ANSI_QUOTES"
	for _, tc := range []struct {
		name, sql string
		query     bool
		wantType  any
	}{
		{"ordinary projection", "SELECT $1", true, &tree.SelectClause{}},
		{"query with leading empty statements", "; /* comment */ ;select $1; select 2;", true, &tree.SelectClause{}},
		{"set with leading empty statements", "; /* comment */ (select 1 where 1=0) union all select $1;", true, &tree.UnionClause{}},
		{"CTE", "WITH q AS (SELECT $1) SELECT * FROM q", true, &tree.SelectClause{}},
		{"table query", `TABLE "db"."t" LIMIT 1`, true, &tree.SelectClause{}},
		{"table as set left term", "table db.t union all select $1", true, &tree.UnionClause{}},
		{"values as set right term", "table db.t union all values row($1)", true, &tree.UnionClause{}},
		{"parenthesized values as set left term", "(values row($1)) union all select 2", true, &tree.UnionClause{}},
		{"scalar subquery with values as set left term", "(values row($1) union all select 2)", false, &tree.Subquery{}},
		{"values column expression", `VALUES("a")`, false, &tree.FuncExpr{}},
		{"parenthesized set", `/* lead */ ((select "a" from "db"."t" where "id"=$1)) union all select 2`, true, &tree.UnionClause{}},
		{"query limit", "(select $1) limit 1", true, &tree.ParenSelect{}},
		{"query order", "(select $1 as n) order by n", true, &tree.ParenSelect{}},
		{"bare scalar subquery", "(select $1)", false, &tree.Subquery{}},
		{"wrapped set scalar subquery", "((select $1 where 1=0) union all select 2)", false, &tree.Subquery{}},
		{"parenthesized argument", "($1 + 1)", false, &tree.ParenExpr{}},
		{"scalar subquery operand", "(select $1) + 1", false, &tree.BinaryExpr{}},
		{"subquery in call argument", "abs((select $1))", false, &tree.FuncExpr{}},
		{"quoted keyword", `"SELECT"`, false, &tree.UnresolvedName{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmts, scalar, query, err := ParseSQLUdfBody(t.Context(), tc.sql, 1, mode)
			require.NoError(t, err)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()
			require.Equal(t, tc.query, query)
			if query {
				require.Nil(t, scalar)
				require.IsType(t, tc.wantType, stmts[0].(*tree.Select).Select)
			} else {
				require.IsType(t, tc.wantType, scalar)
			}
		})
	}
}

func TestParseSQLUdfPerformBody(t *testing.T) {
	const mode = "ANSI_QUOTES"
	for _, tc := range []struct {
		name, sql string
		query     bool
		wantType  any
	}{
		{"projection", "perform select $1", true, &tree.SelectClause{}},
		{"CTE", "PERFORM WITH q AS (SELECT $1 AS x) SELECT x FROM q", true, &tree.SelectClause{}},
		{"parenthesized set", "perform (select 1 where 1=0) union all select $1", true, &tree.UnionClause{}},
		{"leading empty and trailing statement", "; /* comment */ ; perform select $1; select 3;", true, &tree.SelectClause{}},
		{"column", "perform + $1", false, &tree.BinaryExpr{}},
		{"qualified column", `perform."x"`, false, &tree.UnresolvedName{}},
		{"quoted column", `"perform"`, false, &tree.UnresolvedName{}},
		{"scalar with function call", "perform + abs($1)", false, &tree.BinaryExpr{}},
		{"ordinary SQL function call", "some_fn($1)", false, &tree.FuncExpr{}},
		{"identifier before query", "perform; perform select 7;", false, &tree.UnresolvedName{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmts, scalar, query, err := ParseSQLUdfBody(t.Context(), tc.sql, 1, mode)
			require.NoError(t, err)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()
			require.Equal(t, tc.query, query)
			outer := stmts[0].(*tree.Select)
			require.Equal(t, tc.query, outer.IsPerform)
			if query {
				require.Nil(t, scalar)
				require.IsType(t, tc.wantType, outer.Select)
			} else {
				require.IsType(t, tc.wantType, scalar)
			}
		})
	}
}

func TestParseSQLUdfBodyRejectsInvalidBody(t *testing.T) {
	for _, tc := range []struct {
		sql           string
		explicitQuery bool
	}{
		{"", false},
		{"; $1", false},
		{"; (select $1) + 1", false},
		{"(1 + )", false},
		{"(select 1) union all", false},
		{"select from", true},
		{"perform select from", true},
		{"perform with", true},
		{"perform($1)", false},
		{"values row(1)", false},
		{"values row(1) union all select 2", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmts, scalar, query, err := ParseSQLUdfBody(t.Context(), tc.sql, 1, "")
			require.Error(t, err)
			require.Equal(t, tc.explicitQuery, query)
			require.Nil(t, stmts)
			require.Nil(t, scalar)
		})
	}
}
