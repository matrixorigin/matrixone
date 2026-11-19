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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// TestBinaryPrefixOperatorSyntax probes how the `BINARY <expr>` prefix
// operator is accepted across operand classes, clause positions and
// neighboring BINARY/VARBINARY syntax (issue #31563).
func TestBinaryPrefixOperatorSyntax(t *testing.T) {
	okCases := []struct {
		input, want string
	}{
		// legacy narrow operand forms must keep their exact AST and formatting
		{`select binary 'x'`, `select binary('x')`},
		{`select binary 123`, `select binary(123)`},
		{`select binary col from t`, `select binary(col) from t`},
		{`select binary('x')`, `select binary('x')`},
		{`select binary (( 'x' ))`, `select binary(('x'))`},
		// issue #31563 forms: general expression operands now parse
		{`select binary cast('x' as char)`, `select binary(cast('x' as varchar))`},
		{`select binary (concat('a','b'))`, `select binary(concat('a', 'b'))`},
		{`select binary concat('a','b')`, `select binary(concat('a', 'b'))`},
		{`select binary if(true, 'a', 'b')`, `select binary(if(true, 'a', 'b'))`},
		{`select binary case when true then 'a' else 'b' end`, `select binary(case when true then 'a' else 'b' end)`},
		{`select binary (case when true then 'a' else 'b' end)`, `select binary(case when true then 'a' else 'b' end)`},
		{`select binary -1`, `select binary(-1)`},
		{`select binary (select 'a')`, `select binary((select 'a'))`},
		// redundant empty paren layers around a subquery still collapse to the
		// canonical single layer; intermediate SELECT layers that carry semantic
		// clauses (LIMIT/ORDER BY/WITH and friends) must NOT be flattened away:
		// the planner consumes those clauses (PR review 5688289986).
		{`select binary (((select 'a')))`, `select binary((select 'a'))`},
		{`select binary(((select 'a') limit 0))`, `select binary(((select 'a') limit 0))`},
		{`select binary(((select 1 as x) order by 1 limit 2))`, `select binary(((select 1 as x) order by 1 limit 2))`},
		{`select binary((with c as (select 'a' as a) (select a from c) limit 1))`, `select binary((with c as (select 'a' as a) (select a from c) limit 1))`},
		{`select binary exists (select 1)`, `select binary(exists (select 1))`},
		// precedence: BINARY binds tighter than arithmetic/comparison
		{`select binary 'a' = 'A'`, `select binary('a') = 'A'`},
		{`select binary 1 + 1`, `select binary(1) + 1`},
		{`select binary a = binary b from t`, `select binary(a) = binary(b) from t`},
		// clause coverage
		{`select col from t where binary col = 'x'`, `select col from t where binary(col) = 'x'`},
		{`select col from t where binary (concat(col, 'x')) = 'y'`, `select col from t where binary(concat(col, 'x')) = 'y'`},
		{`select col from t order by binary col`, `select col from t order by binary(col)`},
		{`insert into t values (binary 'x'), (binary ('y'))`, `insert into t values (binary('x')), (binary('y'))`},
		{`insert into t (a) select binary col from s`, `insert into t (a) select binary(col) from s`},
		{`update t set a = binary 'x' where binary a = 'x'`, `update t set a = binary('x') where binary(a) = 'x'`},
		// introducers and operand variety
		{`select binary _binary 'x'`, `select binary(_binary 'x')`},
		{`select _binary 'x'`, `select _binary 'x'`},
		{`select binary @a`, `select binary(@a)`},
		{`select _binary 'x' collate binary`, `select (_binary 'x') collate binary`},
		{`select binary 'a' collate utf8mb4_bin`, `select (binary('a')) collate utf8mb4_bin`},
		// DDL/predicate neighbors must keep parsing
		{`create table t (a binary(10), b varbinary(20), c binary, d char(10) default 'x')`, `create table t (a binary(10), b varbinary(20), c binary, d char(10) default 'x')`},
		{`select cast('x' as binary)`, `select cast('x' as binary)`},
		{`select cast('x' as binary(10))`, `select cast('x' as binary(10))`},
		{`select convert('x' using binary)`, `select convert('x' using binary)`},
		// legacy multi-arg functional form stays parseable (tuple operand)
		{`select binary('a', 'b')`, `select binary(('a', 'b'))`},
	}
	for _, test := range okCases {
		t.Run(test.input, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), test.input, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			formatted := tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithSingleQuoteString())
			require.Equal(t, test.want, formatted)
			roundTrip, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err)
			t.Cleanup(roundTrip.Free)
			require.Equal(t, formatted,
				tree.StringWithOpts(roundTrip, dialect.MYSQL, tree.WithSingleQuoteString()))
		})
	}

	errCases := []string{
		// dangling / reserved-word operands must stay syntax errors
		`select binary from t`,
		`select binary`,
		`select binary * from t`,
		`select binary collate utf8mb4_bin`,
		`select binary , 1`,
	}
	for _, sql := range errCases {
		t.Run("ERR: "+sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err)
		})
	}
}

// TestBinaryPrefixPreservesSubqueryClauses asserts on the consumer-visible AST
// (the tree the planner binds), not just on formatted text, that `BINARY
// <subquery>` keeps semantic clauses of intermediate SELECT layers when paren
// wrappers are flattened (PR review 5688289986): dropping LIMIT 0 would turn a
// zero-row scalar subquery into one that returns rows.
func TestBinaryPrefixPreservesSubqueryClauses(t *testing.T) {
	extract := func(t *testing.T, sql string) tree.SelectStatement {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		sel, ok := stmt.(*tree.Select)
		require.True(t, ok)
		sc, ok := sel.Select.(*tree.SelectClause)
		require.True(t, ok)
		require.Len(t, sc.Exprs, 1)
		fn, ok := sc.Exprs[0].Expr.(*tree.FuncExpr)
		require.True(t, ok)
		sub, ok := fn.Exprs[0].(*tree.Subquery)
		require.True(t, ok)
		return sub.Select
	}
	var walk func(root tree.SelectStatement, pred func(*tree.Select) bool, depth int) bool
	walk = func(root tree.SelectStatement, pred func(*tree.Select) bool, depth int) bool {
		if depth <= 0 || root == nil {
			return false
		}
		switch s := root.(type) {
		case *tree.Select:
			if pred(s) {
				return true
			}
			return walk(s.Select, pred, depth-1)
		case *tree.ParenSelect:
			if s.Select == nil {
				return false
			}
			if pred(s.Select) {
				return true
			}
			return walk(s.Select.Select, pred, depth-1)
		}
		return false
	}
	t.Run("limit 0 survives", func(t *testing.T) {
		root := extract(t, `select binary(((select 'a') limit 0))`)
		require.True(t, walk(root, func(s *tree.Select) bool { return s.Limit != nil }, 8),
			"flattened subquery must retain the intermediate LIMIT")
	})
	t.Run("order by and limit survive", func(t *testing.T) {
		root := extract(t, `select binary(((select 1 as x) order by 1 limit 2))`)
		require.True(t, walk(root, func(s *tree.Select) bool {
			return len(s.OrderBy) > 0 && s.Limit != nil
		}, 8), "flattened subquery must retain intermediate ORDER BY and LIMIT")
	})
	t.Run("with cte and limit survive", func(t *testing.T) {
		root := extract(t, `select binary((with c as (select 'a' as a) (select a from c) limit 1))`)
		require.True(t, walk(root, func(s *tree.Select) bool { return s.With != nil }, 8),
			"flattened subquery must retain the intermediate WITH (CTE)")
		require.True(t, walk(root, func(s *tree.Select) bool { return s.Limit != nil }, 8),
			"flattened subquery must retain the intermediate LIMIT")
	})
}
