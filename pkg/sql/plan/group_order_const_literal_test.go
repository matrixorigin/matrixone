// Copyright 2026 Matrix Origin
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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// A root GROUP BY / ORDER BY item that is a legal non-integer literal (string,
// decimal, float, boolean, NULL, ...) is an ordinary constant expression. It
// must bind through the same constant-expression path as its parenthesized
// form and as a select-list alias, instead of being rejected as a
// "non-integer constant". Only bare integer literals keep positional-ordinal
// semantics; zero and out-of-range positions stay rejected, and a minus sign
// makes the item a constant expression, never an ordinal.
//
// A constant grouping key must also stay a REAL grouping: the reachable AGG
// node holds the constant in GroupBy when it is the only group key, so an
// empty input produces zero groups instead of the single row an ungrouped
// aggregate would produce. The GroupBy list of the AGG node is the plan-level
// carrier of that contract; once a real trailing group key remains, the
// optimizer may legally drop the redundant constant key (the same freedom the
// pre-existing NULL order-key behavior already has).
//
// Constant ORDER BY keys carry no ordering, so the plan is free to prune them
// (the pre-existing NULL behavior already does); the protected contract is
// that binding succeeds and the trailing real sort key is never dropped.

func constGroupKeyAggregateNode(t *testing.T, query *planpb.Query) *planpb.Node {
	t.Helper()
	agg := firstReachableNode(query, planpb.Node_AGG)
	require.NotNil(t, agg, "constant group key must keep a real grouping operator")
	require.NotEmpty(t, agg.GroupBy,
		"a constant group key must NOT be eliminated into an ungrouped aggregate")
	require.NotNil(t, agg.GroupBy[0])
	return agg
}

func requireGroupKeyShape(t *testing.T, agg *planpb.Node, keys int) {
	t.Helper()
	require.Len(t, agg.GroupBy, keys)
}

// requireConstantGroupKey asserts the aggregate has exactly `keys` group keys
// and that constantKeyIndex is a constant (any bound literal/cast shape whose
// base is a literal, never a column reference), with the literal carried by
// the base when sval is non-empty (and nullKey when set).
func requireConstantGroupKey(t *testing.T, agg *planpb.Node, keys, constantKeyIndex int, sval string, nullKey bool) {
	t.Helper()
	requireGroupKeyShape(t, agg, keys)
	base := constantBaseOf(t, agg.GroupBy[constantKeyIndex])
	lit := base.GetLit()
	require.NotNil(t, lit,
		"group key %d must resolve to a bound literal, not to a column", constantKeyIndex)
	if sval != "" {
		require.Equal(t, sval, lit.GetSval())
	}
	if nullKey {
		require.True(t, lit.GetIsnull())
	} else {
		require.False(t, lit.GetIsnull())
	}
}

// constantBaseOf unwraps the casts that constant group keys and sort keys are
// allowed to be bound behind. It fails on a column reference: a constant must
// never be recorded as a column group/sort key.
func constantBaseOf(t *testing.T, expr *planpb.Expr) *planpb.Expr {
	t.Helper()
	for depth := 0; depth < 4; depth++ {
		require.NotNil(t, expr)
		if expr.GetLit() != nil {
			return expr
		}
		if col := expr.GetCol(); col != nil {
			// A column reference: not a constant.
			return expr
		}
		if f := expr.GetF(); f != nil {
			// A single-argument wrapper, or a typed-literal cast whose second
			// argument carries the target type (a decimal literal such as 1.5
			// binds as cast('1.5', decimal(2,1))), still evaluates to a
			// constant.
			if len(f.GetArgs()) == 1 {
				expr = f.GetArgs()[0]
				continue
			}
			if len(f.GetArgs()) == 2 && f.GetArgs()[1].GetT() != nil {
				expr = f.GetArgs()[0]
				continue
			}
		}
		require.FailNow(t, "unexpected non-constant group/sort key shape",
			"expr: %v", expr)
	}
	require.FailNow(t, "unresolved group/sort key shape")
	return nil
}

func orderQuerySortNode(t *testing.T, query *planpb.Query) *planpb.Node {
	t.Helper()
	sort := firstReachableNode(query, planpb.Node_SORT)
	require.NotNil(t, sort, "ORDER BY query must keep a SORT operator")
	return sort
}

// requireTrailingOrderKeyColumn asserts that the trailing real sort key on the
// named column survives as an ORDER BY key (directly or through the
// projection/cast indirection the builder may add). Constant keys may legally
// be pruned; the real key may not.
func requireTrailingOrderKeyColumn(t *testing.T, query *planpb.Query, column string, maxKeys int) {
	t.Helper()
	sort := orderQuerySortNode(t, query)
	require.NotEmpty(t, sort.OrderBy)
	require.LessOrEqual(t, len(sort.OrderBy), maxKeys,
		"the query wrote at most %d sort keys", maxKeys)
	remaining := []string{}
	for _, spec := range sort.OrderBy {
		if col := keyBaseColumn(t, query, sort, spec); col != "" {
			remaining = append(remaining, col)
		}
	}
	require.Contains(t, remaining, "emp."+column,
		"the trailing real sort key %s must never be dropped by a constant ORDER BY key", column)
}

// keyBaseColumn resolves one ORDER BY key to the column it ultimately orders
// by, or "" when the key is a constant (constants never reference a column).
func keyBaseColumn(t *testing.T, query *planpb.Query, sort *planpb.Node, spec *planpb.OrderBySpec) string {
	t.Helper()
	require.NotNil(t, spec)
	require.NotNil(t, spec.Expr, "every ORDER BY key must be bound")
	var columnOf func(*planpb.Expr) string
	columnOf = func(expr *planpb.Expr) string {
		require.NotNil(t, expr)
		if expr.GetLit() != nil {
			return ""
		}
		if col := expr.GetCol(); col != nil {
			if col.GetName() != "" {
				return col.GetName()
			}
			// resolve through the projection feeding the sort when one exists
			if len(sort.Children) == 0 {
				return ""
			}
			node := query.Nodes[sort.Children[0]]
			require.NotNil(t, node)
			for hops := 0; hops < 3 && node.NodeType != planpb.Node_PROJECT; hops++ {
				if len(node.Children) == 0 {
					return ""
				}
				node = query.Nodes[node.Children[0]]
				require.NotNil(t, node)
			}
			if node.NodeType != planpb.Node_PROJECT || int(col.ColPos) >= len(node.ProjectList) {
				return ""
			}
			return columnOf(node.ProjectList[col.ColPos])
		}
		if f := expr.GetF(); f != nil && len(f.GetArgs()) == 1 {
			return columnOf(f.GetArgs()[0])
		}
		return ""
	}
	return columnOf(spec.Expr)
}

func TestGroupByConstantLiteralKeepsRealGrouping(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		wantErr string
		check   func(*testing.T, *planpb.Query)
	}{
		{
			name: "string constant groups all rows into one real group",
			sql:  "select count(*), sum(empno) from constraint_test.emp group by 'fixed'",
			check: func(t *testing.T, query *planpb.Query) {
				agg := constGroupKeyAggregateNode(t, query)
				requireConstantGroupKey(t, agg, 1, 0, "fixed", false)
				require.NotEmpty(t, agg.AggList)
			},
		},
		{
			name: "decimal constant groups all rows into one real group",
			sql:  "select count(*), sum(empno) from constraint_test.emp group by 1.5",
			check: func(t *testing.T, query *planpb.Query) {
				agg := constGroupKeyAggregateNode(t, query)
				requireConstantGroupKey(t, agg, 1, 0, "", false)
			},
		},
		{
			name: "string constant precedes a real group key",
			sql:  "select count(*) from constraint_test.emp group by 'fixed', deptno",
			check: func(t *testing.T, query *planpb.Query) {
				agg := constGroupKeyAggregateNode(t, query)
				// A constant group key adds no partition granularity, so once
				// the trailing real key remains the optimizer may drop it.
				require.LessOrEqual(t, len(agg.GroupBy), 2)
				realKey := false
				for _, key := range agg.GroupBy {
					if col := key.GetCol(); col != nil {
						require.Equal(t, "emp.deptno", col.GetName(),
							"the only real group key in this query is deptno")
						realKey = true
						continue
					}
					base := constantBaseOf(t, key)
					lit := base.GetLit()
					require.NotNil(t, lit,
						"a constant group key must stay a constant in any retained shape")
					require.Equal(t, "fixed", lit.GetSval())
				}
				require.True(t, realKey,
					"the trailing real group key must never be dropped")
			},
		},
		{
			name: "parenthesized string constant is the same grouping",
			sql:  "select count(*), sum(empno) from constraint_test.emp group by ('fixed')",
			check: func(t *testing.T, query *planpb.Query) {
				agg := constGroupKeyAggregateNode(t, query)
				requireConstantGroupKey(t, agg, 1, 0, "fixed", false)
			},
		},
		{
			name: "parenthesized decimal constant is the same grouping",
			sql:  "select count(*), sum(empno) from constraint_test.emp group by (1.5)",
			check: func(t *testing.T, query *planpb.Query) {
				agg := constGroupKeyAggregateNode(t, query)
				requireConstantGroupKey(t, agg, 1, 0, "", false)
			},
		},
		{
			name: "negative numeric expression is a constant key, not an ordinal",
			sql:  "select count(*), sum(empno) from constraint_test.emp group by -1",
			check: func(t *testing.T, query *planpb.Query) {
				agg := constGroupKeyAggregateNode(t, query)
				require.Len(t, agg.GroupBy, 1)
				require.Nil(t, agg.GroupBy[0].GetCol(),
					"a negative numeric item is a constant expression and must never select projection position 1")
			},
		},
		{
			name: "null constant remains a real group",
			sql:  "select count(*) from constraint_test.emp group by null",
			check: func(t *testing.T, query *planpb.Query) {
				agg := constGroupKeyAggregateNode(t, query)
				requireConstantGroupKey(t, agg, 1, 0, "", true)
			},
		},
		{
			name: "constant scalar expression is a real group",
			sql:  "select count(*) from constraint_test.emp group by concat('fix','ed')",
			check: func(t *testing.T, query *planpb.Query) {
				agg := constGroupKeyAggregateNode(t, query)
				requireGroupKeyShape(t, agg, 1)
			},
		},
		{
			name: "alias to a string constant groups through the projection",
			sql:  "select 'fixed' as c, count(*) from constraint_test.emp group by c",
			check: func(t *testing.T, query *planpb.Query) {
				agg := constGroupKeyAggregateNode(t, query)
				requireConstantGroupKey(t, agg, 1, 0, "fixed", false)
			},
		},
		{
			name: "alias to a decimal constant groups through the projection",
			sql:  "select 1.5 as c, count(*) from constraint_test.emp group by c",
			check: func(t *testing.T, query *planpb.Query) {
				agg := constGroupKeyAggregateNode(t, query)
				requireGroupKeyShape(t, agg, 1)
				require.Nil(t, agg.GroupBy[0].GetCol(),
					"a decimal selected under an alias groups on a constant, not on a column")
			},
		},
		{
			name: "integer literal keeps positional-ordinal semantics",
			sql:  "select empno, count(*) from constraint_test.emp group by 1",
			check: func(t *testing.T, query *planpb.Query) {
				// The unique-key group-by elimination may legally remove the
				// AGG node; when it remains, the group key must be the
				// referenced projection column, not a constant.
				if agg := firstReachableNode(query, planpb.Node_AGG); agg != nil {
					require.NotEmpty(t, agg.GroupBy)
					require.NotNil(t, agg.GroupBy[0].GetCol())
				}
			},
		},
		{
			name:    "zero ordinal is rejected",
			sql:     "select count(*) from constraint_test.emp group by 0",
			wantErr: "GROUP BY position 0 is not in select list",
		},
		{
			name:    "out-of-range ordinal is rejected",
			sql:     "select count(*) from constraint_test.emp group by 99",
			wantErr: "GROUP BY position 99 is not in select list",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			logical, err := runOneStmt(optimizer, t, tc.sql)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			query := logical.GetQuery()
			require.NotNil(t, query)
			if tc.check != nil {
				tc.check(t, query)
			}
		})
	}
}

func TestOrderByConstantLiteralBindsAsSortKey(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		wantErr string
		check   func(*testing.T, *planpb.Query)
	}{
		{
			name: "string constant before a real column key",
			sql:  "select empno from constraint_test.emp order by 'fixed', empno",
			check: func(t *testing.T, query *planpb.Query) {
				requireTrailingOrderKeyColumn(t, query, "empno", 2)
			},
		},
		{
			name: "decimal constant before a real column key",
			sql:  "select empno from constraint_test.emp order by 1.5, empno",
			check: func(t *testing.T, query *planpb.Query) {
				requireTrailingOrderKeyColumn(t, query, "empno", 2)
			},
		},
		{
			name: "parenthesized decimal constant is the same sort key",
			sql:  "select empno from constraint_test.emp order by (1.5), empno",
			check: func(t *testing.T, query *planpb.Query) {
				requireTrailingOrderKeyColumn(t, query, "empno", 2)
			},
		},
		{
			name: "null constant before a real column key",
			sql:  "select empno from constraint_test.emp order by null, empno",
			check: func(t *testing.T, query *planpb.Query) {
				requireTrailingOrderKeyColumn(t, query, "empno", 2)
			},
		},
		{
			name: "negative numeric expression is not an ordinal before a real column key",
			sql:  "select empno from constraint_test.emp order by -1, empno",
			check: func(t *testing.T, query *planpb.Query) {
				requireTrailingOrderKeyColumn(t, query, "empno", 2)
			},
		},
		{
			name: "integer literal keeps positional-ordinal semantics",
			sql:  "select empno, ename from constraint_test.emp order by 1",
			check: func(t *testing.T, query *planpb.Query) {
				requireTrailingOrderKeyColumn(t, query, "empno", 1)
			},
		},
		{
			name: "distinct keeps the constant sort-key contract for string literals",
			sql:  "select distinct empno from constraint_test.emp order by 'fixed', empno",
			check: func(t *testing.T, query *planpb.Query) {
				requireTrailingOrderKeyColumn(t, query, "empno", 2)
			},
		},
		{
			name: "distinct keeps the existing constant-expression sort-key contract",
			sql:  "select distinct empno from constraint_test.emp order by concat('fix','ed'), empno",
			check: func(t *testing.T, query *planpb.Query) {
				requireTrailingOrderKeyColumn(t, query, "empno", 2)
			},
		},
		{
			name:    "zero ordinal is rejected",
			sql:     "select empno from constraint_test.emp order by 0",
			wantErr: "ORDER BY position 0 is not in select list",
		},
		{
			name:    "out-of-range ordinal is rejected",
			sql:     "select empno from constraint_test.emp order by 99",
			wantErr: "ORDER BY position 99 is not in select list",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			logical, err := runOneStmt(optimizer, t, tc.sql)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			query := logical.GetQuery()
			require.NotNil(t, query)
			if tc.check != nil {
				tc.check(t, query)
			}
		})
	}
}
