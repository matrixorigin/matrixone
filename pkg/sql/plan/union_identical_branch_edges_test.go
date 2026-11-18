// Copyright 2026 MatrixOne
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// These tests target the set-operation selection collector (getUnionSelects in
// utils.go) through its only real consumer: the parser -> BuildPlan ->
// query_builder buildUnion path. The invariant: a mixed UNION/INTERSECT/EXCEPT
// chain must be collected with every operand and every operator edge
// preserved, and only then may the existing INTERSECT-first reassociation
// rebuild precedence.
//
// The historical collector had two "identical textual UNION DISTINCT branch"
// early returns that dropped both the right operand and its UNION edge when a
// chain began with two identical UNION branches (both the SelectClause and the
// ParenSelect arm). MySQL evaluates INTERSECT before UNION/EXCEPT, so e.g.
// `select 1 union select 1 intersect select 2` means `1 union (1 intersect 2)`
// and its plan must keep the UNION node above the INTERSECT node. The oracle
// here is the set-operation graph the real planner built: operator counts per
// family, number of collected operand leaves, and union->intersect
// reachability. Row-set semantics for the same queries are covered by the
// public SQL verification phase; no row executor is involved here.

func buildSetOpChainQuery(t *testing.T, ctx *MockCompilerContext, sql string) *plan.Query {
	t.Helper()
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	p, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	require.NotNil(t, p.GetQuery(), "a set-operation SELECT must build into a query")
	return p.GetQuery()
}

func buildSetOpChainQueryAllowError(t *testing.T, ctx *MockCompilerContext, sql string) error {
	t.Helper()
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	if err != nil {
		return err
	}
	_, err = BuildPlan(ctx, stmt, false, nil)
	return err
}

func buildSetOpChainPrepareQuery(t *testing.T, ctx *MockCompilerContext, sql string) *plan.Query {
	t.Helper()
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	p, _, err := getPreparePlan(ctx, stmt, nil)
	require.NoError(t, err)
	require.NotNil(t, p.GetQuery(), "a prepared set-operation SELECT must build into a query")
	return p.GetQuery()
}

func isUnionFamilyNodeType(nt plan.Node_NodeType) bool {
	return nt == plan.Node_UNION || nt == plan.Node_UNION_ALL
}

func isIntersectFamilyNodeType(nt plan.Node_NodeType) bool {
	return nt == plan.Node_INTERSECT || nt == plan.Node_INTERSECT_ALL
}

func isMinusFamilyNodeType(nt plan.Node_NodeType) bool {
	return nt == plan.Node_MINUS || nt == plan.Node_MINUS_ALL
}

func isSetOpNodeType(nt plan.Node_NodeType) bool {
	return isUnionFamilyNodeType(nt) || isIntersectFamilyNodeType(nt) || isMinusFamilyNodeType(nt)
}

// setOpChainFacts summarizes the set-operation subgraph of a built plan.
// operandLeaves counts the non-set-operation children of set-operation nodes,
// which equals the number of collected operands; unionAboveIntersect records
// that some UNION-family node reaches an INTERSECT-family node through
// Children links, as the INTERSECT-first reassociation must produce.
type setOpChainFacts struct {
	opCount             int
	unionFamily         int
	intersectFamily     int
	minusFamily         int
	operandLeaves       int
	unionAboveIntersect bool
}

func analyzeSetOpChain(t *testing.T, q *plan.Query) *setOpChainFacts {
	t.Helper()
	require.NotNil(t, q)
	facts := &setOpChainFacts{}
	for _, node := range q.Nodes {
		nt := node.NodeType
		if !isSetOpNodeType(nt) {
			continue
		}
		facts.opCount++
		switch {
		case isUnionFamilyNodeType(nt):
			facts.unionFamily++
		case isIntersectFamilyNodeType(nt):
			facts.intersectFamily++
		case isMinusFamilyNodeType(nt):
			facts.minusFamily++
		}
		for _, childID := range node.Children {
			require.GreaterOrEqualf(t, childID, int32(0), "node children are slice indices")
			require.Lessf(t, childID, int32(len(q.Nodes)), "node children are slice indices")
			if !isSetOpNodeType(q.Nodes[childID].NodeType) {
				facts.operandLeaves++
			}
		}
	}
	reachesIntersect := func(start *plan.Node) bool {
		stack := []*plan.Node{start}
		for len(stack) > 0 {
			n := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			for _, childID := range n.Children {
				child := q.Nodes[childID]
				if isIntersectFamilyNodeType(child.NodeType) {
					return true
				}
				if isSetOpNodeType(child.NodeType) {
					stack = append(stack, child)
				}
			}
		}
		return false
	}
	for _, node := range q.Nodes {
		if isUnionFamilyNodeType(node.NodeType) && reachesIntersect(node) {
			facts.unionAboveIntersect = true
			break
		}
	}
	return facts
}

func requireSetOpChain(t *testing.T, q *plan.Query, wantUnion, wantIntersect, wantMinus, wantLeaves int) *setOpChainFacts {
	t.Helper()
	facts := analyzeSetOpChain(t, q)
	require.Equalf(t, wantUnion, facts.unionFamily, "UNION-family set-op node count")
	require.Equalf(t, wantIntersect, facts.intersectFamily, "INTERSECT-family set-op node count")
	require.Equalf(t, wantMinus, facts.minusFamily, "MINUS-family set-op node count")
	require.Equalf(t, wantLeaves, facts.operandLeaves, "collected operand leaves")
	return facts
}

func firstSetOpNodeOfFamily(q *plan.Query, match func(plan.Node_NodeType) bool) *plan.Node {
	for _, node := range q.Nodes {
		if match(node.NodeType) {
			return node
		}
	}
	return nil
}

// TestUnionSetOpChainPreservesIdenticalLeftBranchEdges covers the regression
// probes: every chain starting with two textually identical UNION DISTINCT
// branches must still collect and build all of its operands and edges.
func TestUnionSetOpChainPreservesIdenticalLeftBranchEdges(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(true, proc)

	for _, c := range []struct {
		name                                string
		sql                                 string
		wantUnion, wantIntersect, wantMinus int
		wantLeaves                          int
	}{
		{
			name:      "identical-select-clause-union-then-intersect",
			sql:       "select 1 union select 1 intersect select 2",
			wantUnion: 1, wantIntersect: 1, wantLeaves: 2 + 1,
		},
		{
			name:      "identical-paren-select-union-then-intersect",
			sql:       "select 1 union (select 1) intersect (select 2)",
			wantUnion: 1, wantIntersect: 1, wantLeaves: 2 + 1,
		},
		{
			name:      "identical-union-then-intersect-with-trailing-union",
			sql:       "select 1 union select 1 intersect select 2 union select 3",
			wantUnion: 2, wantIntersect: 1, wantLeaves: 3 + 1,
		},
		{
			name:      "identical-union-then-intersect-all",
			sql:       "select 1 union select 1 intersect all select 2",
			wantUnion: 1, wantIntersect: 1, wantLeaves: 2 + 1,
		},
		{
			name:      "identical-union-then-except",
			sql:       "select 1 union select 1 except select 2",
			wantUnion: 1, wantMinus: 1, wantLeaves: 2 + 1,
		},
		{
			name:      "identical-union-then-intersect-with-order-by-limit",
			sql:       "select 1 union select 1 intersect select 2 order by 1 limit 1",
			wantUnion: 1, wantIntersect: 1, wantLeaves: 2 + 1,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			query := buildSetOpChainQuery(t, ctx, c.sql)
			facts := requireSetOpChain(t, query, c.wantUnion, c.wantIntersect, c.wantMinus, c.wantLeaves)
			if c.wantIntersect > 0 {
				require.Truef(t, facts.unionAboveIntersect,
					"a kept UNION edge must sit above the reassociated INTERSECT edge")
			}
		})
	}

	t.Run("identical-union-then-intersect-with-trailing-union-all", func(t *testing.T) {
		query := buildSetOpChainQuery(t, ctx,
			"select 1 union select 1 intersect select 2 union all select 3")
		facts := requireSetOpChain(t, query, 2, 1, 0, 3+1)
		require.True(t, facts.unionAboveIntersect)
	})

	t.Run("identical-union-then-intersect-null-operand", func(t *testing.T) {
		query := buildSetOpChainQuery(t, ctx,
			"select null union select null intersect select 1")
		facts := requireSetOpChain(t, query, 1, 1, 0, 2+1)
		require.True(t, facts.unionAboveIntersect)
		unionNode := firstSetOpNodeOfFamily(query, isUnionFamilyNodeType)
		require.NotNil(t, unionNode)
		intersectNode := firstSetOpNodeOfFamily(query, isIntersectFamilyNodeType)
		require.NotNil(t, intersectNode)
		require.Equalf(t, intersectNode.ProjectList[0].Typ.Id, unionNode.ProjectList[0].Typ.Id,
			"a NULL operand must not change the common type across the kept UNION edge")
		require.Falsef(t, unionNode.ProjectList[0].Typ.NotNullable,
			"identical NULL branches must keep the UNION output type nullable")
	})

	t.Run("identical-union-then-intersect-string-operand", func(t *testing.T) {
		query := buildSetOpChainQuery(t, ctx,
			"select 'a' union select 'a' intersect select 'b'")
		facts := requireSetOpChain(t, query, 1, 1, 0, 2+1)
		require.True(t, facts.unionAboveIntersect)
		unionNode := firstSetOpNodeOfFamily(query, isUnionFamilyNodeType)
		require.NotNil(t, unionNode)
		require.Equal(t, int32(types.T_varchar), unionNode.ProjectList[0].Typ.Id,
			"string operands must keep the string output type across the kept UNION edge")
	})

	t.Run("identical-union-then-intersect-prepared", func(t *testing.T) {
		query := buildSetOpChainPrepareQuery(t, ctx,
			"select 1 union select 1 intersect select 2")
		facts := requireSetOpChain(t, query, 1, 1, 0, 2+1)
		require.True(t, facts.unionAboveIntersect)
	})

	t.Run("identical-union-then-intersect-prepared-param", func(t *testing.T) {
		query := buildSetOpChainPrepareQuery(t, ctx,
			"select 1 union select 1 intersect select ?")
		facts := requireSetOpChain(t, query, 1, 1, 0, 2+1)
		require.True(t, facts.unionAboveIntersect)
	})
}

// TestUnionSetOpChainControls covers the standard-correct literals from the
// issue thread and the set-operation boundaries that the collector must keep
// working: these hold both before and after the regression fix.
func TestUnionSetOpChainControls(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(true, proc)

	t.Run("reported-literal-1u2i2u3", func(t *testing.T) {
		// Correct {1,2,3}: 1 union (2 intersect 2) union 3.
		query := buildSetOpChainQuery(t, ctx,
			"select 1 union select 2 intersect select 2 union select 3")
		facts := requireSetOpChain(t, query, 2, 1, 0, 3+1)
		require.True(t, facts.unionAboveIntersect)
	})

	t.Run("reported-literal-1u-2i-2-with-operand-parens", func(t *testing.T) {
		// Correct {1,2}: only operand-level parentheses, whole-subexpression
		// parenthesization is covered separately below.
		query := buildSetOpChainQuery(t, ctx,
			"select 1 union (select 2) intersect (select 2)")
		facts := requireSetOpChain(t, query, 1, 1, 0, 2+1)
		require.True(t, facts.unionAboveIntersect)
	})

	t.Run("whole-union-parenthesis-stays-one-outer-operand", func(t *testing.T) {
		// Correct empty: (1 union 1) intersect 2 = {1} intersect {2}.
		// The parenthesized whole UNION is one outer operand for the outer
		// INTERSECT on every branch, so the outer intersect edge is the only
		// operator-level assertion that must hold both before and after the
		// fix; the nested shape inside the parentheses follows the inner
		// chain rules and is covered by the probes above.
		query := buildSetOpChainQuery(t, ctx,
			"(select 1 union select 1) intersect select 2")
		facts := analyzeSetOpChain(t, query)
		require.Equalf(t, 1, facts.intersectFamily,
			"the whole parenthesized UNION must stay one outer operand of one INTERSECT")
		require.Falsef(t, facts.unionAboveIntersect,
			"the outer INTERSECT owns the operand; the inner chain must not leak an outer union edge")
	})

	t.Run("plain-identical-union-distinct-still-builds", func(t *testing.T) {
		// Whether the identical pure UNION collapses to the single-operand
		// fallback or builds a distinct UNION node, the result must dedup to
		// one row; here only the build contract is asserted, the row-set is
		// covered by the public SQL verification.
		require.NoError(t, buildSetOpChainQueryAllowError(t, ctx, "select 1 union select 1"))
	})

	t.Run("ordinary-union-keeps-distinct-edge", func(t *testing.T) {
		query := buildSetOpChainQuery(t, ctx, "select 1 union select 2")
		facts := requireSetOpChain(t, query, 1, 0, 0, 2)
		require.False(t, facts.unionAboveIntersect)
	})

	t.Run("ordinary-union-all-keeps-edge", func(t *testing.T) {
		query := buildSetOpChainQuery(t, ctx, "select 1 union all select 2")
		requireSetOpChain(t, query, 1, 0, 0, 2)
	})

	t.Run("mixed-control-chain-with-order-by-limit", func(t *testing.T) {
		query := buildSetOpChainQuery(t, ctx,
			"select 1 union select 2 intersect select 2 union select 3 order by 1 desc limit 2")
		facts := requireSetOpChain(t, query, 2, 1, 0, 3+1)
		require.True(t, facts.unionAboveIntersect)
	})

	t.Run("prepared-control-chain", func(t *testing.T) {
		query := buildSetOpChainPrepareQuery(t, ctx,
			"select 1 union select 2 intersect select 2 union select 3")
		facts := requireSetOpChain(t, query, 2, 1, 0, 3+1)
		require.True(t, facts.unionAboveIntersect)
	})

	t.Run("prepared-param-control-chain", func(t *testing.T) {
		query := buildSetOpChainPrepareQuery(t, ctx,
			"select ? union select 2 intersect select 2")
		facts := requireSetOpChain(t, query, 1, 1, 0, 2+1)
		require.True(t, facts.unionAboveIntersect)
	})

	t.Run("column-count-mismatch-still-errors", func(t *testing.T) {
		require.Error(t, buildSetOpChainQueryAllowError(t, ctx,
			"select 1 union select 1 intersect select 2, 3"))
	})

	t.Run("syntax-error-still-rejected", func(t *testing.T) {
		require.Error(t, buildSetOpChainQueryAllowError(t, ctx,
			"select 1 union select 1 intersect"))
	})

	t.Run("same-session-reusable-after-error", func(t *testing.T) {
		require.Error(t, buildSetOpChainQueryAllowError(t, ctx,
			"select 1 union select 1 intersect select 2, 3"))
		query := buildSetOpChainQuery(t, ctx, "select 1 union select 2")
		requireSetOpChain(t, query, 1, 0, 0, 2)
	})
}
