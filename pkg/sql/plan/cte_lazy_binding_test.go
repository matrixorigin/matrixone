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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type cteViewTrackingContext struct {
	CompilerContext
	views []string
}

func (c *cteViewTrackingContext) GetViews() []string {
	return c.views
}

func (c *cteViewTrackingContext) SetViews(views []string) {
	c.views = append([]string(nil), views...)
}

type cteViewTrackingOptimizer struct {
	ctx CompilerContext
}

func (o *cteViewTrackingOptimizer) CurrentContext() CompilerContext {
	return o.ctx
}

func (o *cteViewTrackingOptimizer) Optimize(stmt tree.Statement) (*Query, error) {
	logicPlan, err := BuildPlan(o.ctx, stmt, false)
	if err != nil {
		return nil, err
	}
	return logicPlan.GetQuery(), nil
}

func collectGroupingFlags(query *Query, rootIDs ...int32) [][]bool {
	seen := make(map[int32]bool)
	groupingFlags := make([][]bool, 0)
	var visit func(int32)
	visit = func(nodeID int32) {
		if seen[nodeID] {
			return
		}
		seen[nodeID] = true
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_AGG {
			groupingFlags = append(groupingFlags, append([]bool(nil), node.GroupingFlag...))
		}
		for _, childID := range node.Children {
			visit(childID)
		}
	}
	for _, rootID := range rootIDs {
		visit(rootID)
	}
	return groupingFlags
}

func requireRepeatedCTEGroupingFlags(t *testing.T, logicPlan *Plan, expected [][]bool) {
	t.Helper()
	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	for _, node := range query.Nodes {
		if node.NodeType != planpb.Node_JOIN || len(node.Children) != 2 {
			continue
		}
		left := collectGroupingFlags(query, node.Children[0])
		right := collectGroupingFlags(query, node.Children[1])
		if len(left) == 0 || len(right) == 0 {
			continue
		}
		require.ElementsMatch(t, expected, left, "left CTE consumer grouping variants")
		require.ElementsMatch(t, expected, right, "right CTE consumer grouping variants")
		return
	}
	t.Fatal("expected a join with grouping-set CTE consumers on both sides")
}

func TestCTELazyBindingDeclarationScope(t *testing.T) {
	mock := NewMockOptimizer(false)

	t.Run("unused invalid body is not bound", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with bad as (select missing_column from nation)
			select 1`)
		require.NoError(t, err)
	})

	t.Run("referenced invalid body is bound", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with bad as (select missing_column from nation)
			select * from bad`)
		require.ErrorContains(t, err, "missing_column")
	})

	t.Run("body cannot capture declaring block FROM", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with qn as (select * from bvt_test2.t2 where t2.b = t3.a)
			select * from bvt_test2.t3 where exists (select * from qn)`)
		require.ErrorContains(t, err, "missing FROM-clause entry for table 't3'")
	})

	t.Run("body can correlate to an outer query block", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			select (
				with qn as (select t2.a * t1.a as a from cte_test.t1),
				     qn2 as (select 3 * a as b from qn)
				select * from qn2 limit 1
			)
			from bvt_test2.t2`)
		require.NoError(t, err)
	})
}

func TestCTELazyBindingRollupSingleExpansion(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with totals as (
			select n_regionkey, count(*) as n
			from nation
			group by n_regionkey with rollup
		)
		select * from totals`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.ElementsMatch(t, [][]bool{{true}, {false}}, collectGroupingFlags(query, query.Steps...))
}

func TestCTELazyBindingRepeatedGroupingSets(t *testing.T) {
	mock := NewMockOptimizer(false)

	t.Run("rollup keeps both variants for each reference", func(t *testing.T) {
		logicPlan, err := runOneStmt(mock, t, `
			with totals as (
				select n_regionkey, count(*) as n
				from nation
				group by n_regionkey with rollup
			)
			select *
			from totals a join totals b on a.n_regionkey = b.n_regionkey`)
		require.NoError(t, err)
		requireRepeatedCTEGroupingFlags(t, logicPlan, [][]bool{{true}, {false}})
	})

	t.Run("cube keeps all variants for each reference", func(t *testing.T) {
		logicPlan, err := runOneStmt(mock, t, `
			with totals as (
				select count(*) as n
				from nation
				group by cube(n_regionkey, n_nationkey)
			)
			select *
			from totals a join totals b on a.n = b.n`)
		require.NoError(t, err)
		requireRepeatedCTEGroupingFlags(t, logicPlan, [][]bool{
			{false, false},
			{true, false},
			{true, true},
			{false, true},
		})
	})
}

func TestCTELazyBindingVisibilityGuards(t *testing.T) {
	mock := NewMockOptimizer(false)

	t.Run("forward reference stays rejected", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with qn2 as (select a from qn),
			     qn as (select a from cte_test.t1)
			select * from qn2`)
		require.Error(t, err)
	})

	t.Run("self reference stays rejected", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with qn as (select * from qn)
			select * from qn`)
		require.ErrorContains(t, err, "recursive table must be referenced only once")
	})

	t.Run("recursive reference stays accepted", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with recursive c as (
				select a from cte_test.t1
				union all
				select a + 1 from c where a < 3
			)
			select * from c`)
		require.NoError(t, err)
	})
}

func TestCTELazyBindingKeepsRootContextOwnership(t *testing.T) {
	ctx := &cteViewTrackingContext{CompilerContext: NewMockCompilerContext(false)}
	mock := &cteViewTrackingOptimizer{ctx: ctx}

	_, err := runOneStmt(mock, t, `
		with qn as (select * from cte_test.v2)
		select * from qn`)
	require.NoError(t, err)
	require.Contains(t, ctx.views, "cte_test#v2")
}
