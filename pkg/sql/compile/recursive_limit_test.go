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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestRecursiveCTEZeroLimitRetainsReceivers(t *testing.T) {
	c := NewMockCompile(t)
	t.Cleanup(c.proc.Free)
	c.nodeRegs = make(map[[2]int32]*process.WaitRegister)
	c.stepRegs = make(map[int32][][2]int32)
	query := &plan.Query{Nodes: []*plan.Node{
		{NodeType: plan.Node_RECURSIVE_CTE, SourceStep: []int32{0, 1}, Limit: plan2.MakePlan2Uint64ConstExprWithType(0)},
		{NodeType: plan.Node_SINK},
		{NodeType: plan.Node_SINK, RecursiveCte: true},
	}, Steps: []int32{1, 2}}
	c.anal = &AnalyzeModule{qry: query}
	require.NoError(t, c.compileSinkScan(query, 0))
	scopes, err := c.compilePlanScope(2, 0, query.Nodes)
	require.NoError(t, err)
	t.Cleanup(func() { ReleaseScopes(scopes) })
	require.Len(t, scopes, 1)
	receivers := 0
	require.NoError(t, vm.HandleAllOp(scopes[0].RootOp, func(_ vm.Operator, op vm.Operator) error {
		if _, ok := op.(*merge.Merge); ok {
			receivers++
		}
		return nil
	}))
	require.Equal(t, 2, receivers, "zero limit must retain both seed and feedback receivers for cleanup")
	require.Len(t, scopes[0].Proc.Reg.MergeReceivers, 2)
	require.False(t, c.canUseLiteralLimitZeroFastPath(query.Nodes[0]))
	require.True(t, c.canUseLiteralLimitZeroFastPath(&plan.Node{NodeType: plan.Node_PROJECT, Limit: query.Nodes[0].Limit}))
}

func TestDerivedSortZeroLimitSkipsRecursiveProducers(t *testing.T) {
	const cte = "with recursive r(n) as (select 1 union all select n + 1 from r where n < 3) "
	for _, tc := range []struct {
		name string
		sql  string
		zero bool
	}{
		{"zero", "select * from (select n from r order by n) d limit 0", true},
		{"zero with offset", "select * from (select n from r order by n) d limit 0 offset 1", true},
		{"zero above inner page", "select * from (select n from r order by n limit 2 offset 1) d limit 0", true},
		{"nested projections", "select * from (select * from (select n from r order by n) d) e limit 0", true},
		{"positive control", "select * from (select n from r order by n) d limit 2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewMockCompile(t)
			t.Cleanup(c.proc.Free)
			optimizer := plan2.NewMockOptimizer(true, newPlanTestProcess(t))
			stmts, err := mysql.Parse(c.proc.Ctx, cte+tc.sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			logical, err := plan2.BuildPlan(optimizer.CurrentContext(), stmts[0], false, nil)
			require.NoError(t, err)
			query := logical.GetQuery()
			require.Greater(t, len(query.Steps), 1, "the public SQL must retain producer steps")
			c.pn, c.stmt = logical, stmts[0]
			c.anal = &AnalyzeModule{qry: query}
			rootID := query.Steps[query.MainOutputStep()]
			root := query.Nodes[rootID]
			if !tc.zero {
				require.Zero(t, c.firstStepToCompile(query), "positive demand must compile producers")
				require.False(t, c.canUseLiteralLimitZeroFastPath(root))
				return
			}
			require.Equal(t, len(query.Steps)-1, c.firstStepToCompile(query),
				"zero demand must omit the entire producer graph, not only the final consumer")
			require.True(t, c.canUseLiteralLimitZeroFastPath(root))
			scopes, err := c.compilePlanScope(int32(query.MainOutputStep()), rootID, query.Nodes)
			require.NoError(t, err)
			t.Cleanup(func() { ReleaseScopes(scopes) })
			require.Len(t, scopes, 1)
			require.Empty(t, scopes[0].PreScopes)
			require.Empty(t, scopes[0].Proc.Reg.MergeReceivers)
		})
	}
}

func TestRecursiveCTEConsumerOffset(t *testing.T) {
	c := NewMockCompile(t)
	t.Cleanup(c.proc.Free)
	c.nodeRegs = make(map[[2]int32]*process.WaitRegister)
	c.stepRegs = make(map[int32][][2]int32)
	query := &plan.Query{Nodes: []*plan.Node{
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}, Offset: plan2.MakePlan2Uint64ConstExprWithType(1)},
		{NodeType: plan.Node_SINK, RecursiveSink: true},
	}, Steps: []int32{1}}
	c.anal = &AnalyzeModule{qry: query}
	require.NoError(t, c.compileSinkScan(query, 0))
	scopes, err := c.compilePlanScope(1, 0, query.Nodes)
	require.NoError(t, err)
	t.Cleanup(func() { ReleaseScopes(scopes) })
	require.Len(t, scopes, 1)
	window, ok := scopes[0].RootOp.(*offset.Offset)
	require.True(t, ok, "SINK_SCAN must implement the result-only offset")
	require.Equal(t, uint64(1), window.OffsetExpr.GetLit().GetU64Val())
}
