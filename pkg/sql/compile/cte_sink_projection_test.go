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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// The child already projects [1,10,7]. Evaluate the actual compiler output
// before dispatch. The planner represents the narrowed publication schema as
// an explicit PROJECT; SINK metadata alone must never execute another projection.
func TestSharedCTESinkPublishesProjectedValues(t *testing.T) {
	for _, tc := range []struct {
		name            string
		positions       []int32
		want            []int64
		readers         int
		extraProjection bool
		statementType   plan.Query_StatementType
	}{
		{"middle-column", []int32{0, 2}, []int64{1, 7}, 2, true, plan.Query_SELECT},
		{"reordered", []int32{2, 0, 1}, []int64{7, 1, 10}, 2, true, plan.Query_SELECT},
		{"prefix", []int32{0, 1}, []int64{1, 10}, 2, true, plan.Query_SELECT},
		{"insert-plan-context", []int32{0, 2}, []int64{1, 7}, 2, true, plan.Query_INSERT},
		{"identity", []int32{0, 1, 2}, []int64{1, 10, 7}, 2, false, plan.Query_SELECT},
		{"empty-projection", nil, []int64{1, 10, 7}, 2, false, plan.Query_SELECT},
		{"single-reader-streaming", []int32{0, 2}, []int64{1, 7}, 1, true, plan.Query_SELECT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, q := sharedCTESinkProjectionFixture(t, tc.positions, tc.extraProjection)
			q.StmtType = tc.statementType
			for i := range tc.readers {
				require.NoError(t, c.compileSinkScan(q, int32(3+i)))
			}
			scopes, err := c.compilePlanScope(0, q.Steps[0], q.Nodes)
			require.NoError(t, err)
			t.Cleanup(func() { ReleaseScopes(scopes) })
			require.Len(t, scopes, 1)
			fanout := scopes[0].RootOp.(*dispatch.Dispatch)
			require.Equal(t, tc.readers > 1, fanout.MaterializedSource != nil)
			// Materialized producers end in a local merge; execute its producer
			// below the connector so this test needs no channels or goroutines.
			output := fanout.GetChildren(0)
			if tc.readers > 1 {
				require.Len(t, scopes[0].PreScopes, 1)
				output = scopes[0].PreScopes[0].RootOp.GetOperatorBase().GetChildren(0)
			}
			baseline := c.proc.Mp().CurrNB()
			t.Cleanup(func() {
				require.NoError(t, vm.HandleAllOp(output, func(_ vm.Operator, op vm.Operator) error { op.Free(c.proc, false, nil); return nil }))
				require.Equal(t, baseline, c.proc.Mp().CurrNB())
			})
			require.NoError(t, vm.Prepare(output, c.proc))
			result, err := vm.Exec(output, c.proc)
			require.NoError(t, err)
			require.NotNil(t, result.Batch)
			require.Equal(t, 1, result.Batch.RowCount())
			require.Len(t, result.Batch.Vecs, len(tc.want))
			for i, want := range tc.want {
				require.Equal(t, types.T_int64, result.Batch.Vecs[i].GetType().Oid)
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[i], 0))
			}
			_, hasProjection := output.(*projection.Projection)
			require.Equal(t, tc.extraProjection, hasProjection)
		})
	}
}

func sharedCTESinkProjectionFixture(t *testing.T, positions []int32, project bool) (*Compile, *plan.Query) {
	t.Helper()
	c := NewMockCompile(t)
	c.nodeRegs = make(map[[2]int32]*process.WaitRegister)
	c.stepRegs = make(map[int32][][2]int32)
	sinkProject := make([]*plan.Expr, 0, len(positions))
	for _, pos := range positions {
		sinkProject = append(sinkProject, plan2.GetColExpr(plan.Type{Id: int32(types.T_int64)}, 0, pos))
	}
	q := &plan.Query{Nodes: []*plan.Node{
		{NodeType: plan.Node_VALUE_SCAN},
		{NodeType: plan.Node_PROJECT, Children: []int32{0}, ProjectList: []*plan.Expr{
			plan2.MakePlan2Int64ConstExprWithType(1), plan2.MakePlan2Int64ConstExprWithType(10), plan2.MakePlan2Int64ConstExprWithType(7),
		}},
		{NodeType: plan.Node_SINK, Children: []int32{1}, ProjectList: sinkProject, ExtraOptions: materialized.CTESinkOption},
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}},
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}},
	}, Steps: []int32{2}}
	if project {
		q.Nodes = append(q.Nodes, &plan.Node{
			NodeType: plan.Node_PROJECT, Children: []int32{1}, ProjectList: sinkProject,
		})
		q.Nodes[2].Children[0] = int32(len(q.Nodes) - 1)
		q.Nodes[2].ProjectList = make([]*plan.Expr, len(sinkProject))
		for i, expr := range sinkProject {
			q.Nodes[2].ProjectList[i] = plan2.GetColExpr(expr.Typ, 0, int32(i))
		}
	}
	c.anal = &AnalyzeModule{qry: q}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
	t.Cleanup(func() {
		for _, source := range c.materializedSources {
			source.Close()
		}
	})
	return c, q
}

func TestSharedCTESinkProjectionPreservesChildError(t *testing.T) {
	c, q := sharedCTESinkProjectionFixture(t, []int32{0, 2}, true)
	// A deterministic error in the discarded middle output must still be
	// evaluated before the SINK narrows its schema.
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "cast", []*plan.Expr{
		plan2.MakePlan2StringConstExprWithType("not-an-integer"),
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}},
	})
	require.NoError(t, err)
	q.Nodes[1].ProjectList[1] = expr
	require.NoError(t, c.compileSinkScan(q, 3))
	scopes, err := c.compilePlanScope(0, q.Steps[0], q.Nodes)
	require.NoError(t, err)
	defer ReleaseScopes(scopes)
	output := scopes[0].RootOp.GetOperatorBase().GetChildren(0)
	defer func() {
		require.NoError(t, vm.HandleAllOp(output, func(_ vm.Operator, op vm.Operator) error { op.Free(c.proc, true, nil); return nil }))
	}()
	require.NoError(t, vm.Prepare(output, c.proc))
	_, err = vm.Exec(output, c.proc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not-an-integer")
}

func TestSharedCTESinkProjectionOrphanReleasesScopes(t *testing.T) {
	c, q := sharedCTESinkProjectionFixture(t, []int32{0, 2}, true)
	// compileSinkNode owns the new projection and the child even on rejection.
	// The reuse checker detects missing/double releases at test process exit.
	scopes, err := c.compilePlanScope(0, q.Steps[0], q.Nodes)
	require.Nil(t, scopes)
	require.ErrorContains(t, err, "no data receiver for sink node")
}

func TestSharedCTESinkProjectionExcludesLegacySinks(t *testing.T) {
	for _, kind := range []string{"ordinary", "recursive-sink", "recursive-cte", "shared-metadata"} {
		t.Run(kind, func(t *testing.T) {
			c, q := sharedCTESinkProjectionFixture(t, []int32{0, 2}, false)
			sink := q.Nodes[q.Steps[0]]
			switch kind {
			case "ordinary":
				sink.ExtraOptions = ""
			case "recursive-sink":
				sink.RecursiveSink = true
			case "recursive-cte":
				sink.RecursiveCte = true
			case "shared-metadata":
				// DML maintenance can append tagged SINK metadata after remap.
				// It must not be evaluated against the already-produced batch.
				sink.ProjectList[1].GetCol().ColPos = 3
			}
			require.NoError(t, c.compileSinkScan(q, 3))
			scopes, err := c.compilePlanScope(0, q.Steps[0], q.Nodes)
			require.NoError(t, err)
			defer ReleaseScopes(scopes)
			output := scopes[0].RootOp.GetOperatorBase().GetChildren(0)
			require.Equal(t, vm.ValueScan, output.OpType(), "legacy sink keeps its existing pass-through compilation")
		})
	}
}
