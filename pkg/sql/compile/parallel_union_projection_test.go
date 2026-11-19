// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unionall"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func parallelUnionProjectionTestNodes(t *testing.T, c *Compile) []*plan.Node {
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	grouping, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "grouping", []*plan.Expr{col})
	require.NoError(t, err)
	null := &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	normalized, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "if", []*plan.Expr{grouping, null, col})
	require.NoError(t, err)
	nodes := []*plan.Node{
		{NodeType: plan.Node_VALUE_SCAN, ProjectList: []*plan.Expr{plan2.MakePlan2Int64ConstExprWithType(7)}},
		{NodeType: plan.Node_VALUE_SCAN, ProjectList: []*plan.Expr{plan2.MakePlan2Int64ConstExprWithType(8)}},
		{NodeType: plan.Node_UNION_ALL, Children: []int32{0, 1}, ProjectList: []*plan.Expr{col}},
		{NodeType: plan.Node_PROJECT, Children: []int32{2}, ProjectList: []*plan.Expr{normalized, grouping, col}},
	}
	for i, node := range nodes {
		node.NodeId = int32(i)
	}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: nodes, Steps: []int32{3}}}}
	return nodes
}

func TestParallelUnionProjectionPreservesPackedScope(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	c.execType = plan2.ExecTypeAP_ONECN
	c.addr = "cn-local:6001"
	c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 4}}
	c.e = newStubEngineForGenerateNodes("testdb", "t")
	var scopes []*Scope
	t.Cleanup(func() {
		for _, scope := range scopes {
			scope.FreeOperator(c)
			scope.release()
		}
		c.proc.Free()
		require.Zero(t, c.proc.Mp().CurrNB())
	})
	nodes := parallelUnionProjectionTestNodes(t, c)
	col := nodes[2].ProjectList[0]
	nodes[0] = &plan.Node{NodeType: plan.Node_TABLE_SCAN,
		ObjRef:   &plan.ObjectRef{SchemaName: "testdb", ObjName: "t"},
		TableDef: &plan.TableDef{Name: "t", Cols: []*plan.ColDef{{Name: "k", Typ: col.Typ}}},
		Stats:    &plan.Stats{Dop: 4}, ProjectList: []*plan.Expr{col}}
	aggCol := plan2.DeepCopyExpr(col)
	aggCol.GetCol().RelPos = -1
	nodes = append(nodes, &plan.Node{NodeType: plan.Node_AGG, NodeId: 4, Children: []int32{0},
		GroupBy: []*plan.Expr{col}, ProjectList: []*plan.Expr{aggCol},
		Stats: &plan.Stats{Dop: 4, HashmapStats: &plan.HashMapStats{
			Shuffle: true, ShuffleColIdx: 0, ShuffleType: plan.ShuffleType_Hash}}})
	nodes[2].Children[0] = 4
	c.pn.GetQuery().Nodes = nodes
	require.True(t, c.canCompileParallelUnionAllProjection(0, 3, nodes, false))
	var err error
	scopes, err = c.compilePlanScope(0, 3, nodes)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	root := scopes[0]
	require.IsType(t, &merge.Merge{}, root.RootOp)
	require.Len(t, root.PreScopes, 2)
	var firstProjection *projection.Projection
	for i, branch := range root.PreScopes {
		conn, ok := branch.RootOp.(*connector.Connector)
		require.True(t, ok)
		proj, ok := conn.GetChildren(0).(*projection.Projection)
		require.True(t, ok, "projection must precede the fan-in connector")
		require.Equal(t, nodes[3].ProjectList, proj.ProjectList)
		require.Equal(t, 3, proj.GetIdx())
		marker, ok := proj.GetChildren(0).(*unionall.UnionAll)
		require.True(t, ok)
		require.Equal(t, 2, marker.GetIdx())
		require.Zero(t, marker.SequentialBranches)
		require.Equal(t, branch.NodeInfo.Mcpu, root.Proc.Reg.MergeReceivers[i].NilBatchCnt)
		if i == 0 {
			firstProjection = proj
			require.Equal(t, 4, branch.NodeInfo.Mcpu)
			require.IsType(t, &group.Group{}, marker.GetChildren(0))
		} else {
			require.NotSame(t, firstProjection, proj, "each branch owns its projection state")
		}
	}
}

func TestParallelUnionProjectionGuards(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	t.Cleanup(c.proc.Free)
	for _, tc := range []struct {
		name  string
		outer bool
		edit  func(*Compile, []*plan.Node)
	}{
		{name: "TP", edit: func(c *Compile, _ []*plan.Node) { c.execType = plan2.ExecTypeTP }},
		{name: "outer streaming demand", outer: true},
		{name: "local limit", edit: func(_ *Compile, n []*plan.Node) { n[3].Limit = plan2.MakePlan2Uint64ConstExprWithType(1) }},
		{name: "union order", edit: func(_ *Compile, n []*plan.Node) { n[2].OrderBy = []*plan.OrderBySpec{{Expr: n[2].ProjectList[0]}} }},
		{name: "barrier", edit: func(_ *Compile, n []*plan.Node) { n[3].FilterIsBarrier = true }},
		{name: "expander", edit: func(_ *Compile, n []*plan.Node) { n[3].ExtraOptions = "grouping_set_expand:2" }},
		{name: "dependency", edit: func(_ *Compile, n []*plan.Node) { n[3].SourceStep = []int32{0} }},
		{name: "outside expression owner", edit: func(_ *Compile, n []*plan.Node) {
			n[3].BlockFilterList = []*plan.Expr{plan2.MakePlan2BoolConstExprWithType(true)}
		}},
		{name: "invalid column", edit: func(_ *Compile, n []*plan.Node) { n[3].ProjectList[2].GetCol().ColPos = 1 }},
		{name: "invalid child", edit: func(_ *Compile, n []*plan.Node) { n[2].Children[1] = 99 }},
		{name: "text IF", edit: func(_ *Compile, n []*plan.Node) {
			n[3].ProjectList[0].GetF().Args[0] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "not a number"}}}}
		}},
		{name: "fallible cast", edit: func(c *Compile, n []*plan.Node) {
			text := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "bad"}}}}
			target := &plan.Expr{Typ: n[2].ProjectList[0].Typ, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
			expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "cast", []*plan.Expr{text, target})
			require.NoError(t, err)
			n[3].ProjectList[0] = expr
		}},
		{name: "volatile", edit: func(_ *Compile, n []*plan.Node) {
			n[3].ProjectList[0] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "rand", Obj: function.EncodeOverloadID(function.RANDOM, 0)}}}}
		}},
		{name: "fallible literal carrier", edit: func(_ *Compile, n []*plan.Node) {
			n[3].ProjectList[0] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_array_float32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "bad"}}}}
		}},
		{name: "scalar result order", edit: func(_ *Compile, n []*plan.Node) {
			n[0] = &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{4}}
			n[1] = &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{5}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c.execType = plan2.ExecTypeAP_ONECN
			nodes := parallelUnionProjectionTestNodes(t, c)
			nodes = append(nodes, &plan.Node{NodeType: plan.Node_VALUE_SCAN}, &plan.Node{NodeType: plan.Node_VALUE_SCAN})
			c.pn.GetQuery().Nodes = nodes
			require.True(t, c.canCompileParallelUnionAllProjection(0, 3, nodes, false))
			if tc.edit != nil {
				tc.edit(c, nodes)
			}
			require.False(t, c.canCompileParallelUnionAllProjection(0, 3, nodes, tc.outer))
		})
	}
}

func TestParallelUnionProjectionPreservesGroupingRows(t *testing.T) {
	type row struct {
		value       int64
		null        bool
		group       int64
		rawGrouping bool
	}
	for _, parallel := range []bool{false, true} {
		c := newLazyUnionAllTestCompile(t)
		c.execType = plan2.ExecTypeAP_ONECN
		var root *Scope
		branches := make([]*Scope, 0, 2)
		t.Cleanup(func() {
			if root != nil {
				root.FreeOperator(c)
				root.release()
			} else {
				for _, branch := range branches {
					branch.FreeOperator(c)
					branch.release()
				}
			}
			c.proc.Free()
			require.Zero(t, c.proc.Mp().CurrNB())
		})
		nodes := parallelUnionProjectionTestNodes(t, c)
		input := batch.NewWithSize(1)
		inputOwned := true
		t.Cleanup(func() {
			if inputOwned {
				input.Clean(c.proc.Mp())
			}
		})
		input.Vecs[0] = testutil.MakeInt64Vector([]int64{7, 7, 0, 0}, []uint64{2}, c.proc.Mp())
		input.Vecs[0].GetGrouping().Add(3)
		input.SetRowCount(4)
		left := newLazyUnionAllLeaf(c, colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
		branches = append(branches, left)
		inputOwned = false
		right := newLazyUnionAllLeaf(c, colexec.NewMockOperator())
		branches = append(branches, right)
		for _, branch := range branches {
			branch.setRootOperator(constructUnionAll(nodes[2]))
		}
		if parallel {
			root = c.newMergeScope(c.compileProjection(nodes[3], branches))
		} else {
			root = c.compileProjection(nodes[3], []*Scope{c.newMergeScope(branches)})[0]
		}
		var got []row
		root.setRootOperator(output.NewArgument().WithFunc(func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
			if bat == nil {
				return nil
			}
			for i := 0; i < bat.RowCount(); i++ {
				r := row{null: bat.Vecs[0].IsNull(uint64(i)), group: vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], i), rawGrouping: bat.Vecs[2].GetGrouping().Contains(uint64(i))}
				if !r.null {
					r.value = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], i)
				}
				got = append(got, r)
			}
			return nil
		}))
		c.scopes = []*Scope{root}
		c.InitPipelineContextToExecuteQuery()
		require.NoError(t, root.MergeRun(c))
		require.ElementsMatch(t, []row{{value: 7}, {value: 7}, {null: true}, {null: true, group: 1, rawGrouping: true}}, got)
	}
}
