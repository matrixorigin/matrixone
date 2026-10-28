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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	orderop "github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCompileSortRollupUsesOneCoordinatorAggregate(t *testing.T) {
	c := NewMockCompile(t)
	c.anal = &AnalyzeModule{isFirst: true}
	t.Cleanup(func() { c.proc.Free() })

	key := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int32)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	count := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: aggexec.AggIdOfCountStar},
		}},
	}
	nodes := []*planpb.Node{
		{NodeType: planpb.Node_VALUE_SCAN, ProjectList: []*planpb.Expr{key}},
		{NodeType: planpb.Node_SORT, Children: []int32{0},
			ProjectList: []*planpb.Expr{key},
			OrderBy:     []*planpb.OrderBySpec{{Expr: key}}},
		{NodeType: planpb.Node_AGG, Children: []int32{1},
			GroupBy: []*planpb.Expr{key}, AggList: []*planpb.Expr{count},
			ExtraOptions: plan2.EncodeSortRollupOption()},
	}

	scopes, err := c.compilePlanScope(0, 2, nodes)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	t.Cleanup(func() { ReleaseScopes(scopes) })

	groupCount := 0
	var sortRollup *group.Group
	require.NoError(t, vm.HandleAllOp(scopes[0].RootOp,
		func(_ vm.Operator, op vm.Operator) error {
			if candidate, ok := op.(*group.Group); ok {
				groupCount++
				if candidate.SortRollup {
					sortRollup = candidate
				}
			}
			return nil
		}))
	require.Equal(t, 1, groupCount)
	require.NotNil(t, sortRollup)
}

func TestCompileSortRollupCoordinatorRetainsAllOrderedInputs(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local-cn:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.anal = &AnalyzeModule{}
	t.Cleanup(c.proc.Free)

	key := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int32)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	count := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: aggexec.AggIdOfCountStar},
		}},
	}
	sortNode := &planpb.Node{
		NodeType:    planpb.Node_SORT,
		ProjectList: []*planpb.Expr{key},
		OrderBy:     []*planpb.OrderBySpec{{Expr: key}},
	}
	aggNode := &planpb.Node{
		NodeType:     planpb.Node_AGG,
		Children:     []int32{1},
		GroupBy:      []*planpb.Expr{key},
		AggList:      []*planpb.Expr{count},
		ExtraOptions: plan2.EncodeSortRollupOption(),
	}
	nodes := []*planpb.Node{
		{NodeType: planpb.Node_VALUE_SCAN, ProjectList: []*planpb.Expr{key}},
		sortNode,
		aggNode,
	}

	inputs := make([]*Scope, 2)
	for i, addr := range []string{"remote-cn-a:6001", "remote-cn-b:6001"} {
		input := newScope(Remote)
		input.NodeInfo = engine.Node{Addr: addr, Mcpu: 1}
		input.Proc = c.proc.NewNoContextChildProc(0)
		ordered := orderop.NewArgument()
		ordered.OrderBySpec = sortNode.OrderBy
		input.setRootOperator(ordered)
		inputs[i] = input
	}

	orderedInput := c.newMergeTopScope(sortNode, nil, inputs)
	t.Cleanup(func() { ReleaseScopes([]*Scope{orderedInput}) })
	require.Len(t, orderedInput.PreScopes, 2)
	require.Equal(t, vm.MergeTop, orderedInput.RootOp.OpType())
	for i, input := range orderedInput.PreScopes {
		require.Equal(t, vm.Connector, input.RootOp.OpType())
		require.True(t, orderedInput.Proc.Reg.MergeReceivers[i].OrderedStream)
	}

	compiled := c.compileTPGroup(aggNode, []*Scope{orderedInput}, nodes)
	require.Len(t, compiled, 1)
	require.Same(t, orderedInput, compiled[0])
	require.Equal(t, vm.Group, compiled[0].RootOp.OpType())
	rollup := compiled[0].RootOp.(*group.Group)
	require.True(t, rollup.SortRollup)
	require.Equal(t, vm.MergeTop, rollup.GetChildren(0).OpType())

	groupCount := 0
	require.NoError(t, vm.HandleAllOp(compiled[0].RootOp,
		func(_ vm.Operator, op vm.Operator) error {
			if _, ok := op.(*group.Group); ok {
				groupCount++
			}
			return nil
		}))
	require.Equal(t, 1, groupCount)
}

func TestCompileSortRollupAfterCompileOrderUsesSingleMergeOrder(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local-cn:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.anal = &AnalyzeModule{}
	t.Cleanup(c.proc.Free)

	key := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int32)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	count := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: aggexec.AggIdOfCountStar},
		}},
	}
	sortNode := &planpb.Node{
		NodeType:    planpb.Node_SORT,
		ProjectList: []*planpb.Expr{key},
		OrderBy:     []*planpb.OrderBySpec{{Expr: key}},
	}
	aggNode := &planpb.Node{
		NodeType:     planpb.Node_AGG,
		Children:     []int32{1},
		GroupBy:      []*planpb.Expr{key},
		AggList:      []*planpb.Expr{count},
		ExtraOptions: plan2.EncodeSortRollupOption(),
	}
	nodes := []*planpb.Node{
		{NodeType: planpb.Node_VALUE_SCAN, ProjectList: []*planpb.Expr{key}},
		sortNode,
		aggNode,
	}

	inputs := make([]*Scope, 2)
	for i, addr := range []string{"remote-cn-a:6001", "remote-cn-b:6001"} {
		input := newScope(Remote)
		input.NodeInfo = engine.Node{Addr: addr, Mcpu: 1}
		input.Proc = c.proc.NewNoContextChildProc(0)
		ordered := orderop.NewArgument()
		ordered.OrderBySpec = sortNode.OrderBy
		input.setRootOperator(ordered)
		inputs[i] = input
	}

	orderedScopes := c.compileOrder(sortNode, inputs)
	require.Len(t, orderedScopes, 1)
	orderedInput := orderedScopes[0]
	t.Cleanup(func() { ReleaseScopes(orderedScopes) })
	require.Equal(t, vm.MergeOrder, orderedInput.RootOp.OpType())
	require.Len(t, orderedInput.PreScopes, 2)

	compiled := c.compileTPGroup(aggNode, orderedScopes, nodes)
	require.Len(t, compiled, 1)
	rollup := compiled[0].RootOp.(*group.Group)
	require.True(t, rollup.SortRollup)
	require.Equal(t, vm.MergeOrder, rollup.GetChildren(0).OpType())

	groupCount := 0
	require.NoError(t, vm.HandleAllOp(compiled[0].RootOp,
		func(_ vm.Operator, op vm.Operator) error {
			if _, ok := op.(*group.Group); ok {
				groupCount++
			}
			return nil
		}))
	require.Equal(t, 1, groupCount)
}
