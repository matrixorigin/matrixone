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

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCompileBroadcastAsofJoin(t *testing.T) {
	node := newShuffleJoinTestNode(4)
	node.JoinType = plan.Node_ASOF_LEFT
	node.AsofRightCol = 1
	node.Stats.HashmapStats.Shuffle = false
	node.Stats.HashmapStats.HashOnPK = true
	node.OnList = []*plan.Expr{
		makeMarkJoinTestCondition(t, "=", 0, true),
		makeMarkJoinTestCondition(t, ">=", 1, true),
	}
	colType := plan.Type{Id: int32(types.T_int64)}
	left := &plan.Node{ProjectList: []*plan.Expr{
		makeMarkJoinTestColumn(0, 0, true),
		{Typ: colType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
	}}
	right := &plan.Node{ProjectList: []*plan.Expr{
		makeMarkJoinTestColumn(1, 0, true),
		{Typ: colType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 1}}},
	}}
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 4}})
	probes := make([]*Scope, 4)
	for i := range probes {
		probes[i] = newShuffleJoinTestScope(t, c.cnList[0], 1)
	}

	result := c.compileProbeSideForBroadcastJoin(node, left, right, probes)
	require.Len(t, result, 4)
	for _, scope := range result {
		op, ok := scope.RootOp.(*hashjoin.HashJoin)
		require.True(t, ok)
		require.Equal(t, plan.Node_ASOF_LEFT, op.JoinType)
		require.False(t, op.HashOnPK, "ASOF must retain the full equality-key group")
		require.Equal(t, int32(1), op.AsofRightCol)
		require.NotNil(t, op.NonEqCond)
		require.Len(t, op.EqConds[0], 1)
	}
}

func TestCompileAsofBuildLeftForSmallLeftHugeRight(t *testing.T) {
	node := newShuffleJoinTestNode(4)
	node.JoinType = plan.Node_ASOF_LEFT
	node.AsofRightCol = 1
	node.Stats.HashmapStats.Shuffle = false
	node.OnList = []*plan.Expr{
		makeMarkJoinTestCondition(t, "=", 0, true),
		makeMarkJoinTestCondition(t, ">=", 1, true),
	}
	colType := plan.Type{Id: int32(types.T_int64)}
	left := &plan.Node{
		Stats: &plan.Stats{Outcnt: 2, Rowsize: 100},
		ProjectList: []*plan.Expr{
			makeMarkJoinTestColumn(0, 0, true),
			{Typ: colType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
		},
	}
	right := &plan.Node{
		Stats: &plan.Stats{Outcnt: 1_000_000_000, Rowsize: 1000},
		ProjectList: []*plan.Expr{
			makeMarkJoinTestColumn(1, 0, true),
			{Typ: colType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 1}}},
		},
	}
	require.True(t, shouldBuildLeftForAsof(node, left, right))

	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 4}})
	leftScopes := []*Scope{newShuffleJoinTestScope(t, c.cnList[0], 1)}
	rightScopes := make([]*Scope, 4)
	for i := range rightScopes {
		rightScopes[i] = newShuffleJoinTestScope(t, c.cnList[0], 1)
	}
	result := c.compileJoin(node, left, right, leftScopes, rightScopes)
	require.Len(t, result, 1)
	require.Equal(t, 1, result[0].NodeInfo.Mcpu,
		"independent right partitions must merge before one-time finalization")
	op, ok := result[0].RootOp.(*hashjoin.HashJoin)
	require.True(t, ok)
	require.True(t, op.AsofBuildLeft)
	require.Empty(t, op.RuntimeFilterSpecs)
	require.Equal(t, int32(1), op.AsofRightCol)
	require.NotEmpty(t, result[0].PreScopes)
	build, ok := result[0].PreScopes[len(result[0].PreScopes)-1].RootOp.(*hashbuild.HashBuild)
	require.True(t, ok)
	require.True(t, build.NeedBatches)
	require.Len(t, build.Conditions, 1)
}

func TestAsofBuildLeftCostBoundary(t *testing.T) {
	node := &plan.Node{
		JoinType: plan.Node_ASOF,
		Stats:    &plan.Stats{HashmapStats: &plan.HashMapStats{}},
	}
	tests := []struct {
		name      string
		leftRows  float64
		leftSize  float64
		rightRows float64
		rightSize float64
		want      bool
	}{
		{name: "review example", leftRows: 2, leftSize: 100, rightRows: 1_000_000_000, rightSize: 1000, want: true},
		{name: "bounded hot key amplification", leftRows: 64, leftSize: 100, rightRows: 1_000_000_000, rightSize: 1000, want: true},
		{name: "hot key exceeds amplification bound", leftRows: 65, leftSize: 100, rightRows: 1_000_000_000, rightSize: 1000},
		{name: "retained memory cannot hide excessive work amplification", leftRows: 10_000, leftSize: 100, rightRows: 1_000_000_000, rightSize: 1000},
		{name: "candidate retention changes choice", leftRows: 60, leftSize: 1000, rightRows: 100, rightSize: 1000},
		{name: "right is smaller", leftRows: 2, leftSize: 1000, rightRows: 1, rightSize: 100},
		{name: "unknown left", leftRows: 0, rightRows: 1_000_000_000, rightSize: 1000},
		{name: "unknown right", leftRows: 2, leftSize: 100, rightRows: 0},
		{name: "unknown left width", leftRows: 2, rightRows: 1_000_000_000, rightSize: 1000},
		{name: "unknown right width", leftRows: 2, leftSize: 100, rightRows: 1_000_000_000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left := &plan.Node{Stats: &plan.Stats{Outcnt: test.leftRows, Rowsize: test.leftSize}}
			right := &plan.Node{Stats: &plan.Stats{Outcnt: test.rightRows, Rowsize: test.rightSize}}
			require.Equal(t, test.want, shouldBuildLeftForAsof(node, left, right))
		})
	}
}

func TestCompileLocalShuffleAsofBuildLeftSwapsPhysicalSides(t *testing.T) {
	node := newShuffleJoinTestNode(4)
	node.JoinType = plan.Node_ASOF
	node.AsofRightCol = 1
	node.Stats.HashmapStats.Shuffle = true
	node.OnList = []*plan.Expr{
		makeMarkJoinTestCondition(t, "=", 0, true),
		makeMarkJoinTestCondition(t, ">=", 1, true),
	}
	node.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{{Tag: 42}}
	node.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{{Tag: 42}}
	colType := plan.Type{Id: int32(types.T_int64)}
	left := &plan.Node{
		Stats: &plan.Stats{Outcnt: 2, Rowsize: 100, Dop: 4},
		ProjectList: []*plan.Expr{
			makeMarkJoinTestColumn(0, 0, true),
			{Typ: colType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
		},
	}
	right := &plan.Node{
		Stats: &plan.Stats{Outcnt: 1_000_000_000, Rowsize: 1000, Dop: 4},
		ProjectList: []*plan.Expr{
			makeMarkJoinTestColumn(1, 0, true),
			{Typ: colType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 1}}},
		},
	}
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 4}})
	leftScope := newShuffleJoinTestScope(t, c.cnList[0], 4)
	rightScope := newShuffleJoinTestScope(t, c.cnList[0], 4)
	result := c.compileJoin(
		node, left, right, []*Scope{leftScope}, []*Scope{rightScope})
	require.Len(t, result, 1)
	require.Same(t, rightScope, result[0], "logical right must be the physical streaming probe")
	op, ok := result[0].RootOp.(*hashjoin.HashJoin)
	require.True(t, ok)
	require.True(t, op.AsofBuildLeft)
	require.True(t, op.IsShuffle)
	require.Len(t, op.RuntimeFilterSpecs, 1)
	probeShuffle, ok := op.GetChildren(0).(*shuffle.Shuffle)
	require.True(t, ok)
	require.Equal(t, int32(42), probeShuffle.RuntimeFilterSpec.GetTag(),
		"build completion must be awaited by the physical probe")
	require.NotEmpty(t, result[0].PreScopes)
	require.Same(t, leftScope, result[0].PreScopes[0],
		"logical left must own the per-bucket HashBuild")
	build, ok := leftScope.RootOp.(*hashbuild.HashBuild)
	require.True(t, ok)
	require.Equal(t, int32(42), build.RuntimeFilterSpec.GetTag())
	buildShuffle, ok := build.GetChildren(0).(*shuffle.Shuffle)
	require.True(t, ok)
	require.Nil(t, buildShuffle.RuntimeFilterSpec,
		"physical build must not wait for its own completion signal")
}

func TestCompileDistributedShuffleAsofBuildLeftMovesCompletionWait(t *testing.T) {
	const dop = int32(2)
	nodes := engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: int(dop)},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: int(dop)},
	}
	c := newCompileForShuffleJoinTest(t, nodes)
	c.execType = plan2.ExecTypeAP_MULTICN
	node := newShuffleJoinTestNode(dop)
	node.JoinType = plan.Node_ASOF_LEFT
	node.AsofRightCol = 1
	node.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
	node.OnList = []*plan.Expr{
		makeMarkJoinTestCondition(t, "=", 0, true),
		makeMarkJoinTestCondition(t, ">=", 1, true),
	}
	node.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{{Tag: 43}}
	node.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{{Tag: 43}}
	colType := plan.Type{Id: int32(types.T_int64)}
	left := &plan.Node{
		Stats: &plan.Stats{Outcnt: 2, Rowsize: 100, Dop: dop},
		ProjectList: []*plan.Expr{
			makeMarkJoinTestColumn(0, 0, true),
			{Typ: colType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
		},
	}
	right := &plan.Node{
		Stats: &plan.Stats{Outcnt: 1_000_000_000, Rowsize: 1000, Dop: dop},
		ProjectList: []*plan.Expr{
			makeMarkJoinTestColumn(1, 0, true),
			{Typ: colType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 1}}},
		},
	}
	leftScopes := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}
	rightScopes := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}
	result := c.compileJoin(node, left, right, leftScopes, rightScopes)
	require.Len(t, result, len(nodes)*int(dop))
	for _, scope := range result {
		op, ok := scope.RootOp.(*hashjoin.HashJoin)
		require.True(t, ok)
		require.True(t, op.AsofBuildLeft)
		require.Len(t, op.RuntimeFilterSpecs, 1)
		require.NotEmpty(t, scope.PreScopes)
		build, ok := scope.PreScopes[0].RootOp.(*hashbuild.HashBuild)
		require.True(t, ok)
		require.Equal(t, int32(43), build.RuntimeFilterSpec.GetTag())
	}
	for _, scope := range leftScopes {
		_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			if physicalBuildShuffle, ok := op.(*shuffle.Shuffle); ok {
				require.Nil(t, physicalBuildShuffle.RuntimeFilterSpec,
					"logical-left build stream must not wait for itself")
			}
			return nil
		})
	}
	for _, scope := range rightScopes {
		foundCompletionWait := false
		_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			if physicalProbeShuffle, ok := op.(*shuffle.Shuffle); ok &&
				physicalProbeShuffle.RuntimeFilterSpec.GetTag() == 43 {
				foundCompletionWait = true
			}
			return nil
		})
		require.True(t, foundCompletionWait,
			"logical-right probe stream must receive the build completion wait")
	}
}

func TestRemoteAsofJoinProtocolGate(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := runtime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
	arg := hashjoin.NewArgument()
	arg.JoinType = plan.Node_ASOF
	arg.EqConds = [][]*plan.Expr{{}, {}}
	defer arg.Release()
	for _, version := range []int64{
		defines.MORPCVersion17,
		defines.MORPCVersion20,
		defines.MORPCVersion22,
		defines.MORPCVersion23,
		defines.MORPCVersion24,
		defines.MORPCVersion25,
		defines.MORPCVersion26,
	} {
		rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
		_, _, err := convertToPipelineInstruction(arg, proc, &scopeContext{}, 1)
		require.ErrorContains(t, err, "protocol version 27")
	}
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion27)
	arg.JoinType = plan.Node_ASOF_LEFT
	arg.AsofBuildLeft = true
	_, instruction, err := convertToPipelineInstruction(arg, proc, &scopeContext{}, 1)
	require.NoError(t, err)
	require.True(t, instruction.GetHashJoin().GetAsofBuildLeft())
	restored, err := convertToVmOperator(instruction, &scopeContext{}, nil)
	require.NoError(t, err)
	restoredHashJoin, ok := restored.(*hashjoin.HashJoin)
	require.True(t, ok)
	require.True(t, restoredHashJoin.AsofBuildLeft)
	restoredHashJoin.Release()
	require.NoError(t, validateRemoteJoinProtocol(proc, plan.Node_ASOF_LEFT))
	require.NoError(t, validateRemoteJoinProtocol(proc, plan.Node_INNER))
}
