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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
	_, _, err := convertToPipelineInstruction(arg, proc, &scopeContext{}, 1)
	require.NoError(t, err)
	require.NoError(t, validateRemoteJoinProtocol(proc, plan.Node_ASOF_LEFT))
	require.NoError(t, validateRemoteJoinProtocol(proc, plan.Node_INNER))
}
