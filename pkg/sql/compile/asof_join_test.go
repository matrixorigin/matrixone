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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCompileBroadcastAsofJoin(t *testing.T) {
	node := newShuffleJoinTestNode(1)
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
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}})
	probe := newShuffleJoinTestScope(t, c.cnList[0], 1)

	result := c.compileProbeSideForBroadcastJoin(node, left, right, []*Scope{probe})
	require.Len(t, result, 1)
	op, ok := result[0].RootOp.(*hashjoin.HashJoin)
	require.True(t, ok)
	require.Equal(t, plan.Node_ASOF_LEFT, op.JoinType)
	require.False(t, op.HashOnPK, "ASOF must retain the full equality-key group")
	require.Equal(t, int32(1), op.AsofRightCol)
	require.NotNil(t, op.NonEqCond)
	require.Len(t, op.EqConds[0], 1)
}
