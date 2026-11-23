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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestUpdateRowNumberPriorityOrdersWithinPhysicalRow(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_UPDATE, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	ctx := NewBindContext(builder, nil)
	tag := builder.genNewBindTag()
	input := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		BindingTags: []int32{tag},
		ProjectList: []*plan.Expr{
			{Typ: plan.Type{Id: int32(types.T_Rowid)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: 0}}},
			{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: 1}}},
		},
	}
	inputID := builder.appendNode(input, ctx)
	_, _, _, err := builder.appendRowNumberDedupNodeWithPriority(ctx, inputID, input, tag, []int32{0}, 1)
	require.NoError(t, err)

	var partition, window *plan.Node
	for _, node := range builder.qry.Nodes {
		switch node.NodeType {
		case plan.Node_PARTITION:
			partition = node
		case plan.Node_WINDOW:
			window = node
		}
	}
	require.NotNil(t, partition)
	require.Len(t, partition.OrderBy, 1)
	require.Equal(t, int32(0), partition.OrderBy[0].Expr.GetCol().ColPos)
	require.NotNil(t, window)
	require.Len(t, window.WinSpecList, 1)
	spec := window.WinSpecList[0].GetW()
	require.NotNil(t, spec)
	require.Len(t, spec.PartitionBy, 1)
	require.Equal(t, int32(0), spec.PartitionBy[0].GetCol().ColPos)
	require.Len(t, spec.OrderBy, 1)
	require.Equal(t, int32(1), spec.OrderBy[0].Expr.GetCol().ColPos)
	require.NotZero(t, spec.OrderBy[0].Flag&plan.OrderBySpec_DESC)
}
