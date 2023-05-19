// Copyright 2023 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func (builder *QueryBuilder) optimizeDistinctAgg(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	for _, childID := range node.Children {
		builder.optimizeDistinctAgg(childID)
	}

	if node.NodeType == plan.Node_AGG {
		if len(node.AggList) != 1 {
			return
		}

		aggFunc := node.AggList[0].GetF()
		if uint64(aggFunc.Func.Obj)&function.Distinct == 0 || (aggFunc.Func.ObjName != "count" && aggFunc.Func.ObjName != "sum") {
			return
		}

		oldGroupLen := len(node.GroupBy)
		oldGroupBy := node.GroupBy
		toCount := aggFunc.Args[0]

		newGroupTag := builder.genNewTag()
		newAggregateTag := builder.genNewTag()
		aggNodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_AGG,
			Children:    []int32{node.Children[0]},
			GroupBy:     append(oldGroupBy, toCount),
			BindingTags: []int32{newGroupTag, newAggregateTag},
		}, builder.ctxByNode[node.Children[0]])

		node.Children[0] = aggNodeID
		node.GroupBy = make([]*plan.Expr, oldGroupLen)
		for i := range node.GroupBy {
			node.GroupBy[i] = &plan.Expr{
				Typ: DeepCopyType(oldGroupBy[i].Typ),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: newGroupTag,
						ColPos: int32(i),
					},
				},
			}
		}

		aggFunc.Func.Obj &= function.DistinctMask
		aggFunc.Args[0] = &plan.Expr{
			Typ: DeepCopyType(toCount.Typ),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: newGroupTag,
					ColPos: int32(oldGroupLen),
				},
			},
		}
	}
}
