// Copyright 2022 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

func shouldAggPushDown(agg, join, leftChild *plan.Node) bool {
	//this is for debug , will change
	if leftChild.NodeType != plan.Node_TABLE_SCAN {
		return false
	}
	if leftChild.TableDef.Name != "lineorder" {
		return false
	}
	return true
}

func createNewAggNode(agg, join, leftChild *plan.Node, builder *QueryBuilder) {
	newAggList := DeepCopyExprList(agg.AggList)
	newGroupBy := DeepCopyExprList(agg.GroupBy)
	newNodeID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_AGG,
		Children:    []int32{leftChild.NodeId},
		GroupBy:     newGroupBy,
		AggList:     newAggList,
		BindingTags: agg.BindingTags,
	}, builder.ctxByNode[agg.NodeId])

	//set child pointer
	join.Children[0] = newNodeID
}

func (builder *QueryBuilder) agg_pushdown(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]

	if node.NodeType != plan.Node_AGG {
		if len(node.Children) > 0 {
			for i, child := range node.Children {
				node.Children[i] = builder.agg_pushdown(child)
			}
		}
		return nodeID
	}
	//current node is node_agg, child must be a join
	//for now ,only support inner join
	join := builder.qry.Nodes[node.Children[0]]
	if join.NodeType != plan.Node_JOIN || join.JoinType != plan.Node_INNER {
		return nodeID
	}

	leftChild := builder.qry.Nodes[join.Children[0]]

	if !shouldAggPushDown(node, join, leftChild) {
		return nodeID
	}

	//createNewAggNode(node, join, leftChild, builder)
	return nodeID
}
