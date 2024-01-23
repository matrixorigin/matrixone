// Copyright 2022 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// for A*(B*C), if C.sel>0.9 and B<C, change this to (A*B)*C
func (builder *QueryBuilder) applyAssociativeLaw(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for i, child := range node.Children {
			node.Children[i] = builder.applyAssociativeLaw(child)
		}
	}
	if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_INNER {
		return nodeID
	}

	rightChild := builder.qry.Nodes[node.Children[1]]
	if rightChild.NodeType != plan.Node_JOIN || rightChild.JoinType != plan.Node_INNER {
		return nodeID
	}
	NodeB := builder.qry.Nodes[rightChild.Children[0]]
	NodeC := builder.qry.Nodes[rightChild.Children[1]]

	if NodeC.Stats.Selectivity < 0.9 || NodeB.Stats.Outcnt >= NodeC.Stats.Outcnt {
		return nodeID
	}

	node.Children[1] = NodeB.NodeId

	determineHashOnPK(node.NodeId, builder)
	if !node.Stats.HashmapStats.HashOnPK {
		// a join b must be hash on primary key, or we can not do this change
		node.Children[1] = rightChild.NodeId
		return node.NodeId
	}

	rightChild.Children[0] = node.NodeId
	ReCalcNodeStats(rightChild.NodeId, builder, true, false, true)
	return rightChild.NodeId
}
