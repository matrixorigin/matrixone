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

package plan

import (
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
)

// annotatePartitionTopN runs after filter pushdown, while window references
// still use their global binding tags. Only the relevant window expressions
// are folded early; the normal optimizer still folds the complete plan later.
func (builder *QueryBuilder) annotatePartitionTopN(nodeID int32) {
	if builder.isPrepareStatement || builder.qry.StmtType != planpb.Query_SELECT {
		return
	}
	node := builder.qry.Nodes[nodeID]
	for _, childID := range node.Children {
		builder.annotatePartitionTopN(childID)
	}
	if node.NodeType != planpb.Node_WINDOW {
		return
	}
	if _, ok := builder.userWindowNodes[nodeID]; !ok {
		return
	}
	proc := builder.compCtx.GetProcess()
	rule.NewConstantFold(false).Apply(node, builder.qry, proc)
	rule.NewPartitionTopN(false).Apply(node, builder.qry, proc)
	if len(node.Children) == 1 {
		child := builder.qry.Nodes[node.Children[0]]
		if child.NodeType == planpb.Node_PARTITION && child.Limit != nil && child.PartitionByCount > 0 {
			builder.partitionTopNWindowNodes[nodeID] = struct{}{}
		}
	}
}
