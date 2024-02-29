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

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (builder *QueryBuilder) gatherLeavesForMessageFromTopToScan(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]
	switch node.NodeType {
	case plan.Node_JOIN:
		if node.JoinType == plan.Node_INNER || node.JoinType == plan.Node_SEMI {
			// for now, only support inner join and semi join.
			// for left join, top operator can directly push down over this
			return builder.gatherLeavesForMessageFromTopToScan(node.Children[0])
		}
	case plan.Node_FILTER:
		return builder.gatherLeavesForMessageFromTopToScan(node.Children[0])
	case plan.Node_TABLE_SCAN:
		return nodeID
	}
	return -1
}

// send message from top to scan. if block node(like group or window), no need to send this message
func (builder *QueryBuilder) handleMessgaeFromTopToScan(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			builder.handleMessgaeFromTopToScan(child)
		}
	}
	if node.NodeType != plan.Node_SORT {
		return
	}
	if node.Limit == nil {
		return
	}
	if len(node.OrderBy) > 1 {
		// for now ,only support order by one column
		return
	}
	scanID := builder.gatherLeavesForMessageFromTopToScan(node.Children[0])
	if scanID == -1 {
		return
	}
	orderByCol, ok := node.OrderBy[0].Expr.Expr.(*plan.Expr_Col)
	if !ok {
		return
	}
	scanNode := builder.qry.Nodes[scanID]
	if len(scanNode.OrderBy) != 0 {
		panic("orderby in scannode should be nil!")
	}
	if orderByCol.Col.RelPos != scanNode.BindingTags[0] {
		return
	}
	if scanNode.Stats.BlockNum < 64 {
		return
	}
	if GetSortOrder(scanNode.TableDef, orderByCol.Col.ColPos) != 0 {
		return
	}

	msgTag := builder.genNewMsgTag()
	msgHeader := &plan.MsgHeader{MsgTag: msgTag, MsgType: int32(process.MsgTopValue)}
	node.SendMsgList = append(node.SendMsgList, msgHeader)
	scanNode.RecvMsgList = append(scanNode.RecvMsgList, msgHeader)
	scanNode.OrderBy = append(scanNode.OrderBy, DeepCopyOrderBy(node.OrderBy[0]))
}

func (builder *QueryBuilder) handleMessgaes(nodeID int32) {
	builder.handleMessgaeFromTopToScan(nodeID)
}
