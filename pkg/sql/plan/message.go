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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// send message from top to scan. if block node(like group or window), no need to send this message
func (builder *QueryBuilder) handleMessgaeFromTopToScan(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]
	switch node.NodeType {
	case plan.Node_JOIN:
		if node.JoinType == plan.Node_INNER || node.JoinType == plan.Node_SEMI {
			// for now, only support inner join and semi join.
			// for left join, top operator can directly push down over this
			return builder.handleMessgaeFromTopToScan(node.Children[0])
		}
	case plan.Node_FILTER:
		return builder.handleMessgaeFromTopToScan(node.Children[0])
	case plan.Node_AGG, plan.Node_WINDOW, plan.Node_UNION, plan.Node_DISTINCT, plan.Node_PROJECT:
		return -1
	case plan.Node_TABLE_SCAN:
		return nodeID
	case plan.Node_SORT:
		if node.Limit == nil {
			return -1
		}
		if len(node.OrderBy) > 1 {
			// for now ,only support order by one column
			return -1
		}
		scanID := builder.handleMessgaeFromTopToScan(node.Children[0])
		if scanID == -1 {
			return -1
		}
		orderByCol, ok := node.OrderBy[0].Expr.Expr.(*plan.Expr_Col)
		if !ok {
			return -1
		}
		scanNode := builder.qry.Nodes[scanID]
		if orderByCol.Col.RelPos != scanNode.BindingTags[0] {
			return -1
		}
		var signed bool
		// for now, only support numeric value
		switch types.T(node.OrderBy[0].Expr.Typ.Id) {
		case types.T_int64, types.T_int32, types.T_int16, types.T_int8:
			signed = true
		case types.T_uint64, types.T_uint32, types.T_uint16, types.T_uint8:
			signed = false
		default:
			return -1
		}

		var msgType process.MsgType
		if node.OrderBy[0].Flag == plan.OrderBySpec_INTERNAL || node.OrderBy[0].Flag == plan.OrderBySpec_ASC {
			if signed {
				msgType = process.MsgMaxValueSigned
			} else {
				msgType = process.MsgMaxValueUnsigned
			}
		} else if node.OrderBy[0].Flag == plan.OrderBySpec_DESC {
			if signed {
				msgType = process.MsgMinValueSigned
			} else {
				msgType = process.MsgMinValueUnsigned
			}
		} else {
			return -1
		}

		msgTag := builder.genNewMsgTag()
		msgHeader := &plan.MsgHeader{MsgTag: msgTag, MsgType: int32(msgType)}
		node.SendMsgList = append(node.SendMsgList, msgHeader)
		scanNode.RecvMsgList = append(scanNode.RecvMsgList, msgHeader)
	}
	return -1
}

func (builder *QueryBuilder) handleMessgaes(nodeID int32) {
	builder.handleMessgaeFromTopToScan(nodeID)
}
