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
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

func (builder *QueryBuilder) gatherLeavesForMessageFromTopToScan(nodeID int32) (scanID int32, staticLimitSafe bool) {
	node := builder.qry.Nodes[nodeID]
	switch node.NodeType {
	case plan.Node_JOIN:
		if node.JoinType == plan.Node_INNER || node.JoinType == plan.Node_SEMI {
			// for now, only support inner join and semi join.
			// for left join, top operator can directly push down over this
			if node.Stats.HashmapStats.Shuffle {
				// don't need to go shuffle join for this
				node.Stats.HashmapStats.Shuffle = false
				node.RuntimeFilterProbeList = nil
				node.RuntimeFilterBuildList = nil
			}
			scanID, _ = builder.gatherLeavesForMessageFromTopToScan(node.Children[0])
			return scanID, false
		}
	case plan.Node_FILTER:
		scanID, _ = builder.gatherLeavesForMessageFromTopToScan(node.Children[0])
		return scanID, false
	case plan.Node_TABLE_SCAN:
		return nodeID, true
	}
	return -1, false
}

// send message from top to scan. if block node(like group or window), no need to send this message
func (builder *QueryBuilder) handleMessageFromTopToScan(nodeID int32) {
	if builder.optimizerHints != nil && builder.optimizerHints.sendMessageFromTopToScan != 0 {
		return
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			builder.handleMessageFromTopToScan(child)
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
	scanID, staticLimitSafe := builder.gatherLeavesForMessageFromTopToScan(node.Children[0])
	if scanID == -1 {
		return
	}
	orderByCol := node.OrderBy[0].Expr.GetCol()
	if orderByCol == nil {
		return
	}
	scanNode := builder.qry.Nodes[scanID]
	if len(scanNode.OrderBy) != 0 {
		return
	}
	scanOrderBy := DeepCopyOrderBySpec(node.OrderBy[0])
	enableOrderedLimit := false
	if canUseRegularIndexHiddenSortKey(scanNode, orderByCol) {
		eligibleOrderedLimit := staticLimitSafe && node.Offset == nil && node.RankOption == nil && isPositiveLiteralLimit(node.Limit)
		if eligibleOrderedLimit {
			enableOrderedLimit = canPushRegularIndexOrderedLimit(scanNode)
			if !enableOrderedLimit {
				enableOrderedLimit = builder.rewriteRegularIndexCursorRangeFilter(scanNode)
			}
		}
		orderByName := orderByCol.Name
		hiddenKeyExpr := GetColExpr(scanNode.TableDef.Cols[0].Typ, scanNode.BindingTags[0], 0)
		hiddenKeyExpr.GetCol().Name = orderByName
		node.OrderBy[0].Expr = hiddenKeyExpr
		orderByCol = hiddenKeyExpr.GetCol()

		scanHiddenKeyExpr := GetColExpr(scanNode.TableDef.Cols[0].Typ, scanNode.BindingTags[0], 0)
		scanHiddenKeyExpr.GetCol().Name = scanNode.TableDef.Cols[0].Name
		scanOrderBy = &plan.OrderBySpec{
			Expr: scanHiddenKeyExpr,
			Flag: node.OrderBy[0].Flag,
		}
	}
	if orderByCol.RelPos != scanNode.BindingTags[0] {
		return
	}

	msgTag := builder.genNewMsgTag()
	msgHeader := plan.MsgHeader{MsgTag: msgTag, MsgType: int32(message.MsgTopValue)}
	node.SendMsgList = append(node.SendMsgList, msgHeader)
	scanNode.RecvMsgList = append(scanNode.RecvMsgList, msgHeader)
	scanNode.OrderBy = append(scanNode.OrderBy, scanOrderBy)
	if enableOrderedLimit {
		applyRegularIndexOrderedLimitParam(scanNode, scanOrderBy, node.Limit)
	}
}

func (builder *QueryBuilder) handleHashMapMessages(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			builder.handleHashMapMessages(child)
		}
	}
	if node.NodeType != plan.Node_JOIN {
		return
	}
	//index join and non equal join don't need to send hashmap
	if node.JoinType == plan.Node_INDEX {
		return
	}

	msgTag := builder.genNewMsgTag()
	node.SendMsgList = append(node.SendMsgList, plan.MsgHeader{
		MsgTag:  msgTag,
		MsgType: int32(message.MsgJoinMap),
	})
}

func (builder *QueryBuilder) handleMessages(nodeID int32) {
	builder.handleMessageFromTopToScan(nodeID)
	builder.handleHashMapMessages(nodeID)
}
