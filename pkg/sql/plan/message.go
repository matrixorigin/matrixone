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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

func (builder *QueryBuilder) gatherLeavesForMessageFromTopToScan(nodeID int32, orderExpr *plan.Expr, throughIndex *bool) (scanID int32, scanOrderExpr *plan.Expr, staticLimitSafe bool, shuffleJoins []int32) {
	node := builder.qry.Nodes[nodeID]
	switch node.NodeType {
	case plan.Node_PROJECT:
		if len(node.Children) != 1 || len(node.BindingTags) == 0 || node.Limit != nil || node.Offset != nil || len(node.FilterList) != 0 {
			return -1, nil, false, nil
		}
		orderCol := orderExpr.GetCol()
		if orderCol == nil || orderCol.RelPos != node.BindingTags[0] || orderCol.ColPos < 0 || int(orderCol.ColPos) >= len(node.ProjectList) {
			return -1, nil, false, nil
		}
		projectedExpr := node.ProjectList[orderCol.ColPos]
		if projectedExpr.GetCol() == nil {
			return -1, nil, false, nil
		}
		return builder.gatherLeavesForMessageFromTopToScan(node.Children[0], projectedExpr, throughIndex)
	case plan.Node_JOIN:
		if len(node.Children) != 2 || node.Limit != nil || node.Offset != nil {
			return -1, nil, false, nil
		}
		if node.JoinType == plan.Node_INDEX {
			scanID, scanOrderExpr, _, shuffleJoins = builder.gatherLeavesForMessageFromTopToScan(node.Children[0], orderExpr, throughIndex)
			if scanID == -1 {
				return -1, nil, false, nil
			}
			// Index join publishes only probe-side rows. The dynamic TopValue
			// threshold is safe, but a static reader limit is not.
			*throughIndex = true
			return scanID, scanOrderExpr, false, shuffleJoins
		}
		if node.JoinType == plan.Node_INNER || node.JoinType == plan.Node_SEMI {
			// for now, only support inner join and semi join.
			// for left join, top operator can directly push down over this
			scanID, scanOrderExpr, _, shuffleJoins = builder.gatherLeavesForMessageFromTopToScan(node.Children[0], orderExpr, throughIndex)
			if scanID == -1 {
				return -1, nil, false, nil
			}
			if node.Stats.HashmapStats.Shuffle {
				shuffleJoins = append(shuffleJoins, nodeID)
			}
			return scanID, scanOrderExpr, false, shuffleJoins
		}
	case plan.Node_FILTER:
		if len(node.Children) != 1 || node.Limit != nil || node.Offset != nil {
			return -1, nil, false, nil
		}
		scanID, scanOrderExpr, _, shuffleJoins = builder.gatherLeavesForMessageFromTopToScan(node.Children[0], orderExpr, throughIndex)
		return scanID, scanOrderExpr, false, shuffleJoins
	case plan.Node_TABLE_SCAN:
		if node.Limit != nil || node.Offset != nil {
			return -1, nil, false, nil
		}
		return nodeID, DeepCopyExpr(orderExpr), true, nil
	}
	return -1, nil, false, nil
}

// send message from top to scan. if block node(like group or window), no need to send this message
func (builder *QueryBuilder) handleMessageFromTopToScan(nodeID int32) {
	builder.handleMessageFromTopToScanInRoot(nodeID, nodeID)
}

func (builder *QueryBuilder) handleMessageFromTopToScanInRoot(nodeID, rootID int32) {
	if builder.optimizerHints != nil && builder.optimizerHints.sendMessageFromTopToScan != 0 {
		return
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			builder.handleMessageFromTopToScanInRoot(child, rootID)
		}
	}
	if node.NodeType != plan.Node_SORT {
		return
	}
	if node.Limit == nil {
		return
	}
	if len(node.OrderBy) == 0 {
		return
	}
	orderByCol := node.OrderBy[0].Expr.GetCol()
	if orderByCol == nil {
		return
	}
	throughIndex := false
	scanID, scanOrderExpr, staticLimitSafe, shuffleJoins := builder.gatherLeavesForMessageFromTopToScan(node.Children[0], node.OrderBy[0].Expr, &throughIndex)
	if scanID == -1 || scanOrderExpr == nil {
		return
	}
	if throughIndex && (len(shuffleJoins) != 0 || builder.topValueScanHasMultiplePaths(rootID, scanID)) {
		// A shuffled parent may own an RF consumed by the index scan. Do
		// not remove its build while leaving that probe waiting for a message.
		return
	}
	// Zonemaps do not describe NULL ordering. A block whose non-NULL values
	// are worse than the threshold may still contain the winning NULL.
	if !scanOrderExpr.Typ.NotNullable {
		return
	}
	if !encodedOrderMatchesSQLOrder(scanOrderExpr) {
		return
	}
	scanOrderByCol := scanOrderExpr.GetCol()
	if scanOrderByCol == nil {
		return
	}
	scanNode := builder.qry.Nodes[scanID]
	// Readers resolve qualified names by their last component. A literal dot
	// in the column name is ambiguous; keep the full scan in that case.
	colPos := int(scanOrderByCol.ColPos)
	if def := scanNode.TableDef; def != nil && colPos >= 0 && colPos < len(def.Cols) &&
		strings.Contains(def.Cols[colPos].Name, ".") {
		return
	}
	if !builder.regularIndexScanAllowedByOrderHints(scanNode) {
		return
	}
	if len(scanNode.OrderBy) != 0 {
		return
	}
	scanOrderBy := DeepCopyOrderBySpec(node.OrderBy[0])
	scanOrderBy.Expr = scanOrderExpr
	enableOrderedLimit := false
	// A leading-key block order is not a complete multi-key row order: retain
	// the original Top comparator and never install a static reader limit.
	if len(node.OrderBy) == 1 && canUseRegularIndexHiddenSortKey(scanNode, scanOrderByCol) {
		eligibleOrderedLimit := staticLimitSafe && node.Offset == nil && node.RankOption == nil &&
			isPositiveLiteralLimit(node.Limit)
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
		scanOrderByCol = hiddenKeyExpr.GetCol()

		scanHiddenKeyExpr := GetColExpr(scanNode.TableDef.Cols[0].Typ, scanNode.BindingTags[0], 0)
		scanHiddenKeyExpr.GetCol().Name = scanNode.TableDef.Cols[0].Name
		scanOrderBy = &plan.OrderBySpec{
			Expr: scanHiddenKeyExpr,
			Flag: node.OrderBy[0].Flag,
		}
	}
	if scanOrderByCol.RelPos != scanNode.BindingTags[0] {
		return
	}
	for _, joinID := range shuffleJoins {
		joinNode := builder.qry.Nodes[joinID]
		joinNode.Stats.HashmapStats.Shuffle = false
		joinNode.RuntimeFilterProbeList = nil
		joinNode.RuntimeFilterBuildList = nil
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

// A TopValue bound belongs to one consumer. Count reachable paths only for
// the new INDEX case so a shared probe scan cannot be pruned for another path.
func (builder *QueryBuilder) topValueScanHasMultiplePaths(rootID, scanID int32) bool {
	paths := make([]int8, len(builder.qry.Nodes))
	for i := range paths {
		paths[i] = -1
	}
	var count func(int32) int8
	count = func(id int32) int8 {
		if id < 0 || int(id) >= len(paths) || builder.qry.Nodes[id] == nil {
			return 0
		}
		if id == scanID {
			return 1
		}
		if paths[id] >= 0 {
			return paths[id]
		}
		paths[id] = 0
		for _, child := range builder.qry.Nodes[id].Children {
			paths[id] += count(child)
			if paths[id] >= 2 {
				paths[id] = 2
				break
			}
		}
		return paths[id]
	}
	total := count(rootID)
	rootSeen := false
	for _, step := range builder.qry.Steps {
		if step == rootID && !rootSeen {
			rootSeen = true
			continue
		}
		total += count(step)
		if total >= 2 {
			return true
		}
	}
	return total != 1
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
