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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// checkExprInvolvesTags checks if an expression references any of the specified tags
func checkExprInvolvesTags(expr *plan.Expr, tagsMap map[int32]bool) bool {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		return tagsMap[e.Col.RelPos]
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			if checkExprInvolvesTags(arg, tagsMap) {
				return true
			}
		}
	case *plan.Expr_W:
		if checkExprInvolvesTags(e.W.WindowFunc, tagsMap) {
			return true
		}
		for _, order := range e.W.OrderBy {
			if checkExprInvolvesTags(order.Expr, tagsMap) {
				return true
			}
		}
	}
	return false
}

// migrateOnListConditions moves conditions involving specified tags from src to dst
func migrateOnListConditions(src *plan.Node, dst *plan.Node, tagsMap map[int32]bool) {
	var kept []*plan.Expr
	for _, cond := range src.OnList {
		if checkExprInvolvesTags(cond, tagsMap) {
			dst.OnList = append(dst.OnList, cond)
		} else {
			kept = append(kept, cond)
		}
	}
	src.OnList = kept
}

// getTableNameOrLabel returns table name from node's TableDef, or a label (A/B/C) if not available
func (builder *QueryBuilder) getTableNameOrLabel(nodeID int32, label string) string {
	node := builder.qry.Nodes[nodeID]
	if node.TableDef != nil && node.TableDef.Name != "" {
		return node.TableDef.Name
	}
	return label
}

// formatStatsInfo formats stats information for logging
func formatStatsInfo(stats *plan.Stats) string {
	if stats == nil {
		return "stats=nil"
	}
	return fmt.Sprintf("sel=%.4f,outcnt=%.2f,tablecnt=%.2f", stats.Selectivity, stats.Outcnt, stats.TableCnt)
}

// for A*(B*C), if C.sel>0.9 and B<C, change this to (A*B)*C
func (builder *QueryBuilder) applyAssociativeLawRule1(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for i, child := range node.Children {
			node.Children[i] = builder.applyAssociativeLawRule1(child)
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

	tagsC := builder.enumerateTags(NodeC.NodeId)
	// Migrate OnList: conditions involving C must move to outer join
	tagsCMap := make(map[int32]bool)
	for _, tag := range tagsC {
		tagsCMap[tag] = true
	}
	migrateOnListConditions(node, rightChild, tagsCMap)

	// Record table names and stats after migration
	tableNameA := builder.getTableNameOrLabel(node.Children[0], "A")
	tableNameB := builder.getTableNameOrLabel(NodeB.NodeId, "B")
	tableNameC := builder.getTableNameOrLabel(NodeC.NodeId, "C")
	statsInfo := fmt.Sprintf("rule1: A=%s(stats:%s) B=%s(stats:%s) C=%s(stats:%s)",
		tableNameA, formatStatsInfo(builder.qry.Nodes[node.Children[0]].Stats),
		tableNameB, formatStatsInfo(NodeB.Stats),
		tableNameC, formatStatsInfo(NodeC.Stats))
	builder.optimizationHistory = append(builder.optimizationHistory, statsInfo)

	rightChild.Children[0] = node.NodeId
	ReCalcNodeStats(rightChild.NodeId, builder, true, false, true)
	return rightChild.NodeId
}

// for （A*B)*C, if C.sel<0.5, change this to A*(B*C)
func (builder *QueryBuilder) applyAssociativeLawRule2(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for i, child := range node.Children {
			node.Children[i] = builder.applyAssociativeLawRule2(child)
		}
	}
	if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_INNER {
		return nodeID
	}
	leftChild := builder.qry.Nodes[node.Children[0]]
	if leftChild.NodeType != plan.Node_JOIN || leftChild.JoinType != plan.Node_INNER {
		return nodeID
	}
	NodeC := builder.qry.Nodes[node.Children[1]]
	if NodeC.Stats.Selectivity > 0.5 {
		return nodeID
	}
	NodeA := builder.qry.Nodes[leftChild.Children[0]]
	NodeB := builder.qry.Nodes[leftChild.Children[1]]
	node.Children[0] = NodeB.NodeId
	determineHashOnPK(node.NodeId, builder)
	if !node.Stats.HashmapStats.HashOnPK {
		// b join c must be hash on primary key, or we can not do this change
		node.Children[0] = leftChild.NodeId
		return node.NodeId
	}

	// Migrate OnList: conditions involving A must move to outer join
	tagsAMap := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(NodeA.NodeId) {
		tagsAMap[tag] = true
	}
	migrateOnListConditions(node, leftChild, tagsAMap)

	// Record table names and stats after migration
	tableNameA := builder.getTableNameOrLabel(NodeA.NodeId, "A")
	tableNameB := builder.getTableNameOrLabel(NodeB.NodeId, "B")
	tableNameC := builder.getTableNameOrLabel(NodeC.NodeId, "C")
	statsInfo := fmt.Sprintf("rule2: A=%s(stats:%s) B=%s(stats:%s) C=%s(stats:%s)",
		tableNameA, formatStatsInfo(NodeA.Stats),
		tableNameB, formatStatsInfo(NodeB.Stats),
		tableNameC, formatStatsInfo(NodeC.Stats))
	builder.optimizationHistory = append(builder.optimizationHistory, statsInfo)

	leftChild.Children[1] = node.NodeId
	ReCalcNodeStats(leftChild.NodeId, builder, true, false, true)
	return leftChild.NodeId
}

// for （A*B)*C, if A.outcnt>C.outcnt and C.sel<B.sel, change this to (A*C)*B
func (builder *QueryBuilder) applyAssociativeLawRule3(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for i, child := range node.Children {
			node.Children[i] = builder.applyAssociativeLawRule3(child)
		}
	}
	if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_INNER {
		return nodeID
	}
	leftChild := builder.qry.Nodes[node.Children[0]]
	if leftChild.NodeType != plan.Node_JOIN || leftChild.JoinType != plan.Node_INNER {
		return nodeID
	}
	NodeA := builder.qry.Nodes[leftChild.Children[0]]
	NodeB := builder.qry.Nodes[leftChild.Children[1]]
	NodeC := builder.qry.Nodes[node.Children[1]]
	if NodeA.Stats.Outcnt < NodeC.Stats.Outcnt || NodeC.Stats.Selectivity >= NodeB.Stats.Selectivity {
		return nodeID
	}

	node.Children[0] = NodeA.NodeId
	ratio, ok := getHashColsNDVRatio(node.NodeId, builder)
	newSel := NodeC.Stats.Selectivity / ratio
	if !ok || newSel >= NodeB.Stats.Selectivity {
		//new selectivity bigger than b.sel, can't do this change
		node.Children[0] = leftChild.NodeId
		return node.NodeId
	}

	// Migrate OnList:
	// - node: move conditions involving B to leftChild
	// - leftChild: move conditions involving C to node
	tagsBMap := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(NodeB.NodeId) {
		tagsBMap[tag] = true
	}
	tagsCMap := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(NodeC.NodeId) {
		tagsCMap[tag] = true
	}
	migrateOnListConditions(node, leftChild, tagsBMap)
	migrateOnListConditions(leftChild, node, tagsCMap)

	// Record table names and stats after migration
	tableNameA := builder.getTableNameOrLabel(NodeA.NodeId, "A")
	tableNameB := builder.getTableNameOrLabel(NodeB.NodeId, "B")
	tableNameC := builder.getTableNameOrLabel(NodeC.NodeId, "C")
	statsInfo := fmt.Sprintf("rule3: A=%s(stats:%s) B=%s(stats:%s) C=%s(stats:%s)",
		tableNameA, formatStatsInfo(NodeA.Stats),
		tableNameB, formatStatsInfo(NodeB.Stats),
		tableNameC, formatStatsInfo(NodeC.Stats))
	builder.optimizationHistory = append(builder.optimizationHistory, statsInfo)

	leftChild.Children[0] = node.NodeId
	ReCalcNodeStats(leftChild.NodeId, builder, true, false, true)
	return leftChild.NodeId
}

func (builder *QueryBuilder) applyAssociativeLaw(nodeID int32) int32 {
	if builder.optimizerHints != nil && builder.optimizerHints.joinOrdering != 0 {
		return nodeID
	}
	nodeID = builder.applyAssociativeLawRule1(nodeID)
	builder.determineBuildAndProbeSide(nodeID, true)
	nodeID = builder.applyAssociativeLawRule2(nodeID)
	builder.determineBuildAndProbeSide(nodeID, true)
	nodeID = builder.applyAssociativeLawRule3(nodeID)
	builder.determineBuildAndProbeSide(nodeID, true)
	return nodeID
}
