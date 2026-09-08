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

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type joinEdge struct {
	leftCols  []int32
	rightCols []int32
}

type joinVertex struct {
	node                *plan.Node
	children            map[int32]bool
	parent              int32
	selectivityOnParent float64
	joined              bool
}

func (builder *QueryBuilder) pushdownSemiAntiJoins(nodeID int32) int32 {
	if builder.optimizerHints != nil && builder.optimizerHints.pushDownSemiAntiJoins != 0 {
		return nodeID
	}
	// TODO: handle SEMI/ANTI joins in join order
	node := builder.qry.Nodes[nodeID]

	for i, childID := range node.Children {
		node.Children[i] = builder.pushdownSemiAntiJoins(childID)
	}

	if node.NodeType != plan.Node_JOIN || (node.JoinType != plan.Node_SEMI && node.JoinType != plan.Node_ANTI) {
		return nodeID
	}

	var targetNode *plan.Node
	var targetSide int32

	joinNode := builder.qry.Nodes[node.Children[0]]

	semiAntiStat := builder.qry.Nodes[node.Children[1]].Stats
	semiAntiSelectivity := semiAntiStat.Selectivity
	if activeSelectivity, ok := builder.getJoinActiveDomainSelectivity(node.NodeId); ok {
		semiAntiSelectivity = activeSelectivity
	}

	for {
		if joinNode.NodeType != plan.Node_JOIN {
			break
		}

		leftTags := make(map[int32]bool)
		for _, tag := range builder.enumerateTags(joinNode.Children[0]) {
			leftTags[tag] = true
		}

		rightTags := make(map[int32]bool)
		for _, tag := range builder.enumerateTags(joinNode.Children[1]) {
			rightTags[tag] = true
		}

		var joinSide int8
		for _, cond := range node.OnList {
			joinSide |= getJoinSide(cond, leftTags, rightTags, 0)
		}

		// TODO: This logic is problematic. Use this threshold right now just for TPC-H
		ratio := 2.0
		if joinNode.JoinType == plan.Node_SEMI || joinNode.JoinType == plan.Node_ANTI {
			ratio = 1.0
		}

		if joinSide == JoinSideLeft {
			siblingSelectivity := builder.qry.Nodes[joinNode.Children[1]].Stats.Selectivity
			if activeSelectivity, ok := builder.getJoinActiveDomainSelectivity(joinNode.NodeId); ok {
				siblingSelectivity = activeSelectivity
			}
			if semiAntiSelectivity*ratio > siblingSelectivity {
				break
			}
			targetNode = joinNode
			targetSide = 0
			joinNode = builder.qry.Nodes[joinNode.Children[0]]
		} else if joinNode.JoinType == plan.Node_INNER && joinSide == JoinSideRight {
			siblingSelectivity := builder.qry.Nodes[joinNode.Children[0]].Stats.Selectivity
			if activeSelectivity, ok := builder.getJoinActiveDomainSelectivityOnSide(joinNode.NodeId, 0); ok {
				siblingSelectivity = activeSelectivity
			}
			if semiAntiSelectivity*ratio > siblingSelectivity {
				break
			}
			targetNode = joinNode
			targetSide = 1
			joinNode = builder.qry.Nodes[joinNode.Children[1]]
		} else {
			break
		}
	}

	if targetNode != nil {
		nodeID = node.Children[0]
		node.Children[0] = targetNode.Children[targetSide]
		targetNode.Children[targetSide] = node.NodeId
	}

	return nodeID
}

func (builder *QueryBuilder) IsEquiJoin(node *plan.Node) bool {
	if node.NodeType != plan.Node_JOIN {
		return false
	}

	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}

	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
	}

	for _, expr := range node.OnList {
		if equi := isEquiCond(expr, leftTags, rightTags); equi {
			return true
		}
	}
	return false
}

func isEquiCond(expr *plan.Expr, leftTags, rightTags map[int32]bool) bool {
	if e, ok := expr.Expr.(*plan.Expr_F); ok {
		if !IsEqualFunc(e.F.Func.GetObj()) {
			return false
		}

		lside, rside := getJoinSide(e.F.Args[0], leftTags, rightTags, 0), getJoinSide(e.F.Args[1], leftTags, rightTags, 0)
		if lside == JoinSideLeft && rside == JoinSideRight {
			return true
		} else if lside == JoinSideRight && rside == JoinSideLeft {
			// swap to make sure left and right is in order
			e.F.Args[0], e.F.Args[1] = e.F.Args[1], e.F.Args[0]
			return true
		}
	}

	return false
}

// IsEquiJoin2 Judge whether a join node is equi-join (after column remapping)
// Can only be used after optimizer!!!
func IsEquiJoin2(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if isEquiCond2(expr) {
			return true
		}
	}
	return false
}

func isEquiCond2(expr *plan.Expr) bool {
	e, ok := expr.Expr.(*plan.Expr_F)
	if !ok || !IsEqualFunc(e.F.Func.GetObj()) {
		return false
	}
	lpos, rpos := HasColExpr(e.F.Args[0], -1), HasColExpr(e.F.Args[1], -1)
	if lpos == 0 && rpos == 1 {
		return true
	}
	if lpos == 1 && rpos == 0 {
		// Keep the executor contract identical before and after remapping: the
		// probe/left expression is always argument 0.
		e.F.Args[0], e.F.Args[1] = e.F.Args[1], e.F.Args[0]
		return true
	}
	return false
}

func IsEqualFunc(id int64) bool {
	fid, _ := function.DecodeOverloadID(id)
	return fid == function.EQUAL
}

func HasColExpr(expr *plan.Expr, pos int32) int32 {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if pos == -1 {
			return e.Col.RelPos
		}
		if pos != e.Col.RelPos {
			return -1
		}
		return pos
	case *plan.Expr_F:
		for i := range e.F.Args {
			pos0 := HasColExpr(e.F.Args[i], pos)
			switch {
			case pos0 == -1:
			case pos == -1:
				pos = pos0
			case pos != pos0:
				return -1
			}
		}
		return pos
	default:
		return pos
	}
}

func (builder *QueryBuilder) determineJoinOrder(nodeID int32) int32 {
	originalNodeID := nodeID
	if builder.optimizerHints != nil && builder.optimizerHints.joinOrdering != 0 {
		return nodeID
	}
	node := builder.qry.Nodes[nodeID]

	if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_INNER {
		if len(node.Children) > 0 {
			for i, child := range node.Children {
				node.Children[i] = builder.determineJoinOrder(child)
			}
		}
		return nodeID
	}

	if builder.qry.Nodes[node.Children[1]].NodeType == plan.Node_FUNCTION_SCAN {
		return nodeID
	}

	leaves, conds := builder.gatherJoinLeavesAndConds(node, nil, nil)
	// Record middle: gathered leaves and conditions
	builder.optimizationHistory = append(builder.optimizationHistory,
		fmt.Sprintf("determineJoinOrder:middle (nodeID: %d, leaves: %d, conds: %d)", nodeID, len(leaves), len(conds)))
	newConds := deduceNewOnList(conds)
	conds = append(conds, newConds...)
	vertices := builder.getJoinGraph(leaves, conds)

	subTrees := make([]*plan.Node, 0, len(leaves))
	for i, vertex := range vertices {
		// TODO handle cycles in the "dimension -> fact" DAG
		if vertex.parent == -1 {
			builder.buildSubJoinTree(vertices, int32(i))
			subTrees = append(subTrees, vertex.node)
		}
	}
	for _, vertex := range vertices {
		if !vertex.joined {
			subTrees = append(subTrees, vertex.node)
		}
	}

	slices.SortFunc(subTrees, func(a, b *plan.Node) int {
		return compareStats(a.Stats, b.Stats)
	})

	leafByTag := make(map[int32]int32)

	for i, leaf := range subTrees {
		tags := builder.enumerateTags(leaf.NodeId)

		for _, tag := range tags {
			leafByTag[tag] = int32(i)
		}
	}

	nLeaf := int32(len(subTrees))

	adjMat := make([]bool, nLeaf*nLeaf)
	firstConnected := nLeaf
	visited := make([]bool, nLeaf)

	for _, cond := range conds {
		hyperEdge := make(map[int32]bool)
		getHyperEdgeFromExpr(cond, leafByTag, hyperEdge)

		for i := range hyperEdge {
			if i < firstConnected {
				firstConnected = i
			}
			for j := range hyperEdge {
				adjMat[int32(nLeaf)*i+j] = true
			}
		}
	}

	if firstConnected < nLeaf {
		nodeID = subTrees[firstConnected].NodeId
		visited[firstConnected] = true

		eligible := adjMat[firstConnected*nLeaf : (firstConnected+1)*nLeaf]

		for {
			nextSibling := nLeaf
			for i := range eligible {
				if !visited[i] && eligible[i] {
					nextSibling = int32(i)
					break
				}
			}

			if nextSibling == nLeaf {
				break
			}

			visited[nextSibling] = true

			children := []int32{nodeID, subTrees[nextSibling].NodeId}
			if builder.isIndexTableWithoutFilters(children[1]) {
				children[0], children[1] = children[1], children[0]
			}
			nodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: children,
				JoinType: plan.Node_INNER,
				SpillMem: builder.joinSpillMem,
			}, nil)

			for i, adj := range adjMat[nextSibling*nLeaf : (nextSibling+1)*nLeaf] {
				eligible[i] = eligible[i] || adj
			}
		}

		for i := range visited {
			if !visited[i] {
				children := []int32{nodeID, subTrees[i].NodeId}
				if builder.isIndexTableWithoutFilters(children[1]) {
					children[0], children[1] = children[1], children[0]
				}
				nodeID = builder.appendNode(&plan.Node{
					NodeType: plan.Node_JOIN,
					Children: children,
					JoinType: plan.Node_INNER,
					SpillMem: builder.joinSpillMem,
				}, nil)
			}
		}
	} else {
		newNode := subTrees[0]
		nodeID = newNode.NodeId

		for i := 1; i < len(subTrees); i++ {
			children := []int32{nodeID, subTrees[i].NodeId}
			if builder.isIndexTableWithoutFilters(children[1]) {
				children[0], children[1] = children[1], children[0]
			}
			nodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: children,
				JoinType: plan.Node_INNER,
				SpillMem: builder.joinSpillMem,
			}, nil)
		}
	}

	nodeID, conds = builder.pushdownFilters(nodeID, conds, true)
	if len(conds) > 0 {
		nodeID = builder.appendNode(&plan.Node{
			NodeType:   plan.Node_FILTER,
			Children:   []int32{nodeID},
			FilterList: conds,
		}, nil)
	}
	// Record after determineJoinOrder
	if nodeID != originalNodeID {
		builder.optimizationHistory = append(builder.optimizationHistory,
			fmt.Sprintf("determineJoinOrder:after (nodeID: %d -> %d, remainingConds: %d)", originalNodeID, nodeID, len(conds)))
	} else {
		builder.optimizationHistory = append(builder.optimizationHistory,
			fmt.Sprintf("determineJoinOrder:after (nodeID: %d, no change, remainingConds: %d)", nodeID, len(conds)))
	}
	return nodeID
}

func (builder *QueryBuilder) isIndexTableWithoutFilters(nodeId int32) bool {
	node := builder.qry.Nodes[nodeId]
	if node.NodeType != plan.Node_TABLE_SCAN || node.TableDef == nil {
		return false
	}
	if len(node.FilterList) > 0 {
		return false
	}
	return strings.HasPrefix(node.TableDef.Name, catalog.PrefixIndexTableName)
}

func (builder *QueryBuilder) gatherJoinLeavesAndConds(joinNode *plan.Node, leaves []*plan.Node, conds []*plan.Expr) ([]*plan.Node, []*plan.Expr) {
	if joinNode.NodeType != plan.Node_JOIN || joinNode.JoinType != plan.Node_INNER || joinNode.Limit != nil {
		nodeID := builder.determineJoinOrder(joinNode.NodeId)
		leaves = append(leaves, builder.qry.Nodes[nodeID])
		return leaves, conds
	}

	for _, childID := range joinNode.Children {
		leaves, conds = builder.gatherJoinLeavesAndConds(builder.qry.Nodes[childID], leaves, conds)
	}

	conds = append(conds, joinNode.OnList...)

	return leaves, conds
}

func (builder *QueryBuilder) getJoinGraph(leaves []*plan.Node, conds []*plan.Expr) []*joinVertex {
	vertices := make([]*joinVertex, len(leaves))
	tag2Vert := make(map[int32]int32)

	for i, node := range leaves {
		vertices[i] = &joinVertex{
			node:                node,
			children:            make(map[int32]bool),
			parent:              -1,
			selectivityOnParent: -1,
		}

		for _, tag := range builder.enumerateTags(node.NodeId) {
			tag2Vert[tag] = int32(i)
		}
	}

	edgeMap := make(map[[2]int32]*joinEdge)

	for i := 0; i < 2; i++ {
		for _, cond := range conds {
			ok, leftCol, rightCol := checkStrictJoinPred(cond)
			if !ok {
				continue
			}
			var leftId, rightId int32
			if leftId, ok = tag2Vert[leftCol.RelPos]; !ok {
				continue
			}
			if rightId, ok = tag2Vert[rightCol.RelPos]; !ok {
				continue
			}

			if leftId > rightId {
				leftId, rightId = rightId, leftId
				leftCol, rightCol = rightCol, leftCol
			}

			edge := edgeMap[[2]int32{leftId, rightId}]
			if i == 0 {
				if edge == nil {
					edge = &joinEdge{}
				}
				edge.leftCols = append(edge.leftCols, leftCol.ColPos)
				edge.rightCols = append(edge.rightCols, rightCol.ColPos)
				edgeMap[[2]int32{leftId, rightId}] = edge
			}

			leftParent := vertices[leftId].parent
			if isHighNdvCols(edge.leftCols, builder.tag2Table[leftCol.RelPos], builder) {
				if leftParent == -1 || shouldChangeParent(leftId, leftParent, rightId, vertices) {
					if vertices[rightId].parent != leftId {
						setParent(leftId, rightId, vertices)
						builder.setSelectivityOnParent(
							leftId, rightId, edge.leftCols, edge.rightCols,
							builder.tag2Table[leftCol.RelPos],
							builder.tag2Table[rightCol.RelPos], vertices)
					} else if vertices[leftId].node.Stats.Outcnt < vertices[rightId].node.Stats.Outcnt {
						unsetParent(rightId, leftId, vertices)
						setParent(leftId, rightId, vertices)
						builder.setSelectivityOnParent(
							leftId, rightId, edge.leftCols, edge.rightCols,
							builder.tag2Table[leftCol.RelPos],
							builder.tag2Table[rightCol.RelPos], vertices)
					}
				}
			}
			rightParent := vertices[rightId].parent
			if isHighNdvCols(edge.rightCols, builder.tag2Table[rightCol.RelPos], builder) {
				if rightParent == -1 || shouldChangeParent(rightId, rightParent, leftId, vertices) {
					if vertices[leftId].parent != rightId {
						setParent(rightId, leftId, vertices)
						builder.setSelectivityOnParent(
							rightId, leftId, edge.rightCols, edge.leftCols,
							builder.tag2Table[rightCol.RelPos],
							builder.tag2Table[leftCol.RelPos], vertices)
					} else if vertices[rightId].node.Stats.Outcnt < vertices[leftId].node.Stats.Outcnt {
						unsetParent(leftId, rightId, vertices)
						setParent(rightId, leftId, vertices)
						builder.setSelectivityOnParent(
							rightId, leftId, edge.rightCols, edge.leftCols,
							builder.tag2Table[rightCol.RelPos],
							builder.tag2Table[leftCol.RelPos], vertices)
					}
				}
			}
		}
	}
	return vertices
}

func setParent(child, parent int32, vertices []*joinVertex) {
	if child == -1 || parent == -1 || child == parent {
		return
	}
	if findParent(parent, child, vertices) {
		return
	}
	unsetParent(child, vertices[child].parent, vertices)
	vertices[child].parent = parent
	vertices[child].selectivityOnParent = -1
	vertices[parent].children[child] = true
}

func unsetParent(child, parent int32, vertices []*joinVertex) {
	if child == -1 || parent == -1 {
		return
	}
	if vertices[child].parent == parent {
		vertices[child].parent = -1
		vertices[child].selectivityOnParent = -1
		delete(vertices[parent].children, child)
	}
}

func findSelectivityInChildren(self int32, vertices []*joinVertex) bool {
	return findSelectivityInChildrenWithVisited(self, vertices, make([]bool, len(vertices)))
}

func findSelectivityInChildrenWithVisited(self int32, vertices []*joinVertex, visited []bool) bool {
	if !validVertex(self, vertices) {
		return false
	}
	if visited[self] {
		return false
	}
	visited[self] = true
	if vertices[self].node.Stats.Selectivity < 0.9 {
		return true
	}
	for child := range vertices[self].children {
		if findSelectivityInChildrenWithVisited(child, vertices, visited) {
			return true
		}
	}
	return false
}

func findParent(self, target int32, vertices []*joinVertex) bool {
	visited := make([]bool, len(vertices))
	for self != -1 {
		if !validVertex(self, vertices) {
			return false
		}
		if visited[self] {
			return false
		}
		visited[self] = true

		parent := vertices[self].parent
		if parent == target {
			return true
		}
		self = parent
	}
	return false
}

func validVertex(id int32, vertices []*joinVertex) bool {
	return id >= 0 && int(id) < len(vertices)
}

func shouldChangeParent(self, currentParent, nextParent int32, vertices []*joinVertex) bool {
	selfStats := vertices[self].node.Stats
	currentParentStats := vertices[currentParent].node.Stats
	nextParentStats := vertices[nextParent].node.Stats
	if currentParentStats.Cost > selfStats.Cost && currentParentStats.Cost > nextParentStats.Cost {
		// current Parent is the biggest node
		if findParent(nextParent, currentParent, vertices) {
			return true
		}
		if findSelectivityInChildren(self, vertices) {
			return false
		}
	}
	if nextParentStats.Cost > selfStats.Cost && nextParentStats.Cost > currentParentStats.Cost {
		// next Parent is the biggest node
		if findParent(currentParent, nextParent, vertices) {
			return false
		}
		if findSelectivityInChildren(self, vertices) {
			return true
		}
	}
	// self is the biggest node
	return compareStats(nextParentStats, currentParentStats) < 0
}

// buildSubJoinTree build sub- join tree for a fact table and all its dimension tables
func (builder *QueryBuilder) buildSubJoinTree(vertices []*joinVertex, vid int32) {
	vertex := vertices[vid]
	vertex.joined = true

	if len(vertex.children) == 0 {
		return
	}

	dimensions := make([]*joinVertex, 0, len(vertex.children))
	for child := range vertex.children {
		if vertices[child].joined {
			continue
		}
		builder.buildSubJoinTree(vertices, child)
		dimensions = append(dimensions, vertices[child])
	}
	slices.SortFunc(dimensions, compareJoinVertexStats)

	for _, child := range dimensions {

		children := []int32{vertex.node.NodeId, child.node.NodeId}
		nodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: children,
			JoinType: plan.Node_INNER,
			SpillMem: builder.joinSpillMem,
		}, nil)

		vertex.node = builder.qry.Nodes[nodeID]
	}
}

func (builder *QueryBuilder) setSelectivityOnParent(
	child, parent int32,
	childCols, parentCols []int32,
	childTable, parentTable *plan.TableDef,
	vertices []*joinVertex,
) {
	if !validVertex(child, vertices) || vertices[child].parent != parent ||
		!validVertex(parent, vertices) || vertices[child].node == nil ||
		vertices[child].node.Stats == nil || vertices[parent].node == nil ||
		vertices[parent].node.Stats == nil ||
		!builder.hasSingleTableBinding(vertices[child].node.NodeId) {
		return
	}
	parentNDV, ok := builder.getColsNDV(parentCols, parentTable)
	if !ok || parentNDV <= 0 {
		return
	}
	childNDV, ok := builder.getColsNDV(childCols, childTable)
	if !ok || childNDV <= 0 {
		return
	}
	// A high-NDV dimension join key is unique or close to unique. Its filtered
	// row count therefore approximates the number of parent key values retained.
	// Divide by the parent key's active NDV, not by the dimension table's full
	// row count: a date dimension can span centuries while a fact table covers
	// only a few years.
	activeDomain := min(parentNDV, childNDV, vertices[parent].node.Stats.Outcnt)
	if activeDomain <= 0 {
		return
	}
	vertices[child].selectivityOnParent = clampSelectivity(
		vertices[child].node.Stats.Outcnt/activeDomain, 1)
}

func (builder *QueryBuilder) hasSingleTableBinding(nodeID int32) bool {
	if builder == nil || builder.qry == nil || nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return false
	}
	tags := make(map[int32]struct{})
	for _, tag := range builder.enumerateTags(nodeID) {
		if builder.tag2Table[tag] != nil {
			tags[tag] = struct{}{}
		}
	}
	return len(tags) == 1
}

func (builder *QueryBuilder) getColsNDV(cols []int32, tableDef *plan.TableDef) (float64, bool) {
	if tableDef == nil || len(cols) == 0 {
		return 0, false
	}
	w := builder.getStatsInfoByTableID(tableDef.TblId)
	if w == nil || w.GetStats() == nil {
		return 0, false
	}
	stats := w.GetStats()
	if stats.TableCnt <= 0 || math.IsNaN(stats.TableCnt) || math.IsInf(stats.TableCnt, 0) {
		return 0, false
	}
	ndv := 1.0
	for _, colPos := range cols {
		if colPos < 0 || int(colPos) >= len(tableDef.Cols) || tableDef.Cols[colPos] == nil {
			return 0, false
		}
		columnNDV, exists := stats.NdvMap[tableDef.Cols[colPos].Name]
		if !exists || columnNDV <= 0 || math.IsNaN(columnNDV) || math.IsInf(columnNDV, 0) {
			return 0, false
		}
		if ndv > stats.TableCnt/columnNDV {
			ndv = stats.TableCnt
			break
		}
		ndv *= columnNDV
	}
	return min(ndv, stats.TableCnt), true
}

// getJoinActiveDomainSelectivity estimates how much the right side of a join
// filters the left side when the right join key is unique or nearly unique.
// The denominator is the key domain that is actually present on both sides,
// rather than the right table's full historical row count.
func (builder *QueryBuilder) getJoinActiveDomainSelectivity(nodeID int32) (float64, bool) {
	return builder.getJoinActiveDomainSelectivityOnSide(nodeID, 1)
}

func (builder *QueryBuilder) getJoinActiveDomainSelectivityOnSide(
	nodeID int32,
	dimensionSide int,
) (float64, bool) {
	if builder == nil || builder.qry == nil ||
		nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return 0, false
	}
	node := builder.qry.Nodes[nodeID]
	if node == nil || node.NodeType != plan.Node_JOIN || len(node.Children) != 2 ||
		(dimensionSide != 0 && dimensionSide != 1) {
		return 0, false
	}
	switch node.JoinType {
	case plan.Node_INNER, plan.Node_SEMI, plan.Node_ANTI:
	default:
		return 0, false
	}
	if node.Children[0] < 0 || int(node.Children[0]) >= len(builder.qry.Nodes) ||
		node.Children[1] < 0 || int(node.Children[1]) >= len(builder.qry.Nodes) ||
		!builder.hasSingleTableBinding(node.Children[dimensionSide]) {
		return 0, false
	}
	parent := builder.qry.Nodes[node.Children[1-dimensionSide]]
	dimension := builder.qry.Nodes[node.Children[dimensionSide]]
	if parent == nil || dimension == nil || parent.Stats == nil || dimension.Stats == nil {
		return 0, false
	}

	parentTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(parent.NodeId) {
		parentTags[tag] = true
	}
	dimensionTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(dimension.NodeId) {
		dimensionTags[tag] = true
	}

	var parentTable, dimensionTable *plan.TableDef
	parentCols := make([]int32, 0, len(node.OnList))
	dimensionCols := make([]int32, 0, len(node.OnList))
	seen := make(map[[4]int32]struct{})
	for _, condition := range node.OnList {
		ok, first, second := checkStrictJoinPred(condition)
		if !ok {
			continue
		}
		var parentCol, dimensionCol *plan.ColRef
		switch {
		case parentTags[first.RelPos] && dimensionTags[second.RelPos]:
			parentCol, dimensionCol = first, second
		case parentTags[second.RelPos] && dimensionTags[first.RelPos]:
			parentCol, dimensionCol = second, first
		default:
			continue
		}
		conditionParentTable := builder.tag2Table[parentCol.RelPos]
		conditionDimensionTable := builder.tag2Table[dimensionCol.RelPos]
		if conditionParentTable == nil || conditionDimensionTable == nil {
			return 0, false
		}
		if (parentTable != nil && parentTable.TblId != conditionParentTable.TblId) ||
			(dimensionTable != nil && dimensionTable.TblId != conditionDimensionTable.TblId) {
			return 0, false
		}
		parentTable, dimensionTable = conditionParentTable, conditionDimensionTable
		key := [4]int32{parentCol.RelPos, parentCol.ColPos, dimensionCol.RelPos, dimensionCol.ColPos}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		parentCols = append(parentCols, parentCol.ColPos)
		dimensionCols = append(dimensionCols, dimensionCol.ColPos)
	}
	if len(parentCols) == 0 || !isHighNdvCols(dimensionCols, dimensionTable, builder) {
		return 0, false
	}
	parentNDV, parentOK := builder.getColsNDV(parentCols, parentTable)
	dimensionNDV, dimensionOK := builder.getColsNDV(dimensionCols, dimensionTable)
	if !parentOK || !dimensionOK {
		return 0, false
	}
	activeDomain := min(parentNDV, dimensionNDV, parent.Stats.Outcnt)
	if activeDomain <= 0 || math.IsNaN(activeDomain) || math.IsInf(activeDomain, 0) {
		return 0, false
	}
	matchSelectivity := clampSelectivity(dimension.Stats.Outcnt/activeDomain, 1)
	if node.JoinType == plan.Node_ANTI {
		return 1 - matchSelectivity, true
	}
	return matchSelectivity, true
}

func compareJoinVertexStats(left, right *joinVertex) int {
	leftSelectivity := left.node.Stats.Selectivity
	if left.selectivityOnParent >= 0 {
		leftSelectivity = left.selectivityOnParent
	}
	rightSelectivity := right.node.Stats.Selectivity
	if right.selectivityOnParent >= 0 {
		rightSelectivity = right.selectivityOnParent
	}
	return compareStatsValues(
		leftSelectivity, left.node.Stats.Outcnt,
		rightSelectivity, right.node.Stats.Outcnt)
}

func (builder *QueryBuilder) enumerateTags(nodeID int32) []int32 {
	var tags []int32

	node := builder.qry.Nodes[nodeID]
	if len(node.BindingTags) > 0 {
		tags = append(tags, node.BindingTags...)
		//if node.NodeType != plan.Node_JOIN {
		//	return tags
		//}
	}

	for _, childID := range builder.qry.Nodes[nodeID].Children {
		tags = append(tags, builder.enumerateTags(childID)...)
	}

	return tags
}
