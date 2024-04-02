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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type joinEdge struct {
	leftCols  []int32
	rightCols []int32
}

type joinVertex struct {
	node     *plan.Node
	children map[int32]bool
	parent   int32
	joined   bool
}

func (builder *QueryBuilder) pushdownSemiAntiJoins(nodeID int32) int32 {
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
			if semiAntiStat.Selectivity*ratio > builder.qry.Nodes[joinNode.Children[1]].Stats.Selectivity {
				break
			}
			targetNode = joinNode
			targetSide = 0
			joinNode = builder.qry.Nodes[joinNode.Children[0]]
		} else if joinNode.JoinType == plan.Node_INNER && joinSide == JoinSideRight {
			if semiAntiStat.Selectivity*ratio > builder.qry.Nodes[joinNode.Children[0]].Stats.Selectivity {
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
		if e, ok := expr.Expr.(*plan.Expr_F); ok {
			if !IsEqualFunc(e.F.Func.GetObj()) {
				continue
			}
			lpos, rpos := HasColExpr(e.F.Args[0], -1), HasColExpr(e.F.Args[1], -1)
			if lpos == -1 || rpos == -1 || (lpos == rpos) {
				continue
			}
			return true
		}
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

	sort.Slice(subTrees, func(i, j int) bool { return compareStats(subTrees[i].Stats, subTrees[j].Stats) })

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
			nodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: children,
				JoinType: plan.Node_INNER,
			}, nil)

			for i, adj := range adjMat[nextSibling*nLeaf : (nextSibling+1)*nLeaf] {
				eligible[i] = eligible[i] || adj
			}
		}

		for i := range visited {
			if !visited[i] {
				nodeID = builder.appendNode(&plan.Node{
					NodeType: plan.Node_JOIN,
					Children: []int32{nodeID, subTrees[i].NodeId},
					JoinType: plan.Node_INNER,
				}, nil)
			}
		}
	} else {
		newNode := subTrees[0]
		nodeID = newNode.NodeId

		for i := 1; i < len(subTrees); i++ {
			children := []int32{nodeID, subTrees[i].NodeId}
			nodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: children,
				JoinType: plan.Node_INNER,
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
	return nodeID
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
			node:     node,
			children: make(map[int32]bool),
			parent:   -1,
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
					} else if vertices[leftId].node.Stats.Outcnt < vertices[rightId].node.Stats.Outcnt {
						unsetParent(rightId, leftId, vertices)
						setParent(leftId, rightId, vertices)
					}
				}
			}
			rightParent := vertices[rightId].parent
			if isHighNdvCols(edge.rightCols, builder.tag2Table[rightCol.RelPos], builder) {
				if rightParent == -1 || shouldChangeParent(rightId, rightParent, leftId, vertices) {
					if vertices[leftId].parent != rightId {
						setParent(rightId, leftId, vertices)
					} else if vertices[rightId].node.Stats.Outcnt < vertices[leftId].node.Stats.Outcnt {
						unsetParent(leftId, rightId, vertices)
						setParent(rightId, leftId, vertices)
					}
				}
			}
		}
	}
	return vertices
}

func setParent(child, parent int32, vertices []*joinVertex) {
	if child == -1 || parent == -1 {
		return
	}
	unsetParent(child, vertices[child].parent, vertices)
	vertices[child].parent = parent
	vertices[parent].children[child] = true
}

func unsetParent(child, parent int32, vertices []*joinVertex) {
	if child == -1 || parent == -1 {
		return
	}
	if vertices[child].parent == parent {
		vertices[child].parent = -1
		delete(vertices[parent].children, child)
	}
}

func findSelectivityInChildren(self int32, vertices []*joinVertex) bool {
	if vertices[self].node.Stats.Selectivity < 0.9 {
		return true
	}
	for child := range vertices[self].children {
		if findSelectivityInChildren(child, vertices) {
			return true
		}
	}
	return false
}

func findParent(self, target int32, vertices []*joinVertex) bool {
	parent := vertices[self].parent
	if parent == target {
		return true
	} else if parent != -1 {
		return findParent(parent, target, vertices)
	}
	return false
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
	return compareStats(nextParentStats, currentParentStats)
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
	sort.Slice(dimensions, func(i, j int) bool { return compareStats(dimensions[i].node.Stats, dimensions[j].node.Stats) })

	for _, child := range dimensions {

		children := []int32{vertex.node.NodeId, child.node.NodeId}
		nodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: children,
			JoinType: plan.Node_INNER,
		}, nil)

		vertex.node = builder.qry.Nodes[nodeID]
	}
}

func containsAllPKs(cols []int32, tableDef *plan.TableDef) bool {
	pkNames := tableDef.Pkey.Names
	pks := make([]int32, len(pkNames))
	for i := range pkNames {
		pks[i] = tableDef.Name2ColIndex[pkNames[i]]
	}
	if len(pks) == 0 {
		return false
	}
	for _, pk := range pks {
		found := false
		for _, col := range cols {
			if col == pk {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (builder *QueryBuilder) enumerateTags(nodeID int32) []int32 {
	var tags []int32

	node := builder.qry.Nodes[nodeID]
	if len(node.BindingTags) > 0 {
		tags = append(tags, node.BindingTags...)
		if node.NodeType != plan.Node_JOIN {
			return tags
		}
	}

	for _, childID := range builder.qry.Nodes[nodeID].Children {
		tags = append(tags, builder.enumerateTags(childID)...)
	}

	return tags
}
