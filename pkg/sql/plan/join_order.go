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
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type joinEdge struct {
	leftCols  []int32
	rightCols []int32
}

type joinVertex struct {
	node        *plan.Node
	highNDVCols []int32
	pks         []int32
	children    map[int32]any
	parent      int32
	joined      bool
}

func (builder *QueryBuilder) pushdownSemiAntiJoins(nodeID int32) int32 {
	// TODO: handle SEMI/ANTI joins in join order
	node := builder.qry.Nodes[nodeID]

	for i, childID := range node.Children {
		node.Children[i] = builder.pushdownSemiAntiJoins(childID)
	}

	if node.NodeType != plan.Node_JOIN {
		return nodeID
	}

	if node.JoinType != plan.Node_SEMI && node.JoinType != plan.Node_ANTI {
		return nodeID
	}

	for _, filter := range node.OnList {
		if f, ok := filter.Expr.(*plan.Expr_F); ok {
			if f.F.Func.ObjName != "=" {
				return nodeID
			}
		}
	}

	var targetNode *plan.Node
	var targetSide int32

	joinNode := builder.qry.Nodes[node.Children[0]]

	for {
		if joinNode.NodeType != plan.Node_JOIN {
			break
		}

		if joinNode.JoinType != plan.Node_INNER && joinNode.JoinType != plan.Node_LEFT {
			break
		}

		leftTags := make(map[int32]*Binding)
		for _, tag := range builder.enumerateTags(joinNode.Children[0]) {
			leftTags[tag] = nil
		}

		rightTags := make(map[int32]*Binding)
		for _, tag := range builder.enumerateTags(joinNode.Children[1]) {
			rightTags[tag] = nil
		}

		var joinSide int8
		for _, cond := range node.OnList {
			joinSide |= getJoinSide(cond, leftTags, rightTags, 0)
		}

		if joinSide == JoinSideLeft {
			targetNode = joinNode
			targetSide = 0
			joinNode = builder.qry.Nodes[joinNode.Children[0]]
		} else if joinNode.JoinType == plan.Node_INNER && joinSide == JoinSideRight {
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

func IsEquiJoin(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if e, ok := expr.Expr.(*plan.Expr_F); ok {
			if !SupportedJoinCondition(e.F.Func.GetObj()) {
				continue
			}
			lpos, rpos := HasColExpr(e.F.Args[0], -1), HasColExpr(e.F.Args[1], -1)
			if lpos == -1 || rpos == -1 || (lpos == rpos) {
				continue
			}
			return true
		}
	}
	return false || isEquiJoin0(exprs)
}

func isEquiJoin0(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if e, ok := expr.Expr.(*plan.Expr_F); ok {
			if !SupportedJoinCondition(e.F.Func.GetObj()) {
				return false
			}
			lpos, rpos := HasColExpr(e.F.Args[0], -1), HasColExpr(e.F.Args[1], -1)
			if lpos == -1 || rpos == -1 || (lpos == rpos) {
				return false
			}
		}
	}
	return true
}
func SupportedJoinCondition(id int64) bool {
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
		if node.NodeType == plan.Node_JOIN {
			//swap join order for left & right join, inner join is not here
			builder.applySwapRuleByStats(node.NodeId, false)
		}
		return nodeID
	}

	leaves, conds := builder.gatherJoinLeavesAndConds(node, nil, nil)

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

	sort.Slice(subTrees, func(i, j int) bool {
		if subTrees[j].Stats == nil {
			return false
		}
		if subTrees[i].Stats == nil {
			return true
		}
		if math.Abs(subTrees[i].Stats.Selectivity-subTrees[j].Stats.Selectivity) > 0.01 {
			return subTrees[i].Stats.Selectivity < subTrees[j].Stats.Selectivity
		} else {
			return subTrees[i].Stats.Outcnt < subTrees[j].Stats.Outcnt
		}
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
		hyperEdge := make(map[int32]any)
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
			builder.applySwapRuleByStats(nodeID, false)

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
			builder.applySwapRuleByStats(nodeID, false)
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

	ReCalcNodeStats(nodeID, builder, true)

	return nodeID
}

func (builder *QueryBuilder) gatherJoinLeavesAndConds(joinNode *plan.Node, leaves []*plan.Node, conds []*plan.Expr) ([]*plan.Node, []*plan.Expr) {
	if joinNode.NodeType != plan.Node_JOIN || joinNode.JoinType != plan.Node_INNER {
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
			children: make(map[int32]any),
			parent:   -1,
		}

		if node.NodeType == plan.Node_TABLE_SCAN {
			binding := builder.ctxByNode[node.NodeId].bindingByTag[node.BindingTags[0]]
			vertices[i].highNDVCols = GetHighNDVColumns(builder.compCtx.GetStatsCache().GetStatsInfoMap(node.TableDef.TblId), binding)
			pkDef := builder.compCtx.GetPrimaryKeyDef(node.ObjRef.SchemaName, node.ObjRef.ObjName)
			pks := make([]int32, len(pkDef))
			for i, pk := range pkDef {
				pks[i] = binding.FindColumn(pk.Name)
			}
			vertices[i].pks = pks
			tag2Vert[node.BindingTags[0]] = int32(i)
		}
	}

	edgeMap := make(map[[2]int32]*joinEdge)

	for _, cond := range conds {
		if f, ok := cond.Expr.(*plan.Expr_F); ok {
			if f.F.Func.ObjName != "=" {
				continue
			}
			if _, ok = f.F.Args[0].Expr.(*plan.Expr_Col); !ok {
				continue
			}
			if _, ok = f.F.Args[1].Expr.(*plan.Expr_Col); !ok {
				continue
			}

			var leftId, rightId int32

			leftCol := f.F.Args[0].Expr.(*plan.Expr_Col).Col
			rightCol := f.F.Args[1].Expr.(*plan.Expr_Col).Col
			if leftId, ok = tag2Vert[leftCol.RelPos]; !ok {
				continue
			}
			if rightId, ok = tag2Vert[rightCol.RelPos]; !ok {
				continue
			}
			if vertices[leftId].parent != -1 && vertices[rightId].parent != -1 {
				continue
			}

			if leftId > rightId {
				leftId, rightId = rightId, leftId
				leftCol, rightCol = rightCol, leftCol
			}

			edge := edgeMap[[2]int32{leftId, rightId}]
			if edge == nil {
				edge = &joinEdge{}
			}
			edge.leftCols = append(edge.leftCols, leftCol.ColPos)
			edge.rightCols = append(edge.rightCols, rightCol.ColPos)
			edgeMap[[2]int32{leftId, rightId}] = edge

			if vertices[leftId].parent == -1 {
				if containsAllPKs(edge.leftCols, vertices[leftId].pks) || containsHighNDVCol(edge.leftCols, vertices[leftId].highNDVCols) {
					if vertices[rightId].parent != leftId {
						vertices[leftId].parent = rightId
						vertices[rightId].children[leftId] = nil
					}
				}
			}
			if vertices[rightId].parent == -1 {
				if containsAllPKs(edge.rightCols, vertices[rightId].pks) || containsHighNDVCol(edge.rightCols, vertices[rightId].highNDVCols) {
					if vertices[leftId].parent != rightId {
						vertices[rightId].parent = leftId
						vertices[leftId].children[rightId] = nil
					}
				}
			}
		}
	}

	return vertices
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
	sort.Slice(dimensions, func(i, j int) bool {
		if math.Abs(dimensions[i].node.Stats.Selectivity-dimensions[j].node.Stats.Selectivity) > 0.01 {
			return dimensions[i].node.Stats.Selectivity < dimensions[j].node.Stats.Selectivity
		} else {
			return dimensions[i].node.Stats.Outcnt < dimensions[j].node.Stats.Outcnt
		}
	})

	for _, child := range dimensions {

		children := []int32{vertex.node.NodeId, child.node.NodeId}
		nodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: children,
			JoinType: plan.Node_INNER,
		}, nil)

		vertex.node = builder.qry.Nodes[nodeID]
		builder.applySwapRuleByStats(nodeID, false)
	}
}

func containsHighNDVCol(cols, highNDVCols []int32) bool {
	for _, i := range cols {
		for _, j := range highNDVCols {
			if i == j {
				return true
			}
		}
	}
	return false
}

func containsAllPKs(cols, pks []int32) bool {
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
