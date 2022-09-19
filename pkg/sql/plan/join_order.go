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
)

type joinEdge struct {
	leftCols  []int32
	rightCols []int32
}

type joinVertex struct {
	node      *plan.Node
	pks       []int32
	card      float64
	pkSelRate float64

	children map[int32]any
	parent   int32

	joined bool
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
			joinSide |= getJoinSide(cond, leftTags, rightTags)
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

func (builder *QueryBuilder) determineJoinOrder(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]

	if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_INNER {
		if len(node.Children) > 0 {
			for i, child := range node.Children {
				node.Children[i] = builder.determineJoinOrder(child)
			}

			switch node.NodeType {
			case plan.Node_JOIN:
				leftCost := builder.qry.Nodes[node.Children[0]].Cost
				rightCost := builder.qry.Nodes[node.Children[1]].Cost

				switch node.JoinType {
				case plan.Node_LEFT:
					card := leftCost.Card * rightCost.Card
					if len(node.OnList) > 0 {
						card *= 0.1
						card += leftCost.Card
					}
					node.Cost = &plan.Cost{
						Card: card,
					}

				case plan.Node_RIGHT:
					card := leftCost.Card * rightCost.Card
					if len(node.OnList) > 0 {
						card *= 0.1
						card += rightCost.Card
					}
					node.Cost = &plan.Cost{
						Card: card,
					}

				case plan.Node_OUTER:
					card := leftCost.Card * rightCost.Card
					if len(node.OnList) > 0 {
						card *= 0.1
						card += leftCost.Card + rightCost.Card
					}
					node.Cost = &plan.Cost{
						Card: card,
					}

				case plan.Node_SEMI, plan.Node_ANTI:
					node.Cost.Card = leftCost.Card * .7

				case plan.Node_SINGLE, plan.Node_MARK:
					node.Cost.Card = leftCost.Card
				}

			case plan.Node_AGG:
				if len(node.GroupBy) > 0 {
					childCost := builder.qry.Nodes[node.Children[0]].Cost
					node.Cost = &plan.Cost{
						Card: childCost.Card * 0.1,
					}
				} else {
					node.Cost = &plan.Cost{
						Card: 1,
					}
				}

			default:
				childCost := builder.qry.Nodes[node.Children[0]].Cost
				node.Cost.Card = childCost.Card
			}
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
		if subTrees[j].Cost == nil {
			return false
		}

		if subTrees[i].Cost == nil {
			return true
		}

		return subTrees[i].Cost.Card < subTrees[j].Cost.Card
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

		var leftCard, rightCard float64
		leftCard = subTrees[firstConnected].Cost.Card

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

			rightCard = subTrees[nextSibling].Cost.Card

			children := []int32{nodeID, subTrees[nextSibling].NodeId}
			if leftCard < rightCard {
				children[0], children[1] = children[1], children[0]
				leftCard, rightCard = rightCard, leftCard
			}

			nodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: children,
				JoinType: plan.Node_INNER,
			}, nil)

			leftCard = leftCard * rightCard * 0.1

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
			leftCard, rightCard := newNode.Cost.Card, subTrees[i].Cost.Card
			if leftCard < rightCard {
				children[0], children[1] = children[1], children[0]
			}

			nodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: children,
				JoinType: plan.Node_INNER,
			}, nil)
			newNode = builder.qry.Nodes[nodeID]
		}
	}

	nodeID, _ = builder.pushdownFilters(nodeID, conds)

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
			node:      node,
			card:      node.Cost.Card,
			pkSelRate: 1.0,
			children:  make(map[int32]any),
			parent:    -1,
		}

		if node.NodeType == plan.Node_TABLE_SCAN {
			binding := builder.ctxByNode[node.NodeId].bindingByTag[node.BindingTags[0]]
			pkDef := builder.compCtx.GetPrimaryKeyDef(node.ObjRef.SchemaName, node.ObjRef.ObjName)
			pks := make([]int32, len(pkDef))
			for i, pk := range pkDef {
				pks[i] = binding.FindColumn(pk.Name)
			}
			vertices[i].pks = pks
			tag2Vert[node.BindingTags[0]] = int32(i)
		}

		for _, filter := range node.FilterList {
			if builder.filterOnPK(filter, vertices[i].pks) {
				vertices[i].pkSelRate *= 0.1
			}
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

			if vertices[leftId].parent == -1 && containsAllPKs(edge.leftCols, vertices[leftId].pks) {
				if vertices[rightId].parent != leftId {
					vertices[leftId].parent = rightId
					vertices[rightId].children[leftId] = nil
				}
			}
			if vertices[rightId].parent == -1 && containsAllPKs(edge.rightCols, vertices[rightId].pks) {
				if vertices[leftId].parent != rightId {
					vertices[rightId].parent = leftId
					vertices[leftId].children[rightId] = nil
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
		return dimensions[i].pkSelRate < dimensions[j].pkSelRate ||
			(dimensions[i].pkSelRate == dimensions[j].pkSelRate &&
				dimensions[i].card < dimensions[j].card)
	})

	for _, child := range dimensions {
		nodeId := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{vertex.node.NodeId, child.node.NodeId},
			JoinType: plan.Node_INNER,
		}, nil)

		vertex.card *= child.pkSelRate
		vertex.pkSelRate *= child.pkSelRate
		vertex.node = builder.qry.Nodes[nodeId]
		vertex.node.Cost.Card = vertex.card
	}
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

func (builder *QueryBuilder) filterOnPK(filter *plan.Expr, pks []int32) bool {
	// FIXME better handle expressions
	return len(pks) > 0
}

func (builder *QueryBuilder) enumerateTags(nodeID int32) []int32 {
	node := builder.qry.Nodes[nodeID]
	if len(node.BindingTags) > 0 {
		return node.BindingTags
	}

	var tags []int32

	for _, childID := range builder.qry.Nodes[nodeID].Children {
		tags = append(tags, builder.enumerateTags(childID)...)
	}

	return tags
}
