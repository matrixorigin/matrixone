// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type vectorSortContext struct {
	projNode      *plan.Node
	sortNode      *plan.Node
	scanNode      *plan.Node
	childNode     *plan.Node
	orderExpr     *plan.Expr
	distFnExpr    *plan.Function
	sortDirection plan.OrderBySpec_OrderByFlag
	limit         *plan.Expr
	rankOption    *plan.RankOption

	// Fields for join-through pattern (subquery provides vector argument)
	joinNode       *plan.Node
	subqueryScanID int32
	vecArgExpr     *plan.Expr
}

func (builder *QueryBuilder) resolveScanNodeWithIndex(node *plan.Node, depth int32) *plan.Node {
	if node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 {
		return builder.resolveScanNodeWithIndex(builder.qry.Nodes[node.Children[0]], depth)
	}

	if depth == 0 {
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.Indexes != nil {
			return node
		}
		return nil
	}

	if (node.NodeType == plan.Node_SORT || node.NodeType == plan.Node_AGG) && len(node.Children) == 1 {
		return builder.resolveScanNodeWithIndex(builder.qry.Nodes[node.Children[0]], depth-1)
	}

	return nil
}

func (builder *QueryBuilder) buildVectorSortContext(projNode *plan.Node) *vectorSortContext {
	sortNode := builder.resolveSortNode(projNode, 1)
	if sortNode == nil || len(sortNode.OrderBy) != 1 {
		return nil
	}

	scanNode := builder.resolveScanNodeWithIndex(sortNode, 1)
	if scanNode == nil {
		return nil
	}

	orderExpr := sortNode.OrderBy[0].Expr
	distFnExpr := orderExpr.GetF()
	var childNode *plan.Node
	if distFnExpr == nil {
		if len(sortNode.Children) == 0 {
			return nil
		}
		childNode = builder.qry.Nodes[sortNode.Children[0]]
		if childNode.NodeType == plan.Node_PROJECT {
			distFnExpr = childNode.ProjectList[orderExpr.GetCol().ColPos].GetF()
		}
		if distFnExpr == nil {
			return nil
		}
	}

	limit, rankOption := pickVectorLimit(sortNode, scanNode, projNode)
	if limit == nil {
		return nil
	}

	return &vectorSortContext{
		projNode:      projNode,
		sortNode:      sortNode,
		scanNode:      scanNode,
		childNode:     childNode,
		orderExpr:     orderExpr,
		distFnExpr:    distFnExpr,
		sortDirection: sortNode.OrderBy[0].Flag,
		limit:         limit,
		rankOption:    rankOption,
	}
}

func pickVectorLimit(sortNode, scanNode, projNode *plan.Node) (*plan.Expr, *plan.RankOption) {
	if sortNode.Limit != nil {
		return sortNode.Limit, sortNode.RankOption
	}
	if scanNode.Limit != nil {
		return scanNode.Limit, scanNode.RankOption
	}
	if projNode.Limit != nil {
		return projNode.Limit, projNode.RankOption
	}
	return nil, nil
}

// buildVectorSortContextThroughJoin handles the case where the sort node's child
// is a JOIN (from decorrelated subquery). Pattern:
//
//	Project -> Sort -> [Project] -> Join(Scan_main, Scan_subquery)
//
// where l2_distance(main.vec_col, subquery.vec_col) is the order-by expression.
func (builder *QueryBuilder) buildVectorSortContextThroughJoin(projNode *plan.Node) *vectorSortContext {
	sortNode := builder.resolveSortNode(projNode, 1)
	if sortNode == nil || len(sortNode.OrderBy) != 1 {
		return nil
	}

	// Find JOIN node under sort (possibly through a PROJECT)
	joinNode := builder.resolveJoinNodeForVector(sortNode)
	if joinNode == nil {
		return nil
	}

	// Only handle SINGLE and INNER joins (from scalar subqueries)
	if joinNode.JoinType != plan.Node_SINGLE && joinNode.JoinType != plan.Node_INNER {
		return nil
	}
	if len(joinNode.Children) != 2 {
		return nil
	}

	// Find which child has a TABLE_SCAN with vector indexes
	leftNode := builder.qry.Nodes[joinNode.Children[0]]
	rightNode := builder.qry.Nodes[joinNode.Children[1]]

	var scanNode, subqueryNode *plan.Node
	var subqueryNodeID int32

	leftScan := builder.findScanWithVectorIndex(leftNode)
	rightScan := builder.findScanWithVectorIndex(rightNode)

	if leftScan != nil && rightScan == nil {
		scanNode = leftScan
		subqueryNode = rightNode
		subqueryNodeID = joinNode.Children[1]
	} else if rightScan != nil && leftScan == nil {
		scanNode = rightScan
		subqueryNode = leftNode
		subqueryNodeID = joinNode.Children[0]
	} else {
		// Both sides have vector indexes (self-join) or neither — skip
		return nil
	}

	// Resolve the distance function expression
	orderExpr := sortNode.OrderBy[0].Expr
	distFnExpr := orderExpr.GetF()
	var childNode *plan.Node
	if distFnExpr == nil {
		if len(sortNode.Children) == 0 {
			return nil
		}
		childNode = builder.qry.Nodes[sortNode.Children[0]]
		if childNode.NodeType == plan.Node_PROJECT {
			distFnExpr = childNode.ProjectList[orderExpr.GetCol().ColPos].GetF()
		}
		if distFnExpr == nil {
			return nil
		}
	}

	// Identify which distance function argument is the vector column from scanNode
	// and which is the vector from the subquery side
	vecArgExpr := builder.extractVecArgFromJoin(distFnExpr, scanNode, subqueryNode)
	if vecArgExpr == nil {
		return nil
	}

	limit, rankOption := pickVectorLimit(sortNode, scanNode, projNode)
	if limit == nil {
		return nil
	}

	return &vectorSortContext{
		projNode:       projNode,
		sortNode:       sortNode,
		scanNode:       scanNode,
		childNode:      childNode,
		orderExpr:      orderExpr,
		distFnExpr:     distFnExpr,
		sortDirection:  sortNode.OrderBy[0].Flag,
		limit:          limit,
		rankOption:     rankOption,
		joinNode:       joinNode,
		subqueryScanID: subqueryNodeID,
		vecArgExpr:     vecArgExpr,
	}
}

// resolveJoinNodeForVector finds a JOIN node under a sort node, possibly through a PROJECT.
func (builder *QueryBuilder) resolveJoinNodeForVector(sortNode *plan.Node) *plan.Node {
	if len(sortNode.Children) == 0 {
		return nil
	}
	child := builder.qry.Nodes[sortNode.Children[0]]

	if child.NodeType == plan.Node_JOIN {
		return child
	}
	if child.NodeType == plan.Node_PROJECT && len(child.Children) == 1 {
		grandchild := builder.qry.Nodes[child.Children[0]]
		if grandchild.NodeType == plan.Node_JOIN {
			return grandchild
		}
	}
	return nil
}

// findScanWithVectorIndex traverses through PROJECT nodes to find a TABLE_SCAN with vector indexes.
func (builder *QueryBuilder) findScanWithVectorIndex(node *plan.Node) *plan.Node {
	for node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 {
		node = builder.qry.Nodes[node.Children[0]]
	}
	if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.Indexes != nil {
		for _, idx := range node.TableDef.Indexes {
			if catalog.IsIvfIndexAlgo(idx.IndexAlgo) || catalog.IsHnswIndexAlgo(idx.IndexAlgo) {
				return node
			}
		}
	}
	return nil
}

// extractVecArgFromJoin identifies which argument of the distance function comes from the
// subquery side of the join and returns it as the expression to pass to the vector index search.
func (builder *QueryBuilder) extractVecArgFromJoin(distFnExpr *plan.Function, scanNode, subqueryNode *plan.Node) *plan.Expr {
	if distFnExpr == nil || len(distFnExpr.Args) != 2 {
		return nil
	}

	scanTag := scanNode.BindingTags[0]
	subqueryTags := builder.collectBindingTags(subqueryNode)

	// Determine which arg references the scan table and which references the subquery
	arg0Col := distFnExpr.Args[0].GetCol()
	arg1Col := distFnExpr.Args[1].GetCol()

	if arg0Col != nil && arg1Col != nil {
		// Both args are column refs — one should be from scanNode, other from subquery
		if arg0Col.RelPos == scanTag && subqueryTags[arg1Col.RelPos] {
			return distFnExpr.Args[1]
		}
		if arg1Col.RelPos == scanTag && subqueryTags[arg0Col.RelPos] {
			return distFnExpr.Args[0]
		}
	}

	return nil
}

// collectBindingTags collects all binding tags reachable from a node (for subquery detection).
func (builder *QueryBuilder) collectBindingTags(node *plan.Node) map[int32]bool {
	tags := make(map[int32]bool)
	builder.collectBindingTagsRecursive(node, tags)
	return tags
}

func (builder *QueryBuilder) collectBindingTagsRecursive(node *plan.Node, tags map[int32]bool) {
	for _, tag := range node.BindingTags {
		tags[tag] = true
	}
	for _, childID := range node.Children {
		builder.collectBindingTagsRecursive(builder.qry.Nodes[childID], tags)
	}
}

func (builder *QueryBuilder) resolveSortNode(node *plan.Node, depth int32) *plan.Node {
	if depth == 0 {
		if node.NodeType == plan.Node_SORT {
			return node
		}
		return nil
	}

	if node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 {
		return builder.resolveSortNode(builder.qry.Nodes[node.Children[0]], depth-1)
	}

	return nil
}

func (builder *QueryBuilder) resolveScanNodeFromProject(node *plan.Node, depth int32) *plan.Node {
	if depth == 0 {
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.Indexes != nil {
			return node
		}
		return nil
	}

	if node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 {
		return builder.resolveScanNodeFromProject(builder.qry.Nodes[node.Children[0]], depth-1)
	}

	return nil
}
