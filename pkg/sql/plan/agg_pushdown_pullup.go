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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// some restrictions for agg pushdown to make it easier to acheive
// will remove some restrictions in the future
func shouldAggPushDown(agg, join, leftChild, rightChild *plan.Node, builder *QueryBuilder) bool {
	if leftChild.NodeType != plan.Node_TABLE_SCAN || rightChild.NodeType != plan.Node_TABLE_SCAN {
		return false
	}
	if len(agg.GroupBy) != 0 {
		return false
	}
	if len(agg.AggList) != 1 {
		return false
	}
	aggFunc, ok := agg.AggList[0].Expr.(*plan.Expr_F)
	if !ok {
		return false
	}
	if aggFunc.F.Func.ObjName != "sum" {
		return false
	}
	colAgg, ok := aggFunc.F.Args[0].Expr.(*plan.Expr_Col)
	if !ok {
		return false
	}
	leftChildTag := leftChild.BindingTags[0]
	if colAgg.Col.RelPos != leftChildTag {
		return false
	}

	if !IsEquiJoin(join.OnList) || len(join.OnList) != 1 {
		return false
	}
	colGroupBy, ok := filterTag(join.OnList[0], leftChildTag).Expr.(*plan.Expr_Col)
	if !ok {
		return false
	}
	ndv := getColNdv(colGroupBy.Col, join.NodeId, builder)
	if ndv < 0 || ndv > join.Stats.Outcnt {
		return false
	}
	return true
}

func replaceCol(expr *plan.Expr, oldRelPos, oldColPos, newRelPos, newColPos int32) {
	if expr == nil {
		return
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			replaceCol(arg, oldRelPos, oldColPos, newRelPos, newColPos)
		}

	case *plan.Expr_Col:
		//for now, shouldAggPushDown make sure only one column in expr,and only one expr in exprlist, so new colpos is always 0
		//if multi expr in agg list and group list supported in the future, this need to be fixed
		if exprImpl.Col.RelPos == oldRelPos {
			exprImpl.Col.RelPos = newRelPos
			exprImpl.Col.ColPos = 0
		}
	}
}

func filterTag(expr *Expr, tag int32) *Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			retExpr := filterTag(arg, tag)
			if retExpr != nil {
				return retExpr
			}
		}
	case *plan.Expr_Col:
		if exprImpl.Col.RelPos == tag {
			return expr
		}
	}
	return nil
}

func applyAggPushdown(agg, join, leftChild *plan.Node, builder *QueryBuilder) {
	leftChildTag := leftChild.BindingTags[0]
	newAggList := DeepCopyExprList(agg.AggList)
	//newGroupBy := DeepCopyExprList(agg.GroupBy)
	newGroupBy := []*plan.Expr{DeepCopyExpr(filterTag(join.OnList[0], leftChildTag))}

	newGroupTag := builder.genNewTag()
	newAggTag := builder.genNewTag()
	newNodeID := builder.appendNode(
		&plan.Node{
			NodeType:    plan.Node_AGG,
			Children:    []int32{leftChild.NodeId},
			GroupBy:     newGroupBy,
			AggList:     newAggList,
			BindingTags: []int32{newGroupTag, newAggTag},
		},
		builder.ctxByNode[join.NodeId])

	//set child pointer
	join.Children[0] = newNodeID

	//replace relpos for exprs in join and agg node
	replaceCol(join.OnList[0], leftChildTag, 0, newGroupTag, 0)
	replaceCol(agg.AggList[0], leftChildTag, 0, newAggTag, 0)
}

func (builder *QueryBuilder) aggPushDown(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]

	if node.NodeType != plan.Node_AGG {
		if len(node.Children) > 0 {
			for i, child := range node.Children {
				node.Children[i] = builder.aggPushDown(child)
			}
		}
		return nodeID
	}
	//current node is node_agg, child must be a join
	//for now ,only support inner join
	join := builder.qry.Nodes[node.Children[0]]
	if join.NodeType != plan.Node_JOIN || join.JoinType != plan.Node_INNER {
		return nodeID
	}

	leftChild := builder.qry.Nodes[join.Children[0]]
	rightChild := builder.qry.Nodes[join.Children[1]]

	if !shouldAggPushDown(node, join, leftChild, rightChild, builder) {
		return nodeID
	}

	applyAggPushdown(node, join, leftChild, builder)
	return nodeID
}

func applyAggPullup(filter, join, project, agg *plan.Node, builder *QueryBuilder) {
	if len(agg.GroupBy) != 1 {
		return
	}
	groupColInAgg, ok := agg.GroupBy[0].Expr.(*plan.Expr_Col)
	if !ok {
		return
	}
	if !IsEquiJoin(join.OnList) || len(join.OnList) != 1 {
		return
	}
	groupColInJoin, ok := filterTag(join.OnList[0], project.BindingTags[0]).Expr.(*plan.Expr_Col)
	if !ok {
		return
	}
	if agg.Stats.Outcnt/agg.Stats.Cost < join.Stats.Outcnt/project.Stats.Outcnt {
		return
	}
	filter.Children[0] = project.NodeId
	tmp := agg.Children[0]
	agg.Children[0] = join.NodeId
	join.Children[0] = tmp
	groupColInJoin.Col.RelPos = groupColInAgg.Col.RelPos
	groupColInJoin.Col.ColPos = groupColInAgg.Col.ColPos

	//builder.ctxByNode[project.NodeId] = builder.ctxByNode[join.NodeId]
}

func (builder *QueryBuilder) aggPullup(nodeID int32) int32 {
	// agg pullup only support filter->inner join->project->agg for now
	// we can change it to filter->project->agg->inner join
	node := builder.qry.Nodes[nodeID]

	if node.NodeType != plan.Node_FILTER {
		if len(node.Children) > 0 {
			for i, child := range node.Children {
				node.Children[i] = builder.aggPullup(child)
			}
		}
		return nodeID
	}
	filter := node
	join := builder.qry.Nodes[filter.Children[0]]
	if join.NodeType != plan.Node_JOIN || join.JoinType != plan.Node_INNER {
		return nodeID
	}
	project := builder.qry.Nodes[join.Children[0]]
	if project.NodeType != plan.Node_PROJECT {
		return nodeID
	}
	agg := builder.qry.Nodes[project.Children[0]]
	if agg.NodeType != plan.Node_AGG {
		return nodeID
	}

	applyAggPullup(filter, join, project, agg, builder)
	return nodeID
}
