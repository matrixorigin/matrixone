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
		if exprImpl.Col.RelPos == oldRelPos && exprImpl.Col.ColPos == oldColPos {
			exprImpl.Col.RelPos = newRelPos
			exprImpl.Col.ColPos = newColPos
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
	colGroupBy, _ := filterTag(join.OnList[0], leftChildTag).Expr.(*plan.Expr_Col)
	replaceCol(join.OnList[0], leftChildTag, colGroupBy.Col.ColPos, newGroupTag, 0)

	colAgg, _ := filterTag(agg.AggList[0], leftChildTag).Expr.(*plan.Expr_Col)
	replaceCol(agg.AggList[0], leftChildTag, colAgg.Col.ColPos, newAggTag, 0)
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

	//make sure left child is bigger and  agg pushdown to left child
	builder.applySwapRuleByStats(join.NodeId, false)

	leftChild := builder.qry.Nodes[join.Children[0]]
	rightChild := builder.qry.Nodes[join.Children[1]]

	if !shouldAggPushDown(node, join, leftChild, rightChild, builder) {
		return nodeID
	}

	applyAggPushdown(node, join, leftChild, builder)
	return nodeID
}

func getJoinCondCol(cond *Expr, leftTag int32, rightTag int32) (*plan.Expr_Col, *plan.Expr_Col) {
	fun, ok := cond.Expr.(*plan.Expr_F)
	if !ok {
		return nil, nil
	}
	leftCol, ok := fun.F.Args[0].Expr.(*plan.Expr_Col)
	if !ok {
		return nil, nil
	}
	rightCol, ok := fun.F.Args[1].Expr.(*plan.Expr_Col)
	if !ok {
		return nil, nil
	}
	if leftCol.Col.RelPos != leftTag {
		leftCol, rightCol = rightCol, leftCol
	}
	if leftCol.Col.RelPos != leftTag || rightCol.Col.RelPos != rightTag {
		return nil, nil
	}
	return leftCol, rightCol
}

func replaceAllColRefInExprList(exprlist []*plan.Expr, from *plan.Expr_Col, to *plan.Expr_Col) {
	for _, expr := range exprlist {
		replaceCol(expr, from.Col.RelPos, from.Col.ColPos, to.Col.RelPos, to.Col.ColPos)
	}
}

func replaceAllColRefInPlan(nodeID int32, exceptID int32, from *plan.Expr_Col, to *plan.Expr_Col, builder *QueryBuilder) {
	//change all nodes in plan, except join and its children
	if nodeID == exceptID {
		return
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			replaceAllColRefInPlan(child, exceptID, from, to, builder)
		}
	}
	replaceAllColRefInExprList(node.OnList, from, to)
	replaceAllColRefInExprList(node.ProjectList, from, to)
	replaceAllColRefInExprList(node.FilterList, from, to)
	replaceAllColRefInExprList(node.AggList, from, to)
	replaceAllColRefInExprList(node.GroupBy, from, to)
	replaceAllColRefInExprList(node.GroupingSet, from, to)
	for _, orderby := range node.OrderBy {
		replaceCol(orderby.Expr, from.Col.RelPos, from.Col.ColPos, to.Col.RelPos, to.Col.ColPos)
	}
}

func checkColRef(expr *plan.Expr, col *plan.Expr_Col) bool {
	if expr == nil {
		return true
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if !checkColRef(arg, col) {
				return false
			}
		}

	case *plan.Expr_Col:
		if exprImpl.Col.RelPos == col.Col.RelPos && exprImpl.Col.ColPos != col.Col.ColPos {
			return false
		}
	}
	return true
}

func checkAllColRefInExprList(exprlist []*plan.Expr, col *plan.Expr_Col) bool {
	for _, expr := range exprlist {
		if !checkColRef(expr, col) {
			return false
		}
	}
	return true
}

func checkAllColRefInPlan(nodeID int32, exceptID int32, col *plan.Expr_Col, builder *QueryBuilder) bool {
	//change all nodes in plan, except join and its children
	if nodeID == exceptID {
		return true
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			if !checkAllColRefInPlan(child, exceptID, col, builder) {
				return false
			}
		}
	}
	ret := true
	ret = ret && checkAllColRefInExprList(node.OnList, col)
	ret = ret && checkAllColRefInExprList(node.ProjectList, col)
	ret = ret && checkAllColRefInExprList(node.FilterList, col)
	ret = ret && checkAllColRefInExprList(node.AggList, col)
	ret = ret && checkAllColRefInExprList(node.GroupBy, col)
	ret = ret && checkAllColRefInExprList(node.GroupingSet, col)
	for _, orderby := range node.OrderBy {
		ret = ret && checkColRef(orderby.Expr, col)
	}
	return ret
}

func applyAggPullup(rootID int32, join, agg, leftScan, rightScan *plan.Node, builder *QueryBuilder) bool {
	if len(agg.GroupBy) != 1 {
		return false
	}
	groupColInAgg, ok := agg.GroupBy[0].Expr.(*plan.Expr_Col)
	if !ok {
		return false
	}
	if !IsEquiJoin(join.OnList) || len(join.OnList) != 1 {
		return false
	}

	leftCol, rightCol := getJoinCondCol(join.OnList[0], agg.BindingTags[0], rightScan.BindingTags[0])
	if leftCol == nil {
		return false
	}

	//rightcol must be primary key of right table
	// or we  add rowid in group by, implement this in the future
	pkDef := builder.compCtx.GetPrimaryKeyDef(rightScan.ObjRef.SchemaName, rightScan.ObjRef.ObjName)
	if len(pkDef) != 1 {
		return false
	}
	rightBinding := builder.ctxByNode[rightScan.NodeId].bindingByTag[rightScan.BindingTags[0]]
	if rightBinding.FindColumn(pkDef[0].Name) != rightCol.Col.ColPos {
		return false
	}

	if agg.Stats.Outcnt/leftScan.Stats.Outcnt < join.Stats.Outcnt/agg.Stats.Outcnt {
		return false
	}

	//col ref to right table can not been seen after agg pulled up
	//since join cond is leftcol=rightcol, we can change col ref from right col to left col
	// and other col in right table must not be referenced
	if !checkAllColRefInPlan(rootID, join.NodeId, rightCol, builder) {
		return false
	}
	replaceAllColRefInPlan(rootID, join.NodeId, rightCol, leftCol, builder)

	join.Children[0] = agg.Children[0]
	agg.Children[0] = join.NodeId
	leftCol.Col.RelPos = groupColInAgg.Col.RelPos
	leftCol.Col.ColPos = groupColInAgg.Col.ColPos
	return true

}

func (builder *QueryBuilder) aggPullup(rootID, nodeID int32) int32 {
	// agg pullup only support node->inner join->agg for now
	// we can change it to node->agg->inner join
	node := builder.qry.Nodes[nodeID]

	if len(node.Children) > 0 {
		for i, child := range node.Children {
			node.Children[i] = builder.aggPullup(rootID, child)
		}
	} else {
		return nodeID
	}

	join := node
	if join.NodeType != plan.Node_JOIN || join.JoinType != plan.Node_INNER {
		return nodeID
	}

	//make sure left child is bigger
	builder.applySwapRuleByStats(join.NodeId, false)

	agg := builder.qry.Nodes[join.Children[0]]
	if agg.NodeType != plan.Node_AGG {
		return nodeID
	}
	leftScan := builder.qry.Nodes[agg.Children[0]]
	if leftScan.NodeType != plan.Node_TABLE_SCAN {
		return nodeID
	}
	rightScan := builder.qry.Nodes[join.Children[1]]
	if rightScan.NodeType != plan.Node_TABLE_SCAN {
		return nodeID
	}

	if applyAggPullup(rootID, join, agg, leftScan, rightScan, builder) {
		return agg.NodeId
	}
	return nodeID
}
