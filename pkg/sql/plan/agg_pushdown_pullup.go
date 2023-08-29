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

	if len(join.OnList) != 1 || !builder.IsEquiJoin(join) {
		return false
	}
	colGroupBy, ok := filterTag(join.OnList[0], leftChildTag).Expr.(*plan.Expr_Col)
	if !ok {
		return false
	}
	ndv := getColNdv(colGroupBy.Col, builder)
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
	if !ok || fun.F.Func.ObjName != "=" {
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

func replaceAllColRefInExprList(exprlist []*plan.Expr, from []*plan.Expr_Col, to []*plan.Expr_Col) {
	for _, expr := range exprlist {
		for i := range from {
			replaceCol(expr, from[i].Col.RelPos, from[i].Col.ColPos, to[i].Col.RelPos, to[i].Col.ColPos)
		}
	}
}

func replaceAllColRefInPlan(nodeID int32, exceptID int32, from []*plan.Expr_Col, to []*plan.Expr_Col, builder *QueryBuilder) {
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
		for i := range from {
			replaceCol(orderby.Expr, from[i].Col.RelPos, from[i].Col.ColPos, to[i].Col.RelPos, to[i].Col.ColPos)
		}
	}
}

func addAnyValue(expr *plan.Expr, agg *plan.Node, builder *QueryBuilder) {
	col, _ := expr.Expr.(*plan.Expr_Col)
	idx := -1
	for i := range agg.AggList {
		fun, _ := agg.AggList[i].Expr.(*plan.Expr_F)
		if fun.F.Func.ObjName != "any_value" {
			continue
		}
		colAgg := fun.F.Args[0].Expr.(*plan.Expr_Col)
		if col.Col.RelPos == colAgg.Col.RelPos && col.Col.ColPos == colAgg.Col.ColPos {
			idx = i
			break
		}
	}
	if idx == -1 {
		idx = len(agg.AggList)
		anyValueExpr, _ := bindFuncExprImplByPlanExpr(builder.compCtx.GetContext(), "any_value", []*plan.Expr{DeepCopyExpr(expr)})
		agg.AggList = append(agg.AggList, anyValueExpr)
	}
	col.Col.RelPos = agg.BindingTags[1]
	col.Col.ColPos = int32(idx)
}

func addAnyValueForNonPKCol(expr *plan.Expr, cols []*plan.Expr_Col, agg *plan.Node, builder *QueryBuilder) {
	if expr == nil {
		return
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			addAnyValueForNonPKCol(arg, cols, agg, builder)
		}

	case *plan.Expr_Col:
		if exprImpl.Col.RelPos == cols[0].Col.RelPos {
			for i := range cols {
				if exprImpl.Col.RelPos == cols[i].Col.RelPos && exprImpl.Col.ColPos == cols[i].Col.ColPos {
					return
				}
			}
			//nonPK col, need to add any_value
			//first check if it is already in agg list
			addAnyValue(expr, agg, builder)
		}
	}

}

func addAnyValueForNonPKInExprList(exprlist []*plan.Expr, cols []*plan.Expr_Col, agg *plan.Node, builder *QueryBuilder) {
	for _, expr := range exprlist {
		addAnyValueForNonPKCol(expr, cols, agg, builder)
	}
}

func addAnyValueForNonPKInPlan(nodeID int32, exceptID int32, cols []*plan.Expr_Col, agg *plan.Node, builder *QueryBuilder) {
	//change all nodes in plan, except join and its children
	if nodeID == exceptID {
		return
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			addAnyValueForNonPKInPlan(child, exceptID, cols, agg, builder)
		}
	}

	addAnyValueForNonPKInExprList(node.OnList, cols, agg, builder)
	addAnyValueForNonPKInExprList(node.ProjectList, cols, agg, builder)
	addAnyValueForNonPKInExprList(node.FilterList, cols, agg, builder)
	addAnyValueForNonPKInExprList(node.AggList, cols, agg, builder)
	addAnyValueForNonPKInExprList(node.GroupBy, cols, agg, builder)
	addAnyValueForNonPKInExprList(node.GroupingSet, cols, agg, builder)
	for _, orderby := range node.OrderBy {
		addAnyValueForNonPKCol(orderby.Expr, cols, agg, builder)
	}
}

func applyAggPullup(rootID int32, join, agg, leftScan, rightScan *plan.Node, builder *QueryBuilder) bool {
	//rightcol must be primary key of right table
	// or we  add rowid in group by, implement this in the future
	if rightScan.TableDef.Pkey == nil {
		return false
	}
	pkNames := rightScan.TableDef.Pkey.Names

	if len(join.OnList) != len(pkNames) || len(join.OnList) != len(agg.GroupBy) || !builder.IsEquiJoin(join) {
		return false
	}

	leftCols := make([]*plan.Expr_Col, len(join.OnList))
	leftColPos := make([]int32, len(join.OnList))
	rightCols := make([]*plan.Expr_Col, len(join.OnList))
	groupColsInAgg := make([]*plan.Expr_Col, len(join.OnList))

	for i := range join.OnList {
		leftCol, rightCol := getJoinCondCol(join.OnList[i], agg.BindingTags[0], rightScan.BindingTags[0])
		if leftCol == nil {
			return false
		}
		groupColInAgg, ok := agg.GroupBy[i].Expr.(*plan.Expr_Col)
		if !ok {
			return false
		}
		leftCols[i] = leftCol
		rightCols[i] = rightCol
		leftColPos[i] = leftCol.Col.ColPos
		groupColsInAgg[i] = groupColInAgg
	}
	if !containsAllPKs(leftColPos, rightScan.TableDef) {
		return false
	}

	if agg.Stats.Outcnt/leftScan.Stats.Outcnt < join.Stats.Outcnt/agg.Stats.Outcnt || join.Stats.Selectivity > 0.95 {
		return false
	}

	addAnyValueForNonPKInPlan(rootID, join.NodeId, rightCols, agg, builder)

	replaceAllColRefInPlan(rootID, join.NodeId, rightCols, leftCols, builder)

	join.Children[0] = agg.Children[0]
	agg.Children[0] = join.NodeId

	for i := range leftCols {
		j := leftCols[i].Col.ColPos
		leftCols[i].Col.RelPos = groupColsInAgg[j].Col.RelPos
		leftCols[i].Col.ColPos = groupColsInAgg[j].Col.ColPos
	}
	return true

}

func (builder *QueryBuilder) aggPullup(rootID, nodeID int32) int32 {
	// agg pullup only support node->(filter)->inner join->agg for now
	// we can change it to node->agg->(filter)->inner join
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
	for rightScan.NodeType == plan.Node_JOIN && rightScan.JoinType == plan.Node_SEMI {
		rightScan = builder.qry.Nodes[rightScan.Children[0]]
	}
	if rightScan.NodeType != plan.Node_TABLE_SCAN {
		return nodeID
	}

	if applyAggPullup(rootID, join, agg, leftScan, rightScan, builder) {
		return agg.NodeId
	}
	return nodeID
}
