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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

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
	ndv := builder.getColNdv(colGroupBy.Col)
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

	for i, aggExpr := range agg.AggList {
		if funExpr, ok := aggExpr.Expr.(*plan.Expr_F); ok {
			if len(funExpr.F.Args) == 1 && funExpr.F.Args[0].Typ.Id != aggExpr.Typ.Id {
				//rebind if needed:
				// case:  select sum(decimal_64_col) from t1 left join t2 on t1.col = t2.col where t2.col > 10;
				// if result of sum(decimal_64_col) is decimal128, we need to rebind the input of origin agg expr to decimal128
				funExpr.F.Args[0].Typ = aggExpr.Typ
				var err error
				agg.AggList[i], err = BindFuncExprImplByPlanExpr(builder.GetContext(), funExpr.F.Func.ObjName, funExpr.F.Args)
				if err != nil {
					panic("rebind agg expr failed") //should not happen
				}
			}
		}
	}

	//newGroupBy := DeepCopyExprList(agg.GroupBy)
	newGroupBy := []*plan.Expr{DeepCopyExpr(filterTag(join.OnList[0], leftChildTag))}

	newGroupTag := builder.genNewBindTag()
	newAggTag := builder.genNewBindTag()
	newNodeID := builder.appendNode(
		&plan.Node{
			NodeType:    plan.Node_AGG,
			Children:    []int32{leftChild.NodeId},
			GroupBy:     newGroupBy,
			AggList:     newAggList,
			BindingTags: []int32{newGroupTag, newAggTag},
			SpillMem:    builder.aggSpillMem,
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

// agg pushdown only support node->(filter)->inner join->agg for now
// we can change it to node->agg->(filter)->inner join
func (builder *QueryBuilder) aggPushDown(nodeID int32) int32 {
	if builder.optimizerHints != nil && builder.optimizerHints.aggPushDown != 0 {
		return nodeID
	}
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

func getJoinCondCol(cond *Expr, leftTag int32, rightTag int32) (*plan.Expr_Col, *plan.Expr_Col) {
	if cond == nil {
		return nil, nil
	}
	fun := cond.GetF()
	if fun == nil || fun.Func == nil || !IsEqualFunc(fun.Func.Obj) || len(fun.Args) != 2 {
		return nil, nil
	}
	leftRef := fun.Args[0].GetCol()
	rightRef := fun.Args[1].GetCol()
	if leftRef == nil || rightRef == nil {
		return nil, nil
	}
	if leftRef.RelPos != leftTag {
		leftRef, rightRef = rightRef, leftRef
	}
	if leftRef.RelPos != leftTag || rightRef.RelPos != rightTag {
		return nil, nil
	}
	return &plan.Expr_Col{Col: leftRef}, &plan.Expr_Col{Col: rightRef}
}

func replaceAllColRefInExprList(exprlist []*plan.Expr, from []*plan.Expr_Col, to []*plan.Expr_Col) {
	for _, expr := range exprlist {
		if expr == nil {
			continue
		}
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
		anyValueExpr, _ := BindFuncExprImplByPlanExpr(builder.compCtx.GetContext(), "any_value", []*plan.Expr{DeepCopyExpr(expr)})
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
		if expr == nil {
			continue
		}
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
	for _, orderby := range node.OrderBy {
		addAnyValueForNonPKCol(orderby.Expr, cols, agg, builder)
	}
}

func applyAggPullup(rootID int32, join, agg, leftScan, rightScan *plan.Node, builder *QueryBuilder) bool {
	//rightcol must be primary key of right table
	// or we  add rowid in group by, implement this in the future
	pkPositions, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(rightScan.TableDef)
	if !ok || len(agg.BindingTags) == 0 || len(leftScan.BindingTags) != 1 ||
		len(rightScan.BindingTags) != 1 || leftScan.TableDef == nil ||
		agg.Stats == nil || leftScan.Stats == nil || join.Stats == nil {
		return false
	}

	if len(join.OnList) != len(pkPositions) || len(join.OnList) != len(agg.GroupBy) || !builder.IsEquiJoin(join) {
		return false
	}

	leftCols := make([]*plan.Expr_Col, len(join.OnList))
	rightColPos := make([]int32, len(join.OnList))
	rightCols := make([]*plan.Expr_Col, len(join.OnList))
	groupColsInAgg := make([]*plan.ColRef, len(agg.GroupBy))
	seenGroupOutput := make(map[int32]struct{}, len(join.OnList))

	for i, groupExpr := range agg.GroupBy {
		groupCol := groupExpr.GetCol()
		if groupCol == nil || groupCol.RelPos != leftScan.BindingTags[0] ||
			groupCol.ColPos < 0 || int(groupCol.ColPos) >= len(leftScan.TableDef.Cols) ||
			leftScan.TableDef.Cols[groupCol.ColPos] == nil ||
			!sqlEqualityJoinUsesOneIdentityDomain(
				groupExpr.Typ, leftScan.TableDef.Cols[groupCol.ColPos].Typ) {
			return false
		}
		groupColsInAgg[i] = groupCol
	}

	for i := range join.OnList {
		leftCol, rightCol := getJoinCondCol(join.OnList[i], agg.BindingTags[0], rightScan.BindingTags[0])
		joinFn := join.OnList[i].GetF()
		if leftCol == nil || rightCol == nil || joinFn == nil || !sqlEqualityJoinUsesOneIdentityDomain(
			joinFn.Args[0].Typ, joinFn.Args[1].Typ) {
			return false
		}
		groupOutput := leftCol.Col.ColPos
		if groupOutput < 0 || int(groupOutput) >= len(agg.GroupBy) {
			return false
		}
		if _, duplicate := seenGroupOutput[groupOutput]; duplicate {
			return false
		}
		rightPos := rightCol.Col.ColPos
		if rightPos < 0 || int(rightPos) >= len(rightScan.TableDef.Cols) ||
			rightScan.TableDef.Cols[rightPos] == nil ||
			!sqlEqualityJoinUsesOneIdentityDomain(
				joinFn.Args[0].Typ, agg.GroupBy[groupOutput].Typ) ||
			!sqlEqualityJoinUsesOneIdentityDomain(
				joinFn.Args[1].Typ, rightScan.TableDef.Cols[rightPos].Typ) {
			return false
		}
		seenGroupOutput[groupOutput] = struct{}{}
		leftCols[i] = leftCol
		rightCols[i] = rightCol
		rightColPos[i] = rightPos
	}
	if !containsAllSQLEqualityCompatiblePKs(rightColPos, rightScan.TableDef) {
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
		leftCols[i].Col.RelPos = groupColsInAgg[j].RelPos
		leftCols[i].Col.ColPos = groupColsInAgg[j].ColPos
	}
	return true

}

func (builder *QueryBuilder) aggPullup(rootID, nodeID int32) int32 {
	// agg pullup only support node->agg->(filter)->inner join  for now
	// we can change it to node->(filter)->inner join->agg
	if builder.optimizerHints != nil && builder.optimizerHints.aggPullUp != 0 {
		return nodeID
	}
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
