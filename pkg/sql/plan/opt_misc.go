// Copyright 2023 Matrix Origin
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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func (builder *QueryBuilder) countColRefs(nodeID int32, colRefCnt map[[2]int32]int) {
	node := builder.qry.Nodes[nodeID]

	increaseRefCntForExprList(node.ProjectList, 1, colRefCnt)
	increaseRefCntForExprList(node.OnList, 1, colRefCnt)
	increaseRefCntForExprList(node.FilterList, 1, colRefCnt)
	increaseRefCntForExprList(node.GroupBy, 1, colRefCnt)
	increaseRefCntForExprList(node.GroupingSet, 1, colRefCnt)
	increaseRefCntForExprList(node.AggList, 1, colRefCnt)
	increaseRefCntForExprList(node.WinSpecList, 1, colRefCnt)
	for i := range node.OrderBy {
		increaseRefCnt(node.OrderBy[i].Expr, 1, colRefCnt)
	}

	for _, childID := range node.Children {
		builder.countColRefs(childID, colRefCnt)
	}
}

// removeSimpleProjections On top of each subquery or view it has a PROJECT node, which interrupts optimizer rules such as join order.
func (builder *QueryBuilder) removeSimpleProjections(nodeID int32, parentType plan.Node_NodeType, flag bool, colRefCnt map[[2]int32]int) (int32, map[[2]int32]*plan.Expr) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_SINK {
		return builder.removeSimpleProjections(node.Children[0], plan.Node_UNKNOWN, flag, colRefCnt)
	}
	projMap := make(map[[2]int32]*plan.Expr)

	switch node.NodeType {
	case plan.Node_JOIN:
		leftFlag := flag || node.JoinType == plan.Node_RIGHT || node.JoinType == plan.Node_OUTER
		rightFlag := flag || node.JoinType == plan.Node_LEFT || node.JoinType == plan.Node_OUTER

		newChildID, childProjMap := builder.removeSimpleProjections(node.Children[0], plan.Node_JOIN, leftFlag, colRefCnt)
		node.Children[0] = newChildID
		for ref, expr := range childProjMap {
			projMap[ref] = expr
		}

		newChildID, childProjMap = builder.removeSimpleProjections(node.Children[1], plan.Node_JOIN, rightFlag, colRefCnt)
		node.Children[1] = newChildID
		for ref, expr := range childProjMap {
			projMap[ref] = expr
		}

	case plan.Node_AGG, plan.Node_PROJECT, plan.Node_WINDOW, plan.Node_TIME_WINDOW, plan.Node_FILL:
		for i, childID := range node.Children {
			newChildID, childProjMap := builder.removeSimpleProjections(childID, node.NodeType, false, colRefCnt)
			node.Children[i] = newChildID
			for ref, expr := range childProjMap {
				projMap[ref] = expr
			}
		}

	default:
		for i, childID := range node.Children {
			newChildID, childProjMap := builder.removeSimpleProjections(childID, node.NodeType, flag, colRefCnt)
			node.Children[i] = newChildID
			for ref, expr := range childProjMap {
				projMap[ref] = expr
			}
		}
	}

	replaceColumnsForNode(node, projMap)

	if builder.canRemoveProject(parentType, node) {
		allColRef := true
		tag := node.BindingTags[0]
		for i, proj := range node.ProjectList {
			if flag || colRefCnt[[2]int32{tag, int32(i)}] > 1 {
				if proj.GetCol() == nil && (proj.GetLit() == nil || flag) {
					allColRef = false
					break
				}
			}
		}

		if allColRef {
			tag := node.BindingTags[0]
			for i, proj := range node.ProjectList {
				projMap[[2]int32{tag, int32(i)}] = proj
			}

			nodeID = node.Children[0]
		}
	}

	return nodeID, projMap
}

func increaseRefCntForExprList(exprs []*plan.Expr, inc int, colRefCnt map[[2]int32]int) {
	for _, expr := range exprs {
		increaseRefCnt(expr, inc, colRefCnt)
	}
}

// FIXME: We should remove PROJECT node for more cases, but keep them now to avoid intricate issues.
func (builder *QueryBuilder) canRemoveProject(parentType plan.Node_NodeType, node *plan.Node) bool {
	if node.NodeType != plan.Node_PROJECT || node.Limit != nil || node.Offset != nil {
		return false
	}

	if parentType == plan.Node_DISTINCT || parentType == plan.Node_UNKNOWN {
		return false
	}
	if parentType == plan.Node_UNION || parentType == plan.Node_UNION_ALL {
		return false
	}
	if parentType == plan.Node_MINUS || parentType == plan.Node_MINUS_ALL {
		return false
	}
	if parentType == plan.Node_INTERSECT || parentType == plan.Node_INTERSECT_ALL {
		return false
	}
	if parentType == plan.Node_FUNCTION_SCAN || parentType == plan.Node_EXTERNAL_FUNCTION {
		return false
	}
	if parentType == plan.Node_DELETE {
		return false
	}
	if parentType == plan.Node_INSERT || parentType == plan.Node_PRE_INSERT || parentType == plan.Node_PRE_INSERT_UK || parentType == plan.Node_PRE_INSERT_SK {
		return false
	}

	childType := builder.qry.Nodes[node.Children[0]].NodeType
	if childType == plan.Node_VALUE_SCAN || childType == plan.Node_EXTERNAL_SCAN {
		return false
	}
	if childType == plan.Node_FUNCTION_SCAN || childType == plan.Node_EXTERNAL_FUNCTION {
		return false
	}

	return true
}

func replaceColumnsForNode(node *plan.Node, projMap map[[2]int32]*plan.Expr) {
	replaceColumnsForExprList(node.ProjectList, projMap)
	replaceColumnsForExprList(node.OnList, projMap)
	replaceColumnsForExprList(node.FilterList, projMap)
	replaceColumnsForExprList(node.GroupBy, projMap)
	replaceColumnsForExprList(node.GroupingSet, projMap)
	replaceColumnsForExprList(node.AggList, projMap)
	replaceColumnsForExprList(node.WinSpecList, projMap)
	for i := range node.OrderBy {
		node.OrderBy[i].Expr = replaceColumnsForExpr(node.OrderBy[i].Expr, projMap)
	}
}

func replaceColumnsForExprList(exprList []*plan.Expr, projMap map[[2]int32]*plan.Expr) {
	for i, expr := range exprList {
		exprList[i] = replaceColumnsForExpr(expr, projMap)
	}
}

func replaceColumnsForExpr(expr *plan.Expr, projMap map[[2]int32]*plan.Expr) *plan.Expr {
	if expr == nil {
		return nil
	}

	switch ne := expr.Expr.(type) {
	case *plan.Expr_Col:
		mapID := [2]int32{ne.Col.RelPos, ne.Col.ColPos}
		if projExpr, ok := projMap[mapID]; ok {
			return DeepCopyExpr(projExpr)
		}

	case *plan.Expr_F:
		for i, arg := range ne.F.Args {
			ne.F.Args[i] = replaceColumnsForExpr(arg, projMap)
		}

	case *plan.Expr_W:
		ne.W.WindowFunc = replaceColumnsForExpr(ne.W.WindowFunc, projMap)
		for i, arg := range ne.W.PartitionBy {
			ne.W.PartitionBy[i] = replaceColumnsForExpr(arg, projMap)
		}
		for i, order := range ne.W.OrderBy {
			ne.W.OrderBy[i].Expr = replaceColumnsForExpr(order.Expr, projMap)
		}
	}
	return expr
}

func (builder *QueryBuilder) swapJoinChildren(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	for _, child := range node.Children {
		builder.swapJoinChildren(child)
	}

	if node.BuildOnLeft {
		node.Children[0], node.Children[1] = node.Children[1], node.Children[0]
		if node.JoinType == plan.Node_LEFT {
			node.JoinType = plan.Node_RIGHT
		}
	}
}

func (builder *QueryBuilder) remapHavingClause(expr *plan.Expr, groupTag, aggregateTag int32, groupSize int32) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if exprImpl.Col.RelPos == groupTag {
			exprImpl.Col.Name = builder.nameByColRef[[2]int32{groupTag, exprImpl.Col.ColPos}]
			exprImpl.Col.RelPos = -1
		} else {
			exprImpl.Col.Name = builder.nameByColRef[[2]int32{aggregateTag, exprImpl.Col.ColPos}]
			exprImpl.Col.RelPos = -2
			exprImpl.Col.ColPos += groupSize
		}

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			builder.remapHavingClause(arg, groupTag, aggregateTag, groupSize)
		}
	}
}

func (builder *QueryBuilder) remapWindowClause(expr *plan.Expr, windowTag int32, projectionSize int32) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if exprImpl.Col.RelPos == windowTag {
			exprImpl.Col.Name = builder.nameByColRef[[2]int32{windowTag, exprImpl.Col.ColPos}]
			exprImpl.Col.RelPos = -1
			exprImpl.Col.ColPos += projectionSize
		}

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			builder.remapWindowClause(arg, windowTag, projectionSize)
		}
	}
}

// if join cond is a=b and a=c, we can remove a=c to improve join performance
func (builder *QueryBuilder) removeRedundantJoinCond(nodeID int32, colMap map[[2]int32]int, colGroup []int) []int {
	if builder.optimizerHints != nil && builder.optimizerHints.removeRedundantJoinCond != 0 {
		return colGroup
	}
	node := builder.qry.Nodes[nodeID]
	for i := range node.Children {
		colGroup = builder.removeRedundantJoinCond(node.Children[i], colMap, colGroup)
	}
	if len(node.OnList) == 0 {
		return colGroup
	}

	newOnList := make([]*plan.Expr, 0)
	for _, expr := range node.OnList {
		if exprf := expr.GetF(); exprf != nil {
			if IsEqualFunc(exprf.Func.GetObj()) {
				leftcol := exprf.Args[0].GetCol()
				rightcol := exprf.Args[1].GetCol()
				if leftcol != nil && rightcol != nil {
					left, leftok := colMap[[2]int32{leftcol.RelPos, leftcol.ColPos}]
					if !leftok {
						left = len(colGroup)
						colGroup = append(colGroup, left)
						colMap[[2]int32{leftcol.RelPos, leftcol.ColPos}] = left
					}
					right, rightok := colMap[[2]int32{rightcol.RelPos, rightcol.ColPos}]
					if !rightok {
						right = len(colGroup)
						colGroup = append(colGroup, right)
						colMap[[2]int32{rightcol.RelPos, rightcol.ColPos}] = right
					}
					for colGroup[left] != colGroup[colGroup[left]] {
						colGroup[left] = colGroup[colGroup[left]]
					}
					for colGroup[right] != colGroup[colGroup[right]] {
						colGroup[right] = colGroup[colGroup[right]]
					}
					if colGroup[left] == colGroup[right] {
						continue
					}
					newOnList = append(newOnList, expr)
					colGroup[colGroup[left]] = colGroup[right]
				} else {
					newOnList = append(newOnList, expr)
				}
			} else {
				newOnList = append(newOnList, expr)
			}
		} else {
			newOnList = append(newOnList, expr)
		}
	}
	node.OnList = newOnList

	return colGroup
}

func (builder *QueryBuilder) removeEffectlessLeftJoins(nodeID int32, tagCnt map[int32]int) int32 {
	if builder.optimizerHints != nil && builder.optimizerHints.removeEffectLessLeftJoins != 0 {
		return nodeID
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) == 0 {
		return nodeID
	}

	increaseTagCntForExprList(node.ProjectList, 1, tagCnt)
	increaseTagCntForExprList(node.OnList, 1, tagCnt)
	increaseTagCntForExprList(node.FilterList, 1, tagCnt)
	increaseTagCntForExprList(node.GroupBy, 1, tagCnt)
	increaseTagCntForExprList(node.GroupingSet, 1, tagCnt)
	increaseTagCntForExprList(node.AggList, 1, tagCnt)
	increaseTagCntForExprList(node.WinSpecList, 1, tagCnt)
	for i := range node.OrderBy {
		increaseTagCnt(node.OrderBy[i].Expr, 1, tagCnt)
	}
	for i, childID := range node.Children {
		node.Children[i] = builder.removeEffectlessLeftJoins(childID, tagCnt)
	}
	increaseTagCntForExprList(node.OnList, -1, tagCnt)

	if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_LEFT {
		goto END
	}

	// if output column is in right, can not optimize this one
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		if tagCnt[tag] > 0 {
			goto END
		}
	}

	//reuse hash on primary key logic
	if !node.Stats.HashmapStats.HashOnPK {
		goto END
	}

	nodeID = node.Children[0]

END:
	increaseTagCntForExprList(node.ProjectList, -1, tagCnt)
	increaseTagCntForExprList(node.FilterList, -1, tagCnt)
	increaseTagCntForExprList(node.GroupBy, -1, tagCnt)
	increaseTagCntForExprList(node.GroupingSet, -1, tagCnt)
	increaseTagCntForExprList(node.AggList, -1, tagCnt)
	increaseTagCntForExprList(node.WinSpecList, -1, tagCnt)
	for i := range node.OrderBy {
		increaseTagCnt(node.OrderBy[i].Expr, -1, tagCnt)
	}

	return nodeID
}

func increaseTagCntForExprList(exprs []*plan.Expr, inc int, tagCnt map[int32]int) {
	for _, expr := range exprs {
		increaseTagCnt(expr, inc, tagCnt)
	}
}

func increaseTagCnt(expr *plan.Expr, inc int, tagCnt map[int32]int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		tagCnt[exprImpl.Col.RelPos] += inc

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			increaseTagCnt(arg, inc, tagCnt)
		}
	case *plan.Expr_W:
		increaseTagCnt(exprImpl.W.WindowFunc, inc, tagCnt)
		for _, arg := range exprImpl.W.PartitionBy {
			increaseTagCnt(arg, inc, tagCnt)
		}
		for _, order := range exprImpl.W.OrderBy {
			increaseTagCnt(order.Expr, inc, tagCnt)
		}
	}
}

func findHashOnPKTable(nodeID, tag int32, builder *QueryBuilder) *plan.TableDef {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_TABLE_SCAN {
		if node.BindingTags[0] == tag {
			return node.TableDef
		}
	} else if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_INNER {
		if node.Stats.HashmapStats.HashOnPK {
			return findHashOnPKTable(node.Children[0], tag, builder)
		}
	}
	return nil
}

func determineHashOnPK(nodeID int32, builder *QueryBuilder) {
	if builder.optimizerHints != nil && builder.optimizerHints.determineHashOnPK != 0 {
		return
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			determineHashOnPK(child, builder)
		}
	}

	if node.NodeType != plan.Node_JOIN {
		return
	}

	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}

	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
	}

	exprs := make([]*plan.Expr, 0)
	for _, expr := range node.OnList {
		if equi := isEquiCond(expr, leftTags, rightTags); equi {
			exprs = append(exprs, expr)
		}
	}

	hashCols := make([]*plan.ColRef, 0)
	for _, cond := range exprs {
		switch condImpl := cond.Expr.(type) {
		case *plan.Expr_F:
			expr := condImpl.F.Args[1]
			switch exprImpl := expr.Expr.(type) {
			case *plan.Expr_Col:
				hashCols = append(hashCols, exprImpl.Col)
			}
		}
	}

	if len(hashCols) == 0 {
		return
	}

	tableDef := findHashOnPKTable(node.Children[1], hashCols[0].RelPos, builder)
	if tableDef == nil {
		return
	}
	hashColPos := make([]int32, len(hashCols))
	for i := range hashCols {
		hashColPos[i] = hashCols[i].ColPos
	}
	if containsAllPKs(hashColPos, tableDef) {
		node.Stats.HashmapStats.HashOnPK = true
	}

}

func getHashColsNDVRatio(nodeID int32, builder *QueryBuilder) float64 {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType != plan.Node_JOIN {
		return 1
	}
	result := getHashColsNDVRatio(builder.qry.Nodes[node.Children[1]].NodeId, builder)

	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}

	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
	}

	exprs := make([]*plan.Expr, 0)
	for _, expr := range node.OnList {
		if equi := isEquiCond(expr, leftTags, rightTags); equi {
			exprs = append(exprs, expr)
		}
	}

	hashCols := make([]*plan.ColRef, 0)
	for _, cond := range exprs {
		switch condImpl := cond.Expr.(type) {
		case *plan.Expr_F:
			expr := condImpl.F.Args[1]
			switch exprImpl := expr.Expr.(type) {
			case *plan.Expr_Col:
				hashCols = append(hashCols, exprImpl.Col)
			}
		}
	}

	if len(hashCols) == 0 {
		return 0.0001
	}

	tableDef := findHashOnPKTable(node.Children[1], hashCols[0].RelPos, builder)
	if tableDef == nil {
		return 0.0001
	}
	hashColPos := make([]int32, len(hashCols))
	for i := range hashCols {
		hashColPos[i] = hashCols[i].ColPos
	}
	return builder.getColNDVRatio(hashColPos, tableDef) * result
}

func checkExprInTags(expr *plan.Expr, tags []int32) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i := range exprImpl.F.Args {
			if !checkExprInTags(exprImpl.F.Args[i], tags) {
				return false
			}
		}
		return true

	case *plan.Expr_Col:
		for i := range tags {
			if tags[i] == exprImpl.Col.RelPos {
				return true
			}
		}
	}
	return false
}

func (builder *QueryBuilder) rewriteDistinctToAGG(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			builder.rewriteDistinctToAGG(child)
		}
	}
	if node.NodeType != plan.Node_DISTINCT {
		return
	}
	project := builder.qry.Nodes[node.Children[0]]
	if project.NodeType != plan.Node_PROJECT {
		return
	}
	if builder.qry.Nodes[project.Children[0]].NodeType == plan.Node_VALUE_SCAN {
		return
	}

	node.NodeType = plan.Node_AGG
	node.GroupBy = project.ProjectList
	node.BindingTags = project.BindingTags
	node.BindingTags = append(node.BindingTags, builder.genNewTag())
	node.Children[0] = project.Children[0]
}

// reuse removeSimpleProjections to delete this plan node
func (builder *QueryBuilder) rewriteEffectlessAggToProject(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			builder.rewriteEffectlessAggToProject(child)
		}
	}
	if node.NodeType != plan.Node_AGG {
		return
	}
	if node.AggList != nil || node.ProjectList != nil || node.FilterList != nil {
		return
	}
	scan := builder.qry.Nodes[node.Children[0]]
	if scan.NodeType != plan.Node_TABLE_SCAN {
		return
	}
	groupCol := make([]int32, 0)
	for _, expr := range node.GroupBy {
		if col := expr.GetCol(); col != nil {
			groupCol = append(groupCol, col.ColPos)
		}
	}
	if !containsAllPKs(groupCol, scan.TableDef) {
		return
	}
	node.NodeType = plan.Node_PROJECT
	node.BindingTags = node.BindingTags[:1]
	node.ProjectList = node.GroupBy
	node.GroupBy = nil
}

func makeBetweenExprFromDateFormat(equalFunc *plan.Function, dateformatFunc *plan.Function, intervalStr string, builder *QueryBuilder) *plan.Expr {
	dateExpr := DeepCopyExpr(equalFunc.Args[1])
	if intervalStr == "year" {
		sval, _ := dateExpr.GetLit().GetValue().(*plan.Literal_Sval)
		sval.Sval = sval.Sval + "0101"
	}
	begin, err := forceCastExpr(builder.GetContext(), dateExpr, dateformatFunc.Args[0].Typ)
	if err != nil {
		return nil
	}
	begin, err = ConstantFold(batch.EmptyForConstFoldBatch, begin, builder.compCtx.GetProcess(), false)
	if err != nil {
		return nil
	}
	interval := MakeIntervalExpr(1, intervalStr)
	end, err := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "+", []*Expr{DeepCopyExpr(begin), interval})
	if err != nil {
		return nil
	}
	interval = MakeIntervalExpr(1, "microsecond")
	end, err = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "-", []*Expr{DeepCopyExpr(end), interval})
	if err != nil {
		return nil
	}
	args := []*Expr{dateformatFunc.Args[0], begin, end}
	newFilter, err := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "between", args)
	if err != nil {
		return nil
	}
	return newFilter
}

func (builder *QueryBuilder) optimizeDateFormatExpr(nodeID int32) {
	if builder.optimizerHints != nil && builder.optimizerHints.optimizeDateFormatExpr != 0 {
		return
	}
	// for date_format(col,'%Y-%m-%d')= '2024-01-19', change this to col between [2024-01-19 00:00:00,2024-01-19 23:59:59]
	node := builder.qry.Nodes[nodeID]
	for _, childID := range node.Children {
		builder.optimizeDateFormatExpr(childID)
	}
	if node.NodeType != plan.Node_TABLE_SCAN || len(node.FilterList) == 0 {
		return
	}
	for i := range node.FilterList {
		expr := node.FilterList[i]
		equalFunc := expr.GetF()
		if equalFunc != nil && equalFunc.Func.ObjName == "=" {
			dateformatFunc := equalFunc.Args[0].GetF()
			if dateformatFunc == nil || dateformatFunc.Func.ObjName != "date_format" {
				continue
			}
			col := dateformatFunc.Args[0].GetCol()
			if col == nil {
				continue
			}
			if dateformatFunc.Args[1].GetLit() == nil {
				continue
			}
			str := dateformatFunc.Args[1].GetLit().GetSval()
			if len(str) == 0 {
				continue
			}
			if equalFunc.Args[1].GetLit() == nil {
				continue
			}
			dateSval := equalFunc.Args[1].GetLit().GetSval()
			var newFilter *plan.Expr
			switch str {
			case "%Y-%m-%d":
				if len(dateSval) != 10 || dateSval[4] != '-' || dateSval[7] != '-' {
					continue
				}
				newFilter = makeBetweenExprFromDateFormat(equalFunc, dateformatFunc, "day", builder)
			case "%Y%m%d":
				if len(dateSval) != 8 {
					continue
				}
				newFilter = makeBetweenExprFromDateFormat(equalFunc, dateformatFunc, "day", builder)
			case "%Y":
				if len(dateSval) != 4 {
					continue
				}
				newFilter = makeBetweenExprFromDateFormat(equalFunc, dateformatFunc, "year", builder)
			}
			if newFilter != nil {
				node.FilterList[i] = newFilter
			}
		}
	}
}

func (builder *QueryBuilder) optimizeLikeExpr(nodeID int32) {
	if builder.optimizerHints != nil && builder.optimizerHints.optimizeLikeExpr != 0 {
		return
	}
	// for a like "abc%", change it to prefix_equal(a,"abc")
	// for a like "abc%def", add an extra filter prefix_equal(a,"abc")
	node := builder.qry.Nodes[nodeID]

	for _, childID := range node.Children {
		builder.optimizeLikeExpr(childID)
	}
	if node.NodeType != plan.Node_TABLE_SCAN || len(node.FilterList) == 0 {
		return
	}
	var newFilters []*plan.Expr
	for i := range node.FilterList {
		expr := node.FilterList[i]
		fun := expr.GetF()
		if fun != nil && fun.Func.ObjName == "like" {
			col := fun.Args[0].GetCol()
			if col == nil {
				continue
			}
			if fun.Args[1].GetLit() == nil {
				continue
			}
			str := fun.Args[1].GetLit().GetSval()
			if len(str) == 0 {
				continue
			}
			index1 := strings.IndexByte(str, '_')
			if index1 > 0 && str[index1-1] == '\\' {
				index1--
			}
			index2 := strings.IndexByte(str, '%')
			if index2 > 0 && str[index2-1] == '\\' {
				index2--
			}
			if index1 == -1 && index2 == -1 {
				// it's col like string without wildcard, can change to equal
				fun.Func.ObjName = function.EqualFunctionName
				fun.Func.Obj = function.EqualFunctionEncodedID
				continue
			}

			indexOfWildCard := index1
			if index1 == -1 {
				indexOfWildCard = index2
			}
			if index2 != -1 && index2 < index1 {
				indexOfWildCard = index2
			}
			if indexOfWildCard <= 0 {
				continue
			}
			newStr := str[:indexOfWildCard]

			newFilter := node.FilterList[i]
			// if no _ and % in the last, we can replace the origin filter
			replaceOrigin := (index1 == -1) && (index2 == len(str)-1)
			if !replaceOrigin {
				newFilter = DeepCopyExpr(newFilter)
				newFilters = append(newFilters, newFilter)
			}
			newFunc := newFilter.GetF()
			newFunc.Func.ObjName = function.PrefixEqualFunctionName
			newFunc.Func.Obj = function.PrefixEqualFunctionEncodedID
			newFunc.Args[1].GetLit().Value.(*plan.Literal_Sval).Sval = newStr
			if replaceOrigin {
				node.BlockFilterList = append(node.BlockFilterList, DeepCopyExpr(newFilter))
			}
		}
	}
	if len(newFilters) > 0 {
		node.FilterList = append(node.FilterList, newFilters...)
		node.BlockFilterList = append(node.BlockFilterList, DeepCopyExprList(newFilters)...)
	}
}

func (builder *QueryBuilder) forceJoinOnOneCN(nodeID int32, force bool) {
	if builder.optimizerHints != nil && builder.optimizerHints.forceOneCN != 0 {
		return
	}

	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_TABLE_SCAN {
		node.Stats.ForceOneCN = force
	} else if node.NodeType == plan.Node_JOIN {
		if len(node.RuntimeFilterBuildList) > 0 {
			switch node.JoinType {
			case plan.Node_RIGHT:
				if !node.Stats.HashmapStats.Shuffle {
					force = true
				}
			case plan.Node_SEMI, plan.Node_ANTI:
				if node.BuildOnLeft && !node.Stats.HashmapStats.Shuffle {
					force = true
				}
			case plan.Node_INDEX:
				force = true
			}
		}
	}
	for _, childID := range node.Children {
		builder.forceJoinOnOneCN(childID, force)
	}
}

func handleOptimizerHints(str string, builder *QueryBuilder) {
	strs := strings.Split(str, "=")
	if len(strs) != 2 {
		return
	}
	key := strs[0]
	value, err := strconv.Atoi(strs[1])
	if err != nil {
		return
	}
	if builder.optimizerHints == nil {
		builder.optimizerHints = &OptimizerHints{}
	}
	switch key {
	case "pushDownLimitToScan":
		builder.optimizerHints.pushDownLimitToScan = value
	case "pushDownTopThroughLeftJoin":
		builder.optimizerHints.pushDownTopThroughLeftJoin = value
	case "pushDownSemiAntiJoins":
		builder.optimizerHints.pushDownSemiAntiJoins = value
	case "aggPushDown":
		builder.optimizerHints.aggPushDown = value
	case "aggPullUp":
		builder.optimizerHints.aggPullUp = value
	case "removeEffectLessLeftJoins":
		builder.optimizerHints.removeEffectLessLeftJoins = value
	case "removeRedundantJoinCond":
		builder.optimizerHints.removeRedundantJoinCond = value
	case "optimizeLikeExpr":
		builder.optimizerHints.optimizeLikeExpr = value
	case "optimizeDateFormatExpr":
		builder.optimizerHints.optimizeDateFormatExpr = value
	case "determineHashOnPK":
		builder.optimizerHints.determineHashOnPK = value
	case "sendMessageFromTopToScan":
		builder.optimizerHints.sendMessageFromTopToScan = value
	case "determineShuffle":
		builder.optimizerHints.determineShuffle = value
	case "blockFilter":
		builder.optimizerHints.blockFilter = value
	case "applyIndices":
		builder.optimizerHints.applyIndices = value
	case "runtimeFilter":
		builder.optimizerHints.runtimeFilter = value
	case "joinOrdering":
		builder.optimizerHints.joinOrdering = value
	case "forceOneCN":
		builder.optimizerHints.forceOneCN = value
	case "execType":
		builder.optimizerHints.execType = value
	}
}

func (builder *QueryBuilder) parseOptimizeHints() {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables("optimizer_hints")
	if !ok {
		return
	}
	str := v.(string)
	if len(str) == 0 {
		return
	}
	kvs := strings.Split(str, ",")
	for i := range kvs {
		handleOptimizerHints(kvs[i], builder)
	}
}
