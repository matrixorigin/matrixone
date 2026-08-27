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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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

// partialSumPushdownCandidate describes the safe half of a late dimension
// join rewrite. The original aggregate stays above the join, so dimension rows
// with equal display attributes are still merged exactly as the SQL requires.
// Only a partial SUM is inserted on the fact side.
type partialSumPushdownCandidate struct {
	factChildPos    int
	factNode        *plan.Node
	factJoinCols    []*plan.Expr
	joinGroupPos    []int
	partialGroup    []*plan.Expr
	partialGroupNdv []float64
	groupRemap      map[int]int
	partialRows     float64
}

func appendUniqueExpr(exprs []*plan.Expr, expr *plan.Expr) ([]*plan.Expr, int) {
	for i, existing := range exprs {
		if exprStructuralEqual(existing, expr) {
			return exprs, i
		}
	}
	return append(exprs, DeepCopyExpr(expr)), len(exprs)
}

func directColFromTags(expr *plan.Expr, tags map[int32]bool) *plan.Expr_Col {
	col, ok := expr.Expr.(*plan.Expr_Col)
	if !ok || !tags[col.Col.RelPos] {
		return nil
	}
	return col
}

// finiteColumnDomain returns the exact finite set forced on one column by a
// boolean predicate. Unknown means the predicate may admit any column value.
// This is deliberately limited to equality/IN combined with AND/OR; ranges and
// other predicates fall back to the base NDV instead of guessing.
func finiteColumnDomain(expr *plan.Expr, relPos, colPos int32) (map[string]struct{}, bool) {
	if lit := expr.GetLit(); lit != nil {
		if lit.Isnull {
			return map[string]struct{}{}, true
		}
		if value, ok := lit.Value.(*plan.Literal_Bval); ok && !value.Bval {
			return map[string]struct{}{}, true
		}
		return nil, false
	}

	fn := expr.GetF()
	if fn == nil {
		return nil, false
	}
	switch fn.Func.ObjName {
	case "=":
		if len(fn.Args) != 2 {
			return nil, false
		}
		operand, ok := extractDomainFilterOperand(fn.Args[0])
		value := fn.Args[1]
		if !ok {
			operand, ok = extractDomainFilterOperand(fn.Args[1])
			value = fn.Args[0]
		}
		if _, _, literal := unwrapConstLiteral(value); !literal {
			ok = false
		}
		// A narrowing cast can map many source values to one literal, so it
		// does not bound the NDV of the underlying column.
		if !ok || operand.hasCast || operand.relPos != relPos || operand.colPos != colPos {
			return nil, false
		}
		key, ok := constLiteralKeyForOperand(value, operand)
		if !ok {
			return nil, false
		}
		return map[string]struct{}{key: {}}, true
	case "in":
		operand, values, ok := extractInListFilterForDomain(expr)
		if !ok || operand.hasCast || operand.relPos != relPos || operand.colPos != colPos {
			return nil, false
		}
		domain := make(map[string]struct{}, len(values))
		for _, value := range values {
			key, ok := constLiteralKeyForOperand(value, operand)
			if !ok {
				return nil, false
			}
			domain[key] = struct{}{}
		}
		return domain, true
	case "and":
		var domain map[string]struct{}
		known := false
		for _, arg := range fn.Args {
			argDomain, argKnown := finiteColumnDomain(arg, relPos, colPos)
			if !argKnown {
				continue
			}
			if !known {
				domain = argDomain
				known = true
				continue
			}
			for key := range domain {
				if _, exists := argDomain[key]; !exists {
					delete(domain, key)
				}
			}
		}
		return domain, known
	case "or":
		domain := make(map[string]struct{})
		for _, arg := range fn.Args {
			argDomain, known := finiteColumnDomain(arg, relPos, colPos)
			if !known {
				return nil, false
			}
			for key := range argDomain {
				domain[key] = struct{}{}
			}
		}
		return domain, true
	default:
		return nil, false
	}
}

func (builder *QueryBuilder) getExprNdvAfterFilters(expr *plan.Expr) float64 {
	ndv := getExprNdv(expr, builder)
	col := expr.GetCol()
	if col == nil {
		return ndv
	}
	scanID, ok := builder.tag2NodeID[col.RelPos]
	if !ok || scanID < 0 || int(scanID) >= len(builder.qry.Nodes) {
		return ndv
	}
	scan := builder.qry.Nodes[scanID]
	if scan.NodeType != plan.Node_TABLE_SCAN {
		return ndv
	}
	for _, filter := range scan.FilterList {
		domain, known := finiteColumnDomain(filter, col.RelPos, col.ColPos)
		if known && len(domain) > 0 && (ndv <= 0 || float64(len(domain)) < ndv) {
			ndv = float64(len(domain))
		}
	}
	return ndv
}

func (builder *QueryBuilder) buildPartialSumPushdownCandidate(
	agg, join *plan.Node,
	factChildPos int,
) *partialSumPushdownCandidate {
	dimChildPos := 1 - factChildPos
	factNode := builder.qry.Nodes[join.Children[factChildPos]]
	dimScan := builder.qry.Nodes[join.Children[dimChildPos]]
	if dimScan.NodeType != plan.Node_TABLE_SCAN || len(dimScan.BindingTags) != 1 || dimScan.TableDef == nil {
		return nil
	}

	dimTag := dimScan.BindingTags[0]
	factTags := builder.collectBindingTags(factNode)
	if factTags[dimTag] {
		return nil
	}

	pkPositions, ok := primaryKeyColumnPositions(dimScan.TableDef)
	if !ok {
		return nil
	}
	joinedDimColumns := make(map[int32]struct{}, len(join.OnList))
	factJoinCols := make([]*plan.Expr, 0, len(join.OnList))
	for _, cond := range join.OnList {
		fn := cond.GetF()
		if fn == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 {
			return nil
		}
		var factExpr *plan.Expr
		var factCol, dimCol *plan.Expr_Col
		if col := directColFromTags(fn.Args[0], factTags); col != nil {
			factExpr = fn.Args[0]
			factCol = col
			if col, ok := fn.Args[1].Expr.(*plan.Expr_Col); ok && col.Col.RelPos == dimTag {
				dimCol = col
			}
		} else if col := directColFromTags(fn.Args[1], factTags); col != nil {
			factExpr = fn.Args[1]
			factCol = col
			if col, ok := fn.Args[0].Expr.(*plan.Expr_Col); ok && col.Col.RelPos == dimTag {
				dimCol = col
			}
		}
		if factCol == nil || dimCol == nil {
			return nil
		}
		joinedDimColumns[dimCol.Col.ColPos] = struct{}{}
		factJoinCols = append(factJoinCols, GetColExpr(factExpr.Typ, factCol.Col.RelPos, factCol.Col.ColPos))
	}
	for _, pkPos := range pkPositions {
		if _, joined := joinedDimColumns[pkPos]; !joined {
			return nil
		}
	}

	partialGroup := make([]*plan.Expr, 0, len(agg.GroupBy)+len(factJoinCols))
	groupRemap := make(map[int]int)
	hasDimensionGroup := false
	for i, groupExpr := range agg.GroupBy {
		if ContainsVolatileFunction(groupExpr) {
			return nil
		}
		side := getJoinSide(groupExpr, factTags, map[int32]bool{dimTag: true}, -1)
		switch side {
		case JoinSideLeft:
			// getJoinSide's left means the factTags argument above.
			var pos int
			partialGroup, pos = appendUniqueExpr(partialGroup, groupExpr)
			groupRemap[i] = pos
		case JoinSideRight:
			hasDimensionGroup = true
		case 0:
			// Only literal constants are safe here. A zero-column volatile
			// expression (for example rand()) must not be evaluated after rows
			// have already been collapsed by the partial aggregate.
			if groupExpr.GetLit() == nil {
				return nil
			}
		default:
			return nil
		}
	}
	if !hasDimensionGroup {
		return nil
	}

	for _, aggExpr := range agg.AggList {
		fn := aggExpr.GetF()
		if fn == nil || fn.Func.ObjName != "sum" || len(fn.Args) != 1 ||
			len(fn.AggConfig) != 0 || fn.AggConfigType != plan.AggregateConfigType_AGG_CONFIG_NONE ||
			uint64(fn.Func.Obj)&function.Distinct != 0 ||
			ContainsVolatileFunction(fn.Args[0]) ||
			getJoinSide(fn.Args[0], factTags, map[int32]bool{dimTag: true}, -1) != JoinSideLeft {
			return nil
		}
	}
	if len(agg.AggList) == 0 {
		return nil
	}

	joinGroupPos := make([]int, len(factJoinCols))
	for i, joinCol := range factJoinCols {
		var pos int
		partialGroup, pos = appendUniqueExpr(partialGroup, joinCol)
		joinGroupPos[i] = pos
	}

	// A partial aggregate adds one full pass over the fact rows. It pays for
	// itself only if the two downstream operators (join and final aggregate)
	// process fewer than half as many rows. Unknown NDVs reject the rewrite.
	partialRows := 1.0
	if factNode.Stats == nil || dimScan.Stats == nil || join.Stats == nil ||
		factNode.Stats.Outcnt <= 0 || dimScan.Stats.Outcnt <= 0 || join.Stats.Outcnt <= 0 ||
		math.IsNaN(factNode.Stats.Outcnt) || math.IsInf(factNode.Stats.Outcnt, 0) ||
		math.IsNaN(dimScan.Stats.Outcnt) || math.IsInf(dimScan.Stats.Outcnt, 0) ||
		math.IsNaN(join.Stats.Outcnt) || math.IsInf(join.Stats.Outcnt, 0) {
		return nil
	}
	// A PK join proves that matched fact keys contribute at most one distinct
	// key per dimension row. Unmatched fact keys may all be distinct, so keep
	// them in the bound instead of blindly capping by dimension cardinality.
	unmatchedFactRows := math.Max(factNode.Stats.Outcnt-join.Stats.Outcnt, 0)
	joinKeyNdvUpper := math.Min(factNode.Stats.Outcnt, dimScan.Stats.Outcnt+unmatchedFactRows)
	partialGroupNdv := make([]float64, 0, len(partialGroup))
	for _, groupExpr := range partialGroup {
		ndv := builder.getExprNdvAfterFilters(groupExpr)
		if ndv <= 0 || math.IsNaN(ndv) || math.IsInf(ndv, 0) {
			return nil
		}
		for _, joinCol := range factJoinCols {
			if exprStructuralEqual(groupExpr, joinCol) {
				ndv = math.Min(ndv, joinKeyNdvUpper)
				break
			}
		}
		partialGroupNdv = append(partialGroupNdv, ndv)
		partialRows *= ndv
		if partialRows >= join.Stats.Outcnt/2 {
			return nil
		}
	}

	return &partialSumPushdownCandidate{
		factChildPos:    factChildPos,
		factNode:        factNode,
		factJoinCols:    factJoinCols,
		joinGroupPos:    joinGroupPos,
		partialGroup:    partialGroup,
		partialGroupNdv: partialGroupNdv,
		groupRemap:      groupRemap,
		partialRows:     partialRows,
	}
}

func (builder *QueryBuilder) applyPartialSumPushdown(
	agg, join *plan.Node,
	candidate *partialSumPushdownCandidate,
) bool {
	partialAggList := DeepCopyExprList(agg.AggList)
	newGroupTag := builder.genNewBindTag()
	newAggTag := builder.genNewBindTag()

	finalAggList := make([]*plan.Expr, len(agg.AggList))
	for i, original := range agg.AggList {
		arg := GetColExpr(original.Typ, newAggTag, int32(i))
		final, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "sum", []*plan.Expr{arg})
		if err != nil {
			return false
		}
		// A two-stage SUM is an execution strategy, not a SQL type change.
		final.Typ = original.Typ
		finalAggList[i] = final
	}

	partialNode := &plan.Node{
		NodeType:    plan.Node_AGG,
		Children:    []int32{candidate.factNode.NodeId},
		GroupBy:     candidate.partialGroup,
		AggList:     partialAggList,
		BindingTags: []int32{newGroupTag, newAggTag},
		SpillMem:    builder.aggSpillMem,
	}
	if builder.derivedColNdv == nil {
		builder.derivedColNdv = make(map[[2]int32]float64)
	}
	for i, ndv := range candidate.partialGroupNdv {
		builder.derivedColNdv[[2]int32{newGroupTag, int32(i)}] = ndv
	}
	partialNodeID := builder.appendNode(partialNode, builder.ctxByNode[join.NodeId])
	partialNode.Stats.Outcnt = candidate.partialRows
	builder.determineGroupByHashKey(partialNode)
	join.Children[candidate.factChildPos] = partialNodeID

	for i, joinCol := range candidate.factJoinCols {
		col := joinCol.GetCol()
		replaceAllColRefInExprList(join.OnList,
			[]*plan.Expr_Col{{Col: &plan.ColRef{RelPos: col.RelPos, ColPos: col.ColPos}}},
			[]*plan.Expr_Col{{Col: &plan.ColRef{RelPos: newGroupTag, ColPos: int32(candidate.joinGroupPos[i])}}})
	}

	for groupPos, partialPos := range candidate.groupRemap {
		agg.GroupBy[groupPos] = GetColExpr(agg.GroupBy[groupPos].Typ, newGroupTag, int32(partialPos))
	}
	agg.AggList = finalAggList
	return true
}

// pushPartialSumsThroughUniqueDimensions implements a conservative aggregate
// pushdown after join ordering. Constraints prove correctness; statistics only
// decide whether the extra aggregation is likely to reduce work.
func (builder *QueryBuilder) pushPartialSumsThroughUniqueDimensions(nodeID int32) int32 {
	if builder.optimizerHints != nil && builder.optimizerHints.aggPushDown != 0 {
		return nodeID
	}
	node := builder.qry.Nodes[nodeID]
	for i, childID := range node.Children {
		node.Children[i] = builder.pushPartialSumsThroughUniqueDimensions(childID)
	}
	if node.NodeType != plan.Node_AGG || len(node.GroupBy) == 0 || hasInactiveGroupingColumn(node.GroupingFlag) || len(node.Children) != 1 {
		return nodeID
	}
	join := builder.qry.Nodes[node.Children[0]]
	if join.NodeType != plan.Node_JOIN || join.JoinType != plan.Node_INNER || len(join.OnList) == 0 || join.Stats == nil {
		return nodeID
	}

	for factChildPos := 0; factChildPos < 2; factChildPos++ {
		candidate := builder.buildPartialSumPushdownCandidate(node, join, factChildPos)
		if candidate != nil && builder.applyPartialSumPushdown(node, join, candidate) {
			break
		}
	}
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
