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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// removeSimpleProjections On top of each subquery or view it has a PROJECT node, which interrupts optimizer rules such as join order.
func (builder *QueryBuilder) removeSimpleProjections(nodeID int32, parentType plan.Node_NodeType, flag bool, colRefCnt map[[2]int32]int) (int32, map[[2]int32]*plan.Expr) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_SINK {
		return builder.removeSimpleProjections(node.Children[0], plan.Node_UNKNOWN, flag, colRefCnt)
	}
	projMap := make(map[[2]int32]*plan.Expr)

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

	case plan.Node_AGG, plan.Node_PROJECT, plan.Node_WINDOW:
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

	removeProjectionsForExprList(node.ProjectList, projMap)
	removeProjectionsForExprList(node.OnList, projMap)
	removeProjectionsForExprList(node.FilterList, projMap)
	removeProjectionsForExprList(node.GroupBy, projMap)
	removeProjectionsForExprList(node.GroupingSet, projMap)
	removeProjectionsForExprList(node.AggList, projMap)
	removeProjectionsForExprList(node.WinSpecList, projMap)
	for i := range node.OrderBy {
		node.OrderBy[i].Expr = removeProjectionsForExpr(node.OrderBy[i].Expr, projMap)
	}

	if builder.canRemoveProject(parentType, node) {
		allColRef := true
		tag := node.BindingTags[0]
		for i, proj := range node.ProjectList {
			if flag || colRefCnt[[2]int32{tag, int32(i)}] > 1 {
				if _, ok := proj.Expr.(*plan.Expr_Col); !ok {
					if _, ok := proj.Expr.(*plan.Expr_C); !ok {
						allColRef = false
						break
					}
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
	if parentType == plan.Node_INSERT || parentType == plan.Node_PRE_INSERT || parentType == plan.Node_PRE_INSERT_UK {
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

func removeProjectionsForExprList(exprList []*plan.Expr, projMap map[[2]int32]*plan.Expr) {
	for i, expr := range exprList {
		exprList[i] = removeProjectionsForExpr(expr, projMap)
	}
}

func removeProjectionsForExpr(expr *plan.Expr, projMap map[[2]int32]*plan.Expr) *plan.Expr {
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
			ne.F.Args[i] = removeProjectionsForExpr(arg, projMap)
		}

	case *plan.Expr_W:
		ne.W.WindowFunc = removeProjectionsForExpr(ne.W.WindowFunc, projMap)
		for i, arg := range ne.W.PartitionBy {
			ne.W.PartitionBy[i] = removeProjectionsForExpr(arg, projMap)
		}
		for i, order := range ne.W.OrderBy {
			ne.W.OrderBy[i].Expr = removeProjectionsForExpr(order.Expr, projMap)
		}
	}
	return expr
}

func (builder *QueryBuilder) pushdownFilters(nodeID int32, filters []*plan.Expr, separateNonEquiConds bool) (int32, []*plan.Expr) {
	node := builder.qry.Nodes[nodeID]

	var canPushdown, cantPushdown []*plan.Expr

	switch node.NodeType {
	case plan.Node_AGG:
		groupTag := node.BindingTags[0]
		aggregateTag := node.BindingTags[1]

		for _, filter := range filters {
			if !containsTag(filter, aggregateTag) {
				canPushdown = append(canPushdown, replaceColRefs(filter, groupTag, node.GroupBy))
			} else {
				node.FilterList = append(node.FilterList, filter)
			}
		}

		childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown, separateNonEquiConds)

		if len(cantPushdownChild) > 0 {
			childID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[0] = childID

	case plan.Node_WINDOW:
		windowTag := node.BindingTags[0]

		for _, filter := range filters {
			if !containsTag(filter, windowTag) {
				canPushdown = append(canPushdown, replaceColRefs(filter, windowTag, node.WinSpecList))
			} else {
				node.FilterList = append(node.FilterList, filter)
			}
		}

		childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown, separateNonEquiConds)

		if len(cantPushdownChild) > 0 {
			childID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[0] = childID

	case plan.Node_FILTER:
		canPushdown = filters
		for _, filter := range node.FilterList {
			canPushdown = append(canPushdown, splitPlanConjunction(applyDistributivity(builder.GetContext(), filter))...)
		}

		childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown, separateNonEquiConds)

		if len(cantPushdownChild) > 0 {
			node.Children[0] = childID
			node.FilterList = cantPushdownChild
		} else {
			nodeID = childID
		}

	case plan.Node_JOIN:
		leftTags := make(map[int32]any)
		for _, tag := range builder.enumerateTags(node.Children[0]) {
			leftTags[tag] = nil
		}

		rightTags := make(map[int32]any)
		for _, tag := range builder.enumerateTags(node.Children[1]) {
			rightTags[tag] = nil
		}

		var markTag int32
		if node.JoinType == plan.Node_MARK {
			markTag = node.BindingTags[0]
		}

		node.OnList = splitPlanConjunctions(node.OnList)

		if node.JoinType == plan.Node_INNER {
			for _, cond := range node.OnList {
				filters = append(filters, splitPlanConjunction(applyDistributivity(builder.GetContext(), cond))...)
			}

			node.OnList = nil
		}

		var leftPushdown, rightPushdown []*plan.Expr
		var turnInner bool

		joinSides := make([]int8, len(filters))

		for i, filter := range filters {
			canTurnInner := true

			joinSides[i] = getJoinSide(filter, leftTags, rightTags, markTag)
			if f, ok := filter.Expr.(*plan.Expr_F); ok {
				for _, arg := range f.F.Args {
					if getJoinSide(arg, leftTags, rightTags, markTag) == JoinSideBoth {
						canTurnInner = false
						break
					}
				}
			}

			if canTurnInner && node.JoinType == plan.Node_LEFT && joinSides[i]&JoinSideRight != 0 && rejectsNull(filter, builder.compCtx.GetProcess()) {
				for _, cond := range node.OnList {
					filters = append(filters, splitPlanConjunction(applyDistributivity(builder.GetContext(), cond))...)
				}

				node.JoinType = plan.Node_INNER
				node.OnList = nil
				turnInner = true

				break
			}

			// TODO: FULL OUTER join should be handled here. However we don't have FULL OUTER join now.
		}

		if turnInner {
			joinSides = make([]int8, len(filters))

			for i, filter := range filters {
				joinSides[i] = getJoinSide(filter, leftTags, rightTags, markTag)
			}
		} else if node.JoinType == plan.Node_LEFT {
			var newOnList []*plan.Expr
			for _, cond := range node.OnList {
				conj := splitPlanConjunction(applyDistributivity(builder.GetContext(), cond))
				for _, conjElem := range conj {
					side := getJoinSide(conjElem, leftTags, rightTags, markTag)
					if side&JoinSideLeft == 0 {
						rightPushdown = append(rightPushdown, conjElem)
					} else {
						newOnList = append(newOnList, conjElem)
					}
				}
			}

			node.OnList = newOnList
		}

		if !separateNonEquiConds {
			var extraFilters []*plan.Expr
			for i, filter := range filters {
				if joinSides[i] != JoinSideBoth {
					continue
				}
				switch exprImpl := filter.Expr.(type) {
				case *plan.Expr_F:
					if exprImpl.F.Func.ObjName == "or" {
						keys := checkDNF(filter)
						for _, key := range keys {
							extraFilter := walkThroughDNF(builder.GetContext(), filter, key)
							if extraFilter != nil {
								extraFilters = append(extraFilters, DeepCopyExpr(extraFilter))
								joinSides = append(joinSides, getJoinSide(extraFilter, leftTags, rightTags, markTag))
							}
						}
					}
				}
			}
			filters = append(filters, extraFilters...)
		}

		for i, filter := range filters {
			switch joinSides[i] {
			case JoinSideNone:
				if c, ok := filter.Expr.(*plan.Expr_C); ok {
					if c, ok := c.C.Value.(*plan.Const_Bval); ok {
						if c.Bval {
							break
						}
					}
				}

				switch node.JoinType {
				case plan.Node_INNER:
					leftPushdown = append(leftPushdown, DeepCopyExpr(filter))
					rightPushdown = append(rightPushdown, filter)

				case plan.Node_LEFT, plan.Node_SEMI, plan.Node_ANTI, plan.Node_SINGLE, plan.Node_MARK:
					leftPushdown = append(leftPushdown, filter)

				default:
					cantPushdown = append(cantPushdown, filter)
				}

			case JoinSideLeft:
				if node.JoinType != plan.Node_OUTER {
					leftPushdown = append(leftPushdown, filter)
				} else {
					cantPushdown = append(cantPushdown, filter)
				}

			case JoinSideRight:
				if node.JoinType == plan.Node_INNER {
					rightPushdown = append(rightPushdown, filter)
				} else {
					cantPushdown = append(cantPushdown, filter)
				}

			case JoinSideBoth:
				if node.JoinType == plan.Node_INNER {
					if separateNonEquiConds {
						if f, ok := filter.Expr.(*plan.Expr_F); ok {
							if f.F.Func.ObjName == "=" {
								if getJoinSide(f.F.Args[0], leftTags, rightTags, markTag) != JoinSideBoth {
									if getJoinSide(f.F.Args[1], leftTags, rightTags, markTag) != JoinSideBoth {
										node.OnList = append(node.OnList, filter)
										break
									}
								}
							}
						}
					} else {
						node.OnList = append(node.OnList, filter)
						break
					}
				}

				cantPushdown = append(cantPushdown, filter)

			case JoinSideMark:
				if tryMark, ok := filter.Expr.(*plan.Expr_Col); ok {
					if tryMark.Col.RelPos == node.BindingTags[0] {
						node.JoinType = plan.Node_SEMI
						node.BindingTags = nil
						break
					}
				} else if fExpr, ok := filter.Expr.(*plan.Expr_F); ok {
					if filter.Typ.NotNullable && fExpr.F.Func.ObjName == "not" {
						arg := fExpr.F.Args[0]
						if tryMark, ok := arg.Expr.(*plan.Expr_Col); ok {
							if tryMark.Col.RelPos == node.BindingTags[0] {
								node.JoinType = plan.Node_ANTI
								node.BindingTags = nil
								break
							}
						}
					}
				}

				cantPushdown = append(cantPushdown, filter)

			default:
				cantPushdown = append(cantPushdown, filter)
			}
		}

		//when onlist is empty, it will be a cross join, performance will be very poor
		//in this situation, we put the non equal conds in the onlist and go loop join
		//todo: when equal conds and non equal conds both exists, put them in the on list and go hash equal join
		if node.JoinType == plan.Node_INNER && len(node.OnList) == 0 {
			// for tpch q22, do not change the plan for now. will fix in the future
			leftStats := builder.qry.Nodes[node.Children[0]].Stats
			rightStats := builder.qry.Nodes[node.Children[1]].Stats
			if leftStats.Outcnt != 1 && rightStats.Outcnt != 1 {
				node.OnList = append(node.OnList, cantPushdown...)
				cantPushdown = nil
			}
		}

		if node.JoinType == plan.Node_INNER {
			//only inner join can deduce new predicate
			builder.pushdownFilters(node.Children[0], deduceNewFilterList(rightPushdown, node.OnList), separateNonEquiConds)
			builder.pushdownFilters(node.Children[1], deduceNewFilterList(leftPushdown, node.OnList), separateNonEquiConds)
		}

		if builder.qry.Nodes[node.Children[1]].NodeType == plan.Node_FUNCTION_SCAN {

			for _, filter := range filters {
				down := false
				if builder.checkExprCanPushdown(filter, builder.qry.Nodes[node.Children[0]]) {
					leftPushdown = append(leftPushdown, DeepCopyExpr(filter))
					down = true
				}
				if builder.checkExprCanPushdown(filter, builder.qry.Nodes[node.Children[1]]) {
					rightPushdown = append(rightPushdown, DeepCopyExpr(filter))
					down = true
				}
				if !down {
					cantPushdown = append(cantPushdown, DeepCopyExpr(filter))
				}
			}
		}

		childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], leftPushdown, separateNonEquiConds)

		if len(cantPushdownChild) > 0 {
			childID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[0] = childID

		childID, cantPushdownChild = builder.pushdownFilters(node.Children[1], rightPushdown, separateNonEquiConds)

		if len(cantPushdownChild) > 0 {
			childID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[1]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[1] = childID

	case plan.Node_UNION, plan.Node_UNION_ALL, plan.Node_MINUS, plan.Node_MINUS_ALL, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL:
		leftChild := builder.qry.Nodes[node.Children[0]]
		rightChild := builder.qry.Nodes[node.Children[1]]
		var canPushDownRight []*plan.Expr

		for _, filter := range filters {
			canPushdown = append(canPushdown, replaceColRefsForSet(DeepCopyExpr(filter), leftChild.ProjectList))
			canPushDownRight = append(canPushDownRight, replaceColRefsForSet(filter, rightChild.ProjectList))
		}

		childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown, separateNonEquiConds)
		if len(cantPushdownChild) > 0 {
			childID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}
		node.Children[0] = childID

		childID, cantPushdownChild = builder.pushdownFilters(node.Children[1], canPushDownRight, separateNonEquiConds)
		if len(cantPushdownChild) > 0 {
			childID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[1]},
				FilterList: cantPushdownChild,
			}, nil)
		}
		node.Children[1] = childID

	case plan.Node_PROJECT:
		child := builder.qry.Nodes[node.Children[0]]
		if (child.NodeType == plan.Node_VALUE_SCAN || child.NodeType == plan.Node_EXTERNAL_SCAN) && child.RowsetData == nil {
			cantPushdown = filters
			break
		}

		projectTag := node.BindingTags[0]

		for _, filter := range filters {
			canPushdown = append(canPushdown, replaceColRefs(filter, projectTag, node.ProjectList))
		}

		childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown, separateNonEquiConds)

		if len(cantPushdownChild) > 0 {
			childID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[0] = childID

	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN:
		for _, filter := range filters {
			if onlyContainsTag(filter, node.BindingTags[0]) {
				node.FilterList = append(node.FilterList, filter)
			} else {
				cantPushdown = append(cantPushdown, filter)
			}
		}
	case plan.Node_FUNCTION_SCAN:
		downFilters := make([]*plan.Expr, 0)
		selfFilters := make([]*plan.Expr, 0)
		for _, filter := range filters {
			if onlyContainsTag(filter, node.BindingTags[0]) {
				selfFilters = append(selfFilters, DeepCopyExpr(filter))
			} else {
				downFilters = append(downFilters, DeepCopyExpr(filter))
			}
		}
		node.FilterList = append(node.FilterList, selfFilters...)
		childId := node.Children[0]
		childId, _ = builder.pushdownFilters(childId, downFilters, separateNonEquiConds)
		node.Children[0] = childId
	default:
		if len(node.Children) > 0 {
			childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], filters, separateNonEquiConds)

			if len(cantPushdownChild) > 0 {
				childID = builder.appendNode(&plan.Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{node.Children[0]},
					FilterList: cantPushdownChild,
				}, nil)
			}

			node.Children[0] = childID
		} else {
			cantPushdown = filters
		}
	}

	return nodeID, cantPushdown
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

func getJoinCondLeftCol(cond *Expr, leftTags map[int32]any) *plan.Expr_Col {
	fun, ok := cond.Expr.(*plan.Expr_F)
	if !ok || fun.F.Func.ObjName != "=" {
		return nil
	}
	leftCol, ok := fun.F.Args[0].Expr.(*plan.Expr_Col)
	if !ok {
		return nil
	}
	rightCol, ok := fun.F.Args[1].Expr.(*plan.Expr_Col)
	if !ok {
		return nil
	}
	if _, ok := leftTags[leftCol.Col.RelPos]; ok {
		return leftCol
	}
	if _, ok := leftTags[rightCol.Col.RelPos]; ok {
		return rightCol
	}
	return nil
}

// if join cond is a=b and a=c, we can remove a=c to improve join performance
func (builder *QueryBuilder) removeRedundantJoinCond(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for i, child := range node.Children {
			node.Children[i] = builder.removeRedundantJoinCond(child)
		}
	} else {
		return nodeID
	}
	if !builder.IsEquiJoin(node) {
		return nodeID
	}

	leftTags := make(map[int32]any)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = nil
	}

	rightTags := make(map[int32]any)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = nil
	}

	newOnList := make([]*Expr, 0, len(node.OnList))
	colMap := make(map[[2]int32]int32)
	for _, expr := range node.OnList {
		if equi := isEquiCond(expr, leftTags, rightTags); equi {
			col := getJoinCondLeftCol(expr, leftTags)
			if col != nil {
				if _, ok := colMap[[2]int32{col.Col.RelPos, col.Col.ColPos}]; ok {
					continue
				} else {
					colMap[[2]int32{col.Col.RelPos, col.Col.ColPos}] = 0
				}
			}
		}
		newOnList = append(newOnList, expr)
	}

	node.OnList = newOnList
	return nodeID
}

func (builder *QueryBuilder) removeEffectlessLeftJoins(nodeID int32, tagCnt map[int32]int) int32 {
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

	var (
		rightChild *plan.Node
		tableDef   *plan.TableDef
		tag        int32
		name2Pos   map[string]int
		equiCols   []bool
	)

	increaseTagCntForExprList(node.OnList, -1, tagCnt)

	if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_LEFT {
		goto END
	}

	rightChild = builder.qry.Nodes[node.Children[1]]
	if rightChild.NodeType != plan.Node_TABLE_SCAN {
		goto END
	}

	tag = rightChild.BindingTags[0]
	if tagCnt[tag] > 0 {
		goto END
	}

	tableDef = rightChild.TableDef
	if tableDef.Pkey == nil || len(tableDef.Pkey.Names) > len(node.OnList) {
		goto END
	}

	equiCols = make([]bool, len(tableDef.Pkey.Names))
	name2Pos = make(map[string]int)
	for i, name := range tableDef.Pkey.Names {
		name2Pos[name] = i
	}

	for _, cond := range node.OnList {
		if f, ok := cond.Expr.(*plan.Expr_F); ok && SupportedJoinCondition(f.F.Func.Obj) {
			if col, ok := f.F.Args[0].Expr.(*plan.Expr_Col); ok && col.Col.RelPos == tag && !hasTag(f.F.Args[1], tag) {
				if pos, ok := name2Pos[col.Col.Name]; ok {
					equiCols[pos] = true
				}
			}
			if col, ok := f.F.Args[1].Expr.(*plan.Expr_Col); ok && col.Col.RelPos == tag && !hasTag(f.F.Args[0], tag) {
				if pos, ok := name2Pos[col.Col.Name]; ok {
					equiCols[pos] = true
				}
			}
		}
	}

	for i := range equiCols {
		if !equiCols[i] {
			goto END
		}
	}

	// All primary key columns of right table are presented in equi conditions
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
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			determineHashOnPK(child, builder)
		}
	}
	// for now ,only support inner join
	if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_INNER {
		return
	}
	if !builder.IsEquiJoin(node) {
		return
	}
	//for now, only support 1 join cond
	if len(node.OnList) != 1 {
		return
	}

	var hashCol *plan.ColRef
	cond := node.OnList[0]
	switch condImpl := cond.Expr.(type) {
	case *plan.Expr_F:
		expr := condImpl.F.Args[1]
		switch exprImpl := expr.Expr.(type) {
		case *plan.Expr_Col:
			hashCol = exprImpl.Col
		}
	}
	if hashCol == nil {
		return
	}

	tableDef := findHashOnPKTable(node.Children[1], hashCol.RelPos, builder)
	if tableDef == nil {
		return
	}
	if containsAllPKs([]int32{hashCol.ColPos}, tableDef) {
		node.Stats.HashmapStats.HashOnPK = true
	}

}
