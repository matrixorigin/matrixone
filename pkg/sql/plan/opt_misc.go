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
	projMap := make(map[[2]int32]*plan.Expr)
	node := builder.qry.Nodes[nodeID]

	increaseRefCntForExprList(node.ProjectList, colRefCnt)
	increaseRefCntForExprList(node.OnList, colRefCnt)
	increaseRefCntForExprList(node.FilterList, colRefCnt)
	increaseRefCntForExprList(node.GroupBy, colRefCnt)
	increaseRefCntForExprList(node.GroupingSet, colRefCnt)
	increaseRefCntForExprList(node.AggList, colRefCnt)
	for i := range node.OrderBy {
		increaseRefCnt(node.OrderBy[i].Expr, colRefCnt)
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

	case plan.Node_AGG, plan.Node_PROJECT:
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

func increaseRefCntForExprList(exprs []*plan.Expr, colRefCnt map[[2]int32]int) {
	for _, expr := range exprs {
		increaseRefCnt(expr, colRefCnt)
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
				cantPushdown = append(cantPushdown, filter)
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
		leftTags := make(map[int32]*Binding)
		for _, tag := range builder.enumerateTags(node.Children[0]) {
			leftTags[tag] = nil
		}

		rightTags := make(map[int32]*Binding)
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

			canTurnInner = canTurnInner && rejectsNull(filter, builder.compCtx.GetProcess())
			leftOrRightJoin := (node.JoinType == plan.Node_LEFT && joinSides[i]&JoinSideRight != 0) || (node.JoinType == plan.Node_RIGHT && joinSides[i]&JoinSideLeft != 0)
			if canTurnInner && leftOrRightJoin {
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
				if node.JoinType != plan.Node_OUTER && node.JoinType != plan.Node_RIGHT {
					leftPushdown = append(leftPushdown, filter)
				} else {
					cantPushdown = append(cantPushdown, filter)
				}

			case JoinSideRight:
				if node.JoinType == plan.Node_INNER || node.JoinType == plan.Node_RIGHT {
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
		if len(node.OnList) == 0 {
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
			builder.pushdownFilters(node.Children[0], predsDeduction(rightPushdown, node.OnList), separateNonEquiConds)
			builder.pushdownFilters(node.Children[1], predsDeduction(leftPushdown, node.OnList), separateNonEquiConds)
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

func (builder *QueryBuilder) swapJoinBuildSide(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	for _, child := range node.Children {
		builder.swapJoinBuildSide(child)
	}

	// XXX: This function will be eliminated entirely, so don't care about the defective logic below.
	if node.BuildOnLeft && IsEquiJoin(node.OnList) {
		node.Children[0], node.Children[1] = node.Children[1], node.Children[0]
	}
}
