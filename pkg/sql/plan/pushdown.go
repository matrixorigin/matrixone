// Copyright 2024 Matrix Origin
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

func (builder *QueryBuilder) pushdownFilters(nodeID int32, filters []*plan.Expr, separateNonEquiConds bool) (int32, []*plan.Expr) {
	node := builder.qry.Nodes[nodeID]

	var canPushdown, cantPushdown []*plan.Expr

	if node.Limit != nil {
		// can not push down over limit
		cantPushdown = filters
		filters = nil
	}

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

	case plan.Node_SAMPLE:
		groupTag := node.BindingTags[0]
		sampleTag := node.BindingTags[1]

		for _, filter := range filters {
			if !containsTag(filter, sampleTag) {
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

	case plan.Node_TIME_WINDOW:
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
		leftTags := make(map[int32]bool)
		for _, tag := range builder.enumerateTags(node.Children[0]) {
			leftTags[tag] = true
		}

		rightTags := make(map[int32]bool)
		for _, tag := range builder.enumerateTags(node.Children[1]) {
			rightTags[tag] = true
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
			if f := filter.GetF(); f != nil {
				for _, arg := range f.Args {
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
				if filter.GetLit().GetBval() {
					break
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
						if f := filter.GetF(); f != nil {
							if f.Func.ObjName == "=" {
								if getJoinSide(f.Args[0], leftTags, rightTags, markTag) != JoinSideBoth {
									if getJoinSide(f.Args[1], leftTags, rightTags, markTag) != JoinSideBoth {
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
				if tryMark := filter.GetCol(); tryMark != nil {
					if tryMark.RelPos == node.BindingTags[0] {
						node.JoinType = plan.Node_SEMI
						node.BindingTags = nil
						break
					}
				} else if fExpr := filter.GetF(); fExpr != nil && filter.Typ.NotNullable && fExpr.Func.ObjName == "not" {
					arg := fExpr.Args[0]
					if tryMark := arg.GetCol(); tryMark != nil {
						if tryMark.RelPos == node.BindingTags[0] {
							node.JoinType = plan.Node_ANTI
							node.BindingTags = nil
							break
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

		switch node.JoinType {
		case plan.Node_INNER, plan.Node_SEMI:
			//inner and semi join can deduce new predicate from both side
			builder.pushdownFilters(node.Children[0], deduceNewFilterList(rightPushdown, node.OnList), separateNonEquiConds)
			builder.pushdownFilters(node.Children[1], deduceNewFilterList(leftPushdown, node.OnList), separateNonEquiConds)
		case plan.Node_RIGHT:
			//right join can deduce new predicate only from right side to left
			builder.pushdownFilters(node.Children[0], deduceNewFilterList(rightPushdown, node.OnList), separateNonEquiConds)
		case plan.Node_LEFT:
			//left join can deduce new predicate only from left side to right
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
	case plan.Node_APPLY:
		childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], filters, separateNonEquiConds)

		if len(cantPushdownChild) > 0 {
			childID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[0] = childID
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

// order by limit can be pushed down to left child of left join
func (builder *QueryBuilder) pushdownTopThroughLeftJoin(nodeID int32) {
	if builder.optimizerHints != nil && builder.optimizerHints.pushDownTopThroughLeftJoin != 0 {
		return
	}
	node := builder.qry.Nodes[nodeID]
	var joinnode, nodePushDown *plan.Node
	var tags []int32
	var newNodeID int32

	if node.NodeType != plan.Node_SORT || node.Limit == nil {
		goto END
	}
	joinnode = builder.qry.Nodes[node.Children[0]]
	if joinnode.NodeType != plan.Node_JOIN {
		goto END
	}

	//before join order, only left join
	if joinnode.JoinType != plan.Node_LEFT {
		goto END
	}

	// check orderby column
	tags = builder.enumerateTags(builder.qry.Nodes[joinnode.Children[0]].NodeId)
	for i := range node.OrderBy {
		if !checkExprInTags(node.OrderBy[i].Expr, tags) {
			goto END
		}
	}

	nodePushDown = DeepCopyNode(node)

	if nodePushDown.Offset != nil {
		newExpr, err := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "+", []*Expr{nodePushDown.Limit, nodePushDown.Offset})
		if err != nil {
			goto END
		}
		nodePushDown.Offset = nil
		nodePushDown.Limit = newExpr
	}
	newNodeID = builder.appendNode(nodePushDown, nil)
	nodePushDown.Children[0] = joinnode.Children[0]
	joinnode.Children[0] = newNodeID

END:
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			builder.pushdownTopThroughLeftJoin(child)
		}
	}
}

func (builder *QueryBuilder) pushdownLimitToTableScan(nodeID int32) {
	if builder.optimizerHints != nil && builder.optimizerHints.pushDownLimitToScan != 0 {
		return
	}
	node := builder.qry.Nodes[nodeID]
	for _, childID := range node.Children {
		builder.pushdownLimitToTableScan(childID)
	}
	if node.NodeType == plan.Node_PROJECT && len(node.Children) > 0 {
		child := builder.qry.Nodes[node.Children[0]]
		if child.NodeType == plan.Node_TABLE_SCAN {
			child.Limit, child.Offset = node.Limit, node.Offset
			node.Limit, node.Offset = nil, nil

			// if there is a limit, outcnt is limit number
			if child.Limit != nil {
				if cExpr, ok := child.Limit.Expr.(*plan.Expr_Lit); ok {
					if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
						child.Stats.Outcnt = float64(c.U64Val)
						if child.Stats.Selectivity < 0.5 {
							newblockNum := int32(c.U64Val / 2)
							if newblockNum < child.Stats.BlockNum {
								child.Stats.BlockNum = newblockNum
							}
						} else {
							child.Stats.BlockNum = 1
						}
						child.Stats.Cost = float64(child.Stats.BlockNum * DefaultBlockMaxRows)
					}
				}
			}
		}
	}
}
