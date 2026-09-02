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

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

const maxVectorIndexTopPushdownLimit = uint64(^uint(0) >> 1)

func (builder *QueryBuilder) pushdownFilters(nodeID int32, filters []*plan.Expr, separateNonEquiConds bool) (int32, []*plan.Expr) {
	originalNodeID := nodeID
	if builder.checkPlanningCanceled() != nil {
		return originalNodeID, filters
	}
	// Record before pushdownFilters
	builder.optimizationHistory = append(builder.optimizationHistory,
		fmt.Sprintf("pushdownFilters:before (nodeID: %d, nodeType: %s, filters: %d)", nodeID, builder.qry.Nodes[nodeID].NodeType, len(filters)))
	node := builder.qry.Nodes[nodeID]
	if !builder.subqueryPredicatePlanningDisabled() && !separateNonEquiConds &&
		node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK && len(filters) > 0 {
		rewrittenID, remaining := builder.rewriteFilteringOrOfExists(nodeID, filters)
		if rewrittenID != nodeID {
			return builder.pushdownFilters(rewrittenID, remaining, separateNonEquiConds)
		}
	}

	var canPushdown, cantPushdown []*plan.Expr

	if node.Limit != nil {
		// can not push down over limit
		cantPushdown = append(cantPushdown, filters...)
		filters = nil
	}

	switch node.NodeType {
	case plan.Node_AGG:
		// Legacy positional aggregates have no global binding tags. Keep filters
		// above them because tag-based replacement cannot address their outputs.
		if len(node.BindingTags) < 2 {
			return originalNodeID, filters
		}
		groupTag := node.BindingTags[0]
		aggregateTag := node.BindingTags[1]

		for _, filter := range filters {
			if ContainsVolatileFunction(filter) {
				node.FilterList = append(node.FilterList, filter)
				continue
			}
			// A predicate with no column references is not safe below a global
			// aggregate. If it evaluates to false, filtering the aggregate input
			// still leaves the single global-aggregate output row alive. This can
			// happen after set-operation columns are replaced by branch literals.
			if len(node.GroupBy) == 0 && !exprHasColRef(filter) {
				node.FilterList = append(node.FilterList, filter)
			} else if !containsTag(filter, aggregateTag) && !containGrouping(filter) &&
				!referencesSyntheticGroupKey(filter, groupTag, len(node.GroupBy), node.GroupingFlag) {
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
			if ContainsVolatileFunction(filter) {
				node.FilterList = append(node.FilterList, filter)
			} else if !containsTag(filter, sampleTag) {
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

		// Collect only plain PARTITION BY column keys from all window specs.
		// Filters can be safely pushed below the window node only when they
		// exclusively reference columns that are themselves partition keys,
		// because those filters eliminate entire partitions without changing
		// row numbering. Column references nested inside arbitrary partition
		// expressions (e.g. PARTITION BY a+b) are not equivalent to
		// partition keys and must not be treated as pushdown-eligible.
		partCols := make(map[[2]int32]bool)
		for _, w := range node.WinSpecList {
			if we := w.GetW(); we != nil {
				for _, p := range we.PartitionBy {
					if col := p.GetCol(); col != nil {
						partCols[[2]int32{col.RelPos, col.ColPos}] = true
					}
				}
			}
		}

		for _, filter := range filters {
			if ContainsVolatileFunction(filter) {
				node.FilterList = append(node.FilterList, filter)
			} else if containsTag(filter, windowTag) {
				node.FilterList = append(node.FilterList, filter)
			} else if exprColRefsSubsetOf(filter, partCols) {
				canPushdown = append(canPushdown, filter)
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

	case plan.Node_TIME_WINDOW:
		windowTag := node.BindingTags[0]

		for _, filter := range filters {
			if ContainsVolatileFunction(filter) {
				node.FilterList = append(node.FilterList, filter)
			} else if !containsTag(filter, windowTag) {
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
		// IsEnd filters are terminal assertions/action selectors. Moving their
		// predicates below joins can change both assertion scope and marker layout.
		// Barrier filters are cardinality-changing semantic boundaries over a
		// final DML row image. Unlike ASSERT they discard rows, but have the same
		// non-reorderability requirement.
		if node.IsEnd {
			cantPushdown = append(cantPushdown, filters...)
			return originalNodeID, cantPushdown
		}
		if node.FilterIsBarrier {
			childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], nil, separateNonEquiConds)
			if len(cantPushdownChild) > 0 {
				childID = builder.appendNode(&plan.Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{childID},
					FilterList: cantPushdownChild,
				}, nil)
			}
			node.Children[0] = childID
			cantPushdown = append(cantPushdown, filters...)
			return originalNodeID, cantPushdown
		}
		canPushdown = filters
		if !node.RollupFilter {
			for _, filter := range node.FilterList {
				canPushdown = append(canPushdown, splitPlanConjunction(applyDistributivity(
					builder.GetContext(), filter, !builder.subqueryPredicatePlanningDisabled()))...)
			}
		}
		if !node.RollupFilter && !separateNonEquiConds {
			node.Children[0], canPushdown = builder.rewriteFilteringOrOfExists(node.Children[0], canPushdown)
		}

		childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown, separateNonEquiConds)

		if node.RollupFilter {
			if len(cantPushdownChild) > 0 {
				node.Children[0] = childID
				node.FilterList = append(node.FilterList, cantPushdownChild...)
			}
		} else if len(cantPushdownChild) > 0 {
			node.Children[0] = childID
			node.FilterList = cantPushdownChild
		} else {
			nodeID = childID
		}

	case plan.Node_ASSERT:
		// ASSERT is a row-preserving semantic boundary. Its predicates describe
		// the row image at this exact point in the DML pipeline, so neither the
		// assertion nor filters from its parent may cross it.
		childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], nil, separateNonEquiConds)
		if len(cantPushdownChild) > 0 {
			childID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{childID},
				FilterList: cantPushdownChild,
			}, nil)
		}
		node.Children[0] = childID
		cantPushdown = append(cantPushdown, filters...)

	case plan.Node_JOIN:
		dedupIgnoreHasReleaseRows := node.JoinType == plan.Node_DEDUP &&
			node.OnDuplicateAction == plan.Node_IGNORE && node.DedupJoinCtx != nil &&
			len(node.DedupJoinCtx.OldColList) > 1
		if node.JoinType == plan.Node_DEDUP &&
			(node.OnDuplicateAction == plan.Node_UPDATE || dedupIgnoreHasReleaseRows) {
			// DEDUP UPDATE mutates columns from its right input into the final row
			// image. DEDUP IGNORE can also carry delete-only rows that release keys
			// for later candidates. A predicate above either form must stay above
			// the join or it can change conflict detection.
			for i, child := range node.Children {
				childID, cantPushdownChild := builder.pushdownFilters(child, nil, separateNonEquiConds)
				if len(cantPushdownChild) > 0 {
					childID = builder.appendNode(&plan.Node{
						NodeType:   plan.Node_FILTER,
						Children:   []int32{childID},
						FilterList: cantPushdownChild,
					}, nil)
				}
				node.Children[i] = childID
			}
			cantPushdown = append(cantPushdown, filters...)
			break
		}
		// Record middle: processing JOIN node
		builder.optimizationHistory = append(builder.optimizationHistory,
			fmt.Sprintf("pushdownFilters:middle (nodeID: %d, JOIN, filters: %d, onList: %d)", nodeID, len(filters), len(node.OnList)))
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

		getJoinSideForPushdown := getJoinSide
		if separateNonEquiConds {
			// After join ordering, conds can reference tags outside this join subtree.
			// Keep those conds at this join instead of pushing them to one child.
			getJoinSideForPushdown = getJoinSideWithOuterScope
		}

		if node.JoinType == plan.Node_INNER {
			for _, cond := range node.OnList {
				filters = append(filters, splitPlanConjunction(applyDistributivity(
					builder.GetContext(), cond, !builder.subqueryPredicatePlanningDisabled()))...)
			}

			node.OnList = nil
		}

		var leftPushdown, rightPushdown []*plan.Expr
		var turnInner bool

		joinSides := make([]int8, len(filters))

		for i, filter := range filters {
			canTurnInner := true

			joinSides[i] = getJoinSideForPushdown(filter, leftTags, rightTags, markTag)
			if f := filter.GetF(); f != nil {
				for _, arg := range f.Args {
					argSide := getJoinSideForPushdown(arg, leftTags, rightTags, markTag)
					if argSide == JoinSideBoth || argSide&JoinSideOuter != 0 {
						canTurnInner = false
						break
					}
				}
			}

			if canTurnInner && node.JoinType == plan.Node_LEFT && joinSides[i] == JoinSideRight && rejectsNull(filter, builder.compCtx.GetProcess()) {
				for _, cond := range node.OnList {
					filters = append(filters, splitPlanConjunction(applyDistributivity(
						builder.GetContext(), cond, !builder.subqueryPredicatePlanningDisabled()))...)
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
				joinSides[i] = getJoinSideForPushdown(filter, leftTags, rightTags, markTag)
			}
		} else if node.JoinType == plan.Node_LEFT {
			var newOnList []*plan.Expr
			for _, cond := range node.OnList {
				conj := splitPlanConjunction(applyDistributivity(
					builder.GetContext(), cond, !builder.subqueryPredicatePlanningDisabled()))
				for _, conjElem := range conj {
					if ContainsVolatileFunction(conjElem) {
						newOnList = append(newOnList, conjElem)
						continue
					}
					side := getJoinSideForPushdown(conjElem, leftTags, rightTags, markTag)
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
								joinSides = append(joinSides, getJoinSideForPushdown(extraFilter, leftTags, rightTags, markTag))
							}
						}
					}
				}
			}
			filters = append(filters, extraFilters...)
		}

		for i, filter := range filters {
			if joinSides[i]&JoinSideOuter != 0 {
				cantPushdown = append(cantPushdown, filter)
				continue
			}
			if ContainsVolatileFunction(filter) {
				cantPushdown = append(cantPushdown, filter)
				continue
			}

			switch joinSides[i] {
			case JoinSideNone:
				if filter.GetLit().GetBval() {
					break
				}
				switch node.JoinType {
				case plan.Node_INNER:
					leftPushdown = append(leftPushdown, DeepCopyExpr(filter))
					rightPushdown = append(rightPushdown, filter)

				case plan.Node_LEFT, plan.Node_SEMI, plan.Node_ANTI, plan.Node_SINGLE, plan.Node_MARK,
					plan.Node_ASOF, plan.Node_ASOF_LEFT:
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
				if node.JoinType == plan.Node_INNER || node.JoinType == plan.Node_DEDUP {
					rightPushdown = append(rightPushdown, filter)
				} else {
					cantPushdown = append(cantPushdown, filter)
				}

			case JoinSideBoth:
				if node.JoinType == plan.Node_INNER {
					if separateNonEquiConds {
						if f := filter.GetF(); f != nil {
							if f.Func.ObjName == "=" {
								if getJoinSideForPushdown(f.Args[0], leftTags, rightTags, markTag) != JoinSideBoth {
									if getJoinSideForPushdown(f.Args[1], leftTags, rightTags, markTag) != JoinSideBoth {
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
				if isMarkColumn(filter, node.BindingTags[0]) {
					if !builder.subqueryPredicatePlanningDisabled() &&
						!areTruncationSafePredicates(node.OnList) {
						cantPushdown = append(cantPushdown, filter)
						break
					}
					node.JoinType = plan.Node_SEMI
					if !builder.subqueryPredicatePlanningDisabled() {
						node.OnList = unwrapIsTrueFromMarkJoinEqualities(node.OnList, leftTags, rightTags, markTag)
					}
					node.BindingTags = nil
					break
				}
				if fExpr := filter.GetF(); fExpr != nil {
					funcID, _ := function.DecodeOverloadID(fExpr.Func.GetObj())
					if funcID == function.ISTRUE && len(fExpr.Args) == 1 &&
						isMarkColumn(fExpr.Args[0], node.BindingTags[0]) {
						if !builder.subqueryPredicatePlanningDisabled() &&
							!areTruncationSafePredicates(node.OnList) {
							cantPushdown = append(cantPushdown, filter)
							break
						}
						node.JoinType = plan.Node_SEMI
						if !builder.subqueryPredicatePlanningDisabled() {
							node.OnList = unwrapIsTrueFromMarkJoinEqualities(node.OnList, leftTags, rightTags, markTag)
						}
						node.BindingTags = nil
						break
					}
					if filter.Typ.NotNullable && fExpr.Func.ObjName == "not" && len(fExpr.Args) == 1 &&
						isTrueMarkColumn(fExpr.Args[0], node.BindingTags[0]) {
						if !builder.subqueryPredicatePlanningDisabled() &&
							!areTruncationSafePredicates(node.OnList) {
							cantPushdown = append(cantPushdown, filter)
							break
						}
						node.JoinType = plan.Node_ANTI
						if !builder.subqueryPredicatePlanningDisabled() {
							node.OnList = unwrapIsTrueFromMarkJoinEqualities(node.OnList, leftTags, rightTags, markTag)
						}
						node.BindingTags = nil
						break
					}
				}

				cantPushdown = append(cantPushdown, filter)

			default:
				cantPushdown = append(cantPushdown, filter)
			}
		}

		switch node.JoinType {
		case plan.Node_INNER:
			//when onlist is empty, it will be a cross join, performance will be very poor
			//in this situation, we put the non equal conds in the onlist and go loop join
			if len(node.OnList) == 0 {
				// for tpch q22, do not change the plan for now. will fix in the future
				leftStats := builder.qry.Nodes[node.Children[0]].Stats
				rightStats := builder.qry.Nodes[node.Children[1]].Stats
				if leftStats.Outcnt != 1 && rightStats.Outcnt != 1 {
					node.OnList = cantPushdown
					cantPushdown = nil
				}
			}

		case plan.Node_LEFT, plan.Node_SEMI, plan.Node_ANTI, plan.Node_SINGLE,
			plan.Node_ASOF, plan.Node_ASOF_LEFT:
			if len(node.OnList) > 0 {
				var newOnList []*plan.Expr

				for _, cond := range node.OnList {
					joinSide := getJoinSideForPushdown(cond, leftTags, rightTags, markTag)
					if joinSide == JoinSideRight && !ContainsVolatileFunction(cond) {
						rightPushdown = append(rightPushdown, cond)
					} else {
						newOnList = append(newOnList, cond)
					}
				}

				node.OnList = newOnList
			}
		}

		switch node.JoinType {
		case plan.Node_INNER, plan.Node_SEMI:
			//inner and semi join can deduce new predicate from both side
			if deduced := deduceNewFilterList(rightPushdown, node.OnList); len(deduced) > 0 {
				builder.pushdownFilters(node.Children[0], deduced, separateNonEquiConds)
			}
			if deduced := deduceNewFilterList(leftPushdown, node.OnList); len(deduced) > 0 {
				builder.pushdownFilters(node.Children[1], deduced, separateNonEquiConds)
			}
		case plan.Node_RIGHT, plan.Node_ANTI:
			//right join can deduce new predicate only from right side to left
			if deduced := deduceNewFilterList(rightPushdown, node.OnList); len(deduced) > 0 {
				builder.pushdownFilters(node.Children[0], deduced, separateNonEquiConds)
			}
		case plan.Node_LEFT, plan.Node_SINGLE, plan.Node_ASOF, plan.Node_ASOF_LEFT:
			//left join can deduce new predicate only from left side to right
			if deduced := deduceNewFilterList(leftPushdown, node.OnList); len(deduced) > 0 {
				builder.pushdownFilters(node.Children[1], deduced, separateNonEquiConds)
			}
		}

		if builder.qry.Nodes[node.Children[1]].NodeType == plan.Node_FUNCTION_SCAN {

			for _, filter := range filters {
				if ContainsVolatileFunction(filter) {
					continue
				}
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

		wrapChildFilters := func(childID int32, childFilters []*plan.Expr, childTags map[int32]bool) int32 {
			var filtersForChild []*plan.Expr
			for _, filter := range childFilters {
				if containsOnlyTags(filter, childTags) {
					filtersForChild = append(filtersForChild, filter)
				} else {
					cantPushdown = append(cantPushdown, filter)
				}
			}
			if len(filtersForChild) == 0 {
				return childID
			}
			return builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{childID},
				FilterList: filtersForChild,
			}, nil)
		}

		childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], leftPushdown, separateNonEquiConds)

		childID = wrapChildFilters(childID, cantPushdownChild, leftTags)

		node.Children[0] = childID

		childID, cantPushdownChild = builder.pushdownFilters(node.Children[1], rightPushdown, separateNonEquiConds)

		childID = wrapChildFilters(childID, cantPushdownChild, rightTags)

		node.Children[1] = childID

	case plan.Node_UNION, plan.Node_UNION_ALL, plan.Node_MINUS, plan.Node_MINUS_ALL, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL:
		// Record middle: processing UNION/MINUS/INTERSECT node
		builder.optimizationHistory = append(builder.optimizationHistory,
			fmt.Sprintf("pushdownFilters:middle (nodeID: %d, %s, filters: %d)", nodeID, node.NodeType, len(filters)))
		leftChild := builder.qry.Nodes[node.Children[0]]
		rightChild := builder.qry.Nodes[node.Children[1]]
		var canPushDownRight []*plan.Expr

		for _, filter := range filters {
			if ContainsVolatileFunction(filter) {
				cantPushdown = append(cantPushdown, filter)
				continue
			}
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

		if len(node.BindingTags) == 0 {
			node.BindingTags = []int32{0}
		}
		projectTag := node.BindingTags[0]

		for _, filter := range filters {
			introducesVolatile := replaceColRefsIntroducesVolatile(filter, projectTag, node.ProjectList)
			rewritten := replaceColRefs(DeepCopyExpr(filter), projectTag, node.ProjectList)
			if introducesVolatile {
				cantPushdown = append(cantPushdown, filter)
				continue
			}
			canPushdown = append(canPushdown, rewritten)
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
		// Record middle: processing TABLE_SCAN/EXTERNAL_SCAN node
		builder.optimizationHistory = append(builder.optimizationHistory,
			fmt.Sprintf("pushdownFilters:middle (nodeID: %d, %s, filters: %d)", nodeID, node.NodeType, len(filters)))
		for _, filter := range filters {
			if onlyContainsTag(filter, node.BindingTags[0]) {
				node.FilterList = append(node.FilterList, filter)
			} else {
				cantPushdown = append(cantPushdown, filter)
			}
		}
	case plan.Node_FUNCTION_SCAN, plan.Node_VECTOR_INDEX_SCAN:
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
		if len(node.Children) != 0 {
			childId := node.Children[0]
			var cantPushdownChild []*plan.Expr
			childId, cantPushdownChild = builder.pushdownFilters(childId, downFilters, separateNonEquiConds)
			node.Children[0] = childId
			cantPushdown = append(cantPushdown, cantPushdownChild...)
		} else {
			cantPushdown = append(cantPushdown, downFilters...)
		}

	case plan.Node_APPLY:
		for _, filter := range filters {
			if ContainsVolatileFunction(filter) {
				cantPushdown = append(cantPushdown, filter)
			} else {
				canPushdown = append(canPushdown, filter)
			}
		}
		childID, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown, separateNonEquiConds)

		cantPushdown = append(cantPushdown, cantPushdownChild...)

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

	// Record after pushdownFilters
	if nodeID != originalNodeID {
		builder.optimizationHistory = append(builder.optimizationHistory,
			fmt.Sprintf("pushdownFilters:after (nodeID: %d -> %d, cantPushdown: %d)", originalNodeID, nodeID, len(cantPushdown)))
	} else {
		builder.optimizationHistory = append(builder.optimizationHistory,
			fmt.Sprintf("pushdownFilters:after (nodeID: %d, no change, cantPushdown: %d)", nodeID, len(cantPushdown)))
	}
	return nodeID, cantPushdown
}

func isMarkColumn(expr *plan.Expr, markTag int32) bool {
	col := expr.GetCol()
	return col != nil && col.RelPos == markTag
}

func isTrueMarkColumn(expr *plan.Expr, markTag int32) bool {
	if isMarkColumn(expr, markTag) {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 1 {
		return false
	}
	funcID, _ := function.DecodeOverloadID(fn.Func.GetObj())
	return funcID == function.ISTRUE && isMarkColumn(fn.Args[0], markTag)
}

type existenceJoinKey struct {
	outer       *plan.Expr
	inner       *plan.Expr
	equality    *plan.Expr
	innerArgIdx int
}

// rewriteFilteringOrOfExists turns a filtering disjunction of positive
// existential MARK joins into one SEMI join over the UNION ALL of their key
// inputs.  A MARK chain must otherwise materialize every large build before
// the OR can be evaluated, even though a WHERE predicate only needs to know
// whether any branch has a match.
//
// The rule deliberately accepts only a consecutive MARK prefix whose branches
// have the same deterministic outer equality keys.  Projected markers,
// NOT EXISTS, IN/ANY three-valued markers, non-equality correlations, and mixed
// outer keys retain the original MARK semantics.
func (builder *QueryBuilder) rewriteFilteringOrOfExists(
	nodeID int32,
	filters []*plan.Expr,
) (int32, []*plan.Expr) {
	if builder.subqueryPredicatePlanningDisabled() {
		return nodeID, filters
	}
	remaining := append([]*plan.Expr(nil), filters...)
	for {
		rewritten := false
		for i, filter := range remaining {
			newNodeID, ok := builder.rewriteOneFilteringOrOfExists(nodeID, filter)
			if !ok {
				continue
			}
			nodeID = newNodeID
			remaining = append(remaining[:i], remaining[i+1:]...)
			rewritten = true
			break
		}
		if !rewritten {
			return nodeID, remaining
		}
	}
}

func (builder *QueryBuilder) rewriteOneFilteringOrOfExists(
	nodeID int32,
	filter *plan.Expr,
) (int32, bool) {
	var markTags []int32
	if !collectPositiveExistenceMarkTags(filter, &markTags) || len(markTags) < 2 {
		return nodeID, false
	}

	wanted := make(map[int32]struct{}, len(markTags))
	for _, tag := range markTags {
		if _, exists := wanted[tag]; exists {
			return nodeID, false
		}
		wanted[tag] = struct{}{}
	}

	markNodes := make([]*plan.Node, 0, len(markTags))
	baseID := nodeID
	for len(markNodes) < len(markTags) {
		if baseID < 0 || int(baseID) >= len(builder.qry.Nodes) {
			return nodeID, false
		}
		markNode := builder.qry.Nodes[baseID]
		if markNode.NodeType != plan.Node_JOIN || markNode.JoinType != plan.Node_MARK ||
			len(markNode.Children) != 2 || len(markNode.BindingTags) != 1 {
			return nodeID, false
		}
		if _, ok := wanted[markNode.BindingTags[0]]; !ok {
			return nodeID, false
		}
		delete(wanted, markNode.BindingTags[0])
		markNodes = append(markNodes, markNode)
		baseID = markNode.Children[0]
	}
	if len(wanted) != 0 {
		return nodeID, false
	}

	branchKeys := make([][]existenceJoinKey, len(markNodes))
	for i, markNode := range markNodes {
		if builder.planSubtreeContainsVolatile(markNode.Children[1]) {
			return nodeID, false
		}
		keys, ok := builder.extractExistenceJoinKeys(markNode)
		if !ok {
			return nodeID, false
		}
		if i == 0 {
			branchKeys[i] = keys
			continue
		}

		ordered := make([]existenceJoinKey, len(branchKeys[0]))
		used := make([]bool, len(keys))
		for keyIdx, reference := range branchKeys[0] {
			match := -1
			for candidateIdx, candidate := range keys {
				if !used[candidateIdx] && exprStructuralEqual(reference.outer, candidate.outer) {
					if match != -1 {
						return nodeID, false
					}
					match = candidateIdx
				}
			}
			if match == -1 || !makeTypeByPlan2Expr(reference.inner).Eq(makeTypeByPlan2Expr(keys[match].inner)) {
				return nodeID, false
			}
			used[match] = true
			ordered[keyIdx] = keys[match]
		}
		if len(keys) != len(ordered) {
			return nodeID, false
		}
		branchKeys[i] = ordered
	}

	branchIDs := make([]int32, len(markNodes))
	branchTags := make([]int32, len(markNodes))
	for i, markNode := range markNodes {
		projectList := make([]*plan.Expr, len(branchKeys[i]))
		for keyIdx, key := range branchKeys[i] {
			projectList[keyIdx] = DeepCopyExpr(key.inner)
		}
		branchTags[i] = builder.genNewBindTag()
		branchIDs[i] = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{markNode.Children[1]},
			ProjectList: projectList,
			BindingTags: []int32{branchTags[i]},
		}, nil)
	}

	unionID := branchIDs[0]
	unionTag := branchTags[0]
	for i := 1; i < len(branchIDs); i++ {
		leftProject := builder.qry.Nodes[unionID].ProjectList
		rightProject := builder.qry.Nodes[branchIDs[i]].ProjectList
		unionProject := make([]*plan.Expr, len(leftProject))
		for keyIdx, leftExpr := range leftProject {
			unionProject[keyIdx] = &plan.Expr{
				Typ: setOperationOutputType(plan.Node_UNION_ALL, leftExpr.Typ, rightProject[keyIdx].Typ),
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: unionTag,
					ColPos: int32(keyIdx),
				}},
			}
		}
		unionTag = builder.genNewBindTag()
		unionID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_UNION_ALL,
			Children:    []int32{unionID, branchIDs[i]},
			ProjectList: unionProject,
			BindingTags: []int32{unionTag},
		}, nil)
	}

	unionProject := builder.qry.Nodes[unionID].ProjectList
	joinPredicates := make([]*plan.Expr, len(branchKeys[0]))
	for keyIdx, key := range branchKeys[0] {
		equality := DeepCopyExpr(key.equality)
		unionKey := &plan.Expr{
			Typ: unionProject[keyIdx].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: unionTag,
				ColPos: int32(keyIdx),
			}},
		}
		equality.GetF().Args[key.innerArgIdx] = unionKey
		equality.Typ.NotNullable = key.outer.Typ.NotNullable && unionKey.Typ.NotNullable
		joinPredicates[keyIdx] = equality
	}

	return builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{baseID, unionID},
		JoinType: plan.Node_SEMI,
		OnList:   joinPredicates,
		SpillMem: builder.joinSpillMem,
	}, nil), true
}

func collectPositiveExistenceMarkTags(expr *plan.Expr, tags *[]int32) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	funcID, _ := function.DecodeOverloadID(fn.Func.GetObj())
	if funcID == function.OR {
		return len(fn.Args) == 2 &&
			collectPositiveExistenceMarkTags(fn.Args[0], tags) &&
			collectPositiveExistenceMarkTags(fn.Args[1], tags)
	}

	if funcID != function.ISTRUE || len(fn.Args) != 1 {
		return false
	}
	marker := fn.Args[0].GetCol()
	if marker == nil || marker.ColPos != 0 {
		return false
	}
	*tags = append(*tags, marker.RelPos)
	return true
}

func (builder *QueryBuilder) planSubtreeContainsVolatile(nodeID int32) bool {
	visited := make(map[int32]bool)
	var visit func(int32) bool
	visit = func(currentID int32) bool {
		if currentID < 0 || int(currentID) >= len(builder.qry.Nodes) || visited[currentID] {
			return false
		}
		visited[currentID] = true
		node := builder.qry.Nodes[currentID]
		expressions := [][]*plan.Expr{
			node.OnList,
			node.FilterList,
			node.ProjectList,
			node.GroupBy,
			node.AggList,
			node.WinSpecList,
			node.TblFuncExprList,
			node.BlockFilterList,
			node.FillVal,
			node.OnUpdateExprs,
			node.TimeWindowPartitionBy,
		}
		for _, list := range expressions {
			for _, expr := range list {
				if ContainsVolatileFunction(expr) {
					return true
				}
			}
		}
		for _, expr := range []*plan.Expr{
			node.Limit,
			node.Offset,
			node.Interval,
			node.Sliding,
			node.Timestamp,
			node.WEnd,
		} {
			if expr != nil && ContainsVolatileFunction(expr) {
				return true
			}
		}
		for _, orderBy := range node.OrderBy {
			if ContainsVolatileFunction(orderBy.Expr) {
				return true
			}
		}
		for _, childID := range node.Children {
			if visit(childID) {
				return true
			}
		}
		return false
	}
	return visit(nodeID)
}

func (builder *QueryBuilder) extractExistenceJoinKeys(markNode *plan.Node) ([]existenceJoinKey, bool) {
	if len(markNode.OnList) == 0 {
		return nil, false
	}
	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(markNode.Children[0]) {
		leftTags[tag] = true
	}
	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(markNode.Children[1]) {
		rightTags[tag] = true
	}

	keys := make([]existenceJoinKey, 0, len(markNode.OnList))
	for _, predicate := range markNode.OnList {
		if !isTruncationSafePredicateExpr(predicate) {
			return nil, false
		}
		fn := predicate.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 2 || !IsEqualFunc(fn.Func.GetObj()) {
			return nil, false
		}
		leftSide := getJoinSideWithOuterScope(fn.Args[0], leftTags, rightTags, markNode.BindingTags[0])
		rightSide := getJoinSideWithOuterScope(fn.Args[1], leftTags, rightTags, markNode.BindingTags[0])

		var outer, inner *plan.Expr
		innerArgIdx := 1
		switch {
		case leftSide == JoinSideLeft && rightSide == JoinSideRight:
			outer, inner = fn.Args[0], fn.Args[1]
		case leftSide == JoinSideRight && rightSide == JoinSideLeft:
			outer, inner = fn.Args[1], fn.Args[0]
			innerArgIdx = 0
		default:
			return nil, false
		}
		for _, key := range keys {
			if exprStructuralEqual(key.outer, outer) {
				return nil, false
			}
		}
		keys = append(keys, existenceJoinKey{
			outer:       outer,
			inner:       inner,
			equality:    predicate,
			innerArgIdx: innerArgIdx,
		})
	}
	return keys, true
}

func unwrapIsTrueFromMarkJoinEqualities(
	conditions []*plan.Expr,
	leftTags, rightTags map[int32]bool,
	markTag int32,
) []*plan.Expr {
	for i, condition := range conditions {
		isTrue := condition.GetF()
		if isTrue == nil || isTrue.Func == nil || len(isTrue.Args) != 1 {
			continue
		}
		funcID, _ := function.DecodeOverloadID(isTrue.Func.GetObj())
		if funcID != function.ISTRUE {
			continue
		}

		equality := isTrue.Args[0]
		equalFunc := equality.GetF()
		if equalFunc == nil || equalFunc.Func == nil || len(equalFunc.Args) != 2 || !IsEqualFunc(equalFunc.Func.GetObj()) {
			continue
		}
		if !isTruncationSafePredicateExpr(equality) {
			continue
		}

		leftSide := getJoinSideWithOuterScope(equalFunc.Args[0], leftTags, rightTags, markTag)
		rightSide := getJoinSideWithOuterScope(equalFunc.Args[1], leftTags, rightTags, markTag)
		if leftSide == JoinSideLeft && rightSide == JoinSideRight ||
			leftSide == JoinSideRight && rightSide == JoinSideLeft {
			conditions[i] = equality
		}
	}

	return conditions
}

// referencesSyntheticGroupKey reports whether expr cannot be rewritten below
// an aggregate because it refers to a group-key position synthesized by that
// aggregate branch. Invalid positions and expression variants that
// replaceColRefs cannot safely rewrite are kept above the aggregate as well.
func referencesSyntheticGroupKey(expr *plan.Expr, groupTag int32, groupCount int, groupingFlag []bool) bool {
	if expr == nil {
		return true
	}

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F == nil {
			return true
		}
		for _, arg := range exprImpl.F.Args {
			if referencesSyntheticGroupKey(arg, groupTag, groupCount, groupingFlag) {
				return true
			}
		}
		return false

	case *plan.Expr_W:
		// replaceColRefs does not assign its rewritten WindowSpec children back.
		// Keep windows with group output references above the aggregate.
		return exprImpl.W == nil || containsTag(expr, groupTag)

	case *plan.Expr_List:
		// replaceColRefs does not recurse into Expr_List. Keep a list that
		// contains any group output reference above the aggregate, including
		// active keys, rather than pushing an expression with stale tags.
		return exprImpl.List == nil || containsTag(expr, groupTag)

	case *plan.Expr_Col:
		if exprImpl.Col == nil || exprImpl.Col.RelPos != groupTag {
			return exprImpl.Col == nil
		}
		colPos := exprImpl.Col.ColPos
		if colPos < 0 || int(colPos) >= groupCount {
			return true
		}
		return len(groupingFlag) > 0 &&
			(int(colPos) >= len(groupingFlag) || !groupingFlag[colPos])

	case *plan.Expr_Sub, *plan.Expr_Corr:
		return true

	case *plan.Expr_Lit, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Raw,
		*plan.Expr_T, *plan.Expr_Max, *plan.Expr_Vec, *plan.Expr_Fold:
		return false

	default:
		return true
	}
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
		candidateLimit, ok := buildCandidateLimit(nodePushDown.Limit, nodePushDown.Offset)
		if !ok {
			goto END
		}
		nodePushDown.Offset = nil
		nodePushDown.Limit = candidateLimit
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
	if node.NodeType == plan.Node_PROJECT && len(node.Children) > 0 &&
		(node.Limit != nil || node.Offset != nil) {
		child := builder.qry.Nodes[node.Children[0]]
		if child.NodeType == plan.Node_TABLE_SCAN {
			if limit, offset, ok := composePagination(
				child.Limit, child.Offset, node.Limit, node.Offset,
			); ok {
				child.Limit, child.Offset = limit, offset
				node.Limit, node.Offset = nil, nil
			}
		} else if node.Offset == nil &&
			child.NodeType == plan.Node_FUNCTION_SCAN &&
			child.TableDef != nil && child.TableDef.TblFunc != nil &&
			child.TableDef.TblFunc.Name == "mo_check_constraints" {
			// CHECK_CONSTRAINTS is a source function whose rows have no
			// ordering contract.  A plain LIMIT can therefore be evaluated
			// by the producer, but OFFSET (or a sort above it) must remain
			// outside so that the result semantics are unchanged.
			if limit, offset, ok := composePagination(
				child.Limit, child.Offset, node.Limit, nil,
			); ok {
				child.Limit, child.Offset = limit, offset
				node.Limit = nil
			}
		}
	}
}

func (builder *QueryBuilder) pushdownVectorIndexTopToTableScan(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	for _, childID := range node.Children {
		builder.pushdownVectorIndexTopToTableScan(childID)
	}
	if builder.optimizerHints != nil && builder.optimizerHints.pushDownLimitToScan != 0 {
		return
	}

	if node.NodeType != plan.Node_SORT || node.Limit == nil || node.Offset != nil {
		return
	}

	if len(node.OrderBy) != 1 {
		return
	}

	orderCol := node.OrderBy[0].Expr.GetCol()
	if orderCol == nil {
		return
	}

	projNode := builder.qry.Nodes[node.Children[0]]
	if projNode.NodeType != plan.Node_PROJECT || len(projNode.Children) == 0 {
		return
	}

	// The ORDER BY column indexes the child project's list, but the two can disagree:
	// pruning a derived table's projection (`select count(*) from (<top-k>) t`) empties
	// the list while the sort keeps its pre-pruning ColPos. This runs on the final plan,
	// after applyIndices, and the entries-table gate that would reject such a shape is
	// below — so check before dereferencing rather than panicking the CN.
	if orderCol.ColPos < 0 || int(orderCol.ColPos) >= len(projNode.ProjectList) {
		return
	}
	orderFunc := projNode.ProjectList[orderCol.ColPos]
	if metric.DistFuncOpTypes[orderFunc.GetF().GetFunc().GetObjName()] == "" {
		return
	}

	scanNode := builder.qry.Nodes[projNode.Children[0]]
	if scanNode.NodeType != plan.Node_TABLE_SCAN || scanNode.Offset != nil || scanNode.OrderBy != nil {
		return
	}
	limitVal, literal := getLiteralUint64(node.Limit)
	if !literal || limitVal == 0 {
		return
	}
	if limitVal > maxVectorIndexTopPushdownLimit {
		return
	}
	if scanNode.TableDef.TableType != catalog.SystemSI_IVFFLAT_TblType_Entries {
		return
	}

	scanNode.IndexReaderParam = &plan.IndexReaderParam{
		OrderBy: []*plan.OrderBySpec{
			{
				Expr:      orderFunc,
				Collation: node.OrderBy[0].Collation,
				Flag:      node.OrderBy[0].Flag,
			},
		},
		Limit: DeepCopyExpr(node.Limit),
	}

	// if there is a limit, outcnt is limit number
	scanNode.Stats.Outcnt = float64(scanNode.Stats.BlockNum) * float64(limitVal)
	scanNode.Stats.Cost = float64(scanNode.Stats.BlockNum * objectio.BlockMaxRows)

	orderFuncTag := builder.genNewBindTag()
	scanNode.BindingTags = append(scanNode.BindingTags, orderFuncTag)
	projNode.ProjectList[orderCol.ColPos] = &plan.Expr{
		Typ: orderFunc.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: orderFuncTag,
				ColPos: 0,
			},
		},
	}

	builder.nameByColRef[[2]int32{orderFuncTag, 0}] = "__dist_func__"
}

// exprColRefsSubsetOf returns true when every column reference in expr
// belongs to the given set. An expression with no column references
// (e.g. a constant) is considered a subset. Unhandled expression
// variants conservatively return false to avoid incorrect pushdown.
func exprColRefsSubsetOf(expr *plan.Expr, set map[[2]int32]bool) bool {
	if expr == nil {
		return true
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		return set[[2]int32{e.Col.RelPos, e.Col.ColPos}]
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			if !exprColRefsSubsetOf(arg, set) {
				return false
			}
		}
		return true
	case *plan.Expr_Lit, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Raw, *plan.Expr_Vec, *plan.Expr_Max, *plan.Expr_T, *plan.Expr_Fold:
		return true
	default:
		return false
	}
}
