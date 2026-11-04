// Copyright 2026 Matrix Origin
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

import plan "github.com/matrixorigin/matrixone/pkg/pb/plan"

// EXISTS(P OR Q) needs one TRUE witness, not a union of matching outer rows.
// Give each equality its own hashable MARK and combine the totalized markers.
// Admission is read-only; no input is copied until every arm and the complete
// inner relation have been proved safe to evaluate independently.
func (builder *QueryBuilder) tryExistentialDisjunction(
	left, right int32, predicates []*plan.Expr, negate bool, ctx *BindContext,
) (int32, *plan.Expr, bool, error) {
	if builder.qry.StmtType != plan.Query_SELECT || builder.isForUpdate || builder.subqueryPredicatePlanningDisabled() ||
		len(predicates) != 1 {
		return 0, nil, false, nil
	}
	fn := predicates[0].GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "or" {
		return 0, nil, false, nil
	}
	var arms []*plan.Expr
	if !existentialSplit(predicates[0], "or", &arms, maxExistentialArms) || len(arms) < 2 {
		return 0, nil, false, nil
	}
	leftTags, rightTags := make(map[int32]bool), make(map[int32]bool)
	for _, tag := range builder.enumerateTags(left) {
		leftTags[tag] = true
	}
	for _, tag := range builder.enumerateTags(right) {
		rightTags[tag] = true
	}
	for _, arm := range arms {
		fn := arm.GetF()
		if fn == nil || fn.Func == nil || !IsEqualFunc(fn.Func.Obj) || len(fn.Args) != 2 {
			return 0, nil, false, nil
		}
		a := getJoinSideWithOuterScope(fn.Args[0], leftTags, rightTags, -1)
		b := getJoinSideWithOuterScope(fn.Args[1], leftTags, rightTags, -1)
		if !(a == JoinSideLeft && b == JoinSideRight || a == JoinSideRight && b == JoinSideLeft) {
			return 0, nil, false, nil
		}
	}
	if !builder.existentialJoinPredicatesAreTotal(predicates, left, right) {
		return 0, nil, false, nil
	}
	chain, ok := builder.existentialDisjunctionInput(right, rightTags)
	if !ok {
		return 0, nil, false, nil
	}

	markers := make([]*plan.Expr, 0, len(arms))
	for _, arm := range arms {
		if err := builder.checkPlanningCanceled(); err != nil {
			return 0, nil, true, err
		}
		input, tags := builder.copyExistentialDisjunctionInput(chain)
		predicate := DeepCopyExpr(arm)
		for old, fresh := range tags {
			replaceColRefTag(predicate, old, fresh)
		}
		var marker *plan.Expr
		var err error
		left, marker, err = builder.insertMarkJoin(left, input, []*plan.Expr{predicate}, nil, false, ctx)
		if err != nil {
			return 0, nil, true, err
		}
		markers = append(markers, marker)
	}
	result, err := combinePlanExprsBalanced(builder.GetContext(), "or", markers)
	if err == nil && negate {
		result, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "not", []*plan.Expr{result})
	}
	return left, result, true, err
}

// A simple same-block scan chain has no shared evaluation identity. Never copy
// CTEs, views, joins or row-limiting/materializing operators into separate arms.
func (builder *QueryBuilder) existentialDisjunctionInput(root int32, tags map[int32]bool) ([]int32, bool) {
	owner := builder.ctxByNode[root]
	if owner == nil {
		return nil, false
	}
	var chain []int32
	for id := root; ; {
		node := builder.qry.Nodes[id]
		ctx := builder.ctxByNode[id]
		if ctx == nil || ctx.queryBlockOwner != owner.queryBlockOwner ||
			node.Limit != nil || node.Offset != nil || len(node.OrderBy) != 0 ||
			len(node.ViewPath) != 0 || len(node.LockTargets) != 0 || node.FilterIsBarrier || node.IsEnd {
			return nil, false
		}
		for _, predicate := range node.FilterList {
			if !existentialDisjunctionExprIsLocal(predicate, tags) ||
				!(builder.existentialPredicateIsTotal(predicate, nil) || existentialPredicateSafe(predicate)) {
				return nil, false
			}
		}
		for _, expr := range node.ProjectList {
			if !existentialDisjunctionExprIsLocal(expr, tags) || !builder.existentialValueIsTotal(expr, nil) {
				return nil, false
			}
		}
		chain = append(chain, id)
		switch node.NodeType {
		case plan.Node_TABLE_SCAN:
			if len(node.BindingTags) != 1 || len(node.Children) != 0 || node.TableDef == nil || node.TableDef.ViewSql != nil {
				return nil, false
			}
			return chain, true
		case plan.Node_PROJECT:
			if len(node.BindingTags) != 1 || len(node.Children) != 1 {
				return nil, false
			}
		case plan.Node_FILTER:
			if len(node.BindingTags) != 0 || len(node.Children) != 1 {
				return nil, false
			}
		default:
			return nil, false
		}
		id = node.Children[0]
	}
}

func existentialDisjunctionExprIsLocal(expr *plan.Expr, tags map[int32]bool) bool {
	if !containsOnlyTags(expr, tags) {
		return false
	}
	immutable := true
	existentialWalk(expr, func(e *plan.Expr) {
		if e.PreparedNumeric != nil || e.GetP() != nil || e.GetLit() != nil && e.GetLit().Src != nil {
			immutable = false
		}
	})
	return immutable
}

func (builder *QueryBuilder) copyExistentialDisjunctionInput(chain []int32) (int32, map[int32]int32) {
	tags := make(map[int32]int32)
	var input int32
	for i := len(chain) - 1; i >= 0; i-- {
		id := chain[i]
		node := DeepCopyNode(builder.qry.Nodes[id])
		if len(node.Children) != 0 {
			node.Children = []int32{input}
		}
		if len(node.BindingTags) != 0 {
			old := node.BindingTags[0]
			if node.NodeType == plan.Node_TABLE_SCAN {
				builder.rebindScanNode(node)
			} else {
				node.BindingTags[0] = builder.genNewBindTag()
				for col := range node.ProjectList {
					builder.nameByColRef[[2]int32{node.BindingTags[0], int32(col)}] = builder.nameByColRef[[2]int32{old, int32(col)}]
				}
			}
			tags[old] = node.BindingTags[0]
		}
		// Block filters are derived again from the copied semantic filters.
		node.BlockFilterList = nil
		for _, list := range [][]*plan.Expr{node.FilterList, node.ProjectList} {
			for _, expr := range list {
				for old, fresh := range tags {
					replaceColRefTag(expr, old, fresh)
				}
			}
		}
		input = builder.appendNode(node, builder.ctxByNode[id])
		builder.inheritIndexHints(input, id)
	}
	return input, tags
}
