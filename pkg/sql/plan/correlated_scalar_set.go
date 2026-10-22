// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// Equality partitions can retain the existing scalar UNION route. Other
// predicates need set membership rebuilt after matching each outer row.
func (builder *QueryBuilder) scalarSetNeedsReaggregation(root int32) bool {
	n := builder.qry.Nodes[root]
	for n.NodeType == plan.Node_PROJECT || n.NodeType == plan.Node_SORT {
		if len(n.Children) != 1 || n.Limit != nil || n.Offset != nil {
			return false
		}
		child := builder.qry.Nodes[n.Children[0]]
		if n.NodeType == plan.Node_PROJECT {
			if len(n.ProjectList) != 1 || len(child.ProjectList) != 1 || len(child.BindingTags) != 1 {
				return false
			}
			col := n.ProjectList[0].GetCol()
			if col == nil || col.RelPos != child.BindingTags[0] || col.ColPos != 0 || !isSameColumnType(n.ProjectList[0].Typ, child.ProjectList[0].Typ) {
				return false
			}
		}
		root = n.Children[0]
		n = child
	}
	if len(n.ProjectList) != 1 || n.NodeType != plan.Node_UNION || n.Limit != nil || n.Offset != nil || builder.subtreeContainsCorrelatedAggregate(root) || builder.planSubtreeContainsVolatile(root) {
		return false
	}
	var needs func(int32) bool
	needs = func(id int32) bool {
		node := builder.qry.Nodes[id]
		for _, pred := range node.FilterList {
			if hasCorrCol(pred) {
				if _, ok := correlatedWindowIdentityKey(pred); !ok {
					return true
				}
			}
		}
		for _, child := range node.Children {
			if needs(child) {
				return true
			}
		}
		return false
	}
	return needs(root)
}

// subtreeContainsCorrelatedAggregate rejects only aggregates whose input or
// aggregate expressions depend on the outer row. An independent aggregate in a
// JOIN branch is a scalar constant for every outer row and can remain in the
// branch while the surrounding UNION is reaggregated after correlation.
func (builder *QueryBuilder) subtreeContainsCorrelatedAggregate(root int32) bool {
	seen := make(map[int32]bool)
	var hasCorrelation func(int32, map[int32]bool) bool
	hasCorrelation = func(id int32, active map[int32]bool) bool {
		if id < 0 || int(id) >= len(builder.qry.Nodes) || builder.qry.Nodes[id] == nil {
			return true
		}
		if active[id] {
			return true
		}
		active[id] = true
		defer delete(active, id)
		node := builder.qry.Nodes[id]
		for _, exprs := range [][]*plan.Expr{node.ProjectList, node.FilterList, node.OnList, node.GroupBy, node.AggList, node.WinSpecList} {
			for _, expr := range exprs {
				if hasCorrCol(expr) {
					return true
				}
			}
		}
		for _, order := range node.OrderBy {
			if order != nil && hasCorrCol(order.Expr) {
				return true
			}
		}
		for _, child := range node.Children {
			if hasCorrelation(child, active) {
				return true
			}
		}
		return false
	}

	var visit func(int32) bool
	visit = func(id int32) bool {
		if id < 0 || int(id) >= len(builder.qry.Nodes) || builder.qry.Nodes[id] == nil {
			return true
		}
		if seen[id] {
			return false
		}
		seen[id] = true
		node := builder.qry.Nodes[id]
		if node.NodeType == plan.Node_AGG && hasCorrelation(id, make(map[int32]bool)) {
			return true
		}
		for _, child := range node.Children {
			if visit(child) {
				return true
			}
		}
		return false
	}
	return visit(root)
}

func (builder *QueryBuilder) flattenScalarSetAfterCorrelation(
	outerID, subID int32, subCtx *BindContext, preds []*plan.Expr, ctx *BindContext, subquery *plan.SubqueryRef,
) (int32, *plan.Expr, error) {
	fail := func() (int32, *plan.Expr, error) {
		return 0, nil, moerr.NewNYI(builder.GetContext(), "correlated scalar UNION requires preserved outer row identity")
	}
	if len(subCtx.results) != 1 || subquery.Child != nil {
		return fail()
	}
	// Collect only the current stream's output boundary. A derived relation or
	// a join need not expose a base Row_ID, and one base Row_ID need not identify
	// every occurrence in a join or UNION ALL result.
	outputs, ok := builder.collectScalarOuterOutputs(outerID, -1, 0, ctx)
	if !ok {
		return fail()
	}
	groupTag := builder.genNewBindTag()
	outerGroups := make([]*plan.Expr, 0, len(outputs)+1)
	var aliases []scalarReaggAlias
	for _, output := range outputs {
		pos := int32(len(outerGroups))
		outerGroups = append(outerGroups, GetColExpr(output.typ, output.ref[0], output.ref[1]))
		aliases = append(aliases, scalarReaggAlias{ref: output.ref, groupPos: pos})
		for _, ref := range output.aliases {
			aliases = append(aliases, scalarReaggAlias{ref: ref, groupPos: pos})
		}
	}
	// An unpartitioned WINDOW merges its input scopes and numbers the complete
	// current stream once. Number after DISTINCT/JOIN/projection, before the
	// candidate join, so equal outer values and join-expanded rows stay separate.
	rowNumber, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "row_number", nil)
	if err != nil {
		return 0, nil, err
	}
	identityTag := builder.genNewBindTag()
	identity := &plan.Expr{Typ: rowNumber.Typ, Expr: &plan.Expr_W{W: &plan.WindowSpec{
		WindowFunc: rowNumber, Name: "row_number",
		Frame: &plan.FrameClause{Type: plan.FrameClause_ROWS,
			Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
			End:   &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, UnBounded: true}},
	}}}
	outerID = builder.appendNode(&plan.Node{NodeType: plan.Node_WINDOW, Children: []int32{outerID},
		BindingTags: []int32{identityTag}, WinSpecList: []*plan.Expr{identity}, ScalarSetReaggregation: true}, ctx)
	identityPos := int32(len(outerGroups))
	outerGroups = append(outerGroups, GetColExpr(rowNumber.Typ, identityTag, 0))
	// A separate marker distinguishes an empty match from one real NULL member.
	root := builder.qry.Nodes[subID]
	if len(root.BindingTags) != 1 || len(root.ProjectList) == 0 {
		return fail()
	}
	oldTag, bagTag := root.BindingTags[0], builder.genNewBindTag()
	projects := make([]*plan.Expr, len(root.ProjectList), len(root.ProjectList)+1)
	for i, expr := range root.ProjectList {
		projects[i] = GetColExpr(expr.Typ, oldTag, int32(i))
	}
	markerPos := int32(len(projects))
	projects = append(projects, newSubqueryBoolConst(true))
	subID = builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{subID}, BindingTags: []int32{bagTag}, ProjectList: projects}, subCtx)
	for _, pred := range preds {
		_ = plan.VisitExprTree(pred, func(expr *plan.Expr) error {
			if col := expr.GetCol(); col != nil && col.RelPos == oldTag {
				col.RelPos = bagTag
			}
			return nil
		})
	}
	filters, on := decreaseDepthAndDispatch(preds)
	if len(filters) != 0 {
		return fail()
	}
	joined := builder.appendNode(&plan.Node{NodeType: plan.Node_JOIN, Children: []int32{outerID, subID}, JoinType: plan.Node_LEFT, OnList: on, SpillMem: builder.joinSpillMem, ScalarSetReaggregation: true}, ctx)
	value := GetColExpr(root.ProjectList[0].Typ, bagTag, 0)
	value.Typ.NotNullable = false
	marker := GetColExpr(projects[markerPos].Typ, bagTag, markerPos)
	marker.Typ.NotNullable = false
	keys, err := buildSetOperationPhysicalKeys(builder.GetContext(), []*plan.Expr{value}, []bool{hasPadSpaceStringProvenance(value)})
	if err != nil {
		return 0, nil, err
	}
	key := DeepCopyExpr(value)
	if len(keys) > 0 {
		key = keys[0]
	}
	groups := DeepCopyExprList(outerGroups)
	memberMarker, memberValue, memberKey := int32(len(groups)), int32(len(groups)+1), int32(len(groups)+2)
	groups = append(groups, marker, value, key)
	// Only identity, match presence and the visible set key determine membership.
	// Hidden correlation slots never become set keys. Other group columns retain
	// their original representatives, including the visible negative-zero bits.
	first := builder.appendNode(&plan.Node{NodeType: plan.Node_AGG, Children: []int32{joined}, BindingTags: []int32{groupTag, builder.genNewBindTag()}, GroupBy: groups,
		GroupByHashKey: []int32{identityPos, memberMarker, memberKey}, SpillMem: builder.aggSpillMem}, ctx)
	count, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "count", []*plan.Expr{GetColExpr(marker.Typ, groupTag, memberMarker)})
	if err != nil {
		return 0, nil, err
	}
	representative, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "any_value", []*plan.Expr{GetColExpr(value.Typ, groupTag, memberValue)})
	if err != nil {
		return 0, nil, err
	}
	aggTag := builder.genNewBindTag()
	secondGroups := make([]*plan.Expr, len(outerGroups))
	for i, expr := range outerGroups {
		secondGroups[i] = GetColExpr(expr.Typ, groupTag, int32(i))
	}
	second := builder.appendNode(&plan.Node{NodeType: plan.Node_AGG, Children: []int32{first}, BindingTags: []int32{groupTag, aggTag}, GroupBy: secondGroups,
		GroupByHashKey: []int32{identityPos}, AggList: []*plan.Expr{count, representative}, SpillMem: builder.aggSpillMem}, ctx)
	if builder.scalarReaggAliases == nil {
		builder.scalarReaggAliases = make(map[int32][]scalarReaggAlias)
	}
	builder.scalarReaggAliases[first], builder.scalarReaggAliases[second] = aliases, aliases
	// SINGLE owns the established 1242/21000 error: one constant row matches
	// counts 0/1, both match larger counts. No outer producer is evaluated twice.
	var arms []int32
	for _, v := range []int64{0, 1} {
		scan := builder.appendNode(&plan.Node{NodeType: plan.Node_VALUE_SCAN}, ctx)
		arms = append(arms, builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{scan}, BindingTags: []int32{builder.genNewBindTag()}, ProjectList: []*plan.Expr{makePlan2Int64ConstExprWithType(v)}}, ctx))
	}
	constantTag := builder.genNewBindTag()
	constant := makePlan2Int64ConstExprWithType(0)
	constants := builder.appendNode(&plan.Node{NodeType: plan.Node_UNION_ALL, Children: arms, BindingTags: []int32{constantTag}, ProjectList: []*plan.Expr{GetColExpr(constant.Typ, builder.qry.Nodes[arms[0]].BindingTags[0], 0)}}, ctx)
	tooMany, err := BindFuncExprImplByPlanExpr(builder.GetContext(), ">", []*plan.Expr{GetColExpr(count.Typ, aggTag, 0), makePlan2Int64ConstExprWithType(1)})
	if err != nil {
		return 0, nil, err
	}
	firstConstant, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{GetColExpr(constant.Typ, constantTag, 0), constant})
	if err != nil {
		return 0, nil, err
	}
	check, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*plan.Expr{tooMany, firstConstant})
	if err != nil {
		return 0, nil, err
	}
	result := builder.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_SINGLE, Children: []int32{second, constants}, OnList: []*plan.Expr{check}, SpillMem: builder.joinSpillMem}, ctx)
	return result, GetColExpr(representative.Typ, aggTag, 1), nil
}
