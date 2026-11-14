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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// Grouping sets merge branch-owned grouped or sampled values before global
// consumers such as windows and DISTINCT. SAMPLE retains its post-sample
// projection. This binding-local state never escapes into the source AST.
type groupingStage struct {
	orderBy      tree.OrderBy
	projects     []*plan.Expr
	headings     []string
	bareNulls    map[int]bool
	orderBys     []*plan.OrderBySpec
	having       []*plan.Expr
	resultLen    int
	notCacheable bool
	distinct     bool

	// SAMPLE also carries arbitrary input columns through its output. Its
	// complete post-sample projection is transported, rather than a key list.
	projectionInputTag int32
	inputs             map[[2]int32]int32

	// Dependency identity must precede branch-local GROUPING folding. A
	// subtotal marker is neither a SQL literal nor a function of key values.
	sampleProjects []*plan.Expr
}

// Walk only this expression's scope. Subquery plans keep their own bindings;
// the quantified operand belongs to the outer scope. Frame bounds are constants.
func walkGroupingExpr(expr *plan.Expr, visit func(*plan.Expr)) {
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			walkGroupingExpr(arg, visit)
		}
	}
	if w := expr.GetW(); w != nil {
		walkGroupingExpr(w.WindowFunc, visit)
		for _, p := range w.PartitionBy {
			walkGroupingExpr(p, visit)
		}
		for _, order := range w.OrderBy {
			walkGroupingExpr(order.Expr, visit)
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			walkGroupingExpr(item, visit)
		}
	}
	if sub := expr.GetSub(); sub != nil && sub.Child != nil {
		walkGroupingExpr(sub.Child, visit)
	}
	// Rebinding can weaken a child proof. Consumers must see the new child
	// state before recomputing their own proof (notably quantified operands).
	visit(expr)
}

func (builder *QueryBuilder) appendGroupingStage(
	ctx *BindContext, nodeID int32, resultLen int, orders []*plan.OrderBySpec, having []*plan.Expr, notCacheable bool,
) (int32, error) {
	stage := ctx.groupingStage
	stage.projects, stage.headings = ctx.projects, ctx.headings
	stage.bareNulls = ctx.bareNulls
	// Provenance indexes describe the user projection, not this transport
	// projection. A user NULL at slot i must not retype aggregate input i.
	ctx.bareNulls = nil
	stage.resultLen, stage.orderBys = resultLen, orders
	_, stage.having = splitWindowDependentHavingFilters(having, ctx.windowTag)
	stage.notCacheable = notCacheable

	if ctx.sampleFunc.hasSampleFunc {
		stage.projectionInputTag = builder.genNewBindTag()
		selected := stage.sampleProjects[:resultLen]
		stage.projects = make([]*plan.Expr, len(ctx.projects))
		ctx.headings = make([]string, 0, len(ctx.projects))
		payload := make([]*plan.Expr, 0, len(ctx.projects))
		materialize := func(expr *plan.Expr) *plan.Expr {
			pos := int32(len(payload))
			payload = append(payload, expr)
			ctx.headings = append(ctx.headings, fmt.Sprintf("__grouping_%d", pos))
			return &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: stage.projectionInputTag, ColPos: pos,
				}},
			}
		}
		var rebuildOrder func(*plan.Expr) *plan.Expr
		rebuildOrder = func(expr *plan.Expr) *plan.Expr {
			copy := DeepCopyExpr(expr)
			if order, ok := groupingOrderExpr(copy, selected, ctx.projectTag, stage.projectionInputTag); ok {
				return order
			}
			// A mixed expression cannot fall back wholesale into the branch:
			// selected aliases belong to a later projection. Materialize only
			// unavailable dependencies, preserving their pre-fold identities.
			copy = DeepCopyExpr(expr)
			if fn := copy.GetF(); fn != nil && fn.Func.ObjName != "grouping" {
				for i, arg := range fn.Args {
					fn.Args[i] = rebuildOrder(arg)
				}
				return copy
			}
			if list := copy.GetList(); list != nil {
				for i, item := range list.List {
					list.List[i] = rebuildOrder(item)
				}
				return copy
			}
			if sub := copy.GetSub(); sub != nil {
				if sub.Child != nil {
					sub.Child = rebuildOrder(sub.Child)
				}
				return copy
			}
			rewriteSampleGrouping(copy, ctx)
			return materialize(copy)
		}
		for i, expr := range ctx.projects {
			if i < resultLen {
				stage.projects[i] = materialize(expr)
			} else {
				stage.projects[i] = rebuildOrder(stage.sampleProjects[i])
			}
		}
		ctx.projects = payload
	} else {
		stage.inputs = make(map[[2]int32]int32)
		collect := func(expr *plan.Expr) {
			walkGroupingExpr(expr, func(e *plan.Expr) {
				if c := e.GetCol(); c != nil && (c.RelPos == ctx.groupTag || c.RelPos == ctx.aggregateTag) {
					stage.inputs[[2]int32{c.RelPos, c.ColPos}] = -1
				}
			})
		}
		for _, exprs := range [][]*plan.Expr{stage.projects, ctx.windows, stage.having} {
			for _, expr := range exprs {
				collect(expr)
			}
		}
		for _, order := range orders {
			collect(order.Expr)
		}
		ctx.projects = make([]*plan.Expr, 0, len(stage.inputs))
		ctx.headings = make([]string, 0, cap(ctx.projects))
		for _, input := range []struct {
			tag   int32
			exprs []*plan.Expr
		}{{ctx.groupTag, ctx.groups}, {ctx.aggregateTag, ctx.aggregates}} {
			for i, expr := range input.exprs {
				key := [2]int32{input.tag, int32(i)}
				if _, used := stage.inputs[key]; !used {
					continue
				}
				stage.inputs[key] = int32(len(ctx.projects))
				typ := expr.Typ
				if input.tag == ctx.groupTag {
					typ = groupingOutputType(ctx.groups, ctx.groupingFlag, i)
				}
				ctx.headings = append(ctx.headings, fmt.Sprintf("__grouping_%d", len(ctx.projects)))
				ctx.projects = append(ctx.projects, &plan.Expr{
					Typ:  typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: input.tag, ColPos: int32(i)}},
				})
			}
		}
	}
	if len(ctx.projects) == 0 {
		// Carry cardinality without forcing an unused (possibly wide) key.
		ctx.projects = []*plan.Expr{MakePlan2Int64ConstExprWithType(1)}
		ctx.headings = []string{"__grouping_0"}
	}
	nodeID, err := builder.appendProjectionNode(ctx, nodeID, notCacheable)
	ctx.results = ctx.projects
	return nodeID, err
}

// Rebind already-bound expressions, including every window input. No function is
// rebound against branch-local input after grouping sets have been combined.
func rebindGroupingExpr(expr *plan.Expr, branch, output *BindContext, union *plan.Node) {
	walkGroupingExpr(expr, func(e *plan.Expr) {
		col := e.GetCol()
		if col == nil {
			if fn := e.GetF(); fn != nil {
				e.Typ.NotNullable = function.DeduceNotNullable(fn.Func.Obj, fn.Args)
			} else if w := e.GetW(); w != nil {
				e.Typ.NotNullable = function.DeduceWindowNotNullable(w)
				w.WindowFunc.Typ.NotNullable = e.Typ.NotNullable
			}
			return
		}
		if tag := branch.groupingStage.projectionInputTag; tag != 0 && col.RelPos == tag {
			col.RelPos = union.BindingTags[0]
			e.Typ = union.ProjectList[col.ColPos].Typ
			return
		}
		switch col.RelPos {
		case branch.groupTag, branch.aggregateTag:
			col.ColPos = branch.groupingStage.inputs[[2]int32{col.RelPos, col.ColPos}]
			col.RelPos = union.BindingTags[0]
			e.Typ = union.ProjectList[col.ColPos].Typ
		case branch.windowTag:
			col.RelPos = output.windowTag
		case branch.projectTag:
			col.RelPos = output.projectTag
		}
	})
}

// Hidden sort expressions must be evaluated after DISTINCT using only surviving
// output columns. This also keeps volatile sort expressions out of the keys.
func groupingDistinctOrder(expr *plan.Expr, selected []*plan.Expr, selectedTag, tag int32) (*plan.Expr, bool) {
	return groupingOrderExpr(expr, selected, selectedTag, tag)
}

func groupingOrderExpr(expr *plan.Expr, selected []*plan.Expr, selectedTag, tag int32) (*plan.Expr, bool) {
	if col := expr.GetCol(); col != nil && col.RelPos == selectedTag {
		if col.ColPos < 0 || int(col.ColPos) >= len(selected) {
			return nil, false
		}
		// An alias denotes its exact output, not the first structurally equal
		// expression. Independent volatile outputs can have identical trees.
		return &plan.Expr{Typ: selected[col.ColPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: col.ColPos}}}, true
	}
	for i, project := range selected {
		if exprStructuralEqual(expr, project) && groupingOrderCanReuse(expr) {
			return &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: int32(i)}}}, true
		}
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func.ObjName == "grouping" {
			// Key values do not establish lineage after DISTINCT; equal actual
			// and subtotal NULLs can have different masks. Both AGG and SAMPLE
			// must reuse a selected complete expression or materialized slot.
			return nil, false
		}
		for i, arg := range fn.Args {
			var ok bool
			fn.Args[i], ok = groupingOrderExpr(arg, selected, selectedTag, tag)
			if !ok {
				return nil, false
			}
		}
		return expr, true
	}
	if list := expr.GetList(); list != nil {
		for i, item := range list.List {
			var ok bool
			list.List[i], ok = groupingOrderExpr(item, selected, selectedTag, tag)
			if !ok {
				return nil, false
			}
		}
		return expr, true
	}
	switch expr.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_P, *plan.Expr_V, *plan.Expr_T, *plan.Expr_Vec, *plan.Expr_Fold:
		return expr, true
	default:
		return nil, false
	}
}

// Structural equality is only a fallback for deterministic expressions. An
// independent volatile call (also under a deterministic wrapper) is not an alias.
func groupingOrderCanReuse(expr *plan.Expr) bool {
	reusable := true
	walkGroupingExpr(expr, func(e *plan.Expr) {
		if fn := e.GetF(); fn != nil {
			ov, exists := function.GetFunctionByIdWithoutError(fn.Func.Obj)
			if !exists || ov.CannotFold() || ov.IsRealTimeRelated() {
				reusable = false
			}
		}
	})
	return reusable
}

func (builder *QueryBuilder) finishGroupingStage(
	nodeID int32, branch, ctx *BindContext, limit *tree.Limit, rank *tree.RankOption, distinct, isRoot bool,
) (int32, error) {
	stage := branch.groupingStage
	union := builder.qry.Nodes[nodeID]
	copyExprs := func(exprs []*plan.Expr) []*plan.Expr {
		out := DeepCopyExprList(exprs)
		for _, expr := range out {
			rebindGroupingExpr(expr, branch, ctx, union)
		}
		return out
	}
	ctx.headings, ctx.bareNulls = stage.headings, stage.bareNulls
	ctx.projects = copyExprs(stage.projects)
	ctx.windows = copyExprs(branch.windows)
	ctx.windowByAst = branch.windowByAst
	for i, name := range ctx.headings {
		builder.nameByColRef[[2]int32{ctx.projectTag, int32(i)}] = name
	}
	var err error
	if len(ctx.windows) > 0 {
		nodeID, err = builder.appendWindowNode(ctx, nodeID, copyExprs(stage.having))
		if err != nil {
			return 0, err
		}
	}
	orders := DeepCopyOrderBySpecList(stage.orderBys)
	for _, order := range orders {
		rebindGroupingExpr(order.Expr, branch, ctx, union)
	}

	if distinct {
		distinctTag := builder.genNewBindTag()
		// Resolve selected-expression dependencies before flattening replaces
		// subquery identities with fresh join output references.
		resolved := make(map[int]*plan.Expr)
		for i := stage.resultLen; i < len(ctx.projects); i++ {
			if expr, ok := groupingDistinctOrder(DeepCopyExpr(ctx.projects[i]), ctx.projects[:stage.resultLen], ctx.projectTag, distinctTag); ok {
				resolved[i] = expr
			}
		}
		// This projection is an expression consumer just like the ordinary
		// final projection: flatten subqueries before it becomes DISTINCT keys.
		for i, project := range ctx.projects {
			if _, ok := resolved[i]; ok {
				continue
			}
			nodeID, ctx.projects[i], err = builder.flattenSubqueries(nodeID, project, ctx)
			if err != nil {
				return 0, err
			}
		}
		selected := ctx.projects[:stage.resultLen]
		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT, ProjectList: selected,
			Children: []int32{nodeID}, BindingTags: []int32{distinctTag},
		}, ctx)
		nodeID = builder.appendDistinctNode(ctx, nodeID)
		projects := make([]*plan.Expr, len(ctx.projects))
		for i := range projects {
			if i < stage.resultLen {
				projects[i] = &plan.Expr{Typ: selected[i].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: distinctTag, ColPos: int32(i)}}}
				builder.nameByColRef[[2]int32{distinctTag, int32(i)}] = ctx.headings[i]
			} else if expr, ok := resolved[i]; ok {
				projects[i] = expr
			} else {
				var ok bool
				projects[i], ok = groupingDistinctOrder(DeepCopyExpr(ctx.projects[i]), selected, ctx.projectTag, distinctTag)
				if !ok {
					return 0, moerr.NewSyntaxError(builder.GetContext(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
				}
			}
		}
		ctx.projects = projects
	}

	nodeID, err = builder.appendProjectionNode(ctx, nodeID, stage.notCacheable)
	if err != nil {
		return 0, err
	}
	if len(orders) > 0 {
		nodeID = builder.appendSortNode(ctx, nodeID, orders)
	}
	if limit != nil || rank != nil {
		offset, count, option, err := builder.bindLimit(ctx, limit, rank)
		if err != nil {
			return 0, err
		}
		n := builder.qry.Nodes[nodeID]
		n.Offset, n.Limit, n.RankOption = offset, count, option
		if lit := count.GetLit(); lit != nil {
			ctx.hasSingleRow = lit.GetU64Val() == 1
		}
	}
	if builder.qry.Nodes[nodeID].NodeType != plan.Node_PROJECT {
		nodeID = builder.appendResultProjectionNode(ctx, nodeID, stage.resultLen)
	} else {
		ctx.results = ctx.projects[:stage.resultLen]
	}
	if isRoot {
		builder.qry.Headings = append(builder.qry.Headings, ctx.headings...)
	}
	return nodeID, nil
}
