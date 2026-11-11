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

	// SAMPLE also carries arbitrary input columns through its output. Its
	// complete post-sample projection is transported, rather than a key list.
	projectionInputTag int32
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
	stage.resultLen, stage.orderBys, stage.having = resultLen, orders, having
	stage.notCacheable = notCacheable

	if ctx.sampleFunc.hasSampleFunc {
		stage.projectionInputTag = builder.genNewBindTag()
		stage.projects = make([]*plan.Expr, len(ctx.projects))
		ctx.headings = make([]string, len(ctx.projects))
		for i, expr := range ctx.projects {
			ctx.headings[i] = fmt.Sprintf("__grouping_%d", i)
			stage.projects[i] = &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: stage.projectionInputTag, ColPos: int32(i),
				}},
			}
		}
	} else {
		ctx.projects = make([]*plan.Expr, 0, len(ctx.groups)+len(ctx.aggregates))
		ctx.headings = make([]string, 0, cap(ctx.projects))
		for _, input := range []struct {
			tag   int32
			exprs []*plan.Expr
		}{{ctx.groupTag, ctx.groups}, {ctx.aggregateTag, ctx.aggregates}} {
			for i, expr := range input.exprs {
				ctx.headings = append(ctx.headings, fmt.Sprintf("__grouping_%d", len(ctx.projects)))
				ctx.projects = append(ctx.projects, &plan.Expr{
					Typ:  expr.Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: input.tag, ColPos: int32(i)}},
				})
			}
		}
	}
	nodeID, err := builder.appendProjectionNode(ctx, nodeID, notCacheable)
	ctx.results = ctx.projects
	return nodeID, err
}

// Rebind already-bound expressions, including every window input. No function is
// rebound against branch-local input after grouping sets have been combined.
func rebindGroupingExpr(expr *plan.Expr, branch, output *BindContext, union *plan.Node) {
	if col := expr.GetCol(); col != nil {
		if tag := branch.groupingStage.projectionInputTag; tag != 0 && col.RelPos == tag {
			col.RelPos = union.BindingTags[0]
			expr.Typ = union.ProjectList[col.ColPos].Typ
			return
		}
		switch col.RelPos {
		case branch.groupTag:
			col.RelPos = union.BindingTags[0]
			expr.Typ = union.ProjectList[col.ColPos].Typ
		case branch.aggregateTag:
			col.RelPos, col.ColPos = union.BindingTags[0], int32(len(branch.groups))+col.ColPos
			expr.Typ = union.ProjectList[col.ColPos].Typ
		case branch.windowTag:
			col.RelPos = output.windowTag
		case branch.projectTag:
			col.RelPos = output.projectTag
		}
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			rebindGroupingExpr(arg, branch, output, union)
		}
	}
	if w := expr.GetW(); w != nil {
		rebindGroupingExpr(w.WindowFunc, branch, output, union)
		for _, p := range w.PartitionBy {
			rebindGroupingExpr(p, branch, output, union)
		}
		for _, order := range w.OrderBy {
			rebindGroupingExpr(order.Expr, branch, output, union)
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			rebindGroupingExpr(item, branch, output, union)
		}
	}
	if sub := expr.GetSub(); sub != nil && sub.Child != nil {
		// IN/ANY/ALL keep their outer operand here, not in function Args.
		rebindGroupingExpr(sub.Child, branch, output, union)
	}
}

// Hidden sort expressions must be evaluated after DISTINCT using only surviving
// output columns. This also keeps volatile sort expressions out of the keys.
func groupingDistinctOrder(expr *plan.Expr, selected []*plan.Expr, tag int32) (*plan.Expr, bool) {
	for i, project := range selected {
		if exprStructuralEqual(expr, project) {
			return &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: int32(i)}}}, true
		}
	}
	if fn := expr.GetF(); fn != nil {
		for i, arg := range fn.Args {
			var ok bool
			fn.Args[i], ok = groupingDistinctOrder(arg, selected, tag)
			if !ok {
				return nil, false
			}
		}
		return expr, true
	}
	return expr, expr.GetCol() == nil
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
		// This projection is an expression consumer just like the ordinary
		// final projection: flatten subqueries before it becomes DISTINCT keys.
		for i, project := range ctx.projects {
			nodeID, ctx.projects[i], err = builder.flattenSubqueries(nodeID, project, ctx)
			if err != nil {
				return 0, err
			}
		}
		selected := ctx.projects[:stage.resultLen]
		distinctTag := builder.genNewBindTag()
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
			} else {
				var ok bool
				projects[i], ok = groupingDistinctOrder(DeepCopyExpr(ctx.projects[i]), selected, distinctTag)
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
