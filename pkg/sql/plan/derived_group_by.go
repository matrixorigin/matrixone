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

import planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"

const (
	derivedGroupRewriteNodes = 4096
	derivedGroupRewriteDepth = 64
)

type derivedGroupAnchor struct {
	expr     *planpb.Expr
	position int32
}

type derivedGroupExprCost struct {
	nodes int
	depth int
}

// pruneDerivedGroupBy removes total exact functions of retained direct keys.
// All consumers are already bound: reconstruct their original trees rather
// than rebinding SQL under a possibly different numeric assignment context.
func (builder *QueryBuilder) pruneDerivedGroupBy(
	ctx *BindContext,
	exprLists [][]*planpb.Expr,
	orderBys []*planpb.OrderBySpec,
) {
	if ctx == nil || len(ctx.groups) < 2 || ctx.groupingFuncAllowed || ctx.isGroupingSet ||
		ctx.sampleFunc.hasSampleFunc || len(ctx.times) != 0 || ctx.bindingRecurQueryBlock() ||
		len(ctx.groups) > derivedGroupRewriteNodes ||
		(len(ctx.groupingFlag) != 0 && len(ctx.groupingFlag) != len(ctx.groups)) ||
		hasInactiveGroupingColumn(ctx.groupingFlag) ||
		!validGroupByPositions(ctx.groupByAst, len(ctx.groups)) ||
		!validGroupByPositions(ctx.groupByCanonicalAst, len(ctx.groups)) ||
		!validGroupByPositions(ctx.groupByParamAst, len(ctx.groups)) {
		return
	}

	remaining := derivedGroupRewriteNodes
	guard := func(expr *planpb.Expr, depth int) bool {
		if expr == nil {
			return true
		}
		remaining--
		if remaining < 0 || depth >= derivedGroupRewriteDepth || expr.PreparedNumeric != nil {
			return false
		}
		switch impl := expr.Expr.(type) {
		case *planpb.Expr_Sub, *planpb.Expr_Corr:
			// Query/provenance owners may contain references outside executable
			// children. Leave those blocks intact rather than partially remap them.
			return false
		case *planpb.Expr_Lit:
			return impl.Lit != nil && impl.Lit.Src == nil
		}
		return true
	}
	for _, inputs := range [][]*planpb.Expr{ctx.groups, ctx.aggregates} {
		// Input expressions cannot refer to post-aggregate group slots.
		if _, safe := boundExprListHasSlotRef(inputs, ctx.groupTag, nil, guard, 0); !safe {
			return
		}
	}

	anchors := make(map[[2]int32]derivedGroupAnchor)
	for i, group := range ctx.groups {
		if group == nil {
			return
		}
		if col := group.GetCol(); col != nil {
			key := [2]int32{col.RelPos, col.ColPos}
			if previous, exists := anchors[key]; exists {
				if !sameAffineResultType(previous.expr, group) {
					return
				}
				continue
			}
			anchors[key] = derivedGroupAnchor{expr: group, position: int32(i)}
		}
	}
	if len(anchors) == 0 {
		return
	}

	costs := make(map[int32]derivedGroupExprCost)
	for i, group := range ctx.groups {
		if group.GetF() == nil {
			continue
		}
		cost, safe := derivedGroupInputCost(group, anchors, &remaining, 0)
		if remaining < 0 {
			return
		}
		if !safe {
			continue
		}
		if _, exact := exactAffineIntegerRange(builder.GetContext(), group); exact {
			costs[int32(i)] = cost
		}
	}
	if len(costs) == 0 {
		return
	}

	oldToNew := make([]int32, len(ctx.groups))
	groups := make([]*planpb.Expr, 0, len(ctx.groups)-len(costs))
	flags := make([]bool, 0, len(ctx.groupingFlag))
	replacements := make(map[int32]*planpb.Expr, len(ctx.groups))
	for i, group := range ctx.groups {
		oldToNew[i] = -1
		if _, removed := costs[int32(i)]; removed {
			continue
		}
		oldToNew[i] = int32(len(groups))
		replacements[int32(i)] = GetColExpr(group.Typ, ctx.groupTag, oldToNew[i])
		groups = append(groups, group)
		if len(ctx.groupingFlag) > 0 {
			flags = append(flags, ctx.groupingFlag[i])
		}
	}
	sources := make(map[[2]int32]*planpb.Expr, len(anchors))
	for key, anchor := range anchors {
		sources[key] = replacements[anchor.position]
	}
	for old := range costs {
		replacements[old] = replaceColumnsForExpr(DeepCopyExpr(ctx.groups[old]), sources)
	}
	consumerGuard := func(expr *planpb.Expr, depth int) bool {
		if !guard(expr, depth) {
			return false
		}
		if expr == nil {
			return true
		}
		if col := expr.GetCol(); col != nil && col.RelPos == ctx.groupTag {
			if col.ColPos < 0 || int(col.ColPos) >= len(ctx.groups) ||
				!sameAffineResultType(expr, ctx.groups[col.ColPos]) {
				return false
			}
			if cost, reconstructed := costs[col.ColPos]; reconstructed {
				remaining -= cost.nodes
				return remaining >= 0 && depth+cost.depth <= derivedGroupRewriteDepth
			}
		}
		return true
	}
	rewrittenLists := make([][]*planpb.Expr, len(exprLists))
	for i, exprs := range exprLists {
		var safe bool
		rewrittenLists[i], safe = cloneAndRewriteBoundSlotRefs(exprs, ctx.groupTag, replacements, consumerGuard)
		if !safe {
			return
		}
	}
	rewrittenOrders := make([]*planpb.Expr, len(orderBys))
	for i, order := range orderBys {
		if order == nil {
			continue
		}
		var safe bool
		rewrittenOrders[i], safe = cloneAndRewriteBoundSlotRef(order.Expr, ctx.groupTag, replacements, consumerGuard)
		if !safe {
			return
		}
	}

	ctx.groups = groups
	if len(ctx.groupingFlag) > 0 {
		ctx.groupingFlag = flags
	}
	for i := range exprLists {
		copy(exprLists[i], rewrittenLists[i])
	}
	for i, expr := range rewrittenOrders {
		if orderBys[i] != nil {
			orderBys[i].Expr = expr
		}
	}
	remapElidedGroupByPositions(ctx.groupByAst, oldToNew)
	remapElidedGroupByPositions(ctx.groupByCanonicalAst, oldToNew)
	remapElidedGroupByPositions(ctx.groupByParamAst, oldToNew)
}

// Check the small input grammar before invoking the recursive range proof or
// cloning. Every leaf must be determined by an explicit, identically typed key.
func derivedGroupInputCost(
	expr *planpb.Expr,
	anchors map[[2]int32]derivedGroupAnchor,
	remaining *int,
	depth int,
) (derivedGroupExprCost, bool) {
	cost := derivedGroupExprCost{nodes: 1, depth: 1}
	*remaining--
	if expr == nil || *remaining < 0 || depth >= derivedGroupRewriteDepth || expr.PreparedNumeric != nil {
		return cost, false
	}
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_Col:
		if impl.Col == nil {
			return cost, false
		}
		anchor, found := anchors[[2]int32{impl.Col.RelPos, impl.Col.ColPos}]
		return cost, found && sameAffineResultType(expr, anchor.expr)
	case *planpb.Expr_Lit:
		return cost, impl.Lit != nil && impl.Lit.Src == nil
	case *planpb.Expr_T:
		return cost, impl.T != nil
	case *planpb.Expr_F:
		if impl.F == nil || impl.F.Func == nil {
			return cost, false
		}
		for _, arg := range impl.F.Args {
			child, safe := derivedGroupInputCost(arg, anchors, remaining, depth+1)
			if !safe {
				return cost, false
			}
			cost.nodes += child.nodes
			cost.depth = max(cost.depth, child.depth+1)
		}
		return cost, true
	default:
		return cost, false
	}
}
