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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func (builder *QueryBuilder) wrapBareColRefsInAnyValue(expr *plan.Expr, ctx *BindContext) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		// Window results are produced above AGG. They must remain window-tag
		// references in the final projection rather than becoming any_value()
		// inputs to the aggregate stage.
		// Time-window boundary columns are produced by the TIME_WINDOW node
		// above AGG too, so keep them as time-tag references for the same
		// reason. Wrapping them before bindTimeWindow finalizes the public
		// boundary type can leave duplicated _wstart/_wend outputs with stale
		// DATETIME metadata.
		if exprImpl.Col.RelPos == ctx.groupTag || exprImpl.Col.RelPos == ctx.aggregateTag ||
			(ctx.windowTag > 0 && exprImpl.Col.RelPos == ctx.windowTag) ||
			(ctx.timeTag > 0 && exprImpl.Col.RelPos == ctx.timeTag) {
			return expr
		}
		newExpr, _ := BindFuncExprImplByPlanExpr(builder.compCtx.GetContext(), "any_value", []*plan.Expr{expr})
		colPos := len(ctx.aggregates)
		ctx.aggregates = append(ctx.aggregates, newExpr)
		return &plan.Expr{
			Typ: ctx.aggregates[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: ctx.aggregateTag,
					ColPos: int32(colPos),
				},
			},
		}

	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = builder.wrapBareColRefsInAnyValue(arg, ctx)
		}
		return expr

	case *plan.Expr_W:
		exprImpl.W.WindowFunc = builder.wrapBareColRefsInAnyValue(exprImpl.W.WindowFunc, ctx)
		for i, partitionBy := range exprImpl.W.PartitionBy {
			exprImpl.W.PartitionBy[i] = builder.wrapBareColRefsInAnyValue(partitionBy, ctx)
		}
		for _, orderBy := range exprImpl.W.OrderBy {
			orderBy.Expr = builder.wrapBareColRefsInAnyValue(orderBy.Expr, ctx)
		}
		return expr

	default:
		return expr
	}
}

func canShareGroupedBitCountRow(ctx *BindContext) bool {
	if ctx.groupingFuncAllowed || ctx.sampleFunc.hasSampleFunc {
		return false
	}
	for _, group := range ctx.groups {
		if releasedBitCountFloatOverload(types.T(group.Typ.Id)) >= 0 && bitCountSelector(group) != nil {
			return true
		}
	}
	return false
}

// finalizeGroupedBitCountRow gives all eligible implicit group consumers one
// non-NULL tuple per group. ANY_VALUE can pick that tuple even when one or
// more of its fields are NULL. Explicit aggregate inputs retain their owners.
func (builder *QueryBuilder) finalizeGroupedBitCountRow(
	ctx *BindContext, having []*Expr, orderBy []*plan.OrderBySpec,
	fillValues, fillColumns []*Expr, timeOrderBy *plan.OrderBySpec,
) error {
	if !ctx.needsGroupedBitCountRow {
		return nil
	}
	walk := func(visit func(*Expr)) {
		for _, list := range [][]*Expr{ctx.projects, ctx.windows, ctx.times, having, fillValues, fillColumns} {
			for _, expr := range list {
				visitGroupedSourceColumns(expr, visit)
			}
		}
		for _, spec := range orderBy {
			visitGroupedSourceColumns(spec.Expr, visit)
		}
		if timeOrderBy != nil {
			visitGroupedSourceColumns(timeOrderBy.Expr, visit)
		}
	}
	sourceKey := func(expr *Expr) ([2]int32, bool) {
		col := expr.GetCol()
		if col.RelPos == ctx.aggregateTag {
			expr = ctx.implicitGroupHaving[col.ColPos]
			col = expr.GetCol()
		}
		if col == nil || ctx.bindingByTag[col.RelPos] == nil || !function.SerialTypeSupported(types.T(expr.Typ.Id)) {
			return [2]int32{}, false
		}
		return [2]int32{col.RelPos, col.ColPos}, true
	}
	var inputs []*Expr
	positions := make(map[[2]int32]int)
	walk(func(expr *Expr) {
		key, eligible := sourceKey(expr)
		if !eligible {
			return
		}
		if _, ok := positions[key]; !ok {
			if col := expr.GetCol(); col.RelPos == ctx.aggregateTag {
				expr = ctx.implicitGroupHaving[col.ColPos]
			}
			positions[key] = len(inputs)
			inputs = append(inputs, DeepCopyExpr(expr))
		}
	})
	if len(inputs) == 0 {
		return nil
	}
	bindCtx := builder.GetContext()
	tuple, err := BindFuncExprImplByPlanExpr(bindCtx, "serial_full", inputs)
	if err != nil {
		return err
	}
	representative, err := BindFuncExprImplByPlanExpr(bindCtx, "any_value", []*Expr{tuple})
	if err != nil {
		return err
	}
	slot := GetColExpr(representative.Typ, ctx.aggregateTag, int32(len(ctx.aggregates)))
	values := make([]*Expr, len(inputs))
	for pos, input := range inputs {
		value, err := MakeSerialExtractExpr(bindCtx, DeepCopyExpr(slot), input.Typ, int64(pos))
		if err != nil {
			return err
		}
		// The extracted field carries the source domain, including collation.
		value.Typ = input.Typ
		values[pos] = value
	}
	ctx.aggregates = append(ctx.aggregates, representative)
	walk(func(expr *Expr) {
		key, eligible := sourceKey(expr)
		if pos, ok := positions[key]; eligible && ok {
			typ := expr.Typ
			*expr = *DeepCopyExpr(values[pos])
			expr.Typ = typ
		}
	})
	return nil
}

// Visit executable operands in the caller. A literal's Src and a subquery's
// plan are separate owners.
func visitGroupedSourceColumns(expr *Expr, visit func(*Expr)) {
	switch {
	case expr.GetCol() != nil:
		visit(expr)
	case expr.GetF() != nil:
		for _, arg := range expr.GetF().Args {
			visitGroupedSourceColumns(arg, visit)
		}
	case expr.GetList() != nil:
		for _, arg := range expr.GetList().List {
			visitGroupedSourceColumns(arg, visit)
		}
	case expr.GetSub() != nil:
		visitGroupedSourceColumns(expr.GetSub().Child, visit)
	case expr.GetW() != nil:
		window := expr.GetW()
		visitGroupedSourceColumns(window.WindowFunc, visit)
		for _, arg := range window.PartitionBy {
			visitGroupedSourceColumns(arg, visit)
		}
		for _, spec := range window.OrderBy {
			visitGroupedSourceColumns(spec.Expr, visit)
		}
	}
}
