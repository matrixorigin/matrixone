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

type groupedSourceInputKey struct {
	relation, column int32
	rowIDNullTest    bool
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
	walk := func(visit func(*Expr) bool) {
		for _, list := range [][]*Expr{ctx.projects, ctx.windows, ctx.times, having, fillValues, fillColumns} {
			for _, expr := range list {
				visitGroupedSourceInputs(expr, visit)
			}
		}
		for _, spec := range orderBy {
			visitGroupedSourceInputs(spec.Expr, visit)
		}
		if timeOrderBy != nil {
			visitGroupedSourceInputs(timeOrderBy.Expr, visit)
		}
	}
	inputForReference := func(expr *Expr) (key groupedSourceInputKey, input, display *Expr) {
		col := expr.GetCol()
		if col == nil {
			if rowID := groupedRowIDNullTest(expr); rowID != nil && ctx.bindingByTag[rowID.RelPos] != nil {
				return groupedSourceInputKey{relation: rowID.RelPos, column: rowID.ColPos, rowIDNullTest: true}, expr, nil
			}
			return
		}
		if col.RelPos == ctx.aggregateTag {
			expr = ctx.implicitGroupHaving[col.ColPos]
			if expr == nil {
				return
			}
			if source, ok := transparentOutputSourceExpr(expr); ok && expr.GetF().Args[0].GetLit() != nil {
				display, expr = expr, source
			}
			if expr.GetCol() == nil {
				// Compute a bound read that is more than a display adapter at
				// its original input stage, before choosing the group row.
				return groupedSourceInputKey{relation: col.RelPos, column: col.ColPos}, expr, nil
			}
			col = expr.GetCol()
		}
		if ctx.bindingByTag[col.RelPos] == nil || !function.SerialTypeSupported(types.T(expr.Typ.Id)) {
			return groupedSourceInputKey{}, nil, nil
		}
		return groupedSourceInputKey{relation: col.RelPos, column: col.ColPos}, expr, display
	}
	var inputs []*Expr
	positions := make(map[groupedSourceInputKey]int)
	walk(func(expr *Expr) bool {
		key, input, _ := inputForReference(expr)
		if input == nil {
			return false
		}
		if _, ok := positions[key]; !ok {
			positions[key] = len(inputs)
			inputs = append(inputs, DeepCopyExpr(input))
		}
		return true
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
	walk(func(expr *Expr) bool {
		key, input, display := inputForReference(expr)
		if pos, ok := positions[key]; input != nil && ok {
			value := DeepCopyExpr(values[pos])
			if display != nil {
				value.Typ = input.Typ
				wrapped := DeepCopyExpr(display)
				wrapped.GetF().Args[1] = value
				value = wrapped
			}
			value.Typ = expr.Typ
			*expr = *value
			return true
		}
		return false
	})
	return nil
}

// A virtual column on an outer join uses the row ID's null bit to tell a
// missing row from a present row with NULL data. Pack that boolean with the
// other representative fields. Computing the null test for each input is
// safe even when the surrounding virtual expression must stay lazy.
func groupedRowIDNullTest(expr *Expr) *plan.ColRef {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 1 || fn.Args[0] == nil ||
		types.T(fn.Args[0].Typ.Id) != types.T_Rowid || types.T(expr.Typ.Id) != types.T_bool {
		return nil
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if id != function.ISNULL {
		return nil
	}
	return fn.Args[0].GetCol()
}

// Visit executable operands in the caller. A literal's Src and a subquery's
// plan are separate owners. A captured input owns its entire expression.
func visitGroupedSourceInputs(expr *Expr, visit func(*Expr) bool) {
	if expr == nil || visit(expr) {
		return
	}
	switch {
	case expr.GetF() != nil:
		for _, arg := range expr.GetF().Args {
			visitGroupedSourceInputs(arg, visit)
		}
	case expr.GetList() != nil:
		for _, arg := range expr.GetList().List {
			visitGroupedSourceInputs(arg, visit)
		}
	case expr.GetSub() != nil:
		visitGroupedSourceInputs(expr.GetSub().Child, visit)
	case expr.GetW() != nil:
		window := expr.GetW()
		visitGroupedSourceInputs(window.WindowFunc, visit)
		for _, arg := range window.PartitionBy {
			visitGroupedSourceInputs(arg, visit)
		}
		for _, spec := range window.OrderBy {
			visitGroupedSourceInputs(spec.Expr, visit)
		}
	}
}
