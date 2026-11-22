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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// bindGroupedBitCount binds the SELECT-side selector on a group row. The
// group key still uses the selector's ordinary common value. BIT_COUNT sees
// the selected arm with the original conversion and lazy error behavior.
func (b *baseBinder) bindGroupedBitCount(operand *Expr) (*Expr, error) {
	source := bitCountGroupSource(b.ctx, operand)
	if source == operand || b.builder == nil || b.ctx.groupingFuncAllowed || b.ctx.sampleFunc.hasSampleFunc ||
		!(b.builder.mysqlCompatible || b.builder.mysqlFullGroupByCompat) ||
		releasedBitCountFloatOverload(types.T(operand.Typ.Id)) < 0 ||
		bitCountSelector(source) == nil || !canReplaySelectorExpr(b.GetContext(), source) {
		return nil, nil
	}
	groupPos := operand.GetCol().ColPos
	if bound := b.ctx.groupedBitCounts[groupPos]; bound != nil {
		return DeepCopyExpr(bound), nil
	}

	ctx := b.GetContext()
	replacements := make(map[[2]int32]*Expr)
	for pos, expr := range b.ctx.groups {
		if col := expr.GetCol(); col != nil {
			key := [2]int32{col.RelPos, col.ColPos}
			if _, exists := replacements[key]; !exists {
				replacements[key] = GetColExpr(b.ctx.groupOutputType(int32(pos)), b.ctx.groupTag, int32(pos))
			}
		}
	}
	seen := make(map[[2]int32]bool)
	var inputs []*Expr
	var collect func(*Expr)
	collect = func(expr *Expr) {
		if col := expr.GetCol(); col != nil {
			key := [2]int32{col.RelPos, col.ColPos}
			if replacements[key] == nil && !seen[key] {
				seen[key] = true
				// The GROUP BY and aggregate inputs are remapped independently.
				inputs = append(inputs, DeepCopyExpr(expr))
			}
			return
		}
		for _, arg := range expr.GetF().GetArgs() {
			collect(arg)
		}
		for _, item := range expr.GetList().GetList() {
			collect(item)
		}
	}
	collect(source)
	if !b.builder.mysqlCompatible {
		for _, input := range inputs {
			if !b.builder.mysqlFullGroupByAllowsColRef(b.ctx, input) {
				col := input.GetCol()
				name := col.Name
				if binding := b.ctx.bindingByTag[col.RelPos]; name == "" && binding != nil &&
					col.ColPos >= 0 && int(col.ColPos) < len(binding.cols) {
					name = binding.cols[col.ColPos]
				}
				return nil, moerr.NewSyntaxErrorf(ctx, "column %q must appear in the GROUP BY clause or be used in an aggregate function", name)
			}
		}
	}

	lowered, err := b.bindBitCountOperand(DeepCopyExpr(source))
	if err != nil {
		return nil, err
	}
	var representatives []*Expr
	if b.builder.mysqlCompatible && (len(inputs) > 1 || len(inputs) == 1 && !inputs[0].Typ.NotNullable) {
		// ANY_VALUE skips NULLs. A non-NULL row tuple keeps the nullable
		// source values together in permissive mode.
		tuple, err := BindFuncExprImplByPlanExpr(ctx, "serial_full", inputs)
		if err != nil {
			return nil, err
		}
		packed, err := BindFuncExprImplByPlanExpr(ctx, "any_value", []*Expr{tuple})
		if err != nil {
			return nil, err
		}
		slot := GetColExpr(packed.Typ, b.ctx.aggregateTag, int32(len(b.ctx.aggregates)))
		for i, input := range inputs {
			col := input.GetCol()
			value, err := MakeSerialExtractExpr(ctx, DeepCopyExpr(slot), input.Typ, int64(i))
			if err != nil {
				return nil, err
			}
			replacements[[2]int32{col.RelPos, col.ColPos}] = value
		}
		representatives = append(representatives, packed)
	} else {
		// Strict-mode functional dependencies make each such column stable
		// within the group. A nonnullable single permissive input needs no tuple.
		for _, input := range inputs {
			value, err := BindFuncExprImplByPlanExpr(ctx, "any_value", []*Expr{input})
			if err != nil {
				return nil, err
			}
			col := input.GetCol()
			replacements[[2]int32{col.RelPos, col.ColPos}] = GetColExpr(
				value.Typ, b.ctx.aggregateTag, int32(len(b.ctx.aggregates)+len(representatives)))
			representatives = append(representatives, value)
		}
	}
	b.ctx.aggregates = append(b.ctx.aggregates, representatives...)
	lowered = replaceGroupedBitCountColumns(lowered, replacements)
	if b.ctx.groupedBitCounts == nil {
		b.ctx.groupedBitCounts = make(map[int32]*Expr)
	}
	b.ctx.groupedBitCounts[groupPos] = DeepCopyExpr(lowered)
	return lowered, nil
}

// Only replay scalar operations whose evaluation cannot observe or change
// session state, or repeat a conversion warning.
func canReplaySelectorExpr(ctx context.Context, expr *Expr) bool {
	if expr.GetCol() != nil {
		return bitCountGroupPrimitive(types.T(expr.Typ.Id))
	}
	if lit := expr.GetLit(); lit != nil {
		return lit.Src == nil
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	switch id {
	case function.IFF, function.CASE, function.UNARY_PLUS,
		function.ISNULL, function.ISNOTNULL, function.AND, function.OR, function.NOT:
	case function.EQUAL, function.NOT_EQUAL, function.GREAT_THAN,
		function.GREAT_EQUAL, function.LESS_THAN, function.LESS_EQUAL:
		// The binder has resolved the numeric comparison overload. Its
		// primitive operands can differ in width without string coercion.
		for _, arg := range fn.Args {
			if !bitCountGroupPrimitive(types.T(arg.Typ.Id)) {
				return false
			}
		}
	case function.MOD:
		if len(fn.Args) != 2 || !types.T(fn.Args[0].Typ.Id).IsInteger() ||
			!types.T(fn.Args[1].Typ.Id).IsInteger() {
			return false
		}
		divisor := fn.Args[1].GetLit()
		if divisor == nil || divisor.Isnull || divisor.Src != nil {
			return false
		}
		signed, _ := literalSignedValue(divisor)
		unsigned, _ := literalUnsignedValue(divisor)
		if signed == 0 && unsigned == 0 {
			return false
		}
	case function.CAST:
		return len(fn.Args) == 2 && canReplaySelectorExpr(ctx, fn.Args[0]) &&
			canReplaySelectorCast(ctx, fn.Args[0], expr.Typ)
	default:
		return false
	}
	for _, arg := range fn.Args {
		if !canReplaySelectorExpr(ctx, arg) {
			return false
		}
	}
	return true
}

func canReplaySelectorCast(ctx context.Context, operand *Expr, target Type) bool {
	from, to := types.T(operand.Typ.Id), types.T(target.Id)
	if releasedBitCountFloatOverload(to) >= 0 {
		// Fixed precision targets and a fixed FLOAT widened to DOUBLE
		// can invoke the checked, rounding cast path.
		if target.Width > 0 && target.Scale >= 0 ||
			from == types.T_float32 && to != from && operand.Typ.Width > 0 && operand.Typ.Scale > 0 {
			return false
		}
		if from == to {
			return true
		}
	}
	if singleRowCastIsTotal(operand.Typ, target) {
		return true
	}
	// Comparisons narrow an integer literal when it fits the column type.
	return from.IsInteger() && to.IsInteger() && operand.GetLit() != nil &&
		checkNoNeedCast(ctx, makeTypeByPlan2Expr(operand), makeTypeByPlan2Type(target), operand)
}

func bitCountGroupPrimitive(oid types.T) bool {
	// typeswitch:partial Group representatives carry MySQL FLOAT and DOUBLE only.
	return oid.IsInteger() || oid == types.T_bool || oid == types.T_float32 || oid == types.T_float64
}

// Substitute the group-owned values after binding the count overloads.
// Reusing the general projection inliner would mark a representative as a
// derived expression and change its prepared numeric source metadata.
func replaceGroupedBitCountColumns(expr *Expr, replacements map[[2]int32]*Expr) *Expr {
	if col := expr.GetCol(); col != nil {
		if value := replacements[[2]int32{col.RelPos, col.ColPos}]; value != nil {
			return DeepCopyExpr(value)
		}
	}
	if fn := expr.GetF(); fn != nil {
		for i, arg := range fn.Args {
			fn.Args[i] = replaceGroupedBitCountColumns(arg, replacements)
		}
	}
	if list := expr.GetList(); list != nil {
		for i, item := range list.List {
			list.List[i] = replaceGroupedBitCountColumns(item, replacements)
		}
	}
	return expr
}
