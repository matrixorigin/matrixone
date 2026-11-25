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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
		bitCountSelector(source) == nil || !canReplayGroupedSelectorExpr(b.GetContext(), source) {
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
			if col.RelPos == b.ctx.aggregateTag {
				return
			}
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
				// Replaying is optional: the already-bound group slot is legal
				// even when its underlying columns are not group-determined.
				return nil, nil
			}
		}
	}

	working := DeepCopyExpr(source)
	if !b.builder.mysqlCompatible {
		var captureTextPredicates func(*Expr) error
		captureTextPredicates = func(expr *Expr) error {
			if isReplayableTextPredicate(ctx, expr) {
				// A grouped input must keep its group-key value. Only capture
				// predicates whose column inputs all come from the source row.
				found, allInputs := false, true
				_ = plan.VisitExprTree(expr, func(node *Expr) error {
					if col := node.GetCol(); col != nil {
						found = true
						allInputs = allInputs && replacements[[2]int32{col.RelPos, col.ColPos}] == nil
					}
					return nil
				})
				if !found || !allInputs {
					return nil
				}
				value, err := BindFuncExprImplByPlanExpr(ctx, "any_value", []*Expr{DeepCopyExpr(expr)})
				if err != nil {
					return err
				}
				pos := int32(len(b.ctx.aggregates))
				b.ctx.aggregates = append(b.ctx.aggregates, value)
				*expr = *GetColExpr(value.Typ, b.ctx.aggregateTag, pos)
				return nil
			}
			for _, arg := range expr.GetF().GetArgs() {
				if err := captureTextPredicates(arg); err != nil {
					return err
				}
			}
			for _, item := range expr.GetList().GetList() {
				if err := captureTextPredicates(item); err != nil {
					return err
				}
			}
			return nil
		}
		if err := captureTextPredicates(working); err != nil {
			return nil, err
		}
		// Only ungrouped columns outside captured predicates need an input slot.
		inputs = nil
		clear(seen)
		collect(working)
	}
	lowered, err := b.bindBitCountOperand(working)
	if err != nil {
		return nil, err
	}
	if b.builder.mysqlCompatible {
		// Defer row selection until HAVING, SELECT, and ORDER BY can all
		// share the same source row.
		b.ctx.needsGroupedBitCountRow = b.ctx.needsGroupedBitCountRow || len(inputs) > 0
	} else {
		for _, input := range inputs {
			value, err := BindFuncExprImplByPlanExpr(ctx, "any_value", []*Expr{input})
			if err != nil {
				return nil, err
			}
			col := input.GetCol()
			replacements[[2]int32{col.RelPos, col.ColPos}] = GetColExpr(
				value.Typ, b.ctx.aggregateTag, int32(len(b.ctx.aggregates)))
			b.ctx.aggregates = append(b.ctx.aggregates, value)
		}
	}
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
	return canReplaySelectorExprWithText(ctx, expr, false)
}

func canReplayGroupedSelectorExpr(ctx context.Context, expr *Expr) bool {
	return canReplaySelectorExprWithText(ctx, expr, true)
}

func canReplaySelectorExprWithText(ctx context.Context, expr *Expr, captureText bool) bool {
	if captureText && isReplayableTextPredicate(ctx, expr) {
		return true
	}
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
		return len(fn.Args) == 2 && canReplaySelectorExprWithText(ctx, fn.Args[0], captureText) &&
			canReplaySelectorCast(ctx, fn.Args[0], expr.Typ)
	default:
		return false
	}
	for _, arg := range fn.Args {
		if !canReplaySelectorExprWithText(ctx, arg, captureText) {
			return false
		}
	}
	return true
}

// Capture a total, diagnostic-free text predicate as a nullable boolean.
// Carrying the predicate rather than its text inputs bounds per-group storage.
func isReplayableTextPredicate(ctx context.Context, expr *Expr) bool {
	if expr == nil || expr.PreparedNumeric != nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if id == function.ISNULL || id == function.ISNOTNULL {
		return len(fn.Args) == 1 && isReplayableTextOperand(ctx, fn.Args[0])
	}
	if len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil ||
		!types.T(fn.Args[0].Typ.Id).IsMySQLString() || !types.T(fn.Args[1].Typ.Id).IsMySQLString() ||
		!comparisonEvaluationIsTotal(id, fn.Args[0].Typ, fn.Args[1].Typ) {
		return false
	}
	return isReplayableTextOperand(ctx, fn.Args[0]) && isReplayableTextOperand(ctx, fn.Args[1])
}

func isReplayableTextOperand(ctx context.Context, expr *Expr) bool {
	if expr == nil || expr.PreparedNumeric != nil || !types.T(expr.Typ.Id).IsMySQLString() {
		return false
	}
	if expr.GetCol() != nil {
		return true
	}
	if lit := expr.GetLit(); lit != nil {
		return lit.Src == nil
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1].GetT() == nil {
		return false
	}
	id, variant := function.DecodeOverloadID(fn.Func.Obj)
	if id == function.CAST && variant == 1 && expr.Typ.Charset != uint32(types.CharsetBinary) &&
		(types.T(expr.Typ.Id) == types.T_char || types.T(expr.Typ.Id) == types.T_varchar || types.T(expr.Typ.Id) == types.T_text) &&
		(types.StaticStringDomain(makeTypeByPlan2Expr(fn.Args[0])) == types.StringDomainBinary || fn.Args[0].GetLit().GetIsBin()) {
		// Explicit binary-to-text casts can emit an invalid UTF-8 warning.
		// They must remain inside their original, possibly masked branch.
		return false
	}
	return id == function.CAST && variant >= 0 && variant <= 2 &&
		isSameColumnType(fn.Args[1].Typ, expr.Typ) && isReplayableTextOperand(ctx, fn.Args[0]) &&
		(canReplaySelectorCast(ctx, fn.Args[0], expr.Typ) || existentialStringCastIsTotal(fn.Args[0].Typ, expr.Typ))
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
