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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// Only table dump origin validation uses the released expression binding.
type releasedBitCountDumpBindingKey struct{}

func releasedBitCountFloatOverload(oid types.T) int32 {
	switch oid { // typeswitch:partial only native MySQL FLOAT and DOUBLE fields select the rounding overload.
	case types.T_float32:
		return function.BitCountRoundedFloat32Overload
	case types.T_float64:
		return function.BitCountRoundedFloat64Overload
	default:
		return -1
	}
}

func bindBitCountFloatOverload(ctx context.Context, args []*Expr, overload int32) (*Expr, error) {
	t := makeTypeByPlan2Expr(args[0])
	f, err := function.GetFunctionByNameWithOverload(ctx, "bit_count", []types.Type{t}, overload)
	if err != nil {
		return nil, err
	}
	id := f.GetEncodedOverloadID()
	ret := f.GetReturnType()
	typ := makePlan2Type(&ret)
	typ.NotNullable = function.DeduceNotNullableForBinding(ctx, id, args)
	return &Expr{Typ: typ, Expr: &plan.Expr_F{F: &plan.Function{
		Func: getFunctionObjRef(id, "bit_count"),
		Args: args,
	}}}, nil
}

// A scalar subquery in a SELECT list only prevents derived-table merging when
// evaluating that projection depends on the input row or an unstable value.
// The direct tableless scalar exception mirrors MySQL's single-row reduction:
// its projection replaces the scalar node, so an otherwise standalone outer
// reference or RAND() is not a dependent subquery.
func bitCountProjectionHasDependentSubquery(builder *QueryBuilder, expr *Expr) bool {
	if expr == nil || !hasSubquery(expr) || builder == nil || builder.qry == nil {
		return false
	}
	return bitCountProjectionDependsOnRows(builder, expr, true, make(map[int32]bool))
}

func bitCountProjectionRetainsSubqueryAfterReduction(builder *QueryBuilder, expr *Expr, direct bool) bool {
	if expr == nil {
		return false
	}
	if sub := expr.GetSub(); sub != nil {
		scope := bitCountSubqueryScope(builder, sub)
		if direct && sub.Typ == plan.SubqueryRef_SCALAR && sub.Child == nil && sub.RowSize == 1 &&
			scope != nil && bitCountCanReduceScalar(builder, scope) {
			return scope.singleProjectionRetainsSubquery
		}
		if direct && bitCountInSubqueryCanReduce(builder, scope, sub) {
			return bitCountProjectionRetainsSubqueryAfterReduction(builder, sub.Child, true)
		}
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if bitCountProjectionRetainsSubqueryAfterReduction(builder, arg, false) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if bitCountProjectionRetainsSubqueryAfterReduction(builder, item, false) {
				return true
			}
		}
	}
	return false
}

func bitCountProjectionSource(ctx *BindContext, expr *Expr) *Expr {
	if ctx == nil || expr == nil {
		return expr
	}
	col := expr.GetCol()
	if col == nil || col.RelPos != ctx.groupTag || col.ColPos < 0 || int(col.ColPos) >= len(ctx.groups) {
		return expr
	}
	return ctx.groups[col.ColPos]
}

func bitCountSubqueryScope(builder *QueryBuilder, sub *plan.SubqueryRef) *BindContext {
	if builder == nil || builder.qry == nil || sub == nil || sub.NodeId < 0 || int(sub.NodeId) >= len(builder.ctxByNode) {
		return nil
	}
	return builder.ctxByNode[sub.NodeId]
}

func bitCountInSubqueryCanReduce(builder *QueryBuilder, scope *BindContext, sub *plan.SubqueryRef) bool {
	if scope == nil || !scope.canReduceInToSubqueryFreeComparison || sub == nil || sub.RowSize != 1 || sub.Child == nil {
		return false
	}
	return (sub.Typ == plan.SubqueryRef_IN || sub.Typ == plan.SubqueryRef_ANY && sub.Op == "=") &&
		bitCountWhereIsEffectivelyTrue(builder, scope)
}

func bitCountCanReduceScalar(builder *QueryBuilder, scope *BindContext) bool {
	return scope.reducibleTablelessScalar && bitCountWhereIsEffectivelyTrue(builder, scope)
}

func (ctx *BindContext) setWhereFilters(filters []*plan.Expr) {
	ctx.whereFilters = filters
	ctx.bitCountWhereProofKnown = false
	ctx.bitCountWhereProofValue = false
	ctx.bitCountWhereProofNeedsContext = false
}

func bitCountWhereIsEffectivelyTrue(builder *QueryBuilder, ctx *BindContext) bool {
	if ctx == nil {
		return false
	}
	if ctx.bitCountWhereProofKnown {
		if ctx.bitCountWhereProofValue && ctx.bitCountWhereProofNeedsContext {
			if builder == nil || builder.compCtx == nil {
				return false
			}
			bindCtx := builder.GetContext()
			if bindCtx == nil || defines.IsNoEvaluationViewBinding(bindCtx) || bindCtx.Err() != nil {
				return false
			}
		}
		return ctx.bitCountWhereProofValue
	}
	ctx.bitCountWhereProofKnown = true
	ctx.bitCountWhereProofValue = false
	ctx.bitCountWhereProofNeedsContext = false
	var budget *speculativeConstantFoldBudget
	for _, filter := range ctx.whereFilters {
		folded := filter
		if filter.GetLit() == nil {
			ctx.bitCountWhereProofNeedsContext = true
			if builder == nil || builder.compCtx == nil {
				return false
			}
			bindCtx := builder.GetContext()
			if bindCtx == nil || defines.IsNoEvaluationViewBinding(bindCtx) || bindCtx.Err() != nil {
				return false
			}
			if budget == nil {
				budget = &speculativeConstantFoldBudget{ctx: bindCtx}
			}
			foldExpr := DeepCopyExpr(filter)
			foldLargeSpaceLengthExpressions(foldExpr, bindCtx)
			if !isSpeculativeConstantFoldSafe(foldExpr, budget) {
				return false
			}
			proc := builder.compCtx.GetProcess()
			if proc == nil {
				return false
			}
			// The compiler may lend a process from an earlier statement.
			// Borrow its resources, using the current bind request's deadline.
			eval := proc.NewNoContextChildProc(0)
			eval.Ctx = bindCtx
			var err error
			folded, err = ConstantFold(batch.EmptyForConstFoldBatch, foldExpr, eval, false, false)
			if err != nil || folded == nil || bindCtx.Err() != nil {
				return false
			}
		}
		if lit := folded.GetLit(); folded.Typ.Id != int32(types.T_bool) || lit == nil || lit.Isnull || !lit.GetBval() {
			return false
		}
	}
	ctx.bitCountWhereProofValue = true
	return true
}

func foldLargeSpaceLengthExpressions(expr *plan.Expr, ctx context.Context) bool {
	if expr == nil || ctx != nil && ctx.Err() != nil {
		return false
	}
	f := expr.GetF()
	if f == nil || f.Func == nil {
		return false
	}
	name := strings.ToLower(f.Func.GetObjName())
	if (name == "length" || name == "char_length" || name == "octet_length" || name == "bit_length") &&
		len(f.Args) == 1 && function.GetFunctionIsProvenPureFoldableById(f.Func.GetObj()) {
		space := f.Args[0].GetF()
		if space != nil && space.Func != nil && strings.EqualFold(space.Func.GetObjName(), "space") &&
			function.GetFunctionIsProvenPureFoldableById(space.Func.GetObj()) && len(space.Args) == 1 {
			length, ok := speculativeConstantInt64(space.Args[0])
			if ok && length >= 0 && length <= int64(types.MaxBlobLen) {
				if name == "bit_length" {
					length *= 8
				}
				expr.Expr = &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: length}}}
				return true
			}
		}
	}
	changed := false
	for _, arg := range f.Args {
		changed = foldLargeSpaceLengthExpressions(arg, ctx) || changed
	}
	return changed
}

func bitCountProjectionDependsOnRows(builder *QueryBuilder, expr *Expr, direct bool, seen map[int32]bool) bool {
	if expr == nil {
		return false
	}
	if sub := expr.GetSub(); sub != nil {
		scope := bitCountSubqueryScope(builder, sub)
		if direct && bitCountInSubqueryCanReduce(builder, scope, sub) &&
			!bitCountProjectionRetainsSubqueryAfterReduction(builder, sub.Child, true) {
			return false
		}
		if direct && sub.Typ == plan.SubqueryRef_SCALAR && sub.Child == nil && sub.RowSize == 1 &&
			scope != nil && bitCountCanReduceScalar(builder, scope) {
			return scope.hasDependentProjectionSubquery
		}
		if sub.Child != nil && bitCountProjectionDependsOnRows(builder, sub.Child, false, seen) {
			return true
		}
		if scope != nil && scope.isCorrelated {
			return true
		}
		if sub.NodeId >= 0 && int(sub.NodeId) < len(builder.qry.Nodes) &&
			builder.classifyCTEEvaluation(sub.NodeId, seen).valueUnstable {
			return true
		}
		return false
	}
	if expr.GetCol() != nil || expr.GetCorr() != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil {
			unstable, known := function.GetFunctionValueUnstableByID(fn.Func.Obj)
			if !known || unstable {
				return true
			}
		}
		for _, arg := range fn.Args {
			if bitCountProjectionDependsOnRows(builder, arg, false, seen) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if bitCountProjectionDependsOnRows(builder, item, false, seen) {
				return true
			}
		}
	}
	return false
}

// Bind the count inside each selected arm so the control-flow executor keeps
// its row masks and the operand reaches BIT_COUNT with its own conversion.
func (b *baseBinder) bindBitCountOperand(expr *Expr) (*Expr, error) {
	ctx := b.GetContext()
	if releasedBitCountFloatOverload(types.T(expr.Typ.Id)) >= 0 {
		if fn := expr.GetF(); fn != nil && fn.Func != nil {
			id, _ := function.DecodeOverloadID(fn.Func.GetObj())
			switch id {
			case function.UNARY_PLUS:
				return b.bindBitCountOperand(fn.Args[0])
			case function.CASE, function.IFF:
				if !expr.GetPreparedNumeric().GetIfnullCommonValue() {
					args := append([]*Expr(nil), fn.Args...)
					for _, i := range controlFlowValueIndexes(fn.Func.GetObjName(), len(args)) {
						count, err := b.bindBitCountOperand(stripIntegerSelectionReconciliation(args[i]))
						if err != nil {
							return nil, err
						}
						args[i] = count
					}
					return BindFuncExprImplByPlanExpr(ctx, fn.Func.GetObjName(), args)
				}
			}
		}
		if overload := b.bitCountFloatOverload(expr); overload >= 0 {
			return bindBitCountFloatOverload(ctx, []*Expr{expr}, overload)
		}
	}
	return BindFuncExprImplByPlanExpr(ctx, "bit_count", []*Expr{expr})
}

// A mergeable projection forwards its operand's conversion. A materialized
// query block exposes a field even when the original expression was a CAST.
func (b *baseBinder) bitCountFloatOverload(expr *Expr) int32 {
	field, expression := function.BitCountFloat64FieldOverload, function.BitCountFloat64ExpressionOverload
	if expr.Typ.Id == int32(types.T_float32) {
		field, expression = function.BitCountFloat32FieldOverload, function.BitCountFloat32ExpressionOverload
	}
	// A same-block group slot represents the original GROUP BY expression.
	// Inspect its conversion without replacing the slot used for evaluation.
	if col := expr.GetCol(); col != nil && b.ctx != nil && b.ctx.groupTag > 0 &&
		col.RelPos == b.ctx.groupTag && col.ColPos >= 0 && int(col.ColPos) < len(b.ctx.groups) {
		expr = b.ctx.groups[col.ColPos]
	}
	// Projections are bound before consumers. The decreasing node ID keeps
	// recursive or self-referencing projections from reopening the lookup.
	var nodes []*plan.Node
	if b.builder != nil && b.builder.qry != nil {
		nodes = b.builder.qry.Nodes
	}
	limit := len(nodes)
	for {
		if fn := expr.GetF(); fn != nil && fn.Func != nil {
			id, _ := function.DecodeOverloadID(fn.Func.GetObj())
			switch id {
			case function.CASE:
				// IFNULL converts its common result, even after lowering to CASE.
				if expr.GetPreparedNumeric().GetIfnullCommonValue() {
					return int32(expression)
				}
				return -1
			case function.CAST, function.IFF:
				// Conditionals can select operands with different conversion rules.
				return -1
			case function.UNARY_PLUS:
				expr = fn.Args[0]
				continue
			default:
				return int32(expression)
			}
		}
		if nodes == nil {
			if expr.GetCol() != nil || expr.GetCorr() != nil {
				return int32(field)
			}
			return -1
		}
		if sub := expr.GetSub(); sub != nil && sub.Typ == plan.SubqueryRef_SCALAR {
			var scope *BindContext
			if sub.NodeId >= 0 && int(sub.NodeId) < len(b.builder.ctxByNode) {
				scope = b.builder.ctxByNode[sub.NodeId]
			}
			if scope == nil || !bitCountCanReduceScalar(b.builder, scope) || sub.Child != nil || sub.RowSize != 1 ||
				len(scope.projects) != 1 || scope.hasDependentProjectionSubquery {
				return int32(field)
			}
			expr = scope.projects[0]
			continue
		}
		var tag, pos int32
		if col := expr.GetCol(); col != nil {
			tag, pos = col.RelPos, col.ColPos
		} else if col := expr.GetCorr(); col != nil {
			tag, pos = col.RelPos, col.ColPos
		} else {
			return -1
		}
		id, ok := b.builder.tag2NodeID[tag]
		if !ok || id < 0 || int(id) >= limit {
			// Aggregate and window outputs can be bound before their node.
			return int32(field)
		}
		limit = int(id)
		node := nodes[id]
		if node.NodeType != plan.Node_PROJECT || pos < 0 || int(pos) >= len(node.ProjectList) {
			return int32(field)
		}
		scope := b.builder.ctxByNode[id]
		if scope != nil && (scope.isDistinct || scope.hasHaving || len(scope.aggregates) != 0 ||
			len(scope.groups) != 0 || len(scope.windows) != 0 || scope.hasDependentProjectionSubquery) {
			return int32(field)
		}
		for current := node; ; {
			if current.Limit != nil || current.Offset != nil || current.NodeType == plan.Node_VALUE_SCAN ||
				current.NodeType == plan.Node_MATERIAL || isPreparedSetOperationNode(current.NodeType) {
				return int32(field)
			}
			if len(current.Children) != 1 || b.builder.ctxByNode[current.Children[0]] != scope {
				break
			}
			current = nodes[current.Children[0]]
		}
		expr = node.ProjectList[pos]
	}
}
