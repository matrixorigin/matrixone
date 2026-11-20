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
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// Only table dump origin validation uses the released expression binding.
type releasedBitCountDumpBindingKey struct{}

func bitCountFloatFieldOverload(oid types.T) int32 {
	switch oid { // typeswitch:partial only native MySQL FLOAT and DOUBLE fields select the rounding overload.
	case types.T_float32:
		return function.BitCountRoundedFloat32Overload
	case types.T_float64:
		return function.BitCountRoundedFloat64Overload
	default:
		return -1
	}
}

func bindBitCountFloatField(ctx context.Context, args []*Expr) (*Expr, error) {
	t := makeTypeByPlan2Expr(args[0])
	f, err := function.GetFunctionByNameWithOverload(ctx, "bit_count", []types.Type{t}, bitCountFloatFieldOverload(t.Oid))
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
			scope != nil && scope.reducibleTablelessScalar {
			return scope.singleProjectionRetainsSubquery
		}
		if direct && bitCountInSubqueryCanReduce(scope, sub) {
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

func bitCountInSubqueryCanReduce(scope *BindContext, sub *plan.SubqueryRef) bool {
	if scope == nil || !scope.canReduceInToSubqueryFreeComparison || sub == nil || sub.RowSize != 1 || sub.Child == nil {
		return false
	}
	return sub.Typ == plan.SubqueryRef_IN || sub.Typ == plan.SubqueryRef_ANY && sub.Op == "="
}

func bitCountWhereIsEffectivelyTrue(builder *QueryBuilder, ctx *BindContext) bool {
	if ctx == nil {
		return false
	}
	for _, filter := range ctx.whereFilters {
		lit := filter.GetLit()
		if filter.Typ.Id == int32(types.T_bool) && lit != nil && !lit.Isnull && lit.GetBval() {
			continue
		}
		fn := filter.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
			return false
		}
		switch fn.Func.GetObjName() {
		case "=", "<>", "!=", "<", "<=", ">", ">=":
		default:
			return false
		}
		if fn.Args[0].GetLit() == nil || fn.Args[1].GetLit() == nil {
			return false
		}
		if fn.Func.GetObjName() == "=" && reflect.DeepEqual(fn.Args[0].Typ, fn.Args[1].Typ) {
			left, right := fn.Args[0].GetLit(), fn.Args[1].GetLit()
			if left != nil && right != nil && !left.Isnull && !right.Isnull && reflect.DeepEqual(left, right) {
				continue
			}
		}
		// The SELECT/IN transform may remove literal comparisons before reducing
		// the RHS. Fold only two already-bound literals under a comparison
		// operator; general constant folding can call external functions.
		if builder == nil || builder.compCtx == nil {
			return false
		}
		folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(filter), builder.compCtx.GetProcess(), false, true)
		if err != nil || folded == nil || folded.GetLit() == nil || folded.GetLit().Isnull ||
			folded.Typ.Id != int32(types.T_bool) || !folded.GetLit().GetBval() {
			return false
		}
	}
	return true
}

func bitCountProjectionDependsOnRows(builder *QueryBuilder, expr *Expr, direct bool, seen map[int32]bool) bool {
	if expr == nil {
		return false
	}
	if sub := expr.GetSub(); sub != nil {
		scope := bitCountSubqueryScope(builder, sub)
		if direct && bitCountInSubqueryCanReduce(scope, sub) &&
			!bitCountProjectionRetainsSubqueryAfterReduction(builder, sub.Child, true) {
			return false
		}
		if direct && sub.Typ == plan.SubqueryRef_SCALAR && sub.Child == nil && sub.RowSize == 1 &&
			scope != nil && scope.reducibleTablelessScalar {
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

// A mergeable projection forwards its operand's conversion. A materialized
// query block exposes a field even when the original expression was a CAST.
func (b *baseBinder) bitCountReadsFloatField(expr *Expr) bool {
	// A same-block group slot represents the original GROUP BY expression.
	// Inspect its conversion without replacing the slot used for evaluation.
	if col := expr.GetCol(); col != nil && b.ctx != nil && b.ctx.groupTag > 0 &&
		col.RelPos == b.ctx.groupTag && col.ColPos >= 0 && int(col.ColPos) < len(b.ctx.groups) {
		expr = b.ctx.groups[col.ColPos]
	}
	if b.builder == nil || b.builder.qry == nil {
		return expr.GetCol() != nil || expr.GetCorr() != nil
	}
	// Projections are bound before consumers. The decreasing node ID keeps
	// recursive or self-referencing projections from reopening the lookup.
	nodes := b.builder.qry.Nodes
	limit := len(nodes)
	for {
		if fn := expr.GetF(); fn != nil && fn.Func != nil &&
			fn.Func.GetObjName() == "cast" && fn.GetSyntaxExplicitCast() {
			return false
		}
		if sub := expr.GetSub(); sub != nil && sub.Typ == plan.SubqueryRef_SCALAR {
			var scope *BindContext
			if sub.NodeId >= 0 && int(sub.NodeId) < len(b.builder.ctxByNode) {
				scope = b.builder.ctxByNode[sub.NodeId]
			}
			if scope == nil || !scope.reducibleTablelessScalar || sub.Child != nil || sub.RowSize != 1 ||
				len(scope.projects) != 1 || scope.hasDependentProjectionSubquery {
				return true
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
			return false
		}
		id, ok := b.builder.tag2NodeID[tag]
		if !ok || id < 0 || int(id) >= limit {
			// Aggregate and window outputs can be bound before their node.
			return true
		}
		limit = int(id)
		node := nodes[id]
		if node.NodeType != plan.Node_PROJECT || pos < 0 || int(pos) >= len(node.ProjectList) {
			return true
		}
		scope := b.builder.ctxByNode[id]
		if scope != nil && (scope.isDistinct || scope.hasHaving || len(scope.aggregates) != 0 ||
			len(scope.groups) != 0 || len(scope.windows) != 0 || scope.hasDependentProjectionSubquery) {
			return true
		}
		for current := node; ; {
			if current.Limit != nil || current.Offset != nil || current.NodeType == plan.Node_VALUE_SCAN ||
				current.NodeType == plan.Node_MATERIAL || isPreparedSetOperationNode(current.NodeType) {
				return true
			}
			if len(current.Children) != 1 || b.builder.ctxByNode[current.Children[0]] != scope {
				break
			}
			current = nodes[current.Children[0]]
		}
		expr = node.ProjectList[pos]
	}
}
