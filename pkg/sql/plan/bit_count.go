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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

func bitCountGroupSource(ctx *BindContext, expr *Expr) *Expr {
	if ctx == nil || ctx.groupTag <= 0 || expr == nil {
		return expr
	}
	col := expr.GetCol()
	if col == nil || col.RelPos != ctx.groupTag || col.ColPos < 0 || int(col.ColPos) >= len(ctx.groups) {
		return expr
	}
	return ctx.groups[col.ColPos]
}

// Bind the count inside each selected arm so the control-flow executor keeps
// its row masks and the operand reaches BIT_COUNT with its own conversion.
func (b *baseBinder) bindBitCountOperand(expr *Expr) (*Expr, error) {
	if grouped, err := b.bindGroupedBitCount(expr); grouped != nil || err != nil {
		return grouped, err
	}
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
		overload := b.bitCountFloatOverload(expr)
		if overload >= 0 {
			return bindBitCountFloatOverload(ctx, []*Expr{expr}, overload)
		}
	}
	return BindFuncExprImplByPlanExpr(ctx, "bit_count", []*Expr{expr})
}

const (
	foldedFloatArithmetic uint32 = 1
	foldedFloatCast       uint32 = 2
)

// Constant folding must not erase how BIT_COUNT converts a floating source.
// Unary plus is transparent and inherits a literal or CAST source category.
func markFoldedFloatArithmetic(name string, args []*Expr, result *Expr) {
	if result == nil || !types.T(result.Typ.Id).IsFloat() || result.GetLit() == nil {
		return
	}
	switch name {
	case "+", "-", "*", "/", "div", "%", "mod", "unary_minus":
		result.GetLit().FloatConversionSource = foldedFloatArithmetic
	case "unary_plus":
		if len(args) != 1 {
			return
		}
		if lit := args[0].GetLit(); lit != nil {
			result.GetLit().FloatConversionSource = lit.FloatConversionSource
		} else if fn := args[0].GetF(); fn != nil && fn.GetFunc().GetObjName() == "cast" {
			result.GetLit().FloatConversionSource = foldedFloatCast
		} else {
			result.GetLit().FloatConversionSource = foldedFloatArithmetic
		}
	}
}

func bitCountFieldOverload(expr *Expr) int32 {
	if expr.Typ.Id == int32(types.T_float32) {
		return function.BitCountFloat32FieldOverload
	}
	return function.BitCountFloat64FieldOverload
}

func bitCountSelector(value *Expr) *Expr {
	for {
		fn := value.GetF()
		if fn == nil || fn.Func == nil {
			return nil
		}
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		switch id {
		case function.UNARY_PLUS:
			value = fn.Args[0]
		case function.CASE, function.IFF:
			if value.GetPreparedNumeric().GetIfnullCommonValue() {
				return nil
			}
			return value
		default:
			return nil
		}
	}
}

func bitCountScalarOperand(expr tree.Expr) *tree.Subquery {
	for {
		expr = unwrapParenExpr(expr)
		unary, ok := expr.(*tree.UnaryExpr)
		if !ok || unary.Op != tree.UNARY_PLUS {
			sub, _ := expr.(*tree.Subquery)
			return sub
		}
		expr = unary.Expr
	}
}

// Follow only a direct scalar projection that cannot filter or duplicate rows.
// Its AST identity limits the deep-correlation shortcut to BIT_COUNT's source.
func bitCountForwardedScalar(sub *tree.Subquery) *tree.Subquery {
	if sub == nil || sub.Exists {
		return nil
	}
	var selectStmt *tree.Select
	switch stmt := sub.Select.(type) {
	case *tree.Select:
		selectStmt = stmt
	case *tree.ParenSelect:
		selectStmt = stmt.Select
	}
	if selectStmt == nil || selectStmt.IsPerform || selectStmt.RewriteOption != nil ||
		selectStmt.With != nil || !bitCountNoopLimit(selectStmt.Limit) ||
		selectStmt.RankOption != nil || selectStmt.TimeWindow != nil || len(selectStmt.OrderBy) != 0 ||
		selectStmt.Ep != nil || len(selectStmt.IntoVars) != 0 || selectStmt.SelectLockInfo != nil {
		return nil
	}
	clause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || clause.Distinct || clause.Option != 0 || len(clause.Exprs) != 1 ||
		!bitCountSyntheticFrom(clause.From) || clause.Where != nil || clause.GroupBy != nil ||
		clause.Having != nil || len(clause.Windows) != 0 || clause.IntoExport != nil || len(clause.IntoVars) != 0 {
		return nil
	}
	next := bitCountScalarOperand(clause.Exprs[0].Expr)
	if next != nil && !next.Exists {
		return next
	}
	return nil
}

func bitCountNoopLimit(limit *tree.Limit) bool {
	return limit == nil || limit.Count == nil && limit.Offset != nil && zeroLimitLiteral(unwrapParenExpr(limit.Offset))
}

// The MySQL parser represents a SELECT without FROM as one empty table name.
func bitCountSyntheticFrom(from *tree.From) bool {
	if from == nil || len(from.Tables) == 0 {
		return true
	}
	if len(from.Tables) != 1 {
		return false
	}
	aliased, ok := from.Tables[0].(*tree.AliasedTableExpr)
	if !ok || aliased == nil || aliased.As.Alias != "" || len(aliased.As.Cols) != 0 || len(aliased.IndexHints) != 0 {
		return false
	}
	table, ok := aliased.Expr.(*tree.TableName)
	return ok && table != nil && table.ObjectName == "" && table.Schema() == "" && table.Catalog() == "" &&
		!table.ExplicitSchema && !table.ExplicitCatalog && table.AtTsExpr == nil && table.IcebergRef == nil
}

func bitCountReducibleScalarProjection(builder *QueryBuilder, sub *plan.SubqueryRef) *Expr {
	if sub == nil || sub.Typ != plan.SubqueryRef_SCALAR || sub.Child != nil || sub.RowSize != 1 {
		return nil
	}
	project := builder.tablelessProjection(sub.NodeId, true)
	if project == nil || len(project.ProjectList) != 1 {
		return nil
	}
	return project.ProjectList[0]
}

// Lower only when scalar flattening returned the source projection directly.
// A retained scalar still exposes a field, and an IF/CASE synthesized for
// scalar row presence must not be mistaken for the user's selector.
func (builder *QueryBuilder) lowerScalarBitCount(ctx *BindContext, fn *plan.Function, sub *plan.SubqueryRef) (*Expr, error) {
	value := fn.Args[0]
	if releasedBitCountFloatOverload(types.T(value.Typ.Id)) < 0 {
		return nil, nil
	}
	_, overload := function.DecodeOverloadID(fn.Func.Obj)
	if overload == releasedBitCountFloatOverload(types.T(value.Typ.Id)) {
		return nil, nil
	}
	selector := bitCountSelector(value)
	if selector == nil || bitCountReducibleScalarProjection(builder, sub) == nil {
		return nil, nil
	}
	project := builder.qry.Nodes[sub.NodeId].ProjectList
	if len(project) != 1 {
		return nil, nil
	}
	if project[0] != value {
		// Deep tableless flattening rebases a copy, rather than returning the
		// original node. Only accept that exact, independently proven copy.
		rebased := builder.inlineDeepTablelessScalar(sub)
		if rebased == nil || !exprWireEqual(rebased, value) {
			return nil, nil
		}
	}
	binder := baseBinder{builder: builder, ctx: ctx, sysCtx: builder.GetContext()}
	return binder.bindBitCountOperand(selector)
}

// Forward simple projection sources through query blocks without explicit
// materialization. A projected selector exposes its common value as a field.
func (b *baseBinder) bitCountFloatOverload(expr *Expr) int32 {
	field, expression := bitCountFieldOverload(expr), int32(function.BitCountFloat64ExpressionOverload)
	if expr.Typ.Id == int32(types.T_float32) {
		expression = function.BitCountFloat32ExpressionOverload
	}
	fromProjection := false
	// A same-block group slot represents the original GROUP BY expression.
	// Inspect its conversion without replacing the slot used for evaluation.
	expr = bitCountGroupSource(b.ctx, expr)
	// Projections are bound before consumers. The decreasing node ID keeps
	// recursive or self-referencing projections from reopening the lookup.
	var nodes []*plan.Node
	if b.builder != nil && b.builder.qry != nil {
		nodes = b.builder.qry.Nodes
	}
	limit := len(nodes)
	for {
		if lit := expr.GetLit(); lit != nil {
			switch lit.FloatConversionSource {
			case foldedFloatArithmetic:
				return expression
			case foldedFloatCast:
				return -1
			default:
				if expr.Typ.Id == int32(types.T_float64) {
					return function.BitCountFloat64LiteralOverload
				}
				return field
			}
		}
		if fn := expr.GetF(); fn != nil && fn.Func != nil {
			id, _ := function.DecodeOverloadID(fn.Func.GetObj())
			switch id {
			case function.CASE:
				if fromProjection {
					return field
				}
				// IFNULL converts its common result, even after lowering to CASE.
				if expr.GetPreparedNumeric().GetIfnullCommonValue() {
					return int32(expression)
				}
				fallthrough
			case function.IFF:
				if fromProjection {
					// A projected selector exposes its common value as a field.
					// Keep that conversion when the optimizer inlines it.
					return field
				}
				return -1
			case function.CAST:
				return -1
			case function.UNARY_PLUS:
				expr = fn.Args[0]
				continue
			default:
				return int32(expression)
			}
		}
		// A retained dependent scalar is an executable subquery result, not
		// a transparent copy of its projected source.
		if expr.GetDependent() != nil {
			return field
		}
		if nodes == nil {
			if expr.GetCol() != nil || expr.GetCorr() != nil {
				return int32(field)
			}
			return -1
		}
		if sub := expr.GetSub(); sub != nil && sub.Typ == plan.SubqueryRef_SCALAR {
			expr = bitCountReducibleScalarProjection(b.builder, sub)
			if expr == nil {
				return int32(field)
			}
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
			len(scope.groups) != 0 || len(scope.windows) != 0) {
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
		fromProjection = true
	}
}
