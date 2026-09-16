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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// bindIntegerArgumentAst binds selecting expressions in their consumer's
// integer context before ordinary CASE reconciliation converts exact branches
// to DOUBLE. IFNULL/COALESCE and arithmetic are value-producing boundaries,
// not transparent selectors, even when they contain an explicit REAL CAST.
func (b *baseBinder) bindIntegerArgumentAst(ast tree.Expr, depth int32, target types.T) (*Expr, error) {
	return b.bindIntegerSourceAst(ast, depth, target, "", 0)
}

func (b *baseBinder) bindIntegerSourceAst(ast tree.Expr, depth int32, target types.T, name string, position int) (*Expr, error) {
	previousSourceContext := b.integerArgumentSourceContext
	b.integerArgumentSourceContext = true
	defer func() { b.integerArgumentSourceContext = previousSourceContext }()
	switch value := unwrapParenExpr(ast).(type) {
	case *tree.CaseExpr:
		args := make([]*Expr, 0, 2*len(value.Whens)+1)
		for _, when := range value.Whens {
			condition := when.Cond
			if value.Expr != nil {
				condition = tree.NewComparisonExpr(tree.EQUAL, value.Expr, condition)
			} else {
				condition = bindCaseConditionParam(condition)
			}
			predicate, err := b.impl.BindExpr(condition, depth, false)
			if err != nil {
				return nil, err
			}
			branch, err := b.bindIntegerSourceAst(when.Val, depth, target, name, position)
			if err != nil {
				return nil, err
			}
			args = append(args, predicate, branch)
		}
		otherwise := value.Else
		if otherwise == nil {
			otherwise = tree.NewNumVal("", "", false, tree.P_null)
		}
		branch, err := b.bindIntegerSourceAst(otherwise, depth, target, name, position)
		if err != nil {
			return nil, err
		}
		return bindIntegerSelector(b.GetContext(), append(args, branch), function.IntegerArgumentUsesBitSources(name, position))
	case *tree.FuncExpr:
		callee := numericAstFunctionName(value)
		if callee == "nullif" && len(value.Exprs) == 2 {
			predicate, err := b.impl.BindExpr(tree.NewComparisonExpr(tree.EQUAL, value.Exprs[0], value.Exprs[1]), depth, false)
			if err != nil {
				return nil, err
			}
			otherwise, err := b.bindIntegerSourceAst(value.Exprs[0], depth, target, name, position)
			if err != nil {
				return nil, err
			}
			nullValue, err := b.bindIntegerSourceAst(tree.NewNumVal("", "", false, tree.P_null), depth, target, name, position)
			if err != nil {
				return nil, err
			}
			return bindIntegerSelector(b.GetContext(), []*Expr{predicate, nullValue, otherwise}, function.IntegerArgumentUsesBitSources(name, position))
		}
		if (callee == "if" || callee == "iff") && len(value.Exprs) == 3 {
			predicate, err := b.impl.BindExpr(bindCaseConditionParam(value.Exprs[0]), depth, false)
			if err != nil {
				return nil, err
			}
			yes, err := b.bindIntegerSourceAst(value.Exprs[1], depth, target, name, position)
			if err != nil {
				return nil, err
			}
			no, err := b.bindIntegerSourceAst(value.Exprs[2], depth, target, name, position)
			if err != nil {
				return nil, err
			}
			return bindIntegerSelector(b.GetContext(), []*Expr{predicate, yes, no}, function.IntegerArgumentUsesBitSources(name, position))
		}
		if callee == "coalesce" || callee == "ifnull" {
			// These are value-producing boundaries rather than transparent
			// selectors, but their prepared markers still need the numeric domain
			// proven by sibling values. Otherwise PREPARE reconciles a marker and
			// 0e0 as TEXT, and EXECUTE cannot recover DOUBLE semantics from the
			// already-converted literal.
			physical := target.ToType()
			planTarget := makePlan2Type(&physical)
			source, err := b.bindNumericExprWithContext(value, depth, &planTarget)
			if err != nil {
				return nil, err
			}
			source, err = b.integerArgumentStorageSource(source)
			if err != nil {
				return nil, err
			}
			return appendIntegerArgument(b.GetContext(), source, target, false)
		}
	}
	source, err := b.impl.BindExpr(ast, depth, false)
	if err != nil {
		return nil, err
	}
	source, err = b.integerArgumentStorageSource(source)
	if err != nil {
		return nil, err
	}
	return appendIntegerArgument(b.GetContext(), source, target, false)
}

// Recover only direct display wrappers or established reversible provenance.
// Arbitrary string expressions (including IFNULL/COALESCE) own their result
// domain and must not be reinterpreted as the original ENUM/SET storage.
func (b *baseBinder) integerArgumentStorageSource(source *Expr) (*Expr, error) {
	if raw, ok := storedMySQLSpecialTypeExpr(source); ok {
		return raw, nil
	}
	if b.ctx != nil {
		if storage := b.ctx.mysqlSpecialOrderTypeForExpr(source); storage != nil && mysqlSpecialNumericTypeReversible(storage) {
			return makeMySQLSpecialNumericValue(b.GetContext(), source, storage)
		}
	}
	return source, nil
}

// stripIntegerSelectionReconciliation removes only binder-owned reconciliation
// from a selected value. User CAST remains an authoritative value boundary.
func stripIntegerSelectionReconciliation(expr *Expr) *Expr {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || fn.SyntaxExplicitCast || len(fn.Args) != 2 {
		return expr
	}
	_, id := function.DecodeOverloadID(fn.Func.Obj)
	target := types.T(expr.Typ.Id)
	if id != 0 || (!target.IsFloat() && !target.IsMySQLString()) {
		return expr
	}
	// Remove only the selecting parent's approximate/text envelope. A nested
	// CAST0 may construct a DECIMAL literal from text; removing that conversion
	// would turn DECIMAL half-up into string-prefix truncation.
	return fn.Args[0]
}

func appendIntegerArgument(ctx context.Context, expr *Expr, target types.T, selectBranches bool) (*Expr, error) {
	if raw, ok := storedMySQLSpecialTypeExpr(expr); ok {
		expr = raw
	}
	if types.T(expr.Typ.Id) == target {
		return expr, nil
	}
	fn := expr.GetF()
	if selectBranches && fn != nil && fn.Func != nil && (fn.Func.ObjName == "case" || fn.Func.ObjName == "if" || fn.Func.ObjName == "iff") {
		args := append([]*Expr(nil), fn.Args...)
		for i := range args {
			if i%2 == 0 && i != len(args)-1 {
				continue
			}
			branch, err := appendIntegerArgument(ctx, stripIntegerSelectionReconciliation(args[i]), target, true)
			if err != nil {
				return nil, err
			}
			args[i] = branch
		}
		return BindFuncExprImplByPlanExpr(ctx, fn.Func.ObjName, args)
	}
	typ := target.ToType()
	return appendCastBeforeExprWithOverload(ctx, expr, makePlan2Type(&typ), integerSourceCastOverload(expr))
}

func integerSourceCastOverload(expr *Expr) int32 {
	fn := expr.GetF()
	overload := function.IntegerArgumentCastOverload
	if types.T(expr.Typ.Id).IsFloat() && fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" {
		_, sourceOverload := function.DecodeOverloadID(fn.Func.Obj)
		if fn.SyntaxExplicitCast || sourceOverload == 1 {
			overload = function.TruncatedIntegerArgumentCastOverload
		}
	}
	return overload
}

func isIntegerArgumentCast(expr *Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) != 2 {
		return false
	}
	_, overload := function.DecodeOverloadID(fn.Func.Obj)
	return function.IsIntegerArgumentCastOverload(overload)
}

// The parameter's actual source domain belongs to EXECUTE, not to the integer
// result type of this conversion. Preserve both that source and the private
// execution identity instead of reconstructing an ordinary CAST0.
func (rule *ResetParamRefRule) rebindIntegerArgumentCast(expr *Expr) (*Expr, error) {
	rewritten, err := rule.integerArgumentRuntimeSource(expr.GetF().Args[0])
	if err != nil {
		return nil, err
	}
	bound := DeepCopyExpr(expr)
	bound.GetF().Args[0] = rewritten
	bound.Typ.NotNullable = rewritten.Typ.NotNullable
	bound.GetF().Args[1].Typ.NotNullable = rewritten.Typ.NotNullable
	rule.specialized = true
	return bound, nil
}

func (rule *ResetParamRefRule) integerArgumentRuntimeSource(source *Expr) (*Expr, error) {
	if isIntegerArgumentCast(source) {
		return rule.rebindIntegerArgumentCast(source)
	}
	if marker := source.GetP(); marker != nil {
		if value, ok, err := rule.preparedRuntimeSourceExpr(int(marker.Pos), true); err != nil || ok {
			return value, err
		}
		if value, ok, err := rule.typedRuntimeParamExpr(int(marker.Pos)); err != nil || ok {
			return value, err
		}
	}
	if fn := source.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 {
		value, err := rule.integerArgumentRuntimeSource(fn.Args[0])
		if err != nil {
			return nil, err
		}
		return rebindExplicitPreparedCast(rule.ctx, source, []*Expr{value, fn.Args[1]})
	}
	return rule.ApplyExpr(source)
}

// Only validated integer results are reconciled here. Source REAL/DECIMAL
// values must never acquire unsigned range merely from a sibling branch.
func bindIntegerSelector(ctx context.Context, args []*Expr, bitSources bool) (*Expr, error) {
	unsigned := false
	if bitSources {
		for i, arg := range args {
			if (i%2 == 1 || i == len(args)-1) && types.T(arg.Typ.Id) == types.T_uint64 {
				unsigned = true
			}
		}
	}
	if unsigned {
		physical := types.T_uint64.ToType()
		for i, arg := range args {
			if (i%2 == 1 || i == len(args)-1) && types.T(arg.Typ.Id) == types.T_int64 {
				var err error
				args[i], err = appendCastBeforeExprWithOverload(ctx, arg, makePlan2Type(&physical), 1)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return BindFuncExprImplByPlanExpr(ctx, "case", args)
}

func bindIntegerFunctionArguments(ctx context.Context, name string, args []*Expr) ([]*Expr, error) {
	var result []*Expr
	for i, arg := range args {
		if arg == nil {
			continue // Leave absent-argument diagnostics to the owning validator.
		}
		target, ok := function.IntegerArgumentTarget(name, i)
		if !ok {
			continue
		}
		if raw, direct := storedMySQLSpecialTypeExpr(arg); direct {
			arg = raw
		}
		bound, err := appendIntegerArgument(ctx, arg, target, true)
		if err != nil {
			return nil, err
		}
		if bound == arg {
			continue
		}
		if result == nil {
			result = append([]*Expr(nil), args...)
		}
		result[i] = bound
	}
	if result == nil {
		return args, nil
	}
	return result, nil
}
