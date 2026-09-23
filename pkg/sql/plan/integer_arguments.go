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
	}
	source, err := b.impl.BindExpr(ast, depth, false)
	if err != nil {
		return nil, err
	}
	source, err = b.integerArgumentStorageSource(source)
	if err != nil {
		return nil, err
	}
	if function.IntegerArgumentSourceDependent(name, position) {
		return appendSourceDependentIntegerArgument(b.GetContext(), source, name, position)
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
	integerWidening := target.IsDecimal() && types.T(fn.Args[0].Typ.Id).IsInteger()
	if id != 0 || (!target.IsFloat() && !target.IsMySQLString() && !integerWidening) {
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
	value, err := rule.integerArgumentLogicalSource(source)
	if err != nil || value == nil || types.T(value.Typ.Id) != types.T_any {
		return value, err
	}
	// ANY is useful while reconciling domainless NULL with its siblings, but
	// projected/grouped values must have a concrete vector representation.
	typ := types.T_int64.ToType()
	if value.GetLit().GetIsnull() {
		value = DeepCopyExpr(value)
		value.Typ = makePlan2Type(&typ)
		return value, nil
	}
	return appendCastBeforeExpr(rule.ctx, value, makePlan2Type(&typ))
}

func (rule *ResetParamRefRule) integerArgumentLogicalSource(source *Expr) (*Expr, error) {
	if refreshed, changed, err := rule.refreshPreparedNumericSource(source); err != nil {
		return nil, err
	} else if changed {
		source = refreshed
	}
	if isIntegerArgumentCast(source) {
		return rule.rebindIntegerArgumentCast(source)
	}
	if hasSourceDependentIntegerArguments(source) {
		return rule.rebindSourceDependentIntegerArguments(source)
	}
	if marker := source.GetP(); marker != nil {
		if value, ok, err := rule.preparedRuntimeSourceExpr(int(marker.Pos), true); err != nil || ok {
			if value != nil && value.GetLit().GetIsnull() {
				// The transport's placeholder type is not a SQL value domain.
				typ := types.T_any.ToType()
				value.Typ = makePlan2Type(&typ)
			}
			return value, err
		}
		if value, ok, err := rule.typedRuntimeParamExpr(int(marker.Pos)); err != nil || ok {
			return value, err
		}
	}
	if fn := source.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 {
		value, err := rule.integerArgumentLogicalSource(fn.Args[0])
		if err != nil {
			return nil, err
		}
		return rebindExplicitPreparedCast(rule.ctx, source, []*Expr{value, fn.Args[1]})
	}
	// The aggregate owns CAST4. Rebuild it from the actual source domain;
	// ordinary numeric fallback would infer DECIMAL from COM_STMT text.
	if fn := source.GetF(); fn != nil && fn.Func != nil && isPreparedBitwiseAggregate(fn.Func.ObjName) && len(fn.Args) == 1 {
		arg := fn.Args[0]
		if isBitwiseAggregatePrivateCast(arg) {
			arg = arg.GetF().Args[0]
		}
		value, err := rule.integerArgumentRuntimeSource(arg)
		if err != nil {
			return nil, err
		}
		bound, err := BindFuncExprImplByPlanExpr(rule.ctx, fn.Func.ObjName, []*Expr{value})
		if err == nil {
			preserveReboundFunctionMetadata(fn, bound.GetF())
		}
		return bound, err
	}
	// Reconstruct common-type producers from each actual source, not numeric
	// spelling inference or an all-parameters-are-text shortcut. In particular,
	// one numeric marker cannot turn its TEXT sibling into a numeric value.
	if fn := source.GetF(); fn != nil && fn.Func != nil &&
		(fn.Func.ObjName == "coalesce" || fn.Func.ObjName == "case" || fn.Func.ObjName == "if" || fn.Func.ObjName == "iff") {
		args := make([]*Expr, len(fn.Args))
		for i, arg := range fn.Args {
			// This common-type producer is itself inside the private integer
			// conversion, so its binder-owned TEXT/REAL envelope is provisional.
			// Explicit CAST remains authoritative.
			arg = stripIntegerSelectionReconciliation(arg)
			if refreshed, changed, refreshErr := rule.refreshPreparedNumericSource(arg); refreshErr != nil {
				return nil, refreshErr
			} else if changed {
				arg = refreshed
			}
			metadata := arg.GetPreparedNumeric()
			if metadata.GetProvisionalResultPeer() {
				var err error
				arg, err = restorePreparedResultPeer(rule.ctx, arg)
				if err != nil {
					return nil, err
				}
			} else if metadata.GetProvisionalResultCast() && !isExplicitPreparedCast(arg) {
				if cast := arg.GetF(); cast != nil && len(cast.Args) == 2 {
					arg = cast.Args[0]
				}
			}
			var err error
			args[i], err = rule.integerArgumentLogicalSource(arg)
			if err != nil {
				return nil, err
			}
			// A scalar-subquery ColRef can acquire the provisional peer marker
			// only after its producer is rebound. Restore that current occurrence
			// as well as the original PREPARE-time argument above.
			if args[i].GetPreparedNumeric().GetProvisionalResultPeer() {
				args[i], err = restorePreparedResultPeer(rule.ctx, args[i])
				if err != nil {
					return nil, err
				}
			}
		}
		bound, err := BindFuncExprImplByPlanExpr(rule.ctx, fn.Func.ObjName, args)
		if err == nil {
			preserveReboundFunctionMetadata(fn, bound.GetF())
		}
		return bound, err
	}
	return rule.applyExpr(source)
}

// restorePreparedResultPeer works exclusively on the current plan occurrence.
// Provenance records a type, never an executable pre-optimization snapshot:
// old ColRefs and SubqueryRefs are invalid after remapping and flattening.
func restorePreparedResultPeer(ctx context.Context, expr *Expr) (*Expr, error) {
	metadata := expr.GetPreparedNumeric()
	if !metadata.GetProvisionalResultPeer() {
		return expr, nil
	}
	// T_any == 0 is a recorded domainless NULL, not missing provenance.
	if metadata.GetProvisionalResultPeerTypeId() == int32(types.T_any) && expr.GetLit().GetIsnull() {
		value := DeepCopyExpr(expr)
		typ := types.T_any.ToType()
		value.Typ = makePlan2Type(&typ)
		value.PreparedNumeric = nil
		return value, nil
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" &&
		!fn.SyntaxExplicitCast && len(fn.Args) == 2 && types.T(expr.Typ.Id).IsMySQLString() {
		_, overload := function.DecodeOverloadID(fn.Func.Obj)
		if overload == 0 {
			return fn.Args[0], nil
		}
	}
	if !types.T(expr.Typ.Id).IsMySQLString() {
		return expr, nil
	}
	typ := types.New(types.T(metadata.ProvisionalResultPeerTypeId),
		metadata.ProvisionalResultPeerWidth, metadata.ProvisionalResultPeerScale)
	value := DeepCopyExpr(expr)
	value.PreparedNumeric = nil
	return appendCastBeforeExpr(ctx, value, makePlan2Type(&typ))
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
		dependent := function.IntegerArgumentSourceDependent(name, i)
		if !ok && !dependent {
			continue
		}
		if raw, direct := storedMySQLSpecialTypeExpr(arg); direct {
			arg = raw
		}
		var bound *Expr
		var err error
		if dependent {
			bound, err = appendSourceDependentIntegerArgument(ctx, arg, name, i)
		} else {
			bound, err = appendIntegerArgument(ctx, arg, target, true)
		}
		if err != nil {
			return nil, err
		}
		if physical, adapted := function.IntegerArgumentPhysicalTarget(name, i); adapted {
			physicalType := physical.ToType()
			bound, err = forceAssignmentCastExprWithName(ctx, bound, makePlan2Type(&physicalType), "cast_strict")
			if err != nil {
				return nil, err
			}
		}
		if bound == args[i] {
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

// Source-dependent roles choose their domain before any physical bit adapter.
// Numeric selectors also preserve each branch's exact source; a numeric-only
// role retains ordinary reconciliation when the selector produces strings or
// arrays rather than an integer domain.
func appendSourceDependentIntegerArgument(ctx context.Context, source *Expr, name string, position int) (*Expr, error) {
	if raw, ok := storedMySQLSpecialTypeExpr(source); ok {
		source = raw
	}
	bitSources := function.IntegerArgumentUsesBitSources(name, position)
	fn := source.GetF()
	_, numericSelector := function.IntegerArgumentTargetForSource(name, position, types.T(source.Typ.Id), false)
	if (bitSources || numericSelector) && fn != nil && fn.Func != nil && (fn.Func.ObjName == "case" || fn.Func.ObjName == "if" || fn.Func.ObjName == "iff") {
		args := append([]*Expr(nil), fn.Args...)
		for i, arg := range args {
			if i%2 == 1 || i == len(args)-1 {
				var err error
				args[i], err = appendSourceDependentIntegerArgument(ctx, stripIntegerSelectionReconciliation(arg), name, position)
				if err != nil {
					return nil, err
				}
			}
		}
		return bindIntegerSelector(ctx, args, true)
	}
	target, applies := function.IntegerArgumentTargetForSource(name, position, types.T(source.Typ.Id), source.GetLit().GetIsBin())
	if !applies {
		return source, nil
	}
	if bitSources && types.T(source.Typ.Id).IsMySQLString() {
		typ := target.ToType()
		return appendCastBeforeExprWithOverload(ctx, source, makePlan2Type(&typ), function.TextIntegerBitsCastOverload)
	}
	return appendIntegerArgument(ctx, source, target, false)
}

// Include unconverted numeric-only arguments (notably HEX(?)) in the cached
// prepared-source inventory as well as the private conversions themselves.
func integerArgumentSources(expr *Expr) []*Expr {
	if isIntegerArgumentCast(expr) {
		return expr.GetF().Args[:1]
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return nil
	}
	var sources []*Expr
	for i, arg := range fn.Args {
		if function.IntegerArgumentSourceDependent(fn.Func.ObjName, i) {
			sources = append(sources, arg)
		}
	}
	return sources
}

func hasSourceDependentIntegerArguments(expr *Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	for i := range fn.Args {
		if function.IntegerArgumentSourceDependent(fn.Func.ObjName, i) {
			return true
		}
	}
	return false
}

func (rule *ResetParamRefRule) rebindSourceDependentIntegerArguments(expr *Expr) (*Expr, error) {
	fn := expr.GetF()
	args := make([]*Expr, len(fn.Args))
	for i, arg := range fn.Args {
		var err error
		if function.IntegerArgumentSourceDependent(fn.Func.ObjName, i) {
			args[i], err = rule.sourceDependentIntegerRuntimeSource(arg, fn.Func.ObjName, i)
		} else {
			args[i], err = rule.ApplyExpr(arg)
		}
		if err != nil {
			return nil, err
		}
	}
	bound, err := BindFuncExprImplByPlanExpr(rule.ctx, fn.Func.ObjName, args)
	if err == nil {
		preserveReboundFunctionMetadata(fn, bound.GetF())
		rule.specialized = true
	}
	return bound, err
}

// Discard only this role's provisional private conversion and integer bit
// adapters. Explicit casts and nested value-producing functions remain owners
// of their result domains. Selector branches are rebound independently before
// signed/unsigned reconciliation so no unchecked REAL/DECIMAL reaches CAST1.
func (rule *ResetParamRefRule) sourceDependentIntegerRuntimeSource(source *Expr, name string, position int) (*Expr, error) {
	bitSources := function.IntegerArgumentUsesBitSources(name, position)
	// PREPARE may reconcile a selector containing an untyped marker to VARCHAR.
	// That envelope is provisional for a source-dependent consumer: expose the
	// selector before deciding its runtime domain. User-written CAST remains a
	// value boundary because stripIntegerSelectionReconciliation preserves it.
	source = stripIntegerSelectionReconciliation(source)
	if fn := source.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && !fn.SyntaxExplicitCast && len(fn.Args) == 2 {
		_, id := function.DecodeOverloadID(fn.Func.Obj)
		if function.IsIntegerArgumentCastOverload(id) || (id == 1 && types.T(fn.Args[0].Typ.Id) == types.T_int64 && types.T(source.Typ.Id) == types.T_uint64) {
			return rule.sourceDependentIntegerRuntimeSource(fn.Args[0], name, position)
		}
	}
	if fn := source.GetF(); fn != nil && fn.Func != nil && (fn.Func.ObjName == "case" || fn.Func.ObjName == "if" || fn.Func.ObjName == "iff") {
		// Preserve branch domains until the consuming role binds them. This is
		// required for both bit-pattern roles and numeric-only roles such as HEX:
		// the PREPARE-time selector result may otherwise retain a provisional
		// VARCHAR envelope after its marker has acquired a numeric runtime type.
		bound := DeepCopyExpr(source)
		for i, arg := range fn.Args {
			var err error
			if i%2 == 1 || i == len(fn.Args)-1 {
				arg = stripIntegerSelectionReconciliation(arg)
				restoredPeer := arg.GetPreparedNumeric().GetProvisionalResultPeer()
				if restoredPeer {
					arg, err = restorePreparedResultPeer(rule.ctx, arg)
					if err != nil {
						return nil, err
					}
				}
				if !restoredPeer {
					arg, err = rule.sourceDependentIntegerRuntimeSource(arg, name, position)
				}
				if err == nil {
					// Convert each restored source under the consumer's role before
					// reconciling the selector. This keeps a restored numeric CAST from
					// being confused with the obsolete PREPARE-time VARCHAR envelope.
					bound.GetF().Args[i], err = appendSourceDependentIntegerArgument(rule.ctx, arg, name, position)
				}
			} else {
				bound.GetF().Args[i], err = rule.ApplyExpr(arg)
			}
			if err != nil {
				return nil, err
			}
		}
		return bindIntegerSelector(rule.ctx, bound.GetF().Args, bitSources)
	}
	return rule.integerArgumentRuntimeSource(source)
}
