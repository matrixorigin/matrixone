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

import (
	"context"
	"errors"
	"slices"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MigrateLegacyFloatStringTableDef repairs execution-owned catalog expressions.
// SQL-origin proof distinguishes implicit casts from explicit CHAR(32), including
// catalogs written before explicit-cast provenance was recorded. Catalog storage
// and folded DEFAULT values are never rewritten.
func MigrateLegacyFloatStringTableDef(ctx CompilerContext, def *planpb.TableDef) error {
	if ctx == nil || def == nil {
		return nil
	}
	if err := ctx.GetContext().Err(); err != nil {
		return err
	}
	var budget *tableDumpBindingBudget
	for item := range persistedTableExpressions(def) {
		if err := ctx.GetContext().Err(); err != nil {
			return err
		}
		if !hasLegacyFloatStringCast(item.expr) {
			continue
		}
		if budget == nil {
			budget = &tableDumpBindingBudget{}
		}
		rebound, err := rebindLegacyFloatStringExpression(ctx, def, item, budget)
		if err != nil {
			return err
		}
		if err := ctx.GetContext().Err(); err != nil {
			return err
		}
		if rebound == nil {
			continue
		}
		if item.check != nil {
			def.Checks = slices.Clone(def.Checks)
			for i, check := range def.Checks {
				if check == item.check {
					owned := *check
					owned.Check = rebound
					def.Checks[i] = &owned
					break
				}
			}
		} else {
			def.Cols = slices.Clone(def.Cols)
			for i, col := range def.Cols {
				// An earlier DEFAULT repair may already have detached this same
				// column. Merge ON UPDATE into the latest owned wrapper by name.
				if col == nil || col.Name != item.name {
					continue
				}
				owned := *col
				switch item.kind {
				case "generated":
					generated := *col.GeneratedCol
					generated.Expr = rebound
					owned.GeneratedCol = &generated
				case "default":
					defaultExpr := *col.Default
					defaultExpr.Expr = rebound
					owned.Default = &defaultExpr
				case "on_update":
					onUpdate := *col.OnUpdate
					onUpdate.Expr = rebound
					owned.OnUpdate = &onUpdate
				}
				def.Cols[i] = &owned
				break
			}
		}
	}
	return ctx.GetContext().Err()
}

func hasLegacyFloatStringCast(expr *planpb.Expr) bool {
	if expr == nil || expr.GetLit() != nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if isLegacyFloatStringCast(expr) {
			return true
		}
		for _, arg := range fn.Args {
			if hasLegacyFloatStringCast(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, entry := range list.List {
			if hasLegacyFloatStringCast(entry) {
				return true
			}
		}
	}
	return false
}

func isLegacyFloatStringCast(expr *planpb.Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return false
	}
	id, overload := function.DecodeOverloadID(fn.Func.Obj)
	return id == function.CAST && overload == 0 &&
		expr.Typ.Id == int32(types.T_varchar) && expr.Typ.Width == 32 &&
		fn.Args[0].Typ.Id == int32(types.T_float64) &&
		fn.Args[1].GetT() != nil && fn.Args[1].Typ.Id == expr.Typ.Id && fn.Args[1].Typ.Width == 32
}

// Historical byte-string binders omitted implicit coercibility. Reconstruct only
// that bounded candidate era; explicit and native collation provenance stays exact.
func omitHistoricalFloatCoercibility(stored, candidate *planpb.Expr) {
	a, b := &stored.Typ, &candidate.Typ
	if !types.T(a.Id).IsMySQLString() || !types.T(b.Id).IsMySQLString() ||
		a.Charset != b.Charset ||
		(a.Charset != uint32(types.CharsetLegacy) && a.Charset != uint32(types.CharsetBinary) &&
			a.Charset != uint32(types.CharsetUTF8MB4Bin) && a.Charset != uint32(types.CharsetUTF8)) ||
		a.CollationVersion != 0 || b.CollationVersion != 0 ||
		a.CollationMergeConflict || b.CollationMergeConflict ||
		a.CollationCoercibilitySet || a.CollationCoercibility != 0 ||
		!b.CollationCoercibilitySet || b.CollationCoercibility == 0 || b.CollationCoercibility > 6 {
		return
	}
	b.CollationCoercibility, b.CollationCoercibilitySet = 0, false
}

func sameHistoricalFloatBoundExpression(stored, candidate *planpb.Expr) bool {
	right := proto.Clone(candidate).(*planpb.Expr)
	var project func(*planpb.Expr, *planpb.Expr)
	project = func(a, b *planpb.Expr) {
		if a == nil || b == nil {
			return
		}
		omitHistoricalFloatCoercibility(a, b)
		if x, y := a.GetF(), b.GetF(); x != nil && y != nil {
			for i := 0; i < len(x.Args) && i < len(y.Args); i++ {
				project(x.Args[i], y.Args[i])
			}
		}
		if x, y := a.GetList(), b.GetList(); x != nil && y != nil {
			for i := 0; i < len(x.List) && i < len(y.List); i++ {
				project(x.List[i], y.List[i])
			}
		}
		if x, y := a.GetLit(), b.GetLit(); x != nil && y != nil {
			project(x.Src, y.Src)
		}
	}
	project(stored, right)
	return sameTableDumpBoundExpression(stored, right)
}

// SQL-origin proof also recovers missing explicit-cast syntax markers from the
// AST before rewriting. DUMP uses only the coercibility-only historical candidate
// and continues to require exact syntax provenance.
func sameLegacyFloatOrigin(stored, candidate *planpb.Expr) bool {
	right := proto.Clone(candidate).(*planpb.Expr)
	var recoverMissingMarkers func(*planpb.Expr, *planpb.Expr)
	recoverMissingMarkers = func(old, bound *planpb.Expr) {
		if old == nil || bound == nil {
			return
		}
		omitHistoricalFloatCoercibility(old, bound)
		if a, b := old.GetF(), bound.GetF(); a != nil && b != nil && b.Func != nil {
			id, overload := function.DecodeOverloadID(b.Func.Obj)
			if id == function.CAST && overload == 0 && !a.SyntaxExplicitCast {
				b.SyntaxExplicitCast = false
			}
			for i := 0; i < len(a.Args) && i < len(b.Args); i++ {
				recoverMissingMarkers(a.Args[i], b.Args[i])
			}
		}
		if a, b := old.GetList(), bound.GetList(); a != nil && b != nil {
			for i := 0; i < len(a.List) && i < len(b.List); i++ {
				recoverMissingMarkers(a.List[i], b.List[i])
			}
		}
		if a, b := old.GetLit(), bound.GetLit(); a != nil && b != nil {
			recoverMissingMarkers(a.Src, b.Src)
		}
	}
	recoverMissingMarkers(stored, right)
	return sameTableDumpBoundExpression(stored, right)
}

func rebindLegacyFloatStringExpression(ctx CompilerContext, def *planpb.TableDef, item tableDumpExpression, budget *tableDumpBindingBudget) (*planpb.Expr, error) {
	budget.text += len(item.origin)
	if budget.text > tableDumpMaxExpressionText {
		return nil, moerr.NewInvalidInputNoCtx("catalog expression text exceeds limit")
	}
	for _, mode := range tableDumpParserModes() {
		if err := ctx.GetContext().Err(); err != nil {
			return nil, err
		}
		stmt, ast, visit, err := parseTableDumpExpression(ctx.GetContext(), item.origin, mode)
		if err != nil {
			continue
		}
		budget.nodes += visit.nodes
		if budget.nodes > tableDumpMaxExpressionNodes {
			stmt.Free()
			return nil, moerr.NewInvalidInputNoCtx("catalog expression AST exceeds limit")
		}
		maxIncrement := int64(0)
		if visit.hasDivision {
			maxIncrement = 30
		}
		for increment := int64(0); increment <= maxIncrement; increment++ {
			for _, legacySpecial := range []bool{false, true} {
				if err := ctx.GetContext().Err(); err != nil {
					stmt.Free()
					return nil, err
				}
				if legacySpecial && !visit.hasSpecial {
					continue
				}
				for _, legacyFormatting := range []bool{false, true} {
					if legacyFormatting && item.kind != "default" {
						continue
					}
					proofCtx := ctx
					if legacyFormatting {
						proofCtx = tableDumpBindContext{CompilerContext: ctx, ctx: function.WithLegacyFloatStringFormatting(ctx.GetContext()), proc: ctx.GetProcess()}
					}
					budget.visits += visit.nodes
					if budget.visits > tableDumpMaxCandidateVisits {
						stmt.Free()
						return nil, moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
					}
					old, err := bindFloatTableDumpExpression(proofCtx, def, item, ast, increment, legacySpecial, true)
					if err != nil || !sameLegacyFloatOrigin(item.expr, old) {
						continue
					}
					expected := DeepCopyExpr(old)
					rebindCtx := function.WithDivPrecisionIncrement(proofCtx.GetContext(), int32(increment))
					if legacySpecial {
						rebindCtx = function.WithLegacySpecialConsumers(rebindCtx)
					}
					changed, err := widenVerifiedFloatStringExpr(rebindCtx, expected, budget)
					if err != nil {
						stmt.Free()
						if errors.Is(err, errLegacyFloatStringRebind) {
							return nil, nil
						}
						return nil, err
					}
					if !changed {
						stmt.Free()
						return nil, nil
					}
					// Widening can newly require an assignment wrapper when the old
					// root exactly matched the declared column type. Existing wrappers
					// keep their declared type and must not be duplicated.
					if (item.kind == "generated" || item.kind == "default") && isSameColumnType(old.Typ, item.col.Typ) &&
						!isSameColumnType(expected.Typ, item.col.Typ) {
						expected, err = makePlan2AssignmentCastExpr(rebindCtx, expected, item.col.Typ)
						if err != nil {
							stmt.Free()
							return nil, err
						}
					}
					if item.kind == "default" {
						// DEFAULT binding folds constant coercions inside row-dependent
						// expressions too. Replay that owner after reconciling casts;
						// already-folded literal roots never enter this migration.
						if err := chargeFloatStringRebind(expected, budget); err != nil {
							stmt.Free()
							return nil, err
						}
						expected, err = ConstantFold(batch.EmptyForConstFoldBatch, expected, floatStringProofProcess(rebindCtx, ctx.GetProcess()), false, true)
						if err != nil {
							stmt.Free()
							return nil, err
						}
					}
					budget.visits += visit.nodes
					if budget.visits > tableDumpMaxCandidateVisits {
						stmt.Free()
						return nil, moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
					}
					mode := tableDumpBindingCurrent
					if legacySpecial {
						mode = tableDumpBindingLegacySpecial
					}
					if item.kind == "default" {
						mode |= tableDumpBindingFrozenFloatDefaults
						budget.visits += visit.nodes
						if budget.visits > tableDumpMaxCandidateVisits {
							stmt.Free()
							return nil, moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
						}
					}
					current, err := bindTableDumpExpression(proofCtx, def, item, ast, increment, mode, &tableDumpDefaultSession{budget: budget})
					stmt.Free()
					if err != nil {
						var limit *floatStringProofBudgetError
						if ctx.GetContext().Err() != nil {
							return nil, ctx.GetContext().Err()
						}
						if errors.As(err, &limit) {
							return nil, err
						}
						return nil, nil
					}
					if !sameTableDumpBoundExpression(expected, current) {
						return nil, nil
					}
					if err := ctx.GetContext().Err(); err != nil {
						return nil, err
					}
					return current, nil
				}
			}
		}
		stmt.Free()
	}
	return nil, ctx.GetContext().Err()
}

// An unsupported local reconstruction rejects the whole candidate, including
// repairs already made to other siblings. It is not a catalog execution error.
var errLegacyFloatStringRebind = moerr.NewInternalError(moerr.NoReportContext(), "cannot reconstruct legacy float string function")

func rebindFloatStringFunction(ctx context.Context, original *planpb.Expr, args []*planpb.Expr) (*planpb.Expr, error) {
	fn := original.GetF()
	// SQL-origin proof authorizes persisted internal argument layouts, while
	// public SQL keeps using its checked argument-count contract.
	if id, _ := function.DecodeOverloadID(fn.Func.Obj); id == function.FORMAT {
		ctx = function.WithLegacySpecialConsumers(ctx)
		// Catalog FORMAT authors VARCHAR(max) arguments even for existing
		// strings. Recreate that declaration before overload resolution;
		// ordinary typed binding narrows casts and loses this owner contract.
		varchar := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
		for i, arg := range args {
			var err error
			args[i], err = appendCastBeforeExpr(ctx, arg, varchar)
			if err != nil {
				return nil, err
			}
		}
	}
	bound, err := bindFuncExprImplByPlanExpr(ctx, fn.Func.ObjName, args, true, nil, nil, true)
	if err != nil {
		return nil, err
	}
	if id, _ := function.DecodeOverloadID(fn.Func.Obj); id == function.FORMAT {
		if err := preservePersistedFormatCompatibility(ctx, bound); err != nil {
			return nil, err
		}
	}
	// IFNULL's AST lowering owns this contract in addition to ordinary CASE
	// overload binding. Historical SQL-origin proof made this marker trusted.
	if original.PreparedNumeric != nil && original.PreparedNumeric.IfnullCommonValue {
		if fn.Func.ObjName != "case" || len(args) != 3 || bound.GetF() == nil {
			return nil, errLegacyFloatStringRebind
		}
		applyIfNullCommonValueContract(bound, bound.GetF().Args)
	}
	return bound, nil
}

// A one-element safe IN list lowers to equality, so its list-owned cast may
// survive in an '=' descendant of an expanded IN. Recover that binding owner
// only when it exactly reconstructs the historical node; keep the same owner
// for the current binding rather than widening comparison metadata by hand.
func rebindFloatStringInComparison(ctx context.Context, original *planpb.Expr, args []*planpb.Expr) (*planpb.Expr, error) {
	if len(args) != 2 {
		return nil, errLegacyFloatStringRebind
	}
	var name string
	switch original.GetF().Func.ObjName {
	case "=":
		name = "in"
	case "!=":
		name = "not_in"
	default:
		return nil, errLegacyFloatStringRebind
	}
	list := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_tuple)}, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: args[1:]}}}
	return bindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{args[0], list}, true, nil, nil, true)
}

// chargeFloatStringRebind bounds subtree copies and binder passes, including
// nested functions whose reconstruction costs O(nodes * depth).
func chargeFloatStringRebind(expr *planpb.Expr, budget *tableDumpBindingBudget) error {
	if expr == nil {
		return nil
	}
	budget.visits++
	if budget.visits > tableDumpMaxCandidateVisits {
		return &floatStringProofBudgetError{moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")}
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if err := chargeFloatStringRebind(arg, budget); err != nil {
				return err
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			if err := chargeFloatStringRebind(arg, budget); err != nil {
				return err
			}
		}
	}
	if lit := expr.GetLit(); lit != nil {
		return chargeFloatStringRebind(lit.Src, budget)
	}
	return nil
}

// Only implicit float conversions authorize a reconstruction. An unaffected
// sibling keeps its persisted overload, casts and internal argument layout.
// Every scan shares the candidate budget, including repeated ancestor scans.
func affectedFloatStringExpr(ctx context.Context, expr *planpb.Expr, budget *tableDumpBindingBudget) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if expr == nil || expr.GetLit() != nil {
		return false, nil
	}
	budget.visits++
	if budget.visits > tableDumpMaxCandidateVisits {
		return false, moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
	}
	if fn := expr.GetF(); fn != nil {
		if isLegacyFloatStringCast(expr) && !fn.SyntaxExplicitCast {
			return true, nil
		}
		for _, arg := range fn.Args {
			if affected, err := affectedFloatStringExpr(ctx, arg, budget); affected || err != nil {
				return affected, err
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			if affected, err := affectedFloatStringExpr(ctx, arg, budget); affected || err != nil {
				return affected, err
			}
		}
	}
	return false, nil
}

// Peel only a single implicit VARCHAR conversion owned by the current binder
// operation. User casts and value-producing functions remain boundaries.
func peelFloatStringArgument(arg *planpb.Expr) (*planpb.Expr, bool) {
	cast := arg.GetF()
	if cast == nil || cast.Func == nil || cast.SyntaxExplicitCast || len(cast.Args) != 2 ||
		cast.Args[0] == nil || cast.Args[1] == nil || cast.Args[1].GetT() == nil ||
		arg.Typ.Id != int32(types.T_varchar) || !proto.Equal(&arg.Typ, &cast.Args[1].Typ) {
		return arg, false
	}
	id, overload := function.DecodeOverloadID(cast.Func.Obj)
	if id != function.CAST || overload != 0 {
		return arg, false
	}
	return cast.Args[0], isLegacyFloatStringCast(arg)
}

// Reconcile affected functions through the binder, including sibling implicit
// casts and argument-sensitive metadata. Exact historical reconstruction proves
// ownership of peeled casts; the encoded overload and final SQL-bound tree must
// still match. Declared casts remain boundaries around repaired children.
func widenVerifiedFloatStringExpr(ctx context.Context, expr *planpb.Expr, budget *tableDumpBindingBudget) (bool, error) {
	if affected, err := affectedFloatStringExpr(ctx, expr, budget); !affected || err != nil {
		return false, err
	}
	fn := expr.GetF()
	if fn == nil {
		changed := false
		if list := expr.GetList(); list != nil {
			for _, entry := range list.List {
				child, err := widenVerifiedFloatStringExpr(ctx, entry, budget)
				if err != nil {
					return false, err
				}
				changed = changed || child
			}
		}
		return changed, nil
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if id == function.CAST || fn.Func.ObjName == "cast_assign" || fn.Func.ObjName == "cast_strict" || fn.Func.ObjName == "cast_ignore" {
		changed := false
		for _, arg := range fn.Args {
			child, err := widenVerifiedFloatStringExpr(ctx, arg, budget)
			if err != nil {
				return false, err
			}
			changed = changed || child
		}
		if isLegacyFloatStringCast(expr) && !fn.SyntaxExplicitCast {
			expr.Typ.Width = 34
			fn.Args[1].Typ.Width = 34
			return true, nil
		}
		return changed, nil
	}
	if err := chargeFloatStringRebind(expr, budget); err != nil {
		return false, err
	}
	args := DeepCopyExprList(fn.Args)
	changed := false
	for i, arg := range args {
		var legacy bool
		args[i], legacy = peelFloatStringArgument(arg)
		changed = changed || legacy
	}
	// IN owns the conversions of its immediate RHS elements. Leaving those
	// wrappers behind changes its safe-list partition after the left side widens.
	// Exact historical reconstruction below proves each peeled cast's owner.
	if (id == function.IN || id == function.NOT_IN) && len(args) == 2 {
		if list := args[1].GetList(); list != nil {
			budget.visits += len(list.List)
			if budget.visits > tableDumpMaxCandidateVisits {
				return false, moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
			}
			for i, entry := range list.List {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				var legacy bool
				list.List[i], legacy = peelFloatStringArgument(entry)
				changed = changed || legacy
			}
		}
	}

	if id == function.FORMAT {
		if err := chargeFloatStringRebind(expr, budget); err != nil {
			return false, err
		}
	}
	old, err := rebindFloatStringFunction(function.WithLegacyFloatStringBounds(ctx), expr, DeepCopyExprList(args))
	inComparison := false
	if err != nil || !sameTableDumpBoundExpression(expr, old) {
		if len(args) != 2 || (fn.Func.ObjName != "=" && fn.Func.ObjName != "!=") {
			return false, errLegacyFloatStringRebind
		}
		if err := chargeFloatStringRebind(expr, budget); err != nil {
			return false, err
		}
		old, err = rebindFloatStringInComparison(function.WithLegacyFloatStringBounds(ctx), expr, DeepCopyExprList(args))
		if err != nil || !sameTableDumpBoundExpression(expr, old) {
			return false, errLegacyFloatStringRebind
		}
		inComparison = true
	}
	for _, arg := range args {
		child, err := widenVerifiedFloatStringExpr(ctx, arg, budget)
		if err != nil {
			return false, err
		}
		changed = changed || child
	}
	if !changed {
		return false, nil
	}
	for _, arg := range args {
		if err := chargeFloatStringRebind(arg, budget); err != nil {
			return false, err
		}
	}
	if id == function.FORMAT {
		for _, arg := range args {
			if err := chargeFloatStringRebind(arg, budget); err != nil {
				return false, err
			}
		}
	}
	var current *planpb.Expr
	if inComparison {
		current, err = rebindFloatStringInComparison(ctx, expr, args)
	} else {
		current, err = rebindFloatStringFunction(ctx, expr, args)
	}
	if err != nil || current.GetF() == nil || current.GetF().Func == nil || current.GetF().Func.Obj != fn.Func.Obj {
		return false, errLegacyFloatStringRebind
	}
	expr.Typ = current.Typ
	expr.Expr = current.Expr
	return true, nil
}

// floatStringProofProcess changes only the borrowed evaluator's context and
// diagnostics. No context mode is installed on the caller or persisted tree;
// ordinary execution always uses current formatting. The child owns no Base.
func floatStringProofProcess(ctx context.Context, proc *process.Process) *process.Process {
	child := proc.NewNoContextChildProc(0)
	child.Ctx = proc.Ctx
	if function.LegacyFloatStringFormatting(ctx) {
		child.Ctx = function.WithLegacyFloatStringFormatting(child.Ctx)
	}
	child.WarningSink = &process.WarningProbe{}
	return child
}

// Preserve the public error while distinguishing fatal proof limits from an unavailable binder era.
type floatStringProofBudgetError struct{ error }

func (e *floatStringProofBudgetError) Unwrap() error { return e.error }
