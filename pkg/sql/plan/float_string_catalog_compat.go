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
)

// MigrateLegacyFloatStringTableDef repairs execution-owned catalog expressions.
// SQL-origin proof distinguishes implicit casts from explicit CHAR(32), including
// catalogs written before explicit-cast provenance was recorded. Catalog storage
// and folded DEFAULT values are never rewritten.
func MigrateLegacyFloatStringTableDef(ctx CompilerContext, def *planpb.TableDef) error {
	if ctx == nil || def == nil {
		return nil
	}
	budget := &tableDumpBindingBudget{}
	for _, item := range persistedTableExpressions(def, false) {
		if !hasLegacyFloatStringCast(item.expr) {
			continue
		}
		rebound, err := rebindLegacyFloatStringExpression(ctx, def, item, budget)
		if err != nil {
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
	return nil
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

// Only this origin-proof comparison tolerates missing syntax markers. The
// AST-bound candidate supplies authoritative markers before any rewrite.
// Dump payload verification continues to compare provenance exactly.
func sameLegacyFloatOrigin(stored, candidate *planpb.Expr) bool {
	right := proto.Clone(candidate).(*planpb.Expr)
	var recoverMissingMarkers func(*planpb.Expr, *planpb.Expr)
	recoverMissingMarkers = func(old, bound *planpb.Expr) {
		if old == nil || bound == nil {
			return
		}
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
				if legacySpecial && !visit.hasSpecial {
					continue
				}
				budget.visits += visit.nodes
				if budget.visits > tableDumpMaxCandidateVisits {
					stmt.Free()
					return nil, moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
				}
				old, err := bindFloatTableDumpExpression(ctx, def, item, ast, increment, legacySpecial, true)
				if err != nil || !sameLegacyFloatOrigin(item.expr, old) {
					continue
				}
				expected := DeepCopyExpr(old)
				rebindCtx := function.WithDivPrecisionIncrement(ctx.GetContext(), int32(increment))
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
					expected, err = ConstantFold(batch.EmptyForConstFoldBatch, expected, ctx.GetProcess(), false, true)
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
				current, err := bindFloatTableDumpExpression(ctx, def, item, ast, increment, legacySpecial, false)
				stmt.Free()
				if err != nil || !sameTableDumpBoundExpression(expected, current) {
					// A different binder change is not authorized by this compatibility repair.
					return nil, nil
				}
				return current, nil
			}
		}
		stmt.Free()
	}
	return nil, nil
}

// An unsupported local reconstruction rejects the whole candidate, including
// repairs already made to other siblings. It is not a catalog execution error.
var errLegacyFloatStringRebind = moerr.NewInternalError(moerr.NoReportContext(), "cannot reconstruct legacy float string function")

func rebindFloatStringFunction(ctx context.Context, original *planpb.Expr, args []*planpb.Expr) (*planpb.Expr, error) {
	fn := original.GetF()
	bound, err := BindFuncExprImplByPlanExpr(ctx, fn.Func.ObjName, args)
	if err != nil {
		return nil, err
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

// chargeFloatStringRebind bounds subtree copies and binder passes, including
// nested functions whose reconstruction costs O(nodes * depth).
func chargeFloatStringRebind(expr *planpb.Expr, budget *tableDumpBindingBudget) error {
	if expr == nil {
		return nil
	}
	budget.visits++
	if budget.visits > tableDumpMaxCandidateVisits {
		return moerr.NewInvalidInputNoCtx("catalog expression binding exceeds limit")
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

// Reconcile affected functions through the binder, including sibling implicit
// casts and argument-sensitive metadata. Exact historical reconstruction proves
// ownership of peeled casts; the encoded overload and final SQL-bound tree must
// still match. Declared casts remain boundaries around repaired children.
func widenVerifiedFloatStringExpr(ctx context.Context, expr *planpb.Expr, budget *tableDumpBindingBudget) (bool, error) {
	if expr == nil || expr.GetLit() != nil {
		return false, nil
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
		cast := arg.GetF()
		if cast == nil || cast.Func == nil || cast.SyntaxExplicitCast || len(cast.Args) != 2 ||
			cast.Args[0] == nil || cast.Args[1] == nil || cast.Args[1].GetT() == nil ||
			arg.Typ.Id != int32(types.T_varchar) || !proto.Equal(&arg.Typ, &cast.Args[1].Typ) {
			continue
		}
		castID, overload := function.DecodeOverloadID(cast.Func.Obj)
		if castID == function.CAST && overload == 0 {
			// A direct numeric branch can own the only legacy conversion.
			changed = changed || isLegacyFloatStringCast(arg)
			args[i] = cast.Args[0]
		}
	}
	old, err := rebindFloatStringFunction(function.WithLegacyFloatStringBounds(ctx), expr, DeepCopyExprList(args))
	if err != nil || !sameTableDumpBoundExpression(expr, old) {
		return false, errLegacyFloatStringRebind
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
	current, err := rebindFloatStringFunction(ctx, expr, args)
	if err != nil || current.GetF() == nil || current.GetF().Func == nil || current.GetF().Func.Obj != fn.Func.Obj {
		return false, errLegacyFloatStringRebind
	}
	expr.Typ = current.Typ
	expr.Expr = current.Expr
	return true, nil
}
