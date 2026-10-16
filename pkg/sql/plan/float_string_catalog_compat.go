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
	"slices"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
				changed, err := widenVerifiedFloatStringExpr(ctx.GetContext(), expected)
				if err != nil {
					stmt.Free()
					return nil, err
				}
				if !changed {
					stmt.Free()
					return nil, nil
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

// Recompute only derived string metadata with the SAME execution overload.
// Numeric precision, explicit/assignment casts, literals and references remain
// authoritative. Comparing this result to a fresh candidate closes the delta.
func widenVerifiedFloatStringExpr(ctx context.Context, expr *planpb.Expr) (bool, error) {
	if expr == nil || expr.GetLit() != nil {
		return false, nil
	}
	fn := expr.GetF()
	if fn == nil {
		changed := false
		if list := expr.GetList(); list != nil {
			for _, entry := range list.List {
				child, err := widenVerifiedFloatStringExpr(ctx, entry)
				if err != nil {
					return false, err
				}
				changed = changed || child
			}
		}
		return changed, nil
	}
	changed := false
	for _, arg := range fn.Args {
		child, err := widenVerifiedFloatStringExpr(ctx, arg)
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
	if changed && types.T(expr.Typ.Id).IsMySQLString() {
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		// These are declared boundaries, not derived string widths.
		if id != function.CAST && fn.Func.ObjName != "cast_assign" && fn.Func.ObjName != "cast_strict" && fn.Func.ObjName != "cast_ignore" {
			ov, err := function.GetFunctionById(ctx, fn.Func.Obj)
			if err != nil {
				return false, err
			}
			args := make([]types.Type, len(fn.Args))
			for i, arg := range fn.Args {
				args[i] = makeTypeByPlan2Expr(arg)
			}
			result := ov.GetReturnTypeMethod()(args)
			expr.Typ.Id = int32(result.Oid)
			expr.Typ.Width = result.Width
			expr.Typ.Scale = result.Scale
		}
	}
	return changed, nil
}
