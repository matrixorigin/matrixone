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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MigrateLegacyCatalogTableDef normalizes execution-owned catalog expressions
// before binding consumers. Catalog storage and its shared nested objects are
// immutable; individual migrations must detach anything they change.
func MigrateLegacyCatalogTableDef(proc *process.Process, tableDef *plan.TableDef) error {
	if err := MigrateLegacyHexTableDef(proc, tableDef); err != nil {
		return err
	}
	if tableDef == nil {
		return nil
	}
	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	checksOwned, colsOwned := false, false
	for i, check := range tableDef.Checks {
		if check == nil {
			continue
		}
		expr, changed, err := normalizeCatalogDecimalExpr(ctx, check.Check)
		if err != nil {
			return err
		}
		if changed {
			if !checksOwned {
				tableDef.Checks = slices.Clone(tableDef.Checks)
				checksOwned = true
			}
			owned := *check
			owned.Check = expr
			tableDef.Checks[i] = &owned
		}
	}
	for i, col := range tableDef.Cols {
		if col == nil {
			continue
		}
		owned := *col
		changed := false
		if col.Default != nil {
			expr, rewritten, err := normalizeCatalogDecimalExpr(ctx, col.Default.Expr)
			if err != nil {
				return err
			}
			if rewritten {
				def := *col.Default
				def.Expr = expr
				owned.Default = &def
				changed = true
			}
		}
		if col.OnUpdate != nil {
			expr, rewritten, err := normalizeCatalogDecimalExpr(ctx, col.OnUpdate.Expr)
			if err != nil {
				return err
			}
			if rewritten {
				update := *col.OnUpdate
				update.Expr = expr
				owned.OnUpdate = &update
				changed = true
			}
		}
		if col.GeneratedCol != nil {
			expr, rewritten, err := normalizeCatalogDecimalExpr(ctx, col.GeneratedCol.Expr)
			if err != nil {
				return err
			}
			if rewritten {
				generated := *col.GeneratedCol
				generated.Expr = expr
				owned.GeneratedCol = &generated
				changed = true
			}
		}
		if changed {
			if !colsOwned {
				tableDef.Cols = slices.Clone(tableDef.Cols)
				colsOwned = true
			}
			tableDef.Cols[i] = &owned
		}
	}
	return nil
}

// Only the operator that synthesized a coercion may remove it. In particular,
// a top-level column assignment and an explicit CAST keep their target domains.
func catalogDecimalOperator(name string, arity int) bool {
	if arity != 2 {
		return false
	}
	switch name {
	case "=", "<=>", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "div", "%", "mod":
		return true
	default:
		return false
	}
}

func catalogImplicitDecimalSource(expr *plan.Expr) *plan.Expr {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.SyntaxExplicitCast ||
		fn.Func.ObjName != "cast" || len(fn.Args) != 2 ||
		!types.T(expr.Typ.Id).IsDecimal() {
		return nil
	}
	fid, overload := function.DecodeOverloadID(fn.Func.Obj)
	if fid != function.CAST || overload != 0 {
		return nil
	}
	return fn.Args[0]
}

func insufficientCatalogIntegerCast(expr *plan.Expr) bool {
	source := catalogImplicitDecimalSource(expr)
	if source == nil || expr.Typ.Width <= 0 {
		return false
	}
	var digits int32
	switch types.T(source.Typ.Id) {
	case types.T_int8, types.T_uint8:
		digits = 3
	case types.T_int16, types.T_uint16:
		digits = 5
	case types.T_int32, types.T_uint32:
		digits = 10
	case types.T_int64:
		digits = 19
	case types.T_uint64, types.T_bit:
		digits = 20
	default:
		return false
	}
	return digits > expr.Typ.Width-expr.Typ.Scale
}

// normalizeCatalogDecimalExpr is copy-on-write and structural: do not reparse
// OriginString using today's SQL mode or reevaluate folded literal provenance.
// Repair an insufficient implicit integer domain in its numeric operator, then
// resolve type-dependent ancestors (e.g. ABS) rather than leaving stale overloads.
func normalizeCatalogDecimalExpr(ctx context.Context, expr *plan.Expr) (*plan.Expr, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return expr, false, nil
	}
	var ownedArgs []*plan.Expr
	typeChanged := false
	for i, arg := range fn.Args {
		owned, changed, err := normalizeCatalogDecimalExpr(ctx, arg)
		if err != nil {
			return nil, false, err
		}
		if changed {
			if ownedArgs == nil {
				ownedArgs = slices.Clone(fn.Args)
			}
			ownedArgs[i] = owned
			typeChanged = typeChanged || arg.Typ.Id != owned.Typ.Id || arg.Typ.Width != owned.Typ.Width || arg.Typ.Scale != owned.Typ.Scale
		}
	}
	args := fn.Args
	if ownedArgs != nil {
		args = ownedArgs
	}
	operator := catalogDecimalOperator(fn.Func.ObjName, len(args))
	needsDomainRepair := false
	if operator {
		for _, arg := range args {
			needsDomainRepair = needsDomainRepair || insufficientCatalogIntegerCast(arg)
		}
	}
	if ownedArgs == nil && !needsDomainRepair {
		return expr, false, nil
	}
	if operator && (needsDomainRepair || ownedArgs != nil) {
		args = slices.Clone(args)
		for i, arg := range args {
			if source := catalogImplicitDecimalSource(arg); source != nil {
				args[i] = source
			}
		}
		typeChanged = true
	}
	owned := *expr
	if !typeChanged || fn.Func.ObjName == "cast" || fn.Func.ObjName == "cast_strict" || fn.Func.ObjName == "cast_assign" || fn.Func.ObjName == "cast_assign_ignore" {
		// Assignment/explicit casts retain both their overload and destination.
		copyFn := *fn
		copyFn.Args = args
		owned.Expr = &plan.Expr_F{F: &copyFn}
		return &owned, true, nil
	}
	// Binding may normalize argument metadata: detach even unchanged siblings.
	for i, arg := range args {
		if i == 0 {
			args = slices.Clone(args)
		}
		args[i] = DeepCopyExpr(arg)
	}
	rebound, err := BindFuncExprImplByPlanExpr(ctx, fn.Func.ObjName, args)
	if err != nil {
		return nil, false, err
	}
	owned.Typ, owned.Expr = rebound.Typ, rebound.Expr
	if newFn := owned.GetF(); newFn != nil {
		newFn.AggConfig, newFn.AggConfigType = fn.AggConfig, fn.AggConfigType
	}
	return &owned, true, nil
}
