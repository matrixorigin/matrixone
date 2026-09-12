// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MigrateLegacyHexTableDef upgrades the execution-owned TableDef returned by a
// catalog resolver. It runs before the planner can fold or copy expressions
// into a physical generation and never mutates catalog storage.
func MigrateLegacyHexTableDef(proc *process.Process, tableDef *plan.TableDef) error {
	if !supportsHexMySQLNumericProtocol(proc) || tableDef == nil {
		return nil
	}
	for _, check := range tableDef.Checks {
		if check != nil {
			migrateLegacyHexExpr(check.Check)
		}
	}
	for i, col := range tableDef.Cols {
		if col == nil {
			continue
		}
		if col.GeneratedCol != nil {
			migrateLegacyHexExpr(col.GeneratedCol.Expr)
		}
		if col.OnUpdate != nil {
			migrateLegacyHexExpr(col.OnUpdate.Expr)
		}
		if col.Default != nil && col.Default.Expr != nil {
			if col.Default.Expr.GetLit() == nil {
				migrateLegacyHexExpr(col.Default.Expr)
			} else {
				owned := *col
				if err := migrateFoldedHexDefault(proc, &owned); err != nil {
					return err
				}
				if owned.Default != col.Default {
					tableDef.Cols[i] = &owned
				}
			}
		}
	}
	return nil
}

// This deliberately small grammar contains no strings, operators, REAL alias,
// names or functions whose meaning can depend on creation-time SQL mode.
// Parsing arbitrary OriginString with today's SQL mode is not safe.
var unambiguousFoldedHex = regexp.MustCompile(`(?i)^hex\s*\(\s*(true|false|cast\s*\(\s*[+-]?\s*[0-9]+(?:\.[0-9]+)?\s+as\s+(double|decimal(?:\s*\(\s*[0-9]+\s*(?:,\s*[0-9]+\s*)?\))?)\s*\))\s*\)$`)

func migrateFoldedHexDefault(proc *process.Process, col *plan.ColDef) error {
	stored := col.Default.Expr.GetLit()
	// New defaults retain their resolved expression, including CAST type and
	// overload identity. Their folded value is already in the new semantics.
	if stored.Src != nil {
		return nil
	}
	origin := strings.TrimSpace(col.Default.OriginString)
	if strings.HasPrefix(origin, "(") && strings.HasSuffix(origin, ")") {
		origin = strings.TrimSpace(origin[1 : len(origin)-1])
	}
	if !unambiguousFoldedHex.MatchString(origin) {
		return nil
	}
	stmt, err := mysql.ParseOne(proc.Ctx, "select "+origin, 1)
	if err != nil {
		return nil
	}
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	bound, err := NewDefaultBinder(proc.Ctx, nil, nil, plan.Type{Id: int32(types.T_any)}, nil).BindExpr(ast, 0, false)
	if err != nil {
		return nil
	}
	legacy := DeepCopyExpr(bound)
	fn := legacy.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 1 {
		return nil
	}
	id, overload := function.DecodeOverloadID(fn.Func.Obj)
	if id != function.HEX {
		return nil
	}
	switch overload {
	case function.HexExplicitFloat64Overload:
		fn.Func.Obj = function.EncodeOverloadID(function.HEX, 5)
	case 8, 9, 10:
		fn.Args[0], err = makePlan2CastExpr(proc.Ctx, fn.Args[0], plan.Type{Id: int32(types.T_float64)})
		fn.Func.Obj = function.EncodeOverloadID(function.HEX, 5)
	case 2:
		// The accepted BOOL syntax has no user CAST; unwrap the new numeric
		// coercion and reconstruct the old BOOL-to-VARCHAR overload choice.
		arg := fn.Args[0]
		if cast := arg.GetF(); cast != nil && len(cast.Args) > 0 {
			arg = cast.Args[0]
		}
		if types.T(arg.Typ.Id) != types.T_bool {
			return nil
		}
		fn.Args[0], err = makePlan2CastExpr(proc.Ctx, arg, plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen})
		fn.Func.Obj = function.EncodeOverloadID(function.HEX, 0)
	default:
		return nil
	}
	if err != nil {
		return nil
	}
	fold := func(expr *plan.Expr) (*plan.Expr, error) {
		assigned, err := makePlan2AssignmentCastExpr(proc.Ctx, expr, col.Typ)
		if err != nil {
			return nil, err
		}
		return ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(assigned), proc, false, true)
	}
	oldValue, err := fold(legacy)
	if err != nil || oldValue.GetLit() == nil {
		return nil
	}
	old := oldValue.GetLit()
	if old.Isnull != stored.Isnull || !reflect.DeepEqual(old.Value, stored.Value) {
		// Not a demonstrated legacy product: never overwrite catalog data on
		// the strength of display SQL alone.
		return nil
	}
	newValue, err := fold(bound)
	if err != nil {
		return err
	}
	if lit := newValue.GetLit(); lit != nil {
		lit.Src = DeepCopyExpr(bound)
	}
	// CloneTableDefForPlan only clones the column slice. Detach the column
	// and default before publishing a replacement on the execution copy.
	col.Default = &plan.Default{Expr: newValue, OriginString: col.Default.OriginString,
		NullAbility: col.Default.NullAbility}
	return nil
}

func supportsHexMySQLNumericProtocol(proc *process.Process) bool {
	if proc == nil {
		return false
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	return ok && valid && version >= defines.MORPCVersion65
}

func requireHexMySQLNumericProtocol(proc *process.Process, expr *plan.Expr) error {
	if supportsHexMySQLNumericProtocol(proc) || !exprContainsNewHexOverload(expr) {
		return nil
	}
	return moerr.NewNotSupportedNoCtxf(
		"MySQL numeric HEX semantics require all CNs to support MORPC protocol version %d",
		defines.MORPCVersion65)
}

func exprContainsNewHexOverload(expr *plan.Expr) bool {
	return exprContainsHexOverload(expr, function.HexMySQLNumericOverloadStart)
}

func exprContainsHexOverload(expr *plan.Expr, minimum int32) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		functionID, overloadID := function.DecodeOverloadID(fn.GetFunc().GetObj())
		if functionID == function.HEX && overloadID >= minimum {
			return true
		}
		for _, arg := range fn.Args {
			if exprContainsHexOverload(arg, minimum) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprContainsHexOverload(item, minimum) {
				return true
			}
		}
	}
	return false
}

func migrateLegacyHexExpr(expr *plan.Expr) {
	if expr == nil {
		return
	}
	function.MigrateLegacyHexOverload(expr)
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			migrateLegacyHexExpr(arg)
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			migrateLegacyHexExpr(item)
		}
	}
}
