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
	"strings"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
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
	for _, col := range tableDef.Cols {
		if col == nil {
			continue
		}
		if col.GeneratedCol != nil {
			migrateLegacyHexExpr(col.GeneratedCol.Expr)
		}
		if col.OnUpdate != nil {
			migrateLegacyHexExpr(col.OnUpdate.Expr)
		}
		if col.Default == nil || col.Default.Expr == nil {
			continue
		}
		if col.Default.Expr.GetLit() == nil {
			migrateLegacyHexExpr(col.Default.Expr)
			continue
		}
		if err := rebuildFoldedHexDefault(proc, col); err != nil {
			return err
		}
	}
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
	return ok && valid && version >= defines.MORPCVersion64
}

func rebuildFoldedHexDefault(proc *process.Process, col *plan.ColDef) error {
	origin := col.Default.OriginString
	if !strings.Contains(strings.ToLower(origin), "hex") {
		return nil
	}
	stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, "select "+origin, 1)
	if err != nil {
		return nil
	}
	defer stmt.Free()
	selectStmt, ok := stmt.(*tree.Select)
	if !ok {
		return nil
	}
	clause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || len(clause.Exprs) != 1 {
		return nil
	}

	binder := NewDefaultBinder(proc.Ctx, nil, nil, col.Typ, nil)
	bound, err := binder.BindExpr(unwrapParenExpr(clause.Exprs[0].Expr), 0, false)
	if err != nil {
		return nil
	}
	if !exprContainsHex(bound) {
		return nil
	}
	if err = preservePersistedFormatCompatibility(proc.Ctx, bound); err != nil {
		return err
	}
	assigned, err := makePlan2AssignmentCastExpr(proc.Ctx, bound, col.Typ)
	if err != nil {
		return err
	}
	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(assigned), proc, false, true)
	if err != nil {
		return mapDDLAssignmentCastError(proc.Ctx, col.Typ, col.Name, err)
	}
	col.Default.Expr = folded
	return nil
}

func exprContainsHex(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		functionID, _ := function.DecodeOverloadID(fn.GetFunc().GetObj())
		if functionID == function.HEX {
			return true
		}
		for _, arg := range fn.Args {
			if exprContainsHex(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprContainsHex(item) {
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
