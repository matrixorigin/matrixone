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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
		if col.Default != nil && col.Default.Expr != nil && col.Default.Expr.GetLit() == nil {
			migrateLegacyHexExpr(col.Default.Expr)
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
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		functionID, overloadID := function.DecodeOverloadID(fn.GetFunc().GetObj())
		if functionID == function.HEX && overloadID >= function.HexMySQLNumericOverloadStart {
			return true
		}
		for _, arg := range fn.Args {
			if exprContainsNewHexOverload(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprContainsNewHexOverload(item) {
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
