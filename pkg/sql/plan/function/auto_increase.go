// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func builtInInternalAutoIncrement(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[uint64](result)

	eng := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	observationCtx := incrservice.WithAutoIDObservationScope(proc.Ctx)
	for i := uint64(0); i < uint64(length); i++ {
		s1, null1 := p1.GetStrValue(i)
		s2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			return moerr.NewInvalidInput(proc.Ctx, "unsupported parameter `null` for internal_auto_increment")
		}

		// Database/Relation retain these names in the transaction cache. SHOW
		// TABLE STATUS reuses that cache after this projection's vectors are
		// released, so the names must not borrow the input vector storage.
		dbName := string(s1)
		tableName := string(s2)

		database, err := eng.Database(proc.Ctx, dbName, proc.GetTxnOperator())
		if err != nil {
			return moerr.NewInvalidInputf(proc.Ctx, "Database '%s' does not exist", dbName)
		}
		// INFORMATION_SCHEMA.TABLES also projects this value for the private
		// fork ledger. Its schema has no auto-increment column.
		if strings.EqualFold(dbName, catalog.MO_CATALOG) && strings.EqualFold(tableName, catalog.MO_BRANCH_FORK_KEYS) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		relation, err := database.Relation(proc.Ctx, tableName, nil)
		if err != nil {
			return moerr.NewInvalidInputf(proc.Ctx, "Table '%s' does not exist in database '%s'", tableName, dbName)
		}
		tableDef := relation.GetTableDef(proc.Ctx)
		if err = process.CheckIndexRelationDefinitionAccess(proc, relation.GetTableName(), tableDef); err != nil {
			return err
		}
		autoIncrCol := getTableAutoIncrCol(tableDef)
		if autoIncrCol != "" {
			tableID := relation.GetTableID(proc.Ctx)
			ctx := observationCtx
			if extra := relation.GetExtraInfo(); extra != nil {
				ctx = incrservice.WithAutoIDCachePolicy(ctx, tableID, extra.AutoIdCache)
			}
			autoIncrement, err := getCurrentValue(
				ctx,
				proc.GetService(),
				tableID,
				autoIncrCol,
			)
			if err != nil {
				return err
			}
			if err = rs.Append(autoIncrement, false); err != nil {
				return err
			}
		} else {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func getTableAutoIncrCol(tableDef *plan.TableDef) string {
	if tableDef.GetViewSql().GetView() != "" {
		return ""
	}
	for _, col := range tableDef.Cols {
		if col == nil || !col.Typ.AutoIncr {
			continue
		}
		name := col.GetOriginCaseName()
		if name == catalog.Row_ID || (col.Hidden && catalog.IsFakePkName(name)) {
			continue
		}
		// Allocator rows are keyed by the planner column name.
		return col.Name
	}
	return ""
}

func getCurrentValue(
	ctx context.Context,
	sid string,
	tableID uint64,
	col string) (uint64, error) {
	return incrservice.GetAutoIncrementService(sid).CurrentValue(
		ctx,
		tableID,
		col,
	)
}
