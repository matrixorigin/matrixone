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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// XXX confused function.
func builtInInternalAutoIncrement(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[uint64](result)

	eng := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	for i := uint64(0); i < uint64(length); i++ {
		s1, null1 := p1.GetStrValue(i)
		s2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			return moerr.NewInvalidInput(proc.Ctx, "unsupported parameter `null` for internal_auto_increment")
		}

		dbName := functionUtil.QuickBytesToStr(s1)
		tableName := functionUtil.QuickBytesToStr(s2)

		database, err := eng.Database(proc.Ctx, dbName, proc.TxnOperator)
		if err != nil {
			return moerr.NewInvalidInput(proc.Ctx, "Database '%s' does not exist", dbName)
		}
		relation, err := database.Relation(proc.Ctx, tableName, nil)
		if err != nil {
			return moerr.NewInvalidInput(proc.Ctx, "Table '%s' does not exist in database '%s'", tableName, dbName)
		}
		tableId := relation.GetTableID(proc.Ctx)
		engineDefs, err := relation.TableDefs(proc.Ctx)
		if err != nil {
			return err
		}
		autoIncrCol := getTableAutoIncrCol(engineDefs, tableName)
		if autoIncrCol != "" {
			autoIncrement, err := getCurrentValue(
				proc.Ctx,
				tableId,
				autoIncrCol)
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

func getTableAutoIncrCol(
	engineDefs []engine.TableDef,
	tableName string) string {
	for _, def := range engineDefs {
		// FIXME: more than one auto cols??
		if attr, ok := def.(*engine.AttributeDef); ok && attr.Attr.AutoIncrement {
			return attr.Attr.Name
		}
	}
	return ""
}

func getCurrentValue(
	ctx context.Context,
	tableID uint64,
	col string) (uint64, error) {
	return incrservice.GetAutoIncrementService().CurrentValue(
		ctx,
		tableID,
		col)
}
