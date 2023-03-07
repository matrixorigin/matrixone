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

package inside

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// InternalAutoIncrement is the internal system function Implementation of 'internal_auto_increment',
// 'internal_auto_increment' is used to obtain the current auto_increment column value of the table under the specified database
func InternalAutoIncrement(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtyp := types.T_uint64.ToType()
	isAllConst := true
	for i := range ivecs {
		if ivecs[i].IsConstNull() {
			return vector.NewConstNull(rtyp, 1, proc.Mp()), nil
		} else if !ivecs[i].IsConst() {
			isAllConst = false
			break
		}
	}

	eng := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	// New returns a TxnOperator to handle read and write operation for a transaction.
	txnOperator, err := proc.TxnClient.New()
	if err != nil {
		return nil, err
	}
	defer txnOperator.Rollback(proc.Ctx)
	if err := eng.New(proc.Ctx, txnOperator); err != nil {
		return nil, err
	}
	defer eng.Rollback(proc.Ctx, txnOperator)
	if isAllConst {
		var rvec *vector.Vector
		dbName := ivecs[0].GetStringAt(0)
		database, err := eng.Database(proc.Ctx, dbName, txnOperator)
		if err != nil {
			return nil, moerr.NewInvalidInput(proc.Ctx, "Database '%s' does not exist", dbName)
		}
		tableName := ivecs[1].GetStringAt(0)
		relation, err := database.Relation(proc.Ctx, tableName)
		if err != nil {
			return nil, moerr.NewInvalidInput(proc.Ctx, "Table '%s' does not exist in database '%s'", tableName, dbName)
		}
		tableId := relation.GetTableID(proc.Ctx)
		engineDefs, err := relation.TableDefs(proc.Ctx)
		if err != nil {
			return nil, err
		}
		hasAntoIncr, autoIncrCol := getTableAutoIncrCol(engineDefs, tableName)
		if hasAntoIncr {
			colname := fmt.Sprintf("%d_%s", tableId, autoIncrCol.Name)
			autoIncrement, err := getCurrentAutoIncrement(eng, proc, colname, dbName, tableName)
			if err != nil {
				return nil, err // or return 0, nil
			}
			rvec = vector.NewConstFixed(rtyp, autoIncrement, 1, proc.Mp())
		} else {
			rvec = vector.NewConstFixed(rtyp, uint64(0), 1, proc.Mp())
		}
		return rvec, nil
	} else {
		rowCount := ivecs[0].Length()
		rvec, err := proc.AllocVectorOfRows(rtyp, rowCount, ivecs[1].GetNulls())
		if err != nil {
			return nil, err
		}
		resValues := vector.MustFixedCol[uint64](rvec)

		for i := 0; i < rowCount; i++ {
			dbName := ivecs[0].GetStringAt(i)
			database, err := eng.Database(proc.Ctx, dbName, txnOperator)
			if err != nil {
				return nil, moerr.NewInvalidInput(proc.Ctx, "Database '%s' does not exist", dbName)
			}

			tableName := ivecs[1].GetStringAt(i)
			relation, err := database.Relation(proc.Ctx, tableName)
			if err != nil {
				return nil, moerr.NewInvalidInput(proc.Ctx, "Table '%s' does not exist in database '%s'", tableName, dbName)
			}
			tableId := relation.GetTableID(proc.Ctx)
			engineDefs, err := relation.TableDefs(proc.Ctx)
			if err != nil {
				return nil, err
			}
			hasAntoIncr, autoIncrCol := getTableAutoIncrCol(engineDefs, tableName)
			if hasAntoIncr {
				colname := fmt.Sprintf("%d_%s", tableId, autoIncrCol.Name)
				autoIncrement, err := getCurrentAutoIncrement(eng, proc, colname, dbName, tableName)
				if err != nil {
					return nil, err // or return 0, nil
				}
				resValues[i] = autoIncrement
			} else {
				nulls.Add(rvec.GetNulls(), uint64(i))
			}
		}
		return rvec, nil
	}
}
