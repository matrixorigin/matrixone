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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// internal_auto_increment
func InternalAutoIncrement(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rowCount := vector.Length(vectors[0])
	resultType := types.T_uint64.ToType()

	resVector, err := proc.AllocVectorOfRows(resultType, int64(rowCount), vectors[1].Nsp)
	if err != nil {
		return nil, err
	}
	resValues := vector.MustTCols[uint64](resVector)

	dbs := vector.MustStrCols(vectors[0])
	tables := vector.MustStrCols(vectors[1])

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
	for i := 0; i < rowCount; i++ {
		database, err := eng.Database(proc.Ctx, dbs[i], txnOperator)
		if err != nil {
			return nil, err
		}
		relation, err := database.Relation(proc.Ctx, tables[i])
		if err != nil {
			return nil, err
		}
		//tableId := relation.GetTableID(proc.Ctx)
		engineDefs, err := relation.TableDefs(proc.Ctx)
		if err != nil {
			return nil, err
		}
		hasAntoIncr, autoIncrCol := getTableAutoIncrCol(engineDefs, tables[i])
		if hasAntoIncr {
			autoIncrement, err := GetCurrentAutoIncrement(eng, proc, autoIncrCol, dbs[i], tables[i])
			if err != nil {
				return nil, err // or return 0, nil
			}
			resValues = append(resValues, autoIncrement)
		} else {
			nulls.Add(resVector.Nsp, uint64(i))
		}
	}
	return resVector, nil
}

func getTableAutoIncrCol(engineDefs []engine.TableDef, tableName string) (bool, *plan.ColDef) {
	if engineDefs != nil {
		for _, def := range engineDefs {
			if attr, ok := def.(*engine.AttributeDef); ok && attr.Attr.AutoIncrement {
				autoIncrCol := &plan.ColDef{
					ColId: attr.Attr.ID,
					Name:  attr.Attr.Name,
					Typ: &plan.Type{
						Id:          int32(attr.Attr.Type.Oid),
						Width:       attr.Attr.Type.Width,
						Precision:   attr.Attr.Type.Precision,
						Scale:       attr.Attr.Type.Scale,
						AutoIncr:    attr.Attr.AutoIncrement,
						Table:       tableName,
						NotNullable: attr.Attr.Default != nil && !attr.Attr.Default.NullAbility,
					},
					Primary:   attr.Attr.Primary,
					Default:   attr.Attr.Default,
					OnUpdate:  attr.Attr.OnUpdate,
					Comment:   attr.Attr.Comment,
					ClusterBy: attr.Attr.ClusterBy,
				}
				return true, autoIncrCol
			}
		}
	}
	return false, nil
}
