// Copyright 2021 Matrix Origin
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

package compile2

import (
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (s *Scope) CreateDatabase(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	dbName := s.Plan.GetDdl().GetCreateDatabase().GetDatabase()
	return engine.Create(ts, dbName, 0, snapshot)
}

func (s *Scope) DropDatabase(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	dbName := s.Plan.GetDdl().GetCreateDatabase().GetDatabase()
	return engine.Delete(ts, dbName, snapshot)
}

func (s *Scope) CreateTable(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	qry := s.Plan.GetDdl().GetCreateTable()
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
	exeCols := planColsToExeCols(planCols)

	// convert the plan's defs to the execution's defs
	planDefs := qry.GetTableDef().GetDefs()
	exeDefs := planDefsToExeDefs(planDefs)

	dbSource, err := engine.Database(qry.GetDatabase(), nil)
	if err != nil {
		return err
	}
	return dbSource.Create(ts, qry.GetTableDef().GetName(), append(exeCols, exeDefs...), snapshot)
}

func (s *Scope) DropTable(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	qry := s.Plan.GetDdl().GetDropTable()

	dbName := qry.GetDatabase()
	dbSource, err := engine.Database(dbName, nil)
	if err != nil {
		return err
	}

	tblName := qry.GetTable()
	return dbSource.Delete(ts, tblName, snapshot)
}

func (s *Scope) CreateIndex(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	return nil
}

func (s *Scope) DropIndex(ts uint64, snapshot engine.Snapshot, engine engine.Engine) error {
	return nil
}

func planDefsToExeDefs(planDefs []*plan.TableDef_DefType) []engine.TableDef {
	exeDefs := make([]engine.TableDef, 0, len(planDefs))
	for i, def := range planDefs {
		switch defVal := def.GetDef().(type) {
		case *plan.TableDef_DefType_Pk:
			exeDefs[i] = &engine.PrimaryIndexDef{
				Names: defVal.Pk.GetNames(),
			}
		case *plan.TableDef_DefType_Idx:
			exeDefs[i] = &engine.IndexTableDef{
				ColNames: defVal.Idx.GetColNames(),
				Name:     defVal.Idx.GetName(),
			}
		case *plan.TableDef_DefType_Properties:
			properties := make([]engine.Property, len(defVal.Properties.GetProperties()))
			for i, p := range defVal.Properties.GetProperties() {
				properties[i] = engine.Property{
					Key:   p.GetKey(),
					Value: p.GetValue(),
				}
			}
			exeDefs[i] = &engine.PropertiesDef{
				Properties: properties,
			}
		}
	}
	return exeDefs
}

func planColsToExeCols(planCols []*plan.ColDef) []engine.TableDef {
	exeCols := make([]engine.TableDef, 0, len(planCols))
	for i, col := range planCols {
		var alg compress.T
		switch col.Alg {
		case plan.CompressType_None:
			alg = compress.None
		case plan.CompressType_Lz4:
			alg = compress.Lz4
		}
		colTyp := col.GetTyp()
		exeCols[i] = &engine.AttributeDef{
			Attr: engine.Attribute{
				Name: col.Name,
				Alg:  alg,
				Type: types.Type{
					Oid:       types.T(colTyp.GetId()),
					Width:     colTyp.GetWidth(),
					Precision: colTyp.GetPrecision(),
					Scale:     colTyp.GetScale(),
					Size:      colTyp.GetSize(),
				},
				Default: engine.DefaultExpr{
					Exist:  col.GetDefault().GetExist(),
					Value:  col.GetDefault().GetValue(),
					IsNull: col.GetDefault().GetIsNull(),
				},
				Primary: col.GetPrimary(),
			},
		}
	}
	return exeCols
}
