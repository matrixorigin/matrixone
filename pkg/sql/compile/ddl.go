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

package compile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (s *Scope) CreateDatabase(c *Compile) error {
	dbName := s.Plan.GetDdl().GetCreateDatabase().GetDatabase()
	if _, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator); err == nil {
		if s.Plan.GetDdl().GetCreateDatabase().GetIfNotExists() {
			return nil
		}
		return moerr.NewDBAlreadyExists(dbName)
	}
	err := c.e.Create(context.WithValue(c.ctx, defines.SqlKey{}, c.sql),
		dbName, c.proc.TxnOperator)
	if err != nil {
		return err
	}
	return colexec.CreateAutoIncrTable(c.e, c.ctx, c.proc, dbName)
}

func (s *Scope) DropDatabase(c *Compile) error {
	dbName := s.Plan.GetDdl().GetDropDatabase().GetDatabase()
	if _, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator); err != nil {
		if s.Plan.GetDdl().GetDropDatabase().GetIfExists() {
			return nil
		}
		return err
	}
	return c.e.Delete(c.ctx, dbName, c.proc.TxnOperator)
}

func (s *Scope) CreateTable(c *Compile) error {
	qry := s.Plan.GetDdl().GetCreateTable()
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
	tableCols := planCols
	exeCols := planColsToExeCols(planCols)

	// convert the plan's defs to the execution's defs
	planDefs := qry.GetTableDef().GetDefs()
	exeDefs, err := planDefsToExeDefs(planDefs)
	if err != nil {
		return err
	}

	dbName := c.db
	if qry.GetDatabase() != "" {
		dbName = qry.GetDatabase()
	}
	dbSource, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		if dbName == "" {
			return moerr.NewNoDB()
		}
		return err
	}
	tblName := qry.GetTableDef().GetName()
	if _, err := dbSource.Relation(c.ctx, tblName); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}
		return moerr.NewTableAlreadyExists(tblName)
	}
	if err := dbSource.Create(context.WithValue(c.ctx, defines.SqlKey{}, c.sql), tblName, append(exeCols, exeDefs...)); err != nil {
		return err
	}
	// build index table
	for _, def := range qry.IndexTables {
		planCols = def.GetCols()
		exeCols = planColsToExeCols(planCols)
		planDefs = def.GetDefs()
		exeDefs, err = planDefsToExeDefs(planDefs)
		if err != nil {
			return err
		}
		if _, err := dbSource.Relation(c.ctx, def.Name); err != nil {
			if err := dbSource.Create(c.ctx, def.Name, append(exeCols, exeDefs...)); err != nil {
				return err
			}
		}
	}

	return colexec.CreateAutoIncrCol(dbSource, c.ctx, c.proc, tableCols, tblName)
}

func (s *Scope) DropTable(c *Compile) error {
	qry := s.Plan.GetDdl().GetDropTable()

	dbName := qry.GetDatabase()
	dbSource, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}
	tblName := qry.GetTable()
	var rel engine.Relation
	if rel, err = dbSource.Relation(c.ctx, tblName); err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}
	if err := dbSource.Delete(c.ctx, tblName); err != nil {
		return err
	}
	for _, name := range qry.IndexTableNames {
		if err := dbSource.Delete(c.ctx, name); err != nil {
			return err
		}
	}
	return colexec.DeleteAutoIncrCol(rel, dbSource, c.ctx, c.proc, rel.GetTableID(c.ctx))
}

func planDefsToExeDefs(planDefs []*plan.TableDef_DefType) ([]engine.TableDef, error) {
	exeDefs := make([]engine.TableDef, len(planDefs))
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
		case *plan.TableDef_DefType_View:
			exeDefs[i] = &engine.ViewDef{
				View: defVal.View.View,
			}
		case *plan.TableDef_DefType_Partition:
			bytes, err := defVal.Partition.MarshalPartitionInfo()
			if err != nil {
				return nil, err
			}
			exeDefs[i] = &engine.PartitionDef{
				Partition: string(bytes),
			}
		case *plan.TableDef_DefType_ComputeIndex:
			computeIndexDef := &engine.ComputeIndexDef{}
			computeIndexDef.Names = defVal.ComputeIndex.Names
			computeIndexDef.TableNames = defVal.ComputeIndex.TableNames
			computeIndexDef.Uniques = defVal.ComputeIndex.Uniques
			exeDefs[i] = computeIndexDef
		}
	}
	return exeDefs, nil
}

func planColsToExeCols(planCols []*plan.ColDef) []engine.TableDef {
	exeCols := make([]engine.TableDef, len(planCols))
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
				Default:       planCols[i].GetDefault(),
				OnUpdate:      planCols[i].GetOnUpdate(),
				Primary:       col.GetPrimary(),
				Comment:       col.GetComment(),
				AutoIncrement: col.GetAutoIncrement(),
			},
		}
	}
	return exeCols
}
