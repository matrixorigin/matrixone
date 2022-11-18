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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (s *Scope) CreateDatabase(c *Compile) error {
	var span trace.Span
	c.ctx, span = trace.Start(c.ctx, "CreateDatabase")
	defer span.End()
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
		return moerr.NewErrDropNonExistsDB(dbName)
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

	// check in EntireEngine.TempEngine, notice that TempEngine may not init
	tmpDBSource, err := c.e.Database(c.ctx, engine.TEMPORARY_DBNAME, c.proc.TxnOperator)
	if err == nil {

		if _, err := tmpDBSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName)); err == nil {
			if qry.GetIfNotExists() {
				return nil
			}
			return moerr.NewTableAlreadyExists(fmt.Sprintf("temporary '%s'", tblName))
		}
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
		if _, err := dbSource.Relation(c.ctx, def.Name); err == nil {
			return moerr.NewTableAlreadyExists(def.Name)
		}
		if err := dbSource.Create(c.ctx, def.Name, append(exeCols, exeDefs...)); err != nil {
			return err
		}
	}

	return colexec.CreateAutoIncrCol(c.e, c.ctx, dbSource, c.proc, tableCols, dbName, tblName)
}

func (s *Scope) CreateTempTable(c *Compile) error {
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

	// Temporary table names and persistent table names are not allowed to be duplicated
	// So before create temporary table, need to check if it exists a table has same name
	dbName := c.db
	if qry.GetDatabase() != "" {
		dbName = qry.GetDatabase()
	}

	// check in EntireEngine.TempEngine
	tmpDBSource, err := c.e.Database(c.ctx, engine.TEMPORARY_DBNAME, c.proc.TxnOperator)
	if err != nil {
		return err
	}
	tblName := qry.GetTableDef().GetName()
	if _, err := tmpDBSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName)); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}
		return moerr.NewTableAlreadyExists(fmt.Sprintf("temporary '%s'", tblName))
	}

	// check in EntireEngine.Engine
	dbSource, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		return err
	}
	if _, err := dbSource.Relation(c.ctx, tblName); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}
		return moerr.NewTableAlreadyExists(tblName)
	}

	// create temporary table
	if err := tmpDBSource.Create(c.ctx, engine.GetTempTableName(dbName, tblName), append(exeCols, exeDefs...)); err != nil {
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
		if _, err := tmpDBSource.Relation(c.ctx, def.Name); err == nil {
			return moerr.NewTableAlreadyExists(def.Name)
		}

		if err := tmpDBSource.Create(c.ctx, engine.GetTempTableName(dbName, def.Name), append(exeCols, exeDefs...)); err != nil {
			return err
		}
	}

	return colexec.CreateAutoIncrCol(c.e, c.ctx, tmpDBSource, c.proc, tableCols, engine.TEMPORARY_DBNAME, engine.GetTempTableName(dbName, tblName))
}

// Truncation operations cannot be performed if the session holds an active table lock.
func (s *Scope) TruncateTable(c *Compile) error {
	tqry := s.Plan.GetDdl().GetTruncateTable()
	dbName := tqry.GetDatabase()
	var dbSource engine.Database
	var rel engine.Relation
	var err error
	var isTemp bool
	dbSource, err = c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		return err
	}

	tblName := tqry.GetTable()
	if rel, err = dbSource.Relation(c.ctx, tblName); err != nil {
		var e error // avoid contamination of error messages
		dbSource, e = c.e.Database(c.ctx, engine.TEMPORARY_DBNAME, c.proc.TxnOperator)
		if e != nil {
			return err
		}
		rel, e = dbSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName))
		if e != nil {
			return err
		}
		isTemp = true
	}

	// Truncate Index Tables if needed
	for _, name := range tqry.IndexTableNames {
		var err error
		if isTemp {
			err = dbSource.Truncate(c.ctx, engine.GetTempTableName(dbName, name))
		} else {
			err = dbSource.Truncate(c.ctx, name)
		}
		if err != nil {
			return err
		}
	}

	id := rel.GetTableID(c.ctx)
	if isTemp {
		err = dbSource.Truncate(c.ctx, engine.GetTempTableName(dbName, tblName))
		if err != nil {
			return err
		}
		err = colexec.ResetAutoInsrCol(c.e, c.ctx, engine.GetTempTableName(dbName, tblName), dbSource, c.proc, id, engine.TEMPORARY_DBNAME)
	} else {
		err = dbSource.Truncate(c.ctx, tblName)
		if err != nil {
			return err
		}
		err = colexec.ResetAutoInsrCol(c.e, c.ctx, tblName, dbSource, c.proc, id, dbName)
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *Scope) DropTable(c *Compile) error {
	qry := s.Plan.GetDdl().GetDropTable()

	dbName := qry.GetDatabase()
	var dbSource engine.Database
	var rel engine.Relation
	var err error
	var isTemp bool
	dbSource, err = c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}
	tblName := qry.GetTable()
	if rel, err = dbSource.Relation(c.ctx, tblName); err != nil {
		var e error // avoid contamination of error messages
		dbSource, e = c.e.Database(c.ctx, engine.TEMPORARY_DBNAME, c.proc.TxnOperator)
		if dbSource == nil && qry.GetIfExists() {
			return nil
		} else if e != nil {
			return err
		}
		rel, e = dbSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName))
		if e != nil {
			if qry.GetIfExists() {
				return nil
			} else {
				return err
			}
		}
		isTemp = true
	}
	if isTemp {
		if err := dbSource.Delete(c.ctx, engine.GetTempTableName(dbName, tblName)); err != nil {
			return err
		}
		for _, name := range qry.IndexTableNames {
			if err := dbSource.Delete(c.ctx, name); err != nil {
				return err
			}
		}
		return colexec.DeleteAutoIncrCol(c.e, c.ctx, rel, c.proc, engine.TEMPORARY_DBNAME, rel.GetTableID(c.ctx))
	} else {
		if err := dbSource.Delete(c.ctx, tblName); err != nil {
			return err
		}
		for _, name := range qry.IndexTableNames {
			if err := dbSource.Delete(c.ctx, name); err != nil {
				return err
			}
		}
		return colexec.DeleteAutoIncrCol(c.e, c.ctx, rel, c.proc, dbName, rel.GetTableID(c.ctx))
	}
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
			indexDef := &engine.ComputeIndexDef{}
			indexDef.IndexNames = defVal.Idx.IndexNames
			indexDef.TableNames = defVal.Idx.TableNames
			indexDef.Uniques = defVal.Idx.Uniques
			indexDef.Fields = make([][]string, 0)
			for _, field := range defVal.Idx.Fields {
				indexDef.Fields = append(indexDef.Fields, field.ColNames)
			}
			exeDefs[i] = indexDef
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
				AutoIncrement: col.Typ.GetAutoIncr(),
			},
		}
	}
	return exeCols
}
