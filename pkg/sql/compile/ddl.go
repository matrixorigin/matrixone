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
	"github.com/matrixorigin/matrixone/pkg/sql/util"
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
		return moerr.NewDBAlreadyExists(c.ctx, dbName)
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
		return moerr.NewErrDropNonExistsDB(c.ctx, dbName)
	}
	return c.e.Delete(c.ctx, dbName, c.proc.TxnOperator)
}

// Drop the old view, and create the new view.
func (s *Scope) AlterView(c *Compile) error {
	qry := s.Plan.GetDdl().GetAlterView()

	dbName := c.db
	dbSource, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}
	tblName := qry.GetTableDef().GetName()
	if _, err = dbSource.Relation(c.ctx, tblName); err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}

	// Drop view table.
	if err := dbSource.Delete(c.ctx, tblName); err != nil {
		return err
	}

	// Create view table.
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
	exeCols := planColsToExeCols(planCols)

	// convert the plan's defs to the execution's defs
	exeDefs, err := planDefsToExeDefs(qry.GetTableDef())
	if err != nil {
		return err
	}

	// if _, err := dbSource.Relation(c.ctx, tblName); err == nil {
	//  	 return moerr.NewTableAlreadyExists(c.ctx, tblName)
	// }

	return dbSource.Create(context.WithValue(c.ctx, defines.SqlKey{}, c.sql), tblName, append(exeCols, exeDefs...))
}

func (s *Scope) CreateTable(c *Compile) error {
	qry := s.Plan.GetDdl().GetCreateTable()
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
	tableCols := planCols
	exeCols := planColsToExeCols(planCols)

	// convert the plan's defs to the execution's defs
	exeDefs, err := planDefsToExeDefs(qry.GetTableDef())
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
			return moerr.NewNoDB(c.ctx)
		}
		return err
	}
	tblName := qry.GetTableDef().GetName()
	if _, err := dbSource.Relation(c.ctx, tblName); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}
		return moerr.NewTableAlreadyExists(c.ctx, tblName)
	}

	// check in EntireEngine.TempEngine, notice that TempEngine may not init
	tmpDBSource, err := c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
	if err == nil {
		if _, err := tmpDBSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName)); err == nil {
			if qry.GetIfNotExists() {
				return nil
			}
			return moerr.NewTableAlreadyExists(c.ctx, fmt.Sprintf("temporary '%s'", tblName))
		}
	}

	if err := dbSource.Create(context.WithValue(c.ctx, defines.SqlKey{}, c.sql), tblName, append(exeCols, exeDefs...)); err != nil {
		return err
	}

	fkDbs := qry.GetFkDbs()
	if len(fkDbs) > 0 {
		fkTables := qry.GetFkTables()
		newRelation, err := dbSource.Relation(c.ctx, tblName)
		if err != nil {
			return err
		}
		tblId := newRelation.GetTableID(c.ctx)

		// for now ColumnId is equal ColumnIndex, and we have a bug to UpdateConstraint after created immediately
		// so i comment these codes. if you want to remove these code, let @ouyuanning known.
		newTableDef, err := newRelation.TableDefs(c.ctx)
		if err != nil {
			return err
		}
		var colNameToId = make(map[string]uint64)
		for _, def := range newTableDef {
			if attr, ok := def.(*engine.AttributeDef); ok {
				colNameToId[attr.Attr.Name] = attr.Attr.ID
			}
		}
		newFkeys := make([]*plan.ForeignKeyDef, len(qry.GetTableDef().Fkeys))
		for i, fkey := range qry.GetTableDef().Fkeys {
			newDef := &plan.ForeignKeyDef{
				Name:        fkey.Name,
				Cols:        make([]uint64, len(fkey.Cols)),
				ForeignTbl:  fkey.ForeignTbl,
				ForeignCols: make([]uint64, len(fkey.ForeignCols)),
				OnDelete:    fkey.OnDelete,
				OnUpdate:    fkey.OnUpdate,
			}
			copy(newDef.ForeignCols, fkey.ForeignCols)
			for idx, colName := range qry.GetFkCols()[i].Cols {
				newDef.Cols[idx] = colNameToId[colName]
			}
			newFkeys[i] = newDef
		}
		newCt, err := makeNewCreateConstraint(nil, &engine.ForeignKeyDef{
			Fkeys: newFkeys,
		})
		if err != nil {
			return err
		}
		err = newRelation.UpdateConstraint(c.ctx, newCt)
		if err != nil {
			return err
		}

		// need to append TableId to parent's TableDef.RefChildTbls
		for i, fkTableName := range fkTables {
			fkDbName := fkDbs[i]
			fkDbSource, err := c.e.Database(c.ctx, fkDbName, c.proc.TxnOperator)
			if err != nil {
				return err
			}
			fkRelation, err := fkDbSource.Relation(c.ctx, fkTableName)
			if err != nil {
				return err
			}
			fkTableDef, err := fkRelation.TableDefs(c.ctx)
			if err != nil {
				return err
			}
			var oldCt *engine.ConstraintDef
			for _, def := range fkTableDef {
				if ct, ok := def.(*engine.ConstraintDef); ok {
					oldCt = ct
					break
				}
			}
			newRefChildDef := &engine.RefChildTableDef{
				Tables: []uint64{tblId},
			}
			newCt, err := makeNewCreateConstraint(oldCt, newRefChildDef)
			if err != nil {
				return err
			}
			err = fkRelation.UpdateConstraint(c.ctx, newCt)
			if err != nil {
				return err
			}
		}
	}

	// build index table
	for _, def := range qry.IndexTables {
		planCols = def.GetCols()
		exeCols = planColsToExeCols(planCols)
		exeDefs, err = planDefsToExeDefs(def)
		if err != nil {
			return err
		}
		if _, err := dbSource.Relation(c.ctx, def.Name); err == nil {
			return moerr.NewTableAlreadyExists(c.ctx, def.Name)
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
	exeDefs, err := planDefsToExeDefs(qry.GetTableDef())
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
	tmpDBSource, err := c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
	if err != nil {
		return err
	}
	tblName := qry.GetTableDef().GetName()
	if _, err := tmpDBSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName)); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}
		return moerr.NewTableAlreadyExists(c.ctx, fmt.Sprintf("temporary '%s'", tblName))
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
		return moerr.NewTableAlreadyExists(c.ctx, tblName)
	}

	// create temporary table
	if err := tmpDBSource.Create(c.ctx, engine.GetTempTableName(dbName, tblName), append(exeCols, exeDefs...)); err != nil {
		return err
	}

	// build index table
	for _, def := range qry.IndexTables {
		planCols = def.GetCols()
		exeCols = planColsToExeCols(planCols)
		exeDefs, err = planDefsToExeDefs(def)
		if err != nil {
			return err
		}
		if _, err := tmpDBSource.Relation(c.ctx, def.Name); err == nil {
			return moerr.NewTableAlreadyExists(c.ctx, def.Name)
		}

		if err := tmpDBSource.Create(c.ctx, engine.GetTempTableName(dbName, def.Name), append(exeCols, exeDefs...)); err != nil {
			return err
		}
	}

	return colexec.CreateAutoIncrCol(c.e, c.ctx, tmpDBSource, c.proc, tableCols, defines.TEMPORARY_DBNAME, engine.GetTempTableName(dbName, tblName))
}

func (s *Scope) CreateIndex(c *Compile) error {
	qry := s.Plan.GetDdl().GetCreateIndex()
	d, err := c.e.Database(c.ctx, qry.Database, c.proc.TxnOperator)
	if err != nil {
		return err
	}
	r, err := d.Relation(c.ctx, qry.Table)
	if err != nil {
		return err
	}

	// build and create index table
	if qry.TableExist {
		def := qry.GetIndex().GetIndexTables()[0]
		planCols := def.GetCols()
		exeCols := planColsToExeCols(planCols)
		exeDefs, err := planDefsToExeDefs(def)
		if err != nil {
			return err
		}
		if _, err := d.Relation(c.ctx, def.Name); err == nil {
			return moerr.NewTableAlreadyExists(c.ctx, def.Name)
		}
		if err := d.Create(c.ctx, def.Name, append(exeCols, exeDefs...)); err != nil {
			return err
		}

	}
	// build and update constraint def
	defs, err := planDefsToExeDefs(qry.GetIndex().GetTableDef())
	if err != nil {
		return err
	}
	ct := defs[0].(*engine.ConstraintDef)

	tblDefs, err := r.TableDefs(c.ctx)
	if err != nil {
		return err
	}
	var oldCt *engine.ConstraintDef
	for _, def := range tblDefs {
		if ct, ok := def.(*engine.ConstraintDef); ok {
			oldCt = ct
			break
		}
	}
	newCt, err := makeNewCreateConstraint(oldCt, ct.Cts[0])
	if err != nil {
		return err
	}
	err = r.UpdateConstraint(c.ctx, newCt)
	if err != nil {
		return err
	}

	// TODO: implement by insert ... select ...
	// insert data into index table
	switch t := qry.GetIndex().GetTableDef().Defs[0].Def.(type) {
	case *plan.TableDef_DefType_UIdx:
		targetAttrs := getIndexColsFromOriginTable(tblDefs, t.UIdx.Fields[0].Parts)

		ret, err := r.Ranges(c.ctx, nil)
		if err != nil {
			return err
		}
		rds, err := r.NewReader(c.ctx, 1, nil, ret)
		if err != nil {
			return err
		}
		bat, err := rds[0].Read(c.ctx, targetAttrs, nil, c.proc.Mp())
		if err != nil {
			return err
		}
		err = rds[0].Close()
		if err != nil {
			return err
		}

		if bat != nil {
			indexBat, cnt := util.BuildUniqueKeyBatch(bat.Vecs, targetAttrs, t.UIdx.Fields[0].Parts, qry.OriginTablePrimaryKey, c.proc)
			indexR, err := d.Relation(c.ctx, t.UIdx.TableNames[0])
			if err != nil {
				return err
			}
			if cnt != 0 {
				if err := indexR.Write(c.ctx, indexBat); err != nil {
					return err
				}
			}
			indexBat.Clean(c.proc.Mp())
		}
		// other situation is not supported now and check in plan
	}

	return nil
}

func (s *Scope) DropIndex(c *Compile) error {
	qry := s.Plan.GetDdl().GetDropIndex()
	d, err := c.e.Database(c.ctx, qry.Database, c.proc.TxnOperator)
	if err != nil {
		return err
	}
	r, err := d.Relation(c.ctx, qry.Table)
	if err != nil {
		return err
	}

	// build and update constraint def
	tblDefs, err := r.TableDefs(c.ctx)
	if err != nil {
		return err
	}
	var oldCt *engine.ConstraintDef
	for _, def := range tblDefs {
		if ct, ok := def.(*engine.ConstraintDef); ok {
			oldCt = ct
			break
		}
	}
	newCt, err := makeNewDropConstraint(oldCt, qry.GetIndexName())
	if err != nil {
		return err
	}
	err = r.UpdateConstraint(c.ctx, newCt)
	if err != nil {
		return err
	}

	// drop index table
	if qry.IndexTableName != "" {
		if _, err = d.Relation(c.ctx, qry.IndexTableName); err != nil {
			return err
		}
		if err = d.Delete(c.ctx, qry.IndexTableName); err != nil {
			return err
		}
	}
	return nil
}

// TODO:
// func makeNewUpdateConstraint()
func makeNewDropConstraint(oldCt *engine.ConstraintDef, dropName string) (*engine.ConstraintDef, error) {
	// must fount dropName because of being checked in plan
	for j, ct := range oldCt.Cts {
		switch c := ct.(type) {
		case *engine.ForeignKeyDef:
			ok := false
			var def *engine.ForeignKeyDef
			for _, ct := range oldCt.Cts {
				if def, ok = ct.(*engine.ForeignKeyDef); ok {
					for idx, fkDef := range def.Fkeys {
						if fkDef.Name == dropName {
							def.Fkeys = append(def.Fkeys[:idx], def.Fkeys[idx+1:]...)
							break
						}
					}
					break
				}
			}
			if !ok {
				oldCt.Cts = append(oldCt.Cts, c)
			}

		case *engine.UniqueIndexDef:
			u := &plan.UniqueIndexDef{}
			err := u.UnMarshalUniqueIndexDef(([]byte)(c.UniqueIndex))
			if err != nil {
				return nil, err
			}
			for i, name := range u.IndexNames {
				if dropName == name {
					// If all indexes of a table are not defined in plan.UniqueIndexDef, the code will be much simpler
					u.IndexNames = append(u.IndexNames[:i], u.IndexNames[i+1:]...)
					u.TableNames = append(u.TableNames[:i], u.TableNames[i+1:]...)
					u.Fields = append(u.Fields[:i], u.Fields[i+1:]...)
					u.TableExists = append(u.TableExists[:i], u.TableExists[i+1:]...)
					u.Comments = append(u.Comments[:i], u.Comments[i+1:]...)

					oldCt.Cts = append(oldCt.Cts[:j], oldCt.Cts[j+1:]...)
					b, err := u.MarshalUniqueIndexDef()
					if err != nil {
						return nil, err
					}
					oldCt.Cts = append(oldCt.Cts, &engine.UniqueIndexDef{
						UniqueIndex: string(b),
					})
					break
				}
			}
		case *engine.SecondaryIndexDef:
			u := &plan.SecondaryIndexDef{}
			err := u.UnMarshalSecondaryIndexDef(([]byte)(c.SecondaryIndex))
			if err != nil {
				return nil, err
			}
			for i, name := range u.IndexNames {
				if dropName == name {
					// If all indexes of a table are not defined in plan.UniqueIndexDef, the code will be much simpler
					u.IndexNames = append(u.IndexNames[:i], u.IndexNames[i+1:]...)
					u.TableNames = append(u.TableNames[:i], u.TableNames[i+1:]...)
					u.Fields = append(u.Fields[:i], u.Fields[i+1:]...)
					u.TableExists = append(u.TableExists[:i], u.TableExists[i+1:]...)
					u.Comments = append(u.Comments[:i], u.Comments[i+1:]...)

					oldCt.Cts = append(oldCt.Cts[:j], oldCt.Cts[j+1:]...)
					b, err := u.MarshalSecondaryIndexDef()
					if err != nil {
						return nil, err
					}
					oldCt.Cts = append(oldCt.Cts, &engine.SecondaryIndexDef{
						SecondaryIndex: string(b),
					})
					break
				}
			}
		}
	}
	return oldCt, nil
}

func makeNewCreateConstraint(oldCt *engine.ConstraintDef, c engine.Constraint) (*engine.ConstraintDef, error) {
	// duplication has checked in plan
	if oldCt == nil {
		return &engine.ConstraintDef{
			Cts: []engine.Constraint{c},
		}, nil
	}
	switch t := c.(type) {
	case *engine.ForeignKeyDef:
		ok := false
		var def *engine.ForeignKeyDef
		for _, ct := range oldCt.Cts {
			if def, ok = ct.(*engine.ForeignKeyDef); ok {
				// i don't see any clause to change FK. only add or drop. so
				def.Fkeys = append(def.Fkeys, t.Fkeys...)
				break
			}
		}
		if !ok {
			oldCt.Cts = append(oldCt.Cts, c)
		}

	case *engine.RefChildTableDef:
		ok := false
		var def *engine.RefChildTableDef
		for _, ct := range oldCt.Cts {
			if def, ok = ct.(*engine.RefChildTableDef); ok {
				def.Tables = append(def.Tables, t.Tables...)
				break
			}
		}
		if !ok {
			oldCt.Cts = append(oldCt.Cts, c)
		}

	case *engine.UniqueIndexDef:
		d := &plan.UniqueIndexDef{}
		err := d.UnMarshalUniqueIndexDef([]byte(t.UniqueIndex))
		if err != nil {
			return nil, err
		}

		ok := false
		var idx *engine.UniqueIndexDef
		for i, ct := range oldCt.Cts {
			if idx, ok = ct.(*engine.UniqueIndexDef); ok {
				u := &plan.UniqueIndexDef{}
				err := u.UnMarshalUniqueIndexDef([]byte(idx.UniqueIndex))
				if err != nil {
					return nil, err
				}
				u.IndexNames = append(u.IndexNames, d.IndexNames[0])
				u.TableNames = append(u.TableNames, d.TableNames[0])
				u.TableExists = append(u.TableExists, d.TableExists[0])
				u.Fields = append(u.Fields, d.Fields[0])
				u.Comments = append(u.Comments, d.Comments[0])

				oldCt.Cts = append(oldCt.Cts[:i], oldCt.Cts[i+1:]...)

				bytes, err := u.MarshalUniqueIndexDef()
				if err != nil {
					return nil, err
				}
				oldCt.Cts = append(oldCt.Cts, &engine.UniqueIndexDef{
					UniqueIndex: string(bytes),
				})
				break
			}
		}
		if !ok {
			oldCt.Cts = append(oldCt.Cts, c)
		}
	case *engine.SecondaryIndexDef:
		d := &plan.SecondaryIndexDef{}
		err := d.UnMarshalSecondaryIndexDef([]byte(t.SecondaryIndex))
		if err != nil {
			return nil, err
		}

		ok := false
		var idx *engine.SecondaryIndexDef
		for i, ct := range oldCt.Cts {
			if idx, ok = ct.(*engine.SecondaryIndexDef); ok {
				u := &plan.SecondaryIndexDef{}
				err := u.UnMarshalSecondaryIndexDef([]byte(idx.SecondaryIndex))
				if err != nil {
					return nil, err
				}
				u.IndexNames = append(u.IndexNames, d.IndexNames[0])
				u.TableNames = append(u.TableNames, d.TableNames[0])
				u.TableExists = append(u.TableExists, d.TableExists[0])
				u.Fields = append(u.Fields, d.Fields[0])
				u.Comments = append(u.Comments, d.Comments[0])

				oldCt.Cts = append(oldCt.Cts[:i], oldCt.Cts[i+1:]...)

				bytes, err := u.MarshalSecondaryIndexDef()
				if err != nil {
					return nil, err
				}
				oldCt.Cts = append(oldCt.Cts, &engine.SecondaryIndexDef{
					SecondaryIndex: string(bytes),
				})
				break
			}
		}
		if !ok {
			oldCt.Cts = append(oldCt.Cts, c)
		}
	}
	return oldCt, nil
}

// Truncation operations cannot be performed if the session holds an active table lock.
func (s *Scope) TruncateTable(c *Compile) error {
	var dbSource engine.Database
	var rel engine.Relation
	var err error
	var isTemp bool
	var newId uint64

	tqry := s.Plan.GetDdl().GetTruncateTable()
	dbName := tqry.GetDatabase()
	tblName := tqry.GetTable()
	oldId := tqry.GetTableId()

	dbSource, err = c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		return err
	}

	if rel, err = dbSource.Relation(c.ctx, tblName); err != nil {
		var e error // avoid contamination of error messages
		dbSource, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
		if e != nil {
			return err
		}
		rel, e = dbSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName))
		if e != nil {
			return err
		}
		isTemp = true
	}

	if isTemp {
		// memoryengine truncate always return 0, so for temporary table, just use origin tableId as newId
		_, err = dbSource.Truncate(c.ctx, engine.GetTempTableName(dbName, tblName))
		newId = rel.GetTableID(c.ctx)
	} else {
		newId, err = dbSource.Truncate(c.ctx, tblName)
	}

	if err != nil {
		return err
	}

	// Truncate Index Tables if needed
	for _, name := range tqry.IndexTableNames {
		var err error
		if isTemp {
			_, err = dbSource.Truncate(c.ctx, engine.GetTempTableName(dbName, name))
		} else {
			_, err = dbSource.Truncate(c.ctx, name)
		}
		if err != nil {
			return err
		}
	}

	// update tableDef of foreign key's table with new table id
	for _, ftblId := range tqry.ForeignTbl {
		_, _, fkRelation, err := c.e.GetRelationById(c.ctx, c.proc.TxnOperator, ftblId)
		if err != nil {
			return err
		}
		fkTableDef, err := fkRelation.TableDefs(c.ctx)
		if err != nil {
			return err
		}
		var oldCt *engine.ConstraintDef
		for _, def := range fkTableDef {
			if ct, ok := def.(*engine.ConstraintDef); ok {
				oldCt = ct
				break
			}
		}
		for _, ct := range oldCt.Cts {
			if def, ok := ct.(*engine.RefChildTableDef); ok {
				for idx, refTable := range def.Tables {
					if refTable == oldId {
						def.Tables[idx] = newId
						break
					}
				}
				break
			}
		}
		if err != nil {
			return err
		}
		err = fkRelation.UpdateConstraint(c.ctx, oldCt)
		if err != nil {
			return err
		}

	}

	id := rel.GetTableID(c.ctx)

	if isTemp {
		err = colexec.ResetAutoInsrCol(c.e, c.ctx, engine.GetTempTableName(dbName, tblName), dbSource, c.proc, id, newId, defines.TEMPORARY_DBNAME)
	} else {
		err = colexec.ResetAutoInsrCol(c.e, c.ctx, tblName, dbSource, c.proc, id, newId, dbName)
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

	tblName := qry.GetTable()
	tblId := qry.GetTableId()

	dbSource, err = c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}

	if rel, err = dbSource.Relation(c.ctx, tblName); err != nil {
		var e error // avoid contamination of error messages
		dbSource, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
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

	// update tableDef of foreign key's table
	for _, ftblId := range qry.ForeignTbl {
		_, _, fkRelation, err := c.e.GetRelationById(c.ctx, c.proc.TxnOperator, ftblId)
		if err != nil {
			return err
		}
		fkTableDef, err := fkRelation.TableDefs(c.ctx)
		if err != nil {
			return err
		}
		var oldCt *engine.ConstraintDef
		for _, def := range fkTableDef {
			if ct, ok := def.(*engine.ConstraintDef); ok {
				oldCt = ct
				break
			}
		}
		for _, ct := range oldCt.Cts {
			if def, ok := ct.(*engine.RefChildTableDef); ok {
				for idx, refTable := range def.Tables {
					if refTable == tblId {
						def.Tables = append(def.Tables[:idx], def.Tables[idx+1:]...)
						break
					}
				}
				break
			}
		}
		if err != nil {
			return err
		}
		err = fkRelation.UpdateConstraint(c.ctx, oldCt)
		if err != nil {
			return err
		}
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
		return colexec.DeleteAutoIncrCol(c.e, c.ctx, dbSource, rel, c.proc, defines.TEMPORARY_DBNAME, rel.GetTableID(c.ctx))
	} else {
		if err := dbSource.Delete(c.ctx, tblName); err != nil {
			return err
		}
		for _, name := range qry.IndexTableNames {
			if err := dbSource.Delete(c.ctx, name); err != nil {
				return err
			}
		}
		return colexec.DeleteAutoIncrCol(c.e, c.ctx, dbSource, rel, c.proc, dbName, rel.GetTableID(c.ctx))
	}
}

func planDefsToExeDefs(tableDef *plan.TableDef) ([]engine.TableDef, error) {
	planDefs := tableDef.GetDefs()
	var exeDefs []engine.TableDef
	c := new(engine.ConstraintDef)
	for _, def := range planDefs {
		switch defVal := def.GetDef().(type) {
		case *plan.TableDef_DefType_Properties:
			properties := make([]engine.Property, len(defVal.Properties.GetProperties()))
			for i, p := range defVal.Properties.GetProperties() {
				properties[i] = engine.Property{
					Key:   p.GetKey(),
					Value: p.GetValue(),
				}
			}
			exeDefs = append(exeDefs, &engine.PropertiesDef{
				Properties: properties,
			})
		case *plan.TableDef_DefType_Partition:
			bytes, err := defVal.Partition.MarshalPartitionInfo()
			if err != nil {
				return nil, err
			}
			exeDefs = append(exeDefs, &engine.PartitionDef{
				Partition: string(bytes),
			})
		case *plan.TableDef_DefType_UIdx:
			bytes, err := defVal.UIdx.MarshalUniqueIndexDef()
			if err != nil {
				return nil, err
			}
			c.Cts = append(c.Cts, &engine.UniqueIndexDef{
				UniqueIndex: string(bytes),
			})
		case *plan.TableDef_DefType_SIdx:
			bytes, err := defVal.SIdx.MarshalSecondaryIndexDef()
			if err != nil {
				return nil, err
			}
			c.Cts = append(c.Cts, &engine.SecondaryIndexDef{
				SecondaryIndex: string(bytes),
			})
		}
	}

	if tableDef.ViewSql != nil {
		exeDefs = append(exeDefs, &engine.ViewDef{
			View: tableDef.ViewSql.View,
		})
	}

	if len(tableDef.Fkeys) > 0 {
		c.Cts = append(c.Cts, &engine.ForeignKeyDef{
			Fkeys: tableDef.Fkeys,
		})
	}

	if tableDef.Pkey != nil {
		c.Cts = append(c.Cts, &engine.PrimaryKeyDef{
			Pkey: tableDef.Pkey,
		})
	}

	if len(tableDef.RefChildTbls) > 0 {
		c.Cts = append(c.Cts, &engine.RefChildTableDef{
			Tables: tableDef.RefChildTbls,
		})
	}

	if len(c.Cts) > 0 {
		exeDefs = append(exeDefs, c)
	}

	if tableDef.ClusterBy != nil {
		exeDefs = append(exeDefs, &engine.ClusterByDef{
			Name: tableDef.ClusterBy.Name,
		})
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
				ClusterBy:     col.ClusterBy,
				AutoIncrement: col.Typ.GetAutoIncr(),
			},
		}
	}
	return exeCols
}

// Get the required columns of the index table from the original table
func getIndexColsFromOriginTable(tblDefs []engine.TableDef, indexColumns []string) []string {
	colNameMap := make(map[string]int)
	for _, tbldef := range tblDefs {
		if constraintDef, ok := tbldef.(*engine.ConstraintDef); ok {
			for _, ct := range constraintDef.Cts {
				if pk, ok2 := ct.(*engine.PrimaryKeyDef); ok2 {
					for _, name := range pk.Pkey.Names {
						colNameMap[name] = 1
					}
					break
				}
			}
		}
	}

	for _, column := range indexColumns {
		colNameMap[column] = 1
	}

	j := 0
	keys := make([]string, len(colNameMap))
	for k := range colNameMap {
		keys[j] = k
		j++
	}
	return keys
}
