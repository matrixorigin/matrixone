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
	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
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

	if err := lockMoDatabase(c, dbName); err != nil {
		return err
	}

	fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
	c.stmt.Format(fmtCtx)
	ctx := context.WithValue(c.ctx, defines.SqlKey{}, fmtCtx.String())
	datType := ""
	if s.Plan.GetDdl().GetCreateDatabase().SubscriptionOption != nil {
		datType = catalog.SystemDBTypeSubscription
	}
	ctx = context.WithValue(ctx, defines.DatTypKey{}, datType)
	return c.e.Create(ctx, dbName, c.proc.TxnOperator)
}

func (s *Scope) DropDatabase(c *Compile) error {
	dbName := s.Plan.GetDdl().GetDropDatabase().GetDatabase()
	if _, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator); err != nil {
		if s.Plan.GetDdl().GetDropDatabase().GetIfExists() {
			return nil
		}
		return moerr.NewErrDropNonExistsDB(c.ctx, dbName)
	}

	if err := lockMoDatabase(c, dbName); err != nil {
		return err
	}

	err := c.e.Delete(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		return err
	}
	// delete all index object under the database from mo_catalog.mo_indexes
	deleteSql := fmt.Sprintf(deleteMoIndexesWithDatabaseIdFormat, s.Plan.GetDdl().GetDropDatabase().GetDatabaseId())
	err = c.runSql(deleteSql)
	if err != nil {
		return err
	}
	return nil
}

// Drop the old view, and create the new view.
func (s *Scope) AlterView(c *Compile) error {
	qry := s.Plan.GetDdl().GetAlterView()

	dbName := c.db
	tblName := qry.GetTableDef().GetName()

	dbSource, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}
	if _, err = dbSource.Relation(c.ctx, tblName, nil); err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}

	if err := lockMoTable(c, dbName, tblName); err != nil {
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

func addAlterKind(alterKind []api.AlterKind, kind api.AlterKind) []api.AlterKind {
	for i := range alterKind {
		if alterKind[i] == kind {
			return alterKind
		}
	}
	alterKind = append(alterKind, kind)
	return alterKind
}

func getAddColPos(cols []*plan.ColDef, def *plan.ColDef, colName string, pos int32) ([]*plan.ColDef, int32, error) {
	if pos == 0 {
		cols = append([]*plan.ColDef{def}, cols...)
		return cols, pos, nil
	} else if pos == -1 {
		length := len(cols)
		cols = append(cols, nil)
		copy(cols[length:], cols[length-1:])
		cols[length-1] = def
		return cols, int32(length - 1), nil
	}
	var idx int
	for idx = 0; idx < len(cols); idx++ {
		if cols[idx].Name == colName {
			cols = append(cols, nil)
			copy(cols[idx+2:], cols[idx+1:])
			cols[idx+1] = def
			return cols, int32(idx + 1), nil
		}
	}
	return nil, 0, moerr.NewInvalidInputNoCtx("column '%s' doesn't exist in table", colName)
}

func (s *Scope) AlterTableInplace(c *Compile) error {
	qry := s.Plan.GetDdl().GetAlterTable()
	dbName := qry.Database
	if dbName == "" {
		dbName = c.db
	}

	tblName := qry.GetTableDef().GetName()

	dbSource, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		return err
	}
	databaseId := dbSource.GetDatabaseId(c.ctx)

	rel, err := dbSource.Relation(c.ctx, tblName, nil)
	if err != nil {
		return err
	}

	tableDef := plan2.DeepCopyTableDef(qry.TableDef)
	oldDefs, err := rel.TableDefs(c.ctx)
	if err != nil {
		return err
	}

	if err := lockMoTable(c, dbName, tblName); err != nil {
		return err
	}

	if err := lockTable(c.e, c.proc, rel); err != nil {
		return err
	}

	tblId := rel.GetTableID(c.ctx)
	removeRefChildTbls := make(map[string]uint64)
	var addRefChildTbls []uint64
	var newFkeys []*plan.ForeignKeyDef

	var addIndex []*plan.IndexDef
	var dropIndex []*plan.IndexDef
	var alterIndex *plan.IndexDef

	var alterKind []api.AlterKind
	var comment string
	var oldName, newName string
	var addCol []*plan.AlterAddCol
	var dropCol []*plan.AlterDropCol
	cols := tableDef.Cols
	// drop foreign key
	for _, action := range qry.Actions {
		switch act := action.Action.(type) {
		case *plan.AlterTable_Action_Drop:
			alterTableDrop := act.Drop
			constraintName := alterTableDrop.Name
			if alterTableDrop.Typ == plan.AlterTableDrop_FOREIGN_KEY {
				alterKind = addAlterKind(alterKind, api.AlterKind_UpdateConstraint)
				for i, fk := range tableDef.Fkeys {
					if fk.Name == constraintName {
						removeRefChildTbls[constraintName] = fk.ForeignTbl
						tableDef.Fkeys = append(tableDef.Fkeys[:i], tableDef.Fkeys[i+1:]...)
						break
					}
				}
			} else if alterTableDrop.Typ == plan.AlterTableDrop_INDEX {
				alterKind = addAlterKind(alterKind, api.AlterKind_UpdateConstraint)
				for i, indexdef := range tableDef.Indexes {
					if indexdef.IndexName == constraintName {
						dropIndex = append(dropIndex, indexdef)
						tableDef.Indexes = append(tableDef.Indexes[:i], tableDef.Indexes[i+1:]...)
						//1. drop index table
						if indexdef.TableExist {
							if _, err = dbSource.Relation(c.ctx, indexdef.IndexTableName, nil); err != nil {
								return err
							}
							if err = dbSource.Delete(c.ctx, indexdef.IndexTableName); err != nil {
								return err
							}
						}
						//2. delete index object from mo_catalog.mo_indexes
						deleteSql := fmt.Sprintf(deleteMoIndexesWithTableIdAndIndexNameFormat, tableDef.TblId, indexdef.IndexName)
						err = c.runSql(deleteSql)
						if err != nil {
							return err
						}
						break
					}
				}
			} else if alterTableDrop.Typ == plan.AlterTableDrop_COLUMN {
				alterKind = append(alterKind, api.AlterKind_DropColumn)
				var idx int
				for idx = 0; idx < len(cols); idx++ {
					if cols[idx].Name == constraintName {
						drop := &plan.AlterDropCol{
							Idx: uint32(idx),
							Seq: cols[idx].Seqnum,
						}
						dropCol = append(dropCol, drop)
						copy(cols[idx:], cols[idx+1:])
						cols = cols[0 : len(cols)-1]
						break
					}
				}
			}
		case *plan.AlterTable_Action_AddFk:
			alterKind = addAlterKind(alterKind, api.AlterKind_UpdateConstraint)
			addRefChildTbls = append(addRefChildTbls, act.AddFk.Fkey.ForeignTbl)
			newFkeys = append(newFkeys, act.AddFk.Fkey)
		case *plan.AlterTable_Action_AddIndex:
			alterKind = addAlterKind(alterKind, api.AlterKind_UpdateConstraint)
			indexDef := act.AddIndex.IndexInfo.TableDef.Indexes[0]
			for i := range addIndex {
				if indexDef.IndexName == addIndex[i].IndexName {
					return moerr.NewDuplicateKey(c.ctx, indexDef.IndexName)
				}
			}
			addIndex = append(addIndex, indexDef)
			if indexDef.Unique {
				// 0. check original data is not duplicated
				err = genNewUniqueIndexDuplicateCheck(c, qry.Database, tblName, partsToColsStr(indexDef.Parts))
				if err != nil {
					return err
				}
			}

			//1. build and update constraint def
			insertSql, err := makeInsertSingleIndexSQL(c.e, c.proc, databaseId, tblId, indexDef)
			if err != nil {
				return err
			}
			err = c.runSql(insertSql)
			if err != nil {
				return err
			}
			//---------------------------------------------------------
			if act.AddIndex.IndexTableExist {
				def := act.AddIndex.IndexInfo.GetIndexTables()[0]
				// 2. create index table from unique index object
				createSQL := genCreateIndexTableSql(def, indexDef, qry.Database)
				err = c.runSql(createSQL)
				if err != nil {
					return err
				}

				// 3. insert data into index table for unique index object
				insertSQL := genInsertIndexTableSql(tableDef, indexDef, qry.Database)
				err = c.runSql(insertSQL)
				if err != nil {
					return err
				}
			}
		case *plan.AlterTable_Action_AlterIndex:
			alterKind = addAlterKind(alterKind, api.AlterKind_UpdateConstraint)
			tableAlterIndex := act.AlterIndex
			constraintName := tableAlterIndex.IndexName
			for i, indexdef := range tableDef.Indexes {
				if indexdef.IndexName == constraintName {
					alterIndex = indexdef
					alterIndex.Visible = tableAlterIndex.Visible
					tableDef.Indexes[i].Visible = tableAlterIndex.Visible
					// update the index visibility in mo_catalog.mo_indexes
					var updateSql string
					if alterIndex.Visible {
						updateSql = fmt.Sprintf(updateMoIndexesVisibleFormat, 1, tableDef.TblId, indexdef.IndexName)
					} else {
						updateSql = fmt.Sprintf(updateMoIndexesVisibleFormat, 0, tableDef.TblId, indexdef.IndexName)
					}
					err = c.runSql(updateSql)
					if err != nil {
						return err
					}

					break
				}
			}
		case *plan.AlterTable_Action_AlterComment:
			alterKind = addAlterKind(alterKind, api.AlterKind_UpdateComment)
			comment = act.AlterComment.NewComment
		case *plan.AlterTable_Action_AlterName:
			alterKind = addAlterKind(alterKind, api.AlterKind_RenameTable)
			oldName = act.AlterName.OldName
			newName = act.AlterName.NewName
		case *plan.AlterTable_Action_AddCol:
			alterKind = append(alterKind, api.AlterKind_AddColumn)
			col := &plan.ColDef{
				Name: act.AddCol.Name,
				Alg:  plan.CompressType_Lz4,
				Typ:  act.AddCol.Type,
			}
			var pos int32
			cols, pos, err = getAddColPos(cols, col, act.AddCol.PreName, act.AddCol.Pos)
			if err != nil {
				return err
			}
			act.AddCol.Pos = pos
			addCol = append(addCol, act.AddCol)
		}
	}

	// reset origin table's constraint
	var oldCt *engine.ConstraintDef
	newCt := &engine.ConstraintDef{
		Cts: []engine.Constraint{},
	}
	for _, def := range oldDefs {
		if ct, ok := def.(*engine.ConstraintDef); ok {
			oldCt = ct
			break
		}
	}

	if oldCt == nil {
		oldCt = &engine.ConstraintDef{
			Cts: []engine.Constraint{},
		}
	}
	originHasFkDef := false
	originHasIndexDef := false
	for _, ct := range oldCt.Cts {
		switch t := ct.(type) {
		case *engine.ForeignKeyDef:
			for _, fkey := range t.Fkeys {
				if _, ok := removeRefChildTbls[fkey.Name]; !ok {
					newFkeys = append(newFkeys, fkey)
				}
			}
			t.Fkeys = newFkeys
			originHasFkDef = true
			newCt.Cts = append(newCt.Cts, t)
		case *engine.RefChildTableDef:
			newCt.Cts = append(newCt.Cts, t)
		case *engine.IndexDef:
			originHasIndexDef = true
			for in := range dropIndex {
				for i, idx := range t.Indexes {
					if dropIndex[in].IndexName == idx.IndexName {
						t.Indexes = append(t.Indexes[:i], t.Indexes[i+1:]...)
						break
					}
				}
			}
			t.Indexes = append(t.Indexes, addIndex...)
			if alterIndex != nil {
				for i, idx := range t.Indexes {
					if alterIndex.IndexName == idx.IndexName {
						t.Indexes[i].Visible = alterIndex.Visible
					}
				}
			}
			newCt.Cts = append(newCt.Cts, t)
		case *engine.PrimaryKeyDef:
			newCt.Cts = append(newCt.Cts, t)
		}
	}
	if !originHasFkDef {
		newCt.Cts = append(newCt.Cts, &engine.ForeignKeyDef{
			Fkeys: newFkeys,
		})
	}
	if !originHasIndexDef && addIndex != nil {
		newCt.Cts = append(newCt.Cts, &engine.IndexDef{
			Indexes: []*plan.IndexDef(addIndex),
		})
	}

	var addColIdx int
	var dropColIdx int
	constraint := make([][]byte, 0)
	for _, kind := range alterKind {
		var req *api.AlterTableReq
		switch kind {
		case api.AlterKind_UpdateConstraint:
			ct, err := newCt.MarshalBinary()
			if err != nil {
				return err
			}
			req = api.NewUpdateConstraintReq(rel.GetDBID(c.ctx), rel.GetTableID(c.ctx), string(ct))
		case api.AlterKind_UpdateComment:
			req = api.NewUpdateCommentReq(rel.GetDBID(c.ctx), rel.GetTableID(c.ctx), comment)
		case api.AlterKind_RenameTable:
			req = api.NewRenameTableReq(rel.GetDBID(c.ctx), rel.GetTableID(c.ctx), oldName, newName)
		case api.AlterKind_AddColumn:
			name := addCol[addColIdx].Name
			typ := addCol[addColIdx].Type
			pos := addCol[addColIdx].Pos
			addColIdx++
			req = api.NewAddColumnReq(rel.GetDBID(c.ctx), rel.GetTableID(c.ctx), name, typ, pos)
		case api.AlterKind_DropColumn:
			req = api.NewRemoveColumnReq(rel.GetDBID(c.ctx), rel.GetTableID(c.ctx), dropCol[dropColIdx].Idx, dropCol[dropColIdx].Seq)
			dropColIdx++
		default:
		}
		tmp, err := req.Marshal()
		if err != nil {
			return err
		}
		constraint = append(constraint, tmp)
	}

	err = rel.AlterTable(c.ctx, newCt, constraint)
	if err != nil {
		return err
	}

	// remove refChildTbls for drop foreign key clause
	for _, fkTblId := range removeRefChildTbls {
		err := s.removeRefChildTbl(c, fkTblId, tblId)
		if err != nil {
			return err
		}
	}

	// append refChildTbls for add foreign key clause
	for _, fkTblId := range addRefChildTbls {
		_, _, fkRelation, err := c.e.GetRelationById(c.ctx, c.proc.TxnOperator, fkTblId)
		if err != nil {
			return err
		}
		err = s.addRefChildTbl(c, fkRelation, tblId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) CreateTable(c *Compile) error {
	qry := s.Plan.GetDdl().GetCreateTable()
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
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
	tblName := qry.GetTableDef().GetName()

	dbSource, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		if dbName == "" {
			return moerr.NewNoDB(c.ctx)
		}
		return err
	}
	if _, err := dbSource.Relation(c.ctx, tblName, nil); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}
		return moerr.NewTableAlreadyExists(c.ctx, tblName)
	}

	// check in EntireEngine.TempEngine, notice that TempEngine may not init
	tmpDBSource, err := c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
	if err == nil {
		if _, err := tmpDBSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName), nil); err == nil {
			if qry.GetIfNotExists() {
				return nil
			}
			return moerr.NewTableAlreadyExists(c.ctx, fmt.Sprintf("temporary '%s'", tblName))
		}
	}

	if err := lockMoTable(c, dbName, tblName); err != nil {
		return err
	}

	if err := dbSource.Create(context.WithValue(c.ctx, defines.SqlKey{}, c.sql), tblName, append(exeCols, exeDefs...)); err != nil {
		return err
	}

	partitionTables := qry.GetPartitionTables()
	for _, table := range partitionTables {
		storageCols := planColsToExeCols(table.GetCols())
		storageDefs, err := planDefsToExeDefs(table)
		if err != nil {
			return err
		}
		err = dbSource.Create(c.ctx, table.GetName(), append(storageCols, storageDefs...))
		if err != nil {
			return err
		}
	}

	fkDbs := qry.GetFkDbs()
	if len(fkDbs) > 0 {
		fkTables := qry.GetFkTables()
		newRelation, err := dbSource.Relation(c.ctx, tblName, nil)
		if err != nil {
			return err
		}
		tblId := newRelation.GetTableID(c.ctx)

		newTableDef, err := newRelation.TableDefs(c.ctx)
		if err != nil {
			return err
		}
		var colNameToId = make(map[string]uint64)
		var oldCt *engine.ConstraintDef
		for _, def := range newTableDef {
			if attr, ok := def.(*engine.AttributeDef); ok {
				colNameToId[attr.Attr.Name] = attr.Attr.ID
			}
			if ct, ok := def.(*engine.ConstraintDef); ok {
				oldCt = ct
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
		// remove old fk settings
		newCt, err := makeNewCreateConstraint(oldCt, &engine.ForeignKeyDef{
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
			fkRelation, err := fkDbSource.Relation(c.ctx, fkTableName, nil)
			if err != nil {
				return err
			}
			err = s.addRefChildTbl(c, fkRelation, tblId)
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
		if _, err := dbSource.Relation(c.ctx, def.Name, nil); err == nil {
			return moerr.NewTableAlreadyExists(c.ctx, def.Name)
		}
		if err := dbSource.Create(c.ctx, def.Name, append(exeCols, exeDefs...)); err != nil {
			return err
		}
	}

	if checkIndexInitializable(dbName, tblName) {
		newRelation, err := dbSource.Relation(c.ctx, tblName, nil)
		if err != nil {
			return err
		}
		insertSQL, err := makeInsertMultiIndexSQL(c.e, c.ctx, c.proc, dbSource, newRelation)
		if err != nil {
			return err
		}
		err = c.runSql(insertSQL)
		if err != nil {
			return err
		}
	}
	return maybeCreateAutoIncrement(
		c.ctx,
		dbSource,
		qry.GetTableDef(),
		c.proc.TxnOperator,
		nil)
}

func checkIndexInitializable(dbName string, tblName string) bool {
	if dbName == catalog.MOTaskDB {
		return false
	} else if dbName == catalog.MO_CATALOG && tblName == catalog.MO_INDEXES {
		return false
	}
	return true
}

func (s *Scope) CreateTempTable(c *Compile) error {
	qry := s.Plan.GetDdl().GetCreateTable()
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
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
	if _, err := tmpDBSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName), nil); err == nil {
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
	if _, err := dbSource.Relation(c.ctx, tblName, nil); err == nil {
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
		if _, err := tmpDBSource.Relation(c.ctx, def.Name, nil); err == nil {
			return moerr.NewTableAlreadyExists(c.ctx, def.Name)
		}

		if err := tmpDBSource.Create(c.ctx, engine.GetTempTableName(dbName, def.Name), append(exeCols, exeDefs...)); err != nil {
			return err
		}
	}

	return maybeCreateAutoIncrement(
		c.ctx,
		tmpDBSource,
		qry.GetTableDef(),
		c.proc.TxnOperator,
		func() string {
			return engine.GetTempTableName(dbName, tblName)
		})
}

func (s *Scope) CreateIndex(c *Compile) error {
	qry := s.Plan.GetDdl().GetCreateIndex()
	d, err := c.e.Database(c.ctx, qry.Database, c.proc.TxnOperator)
	if err != nil {
		return err
	}
	databaseId := d.GetDatabaseId(c.ctx)

	r, err := d.Relation(c.ctx, qry.Table, nil)
	if err != nil {
		return err
	}
	tableId := r.GetTableID(c.ctx)

	tableDef := plan2.DeepCopyTableDef(qry.TableDef)
	indexDef := qry.GetIndex().GetTableDef().Indexes[0]

	if indexDef.Unique {
		// 0. check original data is not duplicated
		err = genNewUniqueIndexDuplicateCheck(c, qry.Database, tableDef.Name, partsToColsStr(indexDef.Parts))
		if err != nil {
			return err
		}
	}

	// build and create index table for unique index
	if qry.TableExist {
		def := qry.GetIndex().GetIndexTables()[0]
		createSQL := genCreateIndexTableSql(def, indexDef, qry.Database)
		err = c.runSql(createSQL)
		if err != nil {
			return err
		}

		insertSQL := genInsertIndexTableSql(tableDef, indexDef, qry.Database)
		err = c.runSql(insertSQL)
		if err != nil {
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

	// generate insert into mo_indexes metadata
	sql, err := makeInsertSingleIndexSQL(c.e, c.proc, databaseId, tableId, indexDef)
	if err != nil {
		return err
	}
	err = c.runSql(sql)
	if err != nil {
		return err
	}
	return nil
}

func (s *Scope) DropIndex(c *Compile) error {
	qry := s.Plan.GetDdl().GetDropIndex()
	d, err := c.e.Database(c.ctx, qry.Database, c.proc.TxnOperator)
	if err != nil {
		return err
	}
	r, err := d.Relation(c.ctx, qry.Table, nil)
	if err != nil {
		return err
	}

	//1. build and update constraint def
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

	//2. drop index table
	if qry.IndexTableName != "" {
		if _, err = d.Relation(c.ctx, qry.IndexTableName, nil); err != nil {
			return err
		}
		if err = d.Delete(c.ctx, qry.IndexTableName); err != nil {
			return err
		}
	}

	//3. delete index object from mo_catalog.mo_indexes
	deleteSql := fmt.Sprintf(deleteMoIndexesWithTableIdAndIndexNameFormat, r.GetTableID(c.ctx), qry.IndexName)
	err = c.runSql(deleteSql)
	if err != nil {
		return err
	}
	return nil
}

func makeNewDropConstraint(oldCt *engine.ConstraintDef, dropName string) (*engine.ConstraintDef, error) {
	// must fount dropName because of being checked in plan
	for i, ct := range oldCt.Cts {
		switch def := ct.(type) {
		case *engine.ForeignKeyDef:
			for idx, fkDef := range def.Fkeys {
				if fkDef.Name == dropName {
					def.Fkeys = append(def.Fkeys[:idx], def.Fkeys[idx+1:]...)
					oldCt.Cts = append(oldCt.Cts[:i], oldCt.Cts[i+1:]...)
					oldCt.Cts = append(oldCt.Cts, def)
					break
				}
			}
		case *engine.IndexDef:
			for idx, index := range def.Indexes {
				if index.IndexName == dropName {
					def.Indexes = append(def.Indexes[:idx], def.Indexes[idx+1:]...)
					oldCt.Cts = append(oldCt.Cts[:i], oldCt.Cts[i+1:]...)
					oldCt.Cts = append(oldCt.Cts, def)
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
		for i, ct := range oldCt.Cts {
			if _, ok = ct.(*engine.ForeignKeyDef); ok {
				oldCt.Cts = append(oldCt.Cts[:i], oldCt.Cts[i+1:]...)
				oldCt.Cts = append(oldCt.Cts, t)
				break
			}
		}
		if !ok {
			oldCt.Cts = append(oldCt.Cts, c)
		}

	case *engine.RefChildTableDef:
		ok := false
		for i, ct := range oldCt.Cts {
			if _, ok = ct.(*engine.RefChildTableDef); ok {
				oldCt.Cts = append(oldCt.Cts[:i], oldCt.Cts[i+1:]...)
				oldCt.Cts = append(oldCt.Cts, t)
				break
			}
		}
		if !ok {
			oldCt.Cts = append(oldCt.Cts, c)
		}

	case *engine.IndexDef:
		ok := false
		var indexdef *engine.IndexDef
		for i, ct := range oldCt.Cts {
			if indexdef, ok = ct.(*engine.IndexDef); ok {
				indexdef.Indexes = append(indexdef.Indexes, t.Indexes[0])
				oldCt.Cts = append(oldCt.Cts[:i], oldCt.Cts[i+1:]...)
				oldCt.Cts = append(oldCt.Cts, indexdef)
				break
			}
		}
		if !ok {
			oldCt.Cts = append(oldCt.Cts, c)
		}
	}
	return oldCt, nil
}

func (s *Scope) addRefChildTbl(c *Compile, fkRelation engine.Relation, tblId uint64) error {
	fkTableDef, err := fkRelation.TableDefs(c.ctx)
	if err != nil {
		return err
	}
	var oldCt *engine.ConstraintDef
	var oldRefChildDef *engine.RefChildTableDef
	for _, def := range fkTableDef {
		if ct, ok := def.(*engine.ConstraintDef); ok {
			oldCt = ct
			for _, ct := range oldCt.Cts {
				if old, ok := ct.(*engine.RefChildTableDef); ok {
					oldRefChildDef = old
				}
			}
			break
		}
	}
	if oldRefChildDef == nil {
		oldRefChildDef = &engine.RefChildTableDef{}
	}
	oldRefChildDef.Tables = append(oldRefChildDef.Tables, tblId)
	newCt, err := makeNewCreateConstraint(oldCt, oldRefChildDef)
	if err != nil {
		return err
	}
	return fkRelation.UpdateConstraint(c.ctx, newCt)
}

func (s *Scope) removeRefChildTbl(c *Compile, fkTblId uint64, tblId uint64) error {
	_, _, fkRelation, err := c.e.GetRelationById(c.ctx, c.proc.TxnOperator, fkTblId)
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
	return fkRelation.UpdateConstraint(c.ctx, oldCt)
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

	if rel, err = dbSource.Relation(c.ctx, tblName, nil); err != nil {
		var e error // avoid contamination of error messages
		dbSource, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
		if e != nil {
			return err
		}
		rel, e = dbSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName), nil)
		if e != nil {
			return err
		}
		isTemp = true
	}

	if !isTemp && c.proc.TxnOperator.Txn().IsPessimistic() {
		var err error
		if e := lockMoTable(c, dbName, tblName); e != nil {
			if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) {
				return e
			}
			err = e
		}
		// before dropping table, lock it.
		if e := lockTable(c.e, c.proc, rel); e != nil {
			if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) {
				return e
			}
			err = e
		}
		if err != nil {
			return err
		}
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

	//Truncate Partition subtable if needed
	for _, name := range tqry.PartitionTableNames {
		var err error
		if isTemp {
			dbSource.Truncate(c.ctx, engine.GetTempTableName(dbName, name))
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

	if isTemp {
		oldId = rel.GetTableID(c.ctx)
	}
	err = incrservice.GetAutoIncrementService().Reset(
		c.ctx,
		oldId,
		newId,
		false,
		c.proc.TxnOperator)
	if err != nil {
		return err
	}

	// update index information in mo_catalog.mo_indexes
	updateSql := fmt.Sprintf(updateMoIndexesTruncateTableFormat, newId, oldId)
	err = c.runSql(updateSql)
	if err != nil {
		return err
	}
	return nil
}

func (s *Scope) DropSequence(c *Compile) error {
	qry := s.Plan.GetDdl().GetDropSequence()
	dbName := qry.GetDatabase()
	var dbSource engine.Database
	var err error

	tblName := qry.GetTable()
	dbSource, err = c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}

	var rel engine.Relation
	if rel, err = dbSource.Relation(c.ctx, tblName, nil); err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}

	if err := lockMoTable(c, dbName, tblName); err != nil {
		return err
	}

	// Delete the stored session value.
	c.proc.SessionInfo.SeqDeleteKeys = append(c.proc.SessionInfo.SeqDeleteKeys, rel.GetTableID(c.ctx))

	return dbSource.Delete(c.ctx, tblName)
}

func (s *Scope) DropTable(c *Compile) error {
	qry := s.Plan.GetDdl().GetDropTable()
	dbName := qry.GetDatabase()
	tblName := qry.GetTable()
	isView := qry.GetIsView()

	var dbSource engine.Database
	var rel engine.Relation
	var err error
	var isTemp bool

	tblId := qry.GetTableId()

	dbSource, err = c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}

	if rel, err = dbSource.Relation(c.ctx, tblName, nil); err != nil {
		var e error // avoid contamination of error messages
		dbSource, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
		if dbSource == nil && qry.GetIfExists() {
			return nil
		} else if e != nil {
			return err
		}
		rel, e = dbSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName), nil)
		if e != nil {
			if qry.GetIfExists() {
				return nil
			} else {
				return err
			}
		}
		isTemp = true
	}

	if !isTemp && !isView && c.proc.TxnOperator.Txn().IsPessimistic() {
		var err error
		if e := lockMoTable(c, dbName, tblName); e != nil {
			if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) {
				return e
			}
			err = e
		}
		// before dropping table, lock it.
		if e := lockTable(c.e, c.proc, rel); e != nil {
			if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) {
				return e
			}
			err = e
		}
		if err != nil {
			return err
		}
	}

	// update tableDef of foreign key's table
	for _, fkTblId := range qry.ForeignTbl {
		err := s.removeRefChildTbl(c, fkTblId, tblId)
		if err != nil {
			return err
		}
	}

	// delete all index objects of the table in mo_catalog.mo_indexes
	if !qry.IsView && qry.Database != catalog.MO_CATALOG && qry.Table != catalog.MO_INDEXES {
		if qry.GetTableDef().Pkey != nil || len(qry.GetTableDef().Indexes) > 0 {
			deleteSql := fmt.Sprintf(deleteMoIndexesWithTableIdFormat, qry.GetTableDef().TblId)
			err = c.runSql(deleteSql)
			if err != nil {
				return err
			}
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

		//delete partition table
		for _, name := range qry.GetPartitionTableNames() {
			if err = dbSource.Delete(c.ctx, name); err != nil {
				return err
			}
		}

		if dbName != catalog.MO_CATALOG && tblName != catalog.MO_INDEXES {
			err := incrservice.GetAutoIncrementService().Delete(
				c.ctx,
				rel.GetTableID(c.ctx),
				c.proc.TxnOperator)
			if err != nil {
				return err
			}
		}

	} else {
		if err := dbSource.Delete(c.ctx, tblName); err != nil {
			return err
		}
		for _, name := range qry.IndexTableNames {
			if err := dbSource.Delete(c.ctx, name); err != nil {
				return err
			}
		}

		//delete partition table
		for _, name := range qry.GetPartitionTableNames() {
			if err = dbSource.Delete(c.ctx, name); err != nil {
				return err
			}
		}

		if dbName != catalog.MO_CATALOG && tblName != catalog.MO_INDEXES {
			// When drop table 'mo_catalog.mo_indexes', there is no need to delete the auto increment data
			err := incrservice.GetAutoIncrementService().Delete(
				c.ctx,
				rel.GetTableID(c.ctx),
				c.proc.TxnOperator)
			if err != nil {
				return err
			}
		}
	}
	return nil
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
		}
	}

	if tableDef.Indexes != nil {
		c.Cts = append(c.Cts, &engine.IndexDef{
			Indexes: tableDef.Indexes,
		})
	}

	if tableDef.Partition != nil {
		bytes, err := tableDef.Partition.MarshalPartitionInfo()
		if err != nil {
			return nil, err
		}
		exeDefs = append(exeDefs, &engine.PartitionDef{
			Partitioned: 1,
			Partition:   string(bytes),
		})
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
				Name:          col.Name,
				Alg:           alg,
				Type:          types.New(types.T(colTyp.GetId()), colTyp.GetWidth(), colTyp.GetScale()),
				Default:       planCols[i].GetDefault(),
				OnUpdate:      planCols[i].GetOnUpdate(),
				Primary:       col.GetPrimary(),
				Comment:       col.GetComment(),
				ClusterBy:     col.ClusterBy,
				AutoIncrement: col.Typ.GetAutoIncr(),
				IsHidden:      col.Hidden,
				Seqnum:        uint16(col.Seqnum),
			},
		}
	}
	return exeCols
}

func (s *Scope) CreateSequence(c *Compile) error {
	qry := s.Plan.GetDdl().GetCreateSequence()
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
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
	tblName := qry.GetTableDef().GetName()

	dbSource, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		if dbName == "" {
			return moerr.NewNoDB(c.ctx)
		}
		return err
	}

	if _, err := dbSource.Relation(c.ctx, tblName, nil); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}
		// Just report table exists error.
		return moerr.NewTableAlreadyExists(c.ctx, tblName)
	}

	if err := lockMoTable(c, dbName, tblName); err != nil {
		return err
	}

	if err := dbSource.Create(context.WithValue(c.ctx, defines.SqlKey{}, c.sql), tblName, append(exeCols, exeDefs...)); err != nil {
		return err
	}

	// Init the only row of sequence.
	if rel, err := dbSource.Relation(c.ctx, tblName, nil); err == nil {
		if rel == nil {
			return moerr.NewTableAlreadyExists(c.ctx, tblName)
		}
		bat, err := makeSequenceInitBatch(c.ctx, c.stmt.(*tree.CreateSequence), qry.GetTableDef(), c.proc)
		defer func() {
			if bat != nil {
				bat.Clean(c.proc.Mp())
			}
		}()
		if err != nil {
			return err
		}
		err = rel.Write(c.proc.Ctx, bat)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
Sequence table got 1 row and 7 columns(besides row_id).
-----------------------------------------------------------------------------------
last_seq_num | min_value| max_value| start_value| increment_value| cycle| is_called |
-----------------------------------------------------------------------------------

------------------------------------------------------------------------------------
*/
func makeSequenceInitBatch(ctx context.Context, stmt *tree.CreateSequence, tableDef *plan.TableDef, proc *process.Process) (*batch.Batch, error) {
	var bat batch.Batch
	bat.Ro = true
	bat.Cnt = 0
	bat.SetRowCount(1)
	attrs := make([]string, len(plan2.Sequence_cols_name))
	for i := range attrs {
		attrs[i] = plan2.Sequence_cols_name[i]
	}
	bat.Attrs = attrs

	typ := plan2.MakeTypeByPlan2Type(tableDef.Cols[0].Typ)
	sequence_cols_num := 7
	vecs := make([]*vector.Vector, sequence_cols_num)

	// Make sequence vecs.
	switch typ.Oid {
	case types.T_int16:
		incr, minV, maxV, startN, err := makeSequenceParam[int16](ctx, stmt)
		if err != nil {
			return nil, err
		}
		if stmt.MaxValue == nil {
			if incr > 0 {
				maxV = math.MaxInt16
			} else {
				maxV = -1
			}
		}
		if stmt.MinValue == nil && incr < 0 {
			minV = math.MinInt16
		}
		if stmt.StartWith == nil {
			if incr > 0 {
				startN = minV
			} else {
				startN = maxV
			}
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeSequenceVecs(vecs, stmt, typ, proc, incr, minV, maxV, startN)
		if err != nil {
			return nil, err
		}
	case types.T_int32:
		incr, minV, maxV, startN, err := makeSequenceParam[int32](ctx, stmt)
		if err != nil {
			return nil, err
		}
		if stmt.MaxValue == nil {
			if incr > 0 {
				maxV = math.MaxInt32
			} else {
				maxV = -1
			}
		}
		if stmt.MinValue == nil && incr < 0 {
			minV = math.MinInt32
		}
		if stmt.StartWith == nil {
			if incr > 0 {
				startN = minV
			} else {
				startN = maxV
			}
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeSequenceVecs(vecs, stmt, typ, proc, incr, minV, maxV, startN)
		if err != nil {
			return nil, err
		}
	case types.T_int64:
		incr, minV, maxV, startN, err := makeSequenceParam[int64](ctx, stmt)
		if err != nil {
			return nil, err
		}
		if stmt.MaxValue == nil {
			if incr > 0 {
				maxV = math.MaxInt64
			} else {
				maxV = -1
			}
		}
		if stmt.MinValue == nil && incr < 0 {
			minV = math.MinInt64
		}
		if stmt.StartWith == nil {
			if incr > 0 {
				startN = minV
			} else {
				startN = maxV
			}
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeSequenceVecs(vecs, stmt, typ, proc, incr, minV, maxV, startN)
		if err != nil {
			return nil, err
		}
	case types.T_uint16:
		incr, minV, maxV, startN, err := makeSequenceParam[uint16](ctx, stmt)
		if err != nil {
			return nil, err
		}
		if stmt.MaxValue == nil {
			maxV = math.MaxUint16
		}
		if stmt.MinValue == nil && incr < 0 {
			minV = 0
		}
		if stmt.StartWith == nil {
			if incr > 0 {
				startN = minV
			} else {
				startN = maxV
			}
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeSequenceVecs(vecs, stmt, typ, proc, incr, minV, maxV, startN)
		if err != nil {
			return nil, err
		}
	case types.T_uint32:
		incr, minV, maxV, startN, err := makeSequenceParam[uint32](ctx, stmt)
		if err != nil {
			return nil, err
		}
		if stmt.MaxValue == nil {
			maxV = math.MaxUint32
		}
		if stmt.MinValue == nil && incr < 0 {
			minV = 0
		}
		if stmt.StartWith == nil {
			if incr > 0 {
				startN = minV
			} else {
				startN = maxV
			}
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeSequenceVecs(vecs, stmt, typ, proc, incr, minV, maxV, startN)
		if err != nil {
			return nil, err
		}
	case types.T_uint64:
		incr, minV, maxV, startN, err := makeSequenceParam[uint64](ctx, stmt)
		if err != nil {
			return nil, err
		}
		if stmt.MaxValue == nil {
			maxV = math.MaxUint64
		}
		if stmt.MinValue == nil && incr < 0 {
			minV = 0
		}
		if stmt.StartWith == nil {
			if incr > 0 {
				startN = minV
			} else {
				startN = maxV
			}
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeSequenceVecs(vecs, stmt, typ, proc, incr, minV, maxV, startN)
		if err != nil {
			return nil, err
		}
	default:
		return nil, moerr.NewNotSupported(ctx, "Unsupported type for sequence")
	}

	bat.Vecs = vecs
	return &bat, nil
}

func makeSequenceVecs[T constraints.Integer](vecs []*vector.Vector, stmt *tree.CreateSequence, typ types.Type, proc *process.Process, incr int64, minV, maxV, startN T) error {
	vecs[0] = vector.NewConstFixed(typ, startN, 1, proc.Mp())
	vecs[1] = vector.NewConstFixed(typ, minV, 1, proc.Mp())
	vecs[2] = vector.NewConstFixed(typ, maxV, 1, proc.Mp())
	vecs[3] = vector.NewConstFixed(typ, startN, 1, proc.Mp())
	vecs[4] = vector.NewConstFixed(types.T_int64.ToType(), incr, 1, proc.Mp())
	if stmt.Cycle {
		vecs[5] = vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp())
	} else {
		vecs[5] = vector.NewConstFixed(types.T_bool.ToType(), false, 1, proc.Mp())
	}
	vecs[6] = vector.NewConstFixed(types.T_bool.ToType(), false, 1, proc.Mp())
	return nil
}

func makeSequenceParam[T constraints.Integer](ctx context.Context, stmt *tree.CreateSequence) (int64, T, T, T, error) {
	var minValue, maxValue, startNum T
	incrNum := int64(1)
	if stmt.IncrementBy != nil {
		switch stmt.IncrementBy.Num.(type) {
		case uint64:
			return 0, 0, 0, 0, moerr.NewInvalidInput(ctx, "incr value's data type is int64")
		}
		incrNum = getValue[int64](stmt.IncrementBy.Minus, stmt.IncrementBy.Num)
	}
	if incrNum == 0 {
		return 0, 0, 0, 0, moerr.NewInvalidInput(ctx, "Incr value for sequence must not be 0")
	}

	if stmt.MinValue == nil {
		if incrNum > 0 {
			minValue = 1
		} else {
			// Value here is wrong.
			// We will get real value later.
			minValue = 0
		}
	} else {
		minValue = getValue[T](stmt.MinValue.Minus, stmt.MinValue.Num)
	}

	if stmt.MaxValue == nil {
		// Value here is wrong.
		// We will get real value later.
		maxValue = 0
	} else {
		maxValue = getValue[T](stmt.MaxValue.Minus, stmt.MaxValue.Num)
	}

	if stmt.StartWith == nil {
		// The value may be wrong.
		if incrNum > 0 {
			startNum = minValue
		} else {
			startNum = maxValue
		}
	} else {
		startNum = getValue[T](stmt.StartWith.Minus, stmt.StartWith.Num)
	}

	return incrNum, minValue, maxValue, startNum, nil
}

// Checkout values.
func valueCheckOut[T constraints.Integer](maxValue, minValue, startNum T, ctx context.Context) error {
	if maxValue < minValue {
		return moerr.NewInvalidInput(ctx, "Max value of sequence must be bigger than min value of it")
	}
	if startNum < minValue || startNum > maxValue {
		return moerr.NewInvalidInput(ctx, "Start value for sequence must between minvalue and maxvalue")
	}
	return nil
}

func getValue[T constraints.Integer](minus bool, num any) T {
	var v T
	switch num := num.(type) {
	case uint64:
		v = T(num)
	case int64:
		if minus {
			v = -T(num)
		} else {
			v = T(num)
		}
	}
	return v
}

func lockTable(
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation) error {
	id := rel.GetTableID(proc.Ctx)
	defs, err := rel.GetPrimaryKeys(proc.Ctx)
	if err != nil {
		return err
	}
	if len(defs) != 1 {
		panic("invalid primary keys")
	}

	err = lockop.LockTable(
		eng,
		proc,
		id,
		defs[0].Type)
	return err
}

func lockRows(
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation,
	vec *vector.Vector) error {

	if vec == nil || vec.Length() == 0 {
		panic("lock rows is empty")
	}

	id := rel.GetTableID(proc.Ctx)

	err := lockop.LockRows(
		eng,
		proc,
		id,
		vec,
		*vec.GetType())
	return err
}

func maybeCreateAutoIncrement(
	ctx context.Context,
	db engine.Database,
	def *plan.TableDef,
	txnOp client.TxnOperator,
	nameResolver func() string) error {
	if def.TblId == 0 {
		name := def.Name
		if nameResolver != nil {
			name = nameResolver()
		}
		tb, err := db.Relation(ctx, name, nil)
		if err != nil {
			return err
		}
		def.TblId = tb.GetTableID(ctx)
	}
	cols := incrservice.GetAutoColumnFromDef(def)
	if len(cols) == 0 {
		return nil
	}
	return incrservice.GetAutoIncrementService().Create(
		ctx,
		def.TblId,
		cols,
		txnOp)
}

func getRelFromMoCatalog(c *Compile, tblName string) (engine.Relation, error) {
	dbSource, err := c.e.Database(c.ctx, catalog.MO_CATALOG, c.proc.TxnOperator)
	if err != nil {
		return nil, err
	}

	rel, err := dbSource.Relation(c.ctx, tblName, nil)
	if err != nil {
		return nil, err
	}

	return rel, nil
}

func getLockVector(proc *process.Process, accountId uint32, names []string) (*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(names)+1)
	defer func() {
		for _, v := range vecs {
			if v != nil {
				proc.PutVector(v)
			}
		}
	}()

	// append account_id
	accountIdVec := proc.GetVector(types.T_uint32.ToType())
	err := vector.AppendFixed(accountIdVec, accountId, false, proc.GetMPool())
	if err != nil {
		return nil, err
	}
	vecs[0] = accountIdVec
	// append names
	for i, name := range names {
		nameVec := proc.GetVector(types.T_varchar.ToType())
		err := vector.AppendBytes(nameVec, []byte(name), false, proc.GetMPool())
		if err != nil {
			return nil, err
		}
		vecs[i+1] = nameVec
	}

	vec, err := function.RunFunctionDirectly(proc, function.SerialFunctionEncodeID, vecs, 1)
	if err != nil {
		return nil, err
	}
	return vec, nil
}

func lockMoDatabase(c *Compile, dbName string) error {
	dbRel, err := getRelFromMoCatalog(c, catalog.MO_DATABASE)
	if err != nil {
		return err
	}
	vec, err := getLockVector(c.proc, c.proc.SessionInfo.AccountId, []string{dbName})
	if err != nil {
		return err
	}
	if err := lockRows(c.e, c.proc, dbRel, vec); err != nil {
		return err
	}
	return nil
}

func lockMoTable(c *Compile, dbName string, tblName string) error {
	dbRel, err := getRelFromMoCatalog(c, catalog.MO_TABLES)
	if err != nil {
		return err
	}
	vec, err := getLockVector(c.proc, c.proc.SessionInfo.AccountId, []string{dbName, tblName})
	if err != nil {
		return err
	}
	if err := lockRows(c.e, c.proc, dbRel, vec); err != nil {
		return err
	}
	return nil
}
