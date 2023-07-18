// Copyright 2023 Matrix Origin
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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (s *Scope) AlterTable3(c *Compile) error {
	qry := s.Plan.GetDdl().GetAlterTable()
	dbName := c.db
	tblName := qry.GetTableDef().GetName()

	// 1. lock origin table
	if err := lockMoTable(c, dbName, tblName); err != nil {
		return err
	}

	dbSource, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		return err
	}
	//databaseId := dbSource.GetDatabaseId(c.ctx)

	originRel, err := dbSource.Relation(c.ctx, tblName, nil)
	if err != nil {
		return err
	}

	// 2. create a replica table of the original table
	err = c.runSql(qry.NewDdlSql)
	if err != nil {
		return err
	}

	// 3. copy the original table data to the replica table
	// "insert into db1.t1_copy(a, b) select a, b from db1.t1;"
	//err = c.runSql(qry.RemoveDataSql)
	err = c.runSql("insert into db1.t1_copy(a, b) select a, b from db1.t1")
	if err != nil {
		return err
	}

	// 4. Migrate Foreign Key Dependencies

	// 5. drop original table
	if err = dbSource.Delete(c.ctx, tblName); err != nil {
		return err
	}
	//for _, name := range qry.IndexTableNames {
	//	if err := dbSource.Delete(c.ctx, name); err != nil {
	//		return err
	//	}
	//}

	// 6. rename copy table name to original table name
	//------------------------------------------------------------------------------------------------------------------
	replicaRel, err := dbSource.Relation(c.ctx, tblName+"_copy", nil)
	if err != nil {
		return err
	}

	var alterKind []api.AlterKind
	alterKind = addAlterKind(alterKind, api.AlterKind_RenameTable)
	oldName := replicaRel.GetTableName()
	newName := originRel.GetTableName()

	// reset origin table's constraint
	newCt := &engine.ConstraintDef{
		Cts: []engine.Constraint{},
	}

	//var addColIdx int
	//var dropColIdx int
	constraint := make([][]byte, 0)
	for _, kind := range alterKind {
		var req *api.AlterTableReq
		switch kind {
		case api.AlterKind_RenameTable:
			req = api.NewRenameTableReq(replicaRel.GetDBID(c.ctx), replicaRel.GetTableID(c.ctx), oldName, newName)
		default:
		}
		tmp, err := req.Marshal()
		if err != nil {
			return err
		}
		constraint = append(constraint, tmp)
	}

	err = replicaRel.AlterTable(c.ctx, newCt, constraint)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scope) AlterTable2(c *Compile) error {
	qry := s.Plan.GetDdl().GetAlterTable()
	dbName := c.db
	tblName := qry.GetTableDef().GetName()

	if err := lockMoTable(c, dbName, tblName); err != nil {
		return err
	}

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
