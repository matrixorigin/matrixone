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
	"strings"

	"go.uber.org/zap"
	"golang.org/x/exp/constraints"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (s *Scope) CreateDatabase(c *Compile) error {
	var span trace.Span
	c.proc.Ctx, span = trace.Start(c.proc.Ctx, "CreateDatabase")
	defer span.End()
	dbName := s.Plan.GetDdl().GetCreateDatabase().GetDatabase()
	if _, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator()); err == nil {
		if s.Plan.GetDdl().GetCreateDatabase().GetIfNotExists() {
			return nil
		}
		return moerr.NewDBAlreadyExists(c.proc.Ctx, dbName)
	}

	if err := lockMoDatabase(c, dbName); err != nil {
		return err
	}

	ctx := context.WithValue(c.proc.Ctx, defines.SqlKey{}, s.Plan.GetDdl().GetCreateDatabase().GetSql())
	datType := ""
	if s.Plan.GetDdl().GetCreateDatabase().SubscriptionOption != nil {
		datType = catalog.SystemDBTypeSubscription
	}
	ctx = context.WithValue(ctx, defines.DatTypKey{}, datType)
	return c.e.Create(ctx, dbName, c.proc.GetTxnOperator())
}

func (s *Scope) DropDatabase(c *Compile) error {
	dbName := s.Plan.GetDdl().GetDropDatabase().GetDatabase()
	if _, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator()); err != nil {
		if s.Plan.GetDdl().GetDropDatabase().GetIfExists() {
			return nil
		}
		return moerr.NewErrDropNonExistsDB(c.proc.Ctx, dbName)
	}

	if err := lockMoDatabase(c, dbName); err != nil {
		return err
	}

	sql := s.Plan.GetDdl().GetDropDatabase().GetCheckFKSql()
	if len(sql) != 0 {
		err := runDetectFkReferToDBSql(c, sql)
		if err != nil {
			return err
		}
	}

	// whether foreign_key_checks = 0 or 1
	err := s.removeFkeysRelationships(c, dbName)
	if err != nil {
		return err
	}

	err = c.e.Delete(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}

	// 1.delete all index object record under the database from mo_catalog.mo_indexes
	deleteSql := fmt.Sprintf(deleteMoIndexesWithDatabaseIdFormat, s.Plan.GetDdl().GetDropDatabase().GetDatabaseId())
	err = c.runSql(deleteSql)
	if err != nil {
		return err
	}

	// 2.delete all partition object record under the database from mo_catalog.mo_table_partitions
	deleteSql = fmt.Sprintf(deleteMoTablePartitionsWithDatabaseIdFormat, s.Plan.GetDdl().GetDropDatabase().GetDatabaseId())
	err = c.runSql(deleteSql)
	if err != nil {
		return err
	}

	//3. delete fks
	err = c.runSql(s.Plan.GetDdl().GetDropDatabase().GetUpdateFkSql())
	if err != nil {
		return err
	}
	return nil
}

func (s *Scope) removeFkeysRelationships(c *Compile, dbName string) error {
	database, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}

	relations, err := database.Relations(c.proc.Ctx)
	if err != nil {
		return err
	}
	for _, rel := range relations {
		relation, err := database.Relation(c.proc.Ctx, rel, nil)
		if err != nil {
			return err
		}
		tblId := relation.GetTableID(c.proc.Ctx)
		fkeys, refChild, err := s.getFkDefs(c, relation)
		if err != nil {
			return err
		}
		//remove tblId from the parent table
		for _, fkey := range fkeys.Fkeys {
			if fkey.ForeignTbl == 0 {
				continue
			}

			_, _, parentTable, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), fkey.ForeignTbl)
			if err != nil {
				return err
			}
			err = s.removeChildTblIdFromParentTable(c, parentTable, tblId)
			if err != nil {
				return err
			}
		}
		//remove tblId from the child table
		for _, childId := range refChild.Tables {
			if childId == 0 {
				continue
			}
			_, _, childTable, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), childId)
			if err != nil {
				return err
			}
			err = s.removeParentTblIdFromChildTable(c, childTable, tblId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Drop the old view, and create the new view.
func (s *Scope) AlterView(c *Compile) error {
	qry := s.Plan.GetDdl().GetAlterView()

	dbName := c.db
	tblName := qry.GetTableDef().GetName()

	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}
	if _, err = dbSource.Relation(c.proc.Ctx, tblName, nil); err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}

	if err := lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); err != nil {
		return err
	}

	// Drop view table.
	if err := dbSource.Delete(c.proc.Ctx, tblName); err != nil {
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

	// if _, err := dbSource.Relation(c.proc.Ctx, tblName); err == nil {
	//  	 return moerr.NewTableAlreadyExists(c.proc.Ctx, tblName)
	// }

	return dbSource.Create(context.WithValue(c.proc.Ctx, defines.SqlKey{}, c.sql), tblName, append(exeCols, exeDefs...))
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

	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	databaseId := dbSource.GetDatabaseId(c.proc.Ctx)

	rel, err := dbSource.Relation(c.proc.Ctx, tblName, nil)
	if err != nil {
		return err
	}
	tblId := rel.GetTableID(c.proc.Ctx)

	tableDef := plan2.DeepCopyTableDef(qry.TableDef, true)
	oldCt, err := GetConstraintDef(c.proc.Ctx, rel)
	if err != nil {
		return err
	}

	if c.proc.GetTxnOperator().Txn().IsPessimistic() {
		var retryErr error
		// 1. lock origin table metadata in catalog
		if err = lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return err
			}
			retryErr = err
		}

		// 2. lock origin table
		var partitionTableNames []string
		if tableDef.Partition != nil {
			partitionTableNames = tableDef.Partition.PartitionTableNames
		}
		if err = lockTable(c.proc.Ctx, c.e, c.proc, rel, dbName, partitionTableNames, true); err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return err
			}
			retryErr = err
		}
		if retryErr != nil {
			return retryErr
		}
	}
	newCt := &engine.ConstraintDef{
		Cts: []engine.Constraint{},
	}

	//added fk in this alter table statement
	newAddedFkNames := make(map[string]bool)
	/*
		collect old fk names.
		ForeignKeyDef.Name may be empty in previous design.
		So, we only use ForeignKeyDef.Name that is no empty.
	*/
	oldFkNames := make(map[string]bool)
	for _, ct := range oldCt.Cts {
		switch t := ct.(type) {
		case *engine.ForeignKeyDef:
			for _, fkey := range t.Fkeys {
				if len(fkey.Name) != 0 {
					oldFkNames[fkey.Name] = true
				}
			}
		}
	}

	removeRefChildTbls := make(map[string]uint64)
	var addRefChildTbls []uint64
	var newFkeys []*plan.ForeignKeyDef

	var addIndex []*plan.IndexDef
	var dropIndexMap = make(map[string]bool)
	var alterIndex *plan.IndexDef

	var alterKinds []api.AlterKind
	var comment string
	var oldName, newName string
	var addCol []*plan.AlterAddColumn
	var dropCol []*plan.AlterDropColumn
	var changePartitionDef *plan.PartitionByDef

	cols := tableDef.Cols
	// drop foreign key
	for _, action := range qry.Actions {
		switch act := action.Action.(type) {
		case *plan.AlterTable_Action_Drop:
			alterTableDrop := act.Drop
			constraintName := alterTableDrop.Name
			if alterTableDrop.Typ == plan.AlterTableDrop_FOREIGN_KEY {
				//check fk existed in table
				if _, has := oldFkNames[constraintName]; !has {
					return moerr.NewErrCantDropFieldOrKey(c.proc.Ctx, constraintName)
				}
				alterKinds = addAlterKind(alterKinds, api.AlterKind_UpdateConstraint)
				tableDef.Fkeys = plan2.RemoveIf[*plan.ForeignKeyDef](tableDef.Fkeys, func(fk *plan.ForeignKeyDef) bool {
					if fk.Name == constraintName {
						removeRefChildTbls[constraintName] = fk.ForeignTbl
						return true
					}
					return false
				})
			} else if alterTableDrop.Typ == plan.AlterTableDrop_INDEX {
				alterKinds = addAlterKind(alterKinds, api.AlterKind_UpdateConstraint)
				var notDroppedIndex []*plan.IndexDef
				for _, indexdef := range tableDef.Indexes {
					if indexdef.IndexName == constraintName {
						dropIndexMap[indexdef.IndexName] = true

						//1. drop index table
						if indexdef.TableExist {
							if _, err = dbSource.Relation(c.proc.Ctx, indexdef.IndexTableName, nil); err != nil {
								return err
							}
							if err = dbSource.Delete(c.proc.Ctx, indexdef.IndexTableName); err != nil {
								return err
							}
						}
						//2. delete index object from mo_catalog.mo_indexes
						deleteSql := fmt.Sprintf(deleteMoIndexesWithTableIdAndIndexNameFormat, tableDef.TblId, indexdef.IndexName)
						err = c.runSql(deleteSql)
						if err != nil {
							return err
						}
					} else {
						notDroppedIndex = append(notDroppedIndex, indexdef)
					}
				}
				// Avoid modifying slice directly during iteration
				tableDef.Indexes = notDroppedIndex
			} else if alterTableDrop.Typ == plan.AlterTableDrop_COLUMN {
				alterKinds = append(alterKinds, api.AlterKind_DropColumn)
				var idx int
				for idx = 0; idx < len(cols); idx++ {
					if cols[idx].Name == constraintName {
						drop := &plan.AlterDropColumn{
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
			//check fk existed in table
			if _, has := oldFkNames[act.AddFk.Fkey.Name]; has {
				return moerr.NewErrDuplicateKeyName(c.proc.Ctx, act.AddFk.Fkey.Name)
			}
			//check fk existed in this alter table statement
			if _, has := newAddedFkNames[act.AddFk.Fkey.Name]; has {
				return moerr.NewErrDuplicateKeyName(c.proc.Ctx, act.AddFk.Fkey.Name)
			}
			newAddedFkNames[act.AddFk.Fkey.Name] = true
			alterKinds = addAlterKind(alterKinds, api.AlterKind_UpdateConstraint)
			addRefChildTbls = append(addRefChildTbls, act.AddFk.Fkey.ForeignTbl)
			newFkeys = append(newFkeys, act.AddFk.Fkey)
		case *plan.AlterTable_Action_AddIndex:
			alterKinds = addAlterKind(alterKinds, api.AlterKind_UpdateConstraint)

			indexInfo := act.AddIndex.IndexInfo // IndexInfo is named same as planner's IndexInfo
			indexTableDef := act.AddIndex.IndexInfo.TableDef

			// indexName -> meta      -> indexDef
			//     		 -> centroids -> indexDef
			//     		 -> entries   -> indexDef
			multiTableIndexes := make(map[string]*MultiTableIndex)
			for _, indexDef := range indexTableDef.Indexes {

				for i := range addIndex {
					if indexDef.IndexName == addIndex[i].IndexName {
						return moerr.NewDuplicateKey(c.proc.Ctx, indexDef.IndexName)
					}
				}
				addIndex = append(addIndex, indexDef)

				if indexDef.Unique {
					// 1. Unique Index related logic
					err = s.handleUniqueIndexTable(c, indexDef, qry.Database, tableDef, indexInfo)
				} else if !indexDef.Unique && catalog.IsRegularIndexAlgo(indexDef.IndexAlgo) {
					// 2. Regular Secondary index
					err = s.handleRegularSecondaryIndexTable(c, indexDef, qry.Database, tableDef, indexInfo)
				} else if !indexDef.Unique && catalog.IsMasterIndexAlgo(indexDef.IndexAlgo) {
					// 3. Master index
					err = s.handleMasterIndexTable(c, indexDef, qry.Database, tableDef, indexInfo)
				} else if !indexDef.Unique && catalog.IsIvfIndexAlgo(indexDef.IndexAlgo) {
					// 4. IVF indexDefs are aggregated and handled later
					if _, ok := multiTableIndexes[indexDef.IndexName]; !ok {
						multiTableIndexes[indexDef.IndexName] = &MultiTableIndex{
							IndexAlgo: catalog.ToLower(indexDef.IndexAlgo),
							IndexDefs: make(map[string]*plan.IndexDef),
						}
					}
					multiTableIndexes[indexDef.IndexName].IndexDefs[catalog.ToLower(indexDef.IndexAlgoTableType)] = indexDef

				}
				if err != nil {
					return err
				}
			}
			for _, multiTableIndex := range multiTableIndexes {
				switch multiTableIndex.IndexAlgo { // no need for catalog.ToLower() here
				case catalog.MoIndexIvfFlatAlgo.ToString():
					err = s.handleVectorIvfFlatIndex(c, multiTableIndex.IndexDefs, qry.Database, tableDef, indexInfo)
				}

				if err != nil {
					return err
				}
			}

			//1. build and update constraint def
			for _, indexDef := range indexTableDef.Indexes {
				insertSql, err := makeInsertSingleIndexSQL(c.e, c.proc, databaseId, tblId, indexDef)
				if err != nil {
					return err
				}
				err = c.runSql(insertSql)
				if err != nil {
					return err
				}
			}
		case *plan.AlterTable_Action_AlterIndex:
			alterKinds = addAlterKind(alterKinds, api.AlterKind_UpdateConstraint)
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
		case *plan.AlterTable_Action_AlterReindex:
			// NOTE: We hold lock (with retry) during alter reindex, as "alter table" takes an exclusive lock
			//in the beginning for pessimistic mode. We need to see how to reduce the critical section.
			alterKinds = addAlterKind(alterKinds, api.AlterKind_UpdateConstraint)
			tableAlterIndex := act.AlterReindex
			constraintName := tableAlterIndex.IndexName
			multiTableIndexes := make(map[string]*MultiTableIndex)

			for i, indexDef := range tableDef.Indexes {
				if indexDef.IndexName == constraintName {
					alterIndex = indexDef

					// 1. Get old AlgoParams
					newAlgoParamsMap, err := catalog.IndexParamsStringToMap(alterIndex.IndexAlgoParams)
					if err != nil {
						return err
					}

					// 2.a update AlgoParams for the index to be re-indexed
					// NOTE: this will throw error if the algo type is not supported for reindex.
					// So Step 4. will not be executed if error is thrown here.
					indexAlgo := catalog.ToLower(alterIndex.IndexAlgo)
					switch catalog.ToLower(indexAlgo) {
					case catalog.MoIndexIvfFlatAlgo.ToString():
						newAlgoParamsMap[catalog.IndexAlgoParamLists] = fmt.Sprintf("%d", tableAlterIndex.IndexAlgoParamList)
					default:
						return moerr.NewInternalError(c.proc.Ctx, "invalid index algo type for alter reindex")
					}

					// 2.b generate new AlgoParams string
					newAlgoParams, err := catalog.IndexParamsMapToJsonString(newAlgoParamsMap)
					if err != nil {
						return err
					}

					// 3.a Update IndexDef and TableDef
					alterIndex.IndexAlgoParams = newAlgoParams
					tableDef.Indexes[i].IndexAlgoParams = newAlgoParams

					// 3.b Update mo_catalog.mo_indexes
					updateSql := fmt.Sprintf(updateMoIndexesAlgoParams, newAlgoParams, tableDef.TblId, alterIndex.IndexName)
					err = c.runSql(updateSql)
					if err != nil {
						return err
					}

					// 4. Add to multiTableIndexes
					if _, ok := multiTableIndexes[indexDef.IndexName]; !ok {
						multiTableIndexes[indexDef.IndexName] = &MultiTableIndex{
							IndexAlgo: catalog.ToLower(indexDef.IndexAlgo),
							IndexDefs: make(map[string]*plan.IndexDef),
						}
					}
					multiTableIndexes[indexDef.IndexName].IndexDefs[catalog.ToLower(indexDef.IndexAlgoTableType)] = indexDef
				}
			}

			if len(multiTableIndexes) != 1 {
				return moerr.NewInternalError(c.proc.Ctx, "invalid index algo type for alter reindex")
			}

			// update the hidden tables
			for _, multiTableIndex := range multiTableIndexes {
				switch multiTableIndex.IndexAlgo {
				case catalog.MoIndexIvfFlatAlgo.ToString():
					err = s.handleVectorIvfFlatIndex(c, multiTableIndex.IndexDefs, qry.Database, tableDef, nil)
				}

				if err != nil {
					return err
				}
			}
		case *plan.AlterTable_Action_AlterComment:
			alterKinds = addAlterKind(alterKinds, api.AlterKind_UpdateComment)
			comment = act.AlterComment.NewComment
		case *plan.AlterTable_Action_AlterName:
			alterKinds = addAlterKind(alterKinds, api.AlterKind_RenameTable)
			oldName = act.AlterName.OldName
			newName = act.AlterName.NewName
		case *plan.AlterTable_Action_AddColumn:
			alterKinds = append(alterKinds, api.AlterKind_AddColumn)
			col := &plan.ColDef{
				Name: act.AddColumn.Name,
				Alg:  plan.CompressType_Lz4,
				Typ:  act.AddColumn.Type,
			}
			var pos int32
			cols, pos, err = getAddColPos(cols, col, act.AddColumn.PreName, act.AddColumn.Pos)
			if err != nil {
				return err
			}
			act.AddColumn.Pos = pos
			addCol = append(addCol, act.AddColumn)
		case *plan.AlterTable_Action_AddPartition:
			alterKinds = append(alterKinds, api.AlterKind_AddPartition)
			changePartitionDef = act.AddPartition.PartitionDef
			partitionTables := act.AddPartition.GetPartitionTables()
			for _, table := range partitionTables {
				storageCols := planColsToExeCols(table.GetCols())
				storageDefs, err := planDefsToExeDefs(table)
				if err != nil {
					return err
				}
				err = dbSource.Create(c.proc.Ctx, table.GetName(), append(storageCols, storageDefs...))
				if err != nil {
					return err
				}
			}

			insertMoTablePartitionSql := genInsertMoTablePartitionsSql(databaseId, tblId, act.AddPartition.PartitionDef, act.AddPartition.Definitions)
			err = c.runSql(insertMoTablePartitionSql)
			if err != nil {
				return err
			}
		}
	}

	// reset origin table's constraint
	originHasFkDef := false
	originHasIndexDef := false
	for _, ct := range oldCt.Cts {
		switch t := ct.(type) {
		case *engine.ForeignKeyDef:
			for _, fkey := range t.Fkeys {
				//For compatibility, regenerate constraint name for the constraint with empty name.
				if len(fkey.Name) == 0 {
					fkey.Name = plan2.GenConstraintName()
					newFkeys = append(newFkeys, fkey)
				} else if _, ok := removeRefChildTbls[fkey.Name]; !ok {
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
			// NOTE: using map and remainingIndexes slice here to avoid "Modifying a Slice During Iteration".
			var remainingIndexes []*plan.IndexDef
			for _, idx := range t.Indexes {
				if !dropIndexMap[idx.IndexName] {
					remainingIndexes = append(remainingIndexes, idx)
				}
			}
			t.Indexes = remainingIndexes

			t.Indexes = append(t.Indexes, addIndex...)
			if alterIndex != nil {
				for i, idx := range t.Indexes {
					if alterIndex.IndexName == idx.IndexName {
						t.Indexes[i].Visible = alterIndex.Visible
						// NOTE: algo param is same for all the indexDefs of the same indexName.
						// ie for IVFFLAT: meta, centroids, entries all have same algo params.
						// so we don't need multiple `alterIndex`.
						t.Indexes[i].IndexAlgoParams = alterIndex.IndexAlgoParams
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
			Indexes: addIndex,
		})
	}

	var addColIdx int
	var dropColIdx int
	constraint := make([][]byte, 0)
	for _, kind := range alterKinds {
		var req *api.AlterTableReq
		switch kind {
		case api.AlterKind_UpdateConstraint:
			ct, err := newCt.MarshalBinary()
			if err != nil {
				return err
			}
			req = api.NewUpdateConstraintReq(rel.GetDBID(c.proc.Ctx), rel.GetTableID(c.proc.Ctx), string(ct))
		case api.AlterKind_UpdateComment:
			req = api.NewUpdateCommentReq(rel.GetDBID(c.proc.Ctx), rel.GetTableID(c.proc.Ctx), comment)
		case api.AlterKind_RenameTable:
			req = api.NewRenameTableReq(rel.GetDBID(c.proc.Ctx), rel.GetTableID(c.proc.Ctx), oldName, newName)
		case api.AlterKind_AddColumn:
			name := addCol[addColIdx].Name
			typ := &addCol[addColIdx].Type
			pos := addCol[addColIdx].Pos
			addColIdx++
			req = api.NewAddColumnReq(rel.GetDBID(c.proc.Ctx), rel.GetTableID(c.proc.Ctx), name, typ, pos)
		case api.AlterKind_DropColumn:
			req = api.NewRemoveColumnReq(rel.GetDBID(c.proc.Ctx), rel.GetTableID(c.proc.Ctx), dropCol[dropColIdx].Idx, dropCol[dropColIdx].Seq)
			dropColIdx++
		case api.AlterKind_AddPartition:
			req = api.NewAddPartitionReq(rel.GetDBID(c.proc.Ctx), rel.GetTableID(c.proc.Ctx), changePartitionDef)
		default:
		}
		tmp, err := req.Marshal()
		if err != nil {
			return err
		}
		constraint = append(constraint, tmp)
	}

	err = rel.AlterTable(c.proc.Ctx, newCt, constraint)
	if err != nil {
		return err
	}

	// remove refChildTbls for drop foreign key clause
	//remove the child table id -- tblId from the parent table -- fkTblId
	for _, fkTblId := range removeRefChildTbls {
		var fkRelation engine.Relation
		if fkTblId == 0 {
			//fk self refer
			fkRelation = rel
		} else {
			_, _, fkRelation, err = c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), fkTblId)
			if err != nil {
				return err
			}
		}

		err = s.removeChildTblIdFromParentTable(c, fkRelation, tblId)
		if err != nil {
			return err
		}
	}

	// append refChildTbls for add foreign key clause
	//add the child table id -- tblId into the parent table -- fkTblId
	for _, fkTblId := range addRefChildTbls {
		if fkTblId == 0 {
			//fk self refer
			err = AddChildTblIdToParentTable(c.proc.Ctx, rel, fkTblId)
			if err != nil {
				return err
			}
		} else {
			_, _, fkRelation, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), fkTblId)
			if err != nil {
				return err
			}
			err = AddChildTblIdToParentTable(c.proc.Ctx, fkRelation, tblId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Scope) CreateTable(c *Compile) error {
	qry := s.Plan.GetDdl().GetCreateTable()
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
	exeCols := planColsToExeCols(planCols)
	// TODO: debug for #11917
	if strings.Contains(qry.GetTableDef().GetName(), "sbtest") {
		c.proc.Info(c.proc.Ctx, "createTable",
			zap.String("databaseName", c.db),
			zap.String("tableName", qry.GetTableDef().GetName()),
			zap.String("txnID", c.proc.GetTxnOperator().Txn().DebugString()),
		)
	}

	// convert the plan's defs to the execution's defs
	exeDefs, err := planDefsToExeDefs(qry.GetTableDef())
	if err != nil {
		c.proc.Info(c.proc.Ctx, "createTable",
			zap.String("databaseName", c.db),
			zap.String("tableName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}

	dbName := c.db
	if qry.GetDatabase() != "" {
		dbName = qry.GetDatabase()
	}
	tblName := qry.GetTableDef().GetName()

	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if dbName == "" {
			// TODO: debug for #11917
			if strings.Contains(qry.GetTableDef().GetName(), "sbtest") {
				c.proc.Info(c.proc.Ctx, "createTable",
					zap.String("databaseName", c.db),
					zap.String("tableName", qry.GetTableDef().GetName()),
					zap.String("txnID", c.proc.GetTxnOperator().Txn().DebugString()),
				)
			}
			return moerr.NewNoDB(c.proc.Ctx)
		}
		// TODO: debug for #11917
		if strings.Contains(qry.GetTableDef().GetName(), "sbtest") {
			c.proc.Info(c.proc.Ctx, "createTable no exist",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.String("txnID", c.proc.GetTxnOperator().Txn().DebugString()),
			)
		}
		return err
	}
	if _, err := dbSource.Relation(c.proc.Ctx, tblName, nil); err == nil {
		if qry.GetIfNotExists() {
			// TODO: debug for #11917
			if strings.Contains(qry.GetTableDef().GetName(), "sbtest") {
				c.proc.Info(c.proc.Ctx, "createTable no exist",
					zap.String("databaseName", c.db),
					zap.String("tableName", qry.GetTableDef().GetName()),
					zap.String("txnID", c.proc.GetTxnOperator().Txn().DebugString()),
				)
			}
			return nil
		}

		c.proc.Info(c.proc.Ctx, "createTable",
			zap.String("databaseName", c.db),
			zap.String("tableName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return moerr.NewTableAlreadyExists(c.proc.Ctx, tblName)
	}

	// check in EntireEngine.TempEngine, notice that TempEngine may not init
	tmpDBSource, err := c.e.Database(c.proc.Ctx, defines.TEMPORARY_DBNAME, c.proc.GetTxnOperator())
	if err == nil {
		if _, err := tmpDBSource.Relation(c.proc.Ctx, engine.GetTempTableName(dbName, tblName), nil); err == nil {
			if qry.GetIfNotExists() {
				return nil
			}
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return moerr.NewTableAlreadyExists(c.proc.Ctx, fmt.Sprintf("temporary '%s'", tblName))
		}
	}

	if err := lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); err != nil {
		c.proc.Info(c.proc.Ctx, "createTable",
			zap.String("databaseName", c.db),
			zap.String("tableName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}

	if err := dbSource.Create(context.WithValue(c.proc.Ctx, defines.SqlKey{}, c.sql), tblName, append(exeCols, exeDefs...)); err != nil {
		c.proc.Info(c.proc.Ctx, "createTable",
			zap.String("databaseName", c.db),
			zap.String("tableName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}
	// TODO: debug for #11917
	if strings.Contains(qry.GetTableDef().GetName(), "sbtest") {
		c.proc.Info(c.proc.Ctx, "createTable ok",
			zap.String("databaseName", c.db),
			zap.String("tableName", qry.GetTableDef().GetName()),
			zap.String("txnID", c.proc.GetTxnOperator().Txn().DebugString()),
		)
	}

	partitionTables := qry.GetPartitionTables()
	for _, table := range partitionTables {
		storageCols := planColsToExeCols(table.GetCols())
		storageDefs, err := planDefsToExeDefs(table)
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
		err = dbSource.Create(c.proc.Ctx, table.GetName(), append(storageCols, storageDefs...))
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
	}

	//update mo_foreign_keys
	for _, sql := range qry.UpdateFkSqls {
		err = c.runSql(sql)
		if err != nil {
			return err
		}
	}

	// handle fk that refers to others tables
	fkDbs := qry.GetFkDbs()
	if len(fkDbs) > 0 {
		fkTables := qry.GetFkTables()
		//get the relation of created table above again.
		//due to the colId may be changed.
		newRelation, err := dbSource.Relation(c.proc.Ctx, tblName, nil)
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
		tblId := newRelation.GetTableID(c.proc.Ctx)

		newTableDef, err := newRelation.TableDefs(c.proc.Ctx)
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}

		oldCt := GetConstraintDefFromTableDefs(newTableDef)
		//get the columnId of the column from newTableDef
		var colNameToId = make(map[string]uint64)
		for _, def := range newTableDef {
			if attr, ok := def.(*engine.AttributeDef); ok {
				colNameToId[attr.Attr.Name] = attr.Attr.ID
			}
		}
		//old colId -> colName
		colId2Name := make(map[uint64]string)
		for _, col := range planCols {
			colId2Name[col.ColId] = col.Name
		}
		dedupFkName := make(plan2.UnorderedSet[string])
		//1. update fk info in child table.
		//column ids of column names in child table have changed after
		//the table is created by engine.Database.Create.
		//refresh column ids of column names in child table.
		newFkeys := make([]*plan.ForeignKeyDef, len(qry.GetTableDef().Fkeys))
		for i, fkey := range qry.GetTableDef().Fkeys {
			if dedupFkName.Find(fkey.Name) {
				return moerr.NewInternalError(c.proc.Ctx, "deduplicate fk name %s", fkey.Name)
			}
			dedupFkName.Insert(fkey.Name)
			newDef := &plan.ForeignKeyDef{
				Name:        fkey.Name,
				Cols:        make([]uint64, len(fkey.Cols)),
				ForeignTbl:  fkey.ForeignTbl,
				ForeignCols: make([]uint64, len(fkey.ForeignCols)),
				OnDelete:    fkey.OnDelete,
				OnUpdate:    fkey.OnUpdate,
			}
			copy(newDef.ForeignCols, fkey.ForeignCols)

			//if it is fk self, the parent table is same as the child table.
			//refresh the ForeignCols also.
			if fkey.ForeignTbl == 0 {
				for j, colId := range fkey.ForeignCols {
					//old colId -> colName
					colName := colId2Name[colId]
					//colName -> new colId
					newDef.ForeignCols[j] = colNameToId[colName]
				}
			} else {
				copy(newDef.ForeignCols, fkey.ForeignCols)
			}

			//refresh child table column id
			for idx, colName := range qry.GetFkCols()[i].Cols {
				newDef.Cols[idx] = colNameToId[colName]
			}
			newFkeys[i] = newDef
		}
		// remove old fk settings
		newCt, err := MakeNewCreateConstraint(oldCt, &engine.ForeignKeyDef{
			Fkeys: newFkeys,
		})
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
		err = newRelation.UpdateConstraint(c.proc.Ctx, newCt)
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}

		//2. need to append TableId to parent's TableDef.RefChildTbls
		for i, fkTableName := range fkTables {
			fkDbName := fkDbs[i]
			fkey := qry.GetTableDef().Fkeys[i]
			if fkey.ForeignTbl == 0 {
				//fk self refer
				//add current table to parent's children table
				err = AddChildTblIdToParentTable(c.proc.Ctx, newRelation, 0)
				if err != nil {
					c.proc.Info(c.proc.Ctx, "createTable",
						zap.String("databaseName", c.db),
						zap.String("tableName", qry.GetTableDef().GetName()),
						zap.Error(err),
					)
					return err
				}
				continue
			}
			fkDbSource, err := c.e.Database(c.proc.Ctx, fkDbName, c.proc.GetTxnOperator())
			if err != nil {
				c.proc.Info(c.proc.Ctx, "createTable",
					zap.String("databaseName", c.db),
					zap.String("tableName", qry.GetTableDef().GetName()),
					zap.Error(err),
				)
				return err
			}
			fkRelation, err := fkDbSource.Relation(c.proc.Ctx, fkTableName, nil)
			if err != nil {
				c.proc.Info(c.proc.Ctx, "createTable",
					zap.String("databaseName", c.db),
					zap.String("tableName", qry.GetTableDef().GetName()),
					zap.Error(err),
				)
				return err
			}
			//add current table to parent's children table
			err = AddChildTblIdToParentTable(c.proc.Ctx, fkRelation, tblId)
			if err != nil {
				c.proc.Info(c.proc.Ctx, "createTable",
					zap.String("databaseName", c.db),
					zap.String("tableName", qry.GetTableDef().GetName()),
					zap.Error(err),
				)
				return err
			}
		}
	}

	// handle fk forward reference
	fkRefersToMe := qry.GetFksReferToMe()
	if len(fkRefersToMe) > 0 {
		//1. get the relation of created table above again.
		//get the relation of created table above again.
		//due to the colId may be changed.
		newRelation, err := dbSource.Relation(c.proc.Ctx, tblName, nil)
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
		tblId := newRelation.GetTableID(c.proc.Ctx)

		newTableDef, err := newRelation.TableDefs(c.proc.Ctx)
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
		//get the columnId of the column from newTableDef
		var colNameToId = make(map[string]uint64)
		for _, def := range newTableDef {
			if attr, ok := def.(*engine.AttributeDef); ok {
				colNameToId[attr.Attr.Name] = attr.Attr.ID
			}
		}
		//1.1 update the column id of the column names in this table.
		//2. update fk info in the child table.
		for _, info := range fkRefersToMe {
			//update foreignCols in fk
			newDef := &plan.ForeignKeyDef{
				Name:        info.Def.Name,
				Cols:        make([]uint64, len(info.Def.Cols)),
				ForeignTbl:  tblId,
				ForeignCols: make([]uint64, len(info.Def.ForeignCols)),
				OnDelete:    info.Def.OnDelete,
				OnUpdate:    info.Def.OnUpdate,
			}
			//child table column ids of the child table
			copy(newDef.Cols, info.Def.Cols)
			//parent table column ids of the parent table
			for j, colReferred := range info.ColsReferred.Cols {
				//colName -> new colId
				if id, has := colNameToId[colReferred]; has {
					newDef.ForeignCols[j] = id
				} else {
					err := moerr.NewInternalError(c.proc.Ctx, "no column %s", colReferred)
					c.proc.Info(c.proc.Ctx, "createTable",
						zap.String("databaseName", c.db),
						zap.String("tableName", qry.GetTableDef().GetName()),
						zap.Error(err),
					)
					return err
				}
			}

			// add the fk def into the child table
			childDb, err := c.e.Database(c.proc.Ctx, info.Db, c.proc.GetTxnOperator())
			if err != nil {
				c.proc.Info(c.proc.Ctx, "createTable",
					zap.String("databaseName", c.db),
					zap.String("tableName", qry.GetTableDef().GetName()),
					zap.Error(err),
				)
				return err
			}
			childTable, err := childDb.Relation(c.proc.Ctx, info.Table, nil)
			if err != nil {
				c.proc.Info(c.proc.Ctx, "createTable",
					zap.String("databaseName", c.db),
					zap.String("tableName", qry.GetTableDef().GetName()),
					zap.Error(err),
				)
				return err
			}
			err = AddFkeyToRelation(c.proc.Ctx, childTable, newDef)
			if err != nil {
				c.proc.Info(c.proc.Ctx, "createTable",
					zap.String("databaseName", c.db),
					zap.String("tableName", qry.GetTableDef().GetName()),
					zap.Error(err),
				)
				return err
			}
			// add the child table id -- tblId into the current table -- refChildDef
			err = AddChildTblIdToParentTable(c.proc.Ctx, newRelation, childTable.GetTableID(c.proc.Ctx))
			if err != nil {
				c.proc.Info(c.proc.Ctx, "createTable",
					zap.String("databaseName", c.db),
					zap.String("tableName", qry.GetTableDef().GetName()),
					zap.Error(err),
				)
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
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
		if _, err := dbSource.Relation(c.proc.Ctx, def.Name, nil); err == nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return moerr.NewTableAlreadyExists(c.proc.Ctx, def.Name)
		}
		if err := dbSource.Create(c.proc.Ctx, def.Name, append(exeCols, exeDefs...)); err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}

		var initSQL string
		switch def.TableType {
		case catalog.SystemSI_IVFFLAT_TblType_Metadata:
			initSQL = fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`) VALUES('version', '0');",
				qry.Database,
				def.Name,
				catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
				catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
			)

		case catalog.SystemSI_IVFFLAT_TblType_Centroids:
			initSQL = fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`, `%s`) VALUES(0,1,NULL);",
				qry.Database,
				def.Name,
				catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
				catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
				catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
			)
		}
		err = c.runSql(initSQL)
		if err != nil {
			return err
		}

	}

	if checkIndexInitializable(dbName, tblName) {
		newRelation, err := dbSource.Relation(c.proc.Ctx, tblName, nil)
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
		insertSQL, err := makeInsertMultiIndexSQL(c.e, c.proc.Ctx, c.proc, dbSource, newRelation)
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
		err = c.runSql(insertSQL)
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}

		insertSQL2, err := makeInsertTablePartitionsSQL(c.proc.Ctx, dbSource, newRelation)
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
		err = c.runSql(insertSQL2)
		if err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}

	}

	err = maybeCreateAutoIncrement(
		c.proc.Ctx,
		dbSource,
		qry.GetTableDef(),
		c.proc.GetTxnOperator(),
		nil,
	)
	if err != nil {
		return err
	}

	if len(partitionTables) == 0 {
		return nil
	}

	return shardservice.GetService().Create(
		c.proc.Ctx,
		qry.GetTableDef().TblId,
		c.proc.GetTxnOperator(),
	)
}

func (s *Scope) CreateView(c *Compile) error {
	qry := s.Plan.GetDdl().GetCreateView()

	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
	exeCols := planColsToExeCols(planCols)

	// convert the plan's defs to the execution's defs
	exeDefs, err := planDefsToExeDefs(qry.GetTableDef())
	if err != nil {
		getLogger().Info("createView",
			zap.String("databaseName", c.db),
			zap.String("viewName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}

	dbName := c.db
	if qry.GetDatabase() != "" {
		dbName = qry.GetDatabase()
	}
	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if dbName == "" {
			return moerr.NewNoDB(c.proc.Ctx)
		}
		return err
	}

	viewName := qry.GetTableDef().GetName()
	if _, err = dbSource.Relation(c.proc.Ctx, viewName, nil); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}

		if qry.GetReplace() {
			err = c.runSql(fmt.Sprintf("drop view if exists %s", viewName))
			if err != nil {
				getLogger().Info("createView",
					zap.String("databaseName", c.db),
					zap.String("viewName", qry.GetTableDef().GetName()),
					zap.Error(err),
				)
				return err
			}
		} else {
			getLogger().Info("createView",
				zap.String("databaseName", c.db),
				zap.String("viewName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return moerr.NewTableAlreadyExists(c.proc.Ctx, viewName)
		}
	}

	// check in EntireEngine.TempEngine, notice that TempEngine may not init
	tmpDBSource, err := c.e.Database(c.proc.Ctx, defines.TEMPORARY_DBNAME, c.proc.GetTxnOperator())
	if err == nil {
		if _, err = tmpDBSource.Relation(c.proc.Ctx, engine.GetTempTableName(dbName, viewName), nil); err == nil {
			if qry.GetIfNotExists() {
				return nil
			}
			getLogger().Info("createView",
				zap.String("databaseName", c.db),
				zap.String("viewName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return moerr.NewTableAlreadyExists(c.proc.Ctx, fmt.Sprintf("temporary '%s'", viewName))
		}
	}

	if err = lockMoTable(c, dbName, viewName, lock.LockMode_Exclusive); err != nil {
		getLogger().Info("createView",
			zap.String("databaseName", c.db),
			zap.String("viewName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}

	if err = dbSource.Create(context.WithValue(c.proc.Ctx, defines.SqlKey{}, c.sql), viewName, append(exeCols, exeDefs...)); err != nil {
		getLogger().Info("createView",
			zap.String("databaseName", c.db),
			zap.String("viewName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}
	return nil
}

func checkIndexInitializable(dbName string, tblName string) bool {
	if dbName == catalog.MOTaskDB {
		return false
	} else if dbName == catalog.MO_CATALOG && strings.HasPrefix(tblName, catalog.MO_INDEXES) {
		// NOTE: this HasPrefix is very critical.
		// 1. When we do "alter table mo_index add col1, col2 after type",
		// 2. we create a new temporary mo_index_temp table. This mo_index_temp is same as mo_index table, with the new columns.
		// 3. Since the mo_index_temp is same as mo_index, it will have PrimaryKey(id, column_name), and this will result in a recursive behavior on mo_index table.
		// 4. Technically PrimaryKey(id, column_name) will be populated using genInsertMOIndexesSql which already contains both the 2 new columns that will be soon added by Sql based upgradeLogic.
		// 5. So, we need to skip the index table insert here.
		// TODO: verify if this logic is correct.
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
	tmpDBSource, err := c.e.Database(c.proc.Ctx, defines.TEMPORARY_DBNAME, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	tblName := qry.GetTableDef().GetName()
	if _, err := tmpDBSource.Relation(c.proc.Ctx, engine.GetTempTableName(dbName, tblName), nil); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}
		return moerr.NewTableAlreadyExists(c.proc.Ctx, fmt.Sprintf("temporary '%s'", tblName))
	}

	// check in EntireEngine.Engine
	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	if _, err := dbSource.Relation(c.proc.Ctx, tblName, nil); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}
		return moerr.NewTableAlreadyExists(c.proc.Ctx, tblName)
	}

	// create temporary table
	if err := tmpDBSource.Create(c.proc.Ctx, engine.GetTempTableName(dbName, tblName), append(exeCols, exeDefs...)); err != nil {
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
		if _, err := tmpDBSource.Relation(c.proc.Ctx, def.Name, nil); err == nil {
			return moerr.NewTableAlreadyExists(c.proc.Ctx, def.Name)
		}

		if err := tmpDBSource.Create(c.proc.Ctx, engine.GetTempTableName(dbName, def.Name), append(exeCols, exeDefs...)); err != nil {
			return err
		}
	}

	return maybeCreateAutoIncrement(
		c.proc.Ctx,
		tmpDBSource,
		qry.GetTableDef(),
		c.proc.GetTxnOperator(),
		func() string {
			return engine.GetTempTableName(dbName, tblName)
		})
}

func (s *Scope) CreateIndex(c *Compile) error {
	qry := s.Plan.GetDdl().GetCreateIndex()

	{
		// lockMoTable will lock Table  mo_catalog.mo_tables
		// for the row with db_name=dbName & table_name = tblName。
		dbName := c.db
		if qry.GetDatabase() != "" {
			dbName = qry.GetDatabase()
		}
		tblName := qry.GetTableDef().GetName()
		if err := lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); err != nil {
			return err
		}
	}

	d, err := c.e.Database(c.proc.Ctx, qry.Database, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	databaseId := d.GetDatabaseId(c.proc.Ctx)

	r, err := d.Relation(c.proc.Ctx, qry.Table, nil)
	if err != nil {
		return err
	}
	tableId := r.GetTableID(c.proc.Ctx)

	originalTableDef := plan2.DeepCopyTableDef(qry.TableDef, true)
	indexInfo := qry.GetIndex() // IndexInfo is named same as planner's IndexInfo
	indexTableDef := indexInfo.GetTableDef()

	// indexName -> meta      -> indexDef[0]
	//     		 -> centroids -> indexDef[1]
	//     		 -> entries   -> indexDef[2]
	multiTableIndexes := make(map[string]*MultiTableIndex)
	for _, indexDef := range indexTableDef.Indexes {

		indexAlgo := indexDef.IndexAlgo
		if indexDef.Unique {
			// 1. Unique Index related logic
			err = s.handleUniqueIndexTable(c, indexDef, qry.Database, originalTableDef, indexInfo)
		} else if !indexDef.Unique && catalog.IsRegularIndexAlgo(indexAlgo) {
			// 2. Regular Secondary index
			err = s.handleRegularSecondaryIndexTable(c, indexDef, qry.Database, originalTableDef, indexInfo)
		} else if !indexDef.Unique && catalog.IsMasterIndexAlgo(indexAlgo) {
			// 3. Master index
			err = s.handleMasterIndexTable(c, indexDef, qry.Database, originalTableDef, indexInfo)
		} else if !indexDef.Unique && catalog.IsIvfIndexAlgo(indexAlgo) {
			// 4. IVF indexDefs are aggregated and handled later
			if _, ok := multiTableIndexes[indexDef.IndexName]; !ok {
				multiTableIndexes[indexDef.IndexName] = &MultiTableIndex{
					IndexAlgo: catalog.ToLower(indexDef.IndexAlgo),
					IndexDefs: make(map[string]*plan.IndexDef),
				}
			}
			multiTableIndexes[indexDef.IndexName].IndexDefs[catalog.ToLower(indexDef.IndexAlgoTableType)] = indexDef
		}
		if err != nil {
			return err
		}
	}

	for _, multiTableIndex := range multiTableIndexes {
		switch multiTableIndex.IndexAlgo {
		case catalog.MoIndexIvfFlatAlgo.ToString():
			err = s.handleVectorIvfFlatIndex(c, multiTableIndex.IndexDefs, qry.Database, originalTableDef, indexInfo)
		}

		if err != nil {
			return err
		}
	}

	// build and update constraint def (no need to handle IVF related logic here)
	defs, err := planDefsToExeDefs(indexTableDef)
	if err != nil {
		return err
	}
	ct := defs[0].(*engine.ConstraintDef)

	oldCt, err := GetConstraintDef(c.proc.Ctx, r)
	if err != nil {
		return err
	}
	newCt, err := MakeNewCreateConstraint(oldCt, ct.Cts[0])
	if err != nil {
		return err
	}
	err = r.UpdateConstraint(c.proc.Ctx, newCt)
	if err != nil {
		return err
	}

	// generate inserts into mo_indexes metadata
	for _, indexDef := range indexTableDef.Indexes {
		sql, err := makeInsertSingleIndexSQL(c.e, c.proc, databaseId, tableId, indexDef)
		if err != nil {
			return err
		}
		err = c.runSql(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) handleVectorIvfFlatIndex(c *Compile, indexDefs map[string]*plan.IndexDef, qryDatabase string, originalTableDef *plan.TableDef, indexInfo *plan.CreateTable) error {
	if ok, err := s.isExperimentalEnabled(c, ivfFlatIndexFlag); err != nil {
		return err
	} else if !ok {
		return moerr.NewInternalErrorNoCtx("IVF index is not enabled")
	}

	// 1. static check
	if len(indexDefs) != 3 {
		return moerr.NewInternalErrorNoCtx("invalid ivf index table definition")
	} else if len(indexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].Parts) != 1 {
		return moerr.NewInternalErrorNoCtx("invalid ivf index table definition")
	}

	// 2. create hidden tables
	if indexInfo != nil {

		tables := make([]string, 3)
		tables[0] = genCreateIndexTableSqlForIvfIndex(indexInfo.GetIndexTables()[0], indexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata], qryDatabase)
		tables[1] = genCreateIndexTableSqlForIvfIndex(indexInfo.GetIndexTables()[1], indexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids], qryDatabase)
		tables[2] = genCreateIndexTableSqlForIvfIndex(indexInfo.GetIndexTables()[2], indexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries], qryDatabase)

		for _, createTableSql := range tables {
			err := c.runSql(createTableSql)
			if err != nil {
				return err
			}
		}
	}

	// 3. get count of secondary index column in original table
	totalCnt, err := s.handleIndexColCount(c, indexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata], qryDatabase, originalTableDef)
	if err != nil {
		return err
	}

	// 4.a populate meta table
	err = s.handleIvfIndexMetaTable(c, indexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata], qryDatabase)
	if err != nil {
		return err
	}

	// 4.b populate centroids table
	err = s.handleIvfIndexCentroidsTable(c, indexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids], qryDatabase, originalTableDef,
		totalCnt,
		indexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName)
	if err != nil {
		return err
	}

	// 4.c populate entries table
	err = s.handleIvfIndexEntriesTable(c, indexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries], qryDatabase, originalTableDef,
		indexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName,
		indexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName)
	if err != nil {
		return err
	}

	// 4.d delete older entries in index table.
	err = s.handleIvfIndexDeleteOldEntries(c,
		indexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName,
		indexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName,
		indexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName,
		qryDatabase)
	if err != nil {
		return err
	}

	return nil

}

func (s *Scope) DropIndex(c *Compile) error {
	qry := s.Plan.GetDdl().GetDropIndex()
	d, err := c.e.Database(c.proc.Ctx, qry.Database, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	r, err := d.Relation(c.proc.Ctx, qry.Table, nil)
	if err != nil {
		return err
	}

	//1. build and update constraint def
	oldCt, err := GetConstraintDef(c.proc.Ctx, r)
	if err != nil {
		return err
	}
	newCt, err := makeNewDropConstraint(oldCt, qry.GetIndexName())
	if err != nil {
		return err
	}
	err = r.UpdateConstraint(c.proc.Ctx, newCt)
	if err != nil {
		return err
	}

	//2. drop index table
	if qry.IndexTableName != "" {
		if _, err = d.Relation(c.proc.Ctx, qry.IndexTableName, nil); err != nil {
			return err
		}
		if err = d.Delete(c.proc.Ctx, qry.IndexTableName); err != nil {
			return err
		}
	}

	//3. delete index object from mo_catalog.mo_indexes
	deleteSql := fmt.Sprintf(deleteMoIndexesWithTableIdAndIndexNameFormat, r.GetTableID(c.proc.Ctx), qry.IndexName)
	err = c.runSql(deleteSql)
	if err != nil {
		return err
	}
	return nil
}

func makeNewDropConstraint(oldCt *engine.ConstraintDef, dropName string) (*engine.ConstraintDef, error) {
	// must fount dropName because of being checked in plan
	for i := 0; i < len(oldCt.Cts); i++ {
		ct := oldCt.Cts[i]
		switch def := ct.(type) {
		case *engine.ForeignKeyDef:
			pred := func(fkDef *plan.ForeignKeyDef) bool {
				return fkDef.Name == dropName
			}
			def.Fkeys = plan2.RemoveIf[*plan.ForeignKeyDef](def.Fkeys, pred)
			oldCt.Cts[i] = def
		case *engine.IndexDef:
			pred := func(index *plan.IndexDef) bool {
				return index.IndexName == dropName
			}
			def.Indexes = plan2.RemoveIf[*plan.IndexDef](def.Indexes, pred)
			oldCt.Cts[i] = def
		}
	}
	return oldCt, nil
}

func MakeNewCreateConstraint(oldCt *engine.ConstraintDef, c engine.Constraint) (*engine.ConstraintDef, error) {
	// duplication has checked in plan
	if oldCt == nil {
		return &engine.ConstraintDef{
			Cts: []engine.Constraint{c},
		}, nil
	}
	ok := false
	var pred func(engine.Constraint) bool
	switch t := c.(type) {
	case *engine.ForeignKeyDef:
		pred = func(ct engine.Constraint) bool {
			_, ok = ct.(*engine.ForeignKeyDef)
			return ok
		}
		oldCt.Cts = plan2.RemoveIf[engine.Constraint](oldCt.Cts, pred)
		oldCt.Cts = append(oldCt.Cts, c)
	case *engine.RefChildTableDef:
		pred = func(ct engine.Constraint) bool {
			_, ok = ct.(*engine.RefChildTableDef)
			return ok
		}
		oldCt.Cts = plan2.RemoveIf[engine.Constraint](oldCt.Cts, pred)
		oldCt.Cts = append(oldCt.Cts, c)
	case *engine.IndexDef:
		ok := false
		var indexdef *engine.IndexDef
		for i, ct := range oldCt.Cts {
			if indexdef, ok = ct.(*engine.IndexDef); ok {
				//TODO: verify if this is correct @ouyuanning & @qingx
				indexdef.Indexes = append(indexdef.Indexes, t.Indexes...)
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

func AddChildTblIdToParentTable(ctx context.Context, fkRelation engine.Relation, tblId uint64) error {
	oldCt, err := GetConstraintDef(ctx, fkRelation)
	if err != nil {
		return err
	}
	var oldRefChildDef *engine.RefChildTableDef
	for _, ct := range oldCt.Cts {
		if old, ok := ct.(*engine.RefChildTableDef); ok {
			oldRefChildDef = old
		}
	}
	if oldRefChildDef == nil {
		oldRefChildDef = &engine.RefChildTableDef{}
	}
	oldRefChildDef.Tables = append(oldRefChildDef.Tables, tblId)
	newCt, err := MakeNewCreateConstraint(oldCt, oldRefChildDef)
	if err != nil {
		return err
	}
	return fkRelation.UpdateConstraint(ctx, newCt)
}

func AddFkeyToRelation(ctx context.Context, fkRelation engine.Relation, fkey *plan.ForeignKeyDef) error {
	oldCt, err := GetConstraintDef(ctx, fkRelation)
	if err != nil {
		return err
	}
	var oldFkeys *engine.ForeignKeyDef
	for _, ct := range oldCt.Cts {
		if old, ok := ct.(*engine.ForeignKeyDef); ok {
			oldFkeys = old
		}
	}
	if oldFkeys == nil {
		oldFkeys = &engine.ForeignKeyDef{}
	}
	oldFkeys.Fkeys = append(oldFkeys.Fkeys, fkey)
	newCt, err := MakeNewCreateConstraint(oldCt, oldFkeys)
	if err != nil {
		return err
	}
	return fkRelation.UpdateConstraint(ctx, newCt)
}

// removeChildTblIdFromParentTable removes the tblId from the tableDef of fkRelation.
// input the fkRelation as the parameter instead of retrieving it again
// to embrace the fk self refer situation
func (s *Scope) removeChildTblIdFromParentTable(c *Compile, fkRelation engine.Relation, tblId uint64) error {
	oldCt, err := GetConstraintDef(c.proc.Ctx, fkRelation)
	if err != nil {
		return err
	}
	for _, ct := range oldCt.Cts {
		if def, ok := ct.(*engine.RefChildTableDef); ok {
			def.Tables = plan2.RemoveIf[uint64](def.Tables, func(id uint64) bool {
				return id == tblId
			})
			break
		}
	}
	return fkRelation.UpdateConstraint(c.proc.Ctx, oldCt)
}

func (s *Scope) removeParentTblIdFromChildTable(c *Compile, fkRelation engine.Relation, tblId uint64) error {
	oldCt, err := GetConstraintDef(c.proc.Ctx, fkRelation)
	if err != nil {
		return err
	}
	var oldFkeys *engine.ForeignKeyDef
	for _, ct := range oldCt.Cts {
		if old, ok := ct.(*engine.ForeignKeyDef); ok {
			oldFkeys = old
		}
	}
	if oldFkeys == nil {
		oldFkeys = &engine.ForeignKeyDef{}
	}
	newFkeys := &engine.ForeignKeyDef{}
	for _, fkey := range oldFkeys.Fkeys {
		if fkey.ForeignTbl != tblId {
			newFkeys.Fkeys = append(newFkeys.Fkeys, fkey)
		}
	}
	newCt, err := MakeNewCreateConstraint(oldCt, newFkeys)
	if err != nil {
		return err
	}
	return fkRelation.UpdateConstraint(c.proc.Ctx, newCt)
}

func (s *Scope) getFkDefs(c *Compile, fkRelation engine.Relation) (*engine.ForeignKeyDef, *engine.RefChildTableDef, error) {
	var oldFkeys *engine.ForeignKeyDef
	var oldRefChild *engine.RefChildTableDef
	oldCt, err := GetConstraintDef(c.proc.Ctx, fkRelation)
	if err != nil {
		return nil, nil, err
	}
	for _, ct := range oldCt.Cts {
		if old, ok := ct.(*engine.ForeignKeyDef); ok {
			oldFkeys = old
		} else if refChild, ok := ct.(*engine.RefChildTableDef); ok {
			oldRefChild = refChild
		}
	}
	if oldFkeys == nil {
		oldFkeys = &engine.ForeignKeyDef{}
	}
	if oldRefChild == nil {
		oldRefChild = &engine.RefChildTableDef{}
	}
	return oldFkeys, oldRefChild, nil
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
	keepAutoIncrement := false
	affectedRows := uint64(0)

	dbSource, err = c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}

	if rel, err = dbSource.Relation(c.proc.Ctx, tblName, nil); err != nil {
		var e error // avoid contamination of error messages
		dbSource, e = c.e.Database(c.proc.Ctx, defines.TEMPORARY_DBNAME, c.proc.GetTxnOperator())
		if e != nil {
			return err
		}
		rel, e = dbSource.Relation(c.proc.Ctx, engine.GetTempTableName(dbName, tblName), nil)
		if e != nil {
			return err
		}
		isTemp = true
	}

	if !isTemp && c.proc.GetTxnOperator().Txn().IsPessimistic() {
		var err error
		if e := lockMoTable(c, dbName, tblName, lock.LockMode_Shared); e != nil {
			if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return e
			}
			err = e
		}
		// before dropping table, lock it.
		if e := lockTable(c.proc.Ctx, c.e, c.proc, rel, dbName, tqry.PartitionTableNames, false); e != nil {
			if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return e
			}
			err = e
		}
		if err != nil {
			return err
		}
	}

	if tqry.IsDelete {
		keepAutoIncrement = true
		affectedRows, err = rel.Rows(c.proc.Ctx)
		if err != nil {
			return err
		}
	}

	if isTemp {
		// memoryengine truncate always return 0, so for temporary table, just use origin tableId as newId
		_, err = dbSource.Truncate(c.proc.Ctx, engine.GetTempTableName(dbName, tblName))
		newId = rel.GetTableID(c.proc.Ctx)
	} else {
		newId, err = dbSource.Truncate(c.proc.Ctx, tblName)
	}

	if err != nil {
		return err
	}

	// Truncate Index Tables if needed
	for _, name := range tqry.IndexTableNames {
		var err error
		if isTemp {
			_, err = dbSource.Truncate(c.proc.Ctx, engine.GetTempTableName(dbName, name))
		} else {
			_, err = dbSource.Truncate(c.proc.Ctx, name)
		}
		if err != nil {
			return err
		}
	}

	//Truncate Partition subtable if needed
	for _, name := range tqry.PartitionTableNames {
		var err error
		if isTemp {
			_, err = dbSource.Truncate(c.proc.Ctx, engine.GetTempTableName(dbName, name))
		} else {
			_, err = dbSource.Truncate(c.proc.Ctx, name)
		}
		if err != nil {
			return err
		}
	}

	// update tableDef of foreign key's table with new table id
	for _, ftblId := range tqry.ForeignTbl {
		_, _, fkRelation, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), ftblId)
		if err != nil {
			return err
		}
		oldCt, err := GetConstraintDef(c.proc.Ctx, fkRelation)
		if err != nil {
			return err
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
		err = fkRelation.UpdateConstraint(c.proc.Ctx, oldCt)
		if err != nil {
			return err
		}

	}

	if isTemp {
		oldId = rel.GetTableID(c.proc.Ctx)
	}

	// check if contains any auto_increment column(include __mo_fake_pk_col), if so, reset the auto_increment value
	tblDef := rel.GetTableDef(c.proc.Ctx)
	var containAuto bool
	for _, col := range tblDef.Cols {
		if col.Typ.AutoIncr {
			containAuto = true
			break
		}
	}
	if containAuto {
		err = incrservice.GetAutoIncrementService(c.proc.Ctx).Reset(
			c.proc.Ctx,
			oldId,
			newId,
			keepAutoIncrement,
			c.proc.GetTxnOperator())
		if err != nil {
			return err
		}
	}

	// update index information in mo_catalog.mo_indexes
	updateSql := fmt.Sprintf(updateMoIndexesTruncateTableFormat, newId, oldId)
	err = c.runSql(updateSql)
	if err != nil {
		return err
	}
	c.addAffectedRows(uint64(affectedRows))
	return nil
}

func (s *Scope) DropSequence(c *Compile) error {
	qry := s.Plan.GetDdl().GetDropSequence()
	dbName := qry.GetDatabase()
	var dbSource engine.Database
	var err error

	tblName := qry.GetTable()
	dbSource, err = c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}

	var rel engine.Relation
	if rel, err = dbSource.Relation(c.proc.Ctx, tblName, nil); err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}

	if err := lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); err != nil {
		return err
	}

	// Delete the stored session value.
	c.proc.GetSessionInfo().SeqDeleteKeys = append(c.proc.GetSessionInfo().SeqDeleteKeys, rel.GetTableID(c.proc.Ctx))

	return dbSource.Delete(c.proc.Ctx, tblName)
}

func (s *Scope) DropTable(c *Compile) error {
	qry := s.Plan.GetDdl().GetDropTable()
	dbName := qry.GetDatabase()
	tblName := qry.GetTable()
	isView := qry.GetIsView()
	var isSource = false
	if qry.TableDef != nil {
		isSource = qry.TableDef.TableType == catalog.SystemSourceRel
	}
	var dbSource engine.Database
	var rel engine.Relation
	var err error
	var isTemp bool

	tblId := qry.GetTableId()

	dbSource, err = c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}

	if rel, err = dbSource.Relation(c.proc.Ctx, tblName, nil); err != nil {
		var e error // avoid contamination of error messages
		dbSource, e = c.e.Database(c.proc.Ctx, defines.TEMPORARY_DBNAME, c.proc.GetTxnOperator())
		if dbSource == nil && qry.GetIfExists() {
			return nil
		} else if e != nil {
			return err
		}
		rel, e = dbSource.Relation(c.proc.Ctx, engine.GetTempTableName(dbName, tblName), nil)
		if e != nil {
			if qry.GetIfExists() {
				return nil
			} else {
				return err
			}
		}
		isTemp = true
	}

	if !isTemp && !isView && !isSource && c.proc.GetTxnOperator().Txn().IsPessimistic() {
		var err error
		if e := lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); e != nil {
			if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return e
			}
			err = e
		}
		// before dropping table, lock it.
		if e := lockTable(c.proc.Ctx, c.e, c.proc, rel, dbName, qry.PartitionTableNames, false); e != nil {
			if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return e
			}
			err = e
		}
		if err != nil {
			return err
		}
	}

	if len(qry.UpdateFkSqls) > 0 {
		for _, sql := range qry.UpdateFkSqls {
			if err = c.runSql(sql); err != nil {
				return err
			}
		}
	}

	// update tableDef of foreign key's table
	//remove the child table id -- tblId from the parent table -- fkTblId
	for _, fkTblId := range qry.ForeignTbl {
		if fkTblId == 0 {
			//fk self refer
			continue
		}
		_, _, fkRelation, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), fkTblId)
		if err != nil {
			return err
		}

		err = s.removeChildTblIdFromParentTable(c, fkRelation, tblId)
		if err != nil {
			return err
		}
	}

	//remove parent table id from the child table (when foreign_key_checks is disabled)
	for _, childTblId := range qry.FkChildTblsReferToMe {
		if childTblId == 0 {
			continue
		}
		_, _, childRelation, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), childTblId)
		if err != nil {
			return err
		}
		err = s.removeParentTblIdFromChildTable(c, childRelation, tblId)
		if err != nil {
			return err
		}
	}

	// delete all index objects record of the table in mo_catalog.mo_indexes
	if !qry.IsView && qry.Database != catalog.MO_CATALOG && qry.Table != catalog.MO_INDEXES {
		if qry.GetTableDef().Pkey != nil || len(qry.GetTableDef().Indexes) > 0 {
			deleteSql := fmt.Sprintf(deleteMoIndexesWithTableIdFormat, qry.GetTableDef().TblId)
			err = c.runSql(deleteSql)
			if err != nil {
				return err
			}
		}
	}

	// delete all partition objects record of the table in mo_catalog.mo_table_partitions
	if !qry.IsView && qry.Database != catalog.MO_CATALOG && qry.Table != catalog.MO_TABLE_PARTITIONS {
		if qry.TableDef.Partition != nil {
			deleteSql := fmt.Sprintf(deleteMoTablePartitionsWithTableIdFormat, qry.GetTableDef().TblId)
			err = c.runSql(deleteSql)
			if err != nil {
				return err
			}
		}
	}

	if isTemp {
		if err := dbSource.Delete(c.proc.Ctx, engine.GetTempTableName(dbName, tblName)); err != nil {
			return err
		}
		for _, name := range qry.IndexTableNames {
			if err := dbSource.Delete(c.proc.Ctx, name); err != nil {
				return err
			}
		}

		//delete partition table
		for _, name := range qry.GetPartitionTableNames() {
			if err = dbSource.Delete(c.proc.Ctx, name); err != nil {
				return err
			}
		}

		if dbName != catalog.MO_CATALOG && tblName != catalog.MO_INDEXES {
			tblDef := rel.GetTableDef(c.proc.Ctx)
			var containAuto bool
			for _, col := range tblDef.Cols {
				if col.Typ.AutoIncr {
					containAuto = true
					break
				}
			}
			if containAuto {
				err := incrservice.GetAutoIncrementService(c.proc.Ctx).Delete(
					c.proc.Ctx,
					rel.GetTableID(c.proc.Ctx),
					c.proc.GetTxnOperator())
				if err != nil {
					return err
				}
			}

			if err := shardservice.GetService().Delete(
				c.proc.Ctx,
				rel.GetTableID(c.proc.Ctx),
				c.proc.GetTxnOperator(),
			); err != nil {
				return err
			}
		}

	} else {
		if err := dbSource.Delete(c.proc.Ctx, tblName); err != nil {
			return err
		}
		for _, name := range qry.IndexTableNames {
			if err := dbSource.Delete(c.proc.Ctx, name); err != nil {
				return err
			}
		}

		// delete partition subtable
		for _, name := range qry.GetPartitionTableNames() {
			if err = dbSource.Delete(c.proc.Ctx, name); err != nil {
				return err
			}
		}

		if dbName != catalog.MO_CATALOG && tblName != catalog.MO_INDEXES {
			tblDef := rel.GetTableDef(c.proc.Ctx)
			var containAuto bool
			for _, col := range tblDef.Cols {
				if col.Typ.AutoIncr {
					containAuto = true
					break
				}
			}
			if containAuto {
				// When drop table 'mo_catalog.mo_indexes', there is no need to delete the auto increment data
				err := incrservice.GetAutoIncrementService(c.proc.Ctx).Delete(
					c.proc.Ctx,
					rel.GetTableID(c.proc.Ctx),
					c.proc.GetTxnOperator())
				if err != nil {
					return err
				}
			}

			if err := shardservice.GetService().Delete(
				c.proc.Ctx,
				rel.GetTableID(c.proc.Ctx),
				c.proc.GetTxnOperator(),
			); err != nil {
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
			c.Cts = append(c.Cts, &engine.StreamConfigsDef{
				Configs: defVal.Properties.GetProperties(),
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
				EnumVlaues:    colTyp.GetEnumvalues(),
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

	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if dbName == "" {
			return moerr.NewNoDB(c.proc.Ctx)
		}
		return err
	}

	if _, err := dbSource.Relation(c.proc.Ctx, tblName, nil); err == nil {
		if qry.GetIfNotExists() {
			return nil
		}
		// Just report table exists error.
		return moerr.NewTableAlreadyExists(c.proc.Ctx, tblName)
	}

	if err := lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); err != nil {
		return err
	}

	if err := dbSource.Create(context.WithValue(c.proc.Ctx, defines.SqlKey{}, c.sql), tblName, append(exeCols, exeDefs...)); err != nil {
		return err
	}

	// Init the only row of sequence.
	if rel, err := dbSource.Relation(c.proc.Ctx, tblName, nil); err == nil {
		if rel == nil {
			return moerr.NewTableAlreadyExists(c.proc.Ctx, tblName)
		}
		bat, err := makeSequenceInitBatch(c.proc.Ctx, c.stmt.(*tree.CreateSequence), qry.GetTableDef(), c.proc)
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

func (s *Scope) AlterSequence(c *Compile) error {
	var values []interface{}
	var curval string
	qry := s.Plan.GetDdl().GetAlterSequence()
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

	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if dbName == "" {
			return moerr.NewNoDB(c.proc.Ctx)
		}
		return err
	}

	if rel, err := dbSource.Relation(c.proc.Ctx, tblName, nil); err == nil {
		// sequence table exists
		// get pre sequence table row values
		_values, err := c.proc.GetSessionInfo().SqlHelper.ExecSql(fmt.Sprintf("select * from `%s`.`%s`", dbName, tblName))
		if err != nil {
			return err
		}
		if _values == nil {
			return moerr.NewInternalError(c.proc.Ctx, "Failed to get sequence meta data.")
		}
		values = _values[0]

		// get pre curval

		curval = c.proc.GetSessionInfo().SeqCurValues[rel.GetTableID(c.proc.Ctx)]
		// dorp the pre sequence
		err = c.runSql(fmt.Sprintf("drop sequence %s", tblName))
		if err != nil {
			return err
		}
	} else {
		// sequence table not exists
		if qry.GetIfExists() {
			return nil
		}
		return moerr.NewInternalError(c.proc.Ctx, "sequence %s not exists", tblName)
	}

	if err := lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); err != nil {
		return err
	}

	if err := dbSource.Create(context.WithValue(c.proc.Ctx, defines.SqlKey{}, c.sql), tblName, append(exeCols, exeDefs...)); err != nil {
		return err
	}

	//Init the only row of sequence.
	if rel, err := dbSource.Relation(c.proc.Ctx, tblName, nil); err == nil {
		if rel == nil {
			return moerr.NewLockTableNotFound(c.proc.Ctx)
		}
		bat, err := makeSequenceAlterBatch(c.proc.Ctx, c.stmt.(*tree.AlterSequence), qry.GetTableDef(), c.proc, values, curval)
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

func makeSequenceAlterBatch(ctx context.Context, stmt *tree.AlterSequence, tableDef *plan.TableDef, proc *process.Process, result []interface{}, curval string) (*batch.Batch, error) {
	var bat batch.Batch
	bat.Ro = true
	bat.Cnt = 0
	bat.SetRowCount(1)
	attrs := make([]string, len(plan2.Sequence_cols_name))
	for i := range attrs {
		attrs[i] = plan2.Sequence_cols_name[i]
	}
	bat.Attrs = attrs

	// typ is sequenece's type now
	typ := plan2.MakeTypeByPlan2Type(tableDef.Cols[0].Typ)
	vecs := make([]*vector.Vector, len(plan2.Sequence_cols_name))

	switch typ.Oid {
	case types.T_int16:
		lastV, incr, minV, maxV, startN, cycle, err := makeAlterSequenceParam[int16](ctx, stmt, result, curval)
		if err != nil {
			return nil, err
		}
		if maxV < 0 {
			maxV = math.MaxInt16
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeAlterSequenceVecs(vecs, typ, proc, incr, lastV, minV, maxV, startN, cycle)
		if err != nil {
			return nil, err
		}
	case types.T_int32:
		lastV, incr, minV, maxV, startN, cycle, err := makeAlterSequenceParam[int32](ctx, stmt, result, curval)
		if err != nil {
			return nil, err
		}
		if maxV < 0 {
			maxV = math.MaxInt32
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeAlterSequenceVecs(vecs, typ, proc, incr, lastV, minV, maxV, startN, cycle)
		if err != nil {
			return nil, err
		}
	case types.T_int64:
		lastV, incr, minV, maxV, startN, cycle, err := makeAlterSequenceParam[int64](ctx, stmt, result, curval)
		if err != nil {
			return nil, err
		}
		if maxV < 0 {
			maxV = math.MaxInt64
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeAlterSequenceVecs(vecs, typ, proc, incr, lastV, minV, maxV, startN, cycle)
		if err != nil {
			return nil, err
		}
	case types.T_uint16:
		lastV, incr, minV, maxV, startN, cycle, err := makeAlterSequenceParam[uint16](ctx, stmt, result, curval)
		if err != nil {
			return nil, err
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeAlterSequenceVecs(vecs, typ, proc, incr, lastV, minV, maxV, startN, cycle)
		if err != nil {
			return nil, err
		}
	case types.T_uint32:
		lastV, incr, minV, maxV, startN, cycle, err := makeAlterSequenceParam[uint32](ctx, stmt, result, curval)
		if err != nil {
			return nil, err
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeAlterSequenceVecs(vecs, typ, proc, incr, lastV, minV, maxV, startN, cycle)
		if err != nil {
			return nil, err
		}
	case types.T_uint64:
		lastV, incr, minV, maxV, startN, cycle, err := makeAlterSequenceParam[uint64](ctx, stmt, result, curval)
		if err != nil {
			return nil, err
		}
		err = valueCheckOut(maxV, minV, startN, ctx)
		if err != nil {
			return nil, err
		}
		err = makeAlterSequenceVecs(vecs, typ, proc, incr, lastV, minV, maxV, startN, cycle)
		if err != nil {
			return nil, err
		}
	default:
		return nil, moerr.NewNotSupported(ctx, "Unsupported type for sequence")
	}
	bat.Vecs = vecs
	return &bat, nil
}

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

func makeSequenceVecs[T constraints.Integer](vecs []*vector.Vector, stmt *tree.CreateSequence, typ types.Type, proc *process.Process, incr int64, minV, maxV, startN T) (err error) {
	defer func() {
		if err != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(proc.Mp())
				}
			}
		}
	}()

	if vecs[0], err = vector.NewConstFixed(typ, startN, 1, proc.Mp()); err != nil {
		return err
	}
	if vecs[1], err = vector.NewConstFixed(typ, minV, 1, proc.Mp()); err != nil {
		return err
	}
	if vecs[2], err = vector.NewConstFixed(typ, maxV, 1, proc.Mp()); err != nil {
		return err
	}
	if vecs[3], err = vector.NewConstFixed(typ, startN, 1, proc.Mp()); err != nil {
		return err
	}
	if vecs[4], err = vector.NewConstFixed(types.T_int64.ToType(), incr, 1, proc.Mp()); err != nil {
		return err
	}
	if stmt.Cycle {
		vecs[5], err = vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp())
	} else {
		vecs[5], err = vector.NewConstFixed(types.T_bool.ToType(), false, 1, proc.Mp())
	}
	if err != nil {
		return err
	}
	vecs[6], err = vector.NewConstFixed(types.T_bool.ToType(), false, 1, proc.Mp())
	return err
}

func makeAlterSequenceVecs[T constraints.Integer](vecs []*vector.Vector, typ types.Type, proc *process.Process, incr int64, lastV, minV, maxV, startN T, cycle bool) (err error) {
	defer func() {
		if err != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(proc.Mp())
				}
			}
		}
	}()

	if vecs[0], err = vector.NewConstFixed(typ, lastV, 1, proc.Mp()); err != nil {
		return err
	}
	if vecs[1], err = vector.NewConstFixed(typ, minV, 1, proc.Mp()); err != nil {
		return err
	}
	if vecs[2], err = vector.NewConstFixed(typ, maxV, 1, proc.Mp()); err != nil {
		return err
	}
	if vecs[3], err = vector.NewConstFixed(typ, startN, 1, proc.Mp()); err != nil {
		return err
	}
	if vecs[4], err = vector.NewConstFixed(types.T_int64.ToType(), incr, 1, proc.Mp()); err != nil {
		return err
	}
	if vecs[5], err = vector.NewConstFixed(types.T_bool.ToType(), cycle, 1, proc.Mp()); err != nil {
		return err
	}
	vecs[6], err = vector.NewConstFixed(types.T_bool.ToType(), false, 1, proc.Mp())
	return err
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

func makeAlterSequenceParam[T constraints.Integer](ctx context.Context, stmt *tree.AlterSequence, result []interface{}, curval string) (T, int64, T, T, T, bool, error) {
	var minValue, maxValue, startNum, lastNum T
	var incrNum int64
	var cycle bool

	if incr, ok := result[4].(int64); ok {
		incrNum = incr
	}

	// if alter increment value
	if stmt.IncrementBy != nil {
		switch stmt.IncrementBy.Num.(type) {
		case uint64:
			return 0, 0, 0, 0, 0, false, moerr.NewInvalidInput(ctx, "incr value's data type is int64")
		}
		incrNum = getValue[int64](stmt.IncrementBy.Minus, stmt.IncrementBy.Num)
	}
	if incrNum == 0 {
		return 0, 0, 0, 0, 0, false, moerr.NewInvalidInput(ctx, "Incr value for sequence must not be 0")
	}

	// if alter minValue of sequence
	preMinValue := result[1]
	if stmt.MinValue != nil {
		minValue = getValue[T](stmt.MinValue.Minus, stmt.MinValue.Num)
	} else {
		minValue = getInterfaceValue[T](preMinValue)
	}

	// if alter maxValue of sequence
	preMaxValue := result[2]
	if stmt.MaxValue != nil {
		maxValue = getValue[T](stmt.MaxValue.Minus, stmt.MaxValue.Num)
	} else {
		maxValue = getInterfaceValue[T](preMaxValue)
	}

	preLastSeq := result[0]
	preLastSeqNum := getInterfaceValue[T](preLastSeq)
	// if alter startWith value of sequence
	preStartWith := preLastSeqNum
	if stmt.StartWith != nil {
		startNum = getValue[T](stmt.StartWith.Minus, stmt.StartWith.Num)
		if startNum < preStartWith {
			startNum = preStartWith
		}
	} else {
		startNum = getInterfaceValue[T](preStartWith)
	}
	if len(curval) != 0 {
		lastNum = preLastSeqNum + T(incrNum)
		if lastNum < startNum+T(incrNum) {
			lastNum = startNum + T(incrNum)
		}
	} else {
		lastNum = preLastSeqNum
	}

	// if alter cycle state of sequence
	preCycle := result[5]
	if preCycleVal, ok := preCycle.(bool); ok {
		if stmt.Cycle != nil {
			cycle = stmt.Cycle.Cycle
		} else {
			cycle = preCycleVal
		}
	}

	return lastNum, incrNum, minValue, maxValue, startNum, cycle, nil
}

// Checkout values.
func valueCheckOut[T constraints.Integer](maxValue, minValue, startNum T, ctx context.Context) error {
	if maxValue <= minValue {
		return moerr.NewInvalidInput(ctx, "MAXVALUE (%d) of sequence must be bigger than MINVALUE (%d) of it", maxValue, minValue)
	}
	if startNum < minValue || startNum > maxValue {
		return moerr.NewInvalidInput(ctx, "STARTVALUE (%d) for sequence must between MINVALUE (%d) and MAXVALUE (%d)", startNum, minValue, maxValue)
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

func getInterfaceValue[T constraints.Integer](val interface{}) T {
	switch val := val.(type) {
	case int16:
		return T(val)
	case int32:
		return T(val)
	case int64:
		return T(val)
	case uint16:
		return T(val)
	case uint32:
		return T(val)
	case uint64:
		return T(val)
	}
	return 0
}

func doLockTable(
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation,
	defChanged bool) error {
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
		defs[0].Type,
		defChanged)

	return err
}

func lockTable(
	ctx context.Context,
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation,
	dbName string,
	partitionTableNames []string,
	defChanged bool) error {

	if len(partitionTableNames) == 0 {
		return doLockTable(eng, proc, rel, defChanged)
	}

	dbSource, err := eng.Database(ctx, dbName, proc.GetTxnOperator())
	if err != nil {
		return err
	}

	for _, tableName := range partitionTableNames {
		pRel, pErr := dbSource.Relation(ctx, tableName, nil)
		if pErr != nil {
			return pErr
		}
		err = doLockTable(eng, proc, pRel, defChanged)
		if err != nil {
			return err
		}
	}
	return nil
}

func lockRows(
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation,
	vec *vector.Vector,
	lockMode lock.LockMode,
	sharding lock.Sharding,
	group uint32) error {
	if vec == nil || vec.Length() == 0 {
		panic("lock rows is empty")
	}

	id := rel.GetTableID(proc.Ctx)

	err := lockop.LockRows(
		eng,
		proc,
		rel,
		id,
		vec,
		*vec.GetType(),
		lockMode,
		sharding,
		group)
	return err
}

func maybeCreateAutoIncrement(
	ctx context.Context,
	db engine.Database,
	def *plan.TableDef,
	txnOp client.TxnOperator,
	nameResolver func() string) error {
	name := def.Name
	if nameResolver != nil {
		name = nameResolver()
	}
	tb, err := db.Relation(ctx, name, nil)
	if err != nil {
		return err
	}
	def.TblId = tb.GetTableID(ctx)

	cols := incrservice.GetAutoColumnFromDef(def)
	if len(cols) == 0 {
		return nil
	}

	return incrservice.GetAutoIncrementService(ctx).Create(
		ctx,
		def.TblId,
		cols,
		txnOp)
}

func getRelFromMoCatalog(c *Compile, tblName string) (engine.Relation, error) {
	dbSource, err := c.e.Database(c.proc.Ctx, catalog.MO_CATALOG, c.proc.GetTxnOperator())
	if err != nil {
		return nil, err
	}

	rel, err := dbSource.Relation(c.proc.Ctx, tblName, nil)
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
	vec, err := getLockVector(c.proc, c.proc.GetSessionInfo().AccountId, []string{dbName})
	if err != nil {
		return err
	}
	defer vec.Free(c.proc.Mp())
	if err := lockRows(c.e, c.proc, dbRel, vec, lock.LockMode_Exclusive, lock.Sharding_ByRow, c.proc.GetSessionInfo().AccountId); err != nil {
		return err
	}
	return nil
}

func lockMoTable(
	c *Compile,
	dbName string,
	tblName string,
	lockMode lock.LockMode) error {
	dbRel, err := getRelFromMoCatalog(c, catalog.MO_TABLES)
	if err != nil {
		return err
	}
	vec, err := getLockVector(c.proc, c.proc.GetSessionInfo().AccountId, []string{dbName, tblName})
	if err != nil {
		return err
	}
	defer vec.Free(c.proc.Mp())

	if err := lockRows(c.e, c.proc, dbRel, vec, lockMode, lock.Sharding_ByRow, c.proc.GetSessionInfo().AccountId); err != nil {
		return err
	}
	return nil
}
