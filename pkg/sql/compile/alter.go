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

	"go.uber.org/zap"

	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (s *Scope) AlterTableCopy(c *Compile) error {
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

	originRel, err := dbSource.Relation(c.ctx, tblName, nil)
	if err != nil {
		return err
	}

	if c.proc.TxnOperator.Txn().IsPessimistic() {
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
		tableDef := qry.GetTableDef()
		if tableDef.Partition != nil {
			partitionTableNames = tableDef.Partition.PartitionTableNames
		}
		if err = lockTable(c.ctx, c.e, c.proc, originRel, dbName, partitionTableNames, true); err != nil {
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

	// 3. create temporary replica table which doesn't have foreign key constraints
	err = c.runSql(qry.CreateTmpTableSql)
	if err != nil {
		c.proc.Info(c.ctx, "Create copy table for alter table",
			zap.String("databaseName", c.db),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy tableName", qry.CopyTableDef.Name),
			zap.String("CreateTmpTableSql", qry.CreateTmpTableSql),
			zap.Error(err))
		return err
	}

	// 4. copy the original table data to the temporary replica table
	err = c.runSql(qry.InsertTmpDataSql)
	if err != nil {
		c.proc.Info(c.ctx, "insert data to copy table for alter table",
			zap.String("databaseName", c.db),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy tableName", qry.CopyTableDef.Name),
			zap.String("InsertTmpDataSql", qry.InsertTmpDataSql),
			zap.Error(err))
		return err
	}

	// 5. drop original table
	if err = dbSource.Delete(c.ctx, tblName); err != nil {
		c.proc.Info(c.ctx, "drop original table for alter table",
			zap.String("databaseName", c.db),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy tableName", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
	}

	// 5.1 delete all index objects of the table in mo_catalog.mo_indexes
	if qry.Database != catalog.MO_CATALOG && qry.TableDef.Name != catalog.MO_INDEXES {
		if qry.GetTableDef().Pkey != nil || len(qry.GetTableDef().Indexes) > 0 {
			deleteSql := fmt.Sprintf(deleteMoIndexesWithTableIdFormat, qry.GetTableDef().TblId)
			err = c.runSql(deleteSql)
			if err != nil {
				c.proc.Info(c.ctx, "delete all index meta data of origin table in `mo_indexes` for alter table",
					zap.String("databaseName", c.db),
					zap.String("origin tableName", qry.GetTableDef().Name),
					zap.String("delete all index sql", deleteSql),
					zap.Error(err))

				return err
			}
		}
	}

	// 5.2 delete all index table of the original table
	if qry.TableDef.Indexes != nil {
		for _, indexdef := range qry.TableDef.Indexes {
			if indexdef.TableExist {
				if err = dbSource.Delete(c.ctx, indexdef.IndexTableName); err != nil {
					c.proc.Info(c.ctx, "delete all index table of origin table for alter table",
						zap.String("databaseName", c.db),
						zap.String("origin tableName", qry.GetTableDef().Name),
						zap.String("origin tableName index table", indexdef.IndexTableName),
						zap.Error(err))
					return err
				}
			}
		}
	}

	//6. obtain relation for new tables
	newRel, err := dbSource.Relation(c.ctx, qry.CopyTableDef.Name, nil)
	if err != nil {
		c.proc.Info(c.ctx, "obtain new relation for copy table for alter table",
			zap.String("databaseName", c.db),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy table name", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
	}

	//--------------------------------------------------------------------------------------------------------------
	// 7. rename temporary replica table into the original table( Table Id remains unchanged)
	copyTblName := qry.CopyTableDef.Name
	req := api.NewRenameTableReq(newRel.GetDBID(c.ctx), newRel.GetTableID(c.ctx), copyTblName, tblName)
	tmp, err := req.Marshal()
	if err != nil {
		return err
	}
	constraint := make([][]byte, 0)
	constraint = append(constraint, tmp)
	err = newRel.TableRenameInTxn(c.ctx, constraint)
	if err != nil {
		c.proc.Info(c.ctx, "Rename copy tableName to origin tableName in for alter table",
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy table name", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
	}
	//--------------------------------------------------------------------------------------------------------------
	{
		// 8. invoke reindex for the new table, if it contains ivf index.
		multiTableIndexes := make(map[string]*MultiTableIndex)
		newTableDef := newRel.CopyTableDef(c.ctx)

		for _, indexDef := range newTableDef.Indexes {
			if catalog.IsIvfIndexAlgo(indexDef.IndexAlgo) {
				if _, ok := multiTableIndexes[indexDef.IndexName]; !ok {
					multiTableIndexes[indexDef.IndexName] = &MultiTableIndex{
						IndexAlgo: catalog.ToLower(indexDef.IndexAlgo),
						IndexDefs: make(map[string]*plan.IndexDef),
					}
				}
				multiTableIndexes[indexDef.IndexName].IndexDefs[catalog.ToLower(indexDef.IndexAlgoTableType)] = indexDef
			}
		}
		for _, multiTableIndex := range multiTableIndexes {
			switch multiTableIndex.IndexAlgo {
			case catalog.MoIndexIvfFlatAlgo.ToString():
				err = s.handleVectorIvfFlatIndex(c, multiTableIndex.IndexDefs, qry.Database, newTableDef, nil)
			}
			if err != nil {
				c.proc.Info(c.ctx, "invoke reindex for the new table for alter table",
					zap.String("origin tableName", qry.GetTableDef().Name),
					zap.String("copy table name", qry.CopyTableDef.Name),
					zap.String("indexAlgo", multiTableIndex.IndexAlgo),
					zap.Error(err))
				return err
			}
		}
	}

	// get and update the change mapping information of table colIds
	if err = updateNewTableColId(c, newRel, qry.ChangeTblColIdMap); err != nil {
		c.proc.Info(c.ctx, "get and update the change mapping information of table colIds for alter table",
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy table name", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
	}

	if len(qry.CopyTableDef.RefChildTbls) > 0 {
		// Restore the original table's foreign key child table ids to the copy table definition
		if err = restoreNewTableRefChildTbls(c, newRel, qry.CopyTableDef.RefChildTbls); err != nil {
			c.proc.Info(c.ctx, "Restore original table's foreign key child table ids to copyTable definition for alter table",
				zap.String("origin tableName", qry.GetTableDef().Name),
				zap.String("copy table name", qry.CopyTableDef.Name),
				zap.Error(err))
			return err
		}

		// update foreign key child table references to the current table
		for _, tblId := range qry.CopyTableDef.RefChildTbls {
			if err = updateTableForeignKeyColId(c, qry.ChangeTblColIdMap, tblId, originRel.GetTableID(c.ctx), newRel.GetTableID(c.ctx)); err != nil {
				c.proc.Info(c.ctx, "update foreign key child table references to the current table for alter table",
					zap.String("origin tableName", qry.GetTableDef().Name),
					zap.String("copy table name", qry.CopyTableDef.Name),
					zap.Error(err))
				return err
			}
		}
	}

	if len(qry.TableDef.Fkeys) > 0 {
		for _, fkey := range qry.CopyTableDef.Fkeys {
			if err = notifyParentTableFkTableIdChange(c, fkey, originRel.GetTableID(c.ctx)); err != nil {
				c.proc.Info(c.ctx, "notify parent table foreign key TableId Change for alter table",
					zap.String("origin tableName", qry.GetTableDef().Name),
					zap.String("copy table name", qry.CopyTableDef.Name),
					zap.Error(err))
				return err
			}
		}
	}
	return nil
}

func (s *Scope) AlterTable(c *Compile) (err error) {
	qry := s.Plan.GetDdl().GetAlterTable()
	if qry.AlgorithmType == plan.AlterTable_COPY {
		err = s.AlterTableCopy(c)
	} else {
		err = s.AlterTableInplace(c)
	}
	if err != nil {
		return err
	}

	if !plan2.IsFkBannedDatabase(qry.Database) {
		//update the mo_foreign_keys
		for _, sql := range qry.UpdateFkSqls {
			err = c.runSql(sql)
			if err != nil {
				return err
			}
		}
	}

	return err
}

// updateTableForeignKeyColId update foreign key colid of child table references
func updateTableForeignKeyColId(c *Compile, changColDefMap map[uint64]*plan.ColDef, childTblId uint64, oldParentTblId uint64, newParentTblId uint64) error {
	var childRelation engine.Relation
	var err error
	if childTblId == 0 {
		//fk self refer does not update
		return nil
	} else {
		_, _, childRelation, err = c.e.GetRelationById(c.ctx, c.proc.TxnOperator, childTblId)
		if err != nil {
			return err
		}
	}
	oldCt, err := GetConstraintDef(c.ctx, childRelation)
	if err != nil {
		return err
	}
	for _, ct := range oldCt.Cts {
		if def, ok1 := ct.(*engine.ForeignKeyDef); ok1 {
			for i := 0; i < len(def.Fkeys); i++ {
				fkey := def.Fkeys[i]
				if fkey.ForeignTbl == oldParentTblId {
					for j := 0; j < len(fkey.ForeignCols); j++ {
						if newColDef, ok2 := changColDefMap[fkey.ForeignCols[j]]; ok2 {
							fkey.ForeignCols[j] = newColDef.ColId
						}
					}
					fkey.ForeignTbl = newParentTblId
				}
			}
		}
	}
	return childRelation.UpdateConstraint(c.ctx, oldCt)
}

func updateNewTableColId(c *Compile, copyRel engine.Relation, changColDefMap map[uint64]*plan.ColDef) error {
	engineDefs, err := copyRel.TableDefs(c.ctx)
	if err != nil {
		return err
	}
	for _, def := range engineDefs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			for _, vColDef := range changColDefMap {
				if vColDef.Name == attr.Attr.Name {
					vColDef.ColId = attr.Attr.ID
					break
				}
			}
		}
	}
	return nil
}

// restoreNewTableRefChildTbls Restore the original table's foreign key child table ids to the copy table definition
func restoreNewTableRefChildTbls(c *Compile, copyRel engine.Relation, refChildTbls []uint64) error {
	oldCt, err := GetConstraintDef(c.ctx, copyRel)
	if err != nil {
		return err
	}
	oldCt.Cts = append(oldCt.Cts, &engine.RefChildTableDef{
		Tables: refChildTbls,
	})
	return copyRel.UpdateConstraint(c.ctx, oldCt)
}

// notifyParentTableFkTableIdChange Notify the parent table of changes in the tableid of the foreign key table
func notifyParentTableFkTableIdChange(c *Compile, fkey *plan.ForeignKeyDef, oldTableId uint64) error {
	foreignTblId := fkey.ForeignTbl
	_, _, fatherRelation, err := c.e.GetRelationById(c.ctx, c.proc.TxnOperator, foreignTblId)
	if err != nil {
		return err
	}
	oldCt, err := GetConstraintDef(c.ctx, fatherRelation)
	if err != nil {
		return err
	}
	for _, ct := range oldCt.Cts {
		if def, ok1 := ct.(*engine.RefChildTableDef); ok1 {
			for i := 0; i < len(def.Tables); i++ {
				if def.Tables[i] == oldTableId {
					// delete target element
					def.Tables = append(def.Tables[:i], def.Tables[i+1:]...)
					// Because the length of the slice has become shorter, it is necessary to move i forward
					i--
				}
			}
		}
	}
	return fatherRelation.UpdateConstraint(c.ctx, oldCt)
}
