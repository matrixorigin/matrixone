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
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_clone"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

func convertDBEOB(ctx context.Context, e error, name string) error {
	if moerr.IsMoErrCode(e, moerr.OkExpectedEOB) {
		return moerr.NewBadDB(ctx, name)
	}
	return e
}

func (s *Scope) AlterTableCopy(c *Compile) error {
	qry := s.Plan.GetDdl().GetAlterTable()
	dbName := qry.Database
	if dbName == "" {
		dbName = c.db
	}
	tblName := qry.GetTableDef().GetName()
	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return convertDBEOB(c.proc.Ctx, err, dbName)
	}

	accountId, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}

	originRel, err := dbSource.Relation(c.proc.Ctx, tblName, nil)
	if err != nil {
		return err
	}

	oldId := originRel.GetTableID(c.proc.Ctx)
	if c.proc.GetTxnOperator().Txn().IsPessimistic() {
		var retryErr error
		// 0. lock origin database metadata in catalog
		if err = lockMoDatabase(c, dbName, lock.LockMode_Shared); err != nil {
			return err
		}

		// 1. lock origin table metadata in catalog
		if err = lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return err
			}
			// The changes recorded in the data dictionary table imply a change in the structure of the corresponding entity table,
			// therefore it is necessary to rebuild the logical plan and redirect err to ErrTxnNeedRetryWithDefChanged
			retryErr = moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
		}

		// 2. lock origin table
		if err = lockTable(c.proc.Ctx, c.e, c.proc, originRel, dbName, true); err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				c.proc.Error(c.proc.Ctx, "lock origin table for alter table",
					zap.String("databaseName", dbName),
					zap.String("origin tableName", qry.GetTableDef().Name),
					zap.Error(err))
				return err
			}
			retryErr = moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
		}

		if qry.TableDef.Indexes != nil {
			for _, indexdef := range qry.TableDef.Indexes {
				if indexdef.TableExist {
					err = lockIndexTable(
						c.proc.Ctx,
						dbSource,
						c.e,
						c.proc,
						indexdef.IndexTableName,
						true,
					)
					if err != nil {
						if !moerr.IsMoErrCode(err, moerr.ErrParseError) &&
							!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
							!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
							c.proc.Error(c.proc.Ctx, "lock index table for alter table",
								zap.String("databaseName", dbName),
								zap.String("origin tableName", qry.GetTableDef().Name),
								zap.String("index name", indexdef.IndexName),
								zap.String("index tableName", indexdef.IndexTableName),
								zap.Error(err))
							return err
						}
						retryErr = moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
					}
				}
			}
		}

		if retryErr != nil {
			return retryErr
		}
	}

	// 3. create temporary replica table which doesn't have foreign key constraints
	err = c.runSql(qry.CreateTmpTableSql)
	if err != nil {
		c.proc.Error(c.proc.Ctx, "Create copy table for alter table",
			zap.String("databaseName", dbName),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy tableName", qry.CopyTableDef.Name),
			zap.String("CreateTmpTableSql", qry.CreateTmpTableSql),
			zap.Error(err))
		return err
	}
	opt := executor.StatementOption{}
	if qry.Options.SkipPkDedup || len(qry.Options.SkipUniqueIdxDedup) > 0 {
		opt = opt.WithAlterCopyOpt(qry.Options)
	}
	// 4. copy the original table data to the temporary replica table
	err = c.runSqlWithOptions(qry.InsertTmpDataSql, opt)
	if err != nil {
		c.proc.Error(c.proc.Ctx, "insert data to copy table for alter table",
			zap.String("databaseName", dbName),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy tableName", qry.CopyTableDef.Name),
			zap.String("InsertTmpDataSql", qry.InsertTmpDataSql),
			zap.Error(err))
		return err
	}

	//5. obtain relation for new tables
	newRel, err := dbSource.Relation(c.proc.Ctx, qry.CopyTableDef.Name, nil)
	if err != nil {
		c.proc.Error(c.proc.Ctx, "obtain new relation for copy table for alter table",
			zap.String("databaseName", dbName),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy table name", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
	}

	//6. copy on writing unaffected index table
	if err = cowUnaffectedIndexes(
		c, dbName, qry.AffectedCols, newRel, qry.TableDef, nil,
	); err != nil {
		return err
	}

	// 7. drop original table
	dropSql := fmt.Sprintf("drop table `%s`.`%s`", dbName, tblName)
	if err := c.runSqlWithOptions(
		dropSql,
		executor.StatementOption{}.WithIgnoreForeignKey(),
	); err != nil {
		c.proc.Error(c.proc.Ctx, "drop original table for alter table",
			zap.String("databaseName", dbName),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy tableName", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
	}

	// 7.1 delete all index objects of the table in mo_catalog.mo_indexes
	if qry.Database != catalog.MO_CATALOG && qry.TableDef.Name != catalog.MO_INDEXES {
		if qry.GetTableDef().Pkey != nil || len(qry.GetTableDef().Indexes) > 0 {
			deleteSql := fmt.Sprintf(
				deleteMoIndexesWithTableIdFormat,
				qry.GetTableDef().TblId,
			)
			err = c.runSql(deleteSql)
			if err != nil {
				c.proc.Error(c.proc.Ctx, "delete all index meta data of origin table in `mo_indexes` for alter table",
					zap.String("databaseName", dbName),
					zap.String("origin tableName", qry.GetTableDef().Name),
					zap.String("delete all index sql", deleteSql),
					zap.Error(err))

				return err
			}
		}
	}

	newId := newRel.GetTableID(c.proc.Ctx)
	//-------------------------------------------------------------------------
	// 8. rename temporary replica table into the original table(Table Id remains unchanged)
	copyTblName := qry.CopyTableDef.Name
	req := api.NewRenameTableReq(
		newRel.GetDBID(c.proc.Ctx),
		newRel.GetTableID(c.proc.Ctx),
		copyTblName,
		tblName,
	)
	binaryConstraint, err := req.Marshal()
	if err != nil {
		return err
	}
	constraint := [][]byte{binaryConstraint}
	err = newRel.TableRenameInTxn(c.proc.Ctx, constraint)
	if err != nil {
		c.proc.Error(c.proc.Ctx, "Rename copy tableName to origin tableName in for alter table",
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy table name", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
	}
	//--------------------------------------------------------------------------------------------------------------
	{
		// 9. invoke reindex for the new table, if it contains ivf index.
		multiTableIndexes := make(map[string]*MultiTableIndex)
		newTableDef := newRel.CopyTableDef(c.proc.Ctx)
		extra := newRel.GetExtraInfo()
		id := newRel.GetTableID(c.proc.Ctx)

		for _, indexDef := range newTableDef.Indexes {
			if catalog.IsIvfIndexAlgo(indexDef.IndexAlgo) ||
				catalog.IsHnswIndexAlgo(indexDef.IndexAlgo) {
				if _, ok := multiTableIndexes[indexDef.IndexName]; !ok {
					multiTableIndexes[indexDef.IndexName] = &MultiTableIndex{
						IndexAlgo: catalog.ToLower(indexDef.IndexAlgo),
						IndexDefs: make(map[string]*plan.IndexDef),
					}
				}
				ty := catalog.ToLower(indexDef.IndexAlgoTableType)
				multiTableIndexes[indexDef.IndexName].IndexDefs[ty] = indexDef
			}
		}
		for _, multiTableIndex := range multiTableIndexes {
			switch multiTableIndex.IndexAlgo {
			case catalog.MoIndexIvfFlatAlgo.ToString():
				err = s.handleVectorIvfFlatIndex(
					c, id, extra, dbSource, multiTableIndex.IndexDefs,
					qry.Database, newTableDef, nil,
				)
			case catalog.MoIndexHnswAlgo.ToString():
				err = s.handleVectorHnswIndex(
					c, id, extra, dbSource, multiTableIndex.IndexDefs,
					qry.Database, newTableDef, nil,
				)
			}
			if err != nil {
				c.proc.Error(c.proc.Ctx, "invoke reindex for the new table for alter table",
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
		c.proc.Error(c.proc.Ctx, "get and update the change mapping information of table colIds for alter table",
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy table name", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
	}

	if len(qry.CopyTableDef.RefChildTbls) > 0 {
		// Restore the original table's foreign key child table ids to the copy table definition
		if err = restoreNewTableRefChildTbls(c, newRel, qry.CopyTableDef.RefChildTbls); err != nil {
			c.proc.Error(c.proc.Ctx, "Restore original table's foreign key child table ids to copyTable definition for alter table",
				zap.String("origin tableName", qry.GetTableDef().Name),
				zap.String("copy table name", qry.CopyTableDef.Name),
				zap.Error(err))
			return err
		}

		// update foreign key child table references to the current table
		for _, tblId := range qry.CopyTableDef.RefChildTbls {
			err = updateTableForeignKeyColId(
				c, qry.ChangeTblColIdMap, tblId,
				originRel.GetTableID(c.proc.Ctx),
				newRel.GetTableID(c.proc.Ctx),
			)
			if err != nil {
				c.proc.Error(c.proc.Ctx, "update foreign key child table references to the current table for alter table",
					zap.String("origin tableName", qry.GetTableDef().Name),
					zap.String("copy table name", qry.CopyTableDef.Name),
					zap.Error(err))
				return err
			}
		}
	}

	if len(qry.TableDef.Fkeys) > 0 {
		for _, fkey := range qry.CopyTableDef.Fkeys {
			err = notifyParentTableFkTableIdChange(
				c,
				fkey,
				originRel.GetTableID(c.proc.Ctx),
			)
			if err != nil {
				c.proc.Error(c.proc.Ctx, "notify parent table foreign key TableId Change for alter table",
					zap.String("origin tableName", qry.GetTableDef().Name),
					zap.String("copy table name", qry.CopyTableDef.Name),
					zap.Error(err))
				return err
			}
		}
	}

	// update merge settings in mo_catalog.mo_merge_settings
	updateSql := fmt.Sprintf(updateMoMergeSettings, newId, accountId, oldId)
	err = c.runSqlWithSystemTenant(updateSql)
	if err != nil {
		c.proc.Error(c.proc.Ctx, "update mo_catalog.mo_merge_settings for alter table",
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy table name", qry.CopyTableDef.Name),
			zap.Uint64("origin table id", oldId),
			zap.Uint64("copy table id", newId),
			zap.Error(err))
		return err
	}
	return nil
}

func (s *Scope) AlterTable(c *Compile) (err error) {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	qry := s.Plan.GetDdl().GetAlterTable()

	ps := c.proc.GetPartitionService()
	if !ps.Enabled() ||
		!features.IsPartitioned(qry.TableDef.FeatureFlag) {
		return s.doAlterTable(c)
	}

	switch qry.AlgorithmType {
	case plan.AlterTable_COPY:
		return s.doAlterTable(c)
	default:
		// alter primary table
		if err := s.doAlterTable(c); err != nil {
			return err
		}

		// alter all partition tables
		metadata, err := ps.GetPartitionMetadata(
			c.proc.Ctx,
			qry.TableDef.TblId,
			c.proc.Base.TxnOperator,
		)
		if err != nil {
			return err
		}

		st, _ := parsers.ParseOne(
			c.proc.Ctx,
			dialect.MYSQL,
			qry.RawSQL,
			c.getLower(),
		)
		stmt := st.(*tree.AlterTable)
		table := stmt.Table
		stmt.PartitionOption = nil
		for _, p := range metadata.Partitions {
			stmt.Table = tree.NewTableName(
				tree.Identifier(p.PartitionTableName),
				table.ObjectNamePrefix,
				table.AtTsExpr,
			)
			sql := tree.StringWithOpts(
				stmt,
				dialect.MYSQL,
				tree.WithQuoteIdentifier(),
				tree.WithSingleQuoteString(),
			)
			if err := c.runSql(sql); err != nil {
				return err
			}
		}
		return nil
	}
}

func (s *Scope) doAlterTable(c *Compile) error {
	qry := s.Plan.GetDdl().GetAlterTable()

	var err error
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

func (s *Scope) RenameTable(c *Compile) (err error) {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	qry := s.Plan.GetDdl().GetRenameTable()
	for _, alterTable := range qry.AlterTables {
		plan := &plan.Plan{
			Plan: &plan.Plan_Ddl{
				Ddl: &plan.DataDefinition{
					DdlType: plan.DataDefinition_ALTER_TABLE,
					Definition: &plan.DataDefinition_AlterTable{
						AlterTable: alterTable,
					},
				},
			},
		}
		subScope := newScope(AlterTable).withPlan(plan)
		defer subScope.release()
		err = subScope.AlterTable(c)
		if err != nil {
			return err
		}
	}
	return nil
}

// updateTableForeignKeyColId update foreign key colid of child table references
func updateTableForeignKeyColId(
	c *Compile,
	changeColDefMap map[uint64]*plan.ColDef,
	childTblId uint64,
	oldParentTblId uint64,
	newParentTblId uint64,
) error {
	var childRel engine.Relation
	var err error
	if childTblId == 0 {
		//fk self refer does not update
		return nil
	} else {
		_, _, childRel, err = c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), childTblId)
		if err != nil {
			return err
		}
	}
	oldCt, err := GetConstraintDef(c.proc.Ctx, childRel)
	if err != nil {
		return err
	}
	for _, ct := range oldCt.Cts {
		if def, ok1 := ct.(*engine.ForeignKeyDef); ok1 {
			for i := 0; i < len(def.Fkeys); i++ {
				fkey := def.Fkeys[i]
				if fkey.ForeignTbl == oldParentTblId {
					for j := 0; j < len(fkey.ForeignCols); j++ {
						if newColDef, ok2 := changeColDefMap[fkey.ForeignCols[j]]; ok2 {
							fkey.ForeignCols[j] = newColDef.ColId
						}
					}
					fkey.ForeignTbl = newParentTblId
				}
			}
		}
	}
	return childRel.UpdateConstraint(c.proc.Ctx, oldCt)
}

func updateNewTableColId(c *Compile, copyRel engine.Relation, changeColDefMap map[uint64]*plan.ColDef) error {
	tableDefs, err := copyRel.TableDefs(c.proc.Ctx)
	if err != nil {
		return err
	}
	for _, def := range tableDefs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			for _, colDef := range changeColDefMap {
				if colDef.GetOriginCaseName() == attr.Attr.Name {
					colDef.ColId = attr.Attr.ID
					break
				}
			}
		}
	}
	return nil
}

// restoreNewTableRefChildTbls Restore the original table's foreign key child table ids to the copy table definition
func restoreNewTableRefChildTbls(c *Compile, copyRel engine.Relation, refChildTbls []uint64) error {
	oldCt, err := GetConstraintDef(c.proc.Ctx, copyRel)
	if err != nil {
		return err
	}
	oldCt.Cts = append(oldCt.Cts, &engine.RefChildTableDef{
		Tables: refChildTbls,
	})
	return copyRel.UpdateConstraint(c.proc.Ctx, oldCt)
}

// notifyParentTableFkTableIdChange Notify the parent table of changes in the tableid of the foreign key table
func notifyParentTableFkTableIdChange(c *Compile, fkey *plan.ForeignKeyDef, oldTableId uint64) error {
	foreignTblId := fkey.ForeignTbl
	_, _, fatherRelation, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), foreignTblId)
	if err != nil {
		return err
	}
	oldCt, err := GetConstraintDef(c.proc.Ctx, fatherRelation)
	if err != nil {
		return err
	}
	for _, ct := range oldCt.Cts {
		if def, ok1 := ct.(*engine.RefChildTableDef); ok1 {
			def.Tables = plan2.RemoveIf(def.Tables, func(id uint64) bool {
				return id == oldTableId
			})
		}
	}
	return fatherRelation.UpdateConstraint(c.proc.Ctx, oldCt)
}

func cowUnaffectedIndexes(
	c *Compile,
	dbName string,
	affectedCols []string,
	newRel engine.Relation,
	oriTblDef *plan.TableDef,
	cloneSnapshot *plan.Snapshot,
) (err error) {

	var (
		clone *table_clone.TableClone

		oriIdxTblDef *plan.TableDef
		oriIdxObjRef *plan.ObjectRef

		newTblDef = newRel.GetTableDef(c.proc.Ctx)

		oriIdxColNameToTblName  = make(map[string]string)
		newIdxTColNameToTblName = make(map[string]string)
	)

	releaseClone := func() {
		if clone != nil {
			clone.Free(c.proc, false, err)
			clone = nil
		}
	}

	defer func() {
		releaseClone()
	}()

	for _, idxTbl := range oriTblDef.Indexes {
		if slices.Index(affectedCols, idxTbl.IndexName) != -1 {
			continue
		}

		oriIdxColNameToTblName[idxTbl.IndexName] = idxTbl.IndexTableName
	}

	for _, idxTbl := range newTblDef.Indexes {
		newIdxTColNameToTblName[idxTbl.IndexName] = idxTbl.IndexTableName
	}

	cctx := compilerContext{
		ctx:       c.proc.Ctx,
		defaultDB: dbName,
		engine:    c.e,
		proc:      c.proc,
	}

	for colName, oriIdxTblName := range oriIdxColNameToTblName {
		newIdxTblName, ok := newIdxTColNameToTblName[colName]
		if !ok {
			continue
		}

		oriIdxObjRef, oriIdxTblDef, err = cctx.Resolve(dbName, oriIdxTblName, cloneSnapshot)

		clonePlan := plan.CloneTable{
			CreateTable:     nil,
			ScanSnapshot:    cloneSnapshot,
			SrcTableDef:     oriIdxTblDef,
			SrcObjDef:       oriIdxObjRef,
			DstDatabaseName: dbName,
			DstTableName:    newIdxTblName,
		}

		if clone, err = constructTableClone(c, &clonePlan); err != nil {
			return err
		}

		if err = clone.Prepare(c.proc); err != nil {
			releaseClone()
			return err
		}

		if _, err = clone.Call(c.proc); err != nil {
			releaseClone()
			return err
		}

		releaseClone()
	}

	return nil
}
