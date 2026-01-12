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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_clone"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/idxcron"
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
	// Get logicalId from tableDef and pass it when creating the temporary table
	oldLogicalId := qry.GetTableDef().GetLogicalId()
	createTmpOpts := executor.StatementOption{}

	if oldLogicalId != 0 {
		createTmpOpts = createTmpOpts.WithKeepLogicalId(oldLogicalId)
	}
	err = c.runSqlWithOptions(qry.CreateTmpTableSql, createTmpOpts)
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

	//4. obtain relation for new tables
	newRel, err := dbSource.Relation(c.proc.Ctx, qry.CopyTableDef.Name, nil)
	if err != nil {
		c.proc.Error(c.proc.Ctx, "obtain new relation for copy table for alter table",
			zap.String("databaseName", dbName),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy table name", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
	}

	//5. ISCP: temp table already created pitr and iscp job with temp table name
	// and we don't want iscp to run with temp table so drop pitr and iscp job with the temp table here
	newTmpTableDef := newRel.CopyTableDef(c.proc.Ctx)
	err = DropAllIndexCdcTasks(c, newTmpTableDef, dbName, qry.CopyTableDef.Name)
	if err != nil {
		return err
	}

	// Idxcron: remove index update tasks with temp table id
	err = DropAllIndexUpdateTasks(c, newTmpTableDef, dbName, qry.CopyTableDef.Name)
	if err != nil {
		return err
	}

	// 6. copy the original table data to the temporary replica table
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

	//6. copy on writing unaffected index table
	if err = cloneUnaffectedIndexes(
		c, dbName, qry.Options.SkipIndexesCopy, qry.AffectedCols, newRel, qry.TableDef, nil,
	); err != nil {
		return err
	}

	// 7. drop original table.
	// ISCP: That will also drop ISCP related jobs and pitr of the original table.
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

	newTableDef := newRel.CopyTableDef(c.proc.Ctx)
	//--------------------------------------------------------------------------------------------------------------
	{
		// 9. invoke reindex for the new table, if it contains ivf index.
		multiTableIndexes := make(map[string]*MultiTableIndex)
		unaffectedIndexProcessed := make(map[string]bool)
		extra := newRel.GetExtraInfo()
		id := newRel.GetTableID(c.proc.Ctx)

		isAffectedIndex := func(indexDef *plan.IndexDef, affectedCols []string) bool {
			affected := false
			for _, part := range indexDef.Parts {
				if slices.Index(affectedCols, part) != -1 {
					affected = true
					break
				}
			}
			return affected
		}

		for _, indexDef := range newTableDef.Indexes {

			// DO NOT check SkipIndexesCopy here.  SkipIndexesCopy only valids for the unique/master/regular index.
			// Fulltext/HNSW/Ivfflat indexes are always "unaffected" in skipIndexesCopy
			// check affectedCols to see it is affected or not.  If affected is true, it means the secondary index
			// are cloned in cloneUnaffectedIndexes().  Otherwise, build the index again.

			if !indexDef.Unique && (catalog.IsIvfIndexAlgo(indexDef.IndexAlgo) ||
				catalog.IsHnswIndexAlgo(indexDef.IndexAlgo) ||
				catalog.IsFullTextIndexAlgo(indexDef.IndexAlgo)) {
				// ivf/hnsw/fulltext index

				if !isAffectedIndex(indexDef, qry.AffectedCols) {
					// column not affected means index already cloned in cloneUnaffectedIndexes()

					if unaffectedIndexProcessed[indexDef.IndexName] {
						// unaffectedIndex already processed.
						continue
					}

					{
						// ISCP
						valid, err := checkValidIndexCdc(newTableDef, indexDef.IndexName)
						if err != nil {
							return err
						}

						if valid {
							// index table may not be fully sync'd with source table via ISCP during alter table
							// clone index table (with ISCP) may not be a complete clone
							// so register ISCP job with startFromNow = false
							sinker_type := getSinkerTypeFromAlgo(indexDef.IndexAlgo)
							err = CreateIndexCdcTask(c, dbName, newTableDef.Name, indexDef.IndexName, sinker_type, false, "")
							if err != nil {
								return err
							}

							logutil.Infof("ISCP register unaffected index db=%s, table=%s, index=%s", dbName, newTableDef.Name, indexDef.IndexName)
						}
					}

					{
						// idxcron
						metadata, _, err := getIvfflatMetadata(c)
						if err != nil {
							return err
						}

						if catalog.IsIvfIndexAlgo(indexDef.IndexAlgo) {

							err = idxcron.RegisterUpdate(c.proc.Ctx,
								c.proc.GetService(),
								c.proc.GetTxnOperator(),
								id,
								dbName,
								newTableDef.Name,
								indexDef.IndexName,
								idxcron.Action_Ivfflat_Reindex,
								string(metadata))

							if err != nil {
								return err
							}
						}
					}

					unaffectedIndexProcessed[indexDef.IndexName] = true

					continue
				}

			} else {
				// ignore regular/master/unique index
				continue
			}

			// only affected ivf/hnsw/fulltext index will go here
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
			if catalog.IsFullTextIndexAlgo(indexDef.IndexAlgo) {
				err = s.handleFullTextIndexTable(c, id, extra, dbSource, indexDef, qry.Database, newTableDef, nil)
				if err != nil {
					c.proc.Error(c.proc.Ctx, "invoke reindex for the new table for alter table",
						zap.String("origin tableName", qry.GetTableDef().Name),
						zap.String("copy table name", qry.CopyTableDef.Name),
						zap.String("indexAlgo", indexDef.IndexAlgo),
						zap.Error(err))
					return err
				}
			}
		}
		for _, multiTableIndex := range multiTableIndexes {

			switch multiTableIndex.IndexAlgo {
			case catalog.MoIndexIvfFlatAlgo.ToString():
				err = s.handleVectorIvfFlatIndex(
					c, id, extra, dbSource, multiTableIndex.IndexDefs,
					qry.Database, newTableDef, nil, false,
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

	if qry.AlterPartition == nil {
		switch qry.AlgorithmType {
		case plan.AlterTable_COPY:
			return s.doAlterTable(c)
		default:
			// alter primary table
			if err := s.doAlterTable(c); err != nil {
				return err
			}

			// alter all partition tables
			if qry.RawSQL == "" {
				for _, ac := range qry.Actions {
					if _, ok := ac.Action.(*plan.AlterTable_Action_AlterName); ok {
						value := ac.Action.(*plan.AlterTable_Action_AlterName)
						return ps.Rename(
							c.proc.Ctx,
							qry.TableDef.TblId,
							value.AlterName.OldName,
							value.AlterName.NewName,
							c.proc.GetTxnOperator(),
						)
					}
				}

				panic("missing RawSQL for alter partition tables")
			}

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

	switch qry.AlterPartition.AlterType {
	case plan.AlterPartitionType_AddPartitionTables:
		stmt, _ := parsers.ParseOne(
			c.proc.Ctx,
			dialect.MYSQL,
			qry.RawSQL,
			c.getLower(),
		)

		return ps.AddPartitions(
			c.proc.Ctx,
			qry.TableDef.TblId,
			stmt.(*tree.AlterTable).PartitionOption.(*tree.AlterPartitionAddPartitionClause).Partitions,
			qry.AlterPartition.PartitionDefs,
			c.proc.GetTxnOperator(),
		)
	case plan.AlterPartitionType_DropPartitionTables:
		stmt, _ := parsers.ParseOne(
			c.proc.Ctx,
			dialect.MYSQL,
			qry.RawSQL,
			c.getLower(),
		)

		names := stmt.(*tree.AlterTable).PartitionOption.(*tree.AlterPartitionDropPartitionClause).PartitionNames
		partitions := make([]string, 0, len(names))
		for _, p := range names {
			partitions = append(partitions, p.String())
		}

		return ps.DropPartitions(
			c.proc.Ctx,
			qry.TableDef.TblId,
			partitions,
			c.proc.GetTxnOperator(),
		)
	case plan.AlterPartitionType_TruncatePartitionTables:
		stmt, _ := parsers.ParseOne(
			c.proc.Ctx,
			dialect.MYSQL,
			qry.RawSQL,
			c.getLower(),
		)
		var partitions []string
		names := stmt.(*tree.AlterTable).PartitionOption.(*tree.AlterPartitionTruncatePartitionClause).PartitionNames
		for _, p := range names {
			partitions = append(partitions, p.String())
		}

		return ps.TruncatePartitions(
			c.proc.Ctx,
			qry.TableDef.TblId,
			partitions,
			c.proc.GetTxnOperator(),
		)
	case plan.AlterPartitionType_RedefinePartitionTables:
		stmt, _ := parsers.ParseOne(
			c.proc.Ctx,
			dialect.MYSQL,
			qry.RawSQL,
			c.getLower(),
		)
		newOptions := stmt.(*tree.AlterTable).PartitionOption.(*tree.AlterPartitionRedefinePartitionClause).PartitionOption

		return ps.Redefine(
			c.proc.Ctx,
			qry.TableDef.TblId,
			newOptions,
			c.proc.GetTxnOperator(),
		)
	}
	return moerr.NewInternalError(c.proc.Ctx, "unsupported alter partition type")
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

func cloneUnaffectedIndexes(
	c *Compile,
	dbName string,
	skipIndexesCopy map[string]bool,
	affectedCols []string,
	newRel engine.Relation,
	oriTblDef *plan.TableDef,
	cloneSnapshot *plan.Snapshot,
) (err error) {

	type IndexTypeInfo struct {
		IndexTableName string
		AlgoTableType  string
	}

	type IndexTableInfo struct {
		Unique          bool
		IndexAlgo       string
		IndexAlgoParams string
		Indexes         []IndexTypeInfo
	}

	var (
		clone *table_clone.TableClone

		oriIdxTblDef *plan.TableDef
		oriIdxObjRef *plan.ObjectRef

		newTblDef = newRel.GetTableDef(c.proc.Ctx)

		oriIdxColNameToTblName = make(map[string]*IndexTableInfo)
		newIdxColNameToTblName = make(map[string]*IndexTableInfo)
	)

	logutil.Infof("cloneUnaffectedIndex: affected cols %v\n", affectedCols)
	logutil.Infof("cloneUnaffectedIndex: skipIndexesCopy %v\n", skipIndexesCopy)

	releaseClone := func() {
		if clone != nil {
			clone.Free(c.proc, false, err)
			reuse.Free[table_clone.TableClone](clone, nil)
			clone = nil
		}
	}

	defer func() {
		releaseClone()
	}()

	for _, idxTbl := range oriTblDef.Indexes {

		// NOTE: The index name of regular, maste, unqiue index is same as affected column name.
		// SkipIndexesCopy means UnaffectedIndexes[string][bool].
		// Affected indexes are processed in bind_insert when SkipIndexesCopy = false (UnaffectedIndexes = false)
		// Unaffected indexes is cloned here.
		//
		// 1. If affectedPk == true, SkipIndexesCopy[indexname] always false (empty) and nothing being cloned. All indexes are affected.
		// 2. If affectedPK == false, SkipIndexesCopy[indexname] will set to true when index is affected with affected column.
		// The condition is (indexname NOT IN affected_columns) wil set SkipIndexesCopy to true (UnAffectedIndexes == true)
		//
		// NOTE for Fulltext/HNSW/Ivfflat Index:
		// However, fulltext/hnsw/ivfflat index name is user-defined which is not related to column name so
		// SkipIndexesCopy will always be true in these cases (UnAffectedIndex==true).
		// Even SkipIndexesCopy is true, it does not mean it is really unaffected Index for fulltext/hnsw/ivfflat index.
		// check the Parts to determine affected or not.  If unaffected index, try clone.  Otherwise, re-build the index
		if !skipIndexesCopy[idxTbl.IndexName] {
			// This index is affected index, skip it
			continue
		}

		if !idxTbl.TableExist || len(idxTbl.IndexTableName) == 0 {
			continue
		}

		affected := false
		if !idxTbl.Unique && (catalog.IsFullTextIndexAlgo(idxTbl.IndexAlgo) ||
			catalog.IsHnswIndexAlgo(idxTbl.IndexAlgo) ||
			catalog.IsIvfIndexAlgo(idxTbl.IndexAlgo)) {
			// only check parts when fulltext/hnsw/ivfflat index

			for _, part := range idxTbl.Parts {
				if slices.Index(affectedCols, part) != -1 {
					affected = true
					break

				}
			}
		}

		if affected {
			continue
		}

		logutil.Infof("cloneUnaffectedIndex: old %s parts %v\n", idxTbl.IndexTableName, idxTbl.Parts)

		m, ok := oriIdxColNameToTblName[idxTbl.IndexName]
		if !ok {
			m = &IndexTableInfo{
				Unique:          idxTbl.Unique,
				IndexAlgo:       idxTbl.IndexAlgo,
				IndexAlgoParams: idxTbl.IndexAlgoParams,
				Indexes:         make([]IndexTypeInfo, 0, 3),
			}

		}

		m.Indexes = append(m.Indexes,
			IndexTypeInfo{IndexTableName: idxTbl.IndexTableName,
				AlgoTableType: idxTbl.IndexAlgoTableType})
		oriIdxColNameToTblName[idxTbl.IndexName] = m
	}

	for _, idxTbl := range newTblDef.Indexes {
		if !idxTbl.TableExist || len(idxTbl.IndexTableName) == 0 {
			continue
		}

		m, ok := newIdxColNameToTblName[idxTbl.IndexName]
		if !ok {
			m = &IndexTableInfo{
				Unique:          idxTbl.Unique,
				IndexAlgo:       idxTbl.IndexAlgo,
				IndexAlgoParams: idxTbl.IndexAlgoParams,
				Indexes:         make([]IndexTypeInfo, 0, 3),
			}
		}

		m.Indexes = append(m.Indexes,
			IndexTypeInfo{IndexTableName: idxTbl.IndexTableName,
				AlgoTableType: idxTbl.IndexAlgoTableType})
		newIdxColNameToTblName[idxTbl.IndexName] = m
		logutil.Infof("cloneUnaffectedIndex: new %s parts %v\n", idxTbl.IndexTableName, idxTbl.Parts)
	}

	cctx := compilerContext{
		ctx:       c.proc.Ctx,
		defaultDB: dbName,
		engine:    c.e,
		proc:      c.proc,
	}

	for idxName, oriIdxTblNames := range oriIdxColNameToTblName {
		newIdxTblNames, ok := newIdxColNameToTblName[idxName]
		if !ok {
			continue
		}

		async, err := catalog.IsIndexAsync(oriIdxTblNames.IndexAlgoParams)
		if err != nil {
			return err
		}

		if !oriIdxTblNames.Unique &&
			((catalog.IsFullTextIndexAlgo(oriIdxTblNames.IndexAlgo) && async) ||
				catalog.IsHnswIndexAlgo(oriIdxTblNames.IndexAlgo)) {
			// skip fultext async index and hsnw index clone because index table may not be fully sync'd
			logutil.Infof("cloneUnaffectedIndex: skip async index %v\n", oriIdxTblNames)
			continue
		}

		for _, oriIdxTblName := range oriIdxTblNames.Indexes {

			var newIdxTblName IndexTypeInfo
			found := false
			for _, idxinfo := range newIdxTblNames.Indexes {
				if oriIdxTblName.AlgoTableType == idxinfo.AlgoTableType {
					newIdxTblName = idxinfo
					found = true
					break
				}
			}

			if !found {
				continue
			}

			// IVF index table is NOT empty and clone will have duplicate rows
			// Delete the table
			if !oriIdxTblNames.Unique &&
				catalog.IsIvfIndexAlgo(oriIdxTblNames.IndexAlgo) {
				// delete all content but avoid truncate table with WHERE TRUE
				sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE TRUE", dbName, newIdxTblName.IndexTableName)
				err := c.runSql(sql)
				if err != nil {
					return err
				}
			}

			if !oriIdxTblNames.Unique &&
				async &&
				catalog.IsIvfIndexAlgo(oriIdxTblNames.IndexAlgo) &&
				oriIdxTblName.AlgoTableType == catalog.SystemSI_IVFFLAT_TblType_Entries {
				// skip async IVF entries index table
				logutil.Infof("cloneUnaffectedIndex: skip async IVF entries index table %v\n", oriIdxTblName)
				continue
			}

			logutil.Infof("cloneUnaffectedIndex: clone %v -> %v\n", oriIdxTblName, newIdxTblName)
			oriIdxObjRef, oriIdxTblDef, err = cctx.Resolve(dbName, oriIdxTblName.IndexTableName, cloneSnapshot)
			if err != nil {
				return err
			}

			clonePlan := plan.CloneTable{
				CreateTable:     nil,
				ScanSnapshot:    cloneSnapshot,
				SrcTableDef:     oriIdxTblDef,
				SrcObjDef:       oriIdxObjRef,
				DstDatabaseName: dbName,
				DstTableName:    newIdxTblName.IndexTableName,
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
	}

	return nil
}
