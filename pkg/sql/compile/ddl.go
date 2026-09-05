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
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/task"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/taskservice"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/idxcron"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"
)

func (s *Scope) CreateDatabase(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()
	ctx, span := trace.Start(c.proc.Ctx, "CreateDatabase")
	defer span.End()

	createDatabase := s.Plan.GetDdl().GetCreateDatabase()
	dbName := createDatabase.GetDatabase()
	if _, err := c.e.Database(ctx, dbName, c.proc.GetTxnOperator()); err == nil {
		if createDatabase.GetIfNotExists() {
			return nil
		}
		return moerr.NewDBAlreadyExists(ctx, dbName)
	}

	if err := lockMoDatabase(c, dbName, lock.LockMode_Exclusive); err != nil {
		return err
	}

	ctx = context.WithValue(ctx, defines.SqlKey{}, createDatabase.GetSql())
	datType := ""
	// handle sub
	if subOption := createDatabase.SubscriptionOption; subOption != nil {
		datType = catalog.SystemDBTypeSubscription
		if err := createSubscription(ctx, c, dbName, subOption); err != nil {
			return err
		}
	}

	ctx = context.WithValue(ctx, defines.DatTypKey{}, datType)
	if err := c.e.Create(ctx, dbName, c.proc.GetTxnOperator()); err != nil {
		return err
	}
	c.setAffectedRows(1)
	return nil
}

func (s *Scope) DropDatabase(c *Compile) error {
	c.setAffectedRows(0)
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()
	// DROP DATABASE changes PITR ownership and can reclaim branch metadata
	// through its nested table drops. Enter the shared lifecycle before any
	// account, database, or relation lookup/lock.
	if err := c.lockDataBranchLineageOwnerLifecycle(); err != nil {
		return err
	}

	accountId, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}

	dbName := s.Plan.GetDdl().GetDropDatabase().GetDatabase()
	db, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if s.Plan.GetDdl().GetDropDatabase().GetIfExists() {
			return nil
		}
		return moerr.NewErrDropNonExistsDB(c.proc.Ctx, dbName)
	}

	// Check if the database is a CCPR shared database
	if !db.IsSubscription(c.proc.Ctx) {
		dbIDStr := db.GetDatabaseId(c.proc.Ctx)
		dbID, err := strconv.ParseUint(dbIDStr, 10, 64)
		if err == nil {
			canDrop, err := checkCCPRDbBeforeDrop(c, dbID)
			if err != nil {
				return err
			}
			if !canDrop {
				return moerr.NewCCPRReadOnly(c.proc.Ctx)
			}
		}
	}
	if c.proc.Base.IsFrontend && !needSkipDbs[dbName] &&
		(!c.proc.GetSessionInfo().IsRestore || restoreInvalidatesViewMetadata(c.proc.Ctx)) {
		// Recovery takes this gate before locking a target View. Take it before
		// the database lock so DROP cannot hold catalog/target locks while
		// waiting for recovery's refresh-row transaction.
		if err = lockViewMetadataLifecycleGate(c.proc); err != nil {
			return err
		}
	}

	if err = lockMoDatabase(c, dbName, lock.LockMode_Exclusive); err != nil {
		return err
	}

	// After acquiring the exclusive lock on mo_database, advance
	// the transaction's snapshot so that Relations() can see all tables
	// committed by other CNs (e.g. concurrent CLONE) before the lock was
	// granted.
	//
	// AdvanceSnapshot also transfers workspace tombstones to objects visible at
	// the new snapshot. SnapshotTS must remain advanced afterwards because
	// rewinding it alone cannot undo an in-memory tombstone transfer.
	txnOp := c.proc.GetTxnOperator()
	if txnOp.Txn().IsPessimistic() && txnOp.Txn().IsRCIsolation() {
		now, _ := moruntime.ServiceRuntime(c.proc.GetService()).Clock().Now()
		if err = txnOp.GetWorkspace().AdvanceSnapshot(c.proc.Ctx, now); err != nil {
			return err
		}
	}

	// handle sub
	if db.IsSubscription(c.proc.Ctx) {
		if err = dropSubscription(c.proc.Ctx, c, dbName); err != nil {
			return err
		}
	}

	// Internal callers such as DROP ACCOUNT remove every database in the
	// account. In that case, rewriting cross-database FK metadata is both
	// unnecessary and unsafe: a later DROP can observe both catalog versions
	// produced by the rewrite in this transaction.
	ignoreForeignKey, _ := c.proc.Ctx.Value(defines.IgnoreForeignKey{}).(bool)
	if !ignoreForeignKey {
		err = s.removeFkeysRelationships(c, dbName)
		if err != nil {
			return err
		}
	}

	database, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	relations, err := database.Relations(c.proc.Ctx)
	if err != nil {
		return err
	}
	var droppedDatabaseID uint64
	if !needSkipDbs[dbName] {
		droppedDatabaseID, err = strconv.ParseUint(database.GetDatabaseId(c.proc.Ctx), 10, 64)
		if err != nil {
			return err
		}
	}
	if c.proc.Base.IsFrontend && !needSkipDbs[dbName] {
		generation := uint64(c.proc.GetTxnOperator().SnapshotTS().PhysicalTime)
		if err = c.enqueueViewsAfterDatabaseRemoval(dbName, accountId, droppedDatabaseID, generation); err != nil {
			return err
		}
		if err = c.deleteDroppedDatabaseViewMetadata(accountId, droppedDatabaseID, dbName); err != nil {
			return err
		}
	}
	var ignoreTables []string
	existingRelations := make([]string, 0, len(relations))
	for _, r := range relations {
		t, err := database.Relation(c.proc.Ctx, r, nil)
		if err != nil {
			if logAndSkipMissingRelationByNameForDropDatabase(c, dbName, r, "cannot open relation when collecting tables for drop database", err) {
				continue
			}
			return err
		}
		existingRelations = append(existingRelations, r)

		if features.IsPartition(t.GetExtraInfo().FeatureFlag) ||
			features.IsIndexTable(t.GetExtraInfo().FeatureFlag) {
			ignoreTables = append(ignoreTables, r)
			continue
		}

		defs, err := t.TableDefs(c.proc.Ctx)
		if err != nil {
			return err
		}

		constrain := GetConstraintDefFromTableDefs(defs)
		for _, ct := range constrain.Cts {
			if ds, ok := ct.(*engine.IndexDef); ok {
				for _, d := range ds.Indexes {
					ignoreTables = append(ignoreTables, d.IndexTableName)
				}
			}
		}
	}

	deleteTables := make([]string, 0, len(existingRelations))
	for _, r := range existingRelations {
		isIndexTable := false
		for _, d := range ignoreTables {
			if d == r {
				isIndexTable = true
				break
			}
		}
		if !isIndexTable {
			deleteTables = append(deleteTables, r)
		}
	}

	if !needSkipDbs[dbName] {
		if err = c.deleteRolePrivilegesForDroppedDatabase(accountId, droppedDatabaseID); err != nil {
			return err
		}
	}

	for _, t := range deleteTables {
		dropSql := fmt.Sprintf("drop table if exists %s.%s;",
			quoteMySQLIdent(dbName), quoteMySQLIdent(t))
		if err = c.runSqlWithOptions(
			dropSql, executor.StatementOption{}.WithDisableLog().WithIgnorePublish(),
		); err != nil {
			return err
		}
	}

	sql := s.Plan.GetDdl().GetDropDatabase().GetCheckFKSql()
	if len(sql) != 0 {
		if err = runDetectFkReferToDBSql(c, sql); err != nil {
			return err
		}
	}

	err = c.e.Delete(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}

	// 1.delete all index object record under the database from mo_catalog.mo_indexes
	deleteSql := fmt.Sprintf(deleteMoIndexesWithDatabaseIdFormat, s.Plan.GetDdl().GetDropDatabase().GetDatabaseId())
	if err = c.runSqlWithOptions(
		deleteSql, executor.StatementOption{}.WithDisableLog(),
	); err != nil {
		return err
	}

	// 3. delete fks
	if err = c.runSqlWithOptions(
		s.Plan.GetDdl().GetDropDatabase().GetUpdateFkSql(), executor.StatementOption{}.WithDisableLog(),
	); err != nil {
		return err
	}

	// 4.update mo_pitr table
	if !needSkipDbs[dbName] {
		now := c.proc.GetTxnOperator().SnapshotTS().ToStdTime().UTC().UnixNano()
		updatePitrSql := fmt.Sprintf("UPDATE `%s`.`%s` SET `%s` = %d, `%s` = %d WHERE `%s` = %d AND `%s` = '%s' AND `%s` = %d AND `%s` = %s",
			catalog.MO_CATALOG, catalog.MO_PITR,
			catalog.MO_PITR_STATUS, 0,
			catalog.MO_PITR_CHANGED_TIME, now,

			catalog.MO_PITR_ACCOUNT_ID, accountId,
			catalog.MO_PITR_DB_NAME, sqlquote.EscapeString(dbName),
			catalog.MO_PITR_STATUS, 1,
			catalog.MO_PITR_OBJECT_ID, database.GetDatabaseId(c.proc.Ctx),
		)

		err = c.runSqlWithSystemTenant(updatePitrSql)
		if err != nil {
			return err
		}
	}

	// 5.unregister iscp jobs
	err = iscp.UnregisterJobsByDBName(c.proc.Ctx, c.proc.GetService(), c.proc.GetTxnOperator(), dbName)
	if err != nil {
		return err
	}

	// 6.unregister index update
	err = idxcron.UnregisterUpdateByDbName(c.proc.Ctx, c.proc.GetService(), c.proc.GetTxnOperator(), dbName)
	if err != nil {
		return err
	}
	if session, ok := c.proc.GetSession().(interface {
		RemoveTempTablesByDatabase(string)
	}); ok {
		// The catalog removal has completed, so these aliases now refer to
		// relations that cannot be cloned during connection migration. The
		// session implementation journals this cleanup with the DDL statement.
		session.RemoveTempTablesByDatabase(dbName)
	}

	c.setAffectedRows(uint64(len(deleteTables)))
	return nil
}

func logAndSkipMissingRelationByNameForDropDatabase(c *Compile, dbName, rel, msg string, err error) bool {
	if !isMissingTableByNameForDropDatabase(err) {
		return false
	}
	logutil.Warn(
		msg,
		zap.String("table", fmt.Sprintf("%s-%s", dbName, rel)),
		zap.String("txn info", c.proc.GetTxnOperator().Txn().DebugString()),
		zap.Error(err),
	)
	return true
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
			if logAndSkipMissingRelationByNameForDropDatabase(c, dbName, rel, "cannot open relation when drop database fk cleanup", err) {
				continue
			}
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

			var parentTable engine.Relation
			if _, _, parentTable, err = c.e.GetRelationById(
				c.proc.Ctx, c.proc.GetTxnOperator(), fkey.ForeignTbl,
			); err != nil {
				// A has keys reference on B, we cannot find the B when drop A.
				// what's going on?
				//
				// if there has no mistake on the constraints when create A and B, we cannot
				// find B means B has dropped already, but how can we drop a table when there has
				// a table refer to it?
				// the FOREIGN_KEY_CHECKS disabled !!!
				// so this inexistence is expected, no need to return an error.
				if isMissingTableByIdForFkCleanup(err) {
					logutil.Warn(
						"cannot find the referred table when drop database",
						zap.String("table", fmt.Sprintf("%s-%s", dbName, rel)),
						zap.Int("referred table id", int(fkey.ForeignTbl)),
						zap.String("txn info", c.proc.GetTxnOperator().Txn().DebugString()),
						zap.Error(err),
					)
					continue
				}
				return err
			} else {
				if err = s.removeChildTblIdFromParentTable(c, parentTable, tblId); err != nil {
					return err
				}
			}
		}

		//remove tblId from the child table
		for _, childId := range refChild.Tables {
			if childId == 0 {
				continue
			}
			_, _, childTable, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), childId)
			if err != nil {
				if isMissingTableByIdForFkCleanup(err) {
					logutil.Warn(
						"cannot find child table when drop database fk cleanup",
						zap.String("table", fmt.Sprintf("%s-%s", dbName, rel)),
						zap.Int("child table id", int(childId)),
						zap.String("txn info", c.proc.GetTxnOperator().Txn().DebugString()),
						zap.Error(err),
					)
					continue
				}
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

func isMissingTableByNameForDropDatabase(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrNoSuchTable)
}

func isMissingTableByIdForFkCleanup(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) ||
		(moerr.IsMoErrCode(err, moerr.ErrInternal) &&
			strings.Contains(err.Error(), "can not find table by id"))
}

// Drop the old view, and create the new view.
func (s *Scope) AlterView(c *Compile) error {
	qry := s.Plan.GetDdl().GetAlterView()

	dbName := c.db
	tblName := qry.GetTableDef().GetName()

	if err := lockMoDatabase(c, dbName, lock.LockMode_Shared); err != nil {
		return err
	}
	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return convertDBEOB(c.proc.Ctx, err, dbName)
	}
	oldRelation, err := dbSource.Relation(c.proc.Ctx, tblName, nil)
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}
	oldRelationID := oldRelation.GetTableID(c.proc.Ctx)
	oldLogicalID := oldRelation.GetTableDef(c.proc.Ctx).GetLogicalId()

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
	exeCols := engine.PlanColsToExeCols(planCols)

	// convert the plan's defs to the execution's defs
	exeDefs, _, err := engine.PlanDefsToExeDefs(qry.GetTableDef())
	if err != nil {
		return err
	}

	createCtx := context.WithValue(c.proc.Ctx, defines.SqlKey{}, c.sql)
	// ALTER VIEW replaces the physical catalog row but preserves the logical
	// object identity, so exact-view grants remain attached to the view.
	createCtx = context.WithValue(createCtx, defines.LogicalIdKey{}, oldLogicalID)
	if err = dbSource.Create(createCtx, tblName, append(exeCols, exeDefs...)); err != nil {
		return err
	}
	if err = c.persistViewDependencies(dbSource, dbName, qry.GetTableDef()); err != nil {
		return err
	}
	return c.refreshViewsAfterRelationMutation(dbName, tblName, oldRelationID, oldLogicalID)
}

// reindexSpecifiedParams extracts the build options the user wrote on
// `ALTER TABLE ... ALTER REINDEX <indexName> <algo> <options>` from the parse
// tree, keyed by the catalog IndexAlgoParam* name. The REINDEX rule shares
// index_option_list with CREATE INDEX, so every option parses and is carried
// on the tree node (which mirrors tree.IndexOption); the plan node only
// carries lists/force_sync, so the full set is read here off c.stmt rather
// than the plan. Each plugin's Compile.ValidateReindexParams then merges the
// options it honors on a rebuild and rejects the rest. Returns nil when the
// statement is not an ALTER TABLE carrying a matching REINDEX option (e.g. an
// unexpected statement shape); an empty/zero option set yields an empty map.
func reindexSpecifiedParams(stmt tree.Statement, indexName string) map[string]string {
	at, ok := stmt.(*tree.AlterTable)
	if !ok {
		return nil
	}
	var opt *tree.AlterOptionAlterReIndex
	for _, o := range at.Options {
		if ro, ok := o.(*tree.AlterOptionAlterReIndex); ok && plan2.IndexNamesEqual(string(ro.Name), indexName) {
			opt = ro
			break
		}
	}
	if opt == nil {
		return nil
	}
	m := make(map[string]string)
	addInt := func(key string, v int64) {
		if v != 0 {
			m[key] = strconv.FormatInt(v, 10)
		}
	}
	addStr := func(key, v string) {
		if v != "" {
			m[key] = v
		}
	}
	addInt(catalog.IndexAlgoParamLists, opt.AlgoParamList)
	addStr(catalog.IndexAlgoParamOpType, opt.AlgoParamVectorOpType)
	addInt(catalog.HnswM, opt.AlgoParamM)
	addInt(catalog.HnswEfConstruction, opt.HnswEfConstruction)
	addInt(catalog.HnswEfSearch, opt.HnswEfSearch)
	addInt(catalog.BitsPerCode, opt.BitsPerCode)
	addInt(catalog.IntermediateGraphDegree, opt.IntermediateGraphDegree)
	addInt(catalog.GraphDegree, opt.GraphDegree)
	addInt(catalog.ITopkSize, opt.ITopkSize)
	addStr(catalog.DistributionMode, opt.DistributionMode)
	addInt(catalog.IndexAlgoParamKmeansTrainPercent, opt.KmeansTrainPercent)
	addInt(catalog.IndexAlgoParamKmeansMaxIteration, opt.KmeansMaxIteration)
	addInt(catalog.IndexAlgoParamMaxIndexCapacity, opt.MaxIndexCapacity)
	addInt(catalog.IndexAlgoParamMaxPostingsCapacity, opt.MaxPostingsCapacity)
	addInt(catalog.IndexAlgoParamQuantizerTrainLimit, opt.QuantizerTrainLimit)
	// position_free is emitted as an explicit true/false when specified (tri-state:
	// absence ⇒ "keep current"), so a fulltext2 REINDEX can turn it on OR off.
	if opt.PositionFreeSet {
		m[catalog.IndexAlgoParamPositionFree] = strconv.FormatBool(opt.PositionFree)
	}
	// quantization is normalized to lowercase here (matching the CREATE INDEX
	// path) so case-sensitive consumers (GPU build switch / quantizer) behave
	// identically; the per-backend VALUE check (which names a given algorithm
	// accepts) is done in each plugin's ValidateReindexParams.
	if opt.Quantization != "" {
		m[catalog.Quantization] = catalog.ToLower(opt.Quantization)
	}
	return m
}

func validateAlterForeignKeyNameActions(
	ctx context.Context,
	existing map[string]bool,
	actions []*plan.AlterTable_Action,
) error {
	current := make(map[string]bool, len(existing))
	for name := range existing {
		current[name] = true
	}

	for _, action := range actions {
		if action == nil {
			continue
		}
		switch alterAction := action.Action.(type) {
		case *plan.AlterTable_Action_Drop:
			drop := alterAction.Drop
			if drop.Typ != plan.AlterTableDrop_FOREIGN_KEY {
				continue
			}
			if !current[drop.Name] {
				return moerr.NewErrCantDropFieldOrKey(ctx, drop.Name)
			}
			delete(current, drop.Name)
		case *plan.AlterTable_Action_AddFk:
			name := alterAction.AddFk.Fkey.Name
			if current[name] {
				return moerr.NewErrDuplicateKeyName(ctx, name)
			}
			current[name] = true
		}
	}
	return nil
}

func foreignKeyParentIDs(fkeys []*plan.ForeignKeyDef) map[uint64]struct{} {
	parents := make(map[uint64]struct{}, len(fkeys))
	for _, fkey := range fkeys {
		parents[fkey.ForeignTbl] = struct{}{}
	}
	return parents
}

func (s *Scope) AlterTableInplace(c *Compile) (err error) {
	cleanup := newAlterAutoIncrementResetCleanup(c)
	defer cleanup.finish(&err)
	return s.alterTableInplace(c, cleanup)
}

func (s *Scope) alterTableInplace(c *Compile, cleanup *alterAutoIncrementResetCleanup) error {
	qry := s.Plan.GetDdl().GetAlterTable()
	dbName := qry.Database
	if dbName == "" {
		dbName = c.db
	}

	tblName := qry.GetTableDef().GetName()
	isTemp := qry.GetTableDef().GetIsTemporary()
	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return convertDBEOBToNoSuchTable(c.proc.Ctx, err, dbName, tblName)
	}
	databaseId := dbSource.GetDatabaseId(c.proc.Ctx)

	rel, err := dbSource.Relation(c.proc.Ctx, tblName, nil)
	if err != nil {
		return err
	}
	tblId := rel.GetTableID(c.proc.Ctx)
	extra := rel.GetExtraInfo()

	oTableDef := plan2.DeepCopyTableDef(qry.TableDef, true)

	var oldCt *engine.ConstraintDef
	newCt := &engine.ConstraintDef{
		Cts: []engine.Constraint{},
	}

	if qry.GetCopyTableDef() != nil {
		oldCt = engine.PlanDefToCstrDef(qry.GetCopyTableDef())
	} else {
		oldCt, err = GetConstraintDef(c.proc.Ctx, rel)
		if err != nil {
			return err
		}
	}

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
	if err := validateAlterForeignKeyNameActions(c.proc.Ctx, oldFkNames, qry.Actions); err != nil {
		return err
	}

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
		if err = lockTable(c.proc.Ctx, c.e, c.proc, rel, dbName, true); err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return err
			}
			retryErr = moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
		}

		if qry.TableDef.Indexes != nil {
			for _, indexdef := range qry.TableDef.Indexes {
				if indexdef.TableExist {
					if err = lockIndexTableForAlter(
						c.proc.Ctx,
						dbSource,
						c.e,
						c.proc,
						tblName,
						qry.TableDef.TblId,
						indexdef,
						retryErr != nil,
					); err != nil {
						if !moerr.IsMoErrCode(err, moerr.ErrParseError) &&
							!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
							!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
							c.proc.Error(
								c.proc.Ctx,
								"alter.table.lock.index.table",
								zap.String("db", c.db),
								zap.String("main-table", qry.GetTableDef().Name),
								zap.String("index-name", indexdef.IndexName),
								zap.String("index-table-name", indexdef.IndexTableName),
								zap.Error(err))
							return err
						}
						retryErr = moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
					}
				}
			}
		}

		// 3. lock foreign key's table
		for _, action := range qry.Actions {
			if action == nil {
				continue
			}
			switch act := action.Action.(type) {
			case *plan.AlterTable_Action_Drop:
				alterTableDrop := act.Drop
				constraintName := alterTableDrop.Name
				if alterTableDrop.Typ == plan.AlterTableDrop_FOREIGN_KEY {
					for _, fk := range oTableDef.Fkeys {
						if fk.Name == constraintName && fk.ForeignTbl != 0 { //skip self ref foreign key
							// lock fk table
							fkDbName, fkTableName, err := c.e.GetNameById(c.proc.Ctx, c.proc.GetTxnOperator(), fk.ForeignTbl)
							if err != nil {
								return err
							}
							if err = lockMoTable(c, fkDbName, fkTableName, lock.LockMode_Exclusive); err != nil {
								if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
									!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
									return err
								}
								retryErr = moerr.NewTxnNeedRetryWithDefChangedNoCtx()
							}
						}
					}
				}
			case *plan.AlterTable_Action_AddFk:
				// lock fk table
				if !(act.AddFk.DbName != dbName && act.AddFk.TableName != tblName) { //skip self ref foreign key
					if err = lockMoTable(c, act.AddFk.DbName, act.AddFk.TableName, lock.LockMode_Exclusive); err != nil {
						if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
							!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
							return err
						}
						retryErr = moerr.NewTxnNeedRetryWithDefChangedNoCtx()
					}
				}
			}
		}

		if retryErr != nil {
			return retryErr
		}
	}

	initialForeignKeyParents := foreignKeyParentIDs(oTableDef.Fkeys)

	var hasUpdateConstraints bool
	var hasDefReplace bool
	// New foreign keys are stored before pre-existing ones for compatibility
	// with the constraint ordering produced by earlier versions. Within that
	// prefix, preserve the ALTER action order while still allowing later DROP
	// actions to consume an earlier ADD.
	addedForeignKeyCount := 0
	var alterIndex *plan.IndexDef

	reqs := make([]*api.AlterTableReq, 0)
	did := rel.GetDBID(c.proc.Ctx)
	tid := rel.GetTableID(c.proc.Ctx)

	for _, action := range qry.Actions {
		if action == nil {
			continue
		}
		switch act := action.Action.(type) {
		case *plan.AlterTable_Action_Drop:
			alterTableDrop := act.Drop
			constraintName := alterTableDrop.Name
			switch alterTableDrop.Typ {
			case plan.AlterTableDrop_FOREIGN_KEY:
				hasUpdateConstraints = true
				for i, fk := range oTableDef.Fkeys {
					if fk.Name != constraintName {
						continue
					}
					copy(oTableDef.Fkeys[i:], oTableDef.Fkeys[i+1:])
					oTableDef.Fkeys[len(oTableDef.Fkeys)-1] = nil
					oTableDef.Fkeys = oTableDef.Fkeys[:len(oTableDef.Fkeys)-1]
					if i < addedForeignKeyCount {
						addedForeignKeyCount--
					}
					break
				}
			case plan.AlterTableDrop_INDEX:
				hasUpdateConstraints = true
				var notDroppedIndex []*plan.IndexDef
				var newIndexes []uint64
				if err = DropIndexCdcTask(c, oTableDef, dbName, tblName, constraintName); err != nil {
					return err
				}
				if err = DrainIndexCdcTaskConsumer(c, oTableDef, dbName, tblName, constraintName); err != nil {
					return err
				}
				for idx, indexdef := range oTableDef.Indexes {
					if indexdef.IndexName == constraintName {
						//1. drop index table
						if indexdef.TableExist {
							if err := c.runSqlWithOptions(
								"DROP TABLE "+sqlquote.QualifiedIdent(dbName, indexdef.IndexTableName),
								executor.StatementOption{}.WithDisableLog(),
							); err != nil {
								return err
							}
						}
						deleteSql := fmt.Sprintf(deleteMoIndexesWithTableIdAndIndexNameFormat, oTableDef.TblId, indexdef.IndexName)
						if err = c.runSqlWithOptions(
							deleteSql, executor.StatementOption{}.WithDisableLog(),
						); err != nil {
							return err
						}
					} else {
						notDroppedIndex = append(notDroppedIndex, indexdef)
						newIndexes = append(newIndexes, extra.IndexTables[idx])
					}
				}

				// drop index cdc task
				err = DropIndexCdcTask(c, oTableDef, dbName, tblName, constraintName)
				if err != nil {
					return err
				}

				// unregister index update
				err = idxcron.UnregisterUpdate(c.proc.Ctx,
					c.proc.GetService(),
					c.proc.GetTxnOperator(),
					oTableDef.TblId,
					constraintName,
					idxcron.Action_Wildcard)
				if err != nil {
					return err
				}

				// Avoid modifying slice directly during iteration
				oTableDef.Indexes = notDroppedIndex
				extra.IndexTables = newIndexes
			}
		case *plan.AlterTable_Action_AddFk:
			hasUpdateConstraints = true
			oTableDef.Fkeys = append(oTableDef.Fkeys, nil)
			copy(oTableDef.Fkeys[addedForeignKeyCount+1:], oTableDef.Fkeys[addedForeignKeyCount:])
			oTableDef.Fkeys[addedForeignKeyCount] = act.AddFk.Fkey
			addedForeignKeyCount++

		case *plan.AlterTable_Action_AddIndex:
			hasUpdateConstraints = true

			indexInfo := act.AddIndex.IndexInfo // IndexInfo is named same as planner's IndexInfo
			indexTableDef := act.AddIndex.IndexInfo.TableDef

			if isTemp {
				session := c.proc.GetSession()
				if session == nil {
					return moerr.NewInternalError(c.proc.Ctx, "session not found for temporary table")
				}
				tmpDb := act.AddIndex.DbName
				if tmpDb == "" {
					tmpDb = dbName
				}
				indexNameMap := make(map[string]string, len(indexInfo.IndexTables))
				for _, def := range indexInfo.IndexTables {
					orig := def.Name
					if !defines.IsTempTableName(orig) {
						def.Name = defines.GenTempTableName(c.proc.Base.SessionInfo.SessionId, tmpDb, orig)
					}
					def.TableType = catalog.SystemTemporaryTable
					def.IsTemporary = true
					indexNameMap[orig] = def.Name
				}
				for _, idx := range indexTableDef.Indexes {
					if renamed, ok := indexNameMap[idx.IndexTableName]; ok {
						idx.IndexTableName = renamed
					}
				}
			}

			// indexName -> meta      -> indexDef
			//     		 -> centroids -> indexDef
			//     		 -> entries   -> indexDef
			multiTableIndexes := make(map[string]*MultiTableIndex)
			newIndexParts := make(map[string]struct{}, len(indexTableDef.Indexes))
			for _, indexDef := range indexTableDef.Indexes {

				// Check for duplicate index names
				// For vector indexes (IVFFLAT/HNSW), multiple indexDefs share the same IndexName
				// but have different IndexAlgoTableType (metadata, centroids, entries).
				// We need to check both IndexName and IndexAlgoTableType for duplicates.
				isDuplicate := false
				for i := range oTableDef.Indexes {
					if indexDef.IndexName == oTableDef.Indexes[i].IndexName &&
						indexDef.IndexAlgoTableType == oTableDef.Indexes[i].IndexAlgoTableType {
						isDuplicate = true
						break
					}
				}
				if isDuplicate {
					return moerr.NewDuplicateKey(c.proc.Ctx, indexDef.IndexName)
				}
				partKey := indexDef.IndexName + "\x00" + indexDef.IndexAlgoTableType
				if _, exists := newIndexParts[partKey]; exists {
					return moerr.NewDuplicateKey(c.proc.Ctx, indexDef.IndexName)
				}
				newIndexParts[partKey] = struct{}{}
				if indexDef.Unique {
					// 1. Unique Index related logic
					err = s.handleUniqueIndexTable(c, tblId, extra, dbSource, indexDef, qry.Database, oTableDef, indexInfo)
				} else if !indexDef.Unique && catalog.IsRegularIndexAlgo(indexDef.IndexAlgo) {
					// 2. Regular Secondary index
					err = s.handleRegularSecondaryIndexTable(c, tblId, extra, dbSource, indexDef, qry.Database, oTableDef, indexInfo)
				} else if !indexDef.Unique && catalog.IsMasterIndexAlgo(indexDef.IndexAlgo) {
					// 3. Master index
					err = s.handleMasterIndexTable(c, tblId, extra, dbSource, indexDef, qry.Database, oTableDef, indexInfo)
				} else if !indexDef.Unique && indexplugin.IsPluginAlgo(indexDef.IndexAlgo) {
					// 4. Plugin-registered indexes (vector + fulltext)
					// are aggregated and handled later by the per-plugin
					// compile hook.
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
			// cctx is loop-invariant — hoist to avoid per-index allocs.
			var cctx *pluginCompileCtx
			for _, multiTableIndex := range multiTableIndexes {
				if p, ok := indexplugin.Get(multiTableIndex.IndexAlgo); ok {
					if cctx == nil {
						cctx = newPluginCompileCtx(s, c, tblId, extra, dbSource, qry.Database, oTableDef, indexInfo)
					}
					err = p.Compile().HandleCreateIndex(cctx, multiTableIndex.IndexDefs)
				}

				if err != nil {
					return err
				}
			}

			//1. build and update constraint def
			for _, indexDef := range indexTableDef.Indexes {
				insertSql, err := makeInsertSingleIndexSQL(c.e, c.proc, databaseId, tblId, indexDef, oTableDef)
				if err != nil {
					return err
				}
				if err = c.runSqlWithOptions(
					insertSql, executor.StatementOption{}.WithDisableLog(),
				); err != nil {
					return err
				}
			}
			oTableDef.Indexes = append(oTableDef.Indexes, indexTableDef.Indexes...)
		case *plan.AlterTable_Action_AlterIndex:
			hasUpdateConstraints = true
			tableAlterIndex := act.AlterIndex
			constraintName := tableAlterIndex.IndexName
			matchedIndex := false
			for _, indexDef := range oTableDef.Indexes {
				if indexDef.IndexName != constraintName {
					continue
				}
				catalog.SetIndexVisibility(indexDef, tableAlterIndex.Visible)
				matchedIndex = true
			}
			if matchedIndex {
				// update the index visibility in mo_catalog.mo_indexes once for
				// the logical index. Multi-table indexes have several physical
				// IndexDefs with the same name, while the catalog statement
				// updates all of their rows.
				// Escape the index name the same as the AUTO_UPDATE / REINDEX
				// branches: it is user-supplied and a backticked identifier may
				// contain single quotes or backslashes (the scanner still treats
				// backslash as an escape inside '...'), which could corrupt or
				// break out of name = '...'.
				visible := 0
				if tableAlterIndex.Visible {
					visible = 1
				}
				updateSql := fmt.Sprintf(updateMoIndexesVisibleFormat, visible, oTableDef.TblId,
					sqlquote.EscapeString(constraintName))
				if err = c.runSqlWithOptions(
					updateSql, executor.StatementOption{}.WithDisableLog(),
				); err != nil {
					return err
				}
			}
		case *plan.AlterTable_Action_AlterAutoUpdate:
			hasUpdateConstraints = true
			tableAlterIndex := act.AlterAutoUpdate
			constraintName := tableAlterIndex.IndexName

			// simply update the index configuration
			for i, indexDef := range oTableDef.Indexes {
				if indexDef.IndexName == constraintName {
					alterIndex = indexDef

					indexAlgo := catalog.ToLower(alterIndex.IndexAlgo)
					// AlterAutoUpdate updates the scheduled-rebuild
					// cadence. Gate on whether the algorithm participates
					// in idxcron — today only IVF-FLAT does, but CAGRA /
					// IVF-PQ become eligible once their IdxcronAction
					// values are wired.
					p, ok := indexplugin.Get(indexAlgo)
					if !ok || p.Catalog().SyncDescriptor().IdxcronAction == "" {
						return moerr.NewInternalError(c.proc.Ctx, "invalid index algo type for alter reindex")
					}
					// 1. Update AutoUpdate/Day/Hour in AlgoParams.
					newAlgoParamsMap, err := catalog.IndexParamsStringToMap(alterIndex.IndexAlgoParams)
					if err != nil {
						return err
					}
					newAlgoParamsMap[catalog.AutoUpdate] = fmt.Sprintf("%v", tableAlterIndex.AutoUpdate)
					newAlgoParamsMap[catalog.Day] = fmt.Sprintf("%d", tableAlterIndex.Day)
					newAlgoParamsMap[catalog.Hour] = fmt.Sprintf("%d", tableAlterIndex.Hour)
					// Preserve the captured session_vars (skipped by the flat
					// parser above) across the cadence rewrite.
					sessionVars, err := catalog.IndexParamsSessionVars(alterIndex.IndexAlgoParams)
					if err != nil {
						return err
					}
					newAlgoParams, err := catalog.IndexParamsMapToJsonStringWithSessionVars(newAlgoParamsMap, sessionVars)
					if err != nil {
						return err
					}

					// 2. Update IndexDef and mo_catalog.mo_indexes.
					alterIndex.IndexAlgoParams = newAlgoParams
					oTableDef.Indexes[i].IndexAlgoParams = newAlgoParams
					// Escape the SQL string literals, same as the REINDEX branch
					// below: algo_params is a JSON blob (JSON does not escape SQL
					// quotes/backslashes) and the index name is user-supplied, so
					// an unescaped quote or backslash could corrupt or break out of
					// algo_params = '...' / name = '...'.
					updateSql := fmt.Sprintf(updateMoIndexesAlgoParams,
						sqlquote.EscapeString(newAlgoParams), oTableDef.TblId,
						sqlquote.EscapeString(alterIndex.IndexName))
					if err = c.runSqlWithOptions(
						updateSql, executor.StatementOption{}.WithDisableLog(),
					); err != nil {
						return err
					}

					// 3. Re-register the idxcron update with the
					// refreshed metadata. The plugin's IdxcronMetadata
					// hook owns metadata composition; SyncDescriptor
					// supplies the action key (already gated above).
					// Skip re-registration entirely when invoked from a
					// background idxcron job — the existing task's
					// captured metadata is authoritative.
					desc := p.Catalog().SyncDescriptor()
					cctx := newPluginCompileCtx(s, c, tblId, extra, dbSource, qry.Database, oTableDef, nil)
					if !cctx.IsFrontend() {
						continue
					}
					metadata, err := p.Compile().IdxcronMetadata(cctx)
					if err != nil {
						return err
					}
					if err = cctx.RegisterIdxcronUpdate(
						oTableDef.TblId, qry.Database, oTableDef.Name,
						indexDef.IndexName, desc.IdxcronAction, metadata,
					); err != nil {
						return err
					}
				}
			}

		case *plan.AlterTable_Action_AlterReindex:
			// NOTE: We hold lock (with retry) during alter reindex, as "alter table" takes an exclusive lock
			//in the beginning for pessimistic mode. We need to see how to reduce the critical section.
			hasUpdateConstraints = true
			tableAlterIndex := act.AlterReindex
			constraintName := tableAlterIndex.IndexName
			multiTableIndexes := make(map[string]*MultiTableIndex)

			for i, indexDef := range oTableDef.Indexes {
				if indexDef.IndexName == constraintName {
					alterIndex = indexDef

					indexAlgo := catalog.ToLower(alterIndex.IndexAlgo)
					if !indexplugin.IsVectorIndexAlgo(indexAlgo) && !catalog.IsFullText2IndexAlgo(indexAlgo) {
						return moerr.NewInternalError(c.proc.Ctx, "invalid index algo type for alter reindex")
					}
					// Each algorithm's plugin owns parameter-update
					// semantics via Compile.ValidateReindexParams: it
					// merges the build options it honors on a rebuild
					// (e.g. IVF-FLAT's `lists`, HNSW's `m`/`ef_*`, CAGRA's
					// graph degrees) into the algo params and rejects any
					// other option it does not support. (quantization is left
					// entirely to the vecf16 quantization work — reindexSpecified
					// Params does not extract it, so reindex ignores it.) The
					// REINDEX rule shares index_option_list with CREATE INDEX, so
					// the specified options are read straight off the parse tree
					// (c.stmt) here — no plan proto field is needed to carry them.
					oldParams, err := catalog.IndexParamsStringToMap(alterIndex.IndexAlgoParams)
					if err != nil {
						return err
					}
					p, _ := indexplugin.Get(indexAlgo)
					newParamsMap, err := p.Compile().ValidateReindexParams(oldParams,
						compileplugin.ReindexParamUpdate{
							Params: reindexSpecifiedParams(c.stmt, constraintName),
							Merge:  tableAlterIndex.Merge,
						})
					if err != nil {
						return err
					}
					// Preserve the captured session_vars (skipped by the flat
					// parser above) across the param rewrite.
					sessionVars, err := catalog.IndexParamsSessionVars(alterIndex.IndexAlgoParams)
					if err != nil {
						return err
					}
					newAlgoParams, err := catalog.IndexParamsMapToJsonStringWithSessionVars(newParamsMap, sessionVars)
					if err != nil {
						return err
					}
					if newAlgoParams != alterIndex.IndexAlgoParams {
						alterIndex.IndexAlgoParams = newAlgoParams
						oTableDef.Indexes[i].IndexAlgoParams = newAlgoParams
						// Escape the SQL string literals: algo_params can carry
						// user-supplied option values and JSON encoding does not
						// escape single quotes, so an unescaped value could break
						// out of algo_params = '...' (SQL injection). Defense in
						// depth for any future string option (none reach here
						// today). sqlquote.EscapeString doubles quotes.
						updateSql := fmt.Sprintf(updateMoIndexesAlgoParams,
							sqlquote.EscapeString(newAlgoParams), oTableDef.TblId,
							sqlquote.EscapeString(alterIndex.IndexName))
						if err = c.runSqlWithOptions(
							updateSql, executor.StatementOption{}.WithDisableLog(),
						); err != nil {
							return err
						}
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

			// update the hidden tables — cctx is loop-invariant.
			var cctx *pluginCompileCtx
			for _, multiTableIndex := range multiTableIndexes {
				if p, ok := indexplugin.Get(multiTableIndex.IndexAlgo); ok {
					if cctx == nil {
						cctx = newPluginCompileCtx(s, c, tblId, extra, dbSource, qry.Database, oTableDef, nil)
					}
					err = p.Compile().HandleReindex(cctx, multiTableIndex.IndexDefs, tableAlterIndex.ForceSync, tableAlterIndex.Merge)
				}

				if err != nil {
					return err
				}
			}
		case *plan.AlterTable_Action_AlterComment:
			reqs = append(reqs, api.NewUpdateCommentReq(
				did, tid,
				act.AlterComment.NewComment,
			))
		case *plan.AlterTable_Action_AlterAutoIncrement:
			targetTableDef := qry.GetCopyTableDef()
			if targetTableDef == nil {
				targetTableDef = qry.GetTableDef()
			}
			if err := c.appendAlterAutoIncrementReqs(
				dbName,
				qry.GetTableDef(),
				targetTableDef,
				did,
				tid,
				act.AlterAutoIncrement.NewOffset,
				cleanup,
				&reqs,
			); err != nil {
				return err
			}
		case *plan.AlterTable_Action_AlterName:
			reqs = append(reqs, api.NewRenameTableReq(
				did, tid,
				act.AlterName.OldName,
				act.AlterName.NewName,
			))
		case *plan.AlterTable_Action_AlterRenameColumn:
			hasDefReplace = true
			if err := c.requireCheckRenameProtocol(qry.GetCopyTableDef().GetChecks()); err != nil {
				return err
			}
			reqs = append(reqs, api.NewRenameColumnReqWithChecks(
				did, tid,
				act.AlterRenameColumn.OldName, // origin name
				act.AlterRenameColumn.NewName, // origin name
				uint32(act.AlterRenameColumn.SequenceNum),
				qry.GetCopyTableDef().GetChecks(),
			))

		case *plan.AlterTable_Action_AlterReplaceDef:
			hasDefReplace = true
		default:
			return moerr.NewInternalErrorNoCtxf(
				"invalid alter table action: %s",
				action.String(),
			)
		}
	}

	// reset origin table's constraint
	for _, fkey := range oTableDef.Fkeys {
		// For compatibility, regenerate constraint names written by older
		// versions before names became mandatory.
		if len(fkey.Name) == 0 {
			fkey.Name = plan2.GenConstraintName()
		}
	}
	originHasFkDef := false
	originHasIndexDef := false
	for _, ct := range oldCt.Cts {
		switch t := ct.(type) {
		case *engine.ForeignKeyDef:
			t.Fkeys = oTableDef.Fkeys
			originHasFkDef = true
			newCt.Cts = append(newCt.Cts, t)
		case *engine.RefChildTableDef:
			newCt.Cts = append(newCt.Cts, t)
		case *engine.IndexDef:
			originHasIndexDef = true
			t.Indexes = oTableDef.Indexes
			newCt.Cts = append(newCt.Cts, t)
		case *engine.PrimaryKeyDef:
			newCt.Cts = append(newCt.Cts, t)
		case *engine.StreamConfigsDef:
			newCt.Cts = append(newCt.Cts, t)
		}
	}
	if !originHasFkDef {
		newCt.Cts = append(newCt.Cts, &engine.ForeignKeyDef{
			Fkeys: oTableDef.Fkeys,
		})
	}
	if !originHasIndexDef && len(oTableDef.Indexes) > 0 {
		newCt.Cts = append(newCt.Cts, &engine.IndexDef{
			Indexes: oTableDef.Indexes,
		})
	}

	// add requests that require exactly-once execution semantics
	if hasDefReplace {
		// replace def take the very first place
		reqs = append([]*api.AlterTableReq{
			api.NewReplaceDefReq(did, tid, qry.GetCopyTableDef()),
		}, reqs...)
	}

	if hasUpdateConstraints {
		ct, err := newCt.MarshalBinary()
		if err != nil {
			return err
		}
		reqs = append(reqs, api.NewUpdateConstraintReq(did, tid, string(ct)))
	}

	err = rel.AlterTable(c.proc.Ctx, newCt, reqs)
	if err != nil {
		return err
	}

	// post alter table rename -- AlterKind_RenameTable to update iscp job
	for _, req := range reqs {
		if req.Kind == api.AlterKind_RenameTable {
			op, ok := req.Operation.(*api.AlterTableReq_RenameTable)
			if ok {
				// iscp
				err = iscp.RenameSrcTable(c.proc.Ctx,
					c.proc.GetService(),
					c.proc.GetTxnOperator(),
					req.DbId,
					req.TableId,
					op.RenameTable.OldName,
					op.RenameTable.NewName)
				if err != nil {
					return err
				}

				// idxcron
				err = idxcron.RenameSrcTable(c.proc.Ctx,
					c.proc.GetService(),
					c.proc.GetTxnOperator(),
					req.DbId,
					req.TableId,
					op.RenameTable.OldName,
					op.RenameTable.NewName)
				if err != nil {
					return err
				}
			}
		}
	}

	finalForeignKeyParents := foreignKeyParentIDs(oTableDef.Fkeys)
	// Update parent ref-child metadata from the initial and final schemas, not
	// from intermediate ADD/DROP actions. Replacements that end at the same
	// parent are a no-op, while ADD->DROP leaves no stale child reference.
	for fkTblId := range initialForeignKeyParents {
		if _, stillReferenced := finalForeignKeyParents[fkTblId]; stillReferenced {
			continue
		}
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

	for fkTblId := range finalForeignKeyParents {
		if _, alreadyReferenced := initialForeignKeyParents[fkTblId]; alreadyReferenced {
			continue
		}
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
	return s.createTable(c, nil)
}

// createTable invokes tableCreated immediately after it creates the main table.
// This lets callers that have follow-up work distinguish IF NOT EXISTS's no-op
// success from a physical table creation without repeating the existence check.
func (s *Scope) createTable(c *Compile, tableCreated func()) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	qry := s.Plan.GetDdl().GetCreateTable()
	dbName := c.db
	if qry.GetDatabase() != "" {
		dbName = qry.GetDatabase()
	}
	aliasName := qry.GetTableDef().GetName()
	session := c.proc.GetSession()
	isTemp := qry.GetTemporary()
	if isTemp {
		if session == nil {
			return moerr.NewInternalError(c.proc.Ctx, "session not found for temporary table")
		}
		if _, exists := session.GetTempTable(dbName, aliasName); exists {
			if qry.GetIfNotExists() {
				return nil
			}
			return moerr.NewTableAlreadyExists(c.proc.Ctx, aliasName)
		}

		type tempIndexTableState struct {
			def         *plan.TableDef
			name        string
			tableType   string
			isTemporary bool
			tableID     uint64
		}
		type tempIndexState struct {
			def            *plan.IndexDef
			indexTableName string
		}
		originalTableName := qry.TableDef.Name
		indexTableStates := make([]tempIndexTableState, 0, len(qry.IndexTables))
		for _, def := range qry.IndexTables {
			indexTableStates = append(indexTableStates, tempIndexTableState{
				def: def, name: def.Name, tableType: def.TableType, isTemporary: def.IsTemporary, tableID: def.TblId,
			})
		}
		indexStates := make([]tempIndexState, 0, len(qry.TableDef.Indexes))
		for _, idx := range qry.TableDef.Indexes {
			indexStates = append(indexStates, tempIndexState{def: idx, indexTableName: idx.IndexTableName})
		}
		defer func() {
			qry.TableDef.Name = originalTableName
			for _, state := range indexTableStates {
				state.def.Name = state.name
				state.def.TableType = state.tableType
				state.def.IsTemporary = state.isTemporary
				state.def.TblId = state.tableID
			}
			for _, state := range indexStates {
				state.def.IndexTableName = state.indexTableName
			}
		}()

		realName := physicalTemporaryTableName(c.proc, dbName, aliasName)
		qry.TableDef.Name = realName
		indexNameMap := make(map[string]string, len(qry.IndexTables))
		for _, def := range qry.IndexTables {
			orig := def.Name
			newName := physicalTemporaryTableName(c.proc, dbName, orig)
			def.Name = newName
			def.TableType = catalog.SystemTemporaryTable
			def.IsTemporary = true
			indexNameMap[orig] = newName
		}
		for _, idx := range qry.TableDef.Indexes {
			if renamed, ok := indexNameMap[idx.IndexTableName]; ok {
				idx.IndexTableName = renamed
			}
		}
		if len(qry.GetTableDef().Fkeys) > 0 {
			return moerr.NewNotSupported(c.proc.Ctx, "add foreign key for temporary table")
		}
	}
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
	exeCols := engine.PlanColsToExeCols(planCols)

	// convert the plan's defs to the execution's defs
	exeDefs, extra, err := engine.PlanDefsToExeDefs(qry.GetTableDef())
	if err != nil {
		c.proc.Error(c.proc.Ctx, "createTable",
			zap.String("databaseName", c.db),
			zap.String("tableName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}

	if c.adjustTableExtraFunc != nil {
		if err := c.adjustTableExtraFunc(extra); err != nil {
			return err
		}
	}

	tblName := qry.GetTableDef().GetName()

	if !c.disableLock {
		if err := lockMoDatabase(c, dbName, lock.LockMode_Shared); err != nil {
			return err
		}
	}

	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if dbName == "" {
			return moerr.NewNoDB(c.proc.Ctx)
		}
		return convertDBEOB(c.proc.Ctx, err, dbName)
	}

	exists, err := dbSource.RelationExists(c.proc.Ctx, tblName, nil)
	if err != nil {
		c.proc.Error(c.proc.Ctx, "check table relation exists failed",
			zap.String("databaseName", c.db),
			zap.String("tableName", tblName),
			zap.Error(err),
		)
		return err
	}
	if exists {
		if qry.GetIfNotExists() {
			return nil
		}
		return moerr.NewTableAlreadyExists(c.proc.Ctx, tblName)
	}

	if !c.disableLock {
		if err = lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); err != nil {
			c.proc.Error(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
	}

	if len(qry.IndexTables) > 0 {
		for _, def := range qry.IndexTables {
			id, err := c.e.AllocateIDByKey(c.proc.Ctx, "")
			if err != nil {
				return err
			}
			def.TblId = id
			extra.IndexTables = append(extra.IndexTables, id)
		}
	}

	if err = dbSource.Create(
		context.WithValue(c.proc.Ctx,
			defines.SqlKey{}, c.sql), tblName,
		append(exeCols, exeDefs...),
	); err != nil {
		c.proc.Error(c.proc.Ctx, "createTable",
			zap.String("databaseName", c.db),
			zap.String("tableName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}
	if tableCreated != nil {
		tableCreated()
	}

	rollbackTempAlias := false
	if isTemp && session != nil {
		session.AddTempTable(dbName, aliasName, tblName)
		rollbackTempAlias = true
		defer func() {
			if rollbackTempAlias {
				session.RemoveTempTable(dbName, aliasName)
			}
		}()
	}

	//update mo_foreign_keys
	for _, sql := range qry.UpdateFkSqls {
		if err = c.runSqlWithOptions(
			sql, executor.StatementOption{}.WithDisableLog(),
		); err != nil {
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
				colNameToId[strings.ToLower(attr.Attr.Name)] = attr.Attr.ID
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
				return moerr.NewInternalErrorf(c.proc.Ctx, "deduplicate fk name %s", fkey.Name)
			}
			dedupFkName.Insert(fkey.Name)
			newDef := &plan.ForeignKeyDef{
				Name:                fkey.Name,
				Cols:                make([]uint64, len(fkey.Cols)),
				ForeignTbl:          fkey.ForeignTbl,
				ForeignCols:         make([]uint64, len(fkey.ForeignCols)),
				OnDelete:            fkey.OnDelete,
				OnUpdate:            fkey.OnUpdate,
				ReferencedIndexName: fkey.ReferencedIndexName,
				OnDeleteOrigin:      fkey.OnDeleteOrigin,
				OnUpdateOrigin:      fkey.OnUpdateOrigin,
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

		//2. append the child table ID to each distinct parent's RefChildTbls.
		// A table can define several foreign keys to one parent. Updating that
		// parent once per foreign key repeatedly rewrites its whole constraint
		// definition, while one child-table ID is sufficient for the relation.
		parentRelations := make([]engine.Relation, 0, len(fkTables))
		hasSelfReference := false
		ignoreForeignKey, _ := c.proc.Ctx.Value(defines.IgnoreForeignKey{}).(bool)
		for i, fkTableName := range fkTables {
			fkDbName := fkDbs[i]
			if session != nil {
				if _, ok := session.GetTempTable(fkDbName, fkTableName); ok {
					return moerr.NewNotSupported(c.proc.Ctx, "foreign key references temporary table")
				}
			}
			fkey := qry.GetTableDef().Fkeys[i]
			if fkey.ForeignTbl == 0 {
				// A self-reference is represented by the sentinel table ID 0.
				hasSelfReference = true
				continue
			}
			if ignoreForeignKey {
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
			parentRelations = append(parentRelations, fkRelation)
		}

		if hasSelfReference {
			err = AddChildTblIdToParentTable(c.proc.Ctx, newRelation, 0)
			if err != nil {
				c.proc.Info(c.proc.Ctx, "createTable",
					zap.String("databaseName", c.db),
					zap.String("tableName", qry.GetTableDef().GetName()),
					zap.Error(err),
				)
				return err
			}
		}

		if err = addChildTableIDToDistinctParents(c.proc.Ctx, parentRelations, tblId); err != nil {
			c.proc.Info(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
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
				colNameToId[strings.ToLower(attr.Attr.Name)] = attr.Attr.ID
			}
		}
		//1.1 update the column id of the column names in this table.
		//2. update fk info in the child table.
		for _, info := range fkRefersToMe {
			//update foreignCols in fk
			newDef := &plan.ForeignKeyDef{
				Name:                info.Def.Name,
				Cols:                make([]uint64, len(info.Def.Cols)),
				ForeignTbl:          tblId,
				ForeignCols:         make([]uint64, len(info.Def.ForeignCols)),
				OnDelete:            info.Def.OnDelete,
				OnUpdate:            info.Def.OnUpdate,
				ReferencedIndexName: info.Def.ReferencedIndexName,
				OnDeleteOrigin:      info.Def.OnDeleteOrigin,
				OnUpdateOrigin:      info.Def.OnUpdateOrigin,
			}
			//child table column ids of the child table
			copy(newDef.Cols, info.Def.Cols)
			//parent table column ids of the parent table
			for j, colReferred := range info.ColsReferred.Cols {
				//colName -> new colId
				if id, has := colNameToId[colReferred]; has {
					newDef.ForeignCols[j] = id
				} else {
					err := moerr.NewInternalErrorf(c.proc.Ctx, "no column %s", colReferred)
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
	main, err := dbSource.Relation(c.proc.Ctx, tblName, nil)
	if err != nil {
		c.proc.Info(c.proc.Ctx, "createTable",
			zap.String("databaseName", c.db),
			zap.String("tableName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}

	if err := c.maybeInsertIcebergTableMapping(dbSource, main, qry); err != nil {
		c.proc.Error(c.proc.Ctx, "createTable iceberg mapping",
			zap.String("databaseName", dbName),
			zap.String("tableName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}
	if err := c.maybeInsertMongoDBTableMapping(dbSource, main, qry); err != nil {
		return err
	}

	var indexExtra *api.SchemaExtra
	for i, def := range qry.IndexTables {
		planCols = def.GetCols()
		exeCols = engine.PlanColsToExeCols(planCols)
		exeDefs, indexExtra, err = engine.PlanDefsToExeDefs(def)
		if err != nil {
			c.proc.Error(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}

		exists, err := dbSource.RelationExists(c.proc.Ctx, def.Name, nil)
		if err != nil {
			c.proc.Error(c.proc.Ctx, "check index relation exists failed",
				zap.String("databaseName", c.db),
				zap.String("tableName", def.GetName()),
				zap.Error(err),
			)
			return err
		}
		if exists {
			return moerr.NewTableAlreadyExists(c.proc.Ctx, def.Name)
		}

		def.TblId = extra.IndexTables[i]
		indexExtra.FeatureFlag |= features.IndexTable
		indexExtra.ParentTableID = main.GetTableID(c.proc.Ctx)

		if err := dbSource.Create(
			context.WithValue(c.proc.Ctx, defines.TableIDKey{}, def.TblId),
			def.Name,
			append(exeCols, exeDefs...),
		); err != nil {
			c.proc.Error(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}

		err = maybeCreateAutoIncrement(
			c.proc.Ctx,
			c.proc.GetService(),
			dbSource,
			def,
			c.proc.GetTxnOperator(),
			nil,
		)
		if err != nil {
			c.proc.Error(c.proc.Ctx, "create index table for maybeCreateAutoIncrement",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.String("index tableName", def.Name),
				zap.Error(err),
			)
			return err
		}

		var initSQL string
		switch def.TableType {
		case catalog.SystemSI_IVFFLAT_TblType_Metadata:
			initSQL = fmt.Sprintf("INSERT INTO `%s`.`%s` (`%s`, `%s`) VALUES('version', '0');",
				qry.Database,
				def.Name,
				catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
				catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
			)

		case catalog.SystemSI_IVFFLAT_TblType_Centroids:
			initSQL = fmt.Sprintf("INSERT INTO `%s`.`%s` (`%s`, `%s`, `%s`) VALUES(0,1,NULL);",
				qry.Database,
				def.Name,
				catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
				catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
				catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
			)
		}
		err = c.runSqlWithOptions(
			initSQL, executor.StatementOption{}.WithDisableLog(),
		)
		if err != nil {
			c.proc.Error(c.proc.Ctx, "create index table for execute initSQL",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.String("index tableName", def.Name),
				zap.String("initSQL", initSQL),
				zap.Error(err),
			)
			return err
		}
	}

	if checkIndexInitializable(dbName, tblName) {
		newRelation, err := dbSource.Relation(c.proc.Ctx, tblName, nil)
		if err != nil {
			c.proc.Error(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
		err = s.checkTableWithValidIndexes(c, newRelation)
		if err != nil {
			c.proc.Error(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}

		insertSQL, err := makeInsertMultiIndexSQL(c.e, c.proc.Ctx, c.proc, dbSource, newRelation)
		if err != nil {
			c.proc.Error(c.proc.Ctx, "createTable",
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}
		err = c.runSqlWithOptions(
			insertSQL, executor.StatementOption{}.WithDisableLog(),
		)
		if err != nil {
			c.proc.Error(c.proc.Ctx, "createTable",
				zap.String("insertSQL", insertSQL),
				zap.String("dbName0", dbName),
				zap.String("tblName0", tblName),
				zap.String("databaseName", c.db),
				zap.String("tableName", qry.GetTableDef().GetName()),
				zap.Error(err),
			)
			return err
		}

		// create iscp jobs for index async update
		ct, err := GetConstraintDef(c.proc.Ctx, newRelation)
		if err != nil {
			return err
		}
		for _, constraint := range ct.Cts {
			if idxdef, ok := constraint.(*engine.IndexDef); ok && len(idxdef.Indexes) > 0 {
				tableID := newRelation.GetTableID(c.proc.Ctx)
				err = CreateAllIndexCdcTasks(c, idxdef.Indexes, dbName, tblName, tableID, false, qry.GetTableDef())
				if err != nil {
					return err
				}

				// register index update for IVFFLAT
				err = CreateAllIndexUpdateTasks(c, idxdef.Indexes, dbName, tblName, tableID)
				if err != nil {
					return err
				}
			}
		}

	}

	if c.keepAutoIncrement == 0 {
		err = maybeCreateAutoIncrement(
			c.proc.Ctx,
			c.proc.GetService(),
			dbSource,
			qry.GetTableDef(),
			c.proc.GetTxnOperator(),
			nil,
		)
	} else {
		err = maybeResetAutoIncrement(
			c.proc.Ctx,
			c.proc.GetService(),
			dbSource,
			qry.GetTableDef().GetName(),
			c.keepAutoIncrement,
			main.GetTableID(c.proc.Ctx),
			true,
			c.proc.GetTxnOperator(),
		)
	}

	if err != nil {
		c.proc.Error(c.proc.Ctx, "create table for maybeCreateAutoIncrement",
			zap.String("databaseName", c.db),
			zap.String("tableName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}

	ps := c.proc.GetPartitionService()
	if ps.Enabled() && features.IsPartitioned(qry.TableDef.FeatureFlag) {
		// cannot has err.
		stmt, _ := parsers.ParseOne(
			c.proc.Ctx,
			dialect.MYSQL,
			qry.RawSQL,
			c.getLower(),
		)

		err = ps.Create(
			c.proc.Ctx,
			qry.TableDef.TblId,
			stmt.(*tree.CreateTable),
			c.proc.GetTxnOperator(),
		)
		if err != nil {
			return err
		}

		if err = shardservice.GetService(c.proc.GetService()).Create(
			c.proc.Ctx,
			qry.GetTableDef().TblId,
			c.proc.GetTxnOperator(),
		); err != nil {
			return err
		}
	}

	if createAsSelectSql := qry.GetCreateAsSelectSql(); createAsSelectSql != "" {
		if isTemp {
			aliasTable := fmt.Sprintf("`%s`.`%s`", dbName, aliasName)
			realTable := fmt.Sprintf("`%s`.`%s`", dbName, tblName)
			createAsSelectSql = strings.Replace(createAsSelectSql, aliasTable, realTable, 1)
		}
		// Mark current txn as DDL before compiling CTAS follow-up INSERT ... SELECT,
		// so internal SQL stays on one CN and can see uncommitted table metadata.
		c.setHaveDDL(true)
		statementOption := executor.StatementOption{}.WithDisableLog()
		numericPrefixPlan := false
		numericPrefixPositions := make(map[int]bool)
		if c.pn.GetDdl().GetQuery() != nil {
			scanErr := plan.VisitExpressionsInOwner(c.pn.GetDdl().GetQuery(), func(expr *plan.Expr) error {
				return plan.VisitExprTree(expr, func(candidate *plan.Expr) error {
					fn := candidate.GetF()
					if fn == nil || fn.Func.GetObjName() != "cast" || candidate.Typ.Charset != 255 {
						return nil
					}
					numericPrefixPlan = true
					return plan.VisitExprTree(candidate, func(source *plan.Expr) error {
						if param := source.GetP(); param != nil && param.Pos >= 0 {
							numericPrefixPositions[int(param.Pos)] = true
						}
						return nil
					})
				})
			})
			if scanErr != nil {
				return scanErr
			}
		}
		if !numericPrefixPlan {
			clear(numericPrefixPositions)
		}
		if params := c.proc.GetPrepareParams(); c.pn.IsPrepare && params != nil && params.Length() > 0 {
			values := make([]string, params.Length())
			nulls := make([]bool, params.Length())
			for i := range values {
				nulls[i] = params.IsNull(uint64(i))
				if !nulls[i] {
					values[i] = string(params.GetRawBytesAt(i))
					if numericPrefixPositions[i] {
						if prefix, ok := function.GetNumericStringPrefix(values[i]); ok {
							values[i] = prefix
						}
					}
				}
			}
			statementOption = statementOption.WithParamsAndNulls(values, nulls)
		}
		res, err := func() (executor.Result, error) {
			oldCtx := c.proc.Ctx
			// CTAS follow-up SQL needs frontend session for temp-table alias resolution.
			ctxWithSession := attachInternalExecutorSession(c.proc.Ctx, c.proc.GetSession())
			if helper := c.proc.GetSessionInfo().SqlHelper; helper != nil {
				if compilerContext, ok := helper.GetCompilerContext().(plan2.CompilerContext); ok {
					ctxWithSession = attachInternalExecutorCompilerContext(ctxWithSession, compilerContext)
				}
			}
			// Force privilege checking for CTAS follow-up INSERT ... SELECT.
			// Internal executor skips auth by default unless this flag is present.
			c.proc.Ctx = attachInternalExecutorPrivilegeCheck(ctxWithSession)
			defer func() {
				c.proc.Ctx = oldCtx
			}()
			return c.runSqlWithResultAndOptions(
				createAsSelectSql,
				NoAccountId,
				statementOption,
			)
		}()
		if err != nil {
			return err
		}
		c.addAffectedRows(res.AffectedRows)
		res.Close()
	}

	if isTemp && session != nil {
		// The temporary table and all follow-up metadata/index/CTAS work have
		// completed. Keep the alias registered in the session.
		rollbackTempAlias = false
	}
	if !isTemp && !c.ignorePublish && c.proc.Base.IsFrontend {
		if err = c.refreshViewsAfterRelationMutation(dbName, tblName, 0, 0); err != nil {
			return err
		}
	}
	return nil
}

func physicalTemporaryTableName(proc *process.Process, dbName, alias string) string {
	return defines.GenTempTableName(proc.Base.SessionInfo.SessionId, dbName, alias)
}

func (c *Compile) maybeInsertIcebergTableMapping(dbSource engine.Database, rel engine.Relation, qry *plan.CreateTable) error {
	createSQL := icebergCreateSQLFromPlanTableDef(qry.GetTableDef())
	env, found, err := sqliceberg.ParseCreateSQLEnvelope(c.proc.Ctx, createSQL)
	if err != nil || !found {
		return err
	}

	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	dbIDText := dbSource.GetDatabaseId(c.proc.Ctx)
	dbID, err := strconv.ParseUint(dbIDText, 10, 64)
	if err != nil || dbID == 0 {
		return moerr.NewInternalErrorf(c.proc.Ctx, "invalid database id for iceberg mapping: %s", dbIDText)
	}
	// Keep the catalog row locked in the outer CREATE TABLE transaction until
	// the mapping is inserted.  DROP ICEBERG CATALOG uses this same row as its
	// lifecycle lock, so it cannot delete a catalog between this validation and
	// publication of a new mapping.
	catalogID, err := c.lookupIcebergCatalogIDForUpdate(accountID, env.Catalog)
	if err != nil {
		return err
	}
	// This is inert unless an operator explicitly enables the named fault point.
	// The E2E race test uses it to pause CREATE after the lifecycle lock is held
	// without adding file-system polling or an environment-controlled wait to DDL.
	fault.TriggerFaultWithContext(c.proc.Ctx, icebergCreateMappingAfterCatalogLockFault)

	mapping := model.TableMapping{
		AccountID:            accountID,
		DatabaseID:           dbID,
		TableID:              rel.GetTableID(c.proc.Ctx),
		CatalogID:            catalogID,
		Namespace:            env.Namespace,
		TableName:            env.Table,
		DefaultRef:           env.DefaultRef,
		ReadMode:             env.ReadMode,
		WriteMode:            env.WriteMode,
		WriterOwnerAccountID: accountID,
	}
	return c.runSqlWithOptions(
		sqliceberg.InsertTableMappingSQL(mapping),
		executor.StatementOption{}.WithDisableLog(),
	)
}

const icebergCreateMappingAfterCatalogLockFault = "iceberg-create-mapping-after-catalog-lock"

func (c *Compile) lookupIcebergCatalogIDForUpdate(accountID uint32, catalogName string) (uint64, error) {
	res, err := c.runSqlWithResultAndOptions(
		sqliceberg.GetCatalogByNameForUpdateSQL(accountID, catalogName),
		NoAccountId,
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	var catalogID uint64
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 {
			return true
		}
		ids := executor.GetFixedRows[uint64](cols[1])
		if len(ids) > 0 {
			catalogID = ids[0]
		}
		return false
	})
	if catalogID == 0 {
		return 0, moerr.NewInvalidInputf(c.proc.Ctx, "iceberg catalog %s does not exist", catalogName)
	}
	return catalogID, nil
}

func (c *Compile) maybeDeleteIcebergTableMapping(dbSource engine.Database, rel engine.Relation, tableDef *plan.TableDef) error {
	createSQL := icebergCreateSQLFromPlanTableDef(tableDef)
	_, found, err := sqliceberg.ParseCreateSQLEnvelope(c.proc.Ctx, createSQL)
	if err != nil || !found {
		return err
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	dbIDText := dbSource.GetDatabaseId(c.proc.Ctx)
	dbID, err := strconv.ParseUint(dbIDText, 10, 64)
	if err != nil || dbID == 0 {
		return moerr.NewInternalErrorf(c.proc.Ctx, "invalid database id for iceberg mapping delete: %s", dbIDText)
	}
	return c.runSqlWithOptions(
		sqliceberg.DeleteTableMappingSQL(accountID, dbID, rel.GetTableID(c.proc.Ctx)),
		executor.StatementOption{}.WithDisableLog(),
	)
}

func icebergCreateSQLFromPlanTableDef(tableDef *plan.TableDef) string {
	if tableDef == nil {
		return ""
	}
	if tableDef.Createsql != "" {
		return tableDef.Createsql
	}
	for _, def := range tableDef.Defs {
		properties := def.GetProperties()
		if properties == nil {
			continue
		}
		for _, property := range properties.Properties {
			if property.Key == catalog.SystemRelAttr_CreateSQL {
				return property.Value
			}
		}
	}
	return ""
}

func (c *Compile) maybeInsertMongoDBTableMapping(dbSource engine.Database, rel engine.Relation, qry *plan.CreateTable) error {
	if qry == nil || qry.GetTableDef() == nil || !features.IsMongoDBExternal(qry.GetTableDef().FeatureFlag) {
		return nil
	}
	createSQL := icebergCreateSQLFromPlanTableDef(qry.GetTableDef())
	env, found, err := sqlmongodb.ParseCreateSQLEnvelope(c.proc.Ctx, createSQL)
	if err != nil {
		return err
	}
	if !found {
		return moerr.NewInternalError(c.proc.Ctx, "typed MongoDB table plan is missing its catalog envelope")
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	dbIDText := dbSource.GetDatabaseId(c.proc.Ctx)
	dbID, err := strconv.ParseUint(dbIDText, 10, 64)
	if err != nil || dbID == 0 {
		return moerr.NewInternalErrorf(c.proc.Ctx, "invalid database id for MongoDB mapping: %s", dbIDText)
	}
	connectionID, err := c.lookupMongoDBConnectionID(accountID, env.Connection)
	if err != nil {
		return err
	}
	mapping := sqlmongodb.TableMapping{
		AccountID: accountID, DatabaseID: dbID, TableID: rel.GetTableID(c.proc.Ctx),
		ConnectionID: connectionID, Connection: env.Connection,
		Database: env.Database, Collection: env.Collection,
		SchemaMode: env.SchemaMode, Conversion: env.ConversionMode,
		SplitKey: env.SplitKey, MaxParallelism: env.MaxParallelism,
		Columns: env.Columns, Version: 1,
	}
	return c.runSqlWithOptions(
		sqlmongodb.InsertTableMappingSQL(mapping),
		executor.StatementOption{}.WithDisableLog(),
	)
}

func (c *Compile) lookupMongoDBConnectionID(accountID uint32, name string) (uint64, error) {
	res, err := c.runSqlWithResultAndOptions(
		sqlmongodb.GetConnectionByNameForUpdateSQL(accountID, name),
		NoAccountId,
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()
	var connectionID uint64
	var disabled bool
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 || len(cols) < 18 {
			return true
		}
		ids := executor.GetFixedRows[uint64](cols[1])
		disabledValues := executor.GetFixedRows[uint64](cols[17])
		if len(ids) > 0 {
			connectionID = ids[0]
		}
		if len(disabledValues) > 0 {
			disabled = disabledValues[0] != 0
		}
		return false
	})
	if connectionID == 0 || disabled {
		return 0, moerr.NewInvalidInputf(c.proc.Ctx, "MongoDB connection %s does not exist or is disabled", name)
	}
	return connectionID, nil
}

func (c *Compile) maybeDeleteMongoDBTableMapping(dbSource engine.Database, rel engine.Relation, tableDef *plan.TableDef) error {
	isMongoDB, err := plan2.IsMongoDBTableDef(c.proc.Ctx, tableDef)
	if err != nil || !isMongoDB {
		return err
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	dbIDText := dbSource.GetDatabaseId(c.proc.Ctx)
	dbID, err := strconv.ParseUint(dbIDText, 10, 64)
	if err != nil || dbID == 0 {
		return moerr.NewInternalErrorf(c.proc.Ctx, "invalid database id for MongoDB mapping delete: %s", dbIDText)
	}
	return c.runSqlWithOptions(
		sqlmongodb.DeleteTableMappingSQL(accountID, dbID, rel.GetTableID(c.proc.Ctx)),
		executor.StatementOption{}.WithDisableLog(),
	)
}

func (c *Compile) runSqlWithSystemTenant(sql string) error {
	oldCtx := c.proc.Ctx
	c.proc.Ctx = context.WithValue(oldCtx, defines.TenantIDKey{}, uint32(0))
	defer func() {
		c.proc.Ctx = oldCtx
	}()
	return c.runSqlWithOptions(
		sql, executor.StatementOption{}.WithDisableLog(),
	)
}

// reclaimBranchProtectSnapshots is the compile-layer entry point for branch
// protect snapshot reclaim. It loads mo_branch_metadata as sys, runs the
// shared reclaim core from the databranchutils package, and submits the
// resulting DELETE via a sys-tenant executor.
//
// Called synchronously by the plain `DROP TABLE` path. It reports whether the
// dropped table participates in branch lineage and acquires the DAG lock before
// flipping table_deleted=true for the affected tid (design §9.2 / §10).
func (c *Compile) reclaimBranchProtectSnapshots(deadTIDs []uint64) (bool, error) {
	if len(deadTIDs) == 0 {
		return false, nil
	}
	// Fast path: the vast majority of DROP TABLE operations are on tables
	// that have nothing to do with data branch. Skip the `FOR UPDATE`
	// reclaim scan entirely unless at least one of the dead tids actually
	// participates in mo_branch_metadata (as a child row or as a parent
	// referenced by some child). This keeps DROP TABLE fast and, more
	// importantly, avoids lock-contention with unrelated drops that
	// stacked up in `restore account` / `drop database` cascades.
	var idList strings.Builder
	for i, tid := range deadTIDs {
		if i > 0 {
			idList.WriteByte(',')
		}
		idList.WriteString(strconv.FormatUint(tid, 10))
	}
	probeSQL := fmt.Sprintf(
		"select 1 from %s.%s where table_id in (%s) or p_table_id in (%s) limit 1",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
		idList.String(), idList.String(),
	)
	probeRes, err := c.runSqlWithResult(probeSQL, int32(catalog.System_Account))
	if err != nil {
		return false, err
	}
	hasBranchRow := false
	probeRes.ReadRows(func(n int, _ []*vector.Vector) bool {
		if n > 0 {
			hasBranchRow = true
		}
		return true
	})
	probeRes.Close()
	if !hasBranchRow {
		return false, nil
	}

	loadDAG := func() (databranchutils.BranchReclaimDag, error) {
		// `FOR UPDATE` serialises sibling reclaim paths: two concurrent
		// drops from the same parent would otherwise each miss the
		// other's table_deleted flip and both skip the ancestor
		// reclaim, leaking the parent snapshot forever (review PR#24313
		// blocking issue #1). Only reached when the fast-path probe
		// above already confirmed the drop touches mo_branch_metadata,
		// so the lock scope is limited to real branch drops.
		querySql := fmt.Sprintf(
			"select table_id, p_table_id, clone_ts, level, table_deleted from %s.%s for update",
			catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
		)
		res, err := c.runSqlWithResult(querySql, int32(catalog.System_Account))
		if err != nil {
			return databranchutils.BranchReclaimDag{}, err
		}
		defer res.Close()
		var rows []databranchutils.DataBranchMetadata
		res.ReadRows(func(n int, cols []*vector.Vector) bool {
			if n == 0 {
				return true
			}
			tableIDs := vector.MustFixedColWithTypeCheck[uint64](cols[0])
			parentIDs := vector.MustFixedColWithTypeCheck[uint64](cols[1])
			cloneTSs := vector.MustFixedColWithTypeCheck[int64](cols[2])
			levels := executor.GetStringRows(cols[3])
			for i := 0; i < n; i++ {
				deleted := !cols[4].IsNull(uint64(i)) &&
					vector.GetFixedAtWithTypeCheck[bool](cols[4], i)
				rows = append(rows, databranchutils.DataBranchMetadata{
					TableID:      tableIDs[i],
					CloneTS:      cloneTSs[i],
					PTableID:     parentIDs[i],
					Level:        levels[i],
					TableDeleted: deleted,
				})
			}
			return true
		})
		return databranchutils.NewBranchReclaimDag(rows), nil
	}
	markDeleted := func() error {
		return c.runSqlWithSystemTenant(fmt.Sprintf(
			"update %s.%s set table_deleted = true where table_id in (%s)",
			catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, idList.String(),
		))
	}
	execDelete := func(snames []string) error {
		sql := databranchutils.BuildBranchSnapshotDeleteSQL(snames)
		return c.runSqlWithSystemTenant(sql)
	}
	return true, databranchutils.MarkAndReclaimBranchSnapshotsCore(
		deadTIDs, loadDAG, markDeleted, execDelete,
	)
}

// reclaimAndCompactBranchProtectSnapshots completes the branch part of a
// physical-table removal. Replacement paths publish their new lineage edge
// first, then use this same lifecycle transition as an ordinary DROP.
func (c *Compile) reclaimAndCompactBranchProtectSnapshots(deadTIDs []uint64) error {
	branchParticipates, err := c.reclaimBranchProtectSnapshots(deadTIDs)
	if err != nil || !branchParticipates {
		return err
	}
	return c.compactExpiredAlterDataBranchLineage(time.Time{})
}

func (s *Scope) CreateView(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	qry := s.Plan.GetDdl().GetCreateView()

	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
	exeCols := engine.PlanColsToExeCols(planCols)

	// convert the plan's defs to the execution's defs
	exeDefs, _, err := engine.PlanDefsToExeDefs(qry.GetTableDef())
	if err != nil {
		getLogger(s.Proc.GetService()).Info("createView",
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
	if err := lockMoDatabase(c, dbName, lock.LockMode_Shared); err != nil {
		return err
	}
	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if dbName == "" {
			return moerr.NewNoDB(c.proc.Ctx)
		}
		return convertDBEOB(c.proc.Ctx, err, dbName)
	}

	viewName := qry.GetTableDef().GetName()
	exists, err := dbSource.RelationExists(c.proc.Ctx, viewName, nil)
	if err != nil {
		getLogger(s.Proc.GetService()).Error("check view relation exists failed",
			zap.String("databaseName", c.db),
			zap.String("viewName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}
	var oldRelationID, oldLogicalID uint64
	if exists && qry.GetReplace() {
		oldRelation, relationErr := dbSource.Relation(c.proc.Ctx, viewName, nil)
		if relationErr != nil {
			return relationErr
		}
		oldRelationID = oldRelation.GetTableID(c.proc.Ctx)
		oldLogicalID = oldRelation.GetTableDef(c.proc.Ctx).GetLogicalId()
	}

	if exists {
		if qry.GetIfNotExists() {
			return nil
		}

		if qry.GetReplace() {
			if err = c.runSqlWithOptions(
				fmt.Sprintf("DROP VIEW IF EXISTS %s", quoteAlterCopyTableName(dbName, viewName)),
				executor.StatementOption{}.WithDisableLog(),
			); err != nil {
				getLogger(s.Proc.GetService()).Error("drop existing view failed",
					zap.String("databaseName", c.db),
					zap.String("viewName", qry.GetTableDef().GetName()),
					zap.Error(err),
				)
				return err
			}
		} else {
			return moerr.NewTableAlreadyExists(c.proc.Ctx, viewName)
		}
	}

	if err = lockMoTable(c, dbName, viewName, lock.LockMode_Exclusive); err != nil {
		getLogger(s.Proc.GetService()).Info("createView",
			zap.String("databaseName", c.db),
			zap.String("viewName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}

	if err = dbSource.Create(context.WithValue(c.proc.Ctx, defines.SqlKey{}, c.sql), viewName, append(exeCols, exeDefs...)); err != nil {
		getLogger(s.Proc.GetService()).Info("createView",
			zap.String("databaseName", c.db),
			zap.String("viewName", qry.GetTableDef().GetName()),
			zap.Error(err),
		)
		return err
	}
	if err = c.persistViewDependencies(dbSource, dbName, qry.GetTableDef()); err != nil {
		return err
	}
	if oldRelationID != 0 {
		return c.refreshViewsAfterRelationMutation(dbName, viewName, oldRelationID, oldLogicalID)
	}
	return nil
}

var checkIndexInitializable = func(dbName string, tblName string) bool {
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

func (s *Scope) CreateIndex(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	qry := s.Plan.GetDdl().GetCreateIndex()
	tempIndexNameMap := map[string]string{}
	var tempTableSession process.Session

	if qry.GetTableDef().GetIsTemporary() {
		session := c.proc.GetSession()
		if session == nil {
			return moerr.NewInternalError(c.proc.Ctx, "session not found for temporary table")
		}
		if realName, ok := session.GetTempTable(qry.Database, qry.Table); ok {
			qry.Table = realName
			qry.TableDef.Name = realName
		}
		indexNameMap := make(map[string]string, len(qry.Index.IndexTables))
		for _, def := range qry.Index.IndexTables {
			orig := def.Name
			if !defines.IsTempTableName(orig) {
				def.Name = defines.GenTempTableName(c.proc.Base.SessionInfo.SessionId, qry.Database, orig)
			}
			def.TableType = catalog.SystemTemporaryTable
			def.IsTemporary = true
			indexNameMap[orig] = def.Name
		}
		for _, idx := range qry.Index.TableDef.Indexes {
			if renamed, ok := indexNameMap[idx.IndexTableName]; ok {
				idx.IndexTableName = renamed
			}
		}
		tempIndexNameMap = indexNameMap
		tempTableSession = session
	}
	registerTempIndexAliases := func() {
		if tempTableSession == nil {
			return
		}
		for alias, realName := range tempIndexNameMap {
			// Register temp index aliases after DDL succeeds, so failed
			// CREATE INDEX does not leave stale session mappings.
			if indexSession, ok := tempTableSession.(interface {
				AddTempIndexTable(dbName, alias, realName string)
			}); ok {
				indexSession.AddTempIndexTable(qry.Database, alias, realName)
			} else {
				tempTableSession.AddTempTable(qry.Database, alias, realName)
			}
		}
	}
	{
		// lockMoTable will lock Table  mo_catalog.mo_tables
		// for the row with db_name=dbName & table_name = tblName。
		dbName := c.db
		if qry.GetDatabase() != "" {
			dbName = qry.GetDatabase()
		}
		tblName := qry.GetTableDef().GetName()
		if err := lockMoDatabase(c, dbName, lock.LockMode_Shared); err != nil {
			return convertDBEOBToNoSuchTable(c.proc.Ctx, err, dbName, tblName)
		}
		if err := lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); err != nil {
			return err
		}
	}

	dbSource, err := c.e.Database(c.proc.Ctx, qry.Database, c.proc.GetTxnOperator())
	if err != nil {
		return convertDBEOBToNoSuchTable(c.proc.Ctx, err, qry.Database, qry.Table)
	}

	r, err := dbSource.Relation(c.proc.Ctx, qry.Table, nil)
	if err != nil {
		return err
	}

	ps := c.proc.GetPartitionService()
	if !ps.Enabled() ||
		!features.IsPartitioned(r.GetExtraInfo().FeatureFlag) {
		if err := s.doCreateIndex(c, qry, dbSource, r); err != nil {
			return err
		}
		registerTempIndexAliases()
		return nil
	}

	metadata, err := ps.GetPartitionMetadata(
		c.proc.Ctx,
		r.GetTableID(c.proc.Ctx),
		c.proc.Base.TxnOperator,
	)
	if err != nil {
		return err
	}

	for _, p := range metadata.Partitions {
		r, err := dbSource.Relation(c.proc.Ctx, p.PartitionTableName, nil)
		if err != nil {
			return err
		}
		q := cloneCreateIndexForPartition(qry, p.PartitionTableName, p.Name, r.CopyTableDef(c.proc.Ctx))

		err = s.doCreateIndex(c, q, dbSource, r)
		if err != nil {
			return err
		}
	}
	registerTempIndexAliases()
	return nil
}

func (s *Scope) doCreateIndex(
	c *Compile,
	qry *plan.CreateIndex,
	dbSource engine.Database,
	r engine.Relation,
) error {
	var err error
	databaseId := dbSource.GetDatabaseId(c.proc.Ctx)
	tableId := r.GetTableID(c.proc.Ctx)
	tableDef := r.GetTableDef(c.proc.Ctx)
	extra := r.GetExtraInfo()

	originalTableDef := plan2.DeepCopyTableDef(qry.TableDef, true)
	indexInfo := qry.GetIndex() // IndexInfo is named same as planner's IndexInfo
	indexTableDef := indexInfo.GetTableDef()

	// In MySQL, the `CREATE INDEX` syntax can only create one index instance at a time
	// indexName -> meta      -> indexDef[0]
	//     		 -> centroids -> indexDef[1]
	//     		 -> entries   -> indexDef[2]
	multiTableIndexes := make(map[string]*MultiTableIndex)
	for _, indexDef := range indexTableDef.Indexes {
		indexAlgo := indexDef.IndexAlgo
		if indexDef.Unique {
			// 1. Unique Index related logic
			err = s.handleUniqueIndexTable(c, tableId, extra, dbSource, indexDef, qry.Database, originalTableDef, indexInfo)
		} else if !indexDef.Unique && catalog.IsRegularIndexAlgo(indexAlgo) {
			// 2. Regular Secondary index
			err = s.handleRegularSecondaryIndexTable(c, tableId, extra, dbSource, indexDef, qry.Database, originalTableDef, indexInfo)
		} else if !indexDef.Unique && catalog.IsMasterIndexAlgo(indexAlgo) {
			// 3. Master index
			err = s.handleMasterIndexTable(c, tableId, extra, dbSource, indexDef, qry.Database, originalTableDef, indexInfo)
		} else if !indexDef.Unique && indexplugin.IsPluginAlgo(indexAlgo) {
			// 4. Plugin-registered indexes (vector + fulltext) are
			// aggregated and handled later by the per-plugin
			// HandleCreateIndex hook.
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

	// cctx is loop-invariant — hoist to avoid per-index allocs.
	var cctx *pluginCompileCtx
	for _, multiTableIndex := range multiTableIndexes {
		// Plugin-mediated dispatch — every vector-index algorithm has a
		// registered plugin (HNSW, CAGRA, IVF-PQ, IVF-FLAT).
		//
		// Where the per-algo HandleCreateIndex body lives:
		//   pkg/vectorindex/hnsw/plugin/compile/
		//   pkg/vectorindex/cagra/plugin/compile/
		//   pkg/vectorindex/ivfpq/plugin/compile/
		//   pkg/vectorindex/ivfflat/plugin/compile/
		// Each plugin's runtime/ subdir holds the catalog hooks
		// (HiddenTableTypes, ParamsFromTree, SyncDescriptor, ...).
		if p, ok := indexplugin.Get(multiTableIndex.IndexAlgo); ok {
			if cctx == nil {
				cctx = newPluginCompileCtx(s, c, tableId, extra, dbSource, qry.Database, originalTableDef, indexInfo)
			}
			err = p.Compile().HandleCreateIndex(cctx, multiTableIndex.IndexDefs)
		}

		if err != nil {
			return err
		}
	}

	// build and update constraint def (no need to handle IVF related logic here)
	defs, _, err := engine.PlanDefsToExeDefs(indexTableDef)
	if err != nil {
		return err
	}

	var ok bool
	var ct *engine.ConstraintDef
	for _, def := range defs {
		ct, ok = def.(*engine.ConstraintDef)
		if ok {
			break
		}
	}

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
		sql, err := makeInsertSingleIndexSQL(c.e, c.proc, databaseId, tableId, indexDef, tableDef)
		if err != nil {
			return err
		}
		err = c.runSqlWithOptions(
			sql, executor.StatementOption{}.WithDisableLog(),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// indexTableBuild is used to build the index table corresponding to the index
// It converts the column definitions and execution definitions into plan, and then create the table in target database.
func indexTableBuild(
	c *Compile,
	mainTableID uint64,
	mainExtra *api.SchemaExtra,
	def *plan.TableDef,
	dbSource engine.Database,
) error {
	planCols := def.GetCols()
	exeCols := engine.PlanColsToExeCols(planCols)
	exeDefs, extra, err := engine.PlanDefsToExeDefs(def)
	if err != nil {
		c.proc.Info(c.proc.Ctx, "createTable",
			zap.String("databaseName", c.db),
			zap.String("tableName", def.GetName()),
			zap.Error(err),
		)
		return err
	}

	exists, err := dbSource.RelationExists(c.proc.Ctx, def.Name, nil)
	if err != nil {
		c.proc.Error(c.proc.Ctx, "check index relation exists failed",
			zap.String("databaseName", c.db),
			zap.String("tableName", def.GetName()),
			zap.Error(err),
		)
		return err
	}
	if exists {
		return moerr.NewTableAlreadyExists(c.proc.Ctx, def.Name)
	}

	extra.FeatureFlag |= features.IndexTable
	extra.ParentTableID = mainTableID
	if err = dbSource.Create(c.proc.Ctx, def.Name, append(exeCols, exeDefs...)); err != nil {
		c.proc.Info(c.proc.Ctx, "createTable",
			zap.String("databaseName", c.db),
			zap.String("tableName", def.GetName()),
			zap.Error(err),
		)
		return err
	}
	c.setHaveDDL(true)

	err = maybeCreateAutoIncrement(
		c.proc.Ctx,
		c.proc.GetService(),
		dbSource,
		def,
		c.proc.GetTxnOperator(),
		nil,
	)
	mainExtra.IndexTables = append(mainExtra.IndexTables, def.TblId)
	return err
}

func (s *Scope) DropIndex(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	qry := s.Plan.GetDdl().GetDropIndex()
	if qry.GetIndexName() == "" {
		// Planner uses an empty index name for DROP INDEX IF EXISTS no-op plans.
		return nil
	}

	// resolve temporary table real name if needed
	if session := c.proc.GetSession(); session != nil {
		if realName, ok := session.GetTempTable(qry.Database, qry.Table); ok {
			qry.Table = realName
		}
	}

	if err := lockMoDatabase(c, qry.Database, lock.LockMode_Shared); err != nil {
		return err
	}
	d, err := c.e.Database(c.proc.Ctx, qry.Database, c.proc.GetTxnOperator())
	if err != nil {
		return convertDBEOBToNoSuchTable(c.proc.Ctx, err, qry.Database, qry.Table)
	}
	r, err := d.Relation(c.proc.Ctx, qry.Table, nil)
	if err != nil {
		return err
	}

	// old tabledef
	oldTableDef := r.GetTableDef(c.proc.Ctx)

	//1. build and update constraint def
	oldCt, err := GetConstraintDef(c.proc.Ctx, r)
	if err != nil {
		return err
	}
	newCt, dropIndexTableNames, err := makeNewDropConstraint(oldCt, qry.GetIndexName())
	if err != nil {
		return err
	}
	err = DropIndexCdcTask(c, oldTableDef, qry.Database, qry.Table, qry.IndexName)
	if err != nil {
		return err
	}
	err = DrainIndexCdcTaskConsumer(c, oldTableDef, qry.Database, qry.Table, qry.IndexName)
	if err != nil {
		return err
	}
	err = r.UpdateConstraint(c.proc.Ctx, newCt)
	if err != nil {
		return err
	}

	//2. drop index table
	for _, indexTableName := range dropIndexTableNames {
		if err = c.dropIndexChildRelation(
			d, indexTableName, oldTableDef.GetIsTemporary(),
		); err != nil {
			return err
		}
	}

	// 3. unregister index update
	err = idxcron.UnregisterUpdate(c.proc.Ctx,
		c.proc.GetService(),
		c.proc.GetTxnOperator(),
		oldTableDef.TblId,
		qry.IndexName,
		idxcron.Action_Wildcard)
	if err != nil {
		return err
	}

	// 4. delete index object from mo_catalog.mo_indexes
	deleteSql := fmt.Sprintf(deleteMoIndexesWithTableIdAndIndexNameFormat, r.GetTableID(c.proc.Ctx), qry.IndexName)
	err = c.runSqlWithOptions(
		deleteSql, executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		return err
	}

	//6. Plugin-mediated drop hook — mirrors the HandleCreateIndex dispatch in
	// CreateIndex. Vector-index plugins use it to evict their in-process search
	// cache for the dropped index, so GPU/host resources are freed NOW instead of
	// lingering until the 5-min VectorIndexCacheTTL housekeeping. Without this the
	// hook (pkg/vectorindex/*/plugin/compile HandleDropIndex) was never invoked.
	dispatchPluginDropIndexes(s, c, d, qry.Database, qry.Table, oldTableDef, qry.IndexName)

	return nil
}

// dispatchPluginDropIndexes invokes each index plugin's HandleDropIndex for the
// plugin indexes on tableDef, so plugin-owned in-process state (today: the
// vector-index search cache, holding host + GPU memory) is released at DROP
// time rather than at the 5-min VectorIndexCacheTTL sweep.
//
// indexName != "" restricts the dispatch to that one index (DROP INDEX);
// indexName == "" covers every plugin index on the table (DROP TABLE — and
// therefore DROP DATABASE, which drops its tables one at a time).
//
// Best-effort by design: eviction failures are logged, never returned, because
// the TTL sweep is the backstop and a cache miss only costs a reload.
func dispatchPluginDropIndexes(
	s *Scope,
	c *Compile,
	d engine.Database,
	dbName string,
	tblName string,
	tableDef *plan.TableDef,
	indexName string,
) {
	if tableDef == nil {
		return
	}

	// Group by (algo, index name), not by algo alone: HandleDropIndex takes ONE
	// index's defs keyed by algo table type, so two indexes of the same algo on
	// the same table (possible under DROP TABLE) must not share a
	// MultiTableIndex — the second would overwrite the first's defs and only one
	// cache entry would be evicted. order[] keeps the dispatch deterministic.
	groups := make(map[string]*MultiTableIndex)
	order := make([]string, 0, len(tableDef.Indexes))
	for _, idef := range tableDef.Indexes {
		if idef.Unique || !indexplugin.IsPluginAlgo(idef.IndexAlgo) {
			continue
		}
		if indexName != "" && idef.IndexName != indexName {
			continue
		}
		algo := catalog.ToLower(idef.IndexAlgo)
		key := algo + "." + idef.IndexName
		mti, ok := groups[key]
		if !ok {
			mti = &MultiTableIndex{IndexAlgo: algo, IndexDefs: make(map[string]*plan.IndexDef)}
			groups[key] = mti
			order = append(order, key)
		}
		mti.IndexDefs[catalog.ToLower(idef.IndexAlgoTableType)] = idef
	}
	if len(groups) == 0 {
		return
	}

	dctx := newPluginCompileCtx(s, c, tableDef.TblId, nil, d, dbName, tableDef, nil)
	for _, key := range order {
		mti := groups[key]
		p, ok := indexplugin.Get(mti.IndexAlgo)
		if !ok {
			continue
		}
		if e := p.Compile().HandleDropIndex(dctx, mti.IndexDefs); e != nil {
			logutil.Warnf("[plugin] %s HandleDropIndex %s.%s/%s: %v",
				mti.IndexAlgo, dbName, tblName, key, e)
		}
	}
}

func makeNewDropConstraint(oldCt *engine.ConstraintDef, dropName string) (*engine.ConstraintDef, []string, error) {
	dropIndexTables := []hiddenIndexTableDrop{}
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
				if index.IndexName == dropName && len(index.IndexTableName) > 0 {
					dropIndexTables = append(dropIndexTables, hiddenIndexTableDrop{
						name:     index.IndexTableName,
						priority: hiddenIndexTableDropPriority(index),
					})
				}
				return index.IndexName == dropName
			}
			def.Indexes = plan2.RemoveIf[*plan.IndexDef](def.Indexes, pred)
			oldCt.Cts[i] = def
		}
	}
	sort.SliceStable(dropIndexTables, func(i, j int) bool {
		return dropIndexTables[i].priority > dropIndexTables[j].priority
	})
	dropIndexTableNames := make([]string, 0, len(dropIndexTables))
	for _, table := range dropIndexTables {
		dropIndexTableNames = append(dropIndexTableNames, table.name)
	}
	return oldCt, dropIndexTableNames, nil
}

type hiddenIndexTableDrop struct {
	name     string
	priority int
}

func hiddenIndexTableDropPriority(index *plan.IndexDef) int {
	if index == nil {
		return 0
	}
	p, ok := indexplugin.Get(index.IndexAlgo)
	if !ok || p.Compile() == nil {
		return 0
	}
	return p.Compile().HiddenTableDropPriority(catalog.ToLower(index.IndexAlgoTableType))
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
	addRefChildTableIDs(oldCt, []uint64{tblId})
	return fkRelation.UpdateConstraint(ctx, oldCt)
}

func addChildTableIDToDistinctParents(
	ctx context.Context,
	parentRelations []engine.Relation,
	childTableID uint64,
) error {
	updatedParents := make(map[uint64]struct{}, len(parentRelations))
	for _, parentRelation := range parentRelations {
		parentTableID := parentRelation.GetTableID(ctx)
		if _, alreadyUpdated := updatedParents[parentTableID]; alreadyUpdated {
			continue
		}
		if err := AddChildTblIdToParentTable(ctx, parentRelation, childTableID); err != nil {
			return err
		}
		updatedParents[parentTableID] = struct{}{}
	}
	return nil
}

func canonicalRefChildTableIDs(constraintDef *engine.ConstraintDef) []uint64 {
	var tableIDs []uint64
	seen := make(map[uint64]struct{})
	for _, constraint := range constraintDef.Cts {
		refChildDef, ok := constraint.(*engine.RefChildTableDef)
		if !ok {
			continue
		}
		for _, tableID := range refChildDef.Tables {
			if _, exists := seen[tableID]; exists {
				continue
			}
			seen[tableID] = struct{}{}
			tableIDs = append(tableIDs, tableID)
		}
	}
	return tableIDs
}

func setRefChildTableIDs(constraintDef *engine.ConstraintDef, tableIDs []uint64) {
	canonical := make([]uint64, 0, len(tableIDs))
	seen := make(map[uint64]struct{}, len(tableIDs))
	for _, tableID := range tableIDs {
		if _, exists := seen[tableID]; exists {
			continue
		}
		seen[tableID] = struct{}{}
		canonical = append(canonical, tableID)
	}
	constraintDef.Cts = plan2.RemoveIf(
		constraintDef.Cts,
		func(constraint engine.Constraint) bool {
			_, ok := constraint.(*engine.RefChildTableDef)
			return ok
		},
	)
	constraintDef.Cts = append(
		constraintDef.Cts,
		&engine.RefChildTableDef{Tables: canonical},
	)
}

func addRefChildTableIDs(constraintDef *engine.ConstraintDef, added []uint64) {
	tableIDs := canonicalRefChildTableIDs(constraintDef)
	seen := make(map[uint64]struct{}, len(tableIDs)+len(added))
	for _, tableID := range tableIDs {
		seen[tableID] = struct{}{}
	}
	for _, tableID := range added {
		if _, exists := seen[tableID]; exists {
			continue
		}
		seen[tableID] = struct{}{}
		tableIDs = append(tableIDs, tableID)
	}
	setRefChildTableIDs(constraintDef, tableIDs)
}

func removeRefChildTableID(constraintDef *engine.ConstraintDef, removed uint64) {
	tableIDs := canonicalRefChildTableIDs(constraintDef)
	tableIDs = plan2.RemoveIf(tableIDs, func(tableID uint64) bool {
		return tableID == removed
	})
	setRefChildTableIDs(constraintDef, tableIDs)
}

func replaceRefChildTableID(
	constraintDef *engine.ConstraintDef,
	oldTableID uint64,
	newTableID uint64,
) bool {
	tableIDs := canonicalRefChildTableIDs(constraintDef)
	replaced := false
	for i, tableID := range tableIDs {
		if tableID == oldTableID {
			tableIDs[i] = newTableID
			replaced = true
		}
	}
	setRefChildTableIDs(constraintDef, tableIDs)
	return replaced
}

func reconcileRefChildTableID(
	constraintDef *engine.ConstraintDef,
	oldTableID uint64,
	newTableID uint64,
) {
	if !replaceRefChildTableID(constraintDef, oldTableID, newTableID) {
		addRefChildTableIDs(constraintDef, []uint64{newTableID})
	}
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
	removeRefChildTableID(oldCt, tblId)
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

func (s *Scope) TruncateTable(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	truncate := s.Plan.GetDdl().GetTruncateTable()
	db := truncate.GetDatabase()
	table := truncate.GetTable()

	c.db = db

	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}

	dbSource, err := c.e.Database(c.proc.Ctx, db, c.proc.GetTxnOperator())
	if err != nil {
		return convertDBEOB(c.proc.Ctx, err, db)
	}

	rel, err := dbSource.Relation(c.proc.Ctx, table, nil)
	if err != nil {
		return err
	}
	oldID := rel.GetTableID(c.proc.Ctx)

	// Check if target table is a CCPR shared table (from publication)
	if c.shouldBlockCCPRReadOnly(rel.GetTableDef(c.proc.Ctx)) {
		return moerr.NewCCPRReadOnly(c.proc.Ctx)
	}

	if rel.GetTableDef(c.proc.Ctx).TableType == catalog.SystemExternalRel {
		return nil
	}

	if c.proc.GetTxnOperator().Txn().IsPessimistic() {
		var err error
		if e := lockMoTable(c, db, table, lock.LockMode_Exclusive); e != nil {
			if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return e
			}
			err = e
		}
		// before dropping table, lock it.
		if e := lockTable(c.proc.Ctx, c.e, c.proc, rel, db, false); e != nil {
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

	// TRUNCATE is a copy-and-swap rebuild: it creates a replacement relation
	// before the ordinary DROP path retires the old physical generation. Reuse
	// ALTER's lineage publication protocol so the replacement remains the live
	// branch child and the old generation stays protected for DIFF/MERGE.
	lineagePlan := alterDataBranchLineagePlan{}
	lineageTxnOp := c.proc.GetTxnOperator()
	lineageSnapshotAdvanced := false
	lineageCloneTS := int64(0)
	lineageOriginalSnapshot := timestamp.Timestamp{}
	lineageRestoreSnapshot := false
	defer func() {
		if lineageRestoreSnapshot {
			lineageTxnOp.SetSnapshotTS(lineageOriginalSnapshot)
		}
	}()
	if shouldAdvanceAlterDataBranchLineageSnapshot(
		lineageTxnOp.Txn().IsPessimistic(), lineageTxnOp.Txn().IsRCIsolation(),
	) {
		lineageOriginalSnapshot = lineageTxnOp.SnapshotTS()
		lineageRestoreSnapshot = true
		if lineageCloneTS, err = c.advanceAlterDataBranchLineageSnapshot(); err != nil {
			return err
		}
		lineageSnapshotAdvanced = true
	}
	if err = c.lockDataBranchLineageOwnerLifecycle(); err != nil {
		return err
	}
	if lineagePlan, err = c.prepareAlterDataBranchLineage(oldID, db, table, "TRUNCATE"); err != nil {
		return err
	}
	if !lineagePlan.enabled {
		var hasLatestHistory bool
		if hasLatestHistory, err = c.alterTableHasLatestHistoricalBranchSource(oldID, db, table); err != nil {
			return err
		}
		if hasLatestHistory {
			lineagePlan.enabled = true
			lineagePlan.preserveHistoricalSource = true
		}
	}
	if lineagePlan.enabled {
		if lineageSnapshotAdvanced {
			lineagePlan.cloneTS = lineageCloneTS
		} else {
			lineagePlan.cloneTS = lineageTxnOp.SnapshotTS().PhysicalTime
		}
	}
	if lineageSnapshotAdvanced {
		rel, err = dbSource.Relation(c.proc.Ctx, table, nil)
		if err != nil {
			return err
		}
		if rel.GetTableID(c.proc.Ctx) != oldID {
			return moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
		}
	}

	// delete from tables => truncate, need keep increment value
	// Get logicalId from tableDef and pass it when creating the new table
	tableDef := rel.GetTableDef(c.proc.Ctx)
	oldLogicalId := tableDef.GetLogicalId()
	dropOpts := executor.StatementOption{}.WithIgnoreForeignKey().WithIgnorePublish().WithIgnoreCheckExperimental()
	createOpts := executor.StatementOption{}.WithIgnoreForeignKey().WithIgnorePublish().WithIgnoreCheckExperimental()
	if oldLogicalId != 0 {
		createOpts = createOpts.WithKeepLogicalId(oldLogicalId)
	}
	if truncate.IsDelete {
		rows, err := rel.Rows(c.proc.Ctx)
		if err != nil {
			return err
		}

		c.addAffectedRows(rows)
		dropOpts = dropOpts.WithDisableDropIncrStatement()
		createOpts = createOpts.WithKeepAutoIncrement(oldID)
	}
	if lineagePlan.enabled {
		// The successor edge is published after CREATE obtains its physical ID.
		// Keep the old edge until then; the shared reclaim below retires it.
		dropOpts = dropOpts.WithSkipDataBranchReclaim()
	}

	r, err := c.runSqlWithResultAndOptions(
		fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", db, table),
		int32(accountID),
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		return err
	}
	defer r.Close()

	createSQL := ""
	r.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			createSQL = executor.GetStringRows(cols[1])[0]
			return true
		},
	)

	// drop table
	if err = c.runSqlWithAccountIdAndOptions(
		fmt.Sprintf("drop table `%s`.`%s`", db, table),
		int32(accountID),
		dropOpts,
	); err != nil {
		return err
	}

	// create table
	if err = c.runSqlWithAccountIdAndOptions(
		createSQL,
		int32(accountID),
		createOpts,
	); err != nil {
		return err
	}

	rel, err = dbSource.Relation(c.proc.Ctx, table, nil)
	if err != nil {
		return err
	}
	newID := rel.GetTableID(c.proc.Ctx)
	if lineagePlan.enabled {
		if err = c.preserveAlterDataBranchLineage(
			lineagePlan, oldID, newID, db, table,
		); err != nil {
			return err
		}
		if err = c.reclaimAndCompactBranchProtectSnapshots([]uint64{oldID}); err != nil {
			return err
		}
	}

	for _, ftblId := range truncate.ForeignTbl {
		_, _, fkRelation, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), ftblId)
		if err != nil {
			return err
		}
		oldCt, err := GetConstraintDef(c.proc.Ctx, fkRelation)
		if err != nil {
			return err
		}
		replaceRefChildTableID(oldCt, oldID, newID)
		err = fkRelation.UpdateConstraint(c.proc.Ctx, oldCt)
		if err != nil {
			return err
		}
	}

	return nil
}

func cloneCreateIndexForPartition(
	qry *plan.CreateIndex,
	partitionTableName string,
	partitionName string,
	tableDef *plan.TableDef,
) *plan.CreateIndex {
	dd := &plan.DataDefinition{
		DdlType: plan.DataDefinition_CREATE_INDEX,
		Definition: &plan.DataDefinition_CreateIndex{
			CreateIndex: qry,
		},
	}
	cloned := plan2.DeepCopyDataDefinition(dd).Definition.(*plan.DataDefinition_CreateIndex).CreateIndex
	cloned.Table = partitionTableName
	cloned.TableDef = tableDef
	for _, def := range cloned.Index.IndexTables {
		def.Name = fmt.Sprintf("%s_%s", def.Name, partitionName)
	}
	for _, def := range cloned.Index.TableDef.Indexes {
		def.IndexTableName = fmt.Sprintf("%s_%s", def.IndexTableName, partitionName)
	}
	return cloned
}

func (s *Scope) DropSequence(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	qry := s.Plan.GetDdl().GetDropSequence()
	dbName := qry.GetDatabase()
	var dbSource engine.Database
	var err error

	tblName := qry.GetTable()
	if err = lockMoDatabase(c, dbName, lock.LockMode_Shared); err != nil {
		return err
	}
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

	// Sequence grants resolve through mo_tables and therefore use the same
	// logical object identity as table/view grants.
	droppedObjectID := plan2.SnapshotTableID(rel.GetTableDef(c.proc.Ctx))

	// Delete the stored session value.
	c.proc.GetSessionInfo().SeqDeleteKeys = append(c.proc.GetSessionInfo().SeqDeleteKeys, rel.GetTableID(c.proc.Ctx))

	if err = dbSource.Delete(c.proc.Ctx, tblName); err != nil {
		return err
	}
	return c.deleteRolePrivilegesForDroppedRelation(droppedObjectID)
}

func (c *Compile) deleteRolePrivilegesForDroppedDatabase(accountID uint32, databaseID uint64) error {
	ignoreForeignKey, _ := c.proc.Ctx.Value(defines.IgnoreForeignKey{}).(bool)
	// DROP ACCOUNT sets this flag and removes the tenant's privilege catalog
	// before dropping its databases, so the outer lifecycle already owns cleanup.
	if ignoreForeignKey {
		return nil
	}
	if databaseID == 0 {
		return moerr.NewInternalError(c.proc.Ctx, "cannot clean role privileges for database ID 0")
	}
	return c.runSqlWithOptions(
		fmt.Sprintf(deleteMoRolePrivsWithDatabaseIdFormat, databaseID, accountID, databaseID),
		executor.StatementOption{}.WithDisableLog(),
	)
}

func (c *Compile) deleteRolePrivilegesForDroppedRelation(objectID uint64) error {
	ignoreForeignKey, _ := c.proc.Ctx.Value(defines.IgnoreForeignKey{}).(bool)
	// Database recursion and ALTER/TRUNCATE replacement set this flag. The
	// former is cleaned once by DropDatabase; the latter preserves logical ID.
	if ignoreForeignKey {
		return nil
	}
	if objectID == 0 {
		return moerr.NewInternalError(c.proc.Ctx, "cannot clean role privileges for relation ID 0")
	}
	return c.runSqlWithOptions(
		fmt.Sprintf(deleteMoRolePrivsWithObjectIdFormat, objectID),
		executor.StatementOption{}.WithDisableLog(),
	)
}

func (c *Compile) dropIndexChildRelation(
	database engine.Database,
	relationName string,
	isTemporary bool,
) error {
	var logicalID uint64
	// Hidden relations are owned by the parent table lifecycle and cannot be a
	// GRANT target. Do not add a second child metadata lock here: DML acquires
	// shared locks for all index write targets, and upgrading one child while
	// retaining another creates a cross-index wait cycle. We only need the child
	// identity to remove legacy privilege rows after its parent-owned deletion.
	relation, err := database.Relation(c.proc.Ctx, relationName, nil)
	if err != nil {
		return err
	}
	if !isTemporary {
		logicalID = plan2.SnapshotTableID(relation.GetTableDef(c.proc.Ctx))
	}
	if err = maybeDeleteAutoIncrement(
		c.proc.Ctx, c.proc.GetService(), database, relationName, c.proc.GetTxnOperator(),
	); err != nil {
		return err
	}
	if err = database.Delete(c.proc.Ctx, relationName); err != nil {
		return err
	}
	if logicalID != 0 {
		if err = c.deleteRolePrivilegesForDroppedRelation(logicalID); err != nil {
			return err
		}
	}
	if isTemporary {
		if session := c.proc.GetSession(); session != nil {
			session.RemoveTempTableByRealName(relationName)
		}
	}
	return nil
}

func lockDroppedRelation(
	c *Compile,
	dbName string,
	relationName string,
	relation engine.Relation,
	lockStorage bool,
) error {
	var retryErr error
	if err := lockMoTable(c, dbName, relationName, lock.LockMode_Exclusive); err != nil {
		if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
			!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
			return err
		}
		retryErr = err
	}
	// Every persistent relation, including a source, owns a mo_tables lifecycle
	// row. Only ordinary stored tables also have user-table storage to lock.
	if lockStorage {
		if err := lockTable(c.proc.Ctx, c.e, c.proc, relation, dbName, true); err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return err
			}
			retryErr = err
		}
	}
	return retryErr
}

func (s *Scope) DropTable(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	qry := s.Plan.GetDdl().GetDropTable()
	if len(qry.GetTables()) > 0 {
		for _, entry := range qry.GetTables() {
			sub := plan2.DeepCopyDropTable(entry)
			if sub == nil {
				continue
			}
			if err := s.dropTableSingle(c, sub); err != nil {
				return err
			}
		}
		return nil
	}
	return s.dropTableSingle(c, qry)
}

func (s *Scope) dropTableSingle(c *Compile, qry *plan.DropTable) error {
	dbName := qry.GetDatabase()
	tblName := qry.GetTable()
	if tblName == "" {
		return nil
	}
	isView := qry.GetIsView()
	var isSource = false
	if qry.TableDef != nil {
		isSource = qry.TableDef.TableType == catalog.SystemSourceRel
	}
	var dbSource engine.Database
	var rel engine.Relation
	var err error
	var isTemp bool
	var originTableName string

	if session := c.proc.GetSession(); session != nil {
		if real, ok := session.GetTempTable(dbName, tblName); ok {
			originTableName = tblName
			tblName = real
			qry.Table = real
			isTemp = true
		}
	}

	// A plan built for a temporary table must not delete a same-named
	// permanent table if the session alias disappears before execution.
	if qry.GetTableDef().GetIsTemporary() && !isTemp {
		if qry.GetIfExists() {
			return nil
		}
		return moerr.NewNoSuchTable(c.proc.Ctx, dbName, tblName)
	}

	if !isView && qry.TableDef == nil && !isTemp {
		if qry.IfExists {
			return nil
		}
	}
	if !isTemp {
		// A plain DROP TABLE updates PITR state and may reclaim branch-owner
		// rows. The gate must precede mo_database/mo_tables locks.
		if err = c.lockDataBranchLineageOwnerLifecycle(); err != nil {
			return err
		}
	}

	if !c.disableLock {
		if err := lockMoDatabase(c, dbName, lock.LockMode_Shared); err != nil {
			return err
		}
	}

	tblID := qry.GetTableId()
	dbSource, err = c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return convertDBEOBToNoSuchTable(c.proc.Ctx, err, dbName, tblName)
	}

	if rel, err = dbSource.Relation(c.proc.Ctx, tblName, nil); err != nil {
		if qry.GetIfExists() {
			return nil
		}
		return err
	}
	droppedRelationID := rel.GetTableID(c.proc.Ctx)
	droppedTableDef := rel.GetTableDef(c.proc.Ctx)
	droppedLogicalID := droppedTableDef.GetLogicalId()
	droppedObjectID := plan2.SnapshotTableID(droppedTableDef)
	droppedDatabaseID := droppedTableDef.GetDbId()
	if c.proc.Base.IsFrontend && !isTemp && !c.ignorePublish && !needSkipDbs[dbName] &&
		(!c.proc.GetSessionInfo().IsRestore || restoreInvalidatesViewMetadata(c.proc.Ctx)) {
		// Keep the global gate ahead of the target/source relation lock. This is
		// the same order used by recovery when it claims and regenerates a View.
		if err = lockViewMetadataLifecycleGate(c.proc); err != nil {
			return err
		}
	}

	// Check if the table is a CCPR shared table
	if !isTemp && !isView && !isSource {
		tableID := rel.GetTableID(c.proc.Ctx)
		canDrop, err := checkCCPRTableBeforeDrop(c, tableID)
		if err != nil {
			return err
		}
		if !canDrop {
			return moerr.NewCCPRReadOnly(c.proc.Ctx)
		}
	}

	if !c.disableLock &&
		!isTemp &&
		c.proc.GetTxnOperator().Txn().IsPessimistic() {
		if err = lockDroppedRelation(c, dbName, tblName, rel, !isView && !isSource); err != nil {
			return err
		}
	}

	// if dbSource is a pub, update tableList
	if !c.ignorePublish {
		if err = updatePubTableList(c.proc.Ctx, c, dbName, tblName); err != nil {
			return err
		}
	}

	if len(qry.UpdateFkSqls) > 0 {
		for _, sql := range qry.UpdateFkSqls {
			if err = c.runSqlWithOptions(
				sql, executor.StatementOption{}.WithDisableLog(),
			); err != nil {
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

		var fkRelation engine.Relation
		if _, _, fkRelation, err = c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), fkTblId); err != nil {
			// A has keys reference on B, we cannot find the B when drop A.
			// what's going on?
			//
			// if there has no mistake on the constraints when create A and B, we cannot
			// find B means B has dropped already, but how can we drop a table when there has
			// a table refer to it?
			// the FOREIGN_KEY_CHECKS disabled !!!
			// so this inexistence is expected, no need to return an error.
			if isMissingTableByIdForFkCleanup(err) {
				logutil.Warn(
					"cannot find the referred table when drop table",
					zap.String("table", fmt.Sprintf("%s-%s", dbName, tblName)),
					zap.Int("referred table id", int(fkTblId)),
					zap.String("txn info", c.proc.GetTxnOperator().Txn().DebugString()),
					zap.Error(err),
				)
				continue
			}
			return err

		} else {
			if err = s.removeChildTblIdFromParentTable(
				c, fkRelation, tblID,
			); err != nil {
				return err
			}
		}
	}

	//remove parent table id from the child table (when foreign_key_checks is disabled)
	for _, childTblId := range qry.FkChildTblsReferToMe {
		if childTblId == 0 {
			continue
		}
		_, _, childRelation, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), childTblId)
		if err != nil {
			if isMissingTableByIdForFkCleanup(err) {
				logutil.Warn(
					"cannot find child table when drop table fk cleanup",
					zap.String("table", fmt.Sprintf("%s-%s", dbName, tblName)),
					zap.Int("child table id", int(childTblId)),
					zap.String("txn info", c.proc.GetTxnOperator().Txn().DebugString()),
					zap.Error(err),
				)
				continue
			}
			return err
		}
		err = s.removeParentTblIdFromChildTable(c, childRelation, tblID)
		if err != nil {
			return err
		}
	}

	// delete cdc task of the vector and fulltext index here
	err = DropAllIndexCdcTasks(c, rel.GetTableDef(c.proc.Ctx), qry.Database, qry.Table)
	if err != nil {
		return err
	}

	// unregister index update by Table Id
	err = DropAllIndexUpdateTasks(c, rel.GetTableDef(c.proc.Ctx), qry.Database, qry.Table)
	if err != nil {
		return err
	}

	// Plugin-mediated drop hook for EVERY plugin index on the table — the
	// DROP TABLE counterpart of the DROP INDEX dispatch. DROP DATABASE reaches
	// this too, since it drops its tables one at a time. Without it a dropped
	// table's vector index stayed in the search cache (and kept its host + GPU
	// memory) until the 5-min VectorIndexCacheTTL sweep.
	dispatchPluginDropIndexes(s, c, dbSource, dbName, tblName, rel.GetTableDef(c.proc.Ctx), "")

	// delete all index objects record of the table in mo_catalog.mo_indexes
	if !qry.IsView && qry.Database != catalog.MO_CATALOG && qry.Table != catalog.MO_INDEXES {
		if qry.GetTableDef().Pkey != nil || len(qry.GetTableDef().Indexes) > 0 {
			deleteSql := fmt.Sprintf(deleteMoIndexesWithTableIdFormat, qry.GetTableDef().TblId)
			if err = c.runSqlWithOptions(
				deleteSql, executor.StatementOption{}.WithDisableLog(),
			); err != nil {
				return err
			}
		}
	}

	if err := c.maybeDeleteIcebergTableMapping(dbSource, rel, qry.GetTableDef()); err != nil {
		return err
	}
	if err := c.maybeDeleteMongoDBTableMapping(dbSource, rel, qry.GetTableDef()); err != nil {
		return err
	}

	if err := dbSource.Delete(c.proc.Ctx, tblName); err != nil {
		return err
	}
	if !isTemp && !needSkipDbs[dbName] {
		if err = c.deleteRolePrivilegesForDroppedRelation(droppedObjectID); err != nil {
			return err
		}
	}
	if !isTemp && !c.ignorePublish && c.proc.Base.IsFrontend {
		if err = c.enqueueViewsAfterRelationRemoval(
			dbName, tblName, droppedDatabaseID, droppedRelationID, droppedLogicalID); err != nil {
			return err
		}
		if isView {
			if err = c.deleteDroppedViewMetadata(dbName, droppedRelationID); err != nil {
				return err
			}
		}
	}
	// Try to remove temp table alias from session if it exists.
	// tblName is the real name here (because Binder resolved it using the temp name from session if it was an alias).
	// If tblName is not a temp table (e.g. regular table), this call will just do nothing (scan map and find no match).
	if sess := c.proc.GetSession(); sess != nil {
		if isTemp {
			sess.RemoveTempTable(dbName, originTableName)
		} else {
			sess.RemoveTempTableByRealName(tblName)
		}
	}

	for _, name := range qry.IndexTableNames {
		if err = c.dropIndexChildRelation(dbSource, name, isTemp); err != nil {
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
		if containAuto && !c.disableDropAutoIncrement {
			// When drop table 'mo_catalog.mo_indexes', there is no need to delete the auto increment data
			err := incrservice.GetAutoIncrementService(c.proc.GetService()).Delete(
				c.proc.Ctx,
				rel.GetTableID(c.proc.Ctx),
				c.proc.GetTxnOperator())
			if err != nil {
				return err
			}
		}

		if err := shardservice.GetService(c.proc.GetService()).Delete(
			c.proc.Ctx,
			rel.GetTableID(c.proc.Ctx),
			c.proc.GetTxnOperator(),
		); err != nil {
			return err
		}
	}

	if dbName == catalog.MO_CATALOG {
		return nil
	}

	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}

	if !needSkipDbs[dbName] {
		now := c.proc.GetTxnOperator().SnapshotTS().ToStdTime().UTC().UnixNano()
		updatePitrSql := fmt.Sprintf(
			"update `%s`.`%s` set `%s` = %d, `%s` = %d where `%s` = %d and `%s` = '%s' and `%s` = '%s' and `%s` = %d and `%s` = %d",
			catalog.MO_CATALOG, catalog.MO_PITR,
			catalog.MO_PITR_STATUS, 0,
			catalog.MO_PITR_CHANGED_TIME, now,

			catalog.MO_PITR_ACCOUNT_ID, accountID,
			catalog.MO_PITR_DB_NAME, sqlquote.EscapeString(dbName),
			catalog.MO_PITR_TABLE_NAME, sqlquote.EscapeString(tblName),
			catalog.MO_PITR_STATUS, 1,
			catalog.MO_PITR_OBJECT_ID, tblID,
		)

		err = c.runSqlWithSystemTenant(updatePitrSql)
		if err != nil {
			return err
		}
	}

	sqls := []string{
		fmt.Sprintf(
			"delete from mo_catalog.mo_merge_settings where account_id = %d and tid = %d",
			accountID, tblID,
		),
	}

	for _, ss := range sqls {
		if err = c.runSqlWithSystemTenant(ss); err != nil {
			logutil.Error("run extra sql failed when drop table",
				zap.String("sql", commonutil.Abbreviate(ss, 500)),
				zap.Error(err),
			)
			return err
		}
	}

	// Branch Protect Snapshot reclaim: lock the complete metadata DAG before
	// flipping table_deleted=true for this tid, then check whether any subtree
	// has become fully deleted and release the corresponding
	// `__mo_branch_*` snapshots. This must run synchronously so drop paths have
	// identical semantics in the frontend and compile-layer paths (design
	// §5.3 / §9.2).
	if !c.skipDataBranchReclaim {
		if err = c.reclaimAndCompactBranchProtectSnapshots([]uint64{tblID}); err != nil {
			logutil.Error("reclaim branch protect snapshots failed",
				zap.Uint64("tblID", tblID),
				zap.Error(err),
			)
			return err
		}
	}

	ps := partitionservice.GetService(c.proc.GetService())
	extr := rel.GetExtraInfo()
	if extr == nil ||
		!ps.Enabled() ||
		!features.IsPartitioned(extr.FeatureFlag) {
		return nil
	}

	return ps.Delete(
		c.proc.Ctx,
		tblID,
		c.proc.GetTxnOperator(),
	)
}

func (s *Scope) CreateSequence(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	qry := s.Plan.GetDdl().GetCreateSequence()
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
	exeCols := engine.PlanColsToExeCols(planCols)

	// convert the plan's defs to the execution's defs
	exeDefs, _, err := engine.PlanDefsToExeDefs(qry.GetTableDef())
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
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	var values []interface{}
	var curval string
	var oldLogicalID uint64
	qry := s.Plan.GetDdl().GetAlterSequence()
	// convert the plan's cols to the execution's cols
	planCols := qry.GetTableDef().GetCols()
	exeCols := engine.PlanColsToExeCols(planCols)

	// convert the plan's defs to the execution's defs
	exeDefs, _, err := engine.PlanDefsToExeDefs(qry.GetTableDef())
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
		// ALTER SEQUENCE replaces the physical relation but preserves the logical
		// privilege identity, like ALTER VIEW/TABLE replacement.
		oldLogicalID = plan2.SnapshotTableID(rel.GetTableDef(c.proc.Ctx))
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
		if err = c.runSqlWithOptions(
			fmt.Sprintf("DROP SEQUENCE %s", quoteAlterCopyTableName(dbName, tblName)),
			executor.StatementOption{}.WithDisableLog().WithIgnoreForeignKey(),
		); err != nil {
			return err
		}
	} else {
		// sequence table not exists
		if qry.GetIfExists() {
			return nil
		}
		return moerr.NewInternalErrorf(c.proc.Ctx, "sequence %s not exists", tblName)
	}

	if err := lockMoTable(c, dbName, tblName, lock.LockMode_Exclusive); err != nil {
		return err
	}

	createCtx := context.WithValue(c.proc.Ctx, defines.SqlKey{}, c.sql)
	createCtx = context.WithValue(createCtx, defines.LogicalIdKey{}, oldLogicalID)
	if err := dbSource.Create(createCtx, tblName, append(exeCols, exeDefs...)); err != nil {
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

func (s *Scope) TableClone(c *Compile) error {
	var (
		err     error
		created bool
	)

	clonePlan := s.Plan.GetDdl().GetCloneTable()

	if clonePlan.CreateTable != nil {
		s.Plan = clonePlan.CreateTable
		if err = s.createTable(c, func() { created = true }); err != nil {
			return err
		}
		if !created {
			return nil
		}
	}

	return s.RestoreTable(c, clonePlan)
}

// RestoreTable is the clone/restore-path twin of cloneUnaffectedIndexes
// (pkg/sql/compile/alter.go). CreateTable (already run by TableClone) seeds the
// index hidden tables and registers their CDC; the block-level clone in
// table_clone APPENDS onto those tables. So, in place of a bare s.Run(c):
//
//  1. drop the index CDC tasks before cloning data;
//  2. for each hidden table the plugin lists in DeleteBeforeClone (IVF-FLAT's
//     metadata/centroids/entries — seeded non-empty by CreateTable), empty the
//     seed with `DELETE … WHERE TRUE` (a content delete that keeps the table
//     and its id — NOT truncate, which re-creates the table);
//  3. s.Run(c): clone the main table + index hidden tables (append onto empty);
//  4. re-register each index's CDC startFromNow=true with a PLUGIN-PROVIDED
//     InitSQL. For a vector index that InitSQL is `ALTER … REINDEX … FORCE_SYNC`,
//     so the CDC's first iteration runs the reindex in its own post-commit txn —
//     rebuilding the model from the committed cloned rows and re-arming the CDC
//     at the post-clone watermark. Running it as InitSQL (not inline in this
//     clone txn) is what avoids the SnapshotTS replay that double-counts the
//     cloned rows.
func (s *Scope) RestoreTable(c *Compile, clonePlan *plan.CloneTable) error {
	tableDef := clonePlan.GetCreateTable().GetDdl().GetCreateTable().GetTableDef()
	if tableDef == nil {
		return s.Run(c)
	}
	dbName, tblName := clonePlan.GetDstDatabaseName(), clonePlan.GetDstTableName()
	if clonePlan.GetCreateTable().GetDdl().GetCreateTable().GetTemporary() {
		session := c.proc.GetSession()
		if session == nil {
			return moerr.NewInternalError(c.proc.Ctx, "session not found for temporary table clone")
		}
		physicalName, ok := session.GetTempTable(dbName, tblName)
		if !ok {
			return moerr.NewInternalErrorf(c.proc.Ctx,
				"temporary table clone destination %s.%s is not registered", dbName, tblName)
		}
		dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
		if err != nil {
			return err
		}
		dstRelation, err := dbSource.Relation(c.proc.Ctx, physicalName, nil)
		if err != nil {
			return err
		}
		tableDef = dstRelation.GetTableDef(c.proc.Ctx)
		tblName = physicalName
	}
	logutil.Infof("[RestoreTable] BEGIN %s.%s", dbName, tblName)

	// 1. drop the CDC tasks CreateTable registered, before cloning data.
	if err := DropAllIndexCdcTasks(c, tableDef, dbName, tblName); err != nil {
		return err
	}

	// 2. empty the seeded hidden tables the plugin wants delete-before-clone
	//    (IVF-FLAT) so the block-level clone appends onto empty tables.
	for _, idx := range tableDef.GetIndexes() {
		p, ok := indexplugin.Get(catalog.ToLower(idx.GetIndexAlgo()))
		if !ok {
			continue
		}
		if p.Catalog().RestoreBehavior().ContainsDeleteBeforeClone(idx.GetIndexAlgoTableType()) {
			// content delete (keeps the table + id); WHERE TRUE avoids truncate.
			sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE TRUE", dbName, idx.GetIndexTableName())
			logutil.Infof("[RestoreTable] empty seed: %s", sql)
			if err := c.runSql(sql); err != nil {
				return err
			}
		}
	}

	// 3. clone main table + index hidden tables (append onto empty).
	logutil.Infof("[RestoreTable] clone (s.Run): %s.%s", dbName, tblName)
	if err := s.Run(c); err != nil {
		return err
	}

	// 4. group plugin indexes, then re-register each index's CDC startFromNow=true
	//    with a plugin-provided InitSQL (vector: ALTER REINDEX … FORCE_SYNC;
	//    fulltext: "" — none). tblId comes from CreateTable's tableDef.
	multiTableIndexes := make(map[string]*MultiTableIndex)
	for _, idx := range tableDef.GetIndexes() {
		valid, err := checkValidIndexCdcByIndexdef(idx)
		if err != nil {
			return err
		}
		if !valid {
			continue
		}
		if _, ok := multiTableIndexes[idx.IndexName]; !ok {
			multiTableIndexes[idx.IndexName] = &MultiTableIndex{
				IndexAlgo: catalog.ToLower(idx.IndexAlgo),
				IndexDefs: make(map[string]*plan.IndexDef),
			}
		}
		multiTableIndexes[idx.IndexName].IndexDefs[catalog.ToLower(idx.IndexAlgoTableType)] = idx
	}

	var cctx *pluginCompileCtx
	for name, mti := range multiTableIndexes {
		p, ok := indexplugin.Get(mti.IndexAlgo)
		if !ok {
			continue
		}
		if cctx == nil {
			cctx = newPluginCompileCtx(s, c, tableDef.GetTblId(), nil, nil, dbName, tableDef, nil)
		}
		startFromNow, initSQL, err := p.Compile().RestoreInitSQL(cctx, mti.IndexDefs)
		if err != nil {
			return err
		}
		if initSQL == "" {
			// no rebuild → the CDC catches the cloned tables up from their
			// watermark, so it must not start from now.
			startFromNow = false
		}
		logutil.Infof("[RestoreTable] re-register CDC index=%s startFromNow=%v initSQL=%q", name, startFromNow, initSQL)
		if err := CreateIndexCdcTask(c, dbName, tblName, tableDef.GetTblId(), name,
			getSinkerTypeFromAlgo(mti.IndexAlgo), startFromNow, initSQL, tableDef); err != nil {
			return err
		}
	}
	logutil.Infof("[RestoreTable] DONE %s.%s", dbName, tblName)
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
		return moerr.NewInvalidInputf(ctx, "MAXVALUE (%d) of sequence must be bigger than MINVALUE (%d) of it", maxValue, minValue)
	}
	if startNum < minValue || startNum > maxValue {
		return moerr.NewInvalidInputf(ctx, "STARTVALUE (%d) for sequence must between MINVALUE (%d) and MAXVALUE (%d)", startNum, minValue, maxValue)
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

var lockTable = func(
	ctx context.Context,
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation,
	dbName string,
	defChanged bool,
) error {
	return doLockTable(eng, proc, rel, defChanged)
}

// lockIndexTable
var lockIndexTable = func(ctx context.Context, dbSource engine.Database, eng engine.Engine, proc *process.Process, tableName string, defChanged bool) error {
	rel, err := dbSource.Relation(ctx, tableName, nil)
	if err != nil {
		return err
	}
	return doLockTable(eng, proc, rel, defChanged)
}

func lockIndexTableForAlter(
	ctx context.Context,
	dbSource engine.Database,
	eng engine.Engine,
	proc *process.Process,
	parentTableName string,
	plannedParentTableID uint64,
	plannedIndex *plan.IndexDef,
	parentDefinitionChanged bool,
) error {
	if plannedIndex == nil {
		return moerr.NewInternalError(ctx, "nil index definition while locking ALTER TABLE index")
	}
	err := lockIndexTable(ctx, dbSource, eng, proc, plannedIndex.IndexTableName, true)
	if err == nil || !moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
		return err
	}

	// A definition-change result from the parent lock already proves that the
	// planned hidden-table generation is stale. Otherwise, distinguish that
	// race from persistent catalog corruption before asking the RC loop to
	// replan: retrying an unchanged missing generation would never converge.
	if parentDefinitionChanged {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	changed, checkErr := alterIndexGenerationChanged(
		ctx, dbSource, parentTableName, plannedParentTableID, plannedIndex,
	)
	if checkErr != nil {
		return checkErr
	}
	if changed {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	return err
}

func alterIndexGenerationChanged(
	ctx context.Context,
	dbSource engine.Database,
	parentTableName string,
	plannedParentTableID uint64,
	plannedIndex *plan.IndexDef,
) (bool, error) {
	currentParent, err := dbSource.Relation(ctx, parentTableName, nil)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return true, nil
		}
		return false, err
	}
	if currentParent.GetTableID(ctx) != plannedParentTableID {
		return true, nil
	}
	currentTableDef := currentParent.CopyTableDef(ctx)
	if currentTableDef == nil {
		return false, moerr.NewInternalErrorf(ctx,
			"missing table definition for ALTER TABLE parent %s", parentTableName)
	}
	for _, currentIndex := range currentTableDef.Indexes {
		if currentIndex == nil || !strings.EqualFold(currentIndex.IndexName, plannedIndex.IndexName) {
			continue
		}
		return currentIndex.TableExist != plannedIndex.TableExist ||
			currentIndex.IndexTableName != plannedIndex.IndexTableName, nil
	}
	return true, nil
}

func lockRows(
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation,
	bat *batch.Batch,
	idx int32,
	lockMode lock.LockMode,
	sharding lock.Sharding,
	group uint32,
) error {
	var vec *vector.Vector
	if bat != nil {
		vec = bat.GetVector(idx)
	}
	if vec == nil || vec.Length() == 0 {
		panic("lock rows is empty")
	}

	id := rel.GetTableID(proc.Ctx)

	return lockop.LockRows(
		eng,
		proc,
		rel,
		id,
		bat,
		idx,
		*vec.GetType(),
		lockMode,
		sharding,
		group,
	)
}

var maybeCreateAutoIncrement = func(
	ctx context.Context,
	sid string,
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

	return incrservice.GetAutoIncrementService(sid).Create(
		ctx,
		def.TblId,
		cols,
		txnOp)
}

func maybeDeleteAutoIncrement(
	ctx context.Context,
	sid string,
	db engine.Database,
	tblname string,
	txnOp client.TxnOperator) error {

	// check if contains any auto_increment column(include __mo_fake_pk_col), if so, reset the auto_increment value
	rel, err := db.Relation(ctx, tblname, nil)
	if err != nil {
		return err
	}

	tblId := rel.GetTableID(ctx)

	tblDef := rel.GetTableDef(ctx)
	var containAuto bool
	for _, col := range tblDef.Cols {
		if col.Typ.AutoIncr {
			containAuto = true
			break
		}
	}
	if containAuto {
		err = incrservice.GetAutoIncrementService(sid).Delete(
			ctx,
			tblId,
			txnOp)
		if err != nil {
			return err
		}
	}

	return nil
}

func maybeResetAutoIncrement(
	ctx context.Context,
	sid string,
	db engine.Database,
	tblname string,
	oldId uint64,
	newId uint64,
	keepAutoIncrement bool,
	txnOp client.TxnOperator) error {

	// check if contains any auto_increment column(include __mo_fake_pk_col), if so, reset the auto_increment value

	rel, err := db.Relation(ctx, tblname, nil)
	if err != nil {
		return err
	}

	tblDef := rel.GetTableDef(ctx)
	var containAuto bool
	for _, col := range tblDef.Cols {
		if col.Typ.AutoIncr {
			containAuto = true
			break
		}
	}
	if containAuto {
		err = incrservice.GetAutoIncrementService(sid).Reset(
			ctx,
			oldId,
			newId,
			keepAutoIncrement,
			txnOp)
		if err != nil {
			return err
		}
	}

	return nil
}

// requireCheckRenameProtocol is the sender-side safety boundary for the v15
// AlterTableRenameCol.checks field. Planner checks provide an earlier error,
// while this check prevents a synthesized or cached plan from sending required
// CHECK metadata to a receiver whose alter handler cannot apply it.
func (c *Compile) requireCheckRenameProtocol(checks []*plan.CheckDef) error {
	if len(checks) == 0 {
		return nil
	}
	value, ok := moruntime.ServiceRuntime(c.proc.GetService()).
		GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	if !ok || !valid || version < defines.MORPCVersion15 {
		return moerr.NewNotSupported(
			c.proc.Ctx,
			"renaming a column in a table with CHECK constraints requires all services to support protocol version 15",
		)
	}
	return nil
}

func (c *Compile) getAlterAutoIncrementOffset(
	dbName string,
	tblName string,
	colName string,
	requestedOffset uint64,
) (uint64, error) {
	colIdent := sqlquote.Ident(colName)
	sql := fmt.Sprintf(
		"select cast(coalesce(max(case when %s > 0 then %s else 0 end), 0) as unsigned) from %s",
		colIdent,
		colIdent,
		sqlquote.QualifiedIdent(dbName, tblName),
	)
	res, err := c.runSqlWithResultAndOptions(
		sql,
		NoAccountId,
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	maxStored := uint64(0)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 || len(cols) == 0 || cols[0].IsNull(0) {
			return false
		}
		maxStored = executor.GetFixedRows[uint64](cols[0])[0]
		return false
	})
	return max(requestedOffset, maxStored), nil
}

func (c *Compile) appendAlterAutoIncrementReqs(
	dbName string,
	tableDef *plan.TableDef,
	targetTableDef *plan.TableDef,
	did uint64,
	tid uint64,
	requestedOffset uint64,
	cleanup *alterAutoIncrementResetCleanup,
	reqs *[]*api.AlterTableReq,
) error {
	if c.proc.Ctx != nil {
		if err := c.proc.Ctx.Err(); err != nil {
			return err
		}
	}
	if !engine.TxnSupportsAutoIncrEpochFence(c.proc.GetTxnOperator()) {
		return moerr.NewNotSupported(
			c.proc.Ctx,
			"AUTO_INCREMENT allocator reset requires epoch fencing on every TN service",
		)
	}

	svc := incrservice.GetAutoIncrementService(c.proc.GetService())
	for _, col := range incrservice.GetUserAutoColumnFromDef(tableDef) {
		targetCol := plan2.FindColumnByColId(
			targetTableDef.Cols,
			tableDef.Cols[col.ColIndex].ColId,
		)
		if targetCol == nil || !targetCol.Typ.AutoIncr {
			return moerr.NewInternalErrorNoCtxf(
				"AUTO_INCREMENT column %q is missing from final table definition",
				col.ColName,
			)
		}
		offset, err := c.getAlterAutoIncrementOffset(
			dbName,
			tableDef.Name,
			col.ColName,
			requestedOffset,
		)
		if err != nil {
			return err
		}
		if err := incrservice.ValidateAutoColumnOffset(
			c.proc.Ctx,
			types.T(targetCol.Typ.Id),
			offset,
		); err != nil {
			return err
		}
		if err = svc.SetOffset(
			c.proc.Ctx,
			tid,
			col.ColIndex,
			targetCol.Name,
			offset,
			c.proc.GetTxnOperator(),
		); err != nil {
			return err
		}
		cleanup.track(tid)
		if c.proc.Ctx != nil {
			if err := c.proc.Ctx.Err(); err != nil {
				return err
			}
		}
		// disttae assigns the next catalog epoch when applying this request.
		// Do not refresh relation metadata after allocation and substitute a
		// different allocator generation.
		*reqs = append(*reqs, api.NewUpdateAutoIncrementReq(did, tid, offset, 0))
	}
	return nil
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

func getLockBatch(proc *process.Process, accountId uint32, names []string) (*batch.Batch, error) {
	vecs := make([]*vector.Vector, len(names)+1)
	defer func() {
		for _, v := range vecs {
			if v != nil {
				v.Free(proc.GetMPool())
			}
		}
	}()

	// append account_id
	accountIdVec := vector.NewVec(types.T_uint32.ToType())
	err := vector.AppendFixed(accountIdVec, accountId, false, proc.GetMPool())
	if err != nil {
		return nil, err
	}
	vecs[0] = accountIdVec
	// append names
	for i, name := range names {
		nameVec := vector.NewVec(types.T_varchar.ToType())
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
	bat := batch.NewWithSize(1)
	bat.SetVector(0, vec)
	return bat, nil
}

var lockMoDatabase = func(c *Compile, dbName string, lockMode lock.LockMode) error {
	dbRel, err := getRelFromMoCatalog(c, catalog.MO_DATABASE)
	if err != nil {
		return err
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	bat, err := getLockBatch(c.proc, accountID, []string{dbName})
	if err != nil {
		return err
	}
	defer bat.GetVector(0).Free(c.proc.Mp())
	if err := lockRows(c.e, c.proc, dbRel, bat, 0, lockMode, lock.Sharding_None, accountID); err != nil {
		return err
	}
	return nil
}

var lockMoTable = func(
	c *Compile,
	dbName string,
	tblName string,
	lockMode lock.LockMode,
) error {
	dbRel, err := getRelFromMoCatalog(c, catalog.MO_TABLES)
	if err != nil {
		return err
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	bat, err := getLockBatch(c.proc, accountID, []string{dbName, tblName})
	if err != nil {
		return err
	}
	defer bat.GetVector(0).Free(c.proc.Mp())

	if err := lockRows(c.e, c.proc, dbRel, bat, 0, lockMode, lock.Sharding_None, accountID); err != nil {
		return err
	}
	return nil
}

func (s *Scope) CreatePitr(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	createPitr := s.Plan.GetDdl().GetCreatePitr()
	pitrName := createPitr.GetName()
	pitrLevel := tree.PitrLevel(createPitr.GetLevel())

	// Get current account info
	accountId, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}

	// INTERNAL PITR creation bypasses the frontend path. Cross the same stable
	// publication barrier as frontend snapshot/PITR creation before refreshing
	// the target object ID, and retain the write until the PITR row commits.
	// This prevents COPY ALTER from swapping the table generation between
	// planning and publication.
	pitrObjectID, err := preparePitrPublication(
		c.lockDataBranchLineageOwnerLifecycle,
		func() error {
			txnMeta := c.proc.GetTxnOperator().Txn()
			if shouldAdvanceAlterDataBranchLineageSnapshot(
				txnMeta.IsPessimistic(), txnMeta.IsRCIsolation(),
			) {
				_, err := c.advanceAlterDataBranchLineageSnapshot()
				return err
			}
			return nil
		},
		func() (uint64, error) {
			return c.resolveCurrentPitrObjectID(createPitr)
		},
	)
	if err != nil {
		return err
	}

	// check pitr if exists（pitr_name + create_account）
	checkExistSql := getSqlForCheckPitrExists(pitrName, accountId)
	existRes, err := c.runSqlWithResultAndOptions(checkExistSql, int32(sysAccountId), executor.StatementOption{}.WithDisableLog())
	if err != nil {
		return err
	}
	defer existRes.Close()
	if len(existRes.Batches) > 0 && existRes.Batches[0].RowCount() > 0 {
		if !createPitr.GetIfNotExists() {
			return moerr.NewInternalErrorf(c.proc.Ctx, "pitr %s already exists", pitrName)
		} else {
			return nil
		}
	}

	// Check if pitr dup
	checkSql := getSqlForCheckPitrDup(createPitr)
	res, err := c.runSqlWithResultAndOptions(checkSql, int32(sysAccountId), executor.StatementOption{}.WithDisableLog())
	if err != nil {
		return err
	}
	defer res.Close()
	if len(res.Batches) > 0 && res.Batches[0].RowCount() > 0 {
		return pitrDupError(c, createPitr)
	}

	// get pitr id
	newUUid, err := uuid.NewV7()
	if err != nil {
		return err
	}

	now := c.proc.GetTxnOperator().SnapshotTS().ToStdTime().UTC().UnixNano()
	// Build create pitr sql
	sql := fmt.Sprintf(`insert into mo_catalog.mo_pitr(
                               pitr_id,
                               pitr_name,
                               create_account,
                               create_time,
                               modified_time,
                               level,
                               account_id,
                               account_name,
                               database_name,
                               table_name,
                               obj_id,
                               pitr_length,
	                               pitr_unit,
	                               pitr_status_changed_time) values ('%s', '%s', %d, %d, %d, '%s', %d, '%s', '%s', '%s', %d, %d, '%s', %d)`,
		newUUid,
		sqlquote.EscapeString(pitrName),
		createPitr.GetCurrentAccountId(),
		now,
		now,
		sqlquote.EscapeString(pitrLevel.String()),
		createPitr.GetCurrentAccountId(),
		sqlquote.EscapeString(createPitr.GetAccountName()),
		sqlquote.EscapeString(createPitr.GetDatabaseName()),
		sqlquote.EscapeString(createPitr.GetTableName()),
		pitrObjectID,
		createPitr.GetPitrValue(),
		sqlquote.EscapeString(createPitr.GetPitrUnit()),
		now,
	)

	// Execute create pitr sql
	err = c.runSqlWithAccountIdAndOptions(sql, int32(sysAccountId), executor.StatementOption{}.WithDisableLog())
	if err != nil {
		return err
	}

	// --- Begin sys_mo_catalog_pitr logic ---
	const sysMoCatalogPitr = "sys_mo_catalog_pitr"
	const sysAccountId = 0

	// Query for sys_mo_catalog_pitr
	sysPitrSql := "SELECT pitr_length, pitr_unit FROM mo_catalog.mo_pitr WHERE pitr_name = '" + sysMoCatalogPitr + "'"
	sysRes, err := c.runSqlWithResultAndOptions(sysPitrSql, sysAccountId, executor.StatementOption{}.WithDisableLog())
	if err != nil {
		return err
	}
	defer sysRes.Close()

	var needInsertSysPitr = true
	var needUpdateSysPitr = false
	var mergedSysPitrLength = uint64(createPitr.GetPitrValue())
	var mergedSysPitrUnit = createPitr.GetPitrUnit()
	if len(sysRes.Batches) > 0 && sysRes.Batches[0].RowCount() > 0 {
		// sys_mo_catalog_pitr exists
		needInsertSysPitr, needUpdateSysPitr, mergedSysPitrLength, mergedSysPitrUnit, err = CheckSysMoCatalogPitrResult(
			c.proc.Ctx,
			sysRes.Batches[0].Vecs,
			uint64(createPitr.GetPitrValue()),
			createPitr.GetPitrUnit(),
		)
		if err != nil {
			return err
		}
	}

	if needUpdateSysPitr {
		updateSql := fmt.Sprintf("UPDATE mo_catalog.mo_pitr SET pitr_length = %d, pitr_unit = '%s' WHERE pitr_name = '%s'", mergedSysPitrLength, mergedSysPitrUnit, sysMoCatalogPitr)
		err = c.runSqlWithAccountIdAndOptions(updateSql, sysAccountId, executor.StatementOption{}.WithDisableLog())
		if err != nil {
			return err
		}
	}

	if needInsertSysPitr {
		// Get mo_catalog database id
		db, err := c.e.Database(c.proc.Ctx, catalog.MO_CATALOG, c.proc.GetTxnOperator())
		if err != nil {
			return err
		}
		moCatalogId := db.GetDatabaseId(c.proc.Ctx)

		// Generate new UUID for sys_mo_catalog_pitr
		sysPitrUuid, err := uuid.NewV7()
		if err != nil {
			return err
		}
		insertSql := fmt.Sprintf(`insert into mo_catalog.mo_pitr(
			pitr_id,
			pitr_name,
			create_account,
			create_time,
			modified_time,
			level,
			account_id,
			account_name,
			database_name,
			table_name,
			obj_id,
			pitr_length,
			pitr_unit,
            pitr_status_changed_time
        ) values ('%s', '%s', %d, %d, %d, '%s', %d, '%s', '%s', '%s', '%s', %d, '%s', %d)`,

			sysPitrUuid,
			sysMoCatalogPitr,
			sysAccountId,
			now,
			now,
			tree.PITRLEVELDATABASE.String(),
			sysAccountId,
			"sys",
			catalog.MO_CATALOG,
			"",
			moCatalogId,
			createPitr.GetPitrValue(),
			createPitr.GetPitrUnit(),
			now,
		)
		err = c.runSqlWithAccountIdAndOptions(insertSql, sysAccountId, executor.StatementOption{}.WithDisableLog())
		if err != nil {
			return err
		}
	}
	// --- End sys_mo_catalog_pitr logic ---

	return nil
}

func preparePitrPublication(
	lock func() error,
	refreshSnapshot func() error,
	resolveObjectID func() (uint64, error),
) (uint64, error) {
	if err := lock(); err != nil {
		return 0, err
	}
	if err := refreshSnapshot(); err != nil {
		return 0, err
	}
	return resolveObjectID()
}

func (c *Compile) resolveCurrentPitrObjectID(createPitr *plan.CreatePitr) (uint64, error) {
	ctx := c.proc.Ctx
	switch tree.PitrLevel(createPitr.GetLevel()) {
	case tree.PITRLEVELCLUSTER:
		return math.MaxUint64, nil
	case tree.PITRLEVELACCOUNT:
		return uint64(createPitr.GetAccountId()), nil
	case tree.PITRLEVELDATABASE:
		db, err := c.e.Database(ctx, createPitr.GetDatabaseName(), c.proc.GetTxnOperator())
		if err != nil {
			return 0, err
		}
		databaseID, err := strconv.ParseUint(db.GetDatabaseId(ctx), 10, 64)
		if err != nil {
			return 0, moerr.NewInternalErrorf(
				ctx,
				"The databaseid of '%s' is not a valid number",
				createPitr.GetDatabaseName(),
			)
		}
		return databaseID, nil
	case tree.PITRLEVELTABLE:
		db, err := c.e.Database(ctx, createPitr.GetDatabaseName(), c.proc.GetTxnOperator())
		if err != nil {
			return 0, err
		}
		rel, err := db.Relation(ctx, createPitr.GetTableName(), nil)
		if err != nil {
			return 0, err
		}
		return rel.GetTableID(ctx), nil
	default:
		return 0, moerr.NewInternalErrorf(ctx, "invalid pitr level %d", createPitr.GetLevel())
	}
}

func (s *Scope) DropPitr(c *Compile) error {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()
	if err := c.lockDataBranchLineageOwnerLifecycle(); err != nil {
		return err
	}

	dropPitr := s.Plan.GetDdl().GetDropPitr()
	pitrName := dropPitr.GetName()
	if pitrName == "" {
		return moerr.NewInternalErrorf(c.proc.Ctx, "pitr name is empty")
	}
	const sysMoCatalogPitr = "sys_mo_catalog_pitr"
	const sysAccountId = 0

	// Get current account
	accountId, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}

	// 1. Check if PITR exists
	checkSql := fmt.Sprintf("SELECT pitr_id FROM mo_catalog.mo_pitr WHERE pitr_name = '%s' AND create_account = %d", sqlquote.EscapeString(pitrName), accountId)
	res, err := c.runSqlWithResultAndOptions(checkSql, int32(sysAccountId), executor.StatementOption{}.WithDisableLog())
	if err != nil {
		return err
	}
	defer res.Close()
	if len(res.Batches) == 0 || res.Batches[0].RowCount() == 0 {
		if !dropPitr.GetIfExists() {
			return moerr.NewInternalErrorf(c.proc.Ctx, "pitr %s does not exist", pitrName)
		}
		return nil
	}

	// 2. Delete PITR record
	deleteSql := fmt.Sprintf("DELETE FROM mo_catalog.mo_pitr WHERE pitr_name = '%s' AND create_account = %d", sqlquote.EscapeString(pitrName), accountId)
	err = c.runSqlWithAccountIdAndOptions(deleteSql, int32(sysAccountId), executor.StatementOption{}.WithDisableLog())
	if err != nil {
		return err
	}

	// 3. Check if there are other PITR records besides sys_mo_catalog_pitr
	checkOtherSql := fmt.Sprintf("SELECT pitr_id FROM mo_catalog.mo_pitr WHERE pitr_name != '%s'", sysMoCatalogPitr)
	otherRes, err := c.runSqlWithResultAndOptions(checkOtherSql, sysAccountId, executor.StatementOption{}.WithDisableLog())
	if err != nil {
		return err
	}
	defer otherRes.Close()
	if len(otherRes.Batches) == 0 || otherRes.Batches[0].RowCount() == 0 {
		// 4. No other PITR records, delete sys_mo_catalog_pitr
		deleteSysSql := fmt.Sprintf("DELETE FROM mo_catalog.mo_pitr WHERE pitr_name = '%s' AND create_account = %d", sysMoCatalogPitr, sysAccountId)
		err = c.runSqlWithAccountIdAndOptions(deleteSysSql, sysAccountId, executor.StatementOption{}.WithDisableLog())
		if err != nil {
			return err
		}
	}

	return nil
}

func pitrDupError(c *Compile, createPitr *plan.CreatePitr) error {
	pitrLevel := tree.PitrLevel(createPitr.Level)
	switch pitrLevel {
	case tree.PITRLEVELCLUSTER:
		return moerr.NewInternalError(c.proc.Ctx, "cluster level pitr already exists")
	case tree.PITRLEVELACCOUNT:
		return moerr.NewInternalErrorf(c.proc.Ctx, "account %s does not exist", createPitr.AccountName)
	case tree.PITRLEVELDATABASE:
		return moerr.NewInternalErrorf(c.proc.Ctx, "database `%s` already has a pitr", createPitr.DatabaseName)
	default:
		return moerr.NewInternalErrorf(c.proc.Ctx, "table %s.%s does not exist", createPitr.DatabaseName, createPitr.TableName)
	}
}

func getSqlForCheckPitrDup(
	createPitr *plan.CreatePitr,
) string {
	sql := "SELECT pitr_id FROM mo_catalog.mo_pitr WHERE create_account = %d"
	switch tree.PitrLevel(createPitr.GetLevel()) {
	case tree.PITRLEVELCLUSTER:
		return getSqlForCheckDupPitrFormat(createPitr.CurrentAccountId, math.MaxUint64)
	case tree.PITRLEVELACCOUNT:
		if createPitr.OriginAccountName {
			return fmt.Sprintf(sql, createPitr.CurrentAccountId) + fmt.Sprintf(" AND account_name = '%s' AND level = 'account' AND pitr_status = 1;", sqlquote.EscapeString(createPitr.AccountName))
		} else {
			return fmt.Sprintf(sql, createPitr.CurrentAccountId) + fmt.Sprintf(" AND account_name = '%s' AND level = 'account' AND pitr_status = 1;", sqlquote.EscapeString(createPitr.CurrentAccount))
		}
	case tree.PITRLEVELDATABASE:
		return fmt.Sprintf(sql, createPitr.CurrentAccountId) + fmt.Sprintf(" AND database_name = '%s' AND level = 'database' AND pitr_status = 1;", sqlquote.EscapeString(createPitr.DatabaseName))
	case tree.PITRLEVELTABLE:
		return fmt.Sprintf(sql, createPitr.CurrentAccountId) + fmt.Sprintf(" AND database_name = '%s' AND table_name = '%s' AND level = 'table' AND pitr_status = 1;", sqlquote.EscapeString(createPitr.DatabaseName), sqlquote.EscapeString(createPitr.TableName))
	}
	return sql
}

func getSqlForCheckDupPitrFormat(accountId uint32, objId uint64) string {
	return fmt.Sprintf(`SELECT pitr_id FROM mo_catalog.mo_pitr WHERE create_account = %d AND obj_id = %d;`, accountId, objId)
}

// CheckSysMoCatalogPitrResult parses the sys_mo_catalog_pitr query result and determines whether to insert or update.
// Arguments:
//
//	ctx: context for error reporting
//	vecs: the vectors from the query result (should have at least 2 columns)
//	newLength: the new PITR length to compare
//	newUnit: the new PITR unit to compare
//
// Returns:
//
//	needInsert: true if sys_mo_catalog_pitr does not exist
//	needUpdate: true if it exists and needs update
//	mergedLength, mergedUnit: a future-stable range covering old and new values
//	err: error if any
func CheckSysMoCatalogPitrResult(
	ctx context.Context,
	vecs []*vector.Vector,
	newLength uint64,
	newUnit string,
) (needInsert, needUpdate bool, mergedLength uint64, mergedUnit string, err error) {
	needInsert = true
	needUpdate = false
	mergedLength = newLength
	mergedUnit = newUnit
	var oldLength uint64
	oldUnit := ""
	if len(vecs) < 2 {
		return false, false, 0, "", moerr.NewInternalErrorf(ctx, "unexpected sys_mo_catalog_pitr result columns")
	}
	if vecs[0].Length() > 0 {
		col := vector.MustFixedColNoTypeCheck[uint64](vecs[0])
		oldLength = col[0]
	}
	if vecs[1].Length() > 0 {
		col := vector.MustFixedColNoTypeCheck[types.Varlena](vecs[1])
		oldUnit = col[0].GetString(vecs[1].GetArea())
	}
	if vecs[0].Length() > 0 && vecs[1].Length() > 0 {
		needInsert = false
		if oldLength > 100 || newLength > 100 {
			return false, false, 0, "", moerr.NewInvalidInputf(
				ctx,
				"invalid PITR lengths %d and %d",
				oldLength,
				newLength,
			)
		}
		merged, unit, mergeErr := databranchutils.MergePitrRetentionRanges(
			int(oldLength),
			oldUnit,
			int(newLength),
			newUnit,
		)
		if mergeErr != nil {
			return false, false, 0, "", mergeErr
		}
		mergedLength = uint64(merged)
		mergedUnit = unit
		needUpdate = mergedLength != oldLength || mergedUnit != oldUnit
	}
	return needInsert, needUpdate, mergedLength, mergedUnit, nil
}

func getSqlForCheckPitrExists(pitrName string, accountId uint32) string {
	return fmt.Sprintf("SELECT pitr_id FROM mo_catalog.mo_pitr WHERE pitr_name = '%s' AND create_account = %d ORDER BY pitr_id", sqlquote.EscapeString(pitrName), accountId)
}

func (s *Scope) CreateCDC(c *Compile) error {
	ctx := c.proc.Ctx
	planCDC := s.Plan.GetDdl().GetCreateCdc()

	// 1. Verify and populate creation options
	opts := &CDCCreateTaskOptions{}
	if err := opts.ValidateAndFill(ctx, c, planCDC); err != nil {
		return err
	}

	// 1.5 IF NOT EXISTS pre-check to avoid inconsistent rows between mo_cdc_task and daemon task
	if planCDC.GetIfNotExists() {
		preCheckSQL := cdc.CDCSQLBuilder.GetTaskIdSQL(uint64(opts.UserInfo.AccountId), opts.TaskName)
		if res, err := c.runSqlWithResultAndOptions(preCheckSQL, int32(catalog.System_Account), executor.StatementOption{}.WithDisableLog()); err == nil {
			exists := false
			if len(res.Batches) > 0 && res.Batches[0].Vecs[0].Length() > 0 {
				exists = true
			}
			res.Close()
			if exists {
				return nil
			}
		}
	}

	// 2. Build task details
	details, err := opts.BuildTaskDetails()
	if err != nil {
		return err
	}

	// 3. Obtain task service
	taskService := c.proc.GetTaskService()
	if taskService == nil {
		return moerr.NewInternalErrorf(ctx, "taskService is nil")
	}

	// 4. Create a task job function
	createTaskJob := func(
		ctx context.Context,
		tx taskservice.SqlExecutor,
	) (ret int, err error) {
		// Ensure ParameterUnit is available in ctx for helpers using config.GetParameterUnit(ctx)
		if ctx.Value(config.ParameterUnitKey) == nil {
			if v, ok := moruntime.ServiceRuntime(c.proc.GetService()).GetGlobalVariables("parameter-unit"); ok {
				if pu, ok2 := v.(*config.ParameterUnit); ok2 && pu != nil {
					ctx = context.WithValue(ctx, config.ParameterUnitKey, pu)
				}
			}
		}

		var (
			insertSql    string
			rowsAffected int64
		)

		// Generate SQL for inserting tasks
		if insertSql, err = opts.ToInsertTaskSQL(ctx, tx); err != nil {
			return 0, err
		}

		// Execute SQL and obtain the number of affected rows
		if rowsAffected, err = ExecuteAndGetRowsAffected(ctx, tx, insertSql); err != nil {
			return 0, err
		}

		return int(rowsAffected), nil
	}

	// 5. Add CDC task
	_, err = taskService.AddCDCTask(
		ctx, opts.BuildTaskMetadata(), details, createTaskJob,
	)

	return err
}

func (s *Scope) DropCDC(c *Compile) error {
	planCDC := s.Plan.GetDdl().GetDropCdc()
	var (
		targetTaskStatus task.TaskStatus
		// task name may be empty
		taskName  string
		accountId = planCDC.AccountId
		conds     = make([]taskservice.Condition, 0)
	)

	targetTaskStatus = task.TaskStatus_CancelRequested
	conds = append(
		conds,
		taskservice.WithAccountID(taskservice.EQ, accountId),
		taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
	)
	if !planCDC.All {
		taskName = planCDC.TaskName
		conds = append(
			conds,
			taskservice.WithTaskName(taskservice.EQ, taskName),
		)
	}
	return doUpdateCDCTask(
		c.proc,
		targetTaskStatus,
		uint64(accountId),
		taskName,
		planCDC.IfExists,
		conds...,
	)
}

func doUpdateCDCTask(
	proc *process.Process,
	targetTaskStatus task.TaskStatus,
	accountId uint64,
	taskName string,
	ifExists bool,
	conds ...taskservice.Condition,
) (err error) {
	ts := proc.GetTaskService()
	if ts == nil {
		return nil
	}
	_, err = ts.UpdateCDCTask(proc.Ctx,
		targetTaskStatus,
		func(
			ctx context.Context,
			targetStatus task.TaskStatus,
			keys map[taskservice.CDCTaskKey]struct{},
			tx taskservice.SqlExecutor,
		) (int, error) {
			return onPreUpdateCDCTasks(
				ctx,
				targetStatus,
				keys,
				tx,
				accountId,
				taskName,
				ifExists,
			)
		},
		conds...,
	)
	return
}

func onPreUpdateCDCTasks(
	ctx context.Context,
	targetTaskStatus task.TaskStatus,
	keys map[taskservice.CDCTaskKey]struct{},
	tx taskservice.SqlExecutor,
	accountId uint64,
	taskName string,
	ifExists bool,
) (affectedCdcRow int, err error) {
	var cnt int64

	// Get task keys count
	if cnt, err = getTaskKeysCount(ctx, tx, accountId, taskName, keys, ifExists); err != nil {
		return
	}
	affectedCdcRow = int(cnt)

	// Cancel cdc task
	if targetTaskStatus == task.TaskStatus_CancelRequested {
		// Delete mo_cdc_task
		if cnt, err = ExecuteAndGetRowsAffected(ctx, tx, cdc.CDCSQLBuilder.DeleteTaskSQL(accountId, taskName)); err != nil {
			return
		}
		affectedCdcRow += int(cnt)

		// Delete mo_cdc_watermark
		if cnt, err = deleteManyWatermark(ctx, tx, keys, true); err != nil {
			return
		}
		affectedCdcRow += int(cnt)
		return
	}

	// Step2: update or cancel cdc task
	var targetCDCStatus string
	if targetTaskStatus == task.TaskStatus_PauseRequested {
		targetCDCStatus = cdc.CDCState_Pausing
	} else {
		targetCDCStatus = cdc.CDCState_Running
	}

	if cnt, err = ExecuteAndGetRowsAffected(ctx, tx, cdc.CDCSQLBuilder.UpdateTaskStateSQL(accountId, taskName), targetCDCStatus); err != nil {
		return
	}

	affectedCdcRow += int(cnt)

	// Restart cdc task
	if targetTaskStatus == task.TaskStatus_RestartRequested {
		// RESTART intentionally resets the watermark but retains the immutable
		// table-generation epoch. A bounded snapshot may already have committed
		// target groups at that epoch, so choosing a new one would strand stale
		// DELETE/PK-change rows.
		if cnt, err = deleteManyWatermark(ctx, tx, keys, false); err != nil {
			return
		}
		affectedCdcRow += int(cnt)
	}
	return
}

// getTaskKeysCount gets the count of task keys and populates the keys map
func getTaskKeysCount(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	accountId uint64,
	taskName string,
	keys map[taskservice.CDCTaskKey]struct{},
	ifExists bool,
) (cnt int64, err error) {
	var (
		rows *sql.Rows
	)

	querySQL := cdc.CDCSQLBuilder.GetTaskIdSQL(accountId, taskName)
	if rows, err = tx.QueryContext(ctx, querySQL); err != nil {
		return
	}
	defer rows.Close()

	if rows.Err() != nil {
		return 0, rows.Err()
	}

	for rows.Next() {
		var taskId string
		if err = rows.Scan(&taskId); err != nil {
			return
		}
		key := taskservice.CDCTaskKey{
			AccountId: accountId,
			TaskId:    taskId,
		}
		keys[key] = struct{}{}
		cnt++
	}

	if cnt == 0 && taskName != "" && !ifExists {
		err = moerr.NewInternalErrorf(
			ctx,
			"no cdc task found, accountId: %d, taskName: %s",
			accountId,
			taskName,
		)
	}
	return
}

// deleteManyWatermark deletes multiple watermark records
func deleteManyWatermark(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	keys map[taskservice.CDCTaskKey]struct{},
	deleteSnapshotEpochs bool,
) (deletedCnt int64, err error) {
	var (
		cnt int64
	)

	for key := range keys {
		sql := cdc.CDCSQLBuilder.DeleteWatermarkSQL(
			key.AccountId,
			key.TaskId,
		)
		logutil.Info(
			"cdc.compile.delete_watermark_sql",
			zap.Uint64("account-id", key.AccountId),
			zap.String("task-id", key.TaskId),
			zap.String("sql", sql),
		)
		if cnt, err = ExecuteAndGetRowsAffected(ctx, tx, sql); err != nil {
			return
		}
		deletedCnt += cnt

		if !deleteSnapshotEpochs {
			continue
		}

		sql = cdc.CDCSQLBuilder.DeleteSnapshotEpochSQL(key.AccountId, key.TaskId)
		logutil.Info(
			"cdc.compile.delete_snapshot_epoch_sql",
			zap.Uint64("account-id", key.AccountId),
			zap.String("task-id", key.TaskId),
			zap.String("sql", sql),
		)
		if _, err = ExecuteAndGetRowsAffected(ctx, tx, sql); err != nil {
			return
		}
	}
	return
}

const (
	defaultCDCTaskMaxRetryTimes = 10
	defaultCDCTaskRetryInterval = int64(time.Second * 10)
)

func (opts *CDCCreateTaskOptions) BuildTaskMetadata() task.TaskMetadata {
	executor := task.TaskCode_InitCdc
	if !opts.NoFull && cdc.UsesStableEpochInitialSnapshot(opts.ExtraOpts) {
		executor = task.TaskCode_InitCdcStableEpoch
	}
	return task.TaskMetadata{
		ID:       opts.TaskId,
		Executor: executor,
		Options: task.TaskOptions{
			MaxRetryTimes: defaultCDCTaskMaxRetryTimes,
			RetryInterval: defaultCDCTaskRetryInterval,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
}

func ExecuteAndGetRowsAffected(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	query string,
	args ...interface{},
) (int64, error) {
	exec, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	rows, err := exec.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func (opts *CDCCreateTaskOptions) BuildTaskDetails() (details *task.Details, err error) {
	details = &task.Details{
		AccountID: opts.UserInfo.AccountId,
		Account:   opts.UserInfo.AccountName,
		Username:  opts.UserInfo.UserName,
		Details: &task.Details_CreateCdc{
			CreateCdc: &task.CreateCdcDetails{
				TaskName: opts.TaskName,
				TaskId:   opts.TaskId,
				Accounts: []*task.Account{
					{
						Id:   uint64(opts.UserInfo.AccountId),
						Name: opts.UserInfo.AccountName,
					},
				},
			},
		},
	}
	return
}

var initAesKeyBySqlExecutor = func(
	ctx context.Context,
	executor taskservice.SqlExecutor,
	accountId uint32,
) (err error) {
	if len(cdc.AesKey) > 0 {
		return nil
	}

	var (
		encryptedKey string
		cnt          int64
	)
	querySql := cdc.CDCSQLBuilder.GetDataKeySQL(uint64(accountId), cdc.InitKeyId)

	if cnt, err = ForeachQueriedRow(
		ctx,
		executor,
		querySql,
		func(ctx context.Context, rows *sql.Rows) (bool, error) {
			if cnt > 0 {
				return false, nil
			}
			if err2 := rows.Scan(&encryptedKey); err2 != nil {
				return false, err2
			}
			cnt++
			return true, nil
		},
	); err != nil {
		return
	} else if cnt == 0 {
		return moerr.NewInternalError(ctx, "no data key")
	}

	cdc.AesKey, err = cdc.AesCFBDecodeWithKey(
		ctx,
		encryptedKey,
		[]byte(config.GetParameterUnit(ctx).SV.KeyEncryptionKey),
	)
	return
}

var ForeachQueriedRow = func(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	query string,
	onEachRow func(context.Context, *sql.Rows) (bool, error),
) (cnt int64, err error) {
	var (
		ok   bool
		rows *sql.Rows
	)
	if rows, err = tx.QueryContext(ctx, query); err != nil {
		return
	}
	if rows.Err() != nil {
		err = rows.Err()
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		if ok, err = onEachRow(ctx, rows); err != nil {
			return
		}
		if ok {
			cnt++
		}
	}
	return
}

func (opts *CDCCreateTaskOptions) ToInsertTaskSQL(
	ctx context.Context,
	tx taskservice.SqlExecutor,
) (sql string, err error) {
	var encodedSinkPwd string
	if !opts.UseConsole {
		if err = initAesKeyBySqlExecutor(
			ctx, tx, catalog.System_Account,
		); err != nil {
			return
		}
		if encodedSinkPwd, err = opts.SinkUriInfo.GetEncodedPassword(); err != nil {
			return
		}
	}

	sql = cdc.CDCSQLBuilder.InsertTaskSQL(
		uint64(opts.UserInfo.AccountId),
		opts.TaskId,
		opts.TaskName,
		opts.SrcUri,
		"",
		opts.SinkUri,
		opts.SinkType,
		encodedSinkPwd,
		"",
		"",
		"",
		opts.PitrTables,
		opts.Exclude,
		"",
		cdc.CDCState_Common,
		cdc.CDCState_Common,
		opts.StartTs,
		opts.EndTs,
		opts.ConfigFile,
		time.Now().UTC(),
		cdc.CDCState_Running,
		0,
		opts.NoFull,
		"",
		opts.ExtraOpts,
	)
	return
}

type CDCUserInfo struct {
	UserName    string
	AccountId   uint32
	AccountName string
}

type CDCCreateTaskOptions struct {
	TaskName     string
	TaskId       string
	UserInfo     *CDCUserInfo
	Exclude      string
	StartTs      string
	EndTs        string
	MaxSqlLength int64
	PitrTables   string // json encoded pitr tables: cdc2.PatternTuples
	SrcUri       string // json encoded source uri: cdc2.UriInfo
	SrcUriInfo   cdc.UriInfo
	SinkUri      string // json encoded sink uri: cdc2.UriInfo
	SinkUriInfo  cdc.UriInfo
	ExtraOpts    string // json encoded extra opts: map[string]any
	SinkType     string
	NoFull       bool
	ConfigFile   string

	// control options
	UseConsole bool
}

func (opts *CDCCreateTaskOptions) ValidateAndFill(
	ctx context.Context,
	c *Compile,
	planCDC *plan.CreateCDC,
) (err error) {
	taskId := cdc.NewTaskId()
	opts.TaskName = planCDC.TaskName
	opts.TaskId = taskId.String()
	opts.UserInfo = &CDCUserInfo{
		UserName:    planCDC.UserName,
		AccountId:   planCDC.AccountId,
		AccountName: planCDC.AccountName,
	}

	tmpOpts := make(map[string]string, len(planCDC.Option)/2)
	for i := 0; i < len(planCDC.Option)-1; i += 2 {
		key := planCDC.Option[i]
		value := planCDC.Option[i+1]
		tmpOpts[key] = value
	}

	// extract source uri and check connection
	// target field: SrcUri
	{
		if opts.SrcUri, opts.SrcUriInfo, err = cdc.ExtractUriInfo(
			ctx, planCDC.SourceUri, cdc.CDCSourceUriPrefix,
		); err != nil {
			return
		}
		sourceConn, sourceConnErr := cdc.OpenDbConn(
			ctx,
			opts.SrcUriInfo.User, opts.SrcUriInfo.Password, opts.SrcUriInfo.Ip, opts.SrcUriInfo.Port, cdc.CDCDefaultSendSqlTimeout,
		)
		if sourceConnErr != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to connect to source, please check the connection, err: %v", sourceConnErr)
			return
		}
		if closeErr := sourceConn.Close(); closeErr != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to close source connection check: %v", closeErr)
			return
		}
	}

	// 1. Converts sink type to lowercase
	// 2. Enables console sink if configured and requested
	// 3. For non-console sinks, validates that only mysql or matrixone sinks are used
	// 4. Returns error for unsupported sink types
	// target field: SinkType, UseConsole
	{
		opts.SinkType = strings.ToLower(planCDC.SinkType)
		if cdc.EnableConsoleSink && opts.SinkType == cdc.CDCSinkType_Console {
			opts.UseConsole = true
		}
		if !opts.UseConsole && opts.SinkType != cdc.CDCSinkType_MySQL && opts.SinkType != cdc.CDCSinkType_MO {
			err = moerr.NewInternalErrorf(ctx, "unsupported sink type: %s", planCDC.SinkType)
			return
		}

		if opts.SinkUri, opts.SinkUriInfo, err = cdc.ExtractUriInfo(
			ctx, planCDC.SinkUri, cdc.CDCSinkUriPrefix,
		); err != nil {
			return
		}
		sinkConn, sinkConnErr := cdc.OpenDbConn(
			ctx,
			opts.SinkUriInfo.User, opts.SinkUriInfo.Password, opts.SinkUriInfo.Ip, opts.SinkUriInfo.Port, cdc.CDCDefaultSendSqlTimeout,
		)
		if sinkConnErr != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to connect to sink, please check the connection, err: %v", sinkConnErr)
			return
		}
		if closeErr := sinkConn.Close(); closeErr != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to close sink connection check: %v", closeErr)
			return
		}
	}

	var (
		startTs, endTs time.Time
		extraOpts      = make(map[string]any)
		level          string
	)

	for _, key := range cdc.CDCRequestOptions {
		value := tmpOpts[key]
		switch key {
		case cdc.CDCRequestOptions_NoFull:
			opts.NoFull, _ = strconv.ParseBool(value)
		case cdc.CDCRequestOptions_Level:
			if err = opts.handleLevel(ctx, c, value, planCDC.Tables); err != nil {
				return
			}
			level = value
		case cdc.CDCRequestOptions_Exclude:
			if _, err = regexp.Compile(value); err != nil {
				err = moerr.NewInternalErrorf(ctx, "invalid exclude: %s, err: %v", value, err)
				return
			}
			opts.Exclude = strings.ReplaceAll(value, "\\", "\\\\")
		case cdc.CDCRequestOptions_StartTs:
			if value != "" {
				if startTs, err = CDCStrToTime(value, c.proc.GetSessionInfo().TimeZone); err != nil {
					err = moerr.NewInternalErrorf(ctx, "invalid startTs: %s, supported timestamp format: `%s`, or `%s`", value, time.DateTime, time.RFC3339)
					return
				}
				opts.StartTs = startTs.Format(time.RFC3339)
			}
		case cdc.CDCRequestOptions_EndTs:
			if value != "" {
				if endTs, err = CDCStrToTime(value, c.proc.GetSessionInfo().TimeZone); err != nil {
					err = moerr.NewInternalErrorf(ctx, "invalid endTs: %s, supported timestamp format: `%s`, or `%s`", value, time.DateTime, time.RFC3339)
					return
				}
				opts.EndTs = endTs.Format(time.RFC3339)
			}
		case cdc.CDCRequestOptions_MaxSqlLength:
			if value != "" {
				var maxSqlLength int64
				if maxSqlLength, err = strconv.ParseInt(value, 10, 64); err != nil {
					err = moerr.NewInternalErrorf(ctx, "invalid maxSqlLength: %s", value)
					return
				}
				extraOpts[cdc.CDCTaskExtraOptions_MaxSqlLength] = maxSqlLength
			}
		case cdc.CDCRequestOptions_InitSnapshotSplitTxn:
			if value == "false" {
				extraOpts[cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn] = false
			}
		case cdc.CDCRequestOptions_SendSqlTimeout:
			if value != "" {
				if _, err = time.ParseDuration(value); err != nil {
					err = moerr.NewInternalErrorf(ctx, "invalid sendSqlTimeout: %s", value)
					return
				}
				extraOpts[cdc.CDCTaskExtraOptions_SendSqlTimeout] = value
			}
		case cdc.CDCRequestOptions_ConfigFile:
			if value != "" {
				opts.ConfigFile = value
			}
		case cdc.CDCRequestOptions_Frequency:
			extraOpts[cdc.CDCTaskExtraOptions_Frequency] = ""
			if value != "" {
				if err = opts.handleFrequency(ctx, c, level, value, planCDC.Tables); err != nil {
					return
				}
			}
			extraOpts[cdc.CDCTaskExtraOptions_Frequency] = value
		}
	}

	if !startTs.IsZero() && !endTs.IsZero() && !endTs.After(startTs) {
		err = moerr.NewInternalErrorf(ctx, "startTs: %s should be less than endTs: %s", startTs.Format(time.RFC3339), endTs.Format(time.RFC3339))
		return
	}

	// fill default value for additional opts
	if _, ok := extraOpts[cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn]; !ok {
		extraOpts[cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn] = cdc.CDCDefaultTaskExtra_InitSnapshotSplitTxn
	}
	if _, ok := extraOpts[cdc.CDCTaskExtraOptions_SendSqlTimeout]; !ok {
		extraOpts[cdc.CDCTaskExtraOptions_SendSqlTimeout] = cdc.CDCDefaultSendSqlTimeout
	}
	if _, ok := extraOpts[cdc.CDCTaskExtraOptions_MaxSqlLength]; !ok {
		extraOpts[cdc.CDCTaskExtraOptions_MaxSqlLength] = cdc.CDCDefaultTaskExtra_MaxSQLLen
	}
	// Only full snapshots need the stable-epoch capability fence. NoFull tasks
	// remain eligible for legacy executors because they cannot partially commit
	// an initial snapshot.
	if !opts.NoFull {
		cdc.FinalizeInitialSnapshotOptions(extraOpts)
		_, stable := extraOpts[cdc.CDCTaskExtraOptions_InitialSnapshotProtocol]
		if err = validateStableInitialSnapshotCompileProtocol(ctx, c, stable); err != nil {
			return
		}
	}

	var extraOptsBytes []byte
	if extraOptsBytes, err = json.Marshal(extraOpts); err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to marshal extra opts: %v", err)
		return
	}
	opts.ExtraOpts = string(extraOptsBytes)

	return
}

func validateStableInitialSnapshotCompileProtocol(
	ctx context.Context,
	c *Compile,
	stable bool,
) error {
	protocolVersion := int64(defines.MORPCVersion4)
	if c != nil && c.proc != nil {
		if rt := moruntime.ServiceRuntime(c.proc.GetService()); rt != nil {
			if value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion); ok {
				if version, valid := value.(int64); valid {
					protocolVersion = version
				}
			}
		}
	}
	return cdc.ValidateStableInitialSnapshotProtocol(ctx, stable, protocolVersion)
}

func CDCStrToTime(tsStr string, tz *time.Location) (ts time.Time, err error) {
	if tsStr == "" {
		return
	}

	if tz != nil {
		if ts, err = time.ParseInLocation(
			time.DateTime, tsStr, tz,
		); err == nil {
			return
		}
	}

	ts, err = time.Parse(time.RFC3339, tsStr)
	return
}

func (opts *CDCCreateTaskOptions) handleLevel(
	ctx context.Context,
	c *Compile,
	level string,
	tables string,
) (err error) {
	if level != cdc.CDCPitrGranularity_Account && level != cdc.CDCPitrGranularity_DB && level != cdc.CDCPitrGranularity_Table {
		err = moerr.NewInternalErrorf(ctx, "invalid level: %s", level)
		return
	}
	var patterTupples *cdc.PatternTuples
	if patterTupples, err = CDCParsePitrGranularity(
		ctx, level, tables,
	); err != nil {
		return
	}

	// ensure PITR checks run with the target tenant account id
	ctx = defines.AttachAccountId(ctx, opts.UserInfo.AccountId)
	if err = c.checkPitrGranularity(ctx, patterTupples); err != nil {
		return
	}

	opts.PitrTables, err = cdc.JsonEncode(patterTupples)
	return
}

// only accept positive integers that end with m or h
func isValidFrequency(freq string) bool {
	if !strings.HasSuffix(freq, "m") && !strings.HasSuffix(freq, "h") {
		return false
	}

	numPart := strings.TrimSuffix(strings.TrimSuffix(freq, "m"), "h")

	matched, _ := regexp.MatchString(`^[1-9]\d*$`, numPart)
	if !matched {
		return false
	}

	num, err := strconv.Atoi(numPart)
	if err != nil {
		return false
	}

	if num < 0 {
		return false
	}

	if num > 10000000 {
		return false
	}

	return true
}

// call isValidFrequency before calling this function
func transformIntoHours(freq string) int64 {
	if strings.HasSuffix(freq, "h") {
		hoursStr := strings.TrimSuffix(freq, "h")
		hours, _ := strconv.Atoi(hoursStr)
		return int64(hours)
	}

	if strings.HasSuffix(freq, "m") {
		minStr := strings.TrimSuffix(freq, "m")
		minutes, _ := strconv.Atoi(minStr)

		hours := int(math.Ceil(float64(minutes) / 60.0))

		return int64(hours)
	}

	return 0
}

func (c *Compile) checkPitrGranularity(
	ctx context.Context,
	pts *cdc.PatternTuples,
	minLength ...int64,
) error {
	var minPitrLen int64 = 2
	if len(minLength) > 1 {
		return moerr.NewInternalErrorf(ctx, "only one length parameter allowed")
	}
	if len(minLength) > 0 {
		minPitrLen = max(minLength[0]+1, minPitrLen)
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	sqlCluster := fmt.Sprintf(`SELECT pitr_length,pitr_unit FROM %s.%s WHERE level='cluster' AND account_id = %d`,
		catalog.MO_CATALOG, catalog.MO_PITR, accountId)
	if res, err := c.runSqlWithResultAndOptions(sqlCluster, int32(catalog.System_Account), executor.StatementOption{}.WithDisableLog()); err == nil {
		if len(res.Batches) > 0 && res.Batches[0].Vecs[0].Length() > 0 {
			val := int64(vector.MustFixedColNoTypeCheck[uint8](res.Batches[0].Vecs[0])[0])
			unit := strings.ToLower(res.Batches[0].Vecs[1].GetStringAt(0))
			if toHours(val, unit) >= minPitrLen {
				res.Close()
				return nil
			}
		}
		res.Close()
	}

	for _, pt := range pts.Pts {
		// normalize names to lowercase to match stored mo_pitr values
		dbNameLower := strings.ToLower(pt.Source.Database)
		tblNameLower := strings.ToLower(pt.Source.Table)
		isAccountPattern := pt.Source.Database == cdc.CDCPitrGranularity_All && pt.Source.Table == cdc.CDCPitrGranularity_All
		// helper to run query and evaluate requirement
		checkQuery := func(q string) (bool, error) {
			res, err := c.runSqlWithResultAndOptions(q, int32(catalog.System_Account), executor.StatementOption{}.WithDisableLog())
			if err != nil {
				return false, err
			}
			defer res.Close()
			if len(res.Batches) > 0 && res.Batches[0].Vecs[0].Length() > 0 {
				val := int64(vector.MustFixedColNoTypeCheck[uint8](res.Batches[0].Vecs[0])[0])
				unit := strings.ToLower(res.Batches[0].Vecs[1].GetStringAt(0))
				return toHours(val, unit) >= minPitrLen, nil
			}
			return false, nil
		}

		checkDBByName := func(nameLower string) (bool, error) {
			qDB := getPitrDatabaseGranularitySql(nameLower, accountId, true)
			if ok, err := checkQuery(qDB); err != nil {
				return false, err
			} else if ok {
				return true, nil
			}
			qDB2 := getPitrDatabaseGranularitySql(nameLower, accountId, false)
			return checkQuery(qDB2)
		}

		// 1) account level always checked first for any pattern
		qAcc := fmt.Sprintf(`select pitr_length,pitr_unit from %s.%s where level='account' and account_id = %d`,
			catalog.MO_CATALOG, catalog.MO_PITR, accountId)
		if ok, err := checkQuery(qAcc); err != nil {
			return err
		} else if ok {
			continue
		}

		// determine if db/table
		isDB := pt.Source.Table == cdc.CDCPitrGranularity_All
		// Special handling: account-level pattern ("*.*"). If account-level is not configured,
		// allow a database-level PITR of the current database to satisfy the requirement.
		if isAccountPattern {
			if c.db != "" {
				if ok, err := checkDBByName(strings.ToLower(c.db)); err != nil {
					return err
				} else if ok {
					continue
				}
			}
		}
		// 2) db level if applicable
		if isDB || !isDB { // both db and table pattern can be satisfied by db-level PITR
			if ok, err := checkDBByName(dbNameLower); err != nil {
				return err
			} else if ok {
				continue
			}
		}

		// 3) table level only for table pattern
		if !isDB {
			qTbl := getPitrTableGranularitySql(dbNameLower, tblNameLower, accountId)
			if ok, err := checkQuery(qTbl); err != nil {
				return err
			} else if ok {
				continue
			}
		}

		return moerr.NewInternalErrorf(ctx,
			"PITR config of %s.%s insufficient (<%dh)", pt.Source.Database, pt.Source.Table, minPitrLen)
	}
	return nil
}

func getPitrDatabaseGranularitySql(nameLower string, accountId uint32, withAccount bool) string {
	nameLit := sqlquote.EscapeString(nameLower)
	if withAccount {
		return fmt.Sprintf(`select pitr_length,pitr_unit from %s.%s where level='database' and lower(database_name) = '%s' and account_id = %d`,
			catalog.MO_CATALOG, catalog.MO_PITR, nameLit, accountId)
	}
	return fmt.Sprintf(`select pitr_length,pitr_unit from %s.%s where level='database' and lower(database_name) = '%s'`,
		catalog.MO_CATALOG, catalog.MO_PITR, nameLit)
}

func getPitrTableGranularitySql(dbNameLower, tblNameLower string, accountId uint32) string {
	return fmt.Sprintf(`select pitr_length,pitr_unit from %s.%s where level='table' and lower(database_name)='%s' and lower(table_name)='%s' and account_id = %d`,
		catalog.MO_CATALOG, catalog.MO_PITR,
		sqlquote.EscapeString(dbNameLower), sqlquote.EscapeString(tblNameLower), accountId)
}

func toHours(val int64, unit string) int64 {
	switch unit {
	case "h":
		return val
	case "d":
		return val * 24
	case "mo":
		return val * 24 * 30
	case "y":
		return val * 24 * 365
	default:
		return val
	}
}
func (opts *CDCCreateTaskOptions) handleFrequency(
	ctx context.Context,
	c *Compile,
	level, frequency, tables string,
) (err error) {
	if level != cdc.CDCPitrGranularity_Account && level != cdc.CDCPitrGranularity_DB && level != cdc.CDCPitrGranularity_Table {
		err = moerr.NewInternalErrorf(ctx, "invalid level: %s", level)
		return
	}
	if !isValidFrequency(frequency) {
		return moerr.NewInternalErrorf(ctx, "invalid frequency: %s", frequency)
	}

	normalized := transformIntoHours(frequency)
	var patterTupples *cdc.PatternTuples
	if patterTupples, err = CDCParsePitrGranularity(
		ctx, level, tables,
	); err != nil {
		return
	}

	if err = c.checkPitrGranularity(ctx, patterTupples, normalized); err != nil {
		return err
	}
	return nil
}

func CDCParsePitrGranularity(
	ctx context.Context,
	level string,
	tables string,
) (pts *cdc.PatternTuples, err error) {
	pts = &cdc.PatternTuples{}

	if level == cdc.CDCPitrGranularity_Account {
		pts.Append(&cdc.PatternTuple{
			Source: cdc.PatternTable{
				Database: cdc.CDCPitrGranularity_All,
				Table:    cdc.CDCPitrGranularity_All,
			},
			Sink: cdc.PatternTable{
				Database: cdc.CDCPitrGranularity_All,
				Table:    cdc.CDCPitrGranularity_All,
			},
		})
		return
	}

	// split tables by ',' => table pair
	var pt *cdc.PatternTuple
	tablePairs := strings.Split(strings.TrimSpace(tables), ",")
	dup := make(map[string]struct{})
	for _, pair := range tablePairs {
		if pt, err = CDCParseGranularityTuple(
			ctx, level, pair, dup,
		); err != nil {
			return
		}
		pts.Append(pt)
	}
	return
}

func CDCParseGranularityTuple(
	ctx context.Context,
	level string,
	pattern string,
	dup map[string]struct{},
) (pt *cdc.PatternTuple, err error) {
	splitRes := strings.Split(strings.TrimSpace(pattern), ":")
	if len(splitRes) > 2 {
		err = moerr.NewInternalErrorf(
			ctx, "invalid pattern format: %s, must be `source` or `source:sink`.", pattern,
		)
		return
	}

	pt = &cdc.PatternTuple{OriginString: pattern}

	// handle source part
	if pt.Source.Database, pt.Source.Table, err = CDCParseTableInfo(
		ctx, splitRes[0], level,
	); err != nil {
		return
	}
	key := cdc.GenDbTblKey(pt.Source.Database, pt.Source.Table)
	if _, ok := dup[key]; ok {
		err = moerr.NewInternalErrorf(
			ctx, "one db/table: %s can't be used as multi sources in a cdc task", key,
		)
		return
	}
	dup[key] = struct{}{}

	// handle sink part
	if len(splitRes) > 1 {
		if pt.Sink.Database, pt.Sink.Table, err = CDCParseTableInfo(
			ctx, splitRes[1], level,
		); err != nil {
			return
		}
	} else {
		// if not specify sink, then sink = source
		pt.Sink.Database = pt.Source.Database
		pt.Sink.Table = pt.Source.Table
	}
	return
}

func CDCParseTableInfo(
	ctx context.Context,
	input string,
	level string,
) (db string, table string, err error) {
	parts := strings.Split(strings.TrimSpace(input), ".")
	if level == cdc.CDCPitrGranularity_DB && len(parts) != 1 {
		err = moerr.NewInternalErrorf(ctx, "invalid databases format: %s", input)
		return
	} else if level == cdc.CDCPitrGranularity_Table && len(parts) != 2 {
		err = moerr.NewInternalErrorf(ctx, "invalid tables format: %s", input)
		return
	}

	db = strings.TrimSpace(parts[0])
	if !dbNameIsLegal(db) {
		err = moerr.NewInternalErrorf(ctx, "invalid database name: %s", db)
		return
	}

	if level == cdc.CDCPitrGranularity_Table {
		table = strings.TrimSpace(parts[1])
		if !tableNameIsLegal(table) {
			err = moerr.NewInternalErrorf(ctx, "invalid table name: %s", table)
			return
		}
	} else {
		table = cdc.CDCPitrGranularity_All
	}
	return
}

func dbNameIsLegal(name string) bool {
	name = strings.TrimSpace(name)
	if hasSpecialChars(name) {
		return false
	}
	if name == cdc.CDCPitrGranularity_All {
		return true
	}

	createDBSqls := []string{
		"create database " + name,
		"create database `" + name + "`",
	}
	return isLegal(name, createDBSqls)
}

func tableNameIsLegal(name string) bool {
	name = strings.TrimSpace(name)
	if hasSpecialChars(name) {
		return false
	}
	if name == cdc.CDCPitrGranularity_All {
		return true
	}

	createTableSqls := []string{
		"create table " + name + "(a int)",
		"create table `" + name + "`(a int)",
	}
	return isLegal(name, createTableSqls)
}

func hasSpecialChars(s string) bool {
	return strings.ContainsAny(s, ",.:`")
}

func isLegal(name string, sqls []string) bool {
	name = strings.TrimSpace(name)
	if len(name) == 0 || len(sqls) == 0 {
		return false
	}
	for _, sql := range sqls {
		if len(sql) == 0 {
			return false
		}
	}
	yes := false
	for _, sql := range sqls {
		_, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		if err != nil {
			continue
		}
		yes = true
		break
	}
	return yes
}

// checkCCPRTableBeforeDrop checks if a table can be dropped based on CCPR rules.
// Returns:
//   - true if the table can be dropped
//   - false if the table is a CCPR shared object and cannot be dropped
//   - error if any error occurs
func checkCCPRTableBeforeDrop(c *Compile, tableID uint64) (bool, error) {
	// Query mo_ccpr_tables to check if this table is a CCPR shared table
	querySql := fmt.Sprintf(
		"SELECT taskid FROM `%s`.`%s` WHERE tableid = %d",
		catalog.MO_CATALOG,
		catalog.MO_CCPR_TABLES,
		tableID,
	)

	res, err := c.runSqlWithResult(querySql, int32(catalog.System_Account))
	if err != nil {
		if isMissingCCPRMetadataTable(err, catalog.MO_CCPR_TABLES) {
			return true, nil
		}
		return false, err
	}
	defer res.Close()

	var taskID string
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 {
			// taskid is UUID type, use GetFixedAtWithTypeCheck to read it properly
			taskID = vector.GetFixedAtWithTypeCheck[types.Uuid](cols[0], 0).String()
		}
		return false
	})

	// If not found in mo_ccpr_tables, allow deletion
	if taskID == "" {
		return true, nil
	}

	// Check if the task exists and is not dropped in mo_ccpr_log
	checkTaskSql := fmt.Sprintf(
		"SELECT task_id, drop_at FROM `%s`.`%s` WHERE task_id = '%s'",
		catalog.MO_CATALOG,
		catalog.MO_CCPR_LOG,
		taskID,
	)

	taskRes, err := c.runSqlWithResult(checkTaskSql, int32(catalog.System_Account))
	if err != nil {
		return false, err
	}
	defer taskRes.Close()

	var foundTask bool
	var isDropped bool
	taskRes.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 {
			foundTask = true
			// Check if drop_at is null (task not dropped)
			if !cols[1].IsNull(0) {
				isDropped = true
			}
		}
		return false
	})

	// If task doesn't exist or is dropped, allow deletion and log it
	if !foundTask || isDropped {
		logutil.Info("CCPR: allowing drop of shared table because task is dropped or not found",
			zap.Uint64("tableID", tableID),
			zap.String("taskID", taskID),
			zap.Bool("taskFound", foundTask),
			zap.Bool("isDropped", isDropped))

		// Delete the record from mo_ccpr_tables
		deleteSql := fmt.Sprintf(
			"DELETE FROM `%s`.`%s` WHERE tableid = %d",
			catalog.MO_CATALOG,
			catalog.MO_CCPR_TABLES,
			tableID,
		)
		if err := c.runSqlWithSystemTenant(deleteSql); err != nil {
			logutil.Warn("CCPR: failed to delete record from mo_ccpr_tables",
				zap.Uint64("tableID", tableID),
				zap.Error(err))
		}

		return true, nil
	}

	// Task exists and is not dropped, deny deletion
	return false, nil
}

// checkCCPRDbBeforeDrop checks if a database can be dropped based on CCPR rules.
// Returns:
//   - true if the database can be dropped
//   - false if the database is a CCPR shared object and cannot be dropped
//   - error if any error occurs
func checkCCPRDbBeforeDrop(c *Compile, dbID uint64) (bool, error) {
	// Query mo_ccpr_dbs to check if this database is a CCPR shared database
	querySql := fmt.Sprintf(
		"SELECT taskid FROM `%s`.`%s` WHERE dbid = %d",
		catalog.MO_CATALOG,
		catalog.MO_CCPR_DBS,
		dbID,
	)

	res, err := c.runSqlWithResult(querySql, int32(catalog.System_Account))
	if err != nil {
		if isMissingCCPRMetadataTable(err, catalog.MO_CCPR_DBS) {
			return true, nil
		}
		return false, err
	}
	defer res.Close()

	var taskID string
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 {
			// taskid is UUID type, use GetFixedAtWithTypeCheck to read it properly
			taskID = vector.GetFixedAtWithTypeCheck[types.Uuid](cols[0], 0).String()
		}
		return false
	})

	// If not found in mo_ccpr_dbs, allow deletion
	if taskID == "" {
		return true, nil
	}

	// Check if the task exists and is not dropped in mo_ccpr_log
	checkTaskSql := fmt.Sprintf(
		"SELECT task_id, drop_at FROM `%s`.`%s` WHERE task_id = '%s'",
		catalog.MO_CATALOG,
		catalog.MO_CCPR_LOG,
		taskID,
	)

	taskRes, err := c.runSqlWithResult(checkTaskSql, int32(catalog.System_Account))
	if err != nil {
		return false, err
	}
	defer taskRes.Close()

	var foundTask bool
	var isDropped bool
	taskRes.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 {
			foundTask = true
			// Check if drop_at is null (task not dropped)
			if !cols[1].IsNull(0) {
				isDropped = true
			}
		}
		return false
	})

	// If task doesn't exist or is dropped, allow deletion and log it
	if !foundTask || isDropped {
		logutil.Info("CCPR: allowing drop of shared database because task is dropped or not found",
			zap.Uint64("dbID", dbID),
			zap.String("taskID", taskID),
			zap.Bool("taskFound", foundTask),
			zap.Bool("isDropped", isDropped))

		// Delete the record from mo_ccpr_dbs
		deleteSql := fmt.Sprintf(
			"DELETE FROM `%s`.`%s` WHERE dbid = %d",
			catalog.MO_CATALOG,
			catalog.MO_CCPR_DBS,
			dbID,
		)
		if err := c.runSqlWithSystemTenant(deleteSql); err != nil {
			logutil.Warn("CCPR: failed to delete record from mo_ccpr_dbs",
				zap.Uint64("dbID", dbID),
				zap.Error(err))
		}

		return true, nil
	}

	// Task exists and is not dropped, deny deletion
	return false, nil
}

func isMissingCCPRMetadataTable(err error, tableName string) bool {
	if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
		return true
	}
	if !moerr.IsMoErrCode(err, moerr.ErrParseError) {
		return false
	}
	// Query planning currently reports a missing resolved table as ErrParseError.
	return strings.Contains(err.Error(), fmt.Sprintf("table %q does not exist", tableName))
}
