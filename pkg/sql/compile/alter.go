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
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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

func buildAlterDataBranchLineageSQL(
	oldTableID, newTableID uint64,
	cloneTS int64,
	creator uint32,
	lineageLevel, accountName, databaseName, tableName, snapshotID string,
) (metadataSQL, snapshotSQL string) {
	metadataSQL = fmt.Sprintf(
		"insert into %s.%s values(%d, %d, %d, %d, '%s', false)",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
		newTableID, cloneTS, oldTableID, creator, sqlquote.EscapeString(lineageLevel),
	)
	snapshotSQL = fmt.Sprintf(
		`insert into %s.%s(snapshot_id, sname, ts, level, account_name, database_name, table_name, obj_id, kind) `+
			`values ('%s', '%s', %d, 'table', '%s', '%s', '%s', %d, '%s')`,
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS,
		sqlquote.EscapeString(snapshotID),
		databranchutils.BranchSnapshotName(newTableID),
		cloneTS,
		sqlquote.EscapeString(accountName),
		sqlquote.EscapeString(databaseName),
		sqlquote.EscapeString(tableName),
		oldTableID,
		databranchutils.BranchSnapshotKind,
	)
	return
}

type alterDataBranchLineagePlan struct {
	enabled                  bool
	preserveHistoricalSource bool
	cloneTS                  int64
	fixedCopyTS              bool
}

func alterCopySQLAtLineageSnapshot(sql string, plan alterDataBranchLineagePlan) string {
	if !plan.enabled || !plan.fixedCopyTS {
		return sql
	}
	return sql + fmt.Sprintf(" {MO_TS = %d}", plan.cloneTS)
}

func isExplicitAlterTxn(byBegin, autocommit bool) bool {
	return byBegin || !autocommit
}

func shouldUseFixedAlterCopySnapshot(snapshotAdvanced, txnHasWorkspaceHistory bool) bool {
	return snapshotAdvanced && !txnHasWorkspaceHistory
}

func alterDataBranchParticipationSQL(oldTableID uint64) string {
	return fmt.Sprintf(
		"select 1 from %s.%s where table_id = %d or p_table_id = %d limit 1",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, oldTableID, oldTableID,
	)
}

func alterDataBranchHistoricalSourceScopeSQL(
	accountName, databaseName, tableName string,
	tableID uint64,
) string {
	accountName = sqlquote.EscapeString(accountName)
	databaseName = sqlquote.EscapeString(databaseName)
	tableName = sqlquote.EscapeString(tableName)
	return fmt.Sprintf(
		`(level = 'cluster' or (`+
			`account_name = '%s' and (`+
			`level = 'account' or `+
			`(level = 'database' and database_name = '%s') or `+
			`(level = 'table' and (obj_id = %d or (database_name = '%s' and table_name = '%s')))`+
			`)))`,
		accountName, databaseName, tableID, databaseName, tableName,
	)
}

func alterDataBranchHistoricalSnapshotSourceSQL(
	accountName, databaseName, tableName string,
	tableID uint64,
) string {
	return alterDataBranchHistoricalSnapshotSourceProbeSQL(
		accountName, databaseName, tableName, tableID, true,
	)
}

func alterDataBranchHistoricalSnapshotSourceProbeSQL(
	accountName, databaseName, tableName string,
	tableID uint64,
	forUpdate bool,
) string {
	lockClause := ""
	if forUpdate {
		lockClause = " for update"
	}
	return fmt.Sprintf(
		"select 1 from %s.%s where kind = 'user' and %s limit 1%s",
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS,
		alterDataBranchHistoricalSourceScopeSQL(accountName, databaseName, tableName, tableID),
		lockClause,
	)
}

func alterDataBranchHistoricalPitrSourceSQL(
	accountName, databaseName, tableName string,
	tableID uint64,
) string {
	return alterDataBranchHistoricalPitrSourceProbeSQL(
		accountName, databaseName, tableName, tableID, true,
	)
}

func alterDataBranchHistoricalPitrSourceProbeSQL(
	accountName, databaseName, tableName string,
	tableID uint64,
	forUpdate bool,
) string {
	lockClause := ""
	if forUpdate {
		lockClause = " for update"
	}
	return fmt.Sprintf(
		"select 1 from %s.%s where pitr_status = 1 and %s limit 1%s",
		catalog.MO_CATALOG, catalog.MO_PITR,
		alterDataBranchHistoricalSourceScopeSQL(accountName, databaseName, tableName, tableID),
		lockClause,
	)
}

func alterDataBranchHistoricalSourceExists(
	query alterDataBranchQuery,
	sqls []string,
) (bool, error) {
	for _, sql := range sqls {
		res, err := query(sql)
		if err != nil {
			res.Close()
			return false, err
		}
		hasHistory := false
		res.ReadRows(func(rows int, _ []*vector.Vector) bool {
			hasHistory = rows > 0
			return false
		})
		res.Close()
		if hasHistory {
			return true, nil
		}
	}
	return false, nil
}

func (c *Compile) alterTableParticipatesInDataBranch(oldTableID uint64) (bool, error) {
	probeSQL := alterDataBranchParticipationSQL(oldTableID)
	res, err := c.runSqlWithResult(probeSQL, int32(catalog.System_Account))
	if err != nil {
		return false, err
	}
	participates := false
	res.ReadRows(func(rows int, _ []*vector.Vector) bool {
		participates = rows > 0
		return false
	})
	res.Close()
	return participates, nil
}

func (c *Compile) alterTableHasHistoricalBranchSource(
	oldTableID uint64,
	databaseName, tableName string,
) (bool, error) {
	return alterDataBranchHistoricalSourceExists(
		func(sql string) (executor.Result, error) {
			return c.runSqlWithResult(sql, int32(catalog.System_Account))
		},
		[]string{
			alterDataBranchHistoricalSnapshotSourceSQL(
				c.proc.GetSessionInfo().Account, databaseName, tableName, oldTableID,
			),
			alterDataBranchHistoricalPitrSourceSQL(
				c.proc.GetSessionInfo().Account, databaseName, tableName, oldTableID,
			),
		},
	)
}

func (c *Compile) alterTableHasLatestHistoricalBranchSource(
	oldTableID uint64,
	databaseName, tableName string,
) (hasHistory bool, err error) {
	v, ok := moruntime.ServiceRuntime(c.proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return false, moerr.NewInternalErrorNoCtx("missing internal SQL executor")
	}
	exec := v.(executor.SQLExecutor)
	ctx := c.proc.Ctx
	if ctx == nil {
		ctx = c.proc.GetTopContext()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	accountName := c.proc.GetSessionInfo().Account
	err = exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		hasHistory, err = alterDataBranchHistoricalSourceExists(
			func(sql string) (executor.Result, error) {
				return txn.Exec(sql, executor.StatementOption{}.WithAccountID(catalog.System_Account))
			},
			[]string{
				alterDataBranchHistoricalSnapshotSourceProbeSQL(
					accountName, databaseName, tableName, oldTableID, false,
				),
				alterDataBranchHistoricalPitrSourceProbeSQL(
					accountName, databaseName, tableName, oldTableID, false,
				),
			},
		)
		return err
	}, executor.Options{}.WithAccountID(catalog.System_Account))
	return hasHistory, err
}

func (c *Compile) lockDataBranchLineageOwnerLifecycle() error {
	return databranchutils.LockLineageOwnerLifecycle(func(sql string) error {
		return c.runSqlWithAccountId(sql, int32(catalog.System_Account))
	})
}

func (c *Compile) prepareAlterDataBranchLineage(
	oldTableID uint64,
	databaseName, tableName string,
	statement string,
) (alterDataBranchLineagePlan, error) {
	participates, err := c.alterTableParticipatesInDataBranch(oldTableID)
	if err != nil {
		return alterDataBranchLineagePlan{}, err
	}
	hasLiveLineage := false
	if participates {
		// ALTER-only rows preserve physical history for a snapshot or PITR but
		// are not logical data branches. Inspect the complete connected
		// component so an ALTER generation neither triggers a false transaction
		// restriction nor hides a live logical sibling behind an ancestor.
		ownershipDAG, dagErr := c.loadAlterDataBranchDAG(false)
		if dagErr != nil {
			return alterDataBranchLineagePlan{}, dagErr
		}
		if ownershipDAG.ComponentHasLiveLogicalBranch(oldTableID) {
			op := c.proc.GetTxnOperator()
			opts := op.TxnOptions()
			if err = validateAlterDataBranchLineageTxn(
				statement, opts.GetByBegin(), opts.GetAutocommit(), op.Txn().IsPessimistic(),
			); err != nil {
				return alterDataBranchLineagePlan{}, err
			}
		}
		if err = c.compactExpiredAlterDataBranchLineage(time.Time{}); err != nil {
			return alterDataBranchLineagePlan{}, err
		}
		dag, dagErr := c.loadAlterDataBranchDAG(false)
		if dagErr != nil {
			return alterDataBranchLineagePlan{}, dagErr
		}
		hasLiveLineage = dag.SubtreeHasLiveNode(oldTableID)
	}
	preserveHistoricalSource := false
	if !hasLiveLineage {
		if preserveHistoricalSource, err = c.alterTableHasHistoricalBranchSource(
			oldTableID, databaseName, tableName,
		); err != nil {
			return alterDataBranchLineagePlan{}, err
		}
		if !preserveHistoricalSource {
			return alterDataBranchLineagePlan{}, nil
		}
	}

	return alterDataBranchLineagePlan{
		enabled:                  true,
		preserveHistoricalSource: preserveHistoricalSource,
	}, nil
}

func validateAlterDataBranchLineageTxn(statement string, byBegin, autocommit, _ bool) error {
	if isExplicitAlterTxn(byBegin, autocommit) {
		return moerr.NewNotSupportedNoCtxf(
			"%s on a data-branch lineage is not supported inside an explicit transaction", statement,
		)
	}
	return nil
}

func shouldAdvanceAlterDataBranchLineageSnapshot(pessimistic, rcIsolation bool) bool {
	return pessimistic && rcIsolation
}

func (c *Compile) advanceAlterDataBranchLineageSnapshot() (int64, error) {
	op := c.proc.GetTxnOperator()
	physicalTime := op.SnapshotTS().PhysicalTime
	if physicalTime > math.MaxInt64-int64(time.Microsecond) {
		return 0, moerr.NewInternalErrorNoCtx(
			"cannot advance ALTER data-branch lineage snapshot past the timestamp limit",
		)
	}
	requested := physicalTime + int64(time.Microsecond)
	if err := op.UpdateSnapshot(c.proc.Ctx, timestamp.Timestamp{PhysicalTime: requested}); err != nil {
		return 0, err
	}
	updated := op.SnapshotTS().PhysicalTime
	if updated <= requested {
		return 0, moerr.NewInternalErrorNoCtx(
			"failed to advance ALTER data-branch lineage snapshot",
		)
	}
	return updated - int64(time.Nanosecond), nil
}

type alterDataBranchQuery func(string) (executor.Result, error)

func loadAlterDataBranchDAGWithQuery(
	query alterDataBranchQuery,
	forUpdate bool,
) (databranchutils.BranchReclaimDag, error) {
	suffix := ""
	if forUpdate {
		suffix = " for update"
	}
	res, err := query(fmt.Sprintf(
		"select table_id, p_table_id, clone_ts, creator, level, table_deleted from %s.%s%s",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, suffix,
	))
	if err != nil {
		return databranchutils.BranchReclaimDag{}, err
	}
	defer res.Close()
	rows := make([]databranchutils.DataBranchMetadata, 0, res.AffectedRows)
	res.ReadRows(func(rowCount int, cols []*vector.Vector) bool {
		if rowCount == 0 {
			return true
		}
		tableIDs := vector.MustFixedColNoTypeCheck[uint64](cols[0])
		parentIDs := vector.MustFixedColNoTypeCheck[uint64](cols[1])
		cloneTSs := vector.MustFixedColNoTypeCheck[int64](cols[2])
		creators := vector.MustFixedColNoTypeCheck[uint64](cols[3])
		levels := executor.GetStringRows(cols[4])
		deleted := vector.MustFixedColNoTypeCheck[bool](cols[5])
		for i := range tableIDs {
			rows = append(rows, databranchutils.DataBranchMetadata{
				TableID:      tableIDs[i],
				PTableID:     parentIDs[i],
				CloneTS:      cloneTSs[i],
				Creator:      creators[i],
				Level:        levels[i],
				TableDeleted: deleted[i],
			})
		}
		return true
	})
	return databranchutils.NewBranchReclaimDag(rows), nil
}

func (c *Compile) loadAlterDataBranchDAG(forUpdate bool) (databranchutils.BranchReclaimDag, error) {
	return loadAlterDataBranchDAGWithQuery(func(sql string) (executor.Result, error) {
		return c.runSqlWithResult(sql, int32(catalog.System_Account))
	}, forUpdate)
}

func loadAlterDataBranchLineageEdgesWithQuery(
	query alterDataBranchQuery,
) (map[uint64]databranchutils.HistoricalLineageEdge, error) {
	res, err := query(alterDataBranchLineageEdgeSQL())
	if err != nil {
		return nil, err
	}
	defer res.Close()
	edges := make(map[uint64]databranchutils.HistoricalLineageEdge, res.AffectedRows)
	res.ReadRows(func(rowCount int, cols []*vector.Vector) bool {
		if rowCount == 0 {
			return true
		}
		names := executor.GetStringRows(cols[0])
		cloneTSs := vector.MustFixedColNoTypeCheck[int64](cols[1])
		accounts := executor.GetStringRows(cols[2])
		databases := executor.GetStringRows(cols[3])
		tables := executor.GetStringRows(cols[4])
		parentIDs := vector.MustFixedColNoTypeCheck[uint64](cols[5])
		for i, name := range names {
			childID, ok := databranchutils.ParseBranchSnapshotName(name)
			if !ok {
				continue
			}
			edges[childID] = databranchutils.HistoricalLineageEdge{
				ChildTableID:  childID,
				ParentTableID: parentIDs[i],
				CloneTS:       cloneTSs[i],
				AccountName:   accounts[i],
				DatabaseName:  databases[i],
				TableName:     tables[i],
			}
		}
		return true
	})
	return edges, nil
}

func alterDataBranchLineageEdgeSQL() string {
	return fmt.Sprintf(
		"select sname, ts, account_name, database_name, table_name, obj_id from %s.%s where kind = '%s'",
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, databranchutils.BranchSnapshotKind,
	)
}

func alterDataBranchSnapshotSourceSQL() string {
	return fmt.Sprintf(
		"select ts, level, account_name, database_name, table_name, obj_id from %s.%s where kind = 'user'",
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS,
	)
}

func alterDataBranchPitrSourceSQL() string {
	return fmt.Sprintf(
		"select level, account_name, database_name, table_name, obj_id, pitr_length, pitr_unit from %s.%s where pitr_status = 1",
		catalog.MO_CATALOG, catalog.MO_PITR,
	)
}

func (c *Compile) loadAlterDataBranchLineageEdges() (
	map[uint64]databranchutils.HistoricalLineageEdge,
	error,
) {
	return loadAlterDataBranchLineageEdgesWithQuery(func(sql string) (executor.Result, error) {
		return c.runSqlWithResult(sql, int32(catalog.System_Account))
	})
}

func appendAlterDataBranchHistoricalSources(
	res executor.Result,
	oldestTS func(int, []*vector.Vector) (int64, error),
	columnOffset int,
	sources *[]databranchutils.HistoricalSource,
) error {
	var loadErr error
	res.ReadRows(func(rowCount int, cols []*vector.Vector) bool {
		if rowCount == 0 {
			return true
		}
		levels := executor.GetStringRows(cols[columnOffset])
		accounts := executor.GetStringRows(cols[columnOffset+1])
		databases := executor.GetStringRows(cols[columnOffset+2])
		tables := executor.GetStringRows(cols[columnOffset+3])
		objectIDs := vector.MustFixedColNoTypeCheck[uint64](cols[columnOffset+4])
		for i := range levels {
			lowerBound, err := oldestTS(i, cols)
			if err != nil {
				loadErr = err
				return false
			}
			*sources = append(*sources, databranchutils.HistoricalSource{
				Level:        levels[i],
				AccountName:  accounts[i],
				DatabaseName: databases[i],
				TableName:    tables[i],
				ObjectID:     objectIDs[i],
				OldestTS:     lowerBound,
			})
		}
		return true
	})
	return loadErr
}

func loadAlterDataBranchHistoricalSourcesWithQuery(
	query alterDataBranchQuery,
	now time.Time,
) ([]databranchutils.HistoricalSource, error) {
	res, err := query(alterDataBranchSnapshotSourceSQL())
	if err != nil {
		return nil, err
	}
	var sources []databranchutils.HistoricalSource
	err = appendAlterDataBranchHistoricalSources(
		res,
		func(i int, cols []*vector.Vector) (int64, error) {
			return vector.MustFixedColNoTypeCheck[int64](cols[0])[i], nil
		},
		1,
		&sources,
	)
	res.Close()
	if err != nil {
		return nil, err
	}

	res, err = query(alterDataBranchPitrSourceSQL())
	if err != nil {
		return nil, err
	}
	err = appendAlterDataBranchHistoricalSources(
		res,
		func(i int, cols []*vector.Vector) (int64, error) {
			lengths := vector.MustFixedColWithTypeCheck[uint8](cols[5])
			units := executor.GetStringRows(cols[6])
			return databranchutils.PitrRetentionLowerBound(now, int(lengths[i]), units[i])
		},
		0,
		&sources,
	)
	res.Close()
	if err != nil {
		return nil, err
	}
	return sources, nil
}

func (c *Compile) loadAlterDataBranchHistoricalSources(
	now time.Time,
) ([]databranchutils.HistoricalSource, error) {
	return loadAlterDataBranchHistoricalSourcesWithQuery(
		func(sql string) (executor.Result, error) {
			return c.runSqlWithResult(sql, int32(catalog.System_Account))
		},
		now,
	)
}

// compactExpiredAlterDataBranchLineage is ALTER's opportunistic expiry
// hook. DROP paths compact synchronously, but an active PITR can also stop
// covering an edge merely because its rolling retention window advances.
// Locking metadata first keeps the edge/snapshot pair atomic with the ALTER
// that will immediately decide whether to append a new edge.
func (c *Compile) compactExpiredAlterDataBranchLineage(now time.Time) error {
	dag, err := c.loadAlterDataBranchDAG(true)
	if err != nil {
		return err
	}
	if len(dag.Info) == 0 {
		return nil
	}
	if now.IsZero() {
		now = c.proc.GetTxnOperator().SnapshotTS().ToStdTime().UTC()
	}
	edges, err := c.loadAlterDataBranchLineageEdges()
	if err != nil {
		return err
	}
	sources, err := c.loadAlterDataBranchHistoricalSources(now)
	if err != nil {
		return err
	}
	plan := databranchutils.ComputeAlterLineageCompactionPlan(dag, edges, sources)
	if len(plan.TableIDs) == 0 {
		return nil
	}
	if err = c.runSqlWithSystemTenant(
		databranchutils.BuildAlterLineageSnapshotDeleteSQL(plan.SnapshotNames),
	); err != nil {
		return err
	}
	return c.runSqlWithSystemTenant(
		databranchutils.BuildAlterLineageMetadataDeleteSQL(plan.TableIDs),
	)
}

func alterDataBranchLineageMetadata(
	dag databranchutils.BranchReclaimDag,
	oldTableID uint64,
) (creator uint32, level string) {
	meta, ok := dag.Info[oldTableID]
	if !ok || meta.Deleted {
		return catalog.System_Account, databranchutils.AlterLineageLevel
	}
	return uint32(meta.Creator), databranchutils.NextAlterLineageLevel(meta.Level)
}

// preserveAlterDataBranchLineage models ALTER's copy-and-swap as a lineage
// edge whenever the old physical table has a live branch descendant. The
// matching snapshot pins the old generation until every later generation and
// descendant has been dropped.
func (c *Compile) preserveAlterDataBranchLineage(
	plan alterDataBranchLineagePlan,
	oldTableID, newTableID uint64,
	databaseName, tableName string,
) error {
	if !plan.enabled {
		participates, err := c.alterTableParticipatesInDataBranch(oldTableID)
		if err != nil || !participates {
			return err
		}
	}
	dag, err := c.loadAlterDataBranchDAG(true)
	if err != nil {
		return err
	}
	if !dag.SubtreeHasLiveNode(oldTableID) && !plan.preserveHistoricalSource {
		return nil
	}
	if !plan.enabled {
		return moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
	}

	snapshotID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	creator, lineageLevel := alterDataBranchLineageMetadata(dag, oldTableID)
	metadataSQL, snapshotSQL := buildAlterDataBranchLineageSQL(
		oldTableID, newTableID, plan.cloneTS, creator, lineageLevel,
		c.proc.GetSessionInfo().Account, databaseName, tableName, snapshotID.String(),
	)
	if err = c.runSqlWithSystemTenant(metadataSQL); err != nil {
		return err
	}
	if err = c.runSqlWithSystemTenant(snapshotSQL); err != nil {
		return err
	}
	logutil.Info("DataBranch-Alter-Lineage-Preserved",
		zap.Uint64("old-table-id", oldTableID),
		zap.Uint64("new-table-id", newTableID),
		zap.Int64("clone-ts", plan.cloneTS),
	)
	return nil
}

func convertDBEOB(ctx context.Context, e error, name string) error {
	if moerr.IsMoErrCode(e, moerr.OkExpectedEOB) {
		return moerr.NewBadDB(ctx, name)
	}
	return e
}

func convertDBEOBToNoSuchTable(ctx context.Context, e error, dbName, tblName string) error {
	if moerr.IsMoErrCode(e, moerr.OkExpectedEOB) {
		return moerr.NewNoSuchTable(ctx, dbName, tblName)
	}
	return e
}

type alterAutoIncrementResetCleanup struct {
	c        *Compile
	tableIDs []uint64
	tracked  map[uint64]struct{}
}

func newAlterAutoIncrementResetCleanup(c *Compile) *alterAutoIncrementResetCleanup {
	return &alterAutoIncrementResetCleanup{
		c:       c,
		tracked: make(map[uint64]struct{}),
	}
}

func (cleanup *alterAutoIncrementResetCleanup) track(tableID uint64) {
	if _, ok := cleanup.tracked[tableID]; ok {
		return
	}
	cleanup.tracked[tableID] = struct{}{}
	cleanup.tableIDs = append(cleanup.tableIDs, tableID)
}

func (cleanup *alterAutoIncrementResetCleanup) finish(statementErr *error) {
	if *statementErr == nil && cleanup.c.proc.Ctx != nil {
		*statementErr = cleanup.c.proc.Ctx.Err()
	}
	if *statementErr == nil || len(cleanup.tableIDs) == 0 {
		return
	}

	ctx := cleanup.c.proc.Ctx
	if ctx == nil {
		ctx = context.Background()
	} else {
		ctx = context.WithoutCancel(ctx)
	}
	svc := incrservice.GetAutoIncrementService(cleanup.c.proc.GetService())
	var cleanupErr error
	for _, tableID := range cleanup.tableIDs {
		cleanupErr = errors.Join(
			cleanupErr,
			svc.DiscardOffsetReset(ctx, tableID, cleanup.c.proc.GetTxnOperator()),
		)
	}
	if cleanupErr == nil {
		return
	}
	if _, ok := (*statementErr).(*moerr.Error); ok {
		cleanup.c.proc.Error(
			ctx,
			"alter.table.discard.auto.increment.reset",
			zap.Error(cleanupErr),
		)
		return
	}
	*statementErr = errors.Join(*statementErr, cleanupErr)
}

func shouldEnableAlterCopyPipelineFlush(opt *plan.AlterCopyOpt) bool {
	return opt != nil && opt.SkipPkDedup
}

func isAlterAffectedPluginIndex(indexDef *plan.IndexDef, affected []string) bool {
	if indexDef == nil || len(affected) == 0 {
		return false
	}
	if slices.Contains(affected, indexDef.IndexName) {
		return true
	}
	for _, part := range indexDef.Parts {
		if isAlterAffectedColumnName(affected, part) {
			return true
		}
	}
	for _, col := range indexDef.IncludedColumns {
		if isAlterAffectedColumnName(affected, col) {
			return true
		}
	}
	return false
}

func isAlterAffectedColumnName(affected []string, name string) bool {
	if slices.Contains(affected, name) {
		return true
	}
	resolved := catalog.ResolveAlias(name)
	return resolved != name && slices.Contains(affected, resolved)
}

func alterCopyStatementOption(alterOpt *plan.AlterCopyOpt) executor.StatementOption {
	opt := executor.StatementOption{}
	if alterOpt != nil &&
		(alterOpt.SkipPkDedup || len(alterOpt.SkipUniqueIdxDedup) > 0) {
		opt = opt.WithAlterCopyOpt(alterOpt)
	}
	return opt
}

func alterCopyPkPrecheckColumns(tableDef *plan.TableDef) []string {
	if tableDef == nil || tableDef.GetPkey() == nil {
		return nil
	}
	pk := tableDef.GetPkey()
	if len(pk.GetNames()) > 0 {
		return slices.DeleteFunc(slices.Clone(pk.GetNames()), func(name string) bool { return name == "" })
	}

	pkColName := pk.GetPkeyColName()
	if pkColName == "" || catalog.IsFakePkName(pkColName) || pkColName == catalog.CPrimaryKeyColName {
		return nil
	}
	return []string{pkColName}
}

func alterCopyPkColumnValueUnchanged(oldCol, newCol *plan.ColDef) bool {
	if oldCol == nil || newCol == nil {
		return false
	}
	// Generated keys are recomputed by the copy INSERT and can change when a
	// dependency changes even if the generated column's own type is unchanged.
	// Source-side prechecks therefore cannot prove target-key uniqueness.
	if oldCol.GetGeneratedCol() != nil || newCol.GetGeneratedCol() != nil {
		return false
	}
	oldTyp := oldCol.GetTyp()
	newTyp := newCol.GetTyp()
	return oldTyp.GetId() == newTyp.GetId() &&
		oldTyp.GetAutoIncr() == newTyp.GetAutoIncr() &&
		oldTyp.GetWidth() == newTyp.GetWidth() &&
		oldTyp.GetScale() == newTyp.GetScale() &&
		oldTyp.GetTable() == newTyp.GetTable() &&
		oldTyp.GetEnumvalues() == newTyp.GetEnumvalues()
}

// Only precheck source rows when the copied PK columns keep value-preserving
// definitions. If ALTER changes the key value during copy, insert-time dedup
// must remain enabled for the target table.
func getAlterCopyPkPrecheck(qry *plan.AlterTable) (pkCols []string, checkNotNull bool) {
	if qry == nil || qry.Options == nil || qry.Options.GetSkipPkDedup() {
		return nil, false
	}

	pkCols = alterCopyPkPrecheckColumns(qry.CopyTableDef)
	if len(pkCols) == 0 {
		return nil, false
	}
	for _, colName := range pkCols {
		oldCol := plan2.FindColumn(qry.GetTableDef().GetCols(), colName)
		newCol := plan2.FindColumn(qry.CopyTableDef.GetCols(), colName)
		if !alterCopyPkColumnValueUnchanged(oldCol, newCol) {
			return nil, false
		}
		// ChangeTblColIdMap is the compiler-side ownership proof that the target
		// key is populated from this old column. Without it, a same-name DROP/ADD
		// may populate every target row from one default value; checking the old
		// column for duplicates would then say nothing about the copied key.
		mappedCol, ok := qry.ChangeTblColIdMap[oldCol.ColId]
		if !ok || mappedCol == nil || !strings.EqualFold(mappedCol.Name, newCol.Name) {
			return nil, false
		}
		if !oldCol.GetNotNull() && !oldCol.GetTyp().NotNullable {
			checkNotNull = true
		}
	}
	return pkCols, checkNotNull
}

func alterCopySameStatementColumnReplacement(qry *plan.AlterTable) (string, bool) {
	if qry == nil || qry.TableDef == nil || qry.CopyTableDef == nil {
		return "", false
	}
	for _, oldCol := range qry.TableDef.Cols {
		if oldCol == nil || oldCol.Hidden {
			continue
		}
		newCol := plan2.FindColumn(qry.CopyTableDef.Cols, oldCol.Name)
		if newCol != nil &&
			(oldCol.ColId != newCol.ColId || oldCol.Seqnum != newCol.Seqnum) {
			return newCol.Name, true
		}
	}

	removed := false
	for _, oldCol := range qry.TableDef.Cols {
		if oldCol == nil || oldCol.Hidden {
			continue
		}
		if _, ok := qry.ChangeTblColIdMap[oldCol.ColId]; !ok {
			removed = true
		}
	}
	if !removed {
		return "", false
	}
	for _, newCol := range qry.CopyTableDef.Cols {
		if newCol == nil || newCol.Hidden {
			continue
		}
		inherited := false
		for _, mappedCol := range qry.ChangeTblColIdMap {
			if mappedCol != nil && strings.EqualFold(mappedCol.Name, newCol.Name) {
				inherited = true
				break
			}
		}
		if !inherited {
			return newCol.Name, true
		}
	}
	return "", false
}

func quoteAlterCopyIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func quoteAlterCopyTableName(dbName, tblName string) string {
	return quoteAlterCopyIdentifier(dbName) + "." + quoteAlterCopyIdentifier(tblName)
}

func buildAlterCopyPkNullCheckSQL(dbName, tblName string, pkCols []string) string {
	selectCols := make([]string, 0, len(pkCols))
	nullPredicates := make([]string, 0, len(pkCols))
	for _, col := range pkCols {
		quotedCol := quoteAlterCopyIdentifier(col)
		selectCols = append(selectCols, quotedCol)
		nullPredicates = append(nullPredicates, quotedCol+" IS NULL")
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1",
		strings.Join(selectCols, ", "),
		quoteAlterCopyTableName(dbName, tblName),
		strings.Join(nullPredicates, " OR "),
	)
}

func buildAlterCopyPkDuplicateCheckSQL(dbName, tblName string, pkCols []string) string {
	groupByCols := make([]string, 0, len(pkCols))
	for _, col := range pkCols {
		groupByCols = append(groupByCols, quoteAlterCopyIdentifier(col))
	}
	groupBy := strings.Join(groupByCols, ", ")
	return fmt.Sprintf("SELECT %s FROM %s GROUP BY %s HAVING count(*) > 1 LIMIT 1",
		groupBy,
		quoteAlterCopyTableName(dbName, tblName),
		groupBy,
	)
}

func firstAlterCopyResultRow(res executor.Result, colCount int) ([]string, []bool, bool) {
	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}

		values := make([]string, colCount)
		nulls := make([]bool, colCount)
		for i := 0; i < colCount; i++ {
			if i >= len(bat.Vecs) || bat.Vecs[i] == nil || bat.Vecs[i].Length() == 0 {
				continue
			}
			nulls[i] = bat.Vecs[i].IsNull(0)
			if nulls[i] {
				values[i] = "null"
				continue
			}
			values[i] = bat.Vecs[i].RowToString(0)
		}
		return values, nulls, true
	}
	return nil, nil, false
}

func formatAlterCopyPkValue(values []string) string {
	if len(values) == 1 {
		return values[0]
	}
	return "(" + strings.Join(values, ",") + ")"
}

func alterCopyDedupColName(pkCols []string) string {
	if len(pkCols) == 1 {
		return pkCols[0]
	}
	return "(" + strings.Join(pkCols, ",") + ")"
}

func cloneAlterCopyOpt(opt *plan.AlterCopyOpt) *plan.AlterCopyOpt {
	if opt == nil {
		return nil
	}
	clone := *opt
	if opt.SkipUniqueIdxDedup != nil {
		clone.SkipUniqueIdxDedup = make(map[string]bool, len(opt.SkipUniqueIdxDedup))
		for k, v := range opt.SkipUniqueIdxDedup {
			clone.SkipUniqueIdxDedup[k] = v
		}
	}
	if opt.SkipIndexesCopy != nil {
		clone.SkipIndexesCopy = make(map[string]bool, len(opt.SkipIndexesCopy))
		for k, v := range opt.SkipIndexesCopy {
			clone.SkipIndexesCopy[k] = v
		}
	}
	return &clone
}

func (c *Compile) precheckAlterCopyPkDedup(dbName, tblName string, qry *plan.AlterTable) (*plan.AlterCopyOpt, error) {
	if qry == nil || qry.Options == nil {
		return nil, nil
	}
	if qry.Options.GetSkipPkDedup() {
		return qry.Options, nil
	}

	pkCols, checkNotNull := getAlterCopyPkPrecheck(qry)
	if len(pkCols) == 0 {
		return qry.Options, nil
	}

	// Prove PK validity on the source snapshot first, then let insert-copy avoid
	// building the target-side PK dedup hash table for the full backfill.
	if checkNotNull {
		nullCheckSQL := buildAlterCopyPkNullCheckSQL(dbName, tblName, pkCols)
		nullCheckRes, err := c.runSqlWithResultAndOptions(nullCheckSQL, NoAccountId, executor.StatementOption{}.WithDisableLog())
		if err != nil {
			c.proc.Errorf(c.proc.Ctx, "alter copy primary key null check failed, sql is %s", nullCheckSQL)
			return nil, err
		}
		defer nullCheckRes.Close()

		if _, nulls, ok := firstAlterCopyResultRow(nullCheckRes, len(pkCols)); ok {
			for i, isNull := range nulls {
				if isNull {
					return nil, moerr.NewConstraintViolation(c.proc.Ctx, fmt.Sprintf("Column '%s' cannot be null", pkCols[i]))
				}
			}
			return nil, moerr.NewConstraintViolation(c.proc.Ctx, fmt.Sprintf("Column '%s' cannot be null", pkCols[0]))
		}
	}

	duplicateCheckSQL := buildAlterCopyPkDuplicateCheckSQL(dbName, tblName, pkCols)
	duplicateCheckRes, err := c.runSqlWithResultAndOptions(duplicateCheckSQL, NoAccountId, executor.StatementOption{}.WithDisableLog())
	if err != nil {
		c.proc.Errorf(c.proc.Ctx, "alter copy primary key duplicate check failed, sql is %s", duplicateCheckSQL)
		return nil, err
	}
	defer duplicateCheckRes.Close()

	if values, _, ok := firstAlterCopyResultRow(duplicateCheckRes, len(pkCols)); ok {
		return nil, moerr.NewDuplicateEntry(c.proc.Ctx, formatAlterCopyPkValue(values), alterCopyDedupColName(pkCols))
	}

	opt := cloneAlterCopyOpt(qry.Options)
	if opt.TargetTableName == "" {
		opt.TargetTableName = qry.CopyTableDef.GetName()
	}
	opt.SkipPkDedup = true
	return opt, nil
}

func (s *Scope) AlterTableCopy(c *Compile) (err error) {
	cleanup := newAlterAutoIncrementResetCleanup(c)
	defer cleanup.finish(&err)
	return s.alterTableCopy(c, cleanup)
}

func (s *Scope) alterTableCopy(c *Compile, cleanup *alterAutoIncrementResetCleanup) error {
	qry := s.Plan.GetDdl().GetAlterTable()
	dbName := qry.Database

	if dbName == "" {
		dbName = c.db
	}
	tblName := qry.GetTableDef().GetName()
	dbSource, err := c.e.Database(c.proc.Ctx, dbName, c.proc.GetTxnOperator())
	if err != nil {
		return convertDBEOBToNoSuchTable(c.proc.Ctx, err, dbName, tblName)
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
	lineagePlan := alterDataBranchLineagePlan{}
	lineageSnapshotAdvanced := false
	lineageCloneTS := int64(0)
	lineageTxnOp := c.proc.GetTxnOperator()
	lineageOriginalSnapshot := timestamp.Timestamp{}
	lineageRestoreSnapshot := false
	defer func() {
		if lineageRestoreSnapshot {
			lineageTxnOp.SetSnapshotTS(lineageOriginalSnapshot)
		}
	}()
	if lineageTxnOp.Txn().IsPessimistic() {
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
					err = lockIndexTableForAlter(
						c.proc.Ctx,
						dbSource,
						c.e,
						c.proc,
						tblName,
						qry.TableDef.TblId,
						indexdef,
						retryErr != nil,
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
		if shouldAdvanceAlterDataBranchLineageSnapshot(
			lineageTxnOp.Txn().IsPessimistic(), lineageTxnOp.Txn().IsRCIsolation(),
		) {
			// The source metadata lock excludes new current-source branch clones.
			// Under RC, advance the statement snapshot while holding it so a branch
			// that committed just before lock acquisition is visible to the lineage
			// probe below, even when lock acquisition itself did not wait.
			lineageOriginalSnapshot = lineageTxnOp.SnapshotTS()
			lineageRestoreSnapshot = true
			if lineageCloneTS, err = c.advanceAlterDataBranchLineageSnapshot(); err != nil {
				return err
			}
			lineageSnapshotAdvanced = true
		}
	}
	// The stable row exists even when no owner does. Snapshot and PITR creation
	// cross the same write barrier before choosing their timestamp and retain
	// the write through owner publication. Pessimistic transactions wait; an
	// optimistic write-write loser retries the whole statement.
	if err = c.lockDataBranchLineageOwnerLifecycle(); err != nil {
		return err
	}
	lineagePlan, err = c.prepareAlterDataBranchLineage(oldId, dbName, tblName, "ALTER")
	if err != nil {
		return err
	}
	if !lineagePlan.enabled {
		var hasLatestHistory bool
		if hasLatestHistory, err = c.alterTableHasLatestHistoricalBranchSource(
			oldId, dbName, tblName,
		); err != nil {
			return err
		}
		if hasLatestHistory {
			lineagePlan.enabled = true
			lineagePlan.preserveHistoricalSource = true
		}
	}
	if lineagePlan.enabled {
		if columnName, replaced := alterCopySameStatementColumnReplacement(qry); replaced {
			return moerr.NewNotSupportedNoCtxf(
				"ALTER on a data-branch lineage cannot drop and add column '%s' in the same statement",
				columnName,
			)
		}
	}
	if lineagePlan.enabled {
		if lineageSnapshotAdvanced {
			lineagePlan.cloneTS = lineageCloneTS
			// A snapshot hint cannot see this transaction's workspace: it would
			// lose earlier DML and cannot resolve a generation created by earlier
			// DDL. The current operator already has the lock-held advanced snapshot
			// and overlays that workspace, so explicit transactions copy from it.
			lineageTxnOpts := lineageTxnOp.TxnOptions()
			txnHasWorkspaceHistory := isExplicitAlterTxn(
				lineageTxnOpts.GetByBegin(),
				lineageTxnOpts.GetAutocommit(),
			) || c.getHaveDDL()
			lineagePlan.fixedCopyTS = shouldUseFixedAlterCopySnapshot(
				lineageSnapshotAdvanced,
				txnHasWorkspaceHistory,
			)
		} else {
			// Optimistic mode has no row-lock snapshot barrier. Its statement
			// snapshot is nevertheless the exact source view copied by ALTER, so
			// record that same boundary without adding a MO_TS override.
			lineagePlan.cloneTS = c.proc.GetTxnOperator().SnapshotTS().PhysicalTime
		}
	}
	if lineageSnapshotAdvanced {
		// Re-resolve after the lock-held snapshot barrier so ordinary ALTER and
		// lineage ALTER both copy from the exact catalog view just validated.
		originRel, err = dbSource.Relation(c.proc.Ctx, tblName, nil)
		if err != nil {
			return err
		}
		if originRel.GetTableID(c.proc.Ctx) != oldId {
			return moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
		}
	}

	// Read the live relation constraint instead of relying only on the planned
	// TableDef. A just-created foreign key can already be enforced by the engine
	// while the planner-facing cached definition still lacks ForeignKeyDef.
	sourceForeignKeys, sourceRefChildTbls, err := snapshotAlterCopyForeignKeyState(c.proc.Ctx, originRel)
	if err != nil {
		return err
	}

	// 3. create temporary replica table which doesn't have foreign key constraints
	// Get logicalId from tableDef and pass it when creating the temporary table
	oldLogicalId := qry.GetTableDef().GetLogicalId()
	// The temporary relation is not externally visible. Its parent backrefs are
	// reconciled after the original relation is replaced, so avoid materializing
	// an intermediate parent->temporary-table relationship here.
	createTmpOpts := executor.StatementOption{}.WithIgnoreForeignKey()

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
	alterCopyOpt, err := c.precheckAlterCopyPkDedup(dbName, tblName, qry)
	if err != nil {
		c.proc.Error(c.proc.Ctx, "precheck primary key for alter table copy",
			zap.String("databaseName", dbName),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy tableName", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
	}
	opt := alterCopyStatementOption(alterCopyOpt)
	insertTmpDataSQL := alterCopySQLAtLineageSnapshot(qry.InsertTmpDataSql, lineagePlan)
	err = func() error {
		if !shouldEnableAlterCopyPipelineFlush(alterCopyOpt) {
			return c.runSqlWithOptions(insertTmpDataSQL, opt)
		}

		// Enable pipeline flush only when PK dedup can be skipped or was proven safe
		// by the alter-copy precheck.
		origCtx := c.proc.Ctx
		restoreCtx := origCtx
		if restoreCtx == nil {
			restoreCtx = c.proc.GetTopContext()
			if restoreCtx == nil {
				restoreCtx = context.Background()
			}
		}
		c.proc.Ctx = context.WithValue(restoreCtx, ioutil.PipelineFlushKey, true)
		defer func() {
			c.proc.Ctx = restoreCtx
		}()
		return c.runSqlWithOptions(insertTmpDataSQL, opt)
	}()
	if err != nil {
		c.proc.Error(c.proc.Ctx, "insert data to copy table for alter table",
			zap.String("databaseName", dbName),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy tableName", qry.CopyTableDef.Name),
			zap.String("InsertTmpDataSql", insertTmpDataSQL),
			zap.Error(err))
		return err
	}
	if err = c.reconcileAlterCopyAutoIncrement(
		dbName,
		qry.TableDef,
		qry.CopyTableDef,
		newRel,
		hasAlterAutoIncrementReset(qry.Actions),
		cleanup,
	); err != nil {
		return err
	}

	//6. copy on writing unaffected index table
	if err = cloneUnaffectedIndexes(
		c, dbName, qry.Options.SkipIndexesCopy, qry.AffectedCols, newRel, qry.TableDef, nil,
	); err != nil {
		return err
	}

	newId := newRel.GetTableID(c.proc.Ctx)
	if err = c.preserveAlterDataBranchLineage(
		lineagePlan, oldId, newId, dbName, tblName,
	); err != nil {
		return err
	}

	if !plan2.IsFkBannedDatabase(qry.Database) {
		// Apply ALTER actions to the source rows first, then make those rows
		// follow the replacement relation. This preserves catalog-only forward
		// references and avoids exposing a half-renamed self reference.
		for _, sql := range qry.UpdateFkSqls {
			if err = c.runSql(sql); err != nil {
				return err
			}
		}
		prepareFkSqls, _ := plan2.GetSqlForTransferAlterCopyFk(
			qry.Database,
			qry.TableDef.Name,
			qry.CopyTableDef.Name,
		)
		for _, sql := range prepareFkSqls {
			if err = c.runSql(sql); err != nil {
				return err
			}
		}
	}

	// 7. drop original table.
	// ISCP: That will also drop ISCP related jobs and pitr of the original table.
	dropSql := fmt.Sprintf("drop table `%s`.`%s`", dbName, tblName)
	if err := c.runSqlWithOptions(
		dropSql,
		// ALTER TABLE COPY replaces the source table internally. It is not a
		// user-visible DROP TABLE, so keep table-level publications unchanged.
		executor.StatementOption{}.WithIgnoreForeignKey().WithIgnorePublish(),
	); err != nil {
		c.proc.Error(c.proc.Ctx, "drop original table for alter table",
			zap.String("databaseName", dbName),
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy tableName", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
	}

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

	if !plan2.IsFkBannedDatabase(qry.Database) {
		_, finalizeFkSqls := plan2.GetSqlForTransferAlterCopyFk(
			qry.Database,
			qry.TableDef.Name,
			qry.CopyTableDef.Name,
		)
		for _, sql := range finalizeFkSqls {
			if err = c.runSql(sql); err != nil {
				return err
			}
		}
	}

	newTableDef := newRel.CopyTableDef(c.proc.Ctx)
	//--------------------------------------------------------------------------------------------------------------
	{
		// 9. invoke reindex for the new table, if it contains ivf index.
		multiTableIndexes := make(map[string]*MultiTableIndex)
		unaffectedIndexProcessed := make(map[string]bool)
		extra := newRel.GetExtraInfo()
		id := newRel.GetTableID(c.proc.Ctx)

		// cctx for the idxcron re-registration arm below — lazy-init,
		// reused across loop iterations.
		var idxcronCctx *pluginCompileCtx
		for _, indexDef := range newTableDef.Indexes {

			// DO NOT check SkipIndexesCopy here.  SkipIndexesCopy only valids for the unique/master/regular index.
			// Fulltext/HNSW/Ivfflat indexes are always "unaffected" in skipIndexesCopy
			// check affectedCols to see it is affected or not.  If affected is true, it means the secondary index
			// are cloned in cloneUnaffectedIndexes().  Otherwise, build the index again.

			if !indexDef.Unique && indexplugin.IsPluginAlgo(indexDef.IndexAlgo) {
				// vector (ivf/hnsw/cagra/ivfpq) or fulltext index

				if !isAlterAffectedPluginIndex(indexDef, qry.AffectedCols) {
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
							err = CreateIndexCdcTask(c, dbName, newTableDef.Name, newTableDef.TblId, indexDef.IndexName, sinker_type, false, "", newTableDef)
							if err != nil {
								return err
							}

							logutil.Infof("ISCP register unaffected index db=%s, table=%s, index=%s", dbName, newTableDef.Name, indexDef.IndexName)
						}
					}

					{
						// idxcron — register the algorithm's scheduled
						// maintenance task via the plugin. Plugins
						// without IdxcronAction (HNSW / CAGRA / IVF-PQ
						// today) are skipped.
						if p, ok := indexplugin.Get(indexDef.IndexAlgo); ok {
							d := p.Catalog().SyncDescriptor()
							if d.IdxcronAction != "" {
								if idxcronCctx == nil {
									idxcronCctx = newPluginCompileCtx(s, c, id, extra, dbSource, qry.Database, newTableDef, nil)
								}
								metadata, err := p.Compile().IdxcronMetadata(idxcronCctx)
								if err != nil {
									return err
								}
								if err = idxcron.RegisterUpdate(c.proc.Ctx,
									c.proc.GetService(),
									c.proc.GetTxnOperator(),
									id,
									dbName,
									newTableDef.Name,
									indexDef.IndexName,
									d.IdxcronAction,
									string(metadata)); err != nil {
									return err
								}
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

			// Only affected vector (ivf/hnsw/cagra/ivfpq) or fulltext
			// indexes reach here. All are plugin-registered today, so
			// aggregate into multiTableIndexes; the loop below
			// dispatches each through its plugin's HandleCreateIndex.
			if indexplugin.IsPluginAlgo(indexDef.IndexAlgo) {
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
		// cctx is loop-invariant — hoist to avoid per-index allocs.
		var aggCctx *pluginCompileCtx
		for _, multiTableIndex := range multiTableIndexes {

			if p, ok := indexplugin.Get(multiTableIndex.IndexAlgo); ok {
				if aggCctx == nil {
					aggCctx = newPluginCompileCtx(s, c, id, extra, dbSource, qry.Database, newTableDef, nil)
				}
				err = p.Compile().HandleCreateIndex(aggCctx, multiTableIndex.IndexDefs)
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

	if err = applyAlterCopyForeignKeyState(
		c,
		newRel,
		sourceForeignKeys,
		sourceRefChildTbls,
		qry.ChangeTblColIdMap,
		originRel.GetTableID(c.proc.Ctx),
		newRel.GetTableID(c.proc.Ctx),
	); err != nil {
		c.proc.Error(c.proc.Ctx, "restore and reconcile foreign keys for alter table copy",
			zap.String("origin tableName", qry.GetTableDef().Name),
			zap.String("copy table name", qry.CopyTableDef.Name),
			zap.Error(err))
		return err
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

func hasAlterAutoIncrementReset(actions []*plan.AlterTable_Action) bool {
	for _, action := range actions {
		if action != nil && action.GetAlterAutoIncrement() != nil {
			return true
		}
	}
	return false
}

// reconcileAlterCopyAutoIncrement publishes allocator state for the temporary
// table only after copied rows are visible in the ALTER transaction. Retained
// source columns are matched by stable planner column ID, never by position or
// a reused name.
func (c *Compile) reconcileAlterCopyAutoIncrement(
	dbName string,
	srcDef *plan.TableDef,
	copyDef *plan.TableDef,
	newRel engine.Relation,
	explicitReset bool,
	cleanup *alterAutoIncrementResetCleanup,
) error {
	if err := c.proc.Ctx.Err(); err != nil {
		return err
	}
	plannedAutoCols := incrservice.GetUserAutoColumnFromDef(copyDef)
	if len(plannedAutoCols) == 0 {
		return nil
	}
	if !engine.TxnSupportsAutoIncrEpochFence(c.proc.GetTxnOperator()) {
		return moerr.NewNotSupported(
			c.proc.Ctx,
			"AUTO_INCREMENT allocator reset requires epoch fencing on every TN service",
		)
	}
	// The planner copy definition can retain hidden source columns that are not
	// emitted into the temporary CREATE SQL. Use the created relation definition
	// so ColIndex matches mo_increment_columns for SetOffset.
	createdDef := newRel.GetTableDef(c.proc.Ctx)
	if createdDef == nil {
		return moerr.NewInternalError(c.proc.Ctx, "missing ALTER COPY table definition")
	}
	autoCols := incrservice.GetUserAutoColumnFromDef(createdDef)
	plannedNames := make(map[string]struct{}, len(plannedAutoCols))
	for _, col := range plannedAutoCols {
		plannedNames[strings.ToLower(col.ColName)] = struct{}{}
	}
	for _, col := range autoCols {
		name := strings.ToLower(col.ColName)
		if _, ok := plannedNames[name]; !ok {
			return moerr.NewInternalErrorf(
				c.proc.Ctx,
				"unexpected AUTO_INCREMENT column %q on ALTER COPY table",
				col.ColName,
			)
		}
		delete(plannedNames, name)
	}
	if len(plannedNames) != 0 {
		return moerr.NewInternalError(c.proc.Ctx, "missing AUTO_INCREMENT column on ALTER COPY table")
	}

	sourceOffsets := make(map[string]uint64)
	sourceNames := mapCloneAutoIncrColumns(srcDef, copyDef, true)
	retainedNames := make(map[string]struct{}, len(sourceNames))
	for _, name := range sourceNames {
		retainedNames[name] = struct{}{}
	}
	// Ordinary COPY preserves the source allocator high-water mark because old
	// CN caches can still own reserved values. An explicit AUTO_INCREMENT reset
	// is epoch-fenced, so its contract intentionally replaces those reservations.
	if !explicitReset && len(sourceNames) > 0 {
		sql := fmt.Sprintf(
			"select col_index, offset from mo_catalog.mo_increment_columns where table_id = %d",
			srcDef.TblId,
		)
		result, err := c.runSqlWithResultAndOptions(
			sql,
			NoAccountId,
			executor.StatementOption{}.WithDisableLog(),
		)
		if err != nil {
			result.Close()
			return err
		}
		func() {
			defer result.Close()
			result.ReadRows(func(rows int, cols []*vector.Vector) bool {
				colIndexes := vector.MustFixedColWithTypeCheck[int32](cols[0])
				offsets := vector.MustFixedColWithTypeCheck[uint64](cols[1])
				for i := 0; i < rows; i++ {
					if name, ok := sourceNames[colIndexes[i]]; ok {
						sourceOffsets[name] = offsets[i]
					}
				}
				return true
			})
		}()
	}

	tableID := newRel.GetTableID(c.proc.Ctx)
	svc := incrservice.GetAutoIncrementService(c.proc.GetService())
	epochReqs := make([]*api.AlterTableReq, 0, len(autoCols))
	var (
		freshColumnOffset         uint64
		freshColumnOffsetResolved bool
	)
	for _, col := range autoCols {
		if err := c.proc.Ctx.Err(); err != nil {
			return err
		}
		colIdent := quoteAlterCopyIdentifier(col.ColName)
		maxSQL := fmt.Sprintf(
			"select cast(coalesce(max(case when %s > 0 then %s else 0 end), 0) as unsigned) from %s",
			colIdent,
			colIdent,
			quoteAlterCopyTableName(dbName, copyDef.Name),
		)
		result, err := c.runSqlWithResultAndOptions(
			maxSQL,
			NoAccountId,
			executor.StatementOption{}.WithDisableLog(),
		)
		if err != nil {
			result.Close()
			return err
		}
		var copiedMax uint64
		func() {
			defer result.Close()
			result.ReadRows(func(rows int, cols []*vector.Vector) bool {
				if rows > 0 && len(cols) > 0 && !cols[0].IsNull(0) {
					copiedMax = executor.GetFixedRows[uint64](cols[0])[0]
				}
				return false
			})
		}()

		name := strings.ToLower(col.ColName)
		_, retained := retainedNames[name]
		// Internal ALTER COPY SQL may execute without the client session variables.
		// Reapply a non-default session offset to an empty newly added column from
		// the outer compile instead of assuming the temporary CREATE inherited it.
		if !explicitReset && !retained && copyDef.AutoIncrOffset == 0 && copiedMax == 0 {
			if !freshColumnOffsetResolved {
				value, err := resolveVariableOrDefault(
					c.proc,
					"auto_increment_offset",
					true,
					false,
				)
				if err != nil {
					return err
				}
				offset, ok := value.(int64)
				if !ok {
					return moerr.NewInternalErrorf(
						c.proc.Ctx,
						"invalid auto_increment_offset type %T",
						value,
					)
				}
				if offset > 1 {
					freshColumnOffset = uint64(offset - 1)
				}
				freshColumnOffsetResolved = true
			}
			if freshColumnOffset == 0 {
				continue
			}
			copiedMax = freshColumnOffset
		}
		effectiveOffset := max(copyDef.AutoIncrOffset, copiedMax)
		if !explicitReset {
			effectiveOffset = max(effectiveOffset, sourceOffsets[name])
		}
		if err := incrservice.ValidateAutoColumnOffset(
			c.proc.Ctx,
			types.T(createdDef.Cols[col.ColIndex].Typ.Id),
			effectiveOffset,
		); err != nil {
			return err
		}
		if err := svc.SetOffset(
			c.proc.Ctx,
			tableID,
			col.ColIndex,
			col.ColName,
			effectiveOffset,
			c.proc.GetTxnOperator(),
		); err != nil {
			return err
		}
		cleanup.track(tableID)
		epochReqs = append(epochReqs, api.NewUpdateAutoIncrementReq(
			0,
			tableID,
			effectiveOffset,
			0,
		))
	}
	if err := c.proc.Ctx.Err(); err != nil {
		return err
	}
	if len(epochReqs) == 0 {
		return nil
	}
	// SetOffset publishes the next allocator generation. Publish the matching
	// catalog generation on the COPY replacement before it becomes visible, so
	// the next implicit insert does not observe a stale table definition.
	databaseID := newRel.GetDBID(c.proc.Ctx)
	for _, req := range epochReqs {
		req.DbId = databaseID
	}
	return newRel.AlterTable(c.proc.Ctx, nil, epochReqs)
}

func (s *Scope) AlterTable(c *Compile) (err error) {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()
	cleanup := newAlterAutoIncrementResetCleanup(c)
	defer cleanup.finish(&err)

	qry := s.Plan.GetDdl().GetAlterTable()

	// Check if target table is a CCPR shared table (from publication)
	if c.shouldBlockCCPRReadOnly(qry.TableDef) {
		return moerr.NewCCPRReadOnly(c.proc.Ctx)
	}

	ps := c.proc.GetPartitionService()
	if !ps.Enabled() ||
		!features.IsPartitioned(qry.TableDef.FeatureFlag) {
		return s.doAlterTable(c, cleanup)
	}

	if qry.AlterPartition == nil {
		switch qry.AlgorithmType {
		case plan.AlterTable_COPY:
			return s.doAlterTable(c, cleanup)
		default:
			// alter primary table
			if err := s.doAlterTable(c, cleanup); err != nil {
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
			return c.alterPartitionTables(
				st.(*tree.AlterTable),
				metadata.Partitions,
				hasAlterAutoIncrementReset(qry.Actions),
				cleanup,
			)
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

func (c *Compile) alterPartitionTables(
	stmt *tree.AlterTable,
	partitions []partition.Partition,
	trackAutoIncrementReset bool,
	cleanup *alterAutoIncrementResetCleanup,
) error {
	table := stmt.Table
	stmt.PartitionOption = nil
	for _, p := range partitions {
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
		if trackAutoIncrementReset {
			cleanup.track(p.PartitionID)
		}
	}
	return nil
}

func (s *Scope) doAlterTable(c *Compile, cleanup *alterAutoIncrementResetCleanup) (err error) {
	qry := s.Plan.GetDdl().GetAlterTable()
	oldRelationID := qry.GetTableDef().GetTblId()
	oldLogicalID := qry.GetTableDef().GetLogicalId()

	if qry.AlgorithmType == plan.AlterTable_COPY {
		// COPY ALTER transfers mo_foreign_keys around the source-table drop,
		// so its catalog statements are executed inside AlterTableCopy.
		err = s.alterTableCopy(c, cleanup)
	} else {
		err = s.alterTableInplace(c, cleanup)
	}
	if err != nil {
		return err
	}

	if qry.AlgorithmType != plan.AlterTable_COPY && !plan2.IsFkBannedDatabase(qry.Database) {
		//update the mo_foreign_keys
		for _, sql := range qry.UpdateFkSqls {
			err = c.runSql(sql)
			if err != nil {
				return err
			}
		}
	}
	databaseName := qry.Database
	if databaseName == "" {
		databaseName = c.db
	}
	if qry.AlgorithmType != plan.AlterTable_COPY {
		changesViewMetadata := false
		var renamedTo string
		for _, action := range qry.Actions {
			if action.GetAlterReplaceDef() != nil {
				changesViewMetadata = true
				break
			}
			if rename := action.GetAlterName(); rename != nil {
				renamedTo = rename.NewName
			}
		}
		if renamedTo != "" && c.proc.Base.IsFrontend {
			return c.enqueueViewsAfterRelationRemoval(
				databaseName, qry.GetTableDef().GetName(), qry.GetTableDef().GetDbId(),
				oldRelationID, oldLogicalID)
		}
		if !changesViewMetadata {
			return nil
		}
	}
	return c.refreshViewsAfterRelationMutation(
		databaseName, qry.GetTableDef().GetName(), oldRelationID, oldLogicalID)
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

func reconcileAlterCopyChildForeignKeyReferences(
	c *Compile,
	changeColDefMap map[uint64]*plan.ColDef,
	childTblIDs []uint64,
	oldParentTblId uint64,
	newParentTblId uint64,
) error {
	for _, childTblID := range uniqueNonZeroTableIDs(childTblIDs) {
		if err := updateTableForeignKeyColId(
			c,
			changeColDefMap,
			childTblID,
			oldParentTblId,
			newParentTblId,
		); err != nil {
			return err
		}
	}
	return nil
}

func snapshotAlterCopyForeignKeyState(
	ctx context.Context,
	sourceRel engine.Relation,
) ([]*plan.ForeignKeyDef, []uint64, error) {
	constraintDef, err := GetConstraintDef(ctx, sourceRel)
	if err != nil {
		return nil, nil, err
	}
	var foreignKeys []*plan.ForeignKeyDef
	for _, constraint := range constraintDef.Cts {
		switch definition := constraint.(type) {
		case *engine.ForeignKeyDef:
			for _, foreignKey := range definition.Fkeys {
				if foreignKey == nil {
					return nil, nil, moerr.NewInternalError(ctx, "nil foreign key definition in ALTER COPY source constraint")
				}
				foreignKeys = append(foreignKeys, plan2.DeepCopyFkey(foreignKey))
			}
		}
	}
	return foreignKeys, slices.Clone(canonicalRefChildTableIDs(constraintDef)), nil
}

func applyAlterCopyForeignKeyState(
	c *Compile,
	replacementRel engine.Relation,
	sourceForeignKeys []*plan.ForeignKeyDef,
	sourceRefChildTbls []uint64,
	changeColDefMap map[uint64]*plan.ColDef,
	oldTableID uint64,
	newTableID uint64,
) error {
	replacementForeignKeys, replacementRefChildTbls, err := remapAlterCopyForeignKeyState(
		c.proc.Ctx,
		sourceForeignKeys,
		sourceRefChildTbls,
		changeColDefMap,
		oldTableID,
	)
	if err != nil {
		return err
	}

	// The live source relation is authoritative even when either set is empty.
	// Replace both constraints together so a stale planned temporary definition
	// cannot resurrect one side of the relationship.
	if err = restoreAlterCopyForeignKeyState(
		c.proc.Ctx, replacementRel, replacementForeignKeys, replacementRefChildTbls,
	); err != nil {
		return err
	}
	if err = reconcileAlterCopyChildForeignKeyReferences(
		c, changeColDefMap, replacementRefChildTbls, oldTableID, newTableID,
	); err != nil {
		return err
	}
	return reconcileAlterCopyParentForeignKeyReferences(
		c, replacementForeignKeys, oldTableID, newTableID,
	)
}

func remapAlterCopyForeignKeyState(
	ctx context.Context,
	sourceForeignKeys []*plan.ForeignKeyDef,
	sourceRefChildTbls []uint64,
	changeColDefMap map[uint64]*plan.ColDef,
	oldTableID uint64,
) ([]*plan.ForeignKeyDef, []uint64, error) {
	result := make([]*plan.ForeignKeyDef, len(sourceForeignKeys))
	hasSelfReference := false
	for i, sourceForeignKey := range sourceForeignKeys {
		if sourceForeignKey == nil {
			return nil, nil, moerr.NewInternalError(ctx, "nil foreign key definition in ALTER COPY")
		}
		foreignKey := plan2.DeepCopyFkey(sourceForeignKey)
		for j, oldColumnID := range foreignKey.Cols {
			newColumn, ok := changeColDefMap[oldColumnID]
			if !ok {
				return nil, nil, moerr.NewInternalErrorf(ctx,
					"foreign key %s child column %d was not retained by ALTER COPY",
					foreignKey.Name, oldColumnID)
			}
			foreignKey.Cols[j] = newColumn.ColId
		}

		selfReference := foreignKey.ForeignTbl == 0 || foreignKey.ForeignTbl == oldTableID
		if selfReference {
			hasSelfReference = true
			for j, oldColumnID := range foreignKey.ForeignCols {
				newColumn, ok := changeColDefMap[oldColumnID]
				if !ok {
					return nil, nil, moerr.NewInternalErrorf(ctx,
						"foreign key %s parent column %d was not retained by ALTER COPY",
						foreignKey.Name, oldColumnID)
				}
				foreignKey.ForeignCols[j] = newColumn.ColId
			}
			// Zero is the durable self-reference sentinel. Keeping a physical table
			// generation here would make the replacement relation look like an
			// external parent during reconciliation.
			foreignKey.ForeignTbl = 0
		}
		result[i] = foreignKey
	}

	refChildTbls := make([]uint64, 0, len(sourceRefChildTbls)+1)
	seen := make(map[uint64]struct{}, len(sourceRefChildTbls)+1)
	for _, childTableID := range sourceRefChildTbls {
		if childTableID == oldTableID {
			childTableID = 0
		}
		if _, exists := seen[childTableID]; exists {
			continue
		}
		seen[childTableID] = struct{}{}
		refChildTbls = append(refChildTbls, childTableID)
	}
	if hasSelfReference {
		if _, exists := seen[0]; !exists {
			refChildTbls = append(refChildTbls, 0)
		}
	}
	return result, refChildTbls, nil
}

func restoreAlterCopyForeignKeyState(
	ctx context.Context,
	replacementRel engine.Relation,
	foreignKeys []*plan.ForeignKeyDef,
	refChildTbls []uint64,
) error {
	constraintDef, err := GetConstraintDef(ctx, replacementRel)
	if err != nil {
		return err
	}
	constraintDef, err = MakeNewCreateConstraint(constraintDef, &engine.ForeignKeyDef{Fkeys: foreignKeys})
	if err != nil {
		return err
	}
	setRefChildTableIDs(constraintDef, refChildTbls)
	return replacementRel.UpdateConstraint(ctx, constraintDef)
}

// updateTableForeignKeyColId updates one child relation only when it still
// references the replaced parent relation.
func updateTableForeignKeyColId(
	c *Compile,
	changeColDefMap map[uint64]*plan.ColDef,
	childTblID uint64,
	oldParentTblId uint64,
	newParentTblId uint64,
) error {
	_, _, childRel, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), childTblID)
	if err != nil {
		return err
	}
	oldCt, err := GetConstraintDef(c.proc.Ctx, childRel)
	if err != nil {
		return err
	}
	changed, err := rewriteForeignKeyReferencesForAlterCopy(
		c.proc.Ctx,
		oldCt,
		changeColDefMap,
		oldParentTblId,
		newParentTblId,
	)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	return childRel.UpdateConstraint(c.proc.Ctx, oldCt)
}

func rewriteForeignKeyReferencesForAlterCopy(
	ctx context.Context,
	constraintDef *engine.ConstraintDef,
	changeColDefMap map[uint64]*plan.ColDef,
	oldParentTblID uint64,
	newParentTblID uint64,
) (bool, error) {
	changed := false
	for _, ct := range constraintDef.Cts {
		if def, ok1 := ct.(*engine.ForeignKeyDef); ok1 {
			for _, fkey := range def.Fkeys {
				if fkey == nil {
					return false, moerr.NewInternalError(ctx, "nil foreign key definition in ALTER COPY constraint")
				}
				if fkey.ForeignTbl != oldParentTblID {
					continue
				}
				for j, foreignColID := range fkey.ForeignCols {
					if newColDef, ok := changeColDefMap[foreignColID]; ok && foreignColID != newColDef.ColId {
						fkey.ForeignCols[j] = newColDef.ColId
						changed = true
					}
				}
				if fkey.ForeignTbl != newParentTblID {
					fkey.ForeignTbl = newParentTblID
					changed = true
				}
			}
		}
	}
	return changed, nil
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

func reconcileAlterCopyParentForeignKeyReferences(
	c *Compile,
	fkeys []*plan.ForeignKeyDef,
	oldTableID uint64,
	newTableID uint64,
) error {
	for _, fkey := range fkeys {
		if fkey == nil {
			return moerr.NewInternalError(c.proc.Ctx, "nil foreign key definition in ALTER COPY")
		}
	}
	for _, parentTableID := range foreignKeyParentTableIDs(fkeys) {
		if err := updateParentTableRefChildTableIDForAlterCopy(
			c,
			parentTableID,
			oldTableID,
			newTableID,
		); err != nil {
			return err
		}
	}
	return nil
}

func updateParentTableRefChildTableIDForAlterCopy(
	c *Compile,
	parentTableID uint64,
	oldTableID uint64,
	newTableID uint64,
) error {
	_, _, fatherRelation, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), parentTableID)
	if err != nil {
		return err
	}
	oldCt, err := GetConstraintDef(c.proc.Ctx, fatherRelation)
	if err != nil {
		return err
	}
	reconcileRefChildTableID(oldCt, oldTableID, newTableID)
	return fatherRelation.UpdateConstraint(c.proc.Ctx, oldCt)
}

func uniqueNonZeroTableIDs(tableIDs []uint64) []uint64 {
	unique := make([]uint64, 0, len(tableIDs))
	seen := make(map[uint64]struct{}, len(tableIDs))
	for _, tableID := range tableIDs {
		if tableID == 0 {
			continue
		}
		if _, exists := seen[tableID]; exists {
			continue
		}
		seen[tableID] = struct{}{}
		unique = append(unique, tableID)
	}
	return unique
}

func foreignKeyParentTableIDs(fkeys []*plan.ForeignKeyDef) []uint64 {
	parentTableIDs := make([]uint64, 0, len(fkeys))
	for _, fkey := range fkeys {
		parentTableIDs = append(parentTableIDs, fkey.ForeignTbl)
	}
	return uniqueNonZeroTableIDs(parentTableIDs)
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
		// check the plan-carried affected index name, Parts, and included columns to determine affected or not.
		// If unaffected index, try clone.  Otherwise, re-build the index
		if !skipIndexesCopy[idxTbl.IndexName] {
			// This index is affected index, skip it
			continue
		}

		if !idxTbl.TableExist || len(idxTbl.IndexTableName) == 0 {
			continue
		}

		affected := false
		if !idxTbl.Unique && indexplugin.IsPluginAlgo(idxTbl.IndexAlgo) {
			affected = isAlterAffectedPluginIndex(idxTbl, affectedCols)
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

		// IsAsync is the canonical resolution: identity-always-async
		// (HNSW/CAGRA/IVF-PQ), fulltext VERSION=2, or the per-index async param.
		// Used for both the whole-index skip below and the per-hidden-table
		// SkipWhenAsync skip further down.
		async, err := indexplugin.IsAsync(oriIdxTblNames.IndexAlgo, oriIdxTblNames.IndexAlgoParams)
		if err != nil {
			return err
		}

		// Per-algo clone semantics live entirely on the plugin's
		// AlterTableCloneBehavior, which declares two mutually exclusive
		// policies:
		//   - SkipWholeIndex: skip the entire index when async. Algorithms that
		//     leave every hidden table empty at CREATE and rebuild all of them
		//     via CDC from ts=0 (HNSW / CAGRA / IVF-PQ / fulltext).
		//   - DeleteBeforeClone + SkipWhenAsync (per hidden table): IVF-FLAT is
		//     the only case today. All three hidden tables get DELETE'd (the
		//     CREATE on the temp table already seeded them), entries are
		//     additionally skipped when async (CDC rebuilds entries from ts=0),
		//     while metadata + centroids ARE cloned so the sinker has a k-means
		//     model to write against.
		var cloneBehavior catalogplugin.AlterTableCloneBehavior
		if !oriIdxTblNames.Unique {
			if p, ok := indexplugin.Get(oriIdxTblNames.IndexAlgo); ok {
				cloneBehavior = p.Catalog().AlterTableCloneBehavior()
				// Whole-index skip is an EXPLICIT policy (SkipWholeIndex), not
				// inferred from UsesCDC — a CDC algorithm can still need its model
				// tables cloned (IVF-FLAT clones metadata + centroids and only
				// CDC-rebuilds entries via the per-hidden-table policy below).
				if async && cloneBehavior.SkipWholeIndex {
					logutil.Infof("cloneUnaffectedIndex: skip whole async index %v\n", oriIdxTblNames)
					continue
				}
			}
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

			// Hidden tables that were seeded by the temp table's
			// CREATE-INDEX side effects must be emptied before the
			// clone copies source rows on top of the seed.
			if cloneBehavior.ContainsDelete(oriIdxTblName.AlgoTableType) {
				// delete all content but avoid truncate table with WHERE TRUE
				sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE TRUE", dbName, newIdxTblName.IndexTableName)
				if err := c.runSql(sql); err != nil {
					return err
				}
			}

			// Hidden tables the algorithm rebuilds via CDC from ts=0
			// on the new table — cloning them and letting CDC rebuild
			// produces duplicates.
			if async && cloneBehavior.ContainsSkipWhenAsync(oriIdxTblName.AlgoTableType) {
				logutil.Infof("cloneUnaffectedIndex: skip async index hidden table %v\n", oriIdxTblName)
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
