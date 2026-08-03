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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_clone"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
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

func (c *Compile) lockDataBranchLineageOwnerPublication() error {
	return databranchutils.LockLineageOwnerPublication(func(sql string) error {
		return c.runSqlWithAccountId(sql, int32(catalog.System_Account))
	})
}

func (c *Compile) prepareAlterDataBranchLineage(
	oldTableID uint64,
	databaseName, tableName string,
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
				opts.GetByBegin(), opts.GetAutocommit(), op.Txn().IsPessimistic(),
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

func validateAlterDataBranchLineageTxn(byBegin, autocommit, _ bool) error {
	if isExplicitAlterTxn(byBegin, autocommit) {
		return moerr.NewNotSupportedNoCtx(
			"ALTER on a data-branch lineage is not supported inside an explicit transaction",
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

type alterCopyAutoIncrementCleanup struct {
	c        *Compile
	tableIDs []uint64
	tracked  map[uint64]struct{}
}

func newAlterCopyAutoIncrementCleanup(c *Compile) *alterCopyAutoIncrementCleanup {
	return &alterCopyAutoIncrementCleanup{
		c:       c,
		tracked: make(map[uint64]struct{}),
	}
}

func (cleanup *alterCopyAutoIncrementCleanup) track(tableID uint64) {
	if _, ok := cleanup.tracked[tableID]; ok {
		return
	}
	cleanup.tracked[tableID] = struct{}{}
	cleanup.tableIDs = append(cleanup.tableIDs, tableID)
}

func (cleanup *alterCopyAutoIncrementCleanup) finish(statementErr *error) {
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
			"alter.table.copy.discard.auto.increment.reset",
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
	cleanup := newAlterCopyAutoIncrementCleanup(c)
	defer cleanup.finish(&err)

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
	if err = c.lockDataBranchLineageOwnerPublication(); err != nil {
		return err
	}
	lineagePlan, err = c.prepareAlterDataBranchLineage(oldId, dbName, tblName)
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
				newRel.GetTableID(c.proc.Ctx),
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

// reconcileAlterCopyAutoIncrement publishes allocator state for the temporary
// table only after copied rows are visible in the ALTER transaction. Retained
// source columns are matched by stable planner column ID, never by position or
// a reused name.
func (c *Compile) reconcileAlterCopyAutoIncrement(
	dbName string,
	srcDef *plan.TableDef,
	copyDef *plan.TableDef,
	newRel engine.Relation,
	cleanup *alterCopyAutoIncrementCleanup,
) error {
	if err := c.proc.Ctx.Err(); err != nil {
		return err
	}
	autoCols := incrservice.GetUserAutoColumnFromDef(copyDef)
	if len(autoCols) == 0 {
		return nil
	}
	if !engine.TxnSupportsAutoIncrEpochFence(c.proc.GetTxnOperator()) {
		return moerr.NewNotSupported(
			c.proc.Ctx,
			"AUTO_INCREMENT allocator reset requires epoch fencing on every TN service",
		)
	}

	sourceOffsets := make(map[string]uint64)
	sourceNames := mapCloneAutoIncrColumns(srcDef, copyDef, true)
	if len(sourceNames) > 0 {
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
		effectiveOffset := max(copyDef.AutoIncrOffset, copiedMax, sourceOffsets[name])
		if err := incrservice.ValidateAutoColumnOffset(
			c.proc.Ctx,
			types.T(copyDef.Cols[col.ColIndex].Typ.Id),
			effectiveOffset,
		); err != nil {
			return err
		}
		if err := svc.SetOffset(
			c.proc.Ctx,
			tableID,
			col.ColName,
			effectiveOffset,
			c.proc.GetTxnOperator(),
		); err != nil {
			return err
		}
		cleanup.track(tableID)
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

	// Check if target table is a CCPR shared table (from publication)
	if c.shouldBlockCCPRReadOnly(qry.TableDef) {
		return moerr.NewCCPRReadOnly(c.proc.Ctx)
	}

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
	refreshViewMetadata := !features.IsPartition(qry.GetTableDef().GetFeatureFlag()) &&
		(qry.AlgorithmType == plan.AlterTable_COPY || qry.GetCopyTableDef() != nil)
	pendingRecoveryName := ""
	for _, action := range qry.GetActions() {
		if rename, ok := action.GetAction().(*plan.AlterTable_Action_AlterName); ok {
			pendingRecoveryName = rename.AlterName.GetNewName()
		}
	}
	sourceAccountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	sourceLogicalID := qry.GetTableDef().GetLogicalId()
	if sourceLogicalID == 0 {
		sourceLogicalID = qry.GetTableDef().GetTblId()
	}

	if qry.AlgorithmType == plan.AlterTable_COPY {
		// COPY ALTER transfers mo_foreign_keys around the source-table drop,
		// so its catalog statements are executed inside AlterTableCopy.
		if err = s.AlterTableCopy(c); err != nil {
			return err
		}
		if !refreshViewMetadata {
			return nil
		}
		database, err := c.e.Database(c.proc.Ctx, qry.GetDatabase(), c.proc.GetTxnOperator())
		if err != nil {
			return err
		}
		relation, err := database.Relation(
			c.proc.Ctx,
			qry.GetTableDef().GetName(),
			nil,
		)
		if err != nil {
			return err
		}
		return refreshViewMetadataAfterAlter(
			c,
			sourceAccountID,
			sourceLogicalID,
			qry.GetTableDef().GetTblId(),
			relation.GetTableID(c.proc.Ctx),
			qry.GetDatabase(),
			qry.GetTableDef().GetName(),
			false,
		)
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
	if pendingRecoveryName != "" && !renameViewMetadataRecoveryDeferred(c.proc.Ctx) {
		database, err := c.e.Database(c.proc.Ctx, qry.GetDatabase(), c.proc.GetTxnOperator())
		if err != nil {
			return err
		}
		relation, err := database.Relation(c.proc.Ctx, pendingRecoveryName, nil)
		if err != nil {
			return err
		}
		if err = refreshPendingViewMetadataForRelation(
			c, qry.GetDatabase(), pendingRecoveryName, relation,
		); err != nil {
			return err
		}
	}
	if !refreshViewMetadata {
		return nil
	}
	return refreshViewMetadataAfterAlter(
		c,
		sourceAccountID,
		sourceLogicalID,
		qry.GetTableDef().GetTblId(),
		qry.GetTableDef().GetTblId(),
		qry.GetDatabase(),
		qry.GetTableDef().GetName(),
		false,
	)
}

type deferRenameViewMetadataRecoveryKey struct{}

type renameViewMetadataTarget struct {
	database string
	name     string
}

func renameViewMetadataRecoveryDeferred(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	deferred, _ := ctx.Value(deferRenameViewMetadataRecoveryKey{}).(bool)
	return deferred
}

func refreshPendingViewMetadataForRelation(
	c *Compile,
	database string,
	name string,
	relation engine.Relation,
) error {
	return refreshViewMetadataForRelation(c, database, name, relation, true)
}

func refreshViewMetadataForRelation(
	c *Compile,
	database string,
	name string,
	relation engine.Relation,
	onlyPending bool,
) error {
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	tableID := relation.GetTableID(c.proc.Ctx)
	logicalID := relation.GetTableDef(c.proc.Ctx).GetLogicalId()
	if logicalID == 0 {
		logicalID = tableID
	}
	return refreshViewMetadataAfterAlter(
		c, accountID, logicalID, tableID, tableID, database, name, onlyPending,
	)
}

func collectRenameViewMetadataTargets(qry *plan.RenameTable) map[renameViewMetadataTarget]struct{} {
	targets := make(map[renameViewMetadataTarget]struct{})
	for _, alterTable := range qry.GetAlterTables() {
		for _, action := range alterTable.GetActions() {
			if rename, ok := action.GetAction().(*plan.AlterTable_Action_AlterName); ok {
				targets[renameViewMetadataTarget{
					database: alterTable.GetDatabase(),
					name:     rename.AlterName.GetNewName(),
				}] = struct{}{}
			}
		}
	}
	return targets
}

func refreshPendingViewMetadataAfterRename(
	c *Compile,
	targetSet map[renameViewMetadataTarget]struct{},
) error {
	targets := make([]renameViewMetadataTarget, 0, len(targetSet))
	for target := range targetSet {
		targets = append(targets, target)
	}
	slices.SortFunc(targets, func(left, right renameViewMetadataTarget) int {
		if order := strings.Compare(left.database, right.database); order != 0 {
			return order
		}
		return strings.Compare(left.name, right.name)
	})
	for _, target := range targets {
		database, err := c.e.Database(c.proc.Ctx, target.database, c.proc.GetTxnOperator())
		if err != nil {
			return err
		}
		relation, err := database.Relation(c.proc.Ctx, target.name, nil)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
				continue
			}
			return err
		}
		if err = refreshPendingViewMetadataForRelation(c, target.database, target.name, relation); err != nil {
			return err
		}
	}
	return nil
}

type viewMetadataRefresh struct {
	accountID  uint32
	id         uint64
	logicalID  uint64
	version    uint32
	database   string
	name       string
	definition string
	viewData   plan2.ViewData
	skip       bool
}

type viewMetadataRefreshContext struct {
	retry                bool
	sourceAccountID      uint32
	sourceLogicalID      uint64
	currentSourceTableID uint64
	confirmed            *bool
	targetViewID         uint64
	targetViewVersion    uint32
	targetViewDefinition string
}

type viewMetadataRefreshPlanError struct {
	err error
}

type viewMetadataSubscriptionResolverKey struct{}

type viewMetadataCompilerContextKey struct{}

type viewMetadataResolverKey struct{}

type viewMetadataSQLModeKey struct{}

type viewMetadataSubscriptionResolver interface {
	GetSubscriptionMeta(string, *plan2.Snapshot) (*plan2.SubscriptionMeta, error)
}

type currentViewSubscriptionResolver struct {
	accountID          uint32
	loadSnapshot       func(uint32, string, *plan2.Snapshot) (*plan2.SubscriptionMeta, error)
	byDatabase         map[string]*plan2.SubscriptionMeta
	snapshotByIdentity map[viewMetadataSnapshotSubscriptionKey]*plan2.SubscriptionMeta
	loadedSnapshots    map[viewMetadataSnapshotSubscriptionKey]struct{}
}

type viewMetadataSnapshotSubscriptionKey struct {
	accountID    uint32
	database     string
	physicalTime int64
	logicalTime  uint32
}

func (r currentViewSubscriptionResolver) GetSubscriptionMeta(
	database string,
	snapshot *plan2.Snapshot,
) (*plan2.SubscriptionMeta, error) {
	if !plan2.IsSnapshotValid(snapshot) {
		return r.byDatabase[strings.ToLower(database)], nil
	}
	accountID := r.accountID
	if snapshot.GetTenant() != nil {
		accountID = snapshot.GetTenant().GetTenantID()
	}
	key := viewMetadataSnapshotSubscriptionKey{
		accountID:    accountID,
		database:     strings.ToLower(database),
		physicalTime: snapshot.GetTS().GetPhysicalTime(),
		logicalTime:  snapshot.GetTS().GetLogicalTime(),
	}
	if _, ok := r.loadedSnapshots[key]; ok {
		return r.snapshotByIdentity[key], nil
	}
	if r.loadSnapshot == nil {
		return nil, nil
	}
	meta, err := r.loadSnapshot(accountID, database, snapshot)
	if err != nil {
		return nil, err
	}
	r.snapshotByIdentity[key] = meta
	r.loadedSnapshots[key] = struct{}{}
	return meta, nil
}

func (e *viewMetadataRefreshPlanError) Error() string {
	return e.err.Error()
}

func (e *viewMetadataRefreshPlanError) Unwrap() error {
	return e.err
}

func wrapViewMetadataRefreshPlanError(ctx context.Context, err error) error {
	if err == nil || !isViewMetadataRefresh(ctx) {
		return err
	}
	return &viewMetadataRefreshPlanError{err: err}
}

type viewMetadataRefreshSource struct {
	accountID  uint32
	logicalID  uint64
	previousID uint64
	currentID  uint64
	database   string
	tableName  string
}

type viewMetadataRefreshWork struct {
	source           viewMetadataRefreshSource
	legacyCandidates []viewMetadataRefreshSource
}

func isViewMetadataRefresh(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	_, ok := viewMetadataRefreshContextFromContext(ctx)
	return ok
}

func viewMetadataRefreshContextFromContext(ctx context.Context) (viewMetadataRefreshContext, bool) {
	if ctx == nil {
		return viewMetadataRefreshContext{}, false
	}
	if refresh, ok := ctx.Value(defines.ViewMetadataRefreshKey{}).(viewMetadataRefreshContext); ok {
		return refresh, true
	}
	retry, ok := ctx.Value(defines.ViewMetadataRetryKey{}).(defines.ViewMetadataRetry)
	if !ok {
		return viewMetadataRefreshContext{}, false
	}
	return viewMetadataRefreshContext{
		retry:                true,
		targetViewID:         retry.TargetViewID,
		targetViewVersion:    retry.TargetViewVersion,
		targetViewDefinition: retry.TargetViewDefinition,
	}, true
}

func refreshViewMetadataAfterAlter(
	c *Compile,
	sourceAccountID uint32,
	sourceLogicalID uint64,
	previousSourceTableID uint64,
	currentSourceTableID uint64,
	sourceDatabase string,
	sourceTable string,
	onlyPending bool,
) error {
	subscriptionsByAccount := make(map[uint32]currentViewSubscriptionResolver)
	work := []viewMetadataRefreshWork{{source: viewMetadataRefreshSource{
		accountID:  sourceAccountID,
		logicalID:  sourceLogicalID,
		previousID: previousSourceTableID,
		currentID:  currentSourceTableID,
		database:   sourceDatabase,
		tableName:  sourceTable,
	}}}
	processedViews := make(map[[2]uint64]struct{})
	for len(work) > 0 {
		currentWork := work[0]
		work = work[1:]
		source := currentWork.source
		var nextLegacyCandidates []viewMetadataRefreshSource
		var afterViewID uint64
		for {
			views, err := loadViewMetadataRefreshPage(
				c,
				source.accountID,
				source.logicalID,
				source.previousID,
				source.database,
				source.tableName,
				afterViewID,
				onlyPending,
				currentWork.legacyCandidates,
			)
			if err != nil {
				return err
			}
			if len(views) == 0 {
				break
			}
			for _, view := range views {
				afterViewID = view.id
				viewKey := [2]uint64{uint64(view.accountID), view.id}
				if _, ok := processedViews[viewKey]; ok {
					continue
				}
				var subscriptions currentViewSubscriptionResolver
				subscriptionsLoaded := false
				if len(view.viewData.Dependencies) > 0 {
					confirmed := viewDependenciesContainLiveSource(
						view.viewData.Dependencies,
						source,
						nil,
					)
					if !confirmed && viewDependenciesHaveLiveSubscription(view.viewData.Dependencies) {
						var ok bool
						subscriptions, ok = subscriptionsByAccount[view.accountID]
						if !ok {
							subscriptions, err = loadCurrentViewSubscriptions(c, view.accountID)
							if err != nil {
								return err
							}
							subscriptionsByAccount[view.accountID] = subscriptions
						}
						subscriptionsLoaded = true
						confirmed = viewDependenciesContainLiveSource(
							view.viewData.Dependencies,
							source,
							subscriptions.byDatabase,
						)
					}
					if !confirmed {
						processedViews[viewKey] = struct{}{}
						continue
					}
				}
				if view.skip {
					processedViews[viewKey] = struct{}{}
					continue
				}
				sql, err := buildRefreshViewSQL(c.proc.Ctx, c.getLower(), view)
				if err != nil {
					logutil.Warn("skip refreshing view that cannot be parsed",
						zap.String("database", view.database),
						zap.String("view", view.name),
						zap.Error(err))
					processedViews[viewKey] = struct{}{}
					continue
				}
				if !subscriptionsLoaded {
					var ok bool
					subscriptions, ok = subscriptionsByAccount[view.accountID]
					if ok {
						subscriptionsLoaded = true
					}
				}
				if !subscriptionsLoaded {
					subscriptions, err = loadCurrentViewSubscriptions(c, view.accountID)
					if err != nil {
						return err
					}
					subscriptionsByAccount[view.accountID] = subscriptions
				}
				oldCtx := c.proc.Ctx
				refreshCtx := oldCtx
				if refreshCtx == nil {
					refreshCtx = c.proc.GetTopContext()
				}
				if refreshCtx == nil {
					refreshCtx = context.Background()
				}
				confirmed := false
				c.proc.Ctx = context.WithValue(
					refreshCtx,
					defines.ViewMetadataRefreshKey{},
					viewMetadataRefreshContext{
						retry:                onlyPending,
						sourceAccountID:      source.accountID,
						sourceLogicalID:      source.logicalID,
						currentSourceTableID: source.currentID,
						confirmed:            &confirmed,
						targetViewID:         view.id,
						targetViewVersion:    view.version,
						targetViewDefinition: view.definition,
					},
				)
				c.proc.Ctx = defines.AttachAccountId(c.proc.Ctx, view.accountID)
				err = runViewMetadataRefreshSQL(
					c,
					sql,
					view.viewData,
					subscriptions,
				)
				c.proc.Ctx = oldCtx
				if err != nil {
					if !canSkipViewMetadataRefreshError(err) {
						return err
					}
					if err := markViewMetadataRefreshPending(c, view); err != nil {
						return err
					}
					logutil.Warn("skip refreshing invalid view metadata",
						zap.String("database", view.database),
						zap.String("view", view.name),
						zap.Error(err))
					processedViews[viewKey] = struct{}{}
					continue
				}
				if confirmed {
					processedViews[viewKey] = struct{}{}
					if len(view.viewData.Dependencies) > 0 {
						continue
					}
					logicalID := view.logicalID
					if logicalID == 0 {
						logicalID = view.id
					}
					nextLegacyCandidates = append(nextLegacyCandidates, viewMetadataRefreshSource{
						accountID:  view.accountID,
						logicalID:  logicalID,
						previousID: view.id,
						currentID:  view.id,
						database:   view.database,
						tableName:  view.name,
					})
				}
			}
		}
		for len(nextLegacyCandidates) > 0 {
			count := min(len(nextLegacyCandidates), viewMetadataRefreshPageSize)
			work = append(work, viewMetadataRefreshWork{
				source:           source,
				legacyCandidates: nextLegacyCandidates[:count],
			})
			nextLegacyCandidates = nextLegacyCandidates[count:]
		}
	}
	return nil
}

func markViewMetadataRefreshPending(c *Compile, view viewMetadataRefresh) error {
	if view.viewData.MetadataRefreshPending {
		return nil
	}
	oldCtx := c.proc.Ctx
	c.proc.Ctx = defines.AttachAccountId(oldCtx, view.accountID)
	defer func() { c.proc.Ctx = oldCtx }()
	if err := lockMoDatabase(c, view.database, lock.LockMode_Shared); err != nil {
		return err
	}
	db, err := c.e.Database(c.proc.Ctx, view.database, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	if err = lockMoTable(c, view.database, view.name, lock.LockMode_Exclusive); err != nil {
		return err
	}
	rel, err := db.Relation(c.proc.Ctx, view.name, nil)
	if err != nil {
		return err
	}
	refresh := viewMetadataRefreshContext{
		targetViewID:         view.id,
		targetViewVersion:    view.version,
		targetViewDefinition: view.definition,
	}
	if !viewMetadataRefreshGenerationMatches(c.proc.Ctx, rel, refresh) {
		return moerr.NewTxnNeedRetryWithDefChanged(c.proc.Ctx)
	}
	def := plan2.DeepCopyTableDef(rel.GetTableDef(c.proc.Ctx), true)
	var current plan2.ViewData
	if def.GetViewSql() == nil || json.Unmarshal([]byte(def.GetViewSql().GetView()), &current) != nil {
		return nil
	}
	current.MetadataRefreshPending = true
	encoded, err := json.Marshal(current)
	if err != nil {
		return err
	}
	def.ViewSql = &plan2.ViewDef{View: string(encoded)}
	databaseID, err := strconv.ParseUint(db.GetDatabaseId(c.proc.Ctx), 10, 64)
	if err != nil {
		return err
	}
	c.proc.Ctx = context.WithValue(c.proc.Ctx, defines.ViewMetadataRefreshKey{}, refresh)
	return rel.AlterTable(
		context.WithValue(c.proc.Ctx, defines.SqlKey{}, c.sql),
		nil,
		[]*api.AlterTableReq{api.NewReplaceDefReq(databaseID, rel.GetTableID(c.proc.Ctx), def)},
	)
}

func viewDependenciesContainLiveSource(
	dependencies []plan2.ViewDependency,
	source viewMetadataRefreshSource,
	currentSubscriptions map[string]*plan2.SubscriptionMeta,
) bool {
	for _, dependency := range dependencies {
		if dependency.Snapshot || !dependency.AccountIDSet {
			continue
		}
		if dependency.AccountID == source.accountID &&
			(dependency.LogicalID == source.logicalID ||
				dependency.TableID == source.previousID ||
				dependency.TableID == source.currentID) {
			return true
		}
		if dependency.Subscription {
			if dependency.PublisherAccountIDSet &&
				dependency.PublisherAccountID == source.accountID &&
				strings.EqualFold(dependency.PublisherDB, source.database) &&
				strings.EqualFold(dependency.PublisherTable, source.tableName) {
				return true
			}
			current := currentSubscriptions[strings.ToLower(dependency.SubscriptionDB)]
			if current != nil && uint32(current.GetAccountId()) == source.accountID &&
				strings.EqualFold(current.GetDbName(), source.database) &&
				strings.EqualFold(dependency.SubscriptionTable, source.tableName) &&
				pubsub.InSubMetaTables(current, source.tableName) {
				return true
			}
			continue
		}
		if dependency.AccountID != source.accountID {
			continue
		}
		if strings.EqualFold(dependency.DatabaseName, source.database) &&
			strings.EqualFold(dependency.TableName, source.tableName) {
			return true
		}
	}
	return false
}

func viewDependenciesHaveLiveSubscription(dependencies []plan2.ViewDependency) bool {
	for _, dependency := range dependencies {
		if dependency.Subscription && !dependency.Snapshot && dependency.SubscriptionDB != "" {
			return true
		}
	}
	return false
}

func loadCurrentViewSubscriptions(
	c *Compile,
	accountID uint32,
) (currentViewSubscriptionResolver, error) {
	resolver := currentViewSubscriptionResolver{
		accountID:          accountID,
		byDatabase:         make(map[string]*plan2.SubscriptionMeta),
		snapshotByIdentity: make(map[viewMetadataSnapshotSubscriptionKey]*plan2.SubscriptionMeta),
		loadedSnapshots:    make(map[viewMetadataSnapshotSubscriptionKey]struct{}),
	}
	resolver.loadSnapshot = func(accountID uint32, database string, snapshot *plan2.Snapshot) (*plan2.SubscriptionMeta, error) {
		return loadSnapshotViewSubscription(c, accountID, database, snapshot)
	}
	sql := fmt.Sprintf(
		"select sub_name, pub_account_id, pub_account_name, pub_name, "+
			"pub_database, pub_tables from %s.%s "+
			"where sub_account_id = %d and sub_name is not null and status = %d",
		catalog.MO_CATALOG,
		catalog.MO_SUBS,
		accountID,
		pubsub.SubStatusNormal,
	)
	result, err := c.runSqlWithResultAndOptions(
		sql,
		int32(catalog.System_Account),
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		result.Close()
		return resolver, err
	}
	defer result.Close()
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		subNames := executor.GetStringRows(cols[0])
		pubAccountIDs := executor.GetFixedRows[int32](cols[1])
		pubAccountNames := executor.GetStringRows(cols[2])
		pubNames := executor.GetStringRows(cols[3])
		pubDatabases := executor.GetStringRows(cols[4])
		pubTables := executor.GetStringRows(cols[5])
		for i := 0; i < rows; i++ {
			resolver.byDatabase[strings.ToLower(subNames[i])] = &plan2.SubscriptionMeta{
				Name:        pubNames[i],
				AccountId:   pubAccountIDs[i],
				DbName:      pubDatabases[i],
				AccountName: pubAccountNames[i],
				SubName:     subNames[i],
				Tables:      pubTables[i],
			}
		}
		return true
	})
	return resolver, nil
}

func refreshPendingViewMetadataAfterSubscriptionCreate(c *Compile, subscriptionDatabase string) error {
	subscriberAccountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	subscriptions, err := loadCurrentViewSubscriptions(c, subscriberAccountID)
	if err != nil {
		return err
	}
	meta := subscriptions.byDatabase[strings.ToLower(subscriptionDatabase)]
	if meta == nil {
		return moerr.NewInternalErrorf(
			c.proc.Ctx,
			"subscription metadata for database %s is not visible after creation",
			subscriptionDatabase,
		)
	}

	databaseNames := []string{meta.DbName}
	if strings.EqualFold(meta.DbName, pubsub.TableAll) {
		err = func() error {
			oldCtx := c.proc.Ctx
			c.proc.Ctx = defines.AttachAccountId(oldCtx, uint32(meta.AccountId))
			defer func() { c.proc.Ctx = oldCtx }()
			databaseNames, err = c.e.Databases(c.proc.Ctx, c.proc.GetTxnOperator())
			return err
		}()
		if err != nil {
			return err
		}
	}
	slices.Sort(databaseNames)
	databaseNames = slices.Compact(databaseNames)
	for _, databaseName := range databaseNames {
		var sources []viewMetadataRefreshSource
		err = func() error {
			oldCtx := c.proc.Ctx
			c.proc.Ctx = defines.AttachAccountId(oldCtx, uint32(meta.AccountId))
			defer func() { c.proc.Ctx = oldCtx }()

			publisherDatabase, err := c.e.Database(c.proc.Ctx, databaseName, c.proc.GetTxnOperator())
			if err != nil {
				return err
			}
			var tableNames []string
			if strings.EqualFold(meta.Tables, pubsub.TableAll) {
				tableNames, err = publisherDatabase.Relations(c.proc.Ctx)
				if err != nil {
					return err
				}
			} else {
				for _, tableName := range strings.Split(meta.Tables, pubsub.Sep) {
					if tableName != "" {
						tableNames = append(tableNames, tableName)
					}
				}
			}
			slices.Sort(tableNames)
			tableNames = slices.Compact(tableNames)
			for _, tableName := range tableNames {
				relation, err := publisherDatabase.Relation(c.proc.Ctx, tableName, nil)
				if err != nil {
					return err
				}
				tableID := relation.GetTableID(c.proc.Ctx)
				logicalID := relation.GetTableDef(c.proc.Ctx).GetLogicalId()
				if logicalID == 0 {
					logicalID = tableID
				}
				sources = append(sources, viewMetadataRefreshSource{
					accountID:  uint32(meta.AccountId),
					logicalID:  logicalID,
					previousID: tableID,
					currentID:  tableID,
					database:   databaseName,
					tableName:  tableName,
				})
			}
			return nil
		}()
		if err != nil {
			return err
		}
		for _, source := range sources {
			if err = refreshViewMetadataAfterAlter(
				c,
				source.accountID,
				source.logicalID,
				source.previousID,
				source.currentID,
				source.database,
				source.tableName,
				true,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func loadSnapshotViewSubscription(
	c *Compile,
	accountID uint32,
	database string,
	snapshot *plan2.Snapshot,
) (*plan2.SubscriptionMeta, error) {
	if c == nil || !plan2.IsSnapshotValid(snapshot) {
		return nil, nil
	}
	txnOp := c.proc.GetTxnOperator()
	if snapshot.GetTS().Less(txnOp.Txn().SnapshotTS) {
		txnOp = txnOp.CloneSnapshotOp(*snapshot.GetTS())
	}
	sql := fmt.Sprintf(
		"select sub_name, pub_account_id, pub_account_name, pub_name, "+
			"pub_database, pub_tables from %s.%s "+
			"where sub_account_id = %d and lower(sub_name) = lower(%s) and status = %d",
		catalog.MO_CATALOG,
		catalog.MO_SUBS,
		accountID,
		sqlquote.String(database),
		pubsub.SubStatusNormal,
	)
	result, err := c.runSqlWithResultAndOptionsOnTxn(
		sql,
		int32(catalog.System_Account),
		executor.StatementOption{}.WithDisableLog(),
		txnOp,
	)
	if err != nil {
		result.Close()
		return nil, err
	}
	defer result.Close()

	var subscriptions []*plan2.SubscriptionMeta
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		subNames := executor.GetStringRows(cols[0])
		pubAccountIDs := executor.GetFixedRows[int32](cols[1])
		pubAccountNames := executor.GetStringRows(cols[2])
		pubNames := executor.GetStringRows(cols[3])
		pubDatabases := executor.GetStringRows(cols[4])
		pubTables := executor.GetStringRows(cols[5])
		for i := 0; i < rows; i++ {
			subscriptions = append(subscriptions, &plan2.SubscriptionMeta{
				Name:        pubNames[i],
				AccountId:   pubAccountIDs[i],
				DbName:      pubDatabases[i],
				AccountName: pubAccountNames[i],
				SubName:     subNames[i],
				Tables:      pubTables[i],
			})
		}
		return true
	})
	if len(subscriptions) > 1 {
		return nil, moerr.NewInternalErrorf(
			c.proc.Ctx,
			"find %d subscription records for account %d database %s at snapshot, expect at most 1",
			len(subscriptions),
			accountID,
			database,
		)
	}
	if len(subscriptions) == 0 {
		return nil, nil
	}
	return subscriptions[0], nil
}

const viewMetadataRefreshPageSize = 128

func loadViewMetadataRefreshPage(
	c *Compile,
	sourceAccountID uint32,
	sourceLogicalID uint64,
	sourceTableID uint64,
	sourceDatabase string,
	sourceTable string,
	afterViewID uint64,
	onlyPending bool,
	legacyCandidates []viewMetadataRefreshSource,
) ([]viewMetadataRefresh, error) {
	sql := buildViewMetadataRefreshQueryWithLegacyCandidates(
		sourceAccountID,
		sourceLogicalID,
		sourceTableID,
		sourceDatabase,
		sourceTable,
		afterViewID,
		viewMetadataRefreshPageSize,
		onlyPending,
		legacyCandidates,
	)
	result, err := c.runSqlWithResultAndOptions(
		sql,
		int32(catalog.System_Account),
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		result.Close()
		return nil, err
	}
	defer result.Close()

	views := make([]viewMetadataRefresh, 0, viewMetadataRefreshPageSize)
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		accountIDs := executor.GetFixedRows[uint32](cols[0])
		ids := executor.GetFixedRows[uint64](cols[1])
		logicalIDs := executor.GetFixedRows[uint64](cols[2])
		versions := executor.GetFixedRows[uint32](cols[3])
		databases := executor.GetStringRows(cols[4])
		names := executor.GetStringRows(cols[5])
		definitions := executor.GetStringRows(cols[6])
		for i := 0; i < rows; i++ {
			var viewData plan2.ViewData
			if err := json.Unmarshal([]byte(definitions[i]), &viewData); err != nil {
				logutil.Warn("skip refreshing view with invalid definition",
					zap.String("database", databases[i]),
					zap.String("view", names[i]),
					zap.Error(err))
				views = append(views, viewMetadataRefresh{
					accountID:  accountIDs[i],
					id:         ids[i],
					logicalID:  logicalIDs[i],
					version:    versions[i],
					database:   databases[i],
					name:       names[i],
					definition: definitions[i],
					skip:       true,
				})
				continue
			}
			views = append(views, viewMetadataRefresh{
				accountID:  accountIDs[i],
				id:         ids[i],
				logicalID:  logicalIDs[i],
				version:    versions[i],
				database:   databases[i],
				name:       names[i],
				definition: definitions[i],
				viewData:   viewData,
			})
		}
		return true
	})
	return views, nil
}

func buildViewMetadataRefreshQuery(
	sourceAccountID uint32,
	sourceLogicalID uint64,
	sourceTableID uint64,
	sourceDatabase string,
	sourceTable string,
	afterViewID uint64,
	pageSize int,
	onlyPending ...bool,
) string {
	pending := len(onlyPending) > 0 && onlyPending[0]
	return buildViewMetadataRefreshQueryWithLegacyCandidates(
		sourceAccountID,
		sourceLogicalID,
		sourceTableID,
		sourceDatabase,
		sourceTable,
		afterViewID,
		pageSize,
		pending,
		nil,
	)
}

func buildViewMetadataRefreshQueryWithLegacyCandidates(
	sourceAccountID uint32,
	sourceLogicalID uint64,
	sourceTableID uint64,
	sourceDatabase string,
	sourceTable string,
	afterViewID uint64,
	pageSize int,
	onlyPending bool,
	legacyCandidates []viewMetadataRefreshSource,
) string {
	pendingFilter := ""
	if onlyPending {
		pendingFilter = "and json_unquote(json_extract(viewdef, '$.metadata_refresh_pending')) = 'true' "
	}
	legacyBatchPredicate := buildViewMetadataLegacyBatchPredicate(legacyCandidates)
	if legacyBatchPredicate != "" {
		return fmt.Sprintf(
			"select account_id, rel_id, rel_logical_id, rel_version, reldatabase, relname, viewdef from %s.mo_tables "+
				"where relkind = '%s' %s"+
				"and reldatabase not in ('%s', '%s') and rel_id > %d "+
				"and (%s) order by rel_id limit %d",
			catalog.MO_CATALOG,
			catalog.SystemViewRel,
			pendingFilter,
			catalog.MO_CATALOG,
			"information_schema",
			afterViewID,
			strings.TrimPrefix(legacyBatchPredicate, " or "),
			pageSize,
		)
	}
	legacyCandidate, quotedLegacyCandidate, ansiQuotedLegacyCandidate :=
		viewMetadataLegacyNameCandidates(sourceTable)
	databaseNameJSON, _ := json.Marshal(sourceDatabase)
	tableNameJSON, _ := json.Marshal(sourceTable)
	qualifiedNameCandidate := sqlquote.String(
		"\"database_name\":" + string(databaseNameJSON) +
			",\"table_name\":" + string(tableNameJSON),
	)
	publisherQualifiedNameCandidate := sqlquote.String(
		"\"publisher_db\":" + string(databaseNameJSON) +
			",\"publisher_table\":" + string(tableNameJSON),
	)
	subscriptionNamePrefix := sqlquote.String("\"subscription_db\":")
	subscriptionTableSuffix := sqlquote.String(
		",\"subscription_table\":" + string(tableNameJSON),
	)
	if pendingFilter != "" {
		return fmt.Sprintf(
			"select account_id, rel_id, rel_logical_id, rel_version, reldatabase, relname, viewdef from %s.mo_tables "+
				"where relkind = '%s' %s"+
				"and reldatabase not in ('%s', '%s') and rel_id > %d "+
				"and ((account_id = %d and json_extract(viewdef, '$.dependencies') is null "+
				"and (instr(viewdef, %s) > 0 or instr(viewdef, %s) > 0 or instr(viewdef, %s) > 0)) "+
				"or (viewdef like '%%\\\"account_id\\\":%d,%%' "+
				"and (viewdef like '%%\\\"logical_id\\\":%d,%%' "+
				"or viewdef like '%%\\\"table_id\\\":%d,%%')) "+
				"or (account_id = %d and viewdef like '%%\\\"table_id\\\":%d,%%' "+
				"and viewdef not like '%%\\\"logical_id\\\":%%') "+
				"or (json_extract(viewdef, '$.dependencies') is not null "+
				"and ((account_id = %d and instr(viewdef, %s) > 0) "+
				"or instr(viewdef, %s) > 0)) "+
				"or (json_extract(viewdef, '$.dependencies') is null "+
				"and (instr(lower(viewdef), lower(%s)) > 0 or instr(lower(viewdef), lower(%s)) > 0 "+
				"or instr(lower(viewdef), lower(%s)) > 0) "+
				"and exists (select 1 from %s.%s where sub_account_id = account_id "+
				"and pub_account_id = %d and sub_name is not null and status = %d "+
				"and (lower(pub_database) = lower(%s) or pub_database = %s) "+
				"and (pub_tables = %s or find_in_set(lower(%s), lower(pub_tables)) > 0) "+
				"and instr(lower(viewdef), lower(sub_name)) > 0))%s) "+
				"order by rel_id limit %d",
			catalog.MO_CATALOG,
			catalog.SystemViewRel,
			pendingFilter,
			catalog.MO_CATALOG,
			"information_schema",
			afterViewID,
			sourceAccountID,
			legacyCandidate,
			quotedLegacyCandidate,
			ansiQuotedLegacyCandidate,
			sourceAccountID,
			sourceLogicalID,
			sourceTableID,
			sourceAccountID,
			sourceTableID,
			sourceAccountID,
			qualifiedNameCandidate,
			publisherQualifiedNameCandidate,
			legacyCandidate,
			quotedLegacyCandidate,
			ansiQuotedLegacyCandidate,
			catalog.MO_CATALOG,
			catalog.MO_SUBS,
			sourceAccountID,
			pubsub.SubStatusNormal,
			sqlquote.String(sourceDatabase),
			sqlquote.String(pubsub.TableAll),
			sqlquote.String(pubsub.TableAll),
			legacyCandidate,
			legacyBatchPredicate,
			pageSize,
		)
	}
	return fmt.Sprintf(
		"select account_id, rel_id, rel_logical_id, rel_version, reldatabase, relname, viewdef from %s.mo_tables "+
			"where relkind = '%s' %s"+
			"and reldatabase not in ('%s', '%s') and rel_id > %d "+
			"and ((((account_id = %d) or account_id in "+
			"(select sub_account_id from %s.%s where pub_account_id = %d and status = %d)) "+
			"and json_extract(viewdef, '$.dependencies') is null "+
			"and (instr(viewdef, %s) > 0 or instr(viewdef, %s) > 0 or instr(viewdef, %s) > 0)) "+
			"or (viewdef like '%%\\\"account_id\\\":%d,%%' "+
			"and (viewdef like '%%\\\"logical_id\\\":%d,%%' "+
			"or viewdef like '%%\\\"table_id\\\":%d,%%')) "+
			"or (account_id = %d "+
			"and viewdef like '%%\\\"table_id\\\":%d,%%' "+
			"and viewdef not like '%%\\\"logical_id\\\":%%') "+
			"or ((((account_id = %d) or account_id in "+
			"(select sub_account_id from %s.%s where pub_account_id = %d and status = %d)) "+
			"and json_extract(viewdef, '$.dependencies') is not null and "+
			"(instr(viewdef, %s) > 0 or instr(viewdef, %s) > 0)) "+
			"or exists (select 1 from %s.%s where sub_account_id = account_id "+
			"and pub_account_id = %d and lower(pub_database) = lower(%s) and status = %d "+
			"and instr(viewdef, concat(%s, char(34), "+
			"replace(replace(sub_name, char(92), concat(char(92), char(92))), "+
			"char(34), concat(char(92), char(34))), char(34), %s)) > 0))%s) "+
			"order by rel_id limit %d",
		catalog.MO_CATALOG,
		catalog.SystemViewRel,
		pendingFilter,
		catalog.MO_CATALOG,
		"information_schema",
		afterViewID,
		sourceAccountID,
		catalog.MO_CATALOG,
		catalog.MO_SUBS,
		sourceAccountID,
		pubsub.SubStatusNormal,
		legacyCandidate,
		quotedLegacyCandidate,
		ansiQuotedLegacyCandidate,
		sourceAccountID,
		sourceLogicalID,
		sourceTableID,
		sourceAccountID,
		sourceTableID,
		sourceAccountID,
		catalog.MO_CATALOG,
		catalog.MO_SUBS,
		sourceAccountID,
		pubsub.SubStatusNormal,
		qualifiedNameCandidate,
		publisherQualifiedNameCandidate,
		catalog.MO_CATALOG,
		catalog.MO_SUBS,
		sourceAccountID,
		sqlquote.String(sourceDatabase),
		pubsub.SubStatusNormal,
		subscriptionNamePrefix,
		subscriptionTableSuffix,
		legacyBatchPredicate,
		pageSize,
	)
}

func buildViewMetadataLegacyBatchPredicate(candidates []viewMetadataRefreshSource) string {
	var predicate strings.Builder
	for _, candidate := range candidates {
		rawName, quotedName, ansiQuotedName := viewMetadataLegacyNameCandidates(candidate.tableName)
		fmt.Fprintf(
			&predicate,
			" or (account_id = %d and json_extract(viewdef, '$.dependencies') is null "+
				"and (instr(viewdef, %s) > 0 or instr(viewdef, %s) > 0 or instr(viewdef, %s) > 0))",
			candidate.accountID,
			rawName,
			quotedName,
			ansiQuotedName,
		)
	}
	return predicate.String()
}

func viewMetadataLegacyNameCandidates(name string) (string, string, string) {
	quotedNameJSON, _ := json.Marshal(sqlquote.Ident(name))
	ansiQuotedName := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	ansiQuotedNameJSON, _ := json.Marshal(ansiQuotedName)
	return sqlquote.String(name),
		sqlquote.String(string(quotedNameJSON[1 : len(quotedNameJSON)-1])),
		sqlquote.String(string(ansiQuotedNameJSON[1 : len(ansiQuotedNameJSON)-1]))
}

func runViewMetadataRefreshSQL(
	c *Compile,
	sql string,
	viewData plan2.ViewData,
	subscriptions currentViewSubscriptionResolver,
) error {
	oldDatabase := c.db
	oldCtx := c.proc.Ctx
	oldResolveVariable := c.proc.GetResolveVariableFunc()
	oldSQLMode := c.proc.GetSessionInfo().SqlMode
	lower := c.getLower()
	sqlMode := plan2.LegacyViewParserSQLMode()
	if viewData.SQLMode != nil {
		sqlMode = *viewData.SQLMode
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	c.db = viewData.DefaultDatabase
	c.proc.GetSessionInfo().SqlMode = sqlMode
	c.proc.Ctx = context.WithValue(
		c.proc.Ctx,
		viewMetadataSubscriptionResolverKey{},
		viewMetadataSubscriptionResolver(subscriptions),
	)
	c.proc.Ctx = context.WithValue(
		c.proc.Ctx,
		viewMetadataResolverKey{},
		viewMetadataRefreshResolver{
			compile:         c,
			accountID:       accountID,
			defaultDatabase: viewData.DefaultDatabase,
			subscriptions:   subscriptions,
			dependencies:    viewData.Dependencies,
		},
	)
	c.proc.Ctx = context.WithValue(c.proc.Ctx, viewMetadataSQLModeKey{}, sqlMode)
	if helper := c.proc.GetSessionInfo().SqlHelper; helper != nil {
		if compilerContext, ok := helper.GetCompilerContext().(plan2.CompilerContext); ok {
			c.proc.Ctx = context.WithValue(
				c.proc.Ctx,
				viewMetadataCompilerContextKey{},
				compilerContext,
			)
		}
	}
	c.proc.SetResolveVariableFunc(func(name string, isSystemVar, isGlobalVar bool) (any, error) {
		if isSystemVar && !isGlobalVar && strings.EqualFold(name, "sql_mode") {
			return sqlMode, nil
		}
		if isSystemVar && !isGlobalVar && strings.EqualFold(name, "lower_case_table_names") {
			return lower, nil
		}
		if oldResolveVariable == nil {
			return nil, nil
		}
		return oldResolveVariable(name, isSystemVar, isGlobalVar)
	})
	defer func() {
		c.db = oldDatabase
		c.proc.Ctx = oldCtx
		c.proc.GetSessionInfo().SqlMode = oldSQLMode
		c.proc.SetResolveVariableFunc(oldResolveVariable)
	}()
	return c.runSqlWithOptions(sql, executor.StatementOption{}.WithDisableLog())
}

func canSkipViewMetadataRefreshError(err error) bool {
	var planErr *viewMetadataRefreshPlanError
	if !errors.As(err, &planErr) {
		return false
	}
	var snapshotNotFound *viewMetadataSnapshotNotFoundError
	if errors.As(planErr.err, &snapshotNotFound) {
		return true
	}
	var udfNotFound *viewMetadataUDFNotFoundError
	if errors.As(planErr.err, &udfNotFound) {
		return true
	}
	code, ok := moerr.GetMoErrCode(planErr.err)
	if !ok {
		return false
	}
	switch code {
	case moerr.ErrInvalidInput,
		moerr.ErrConstraintViolation,
		moerr.ErrParseError,
		moerr.ErrBadFieldError,
		moerr.ErrOperandColumns,
		moerr.ErrViewWrongList,
		moerr.ErrBadDB,
		moerr.ErrNoSuchTable,
		moerr.ErrNoDB,
		moerr.ErrBadView:
		return true
	default:
		return false
	}
}

func CanSkipViewMetadataRefreshError(err error) bool {
	if canSkipViewMetadataRefreshError(err) || plan2.IsSnapshotNotFound(err) {
		return true
	}
	code, ok := moerr.GetMoErrCode(err)
	if !ok {
		return false
	}
	switch code {
	case moerr.ErrInvalidInput,
		moerr.ErrConstraintViolation,
		moerr.ErrParseError,
		moerr.ErrBadFieldError,
		moerr.ErrOperandColumns,
		moerr.ErrViewWrongList,
		moerr.ErrBadDB,
		moerr.ErrNoSuchTable,
		moerr.ErrNoDB,
		moerr.ErrBadView,
		moerr.ErrNotSupported:
		return true
	default:
		return false
	}
}

func buildRefreshViewSQL(
	ctx context.Context,
	lower int64,
	view viewMetadataRefresh,
) (string, error) {
	sqlMode := plan2.LegacyViewParserSQLMode()
	if view.viewData.SQLMode != nil {
		sqlMode = *view.viewData.SQLMode
	}
	stmts, err := mysql.ParseWithSQLMode(ctx, view.viewData.Stmt, lower, sqlMode)
	if err != nil {
		return "", err
	}
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	if len(stmts) != 1 {
		return "", moerr.NewParseError(ctx, "invalid view definition")
	}

	var colNames tree.IdentifierList
	var source *tree.Select
	switch stmt := stmts[0].(type) {
	case *tree.CreateView:
		colNames = stmt.ColNames
		source = stmt.AsSource
	case *tree.AlterView:
		colNames = stmt.ColNames
		source = stmt.AsSource
	default:
		return "", moerr.NewParseError(ctx, "invalid view definition")
	}

	fmtCtx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
	)
	fmtCtx.WriteString("alter view ")
	fmtCtx.WriteString(quoteAlterCopyTableName(view.database, view.name))
	if len(colNames) > 0 {
		fmtCtx.WriteString(" (")
		colNames.Format(fmtCtx)
		fmtCtx.WriteByte(')')
	}
	fmtCtx.WriteString(" as ")
	source.Format(fmtCtx)
	return fmtCtx.String(), nil
}

func BuildViewMetadataRefreshSQL(
	ctx context.Context,
	lower int64,
	database string,
	name string,
	viewData plan2.ViewData,
) (string, error) {
	return buildRefreshViewSQL(ctx, lower, viewMetadataRefresh{
		database: database,
		name:     name,
		viewData: viewData,
	})
}

func (s *Scope) RenameTable(c *Compile) (err error) {
	if s.ScopeAnalyzer == nil {
		s.ScopeAnalyzer = NewScopeAnalyzer()
	}
	s.ScopeAnalyzer.Start()
	defer s.ScopeAnalyzer.Stop()

	qry := s.Plan.GetDdl().GetRenameTable()
	targetSet := collectRenameViewMetadataTargets(qry)
	oldCtx := c.proc.Ctx
	c.proc.Ctx = context.WithValue(oldCtx, deferRenameViewMetadataRecoveryKey{}, true)
	defer func() { c.proc.Ctx = oldCtx }()
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
	c.proc.Ctx = oldCtx
	return refreshPendingViewMetadataAfterRename(c, targetSet)
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
	addRefChildTableIDs(oldCt, refChildTbls)
	return copyRel.UpdateConstraint(c.proc.Ctx, oldCt)
}

// notifyParentTableFkTableIdChange Notify the parent table of changes in the tableid of the foreign key table
func reconcileParentRefChildTableID(
	constraintDef *engine.ConstraintDef,
	oldTableID uint64,
	newTableID uint64,
) {
	reconcileRefChildTableID(constraintDef, oldTableID, newTableID)
}

func notifyParentTableFkTableIdChange(
	c *Compile,
	fkey *plan.ForeignKeyDef,
	oldTableID uint64,
	newTableID uint64,
) error {
	foreignTblId := fkey.ForeignTbl
	if foreignTblId == 0 {
		// Self-referencing foreign keys use 0 as the parent-table sentinel.
		// The ALTER copy is already carrying that constraint on newRel, and
		// there is no separate parent relation to update.
		return nil
	}
	_, _, fatherRelation, err := c.e.GetRelationById(c.proc.Ctx, c.proc.GetTxnOperator(), foreignTblId)
	if err != nil {
		return err
	}
	oldCt, err := GetConstraintDef(c.proc.Ctx, fatherRelation)
	if err != nil {
		return err
	}
	reconcileParentRefChildTableID(oldCt, oldTableID, newTableID)
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

		async, err := catalog.IsIndexAsync(oriIdxTblNames.IndexAlgoParams)
		if err != nil {
			return err
		}

		// Per-algo clone semantics live entirely on the plugin's
		// AlterTableCloneBehavior, which declares two mutually exclusive
		// policies:
		//   - SkipWholeIndex: skip the entire index when async. Algorithms that
		//     leave every hidden table empty at CREATE and rebuild all of them
		//     via CDC from ts=0 (HNSW / CAGRA / IVF-PQ / fulltext). HNSW is
		//     AlwaysAsync; the others gate on the per-index async param.
		//   - DeleteBeforeClone + SkipWhenAsync (per hidden table): IVF-FLAT is
		//     the only case today. All three hidden tables get DELETE'd (the
		//     CREATE on the temp table already seeded them), entries are
		//     additionally skipped when async (CDC rebuilds entries from ts=0),
		//     while metadata + centroids ARE cloned so the sinker has a k-means
		//     model to write against.
		var cloneBehavior catalogplugin.AlterTableCloneBehavior
		if !oriIdxTblNames.Unique {
			if p, ok := indexplugin.Get(oriIdxTblNames.IndexAlgo); ok {
				d := p.Catalog().SyncDescriptor()
				cloneBehavior = p.Catalog().AlterTableCloneBehavior()
				// Whole-index skip is an EXPLICIT policy (SkipWholeIndex), not
				// inferred from UsesCDC — a CDC algorithm can still need its model
				// tables cloned (IVF-FLAT clones metadata + centroids and only
				// CDC-rebuilds entries via the per-hidden-table policy below).
				// HNSW is AlwaysAsync; CAGRA / IVF-PQ / fulltext gate on the
				// per-index async param.
				if (d.AlwaysAsync || async) && cloneBehavior.SkipWholeIndex {
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
