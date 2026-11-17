// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type restoredBranchLineage struct {
	parentTableID uint64
	cloneTS       int64
	creator       uint64
	level         string
}

// restoreBranchLineage restitches logical branch identity after table or
// database restore recreates a table with a new physical ID. Account and
// cluster restore already restore the identity-bearing catalog rows directly.
func restoreBranchLineage(
	ctx context.Context,
	bh BackgroundExec,
	receipt *cloneReceipt,
) error {
	level, _ := ctx.Value(tree.CloneLevelCtxKey{}).(tree.CloneLevelType)
	switch level {
	case tree.RestoreCloneLevelTable, tree.RestoreCloneLevelDatabase:
	default:
		return nil
	}

	if err := resolveRestoredBranchTableIDs(ctx, bh, receipt); err != nil {
		return err
	}
	return restitchRestoredBranchLineage(ctx, bh, receipt)
}

func restoredBranchTableIDSQL(receipt *cloneReceipt, historical bool) string {
	accountID := receipt.toAccount
	databaseName := receipt.dstDb
	tableName := receipt.dstTbl
	timestamp := ""
	if historical {
		accountID = receipt.srcAccount
		databaseName = receipt.srcDb
		tableName = receipt.srcTbl
		timestamp = fmt.Sprintf(" {MO_TS = %d}", receipt.snapshotTS)
	}
	return fmt.Sprintf(
		"select rel_id from %s.%s%s where account_id = %d and reldatabase = %s and relname = %s",
		catalog.MO_CATALOG,
		catalog.MO_TABLES,
		timestamp,
		accountID,
		quoteSQLStringLiteral(databaseName),
		quoteSQLStringLiteral(tableName),
	)
}

func loadRestoredBranchTableID(
	ctx context.Context,
	bh BackgroundExec,
	receipt *cloneReceipt,
	historical bool,
) (uint64, error) {
	results, err := historicalLineageQuery(ctx, bh, restoredBranchTableIDSQL(receipt, historical))
	if err != nil {
		return 0, err
	}

	databaseName := receipt.dstDb
	tableName := receipt.dstTbl
	if historical {
		databaseName = receipt.srcDb
		tableName = receipt.srcTbl
	}
	var tableID uint64
	found := false
	for _, result := range results {
		for row := uint64(0); row < result.GetRowCount(); row++ {
			if found {
				return 0, moerr.NewInternalErrorf(ctx,
					"restore branch lineage: multiple physical rows for table %s.%s",
					databaseName, tableName)
			}
			if tableID, err = result.GetUint64(ctx, row, 0); err != nil {
				return 0, err
			}
			found = true
		}
	}
	if !found {
		return 0, moerr.NewInternalErrorf(ctx,
			"restore branch lineage: cannot resolve physical ID for table %s.%s",
			databaseName, tableName)
	}
	return tableID, nil
}

func resolveRestoredBranchTableIDs(
	ctx context.Context,
	bh BackgroundExec,
	receipt *cloneReceipt,
) error {
	sourceCtx := defines.AttachAccountId(ctx, receipt.srcAccount)
	sourceID, err := loadRestoredBranchTableID(sourceCtx, bh, receipt, true)
	if err != nil {
		return err
	}
	destinationCtx := defines.AttachAccountId(ctx, receipt.toAccount)
	destinationID, err := loadRestoredBranchTableID(destinationCtx, bh, receipt, false)
	if err != nil {
		return err
	}
	receipt.srcTableID = sourceID
	receipt.dstTableID = destinationID
	return nil
}

func restoredBranchLineageMetadataSQL(receipt *cloneReceipt) string {
	return fmt.Sprintf(
		"select p_table_id, clone_ts, creator, level from %s.%s {MO_TS = %d} where table_id = %d and table_deleted = false",
		catalog.MO_CATALOG,
		catalog.MO_BRANCH_METADATA,
		receipt.snapshotTS,
		receipt.srcTableID,
	)
}

func restoredBranchAncestorLineageMetadataSQL(receipt *cloneReceipt, tableID uint64) string {
	return fmt.Sprintf(
		"select p_table_id, clone_ts, creator, level from %s.%s {MO_TS = %d} where table_id = %d",
		catalog.MO_CATALOG,
		catalog.MO_BRANCH_METADATA,
		receipt.snapshotTS,
		tableID,
	)
}

func restoredBranchProtectionSQLForTable(snapshotTS int64, tableID uint64, historical bool) string {
	timestamp := ""
	if historical {
		timestamp = fmt.Sprintf(" {MO_TS = %d}", snapshotTS)
	}
	return fmt.Sprintf(
		"select snapshot_id, sname, ts, level, account_name, database_name, table_name, obj_id, kind from %s.%s%s where sname = %s and kind = %s",
		catalog.MO_CATALOG,
		catalog.MO_SNAPSHOTS,
		timestamp,
		quoteSQLStringLiteral(branchSnapshotName(tableID)),
		quoteSQLStringLiteral(branchSnapshotKind),
	)
}

func restoredBranchProtectionSQL(receipt *cloneReceipt) string {
	return restoredBranchProtectionSQLForTable(receipt.snapshotTS, receipt.srcTableID, true)
}

func restoredBranchAncestorProtectionSQL(receipt *cloneReceipt, tableID uint64) string {
	return restoredBranchProtectionSQLForTable(receipt.snapshotTS, tableID, true)
}

func currentRestoredBranchProtectionSQL(tableID uint64) string {
	return restoredBranchProtectionSQLForTable(0, tableID, false)
}

func loadRestoredBranchLineageRow(
	ctx context.Context,
	bh BackgroundExec,
	sql string,
	tableID uint64,
	duplicateRows string,
) (*restoredBranchLineage, error) {
	results, err := historicalLineageQuery(ctx, bh, sql)
	if err != nil {
		return nil, err
	}

	var lineage *restoredBranchLineage
	for _, result := range results {
		for row := uint64(0); row < result.GetRowCount(); row++ {
			if lineage != nil {
				return nil, moerr.NewInternalErrorf(ctx,
					"restore branch lineage: multiple %s for table %d", duplicateRows, tableID)
			}
			lineage = &restoredBranchLineage{}
			if lineage.parentTableID, err = result.GetUint64(ctx, row, 0); err != nil {
				return nil, err
			}
			if lineage.cloneTS, err = result.GetInt64(ctx, row, 1); err != nil {
				return nil, err
			}
			if lineage.creator, err = result.GetUint64(ctx, row, 2); err != nil {
				return nil, err
			}
			if lineage.level, err = result.GetString(ctx, row, 3); err != nil {
				return nil, err
			}
		}
	}
	return lineage, nil
}

func loadRestoredBranchLineage(
	ctx context.Context,
	bh BackgroundExec,
	receipt *cloneReceipt,
) (*restoredBranchLineage, error) {
	return loadRestoredBranchLineageRow(
		ctx, bh, restoredBranchLineageMetadataSQL(receipt), receipt.srcTableID, "active rows",
	)
}

func loadRestoredBranchProtectionRow(
	ctx context.Context,
	bh BackgroundExec,
	sql string,
	tableID uint64,
) (*snapshotRecord, error) {
	results, err := historicalLineageQuery(ctx, bh, sql)
	if err != nil {
		return nil, err
	}

	var protection *snapshotRecord
	for _, result := range results {
		for row := uint64(0); row < result.GetRowCount(); row++ {
			if protection != nil {
				return nil, moerr.NewInternalErrorf(ctx,
					"restore branch lineage: multiple protection rows for table %d", tableID)
			}
			protection = &snapshotRecord{}
			if protection.snapshotId, err = result.GetString(ctx, row, 0); err != nil {
				return nil, err
			}
			if protection.snapshotName, err = result.GetString(ctx, row, 1); err != nil {
				return nil, err
			}
			if protection.ts, err = result.GetInt64(ctx, row, 2); err != nil {
				return nil, err
			}
			if protection.level, err = result.GetString(ctx, row, 3); err != nil {
				return nil, err
			}
			if protection.accountName, err = result.GetString(ctx, row, 4); err != nil {
				return nil, err
			}
			if protection.databaseName, err = result.GetString(ctx, row, 5); err != nil {
				return nil, err
			}
			if protection.tableName, err = result.GetString(ctx, row, 6); err != nil {
				return nil, err
			}
			if protection.objId, err = result.GetUint64(ctx, row, 7); err != nil {
				return nil, err
			}
			if protection.kind, err = result.GetString(ctx, row, 8); err != nil {
				return nil, err
			}
		}
	}
	return protection, nil
}

func loadRestoredBranchProtection(
	ctx context.Context,
	bh BackgroundExec,
	receipt *cloneReceipt,
) (*snapshotRecord, error) {
	return loadRestoredBranchProtectionRow(
		ctx, bh, restoredBranchProtectionSQL(receipt), receipt.srcTableID,
	)
}

type restoredBranchProtectionInsert struct {
	childTableID uint64
	record       *snapshotRecord
}

type restoredBranchAncestor struct {
	tableID    uint64
	lineage    *restoredBranchLineage
	protection *snapshotRecord
}

func restoredBranchProtectionMatchesLineage(
	protection *snapshotRecord,
	lineage *restoredBranchLineage,
) bool {
	return protection.objId == lineage.parentTableID && protection.ts == lineage.cloneTS
}

func collectRestoredBranchHistory(
	ctx context.Context,
	bh BackgroundExec,
	receipt *cloneReceipt,
	lineage *restoredBranchLineage,
) (*snapshotRecord, []restoredBranchAncestor, error) {
	directProtection, err := loadRestoredBranchProtection(ctx, bh, receipt)
	if err != nil {
		return nil, nil, err
	}
	if directProtection != nil && !restoredBranchProtectionMatchesLineage(directProtection, lineage) {
		return nil, nil, moerr.NewInternalErrorf(ctx,
			"restore branch lineage: protection for table %d does not match its lineage",
			receipt.srcTableID)
	}

	// A restored descendant makes every ancestor edge reachable again.
	// Ancestors may already be deleted at the restore timestamp, so their
	// historical lineage lookup deliberately includes deleted rows.
	var ancestors []restoredBranchAncestor
	visited := make(map[uint64]struct{})
	visited[receipt.srcTableID] = struct{}{}
	for ancestorID := lineage.parentTableID; ancestorID != 0; {
		if _, seen := visited[ancestorID]; seen {
			return nil, nil, moerr.NewInternalErrorf(ctx,
				"restore branch lineage: ancestor cycle at table %d", ancestorID)
		}
		visited[ancestorID] = struct{}{}

		ancestor, err := loadRestoredBranchLineageRow(
			ctx, bh, restoredBranchAncestorLineageMetadataSQL(receipt, ancestorID),
			ancestorID, "historical rows",
		)
		if err != nil {
			return nil, nil, err
		}
		protection, err := loadRestoredBranchProtectionRow(
			ctx, bh, restoredBranchAncestorProtectionSQL(receipt, ancestorID), ancestorID,
		)
		if err != nil {
			return nil, nil, err
		}
		if ancestor == nil {
			if protection != nil {
				return nil, nil, moerr.NewInternalErrorf(ctx,
					"restore branch lineage: protection for table %d has no historical lineage",
					ancestorID)
			}
			break
		}
		if protection != nil && !restoredBranchProtectionMatchesLineage(protection, ancestor) {
			return nil, nil, moerr.NewInternalErrorf(ctx,
				"restore branch lineage: protection for table %d does not match its lineage",
				ancestorID)
		}
		ancestors = append(ancestors, restoredBranchAncestor{
			tableID:    ancestorID,
			lineage:    ancestor,
			protection: protection,
		})
		ancestorID = ancestor.parentTableID
	}
	return directProtection, ancestors, nil
}

func loadCurrentRestoredBranchLineage(
	ctx context.Context,
	bh BackgroundExec,
) (map[uint64][]databranchutils.DataBranchMetadata, error) {
	results, err := historicalLineageQuery(ctx, bh, historicalAlterLineageMetadataSQL())
	if err != nil {
		return nil, err
	}
	rows := make(map[uint64][]databranchutils.DataBranchMetadata)
	for _, result := range results {
		for row := uint64(0); row < result.GetRowCount(); row++ {
			var metadata databranchutils.DataBranchMetadata
			if metadata.TableID, err = result.GetUint64(ctx, row, 0); err != nil {
				return nil, err
			}
			if metadata.PTableID, err = result.GetUint64(ctx, row, 1); err != nil {
				return nil, err
			}
			if metadata.CloneTS, err = result.GetInt64(ctx, row, 2); err != nil {
				return nil, err
			}
			if metadata.Creator, err = result.GetUint64(ctx, row, 3); err != nil {
				return nil, err
			}
			if metadata.Level, err = result.GetString(ctx, row, 4); err != nil {
				return nil, err
			}
			deleted, err := result.GetInt64(ctx, row, 5)
			if err != nil {
				return nil, err
			}
			metadata.TableDeleted = deleted != 0
			rows[metadata.TableID] = append(rows[metadata.TableID], metadata)
		}
	}
	return rows, nil
}

func refreshRestoredBranchLineageSnapshot(ctx context.Context, bh BackgroundExec) error {
	back, ok := bh.(*backExec)
	if !ok {
		return nil
	}
	txnOp := back.GetTxnOperator()
	if txnOp == nil || txnOp.GetWorkspace() == nil {
		return moerr.NewInternalError(ctx, "restore branch lineage: missing transaction workspace")
	}
	if !txnOp.Txn().IsPessimistic() || !txnOp.Txn().IsRCIsolation() {
		return moerr.NewInternalError(ctx,
			"restore branch lineage requires a pessimistic read committed transaction")
	}
	service := back.Service()
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return moerr.NewInternalError(ctx, "restore branch lineage: missing service runtime")
	}
	txnClient := getPu(service).TxnClient
	if txnClient == nil {
		return moerr.NewInternalError(ctx, "restore branch lineage: missing transaction client")
	}
	now, _ := rt.Clock().Now()
	applied, err := txnClient.WaitLogTailAppliedAt(ctx, now)
	if err != nil {
		return err
	}
	return txnOp.GetWorkspace().AdvanceSnapshot(ctx, applied)
}

func lockRestoredBranchLineageDAG(
	ctx context.Context,
	bh BackgroundExec,
) (map[uint64][]databranchutils.DataBranchMetadata, error) {
	// This is the same full-metadata lock used by branch reclamation and ALTER
	// compaction. Hold it in the surrounding restore transaction through commit.
	if _, err := historicalLineageQuery(ctx, bh, historicalAlterLineageMetadataSQL()); err != nil {
		return nil, err
	}
	// A locking read can wait without advancing its transaction snapshot. After
	// acquiring the DAG lock, cross the applied log tail and re-read both metadata
	// and protection so a completed sibling reclaim cannot be missed.
	if err := refreshRestoredBranchLineageSnapshot(ctx, bh); err != nil {
		return nil, err
	}
	return loadCurrentRestoredBranchLineage(ctx, bh)
}

func restoredBranchCurrentLineageMatches(
	current databranchutils.DataBranchMetadata,
	historical *restoredBranchLineage,
) bool {
	return current.PTableID == historical.parentTableID &&
		current.CloneTS == historical.cloneTS &&
		current.Creator == historical.creator &&
		current.Level == historical.level
}

func reconcileRestoredBranchLineage(
	ctx context.Context,
	bh BackgroundExec,
	receipt *cloneReceipt,
	lineage *restoredBranchLineage,
	directProtection *snapshotRecord,
	ancestors []restoredBranchAncestor,
	current map[uint64][]databranchutils.DataBranchMetadata,
) ([]databranchutils.DataBranchMetadata, []restoredBranchProtectionInsert, error) {
	if len(current[receipt.dstTableID]) != 0 {
		return nil, nil, moerr.NewInternalErrorf(ctx,
			"restore branch lineage: current lineage already exists for restored table %d",
			receipt.dstTableID)
	}

	metadataInserts := make([]databranchutils.DataBranchMetadata, 0, len(ancestors))
	for _, ancestor := range ancestors {
		rows := current[ancestor.tableID]
		if len(rows) > 1 {
			return nil, nil, moerr.NewInternalErrorf(ctx,
				"restore branch lineage: multiple current rows for table %d", ancestor.tableID)
		}
		if len(rows) == 1 {
			if !restoredBranchCurrentLineageMatches(rows[0], ancestor.lineage) {
				return nil, nil, moerr.NewInternalErrorf(ctx,
					"restore branch lineage: current lineage for table %d does not match its historical lineage",
					ancestor.tableID)
			}
			// In particular, never resurrect a deleted ancestor or change its level.
			continue
		}
		if !databranchutils.IsAlterLineageLevel(ancestor.lineage.level) {
			return nil, nil, moerr.NewInternalErrorf(ctx,
				"restore branch lineage: current lineage for ordinary ancestor %d is missing",
				ancestor.tableID)
		}
		// ALTER compaction can remove an unowned physical generation. Recreate
		// only that historical edge, as deleted history; the restored direct table
		// is the sole row made live by this repair.
		metadataInserts = append(metadataInserts, databranchutils.DataBranchMetadata{
			TableID:      ancestor.tableID,
			PTableID:     ancestor.lineage.parentTableID,
			CloneTS:      ancestor.lineage.cloneTS,
			Creator:      ancestor.lineage.creator,
			Level:        ancestor.lineage.level,
			TableDeleted: true,
		})
	}

	protectionInserts := make([]restoredBranchProtectionInsert, 0, len(ancestors)+1)
	if directProtection != nil {
		currentProtection, err := loadRestoredBranchProtectionRow(
			ctx, bh, currentRestoredBranchProtectionSQL(receipt.dstTableID), receipt.dstTableID,
		)
		if err != nil {
			return nil, nil, err
		}
		if currentProtection != nil {
			if !restoredBranchProtectionMatchesLineage(currentProtection, lineage) {
				return nil, nil, moerr.NewInternalErrorf(ctx,
					"restore branch lineage: current protection for table %d does not match its historical lineage",
					receipt.dstTableID)
			}
		} else {
			protectionInserts = append(protectionInserts, restoredBranchProtectionInsert{
				childTableID: receipt.dstTableID,
				record:       directProtection,
			})
		}
	}

	for _, ancestor := range ancestors {
		// ALTER compaction retains ownership of ALTER protection. Restore walks
		// those generations but republishes only ordinary branch protections.
		if ancestor.protection == nil || databranchutils.IsAlterLineageLevel(ancestor.lineage.level) {
			continue
		}
		currentProtection, err := loadRestoredBranchProtectionRow(
			ctx, bh, currentRestoredBranchProtectionSQL(ancestor.tableID), ancestor.tableID,
		)
		if err != nil {
			return nil, nil, err
		}
		if currentProtection != nil {
			if !restoredBranchProtectionMatchesLineage(currentProtection, ancestor.lineage) {
				return nil, nil, moerr.NewInternalErrorf(ctx,
					"restore branch lineage: current protection for table %d does not match its historical lineage",
					ancestor.tableID)
			}
			continue
		}
		protectionInserts = append(protectionInserts, restoredBranchProtectionInsert{
			childTableID: ancestor.tableID,
			record:       ancestor.protection,
		})
	}
	return metadataInserts, protectionInserts, nil
}

func restoredBranchLineageInsertSQL(
	tableID uint64,
	lineage *restoredBranchLineage,
	deleted bool,
) string {
	return fmt.Sprintf(
		"insert into %s.%s values(%d, %d, %d, %d, %s, %t)",
		catalog.MO_CATALOG,
		catalog.MO_BRANCH_METADATA,
		tableID,
		lineage.cloneTS,
		lineage.parentTableID,
		lineage.creator,
		quoteSQLStringLiteral(lineage.level),
		deleted,
	)
}

func restitchRestoredBranchLineage(
	ctx context.Context,
	bh BackgroundExec,
	receipt *cloneReceipt,
) error {
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	lineage, err := loadRestoredBranchLineage(sysCtx, bh, receipt)
	if err != nil {
		return err
	}
	// Ordinary tables have no lineage. A plain ALTER edge is physical history,
	// while alter:<level> retains the logical branch level and must survive.
	if lineage == nil || lineage.level == databranchutils.AlterLineageLevel {
		return nil
	}

	directProtection, ancestors, err := collectRestoredBranchHistory(sysCtx, bh, receipt, lineage)
	if err != nil {
		return err
	}
	current, err := lockRestoredBranchLineageDAG(sysCtx, bh)
	if err != nil {
		return err
	}
	metadataInserts, protections, err := reconcileRestoredBranchLineage(
		sysCtx, bh, receipt, lineage, directProtection, ancestors, current,
	)
	if err != nil {
		return err
	}

	// All historical/current reads and validation finish before publication.
	// Insert missing ancestors top-down, then the restored direct edge and its
	// protection, all through the existing restore transaction.
	for i := len(metadataInserts) - 1; i >= 0; i-- {
		metadata := metadataInserts[i]
		insertSQL := restoredBranchLineageInsertSQL(metadata.TableID, &restoredBranchLineage{
			parentTableID: metadata.PTableID,
			cloneTS:       metadata.CloneTS,
			creator:       metadata.Creator,
			level:         metadata.Level,
		}, metadata.TableDeleted)
		bh.ClearExecResultSet()
		if err = bh.Exec(sysCtx, insertSQL); err != nil {
			return err
		}
	}
	bh.ClearExecResultSet()
	if err = bh.Exec(sysCtx, restoredBranchLineageInsertSQL(receipt.dstTableID, lineage, false)); err != nil {
		return err
	}
	for _, protection := range protections {
		if err = insertBranchProtectSnapshot(
			sysCtx, bh, protection.childTableID, protection.record,
		); err != nil {
			return err
		}
	}
	return nil
}
