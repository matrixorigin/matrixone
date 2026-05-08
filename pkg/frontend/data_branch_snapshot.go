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

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

// branchSnapshotName is a thin alias over the databranchutils helper so that
// the lion's share of the frontend package keeps reading naturally (the
// databranchutils helper is the single source of truth for the sname
// format).
func branchSnapshotName(childTableID uint64) string {
	return databranchutils.BranchSnapshotName(childTableID)
}

// branchSnapshotKind duplicates the shared constant for readability inside
// the frontend package. It must stay in lockstep with
// databranchutils.BranchSnapshotKind.
const branchSnapshotKind = databranchutils.BranchSnapshotKind

// loadBranchDAGWithBH reads mo_branch_metadata under the sys account and
// returns an in-memory DAG. It is used by the frontend reclaim entry point
// which has a BackgroundExec available.
func loadBranchDAGWithBH(
	ctx context.Context,
	bh BackgroundExec,
) (databranchutils.BranchReclaimDag, error) {
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	bh.ClearExecResultSet()
	sql := fmt.Sprintf(
		"select table_id, p_table_id, clone_ts, table_deleted from %s.%s",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
	)
	if err := bh.Exec(sysCtx, sql); err != nil {
		return databranchutils.BranchReclaimDag{}, err
	}

	erArray, err := getResultSet(sysCtx, bh)
	if err != nil {
		return databranchutils.BranchReclaimDag{}, err
	}

	if !execResultArrayHasData(erArray) {
		return databranchutils.NewBranchReclaimDag(nil), nil
	}

	var rows []databranchutils.DataBranchMetadata
	for _, er := range erArray {
		rowCount := er.GetRowCount()
		for row := uint64(0); row < rowCount; row++ {
			tableID, gerr := er.GetUint64(sysCtx, row, 0)
			if gerr != nil {
				return databranchutils.BranchReclaimDag{}, gerr
			}
			parentID, gerr := er.GetUint64(sysCtx, row, 1)
			if gerr != nil {
				return databranchutils.BranchReclaimDag{}, gerr
			}
			cloneTS, gerr := er.GetInt64(sysCtx, row, 2)
			if gerr != nil {
				return databranchutils.BranchReclaimDag{}, gerr
			}
			deletedInt, gerr := er.GetInt64(sysCtx, row, 3)
			if gerr != nil {
				return databranchutils.BranchReclaimDag{}, gerr
			}
			rows = append(rows, databranchutils.DataBranchMetadata{
				TableID:      tableID,
				CloneTS:      cloneTS,
				PTableID:     parentID,
				TableDeleted: deletedInt != 0,
			})
		}
	}
	return databranchutils.NewBranchReclaimDag(rows), nil
}

// reclaimBranchSnapshotsWithBH is the BackgroundExec-backed entry point used
// by dataBranchDeleteTable and dataBranchDeleteDatabase. It always executes
// the DELETE under the sys account so snapshot rows owned by cross-account
// parents can be removed.
func reclaimBranchSnapshotsWithBH(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	deadTIDs []uint64,
) error {
	if len(deadTIDs) == 0 {
		return nil
	}
	logutil.Info(
		"DataBranch-ProtectSnapshot-Reclaim-Start",
		zap.String("entry", "bh"),
		zap.Uint64s("dead_tids", deadTIDs),
	)
	loadDAG := func() (databranchutils.BranchReclaimDag, error) {
		return loadBranchDAGWithBH(ctx, bh)
	}
	execDelete := func(snames []string) error {
		sysCtx := defines.AttachAccountId(ctx, sysAccountID)
		sql := databranchutils.BuildBranchSnapshotDeleteSQL(snames)
		bh.ClearExecResultSet()
		if err := bh.Exec(sysCtx, sql); err != nil {
			return err
		}
		logutil.Info(
			"DataBranch-ProtectSnapshot-Reclaim-Done",
			zap.String("entry", "bh"),
			zap.Strings("released", snames),
		)
		return nil
	}
	_ = ses
	return databranchutils.ReclaimBranchSnapshotsCore(deadTIDs, loadDAG, execDelete)
}

// getBranchParentAccountName resolves the account name for the source
// account id recorded on the receipt. It is cached on the receipt to avoid
// repeated lookups when the same receipt is used for both metadata and
// snapshot inserts.
func getBranchParentAccountName(
	ctx context.Context,
	bh BackgroundExec,
	receipt *cloneReceipt,
) (string, error) {
	if receipt.srcAccountName != "" {
		return receipt.srcAccountName, nil
	}
	if receipt.srcAccount == sysAccountID {
		receipt.srcAccountName = sysAccountName
		return receipt.srcAccountName, nil
	}
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	bh.ClearExecResultSet()
	sql := fmt.Sprintf(
		"select account_name from %s.mo_account where account_id = %d",
		catalog.MO_CATALOG, receipt.srcAccount,
	)
	if err := bh.Exec(sysCtx, sql); err != nil {
		return "", err
	}
	erArray, err := getResultSet(sysCtx, bh)
	if err != nil {
		return "", err
	}
	if !execResultArrayHasData(erArray) {
		return "", moerr.NewInternalErrorf(ctx,
			"branch protect snapshot: cannot resolve account name for id %d",
			receipt.srcAccount)
	}
	name, err := erArray[0].GetString(sysCtx, 0, 0)
	if err != nil {
		return "", err
	}
	receipt.srcAccountName = name
	return name, nil
}

// createBranchProtectSnapshot inserts a `kind='branch'` row into mo_snapshots
// that pins the parent table at the edge's clone_ts. It must run inside the
// same background-executor txn that produced the matching
// mo_branch_metadata row so both inserts commit or roll back together
// (§5.2).
//
// It is a no-op for clone receipts whose ids were never populated (e.g.
// restore-time clones that bypassed the branch-metadata hook). This mirrors
// updateBranchMetaTable's short-circuit behaviour.
func createBranchProtectSnapshot(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	receipt *cloneReceipt,
) error {
	if receipt == nil || receipt.dstTableID == 0 || receipt.srcTableID == 0 {
		return nil
	}

	parentAccountName, err := getBranchParentAccountName(ctx, bh, receipt)
	if err != nil {
		return err
	}

	newUUID, err := uuid.NewV7()
	if err != nil {
		return err
	}

	sname := branchSnapshotName(receipt.dstTableID)
	// Branch protect snapshots are written with kind='branch' directly so
	// they are never visible as kind='user' — not even transiently. The
	// existing insertIntoMoSnapshots format does not carry the kind column
	// (it relies on the 'user' default), so this path uses its own insert.
	insertSQL := fmt.Sprintf(
		`insert into %s.%s(snapshot_id, sname, ts, level, account_name, database_name, table_name, obj_id, kind) `+
			`values ('%s', '%s', %d, '%s', '%s', '%s', '%s', %d, '%s')`,
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS,
		newUUID.String(),
		sname,
		receipt.snapshotTS,
		dataBranchLevel_Table,
		parentAccountName,
		receipt.srcDb,
		receipt.srcTbl,
		receipt.srcTableID,
		branchSnapshotKind,
	)

	// Execute as sys so the row can be written into the parent's account
	// regardless of the caller tenant. Runs under the same BackgroundExec
	// txn wrap, so it commits atomically with the surrounding CLONE +
	// mo_branch_metadata insert (§5.2).
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	bh.ClearExecResultSet()
	if err := bh.Exec(sysCtx, insertSQL); err != nil {
		return err
	}

	logutil.Info(
		"DataBranch-ProtectSnapshot-Create",
		zap.Uint64("child_tid", receipt.dstTableID),
		zap.Uint64("parent_tid", receipt.srcTableID),
		zap.String("parent_account", parentAccountName),
		zap.Int64("clone_ts", receipt.snapshotTS),
	)
	_ = ses
	return nil
}
