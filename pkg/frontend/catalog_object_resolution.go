// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"errors"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/identifier"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// resolveCatalogObjectInBackgroundTxn keeps a data-branch catalog name and ID
// from the same tenant and transaction snapshot. The caller has already
// checked privileges and must still reject protected physical objects.
func resolveCatalogObjectInBackgroundTxn(
	ctx context.Context, bh BackgroundExec, databaseName, tableName string,
) (physicalDatabase, physicalTable string, objectID uint64, err error) {
	back, ok := bh.(*backExec)
	if !ok || back.backSes == nil || back.backSes.GetTxnHandler() == nil || back.backSes.GetTxnHandler().GetTxn() == nil {
		return "", "", 0, moerr.NewInternalError(ctx, "catalog resolution requires a background transaction")
	}
	return resolveCatalogObjectWithTxn(ctx, back.backSes.GetStorage(),
		back.backSes.GetTxnHandler().GetTxn(), databaseName, tableName, false)
}

// Table snapshots use the stable logical ID across copy-table ALTERs.
func resolveSnapshotObjectInBackgroundTxn(
	ctx context.Context, bh BackgroundExec, databaseName, tableName string,
) (physicalDatabase, physicalTable string, objectID uint64, err error) {
	back, ok := bh.(*backExec)
	if !ok || back.backSes == nil || back.backSes.GetTxnHandler() == nil || back.backSes.GetTxnHandler().GetTxn() == nil {
		return "", "", 0, moerr.NewInternalError(ctx, "catalog resolution requires a background transaction")
	}
	return resolveCatalogObjectWithTxn(ctx, back.backSes.GetStorage(),
		back.backSes.GetTxnHandler().GetTxn(), databaseName, tableName, true)
}

// resolveCatalogObjectAtSnapshot uses the engine's mode-2 historical resolver.
// This retains its streaming fallback, typed ambiguity and snapshot tenant
// boundary rather than duplicating catalog-name matching in SQL.
func resolveCatalogObjectAtSnapshot(
	ctx context.Context, bh BackgroundExec, accountID uint32, physicalTime int64,
	databaseName, tableName string,
) (physicalDatabase, physicalTable string, objectID uint64, err error) {
	back, ok := bh.(*backExec)
	if !ok || back.backSes == nil || back.backSes.GetTxnHandler() == nil || back.backSes.GetTxnHandler().GetTxn() == nil {
		return "", "", 0, moerr.NewInternalError(ctx, "historical catalog resolution requires a background transaction")
	}
	ownerCtx := defines.AttachAccountId(ctx, accountID)
	txn := back.backSes.GetTxnHandler().GetTxn().CloneSnapshotOp(timestamp.Timestamp{PhysicalTime: physicalTime})
	return resolveCatalogObjectWithTxn(ownerCtx, back.backSes.GetStorage(), txn, databaseName, tableName, false)
}

func resolveSnapshotObjectAtSnapshot(
	ctx context.Context, bh BackgroundExec, accountID uint32, physicalTime int64,
	databaseName, tableName string,
) (physicalDatabase, physicalTable string, objectID uint64, err error) {
	back, ok := bh.(*backExec)
	if !ok || back.backSes == nil || back.backSes.GetTxnHandler() == nil || back.backSes.GetTxnHandler().GetTxn() == nil {
		return "", "", 0, moerr.NewInternalError(ctx, "historical catalog resolution requires a background transaction")
	}
	ownerCtx := defines.AttachAccountId(ctx, accountID)
	txn := back.backSes.GetTxnHandler().GetTxn().CloneSnapshotOp(timestamp.Timestamp{PhysicalTime: physicalTime})
	return resolveCatalogObjectWithTxn(ownerCtx, back.backSes.GetStorage(), txn, databaseName, tableName, true)
}

// Older table PITRs stored the logical ID; newer ones store the physical ID.
// Both identify the same table generation until it is dropped and recreated.
func historicalTableIDMatchesPitr(
	ctx context.Context, bh BackgroundExec, accountID uint32, physicalTime int64,
	databaseName, tableName string, physicalID, storedID uint64,
) (bool, error) {
	if physicalID == storedID {
		return true, nil
	}
	_, _, logicalID, err := resolveSnapshotObjectAtSnapshot(
		ctx, bh, accountID, physicalTime, databaseName, tableName)
	return err == nil && logicalID == storedID, err
}

func resolveCatalogObjectWithTxn(
	ctx context.Context, storage engine.Engine, txn client.TxnOperator, databaseName, tableName string, snapshotIdentity bool,
) (physicalDatabase, physicalTable string, objectID uint64, err error) {
	db, err := storage.Database(ctx, databaseName, txn)
	if err != nil {
		return "", "", 0, err
	}
	if db == nil {
		return "", "", 0, moerr.NewBadDB(ctx, databaseName)
	}
	physicalDatabase = resolvedDatabaseName(db, databaseName)
	if tableName == "" {
		objectID, err = strconv.ParseUint(db.GetDatabaseId(ctx), 10, 64)
		return physicalDatabase, "", objectID, err
	}
	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return "", "", 0, err
	}
	if rel == nil {
		return "", "", 0, moerr.NewNoSuchTable(ctx, physicalDatabase, tableName)
	}
	if snapshotIdentity {
		return physicalDatabase, rel.GetTableName(), plan2.SnapshotTableID(rel.GetTableDef(ctx)), nil
	}
	return physicalDatabase, rel.GetTableName(), rel.GetTableID(ctx), nil
}

func sameRecoveryObjectName(ctx context.Context, stored, input string) bool {
	if defines.Mode2NameResolutionEnabled(ctx) {
		return identifier.Fold(stored) == identifier.Fold(input)
	}
	return stored == input
}

func isProtectedRecoveryDatabase(ctx context.Context, name string) bool {
	if defines.Mode2NameResolutionEnabled(ctx) {
		name = identifier.Fold(name)
	}
	return needSkipDb(name)
}

func isAbsentCatalogObjectError(err error) bool {
	return moerr.IsMoErrCode(err, moerr.OkExpectedEOB) ||
		moerr.IsMoErrCode(err, moerr.ErrBadDB) ||
		moerr.IsMoErrCode(err, moerr.ErrNoSuchTable)
}

// resolveCurrentRestoreTarget returns the current target's physical spelling.
// A missing target is normal for restore; ambiguity and storage errors are not.
func resolveCurrentRestoreTarget(
	ctx context.Context, bh BackgroundExec, databaseName, tableName string,
) (physicalDatabase, physicalTable string, found bool, err error) {
	if !defines.Mode2NameResolutionEnabled(ctx) {
		return databaseName, tableName, true, nil
	}
	// Account creation during a restore can commit the background transaction.
	// Resolve with a short transaction when no restore transaction is active.
	back, ok := bh.(*backExec)
	if !ok || back.backSes == nil || back.backSes.GetTxnHandler() == nil {
		return "", "", false, moerr.NewInternalError(ctx, "catalog resolution requires a background session")
	}
	started := back.backSes.GetTxnHandler().GetTxn() == nil
	if started {
		if err = bh.Exec(ctx, "begin"); err != nil {
			return "", "", false, err
		}
	}
	physicalDatabase, physicalTable, _, err = resolveCatalogObjectInBackgroundTxn(
		ctx, bh, databaseName, tableName)
	absent := isAbsentCatalogObjectError(err)
	if absent {
		err = nil
	}
	if started {
		finish := "commit"
		if err != nil {
			finish = "rollback"
		}
		finishErr := bh.Exec(ctx, finish)
		if finishErr != nil {
			return "", "", false, errors.Join(err, finishErr)
		}
	}
	if absent {
		return databaseName, tableName, false, nil
	}
	return physicalDatabase, physicalTable, err == nil, err
}
