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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
)

type logicalRestoreDatabaseDefinition struct {
	createSQL    string
	databaseType string
}

type logicalRestoreDatabaseDefinitionLoader func(string) (logicalRestoreDatabaseDefinition, error)

func newLogicalRestoreDatabaseDefinition(ctx context.Context, dbName string, cols []string) (logicalRestoreDatabaseDefinition, error) {
	if len(cols) < 3 {
		return logicalRestoreDatabaseDefinition{}, moerr.NewBadDB(ctx, dbName)
	}
	return logicalRestoreDatabaseDefinition{createSQL: cols[1], databaseType: cols[2]}, nil
}

// Only the catalog-owned branch category is propagated. A fresh context also
// prevents a preceding database's identity leaking into an unmarked restore.
func prepareLogicalRestoreDatabase(ctx context.Context, definition logicalRestoreDatabaseDefinition) context.Context {
	databaseType := ""
	if definition.databaseType == catalog.SystemDBTypeDataBranch {
		databaseType = definition.databaseType
	}
	return context.WithValue(ctx, defines.DatTypKey{}, databaseType)
}

func preflightLogicalRestoreDatabases(ctx context.Context, names []string, load logicalRestoreDatabaseDefinitionLoader) error {
	for _, name := range names {
		if err := CancelCheck(ctx); err != nil {
			return err
		}
		if needSkipDb(name) {
			continue
		}
		if _, err := load(name); err != nil {
			return err
		}
	}
	return nil
}

// Hold the same account/name catalog key as CREATE/ALTER/DROP object DDL.
// Validation and the nested DROP then observe one stable ordinary-table set.
func lockDataBranchDatabaseCatalogRow(ctx context.Context, ses *Session, bh BackgroundExec, accountID uint32, databaseName string) error {
	back, ok := bh.(*backExec)
	if !ok || back.backSes == nil {
		return moerr.NewInternalError(ctx, "database branch lock requires a background executor")
	}
	lockProc := newDataBranchCloneLockProcess(ctx, ses, bh)
	defer lockProc.Free()
	txnOp := lockProc.GetTxnOperator()
	if !txnOp.Txn().IsPessimistic() {
		return moerr.NewNotSupportedNoCtx("DATA BRANCH DELETE DATABASE requires a pessimistic transaction")
	}
	targetCtx := defines.AttachAccountId(ctx, accountID)
	eng := lockProc.GetSessionInfo().StorageEngine
	db, err := eng.Database(targetCtx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return err
	}
	rel, err := db.Relation(targetCtx, catalog.MO_DATABASE, nil)
	if err != nil {
		return err
	}
	lockBat, err := dataBranchCloneCatalogLockBatch(lockProc, accountID, databaseName)
	if err != nil {
		return err
	}
	defer lockBat.Vecs[0].Free(lockProc.Mp())
	return withDataBranchCloneLockContext(lockProc, targetCtx, func() error {
		return lockop.LockRows(eng, lockProc, rel, rel.GetTableID(targetCtx), lockBat, 0,
			*lockBat.Vecs[0].GetType(), lock.LockMode_Exclusive, lock.Sharding_None, accountID)
	})
}
