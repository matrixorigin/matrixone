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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
	txnOp := cloneSnapshotTxnOperator(ses, bh)
	if !txnOp.Txn().IsPessimistic() {
		return moerr.NewNotSupportedNoCtx("DATA BRANCH DELETE DATABASE requires a pessimistic transaction")
	}
	// This catalog's primary key is (account_id, datname), the same key used
	// by lockMoDatabase during object DDL. Use the ordinary statement executor
	// so its existing RC retry owner refreshes a lock wait before validation.
	query := fmt.Sprintf("select dat_id from mo_catalog.mo_database where account_id = %d and datname = %s for update", accountID, quoteSQLStringLiteral(databaseName))
	result, err := runSqlWithBackExec(ctx, ses, bh, query)
	result.Close()
	return err
}
