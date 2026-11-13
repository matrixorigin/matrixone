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

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Read source catalog identities before resolving/recreating a target account,
// invalidating its metadata, or removing FK-linked target objects.
func preflightRestoreSnapshotEntry(ctx context.Context, ses *Session, bh BackgroundExec, stmt *tree.RestoreSnapShot, snapshot snapshotRecord) error {
	switch stmt.Level {
	case tree.RESTORELEVELCLUSTER:
		// restoreToCluster preflights every source account before account effects.
		return nil
	case tree.RESTORELEVELACCOUNT:
		if snapshot.level == tree.RESTORELEVELCLUSTER.String() {
			account, err := getAccountRecordByTs(ctx, ses, bh, snapshot.snapshotName, snapshot.ts, string(stmt.AccountName))
			if err != nil {
				return err
			}
			accountID := uint32(account.accountId)
			return preflightLogicalRestoreAccountFromTS(ctx, ses.GetService(), bh, snapshot.ts, accountID, accountID)
		}
		// Authorization has resolved this snapshot in the caller's namespace.
		// Its source account may differ from that owner (sys -> tenant restore).
		// Use the authorized timestamp when reading under the source account;
		// re-resolving the name there would look for a different snapshot owner.
		accountID := uint32(snapshot.objId)
		return preflightLogicalRestoreAccountFromTS(ctx, ses.GetService(), bh, snapshot.ts, accountID, accountID)
	case tree.RESTORELEVELDATABASE, tree.RESTORELEVELTABLE:
		return preflightLogicalRestoreDatabases(ctx, []string{string(stmt.DatabaseName)}, func(dbName string) (logicalRestoreDatabaseDefinition, error) {
			return getCreateDatabaseSql(ctx, ses.GetService(), bh, snapshot.snapshotName, dbName, ses.GetTenantInfo().GetTenantID())
		})
	default:
		return nil
	}
}

func preflightLogicalRestoreAccountFromTS(ctx context.Context, sid string, bh BackgroundExec, ts int64, sourceAccount, targetAccount uint32) error {
	names, err := showDatabasesFromTS(ctx, sid, bh, ts, sourceAccount, targetAccount)
	if err != nil {
		return err
	}
	return preflightLogicalRestoreDatabases(ctx, names, func(name string) (logicalRestoreDatabaseDefinition, error) {
		return getCreateDatabaseSqlFromTS(ctx, sid, bh, name, ts, sourceAccount, targetAccount)
	})
}

func preflightRestorePitrEntry(ctx context.Context, sid string, bh BackgroundExec, pitrName string, ts int64, level tree.RestoreLevel, dbName string, sourceAccount uint32) error {
	sourceCtx := defines.AttachAccountId(ctx, sourceAccount)
	var names []string
	var err error
	switch level {
	case tree.RESTORELEVELACCOUNT:
		names, err = showDatabasesWithPitr(sourceCtx, sid, bh, pitrName, ts)
		if err != nil {
			return err
		}
	case tree.RESTORELEVELDATABASE, tree.RESTORELEVELTABLE:
		names = []string{dbName}
	default:
		return nil
	}
	return preflightLogicalRestoreDatabases(sourceCtx, names, func(name string) (logicalRestoreDatabaseDefinition, error) {
		return getCreateDatabaseSqlInPitr(sourceCtx, sid, bh, pitrName, name, sourceAccount, ts)
	})
}
