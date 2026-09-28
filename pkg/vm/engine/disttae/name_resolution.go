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

package disttae

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/identifier"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
)

func (e *Engine) resolveMode2Database(
	ctx context.Context, name string, op client.TxnOperator, txn *Transaction,
) (engine.Database, error) {
	if identifier.Fold(name) == catalog.MO_CATALOG {
		return &txnDatabase{
			op: op, databaseId: catalog.MO_CATALOG_ID,
			databaseName: catalog.MO_CATALOG,
		}, nil
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	folded := identifier.Fold(name)
	local := txn.databaseOps.foldedSnapshot(accountID, folded)
	var own *txnDatabase
	for _, operation := range local {
		if operation.kind != INSERT {
			continue
		}
		if own != nil {
			return nil, moerr.NewAmbiguousIdentifier(ctx, "database", name)
		}
		own = operation.payload
	}
	var committed *cache.DatabaseItem
	var historicalName string
	var ambiguous bool
	catalogCache := e.GetLatestCatalogCache()
	snapshot := op.SnapshotTS()
	cacheComplete := visitCompleteMode2CatalogSnapshot(catalogCache, snapshot, func() {
		catalogCache.VisitFoldedDatabases(accountID, name, snapshot,
			func(item *cache.DatabaseItem) bool {
				if _, overwritten := local[item.Name]; overwritten {
					return true
				}
				if own != nil || committed != nil {
					ambiguous = true
					return false
				}
				committed = item
				return true
			})
	})
	if !cacheComplete {
		// GC may advance its start watermark between the first CanServe and
		// VisitFolded. Discard an incomplete cache result, including ambiguity.
		committed, ambiguous = nil, false
		err = scanHistoricalDatabaseNames(ctx, op, accountID, folded,
			func(physicalName string) bool {
				if _, overwritten := local[physicalName]; overwritten {
					return true
				}
				if own != nil || historicalName != "" {
					ambiguous = true
					return false
				}
				historicalName = physicalName
				return true
			})
		if err != nil {
			return nil, err
		}
	}
	if ambiguous {
		return nil, moerr.NewAmbiguousIdentifier(ctx, "database", name)
	}
	if own != nil {
		return own, nil
	}
	if historicalName != "" {
		committed, err = e.loadDatabaseFromStorage(ctx, accountID, historicalName, op)
		if err != nil {
			return nil, err
		}
		if committed == nil {
			return nil, moerr.NewInternalErrorf(ctx,
				"database %s disappeared at a fixed snapshot", historicalName)
		}
	}
	if committed == nil {
		return nil, moerr.GetOkExpectedEOB()
	}
	return &txnDatabase{
		accountId: accountID, op: op,
		databaseName: committed.Name, databaseId: committed.Id,
		databaseType: committed.Typ, databaseCreateSql: committed.CreateSql,
	}, nil
}

// scanHistoricalDatabaseNames reads one scoped SQL stream at a fixed snapshot.
// Comparing folded names in Go avoids SQL collation differences.
func scanHistoricalDatabaseNames(
	ctx context.Context, op client.TxnOperator, accountID uint32, folded string,
	visit func(string) bool,
) error {
	snapshot := op.SnapshotTS()
	if !op.SnapshotTS().Equal(snapshot) {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	sql := fmt.Sprintf(
		"select datname from mo_catalog.mo_database where account_id = %d",
		accountID,
	)
	stop := false
	err := scanReadSql(ctx, op, sql, func(result executor.Result) error {
		if !op.SnapshotTS().Equal(snapshot) {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		if !stop {
			for _, bat := range result.Batches {
				for i := 0; i < bat.RowCount(); i++ {
					physicalName := bat.Vecs[0].GetStringAt(i)
					if identifier.Fold(physicalName) == folded && !visit(physicalName) {
						stop = true
						break
					}
				}
				if stop {
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !op.SnapshotTS().Equal(snapshot) {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	return nil
}

func (db *txnDatabase) resolveMode2TableName(
	ctx context.Context, accountID uint32, input string, txn *Transaction,
) (physical string, found bool, err error) {
	folded := identifier.Fold(input)
	local := txn.tableOps.foldedSnapshot(accountID, db.databaseId, folded)
	for name, operation := range local {
		if operation.kind != INSERT {
			continue
		}
		if found {
			return "", false, moerr.NewAmbiguousIdentifier(ctx, "table", input)
		}
		physical, found = name, true
	}
	catalogCache := txn.engine.GetLatestCatalogCache()
	snapshot := db.op.SnapshotTS()
	var ambiguous bool
	localPhysical, localFound := physical, found
	visit := func(name string) bool {
		if _, overwritten := local[name]; overwritten {
			return true
		}
		if found {
			ambiguous = true
			return false
		}
		physical, found = name, true
		return true
	}
	cacheComplete := visitCompleteMode2CatalogSnapshot(catalogCache, snapshot, func() {
		catalogCache.VisitFoldedTables(accountID, db.databaseId, input, snapshot,
			func(item *cache.TableItem) bool { return visit(item.Name) })
	})
	if !cacheComplete {
		physical, found, ambiguous = localPhysical, localFound, false
		err = scanHistoricalTableNames(ctx, db.op, accountID, db.databaseName, db.databaseId,
			folded, visit)
		if err != nil {
			return "", false, err
		}
	}
	if ambiguous {
		return "", false, moerr.NewAmbiguousIdentifier(ctx, "table", input)
	}
	return physical, found, nil
}

// visitCompleteMode2CatalogSnapshot ensures a cache lookup did not cross the
// start-watermark advance that precedes destructive catalog GC. The visitor's
// result is usable only if the cache still covers the snapshot afterwards.
func visitCompleteMode2CatalogSnapshot(
	catalogCache *cache.CatalogCache, snapshot timestamp.Timestamp, visit func(),
) bool {
	ts := types.TimestampToTS(snapshot)
	if !catalogCache.CanServe(ts) {
		return false
	}
	visit()
	return catalogCache.CanServe(ts)
}

func scanHistoricalTableNames(
	ctx context.Context, op client.TxnOperator, accountID uint32, databaseName string, databaseID uint64,
	folded string, visit func(string) bool,
) error {
	snapshot := op.SnapshotTS()
	if !op.SnapshotTS().Equal(snapshot) {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	sql := fmt.Sprintf(
		"select relname from mo_catalog.mo_tables where account_id = %d and reldatabase = %s and reldatabase_id = %d",
		accountID, sqlquote.String(databaseName), databaseID,
	)
	stop := false
	err := scanReadSql(ctx, op, sql, func(result executor.Result) error {
		if !op.SnapshotTS().Equal(snapshot) {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		if !stop {
			for _, bat := range result.Batches {
				for i := 0; i < bat.RowCount(); i++ {
					physicalName := bat.Vecs[0].GetStringAt(i)
					if identifier.Fold(physicalName) == folded && !visit(physicalName) {
						stop = true
						break
					}
				}
				if stop {
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !op.SnapshotTS().Equal(snapshot) {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	return nil
}
