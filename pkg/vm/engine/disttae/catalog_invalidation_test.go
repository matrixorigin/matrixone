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

package disttae

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

func insertCatalogTableAt(
	t *testing.T,
	eng *Engine,
	txn *Transaction,
	accountID uint32,
	databaseID, tableID uint64,
	version uint32,
	databaseName, tableName string,
	ts types.TS,
) {
	t.Helper()

	packer := types.NewPacker()
	defer packer.Close()
	bat, err := catalog.GenCreateTableTuple(catalog.Table{
		AccountId:    accountID,
		DatabaseId:   databaseID,
		DatabaseName: databaseName,
		TableId:      tableID,
		TableName:    tableName,
		Version:      version,
	}, txn.proc.Mp(), packer)
	require.NoError(t, err)
	defer bat.Clean(txn.proc.Mp())
	_, err = fillRandomRowidAndZeroTs(bat, txn.proc.Mp())
	require.NoError(t, err)
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))
	timestamps[0] = ts
	eng.GetLatestCatalogCache().InsertTable(bat)
}

func TestGetCachedTableByKeyRevalidatesAfterCatalogReplayAndGC(t *testing.T) {
	base := newTxnTableForTest()
	eng := base.eng.(*Engine)
	op, txn := newResetTxnForTest(t, eng)
	require.True(t, op.Txn().IsRCIsolation())

	db := &txnDatabase{
		op:           op,
		accountId:    7,
		databaseId:   10,
		databaseName: "db",
	}
	origin := newTxnTableWithItem(db, cache.TableItem{
		AccountId:  7,
		DatabaseId: 10,
		Name:       "t",
		Id:         42,
		Version:    1,
	}, txn.proc, eng)
	delegate := &txnTableDelegate{origin: origin}
	key := tableKey{accountId: 7, databaseId: 10, dbName: "db", name: "t"}
	txn.tableCache.Store(key, delegate)

	insertCatalogTableAt(t, eng, txn, 7, 10, 42, 1, "db", "t", types.BuildTS(100, 0))
	require.Same(t, delegate, txn.getCachedTableByKey(context.Background(), key, key))

	// Replay an ALTER with the same identity but a newer schema version. The
	// RC table cache must evict the canonical relation before it is reused.
	insertCatalogTableAt(t, eng, txn, 7, 10, 42, 2, "db", "t", types.BuildTS(200, 0))
	require.Nil(t, txn.getCachedTableByKey(context.Background(), key, key))
	_, ok := txn.tableCache.Load(key)
	require.False(t, ok)

	// Reinsert the cached relation, then exercise the retained replay history
	// after GC and a drop/recreate identity change.
	txn.tableCache.Store(key, delegate)
	eng.GetLatestCatalogCache().GC(timestamp.Timestamp{PhysicalTime: 150})
	insertCatalogTableAt(t, eng, txn, 7, 10, 43, 1, "db", "t", types.BuildTS(300, 0))
	require.Nil(t, txn.getCachedTableByKey(context.Background(), key, key))
}

// A new session definition is visible without advancing an existing data
// snapshot. Neither the name convention nor the catalog marker alone suffices.
func TestSessionTemporarySchemaCatalogVisibility(t *testing.T) {
	eng := &Engine{}
	eng.catalog.Store(cache.NewCatalog())
	op, txn := newResetTxnForTest(t, eng)
	old := op.SnapshotTS()
	eng.GetLatestCatalogCache().UpdateDuration(types.TS{}, types.MaxTs())
	db := &txnDatabase{op: op, accountId: 7, databaseId: 10, databaseName: "db"}
	physical := defines.GenTempTableName(uuid.New(), "db", "t")
	for i, tc := range []struct {
		name, kind string
		account    uint32
		visible    bool
	}{
		{physical, catalog.SystemTemporaryTable, 7, true},
		{physical + "_persistent", catalog.SystemOrdinaryRel, 7, false},
		{"unprefixed", catalog.SystemTemporaryTable, 7, false},
		{physical + "_other_tenant", catalog.SystemTemporaryTable, 8, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			packer := types.NewPacker()
			defer packer.Close()
			bat, err := catalog.GenCreateTableTuple(catalog.Table{AccountId: tc.account, DatabaseId: 10, DatabaseName: "db", TableId: uint64(42 + i), TableName: tc.name, Kind: tc.kind}, txn.proc.Mp(), packer)
			require.NoError(t, err)
			defer bat.Clean(txn.proc.Mp())
			_, err = fillRandomRowidAndZeroTs(bat, txn.proc.Mp())
			require.NoError(t, err)
			vector.MustFixedColWithTypeCheck[types.TS](bat.GetVector(cache.MO_TIMESTAMP_IDX))[0] = types.BuildTS(old.PhysicalTime+1, 0)
			eng.GetLatestCatalogCache().InsertTable(bat)
			item, err := db.getTableItem(context.Background(), 7, tc.name, eng)
			require.NoError(t, err)
			if tc.visible {
				require.NotNil(t, item)
				require.Equal(t, uint64(42+i), item.Id)
			} else {
				require.Nil(t, item)
			}
			require.Equal(t, old, op.SnapshotTS())
		})
	}
}
