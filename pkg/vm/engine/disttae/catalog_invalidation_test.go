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
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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
	_, err = fillRandomRowidAndZeroTs(bat, catalog.MoTablesSchema, txn.proc.Mp())
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
			_, err = fillRandomRowidAndZeroTs(bat, catalog.MoTablesSchema, txn.proc.Mp())
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

// The optimizer borrows the ordinary resolver's schema domain. Reconnect or
// replay advancing the latest catalog must not override an SI relation that
// remains valid in its transaction; RC has the separate eviction control above.
func TestTemporalCanonicalRelationKeepsSISchemaAcrossReplay(t *testing.T) {
	base := newTxnTableForTest()
	eng := base.eng.(*Engine)
	client.RunTxnTests(func(cli client.TxnClient, _ rpc.TxnSender) {
		ctx := defines.AttachAccountId(t.Context(), 7)
		op, err := cli.New(ctx, timestamp.Timestamp{PhysicalTime: 150}, client.WithTxnIsolation(pbtxn.TxnIsolation_SI))
		require.NoError(t, err)
		require.False(t, op.Txn().IsRCIsolation())
		op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 150})
		workspace := &Transaction{op: op, engine: eng, proc: testutil.NewProc(t), tableCache: new(sync.Map), tableOps: newTableOps(), databaseOps: newDbOps()}
		op.AddWorkspace(workspace)
		db := &txnDatabase{op: op, accountId: 7, databaseId: 10, databaseName: "db"}
		origin := newTxnTableWithItem(db, cache.TableItem{AccountId: 7, DatabaseId: 10, Name: "t", Id: 42, Version: 1}, workspace.proc, eng)
		canonical := &txnTableDelegate{origin: origin}
		key := tableKey{accountId: 7, databaseId: 10, dbName: "db", name: "t"}
		workspace.tableCache.Store(key, canonical)
		insertCatalogTableAt(t, eng, workspace, 7, 10, 42, 1, "db", "t", types.BuildTS(100, 0))
		first, err := db.relation(ctx, "t", nil)
		require.NoError(t, err)
		require.Same(t, canonical, first)
		insertCatalogTableAt(t, eng, workspace, 7, 10, 42, 2, "db", "t", types.BuildTS(200, 0))
		insertCatalogTableAt(t, eng, workspace, 7, 10, 43, 1, "db", "t", types.BuildTS(300, 0))
		afterReplay, err := db.relation(ctx, "t", nil)
		require.NoError(t, err)
		require.Same(t, first, afterReplay)
		require.Equal(t, uint64(42), canonical.origin.tableId)
		require.Equal(t, uint32(1), canonical.origin.version)
		require.Equal(t, timestamp.Timestamp{PhysicalTime: 150}, op.SnapshotTS())
	})
}

func TestTxnTableOwnsReconstructedDefinitions(t *testing.T) {
	base := newTxnTableForTest()
	eng := base.eng.(*Engine)
	op, txn := newResetTxnForTest(t, eng)
	item := cache.TableItem{Name: "t", Defs: []engine.TableDef{&engine.AttributeDef{Attr: engine.Attribute{Name: "id", Type: types.T_int64.ToType(), Primary: true, Default: &plan.Default{OriginString: "original"}}}}}
	db := &txnDatabase{op: op}
	first := newTxnTableWithItem(db, item, txn.proc, eng)
	second := newTxnTableWithItem(db, item, txn.proc, eng)
	ctx := context.Background()
	cols, err := first.TableColumns(ctx)
	require.NoError(t, err)
	require.Len(t, cols, 1)
	keys, err := first.GetPrimaryKeys(ctx)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	defs, err := first.TableDefs(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, defs)
	cols[0].Default.OriginString = "changed"
	other, err := second.TableColumns(ctx)
	require.NoError(t, err)
	require.Equal(t, "original", other[0].Default.OriginString)
	require.Equal(t, "original", item.Defs[0].(*engine.AttributeDef).Attr.Default.OriginString)
}

func TestPublishedPlannerBorrowKeepsVersionsAndCopiesIsolated(t *testing.T) {
	base := newTxnTableForTest()
	eng := base.eng.(*Engine)
	op, txn := newResetTxnForTest(t, eng)
	db := &txnDatabase{op: op}
	columnType := types.T_int64.ToType()
	typ, err := columnType.Marshal()
	require.NoError(t, err)
	makeItem := func(name string) cache.TableItem {
		item := cache.TableItem{Name: "t"}
		cache.InitTableItemWithColumns(&item, cache.Columns{{Name: name, Num: 1, Typ: typ}})
		return item
	}
	oldItem := makeItem("old")
	old := newTxnTableWithItem(db, oldItem, txn.proc, eng)
	peer := newTxnTableWithItem(db, oldItem, txn.proc, eng)
	newItem := makeItem("new")
	newer := newTxnTableWithItem(db, newItem, txn.proc, eng)
	ctx := context.Background()
	require.Same(t, old.GetTableDef(ctx), peer.GetTableDef(ctx))
	require.NotSame(t, old.GetTableDef(ctx), newer.GetTableDef(ctx))
	require.Equal(t, "old", old.GetTableDef(ctx).Cols[0].Name)
	require.Equal(t, "new", newer.GetTableDef(ctx).Cols[0].Name)
	copyDef := old.CopyTableDef(ctx)
	copyDef.Cols[0].Name = "changed"
	require.Equal(t, "old", peer.GetTableDef(ctx).Cols[0].Name)
}

func TestPublishedRelationPrimaryKeys(t *testing.T) {
	base := newTxnTableForTest()
	eng := base.eng.(*Engine)
	op, txn := newResetTxnForTest(t, eng)
	db := &txnDatabase{op: op}
	ctx := context.Background()

	ordinaryType := types.T_int64.ToType()
	ordinary, err := ordinaryType.Marshal()
	require.NoError(t, err)
	keyType := types.T_varchar.ToType()
	keyType.Size++ // Preserve the catalog type when the planner cannot round-trip it.
	key, err := keyType.Marshal()
	require.NoError(t, err)
	defaultExpr, err := (&plan.Default{OriginString: "'original'"}).Marshal()
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		key  string
	}{
		{name: "none"},
		{name: "single", key: "a"},
		{name: "hidden composite", key: catalog.CPrimaryKeyColName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := cache.TableItem{Name: "t"}
			cols := cache.Columns{
				{Name: "a", Num: 1, Typ: ordinary},
				{Name: "b", Num: 2, Seqnum: 1, Typ: ordinary},
			}
			switch tc.key {
			case "a":
				cols[0].ConstraintType = catalog.SystemColPKConstraint
				cols[0].HasDef = 1
				cols[0].DefaultExpr = defaultExpr
			case catalog.CPrimaryKeyColName:
				cols = append(cols, catalog.Column{
					Name: tc.key, Num: 3, Seqnum: 2, Typ: key,
					IsHidden: 1, ConstraintType: catalog.SystemColPKConstraint,
				})
			}
			cache.InitTableItemWithColumns(&item, cols)
			require.True(t, item.SchemaSource().Valid())
			rel := newTxnTableWithItem(db, item, txn.proc, eng)

			keys, err := rel.GetPrimaryKeys(ctx)
			require.NoError(t, err)
			if tc.key == "" {
				require.Empty(t, keys)
				return
			}
			require.Len(t, keys, 1)
			require.Equal(t, tc.key, keys[0].Name)
			require.True(t, keys[0].Primary)
			if tc.key == catalog.CPrimaryKeyColName {
				require.True(t, keys[0].IsHidden)
				require.Equal(t, keyType, keys[0].Type)
				require.EqualValues(t, 2, keys[0].Seqnum)
				return
			}
			require.Equal(t, "'original'", keys[0].Default.OriginString)
			keys[0].Default.OriginString = "changed"
			keys, err = rel.GetPrimaryKeys(ctx)
			require.NoError(t, err)
			require.Equal(t, "'original'", keys[0].Default.OriginString)
		})
	}
}
