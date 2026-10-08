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

package disttae

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	pbstats "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxnTableWriteTableName(t *testing.T) {
	tbl := &txnTable{tableId: catalog.MO_COLUMNS_ID, tableName: catalog.MO_COLUMNS}
	require.Equal(t, catalog.MO_COLUMNS, tbl.writeTableName(context.Background()))
	require.Equal(t, catalog.MO_COLUMNS_UPDATE, tbl.writeTableName(
		context.WithValue(context.Background(), defines.MoColumnsUpdateKey{}, true),
	))

	tbl.tableId++
	require.Equal(t, catalog.MO_COLUMNS, tbl.writeTableName(
		context.WithValue(context.Background(), defines.MoColumnsUpdateKey{}, true),
	))
}

func TestTxnTableWriteObjectStatsUsesAuthorizedTableName(t *testing.T) {
	colexec.NewServer("")
	for _, tc := range []struct {
		name       string
		tableID    uint64
		authorized bool
		want       string
	}{
		{"authorized catalog update", catalog.MO_COLUMNS_ID, true, catalog.MO_COLUMNS_UPDATE},
		{"ordinary catalog write", catalog.MO_COLUMNS_ID, false, catalog.MO_COLUMNS},
		{"other table with capability", catalog.MO_COLUMNS_ID + 1, true, catalog.MO_COLUMNS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := newTransactionWithActivePKTableForTest(t, "pk")
			txn.tnStores = []DNStore{{}}
			txn.cnObjsSummary = make(map[types.Objectid]Summary)
			txn.op.(*mock_frontend.MockTxnOperator).EXPECT().IsSnapOp().Return(false).AnyTimes()
			tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
			require.NotNil(t, tbl)
			tbl.tableId = tc.tableID
			tbl.tableName = catalog.MO_COLUMNS
			tbl.db.databaseId = catalog.MO_CATALOG_ID
			tbl.db.databaseName = catalog.MO_CATALOG
			tbl.extraInfo = &api.SchemaExtra{}

			location := objectio.NewRandomLocation(1, 1)
			var info objectio.BlockInfo
			info.SetMetaLocation(location)
			stats := objectio.NewObjectStats()
			require.NoError(t, objectio.SetObjectStatsLocation(stats, location))
			bat := batch.New([]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})
			bat.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
			bat.SetVector(1, vector.NewVec(types.T_varchar.ToType()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], objectio.EncodeBlockInfo(&info), false, txn.proc.Mp()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], stats.Marshal(), false, txn.proc.Mp()))
			bat.SetRowCount(1)
			defer bat.Clean(txn.proc.Mp())

			ctx := context.Background()
			if tc.authorized {
				ctx = context.WithValue(ctx, defines.MoColumnsUpdateKey{}, true)
			}
			require.NoError(t, tbl.Write(ctx, bat))
			require.Len(t, txn.writes, 1)
			require.Equal(t, tc.want, txn.writes[0].tableName)
			summary, ok := txn.cnObjsSummary[*stats.ObjectName().ObjectId()]
			require.True(t, ok)
			require.Equal(t, tbl.db.databaseId, summary.databaseId)
			require.Equal(t, tc.tableID, summary.tableId)
			require.Equal(t, tc.want, summary.tbName)
			if tc.want == catalog.MO_COLUMNS_UPDATE {
				protoBat, err := batch.BatchToProtoBatch(txn.writes[0].bat)
				require.NoError(t, err)
				req, remaining, err := catalog.ParseEntryList([]*api.Entry{{
					EntryType: api.Entry_Insert, DatabaseId: catalog.MO_CATALOG_ID,
					TableId: catalog.MO_COLUMNS_ID, TableName: tc.want, Bat: protoBat,
				}})
				require.NoError(t, err)
				require.Equal(t, tc.want, req.(*api.Entry).TableName)
				require.Empty(t, remaining)
			}
			txn.writes[0].bat.Clean(txn.proc.Mp())
		})
	}
}

func TestTxnTableDeleteObjectStatsUsesAuthorizedTableName(t *testing.T) {
	for _, skipTransfer := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip-transfer=%v", skipTransfer), func(t *testing.T) {
			txn := newTransactionWithActivePKTableForTest(t, "pk")
			txn.tnStores = []DNStore{{}}
			txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
			txn.op.(*mock_frontend.MockTxnOperator).EXPECT().IsSnapOp().Return(false).AnyTimes()

			tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
			require.NotNil(t, tbl)
			tbl.tableId = catalog.MO_COLUMNS_ID
			tbl.tableName = catalog.MO_COLUMNS
			tbl.extraInfo = &api.SchemaExtra{}

			stats := objectio.NewObjectStats()
			require.NoError(t, objectio.SetObjectStatsLocation(stats, objectio.NewRandomLocation(1, 1)))
			bat := batch.New([]string{catalog.ObjectMeta_ObjectStats})
			bat.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], stats.Marshal(), false, txn.proc.Mp()))
			bat.SetRowCount(1)
			defer bat.Clean(txn.proc.Mp())

			ctx := context.WithValue(context.Background(), defines.MoColumnsUpdateKey{}, true)
			if skipTransfer {
				ctx = context.WithValue(ctx, defines.SkipTransferKey{}, true)
			}
			require.NoError(t, tbl.Delete(ctx, bat, ""))
			require.Len(t, txn.writes, 1)
			require.Equal(t, catalog.MO_COLUMNS_UPDATE, txn.writes[0].tableName)
			require.Equal(t, skipTransfer, txn.writes[0].skipTransfer)

			protoBat, err := batch.BatchToProtoBatch(txn.writes[0].bat)
			require.NoError(t, err)
			req, remaining, err := catalog.ParseEntryList([]*api.Entry{{
				EntryType:  api.Entry_Delete,
				DatabaseId: catalog.MO_CATALOG_ID,
				TableId:    catalog.MO_COLUMNS_ID,
				TableName:  txn.writes[0].tableName,
				Bat:        protoBat,
			}})
			require.NoError(t, err)
			require.Equal(t, txn.writes[0].tableName, req.(*api.Entry).TableName)
			require.Empty(t, remaining)
			_, _, err = catalog.ParseEntryList([]*api.Entry{{
				EntryType:  api.Entry_Delete,
				DatabaseId: catalog.MO_CATALOG_ID,
				TableId:    catalog.MO_COLUMNS_ID,
				TableName:  catalog.MO_COLUMNS,
				Bat:        protoBat,
			}})
			require.ErrorContains(t, err, "bad write format")
			txn.writes[0].bat.Clean(txn.proc.Mp())
		})
	}
}

func newTxnTableForTest() *txnTable {
	engine := &Engine{
		packerPool: fileservice.NewPool(
			128,
			func() *types.Packer {
				return types.NewPacker()
			},
			func(packer *types.Packer) {
				packer.Reset()
			},
			func(packer *types.Packer) {
				packer.Close()
			},
		),
	}
	engine.catalog.Store(cache.NewCatalog())
	var tnStore DNStore
	txn := &Transaction{
		engine:   engine,
		tnStores: []DNStore{tnStore},
	}
	rt := runtime.DefaultRuntime()
	s, err := rpc.NewSender(rpc.Config{}, rt)
	if err != nil {
		panic(err)
	}
	c := client.NewTxnClient("", s)
	c.Resume()
	op, _ := c.New(context.Background(), timestamp.Timestamp{})
	op.AddWorkspace(txn)

	db := &txnDatabase{
		op: op,
	}
	table := &txnTable{
		db:         db,
		primaryIdx: 0,
		eng:        engine,
	}
	return table
}

func TestTxnTableGetTableDefKeepsTemporarySessionStateContextual(t *testing.T) {
	table := &txnTable{
		db:      &txnDatabase{},
		relKind: catalog.SystemTemporaryTable,
	}
	tableDef := table.GetTableDef(context.Background())
	require.NotNil(t, tableDef)
	require.Equal(t, catalog.SystemTemporaryTable, tableDef.TableType)
	require.False(t, tableDef.IsTemporary)
}

func TestTxnTableGetTableDefRestoresDefaultCharset(t *testing.T) {
	table := &txnTable{
		db: &txnDatabase{},
		extraInfo: &api.SchemaExtra{
			DefaultCharset: uint32(types.CharsetUTF8MB4Bin),
		},
	}
	tableDef := table.GetTableDef(context.Background())
	require.NotNil(t, tableDef)
	require.Equal(t, uint32(types.CharsetUTF8MB4Bin), tableDef.DefaultCharset)
}

func makeBatchForTest(
	mp *mpool.MPool,
	ints ...int64,
) *batch.Batch {
	bat := batch.New([]string{"a"})
	vec := vector.NewVec(types.T_int64.ToType())
	for _, n := range ints {
		vector.AppendFixed(vec, n, false, mp)
	}
	bat.SetVector(0, vec)
	bat.SetRowCount(len(ints))
	return bat
}

func newResetTxnForTest(t *testing.T, eng *Engine) (client.TxnOperator, *Transaction) {
	t.Helper()

	op, closeFn := client.NewTestTxnOperator(context.Background())
	t.Cleanup(closeFn)
	proc := testutil.NewProc(t)
	txn := &Transaction{
		op:          op,
		proc:        proc,
		engine:      eng,
		tableCache:  new(sync.Map),
		tableOps:    newTableOps(),
		databaseOps: newDbOps(),
	}
	op.AddWorkspace(txn)
	return op, txn
}

func insertCatalogTableForResetTest(
	t *testing.T,
	eng *Engine,
	txn *Transaction,
	accountID uint32,
	databaseID, tableID uint64,
	databaseName, tableName string,
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
	}, txn.proc.Mp(), packer)
	require.NoError(t, err)
	defer bat.Clean(txn.proc.Mp())
	_, err = fillRandomRowidAndZeroTs(bat, txn.proc.Mp())
	require.NoError(t, err)
	eng.GetLatestCatalogCache().InsertTable(bat)
}

func TestReusableRelationHandleResetDoesNotMutateSharedTable(t *testing.T) {
	oldCanonical := newTxnTableForTest()
	oldCanonical.accountId = 0
	oldCanonical.tableId = 42
	oldCanonical.tableName = "t"
	oldCanonical.db.accountId = 0
	oldCanonical.db.databaseId = 10
	oldCanonical.db.databaseName = "db"

	shared := &txnTableDelegate{origin: oldCanonical}
	shared.isLocal = shared.isLocalFunc
	require.Error(t, shared.Reset(oldCanonical.db.op))

	handle1 := shared.NewRelationHandle().(*txnTableDelegate)
	handle2 := shared.NewRelationHandle().(*txnTableDelegate)
	require.NotSame(t, shared, handle1)
	require.Same(t, oldCanonical, handle1.origin)
	require.Error(t, handle1.Reset(nil))
	require.Error(t, oldCanonical.Reset(oldCanonical.db.op))

	newOp, newTxn := newResetTxnForTest(t, oldCanonical.eng.(*Engine))
	newProc := newTxn.proc

	newDB := &txnDatabase{
		accountId:    0,
		databaseId:   10,
		databaseName: "db",
		op:           newOp,
	}
	newCanonical := &txnTable{
		accountId: 0,
		tableId:   42,
		tableName: "t",
		db:        newDB,
		eng:       oldCanonical.eng,
		lastTS:    newOp.SnapshotTS(),
	}
	newCanonical.proc.Store(newProc)
	newShared := &txnTableDelegate{origin: newCanonical}
	newShared.isLocal = newShared.isLocalFunc
	newShared.parent = shared
	newShared.combined.is = true
	combinedPrimary := &txnTable{
		tableId:   84,
		tableName: "t_partition",
		db:        newDB,
		eng:       oldCanonical.eng,
	}
	combinedPrimary.proc.Store(newProc)
	newShared.combined.tbl = &combinedTxnTable{primary: combinedPrimary}
	newTxn.tableCache.Store(genTableKey(0, "t", 10, "db"), newShared)
	require.ErrorContains(t, newShared.combined.tbl.Reset(newOp), "cannot reset a shared combined relation")

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, handle := range []*txnTableDelegate{handle1, handle2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- handle.Reset(newOp)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Same(t, oldCanonical, shared.origin)
	require.Same(t, newCanonical, handle1.origin)
	require.Same(t, newCanonical, handle2.origin)
	require.Same(t, newCanonical, newShared.origin)
	require.Same(t, shared, handle1.parent)
	require.True(t, handle1.combined.is)
	require.Same(t, newShared.combined.tbl, handle1.combined.tbl)
	require.Equal(t, uint64(84), handle1.GetTableID(context.Background()))
	require.Equal(t, "t_partition", handle1.GetTableName())

	var resetErr error
	allocs := testing.AllocsPerRun(100, func() {
		resetErr = handle1.Reset(newOp)
	})
	require.NoError(t, resetErr)
	require.Zero(t, allocs, "steady-state relation handle reset must not allocate")
}

func TestReusableRelationHandleResetFromCatalogCacheMiss(t *testing.T) {
	oldCanonical := newTxnTableForTest()
	oldCanonical.accountId = 7
	oldCanonical.tableId = 42
	oldCanonical.tableName = "t"
	oldCanonical.db.accountId = 7
	oldCanonical.db.databaseId = 10
	oldCanonical.db.databaseName = "db"
	handle := (&txnTableDelegate{origin: oldCanonical}).NewRelationHandle().(*txnTableDelegate)

	eng := oldCanonical.eng.(*Engine)
	newOp, newTxn := newResetTxnForTest(t, eng)
	key := genTableKey(7, "t", 10, "db")
	_, cached := newTxn.tableCache.Load(key)
	require.False(t, cached)
	require.Nil(t, newTxn.tableOps.existAndActive(key))
	insertCatalogTableForResetTest(t, eng, newTxn, 7, 10, 84, "db", "t")

	require.NoError(t, handle.Reset(newOp))
	require.NotSame(t, oldCanonical, handle.origin)
	require.Equal(t, uint64(84), handle.GetTableID(context.Background()))
	require.Same(t, newOp, handle.origin.db.op)
	require.Same(t, newTxn.proc, handle.origin.proc.Load())
	require.Same(t, newTxn, handle.origin.db.getTxn())

	value, cached := newTxn.tableCache.Load(key)
	require.True(t, cached)
	require.Same(t, handle.origin, value.(*txnTableDelegate).origin)
}

func TestReusableRelationHandleResetFromCatalogMissingTable(t *testing.T) {
	oldCanonical := newTxnTableForTest()
	oldCanonical.accountId = 7
	oldCanonical.tableId = 42
	oldCanonical.tableName = "t"
	oldCanonical.db.accountId = 7
	oldCanonical.db.databaseId = 10
	oldCanonical.db.databaseName = "db"
	handle := (&txnTableDelegate{origin: oldCanonical}).NewRelationHandle().(*txnTableDelegate)

	eng := oldCanonical.eng.(*Engine)
	newOp, _ := newResetTxnForTest(t, eng)
	eng.GetLatestCatalogCache().UpdateDuration(types.TS{}, types.MaxTs())

	err := handle.Reset(newOp)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable), err)
	require.Same(t, oldCanonical, handle.origin)
}

func TestReusableRelationHandleResetRejectsDeletedTable(t *testing.T) {
	oldCanonical := newTxnTableForTest()
	oldCanonical.accountId = 0
	oldCanonical.tableId = 42
	oldCanonical.tableName = "t"
	oldCanonical.db.accountId = 0
	oldCanonical.db.databaseId = 10
	oldCanonical.db.databaseName = "db"
	handle := (&txnTableDelegate{origin: oldCanonical}).NewRelationHandle().(*txnTableDelegate)

	newOp, newTxn := newResetTxnForTest(t, oldCanonical.eng.(*Engine))
	key := genTableKey(0, "t", 10, "db")
	newTxn.tableOps.addDeleteTable(key, 0, oldCanonical.tableId)
	// A txn-local DROP must win even if a stale canonical relation is cached.
	newTxn.tableCache.Store(key, &txnTableDelegate{origin: oldCanonical})

	err := handle.Reset(newOp)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable), err)
	require.Same(t, oldCanonical, handle.origin)
}

func TestReusableRelationHandleRejectsNonDisttaeWorkspace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	canonical := newTxnTableForTest()
	canonical.tableName = "t"
	canonical.db.databaseName = "db"
	handle := (&txnTableDelegate{origin: canonical}).NewRelationHandle()

	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().GetWorkspace().Return(mock_frontend.NewMockWorkspace(ctrl))
	op.EXPECT().Status().Return(txn.TxnStatus_Active)

	require.ErrorContains(t, handle.Reset(op), "disttae transaction workspace")
}

func newPrimaryKeyCheckTableForTest(t *testing.T) (*txnTable, *Engine) {
	t.Helper()

	eng := &Engine{
		partitions: make(map[[2]uint64]*logtailreplay.Partition),
		packerPool: fileservice.NewPool(
			8,
			func() *types.Packer { return types.NewPacker() },
			func(packer *types.Packer) { packer.Reset() },
			func(packer *types.Packer) { packer.Close() },
		),
	}
	eng.catalog.Store(cache.NewCatalog())
	eng.pClient.eng = eng
	eng.pClient.subscribed = subscribedTable{
		eng: eng,
		m:   make(map[uint64]*subEntry),
	}
	eng.pClient.receivedLogTailTime.ready.Store(true)

	op, closeFn := client.NewTestTxnOperator(context.Background())
	t.Cleanup(closeFn)
	proc := testutil.NewProc(t)

	tbl := &txnTable{
		accountId: 1,
		tableId:   42,
		tableName: "t",
		db: &txnDatabase{
			databaseId:   10,
			databaseName: "db",
			op:           op,
		},
		eng:  eng,
		fake: true,
	}
	tbl.proc.Store(proc)

	part := eng.GetOrCreateLatestPart(context.Background(), 1, 10, 42)
	state, done := part.MutateState()
	state.UpdateDuration(types.TS{}, types.MaxTs())
	done()

	return tbl, eng
}

func TestPrimaryKeysMayBeModifiedRequiresReadySubscription(t *testing.T) {
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	mp := tbl.proc.Load().Mp()
	bat := makeBatchForTest(mp, 7)
	defer bat.Clean(mp)

	from := types.BuildTS(10, 0)
	to := types.BuildTS(20, 0)

	eng.pClient.SetSubscribeState(10, 42, Subscribing)
	go func() {
		time.Sleep(10 * time.Millisecond)
		eng.pClient.SetSubscribeState(10, 42, Subscribed)
	}()
	changed, err := tbl.PrimaryKeysMayBeModified(context.Background(), from, to, bat, 0, -1)
	require.NoError(t, err)
	require.False(t, changed, "the PK check must use the completed subscription instead of retrying the statement")

	eng.pClient.SetSubscribeState(10, 42, SubRspReceived)
	changed, err = tbl.PrimaryKeysMayBeModified(context.Background(), from, to, bat, 0, -1)
	require.NoError(t, err)
	require.False(t, changed, "the PK check must finish loading the subscription response before checking")

	eng.pClient.SetSubscribeState(10, 42, Subscribing)
	changed, err = tbl.PrimaryKeysMayBeUpserted(context.Background(), from, to, bat, 0)
	require.NoError(t, err)
	require.False(t, changed, "the auto-increment recheck keeps its existing lazy-state semantics")

	eng.pClient.SetSubscribeState(10, 42, Subscribed)
	changed, err = tbl.PrimaryKeysMayBeModified(context.Background(), from, to, bat, 0, -1)
	require.NoError(t, err)
	require.False(t, changed, "a complete subscribed state with no matching key should keep the fast path")
}

func TestPrimaryKeysMayBeModifiedWaitsForSubscriptionData(t *testing.T) {
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	mp := tbl.proc.Load().Mp()
	bat := makeBatchForTest(mp, 7)
	defer bat.Clean(mp)

	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())
	pkVec := vector.NewVec(types.T_int64.ToType())
	defer rowIDVec.Free(mp)
	defer tsVec.Free(mp)
	defer pkVec.Free(mp)
	require.NoError(t, vector.AppendFixed(rowIDVec, types.RandomRowid(), false, mp))
	require.NoError(t, vector.AppendFixed(tsVec, types.BuildTS(15, 0), false, mp))
	require.NoError(t, vector.AppendFixed(pkVec, int64(7), false, mp))
	insert := &api.Batch{
		Attrs: []string{"rowid", "time", "pk"},
		Vecs: []api.Vector{
			mustVectorToProtoForMaterializedSnapshotTest(t, rowIDVec),
			mustVectorToProtoForMaterializedSnapshotTest(t, tsVec),
			mustVectorToProtoForMaterializedSnapshotTest(t, pkVec),
		},
	}

	eng.pClient.SetSubscribeState(10, 42, Subscribing)
	part := eng.GetOrCreateLatestPart(context.Background(), 1, 10, 42)
	doneC := make(chan struct{})
	defer func() { <-doneC }()
	go func() {
		defer close(doneC)
		time.Sleep(10 * time.Millisecond)
		packer := types.NewPacker()
		defer packer.Close()
		state, done := part.MutateState()
		state.HandleRowsInsert(context.Background(), insert, 0, packer, mp)
		done()
		eng.pClient.SetSubscribeState(10, 42, Subscribed)
	}()

	changed, err := tbl.PrimaryKeysMayBeModified(
		context.Background(),
		types.BuildTS(10, 0),
		types.BuildTS(20, 0),
		bat,
		0,
		-1,
	)
	require.NoError(t, err)
	require.True(t, changed, "the PK check must inspect versions loaded by the completed subscription")
}

func TestPrimaryKeysMayBeModifiedStopsWaitingWhenSubscriptionContextEnds(t *testing.T) {
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	mp := tbl.proc.Load().Mp()
	bat := makeBatchForTest(mp, 7)
	defer bat.Clean(mp)

	eng.pClient.SetSubscribeState(10, 42, Subscribing)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := tbl.PrimaryKeysMayBeModified(
		ctx,
		types.BuildTS(10, 0),
		types.BuildTS(20, 0),
		bat,
		0,
		-1,
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPrimaryKeysMayBeModifiedFailsFastWhenPushClientIsNotReady(t *testing.T) {
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	mp := tbl.proc.Load().Mp()
	bat := makeBatchForTest(mp, 7)
	defer bat.Clean(mp)

	eng.pClient.SetSubscribeState(10, 42, Subscribed)
	eng.pClient.receivedLogTailTime.ready.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := tbl.PrimaryKeysMayBeModified(
		ctx,
		types.BuildTS(10, 0),
		types.BuildTS(20, 0),
		bat,
		0,
		-1,
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart))
}

func TestPrimaryKeysMayBeModifiedSkipsReadinessForEmptyKeys(t *testing.T) {
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	mp := tbl.proc.Load().Mp()
	bat := makeBatchForTest(mp)
	defer bat.Clean(mp)

	eng.pClient.SetSubscribeState(10, 42, Subscribing)
	changed, err := tbl.PrimaryKeysMayBeModified(
		context.Background(),
		types.BuildTS(10, 0),
		types.BuildTS(20, 0),
		bat,
		0,
		-1,
	)
	require.NoError(t, err)
	require.False(t, changed, "an empty key set cannot conflict even while the table is rebuilding")
}

func TestPrimaryKeysMayBeModifiedHonorsPendingTableApply(t *testing.T) {
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	mp := tbl.proc.Load().Mp()
	bat := makeBatchForTest(mp, 7)
	defer bat.Clean(mp)

	from := types.BuildTS(10, 0)
	to := types.BuildTS(20, 0)
	eng.pClient.SetSubscribeState(10, 42, Subscribed)
	eng.pClient.subscribed.setTablePendingUpdate(10, 42, to.ToTimestamp())

	part := eng.GetOrCreateLatestPart(context.Background(), 1, 10, 42)
	go func() {
		time.Sleep(10 * time.Millisecond)
		state, done := part.MutateState()
		state.UpdateAppliedTo(to.Prev())
		done()
	}()

	changed, err := tbl.PrimaryKeysMayBeModified(context.Background(), from, to, bat, 0, -1)
	require.NoError(t, err)
	require.False(t, changed, "the PK check must wait for the pending table update before proving no change")
}

func TestPrimaryKeysMayBeModifiedForTableCreatedInTxn(t *testing.T) {
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	tbl.fake = false
	tbl.remoteWorkspace = true
	tbl.createdInTxn = true
	eng.pClient.receivedLogTailTime.ready.Store(false)

	mp := tbl.proc.Load().Mp()
	bat := makeBatchForTest(mp, 7)
	defer bat.Clean(mp)

	changed, err := tbl.PrimaryKeysMayBeModified(
		context.Background(),
		types.BuildTS(10, 0),
		types.BuildTS(20, 0),
		bat,
		0,
		-1,
	)
	require.NoError(t, err)
	require.False(t, changed,
		"a table created in this transaction has no committed remote history even during reconnect")
}

func TestPKCheckHistoricalSnapshotFallbackIsTerminal(t *testing.T) {
	tbl, eng := newPrimaryKeyCheckTableForTest(t)
	tbl.db.op.AddWorkspace(&Transaction{engine: eng})
	snapshot := types.BuildTS(15, 0)
	to := types.BuildTS(20, 0)
	tbl.db.op.SetSnapshotTS(snapshot.ToTimestamp())

	eng.snapshotMgr = NewSnapshotManager()
	eng.snapshotMgr.Init()
	historical := logtailreplay.NewPartition(
		eng.service,
		eng.GetLatestCatalogCache(),
		uint64(tbl.accountId),
		tbl.db.databaseId,
		tbl.tableId,
		nil,
	)
	state, done := historical.MutateState()
	state.UpdateDuration(types.TS{}, to)
	done()
	expected := eng.snapshotMgr.Add(
		tbl.db.databaseId,
		tbl.tableId,
		historical,
		tbl.tableName,
		snapshot,
	)

	oldSnapshotRead := RequestSnapshotRead
	snapshotReads := 0
	RequestSnapshotRead = func(
		context.Context,
		*txnTable,
		*types.TS,
	) (any, error) {
		snapshotReads++
		start := types.TS{}.ToTimestamp()
		end := to.ToTimestamp()
		return &cmd_util.SnapshotReadResp{
			Succeed: true,
			Entries: []*cmd_util.CheckpointEntryResp{
				{
					Start: &start,
					End:   &end,
				},
			},
		}, nil
	}
	t.Cleanup(func() { RequestSnapshotRead = oldSnapshotRead })
	subscribeAttempts := 0
	eng.pClient.subscriber = newLogTailSubscriber()
	eng.pClient.subscriber.sendSubscribe = func(context.Context, api.TableID) error {
		subscribeAttempts++
		return nil
	}
	eng.pClient.subscriber.setReady()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	eng.pClient.SetSubscribeState(10, 42, SubRspTableNotExist)
	ps, ready, err := tbl.getPartitionStateForPKCheck(ctx, to)
	require.NoError(t, err)
	require.True(t, ready)
	require.Same(t, expected, ps)
	require.Equal(t, 1, snapshotReads,
		"a successful historical fallback must not start another subscription cycle")
	require.Zero(t, subscribeAttempts)

	eng.pClient.SetSubscribeState(10, 42, SubRspTableNotExist)
	ps, ready, err = tbl.getPartitionStateForPKCheck(ctx, to.Next())
	require.NoError(t, err)
	require.False(t, ready,
		"a historical fallback outside its physical range must remain conservative")
	require.Same(t, expected, ps)
	require.Equal(t, 2, snapshotReads,
		"an out-of-range fallback must also terminate after one snapshot read")
	require.Zero(t, subscribeAttempts)
}

// func TestPrimaryKeyCheck(t *testing.T) {
// 	ctx := context.Background()
// 	mp := mpool.MustNewZero()

// 	getRowIDsBatch := func(table *txnTable) *batch.Batch {
// 		bat := batch.New(false, []string{catalog.Row_ID})
// 		vec := vector.NewVec(types.T_Rowid.ToType())
// 		iter := table.localState.NewRowsIter(
// 			types.TimestampToTS(table.nextLocalTS()),
// 			nil,
// 			false,
// 		)
// 		l := 0
// 		for iter.Next() {
// 			entry := iter.Entry()
// 			vector.AppendFixed(vec, entry.RowID, false, mp)
// 			l++
// 		}
// 		iter.Close()
// 		bat.SetVector(0, vec)
// 		bat.SetZs(l, mp)
// 		return bat
// 	}

// 	table := newTxnTableForTest(mp)

// 	// insert
// 	err := table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 1),
// 	)
// 	assert.Nil(t, err)

// 	// // insert duplicated
// 	// we check duplicated in pipeline runing now
// 	// err = table.Write(
// 	// 	ctx,
// 	// 	makeBatchForTest(mp, 1),
// 	// )
// 	// assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))

// 	// insert no duplicated
// 	err = table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 2, 3),
// 	)
// 	assert.Nil(t, err)

// 	// duplicated in same batch
// 	// we check duplicated in pipeline runing now
// 	// err = table.Write(
// 	// 	ctx,
// 	// 	makeBatchForTest(mp, 4, 4),
// 	// )
// 	// assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))

// 	table = newTxnTableForTest(mp)

// 	// insert, delete then insert
// 	err = table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 1),
// 	)
// 	assert.Nil(t, err)
// 	err = table.Delete(
// 		ctx,
// 		getRowIDsBatch(table),
// 		catalog.Row_ID,
// 	)
// 	assert.Nil(t, err)
// 	err = table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 5),
// 	)
// 	assert.Nil(t, err)

// }

func BenchmarkTxnTableInsert(b *testing.B) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	table := newTxnTableForTest()
	for i, max := int64(0), int64(b.N); i < max; i++ {
		err := table.Write(
			ctx,
			makeBatchForTest(mp, i),
		)
		assert.Nil(b, err)
	}
}

func TestWorkspaceInsertRowEstimate(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	mem := batch.NewWithSize(0)
	mem.SetRowCount(5)
	other := batch.NewWithSize(0)
	other.SetRowCount(500)
	meta := batch.NewWithSize(1)
	meta.Attrs = []string{catalog.ObjectMeta_ObjectStats}
	meta.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { meta.Clean(txn.proc.Mp()) })
	for _, rows := range []uint32{8192, 1844} {
		stats := objectio.NewObjectStats()
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, rows))
		require.NoError(t, vector.AppendBytes(meta.Vecs[0], stats.Marshal(), false, txn.proc.Mp()))
	}
	meta.SetRowCount(2)
	txn.writes = []Entry{
		{typ: INSERT, databaseId: 7, tableId: 42, bat: mem},
		{typ: INSERT, databaseId: 7, tableId: 43, bat: other},
		{typ: DELETE, databaseId: 7, tableId: 42, bat: mem},
		{typ: INSERT, databaseId: 7, tableId: 42, bat: meta, fileName: "object"},
	}
	txn.snapshotWriteOffset.Store(0)
	require.Equal(t, float64(10041), tbl.workspaceInsertRowEstimate(),
		"planning precedes execution boundary advancement; include prior writes, ignoring unrelated inserts/deletes")
	txn.writes = txn.writes[:3]
	require.Equal(t, float64(5), tbl.workspaceInsertRowEstimate(), "rolled-back entries no longer contribute")
	txn.writes = txn.writes[:4]
	txn.Lock()
	got := tbl.workspaceInsertRowEstimate()
	txn.Unlock()
	require.Equal(t, float64(^uint64(0)), got, "internal SQL must not reenter the workspace mutex")
	txn.readOnly.Store(true)
	txn.Lock()
	got = tbl.workspaceInsertRowEstimate()
	txn.Unlock()
	require.Zero(t, got, "read-only fast path neither locks nor scans")
	txn.readOnly.Store(false)
	meta.Attrs[0] = "wrong_attribute"
	require.Equal(t, float64(^uint64(0)), tbl.workspaceInsertRowEstimate(), "missing object metadata cannot leave a partial low bound")
	meta.Attrs[0] = catalog.ObjectMeta_ObjectStats
	meta.SetRowCount(3)
	require.Equal(t, float64(^uint64(0)), tbl.workspaceInsertRowEstimate(), "incomplete metadata cannot leave a partial low bound")
}

func TestTransientTableStatsPreservePublishedOwner(t *testing.T) {
	published := &pbstats.StatsInfo{TableName: "events", TableCnt: 5, AccurateObjectNumber: 1,
		NdvMap: map[string]float64{"id": 5}, SizeMap: map[string]uint64{"id": 40, "payload": 40960}}
	got := transientTableStats(published, 10005)
	require.Equal(t, float64(10005), got.TableCnt)
	require.Empty(t, got.TableName)
	require.Equal(t, published.AccurateObjectNumber, got.AccurateObjectNumber)
	require.Equal(t, published.NdvMap, got.NdvMap)
	require.Equal(t, map[string]uint64{"id": 80040, "payload": 81960960}, got.SizeMap)
	require.Equal(t, float64(8192), float64(got.SizeMap["payload"])/got.TableCnt)
	got.SizeMap["payload"] = 1
	require.Equal(t, uint64(40960), published.SizeMap["payload"], "temporary byte estimates cannot mutate the published map")
	require.Equal(t, "events", published.TableName)
	require.Equal(t, float64(5), published.TableCnt, "workspace overlay cannot mutate committed Rows/Size statistics")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	tbl.tableId = 43 // a committed table, distinct from the fixture's created ID
	eng := mock_frontend.NewMockEngine(gomock.NewController(t))
	tbl.eng = eng
	eng.EXPECT().Stats(gomock.Any(), gomock.Any(), false).Return(published).AnyTimes()
	txn.readOnly.Store(true)
	actual, err := tbl.Stats(context.Background(), false)
	require.NoError(t, err)
	require.Same(t, published, actual, "readonly completed observation retains the original fast owner")
	tbl.remoteWorkspace = true
	actual, err = tbl.Stats(context.Background(), false)
	require.NoError(t, err)
	require.Same(t, published, actual, "remote readonly completed observation remains available")
	txn.readOnly.Store(false)
	actual, err = tbl.Stats(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, float64(^uint64(0)), actual.TableCnt, "remote workspace cannot publish a partial global bound")
	require.Empty(t, actual.TableName)
}

func TestPartitionRowEstimate(t *testing.T) {
	state := logtailreplay.NewPartitionState("test", false, 42, false)
	rows, err := partitionRowEstimate(state, types.MaxTs())
	require.NoError(t, err)
	require.Zero(t, rows)
	mp := mpool.MustNew("transient-stat-test")
	bat := batch.NewWithSize(1)
	bat.Attrs = []string{"v"}
	bat.Vecs[0] = testutil.MakeVarcharVector([]string{"a", "b", "c"}, nil, mp)
	bat.SetRowCount(3)
	defer bat.Clean(mp)
	insert, err := fillRandomRowidAndZeroTs(bat, mp)
	require.NoError(t, err)
	packer := types.NewPacker()
	defer packer.Close()
	state.HandleRowsInsert(context.Background(), insert, 0, packer, mp)
	rows, err = partitionRowEstimate(state, types.MaxTs())
	require.NoError(t, err)
	require.Equal(t, float64(3), rows)
	for i, count := range []uint32{8192, 1844, 0} {
		oid := types.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&oid, false, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, count))
		require.NoError(t, objectio.SetObjectStatsSize(stats, 1))
		require.NoError(t, state.HandleObjectEntry(context.Background(), nil, objectio.ObjectEntry{
			ObjectStats: *stats, CreateTime: types.BuildTS(int64(i+1), 0),
		}, false))
	}
	rows, err = partitionRowEstimate(state, types.MaxTs())
	require.NoError(t, err)
	require.Equal(t, float64(3+8192+1844)+float64(^uint32(0)), rows, "unknown object is not a partial zero bound")
	state.UpdateDuration(types.BuildTS(10, 0), types.MaxTs())
	_, err = partitionRowEstimate(state, types.BuildTS(9, 0))
	require.Error(t, err, "historical state must not be admitted as current statistics")
}

func TestPartitionRowEstimateAfterAppendableFlush(t *testing.T) {
	state := logtailreplay.NewPartitionState("test", false, 42, false)
	for i, spec := range []struct {
		rows             uint32
		appendable       bool
		created, deleted int64
	}{{5, true, 10, 20}, {5, false, 20, 0}, {7, false, 30, 0}, {0, true, 40, 50}} {
		oid := types.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&oid, spec.appendable, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, spec.rows))
		require.NoError(t, objectio.SetObjectStatsSize(stats, 1))
		require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
		entry := objectio.ObjectEntry{ObjectStats: *stats, CreateTime: types.BuildTS(spec.created, 0)}
		if spec.deleted != 0 {
			entry.DeleteTime = types.BuildTS(spec.deleted, 0)
		}
		require.NoError(t, state.HandleObjectEntry(context.Background(), nil, entry, false), i)
	}
	for _, snapshot := range []int64{15, 20, 25} {
		rows, err := partitionRowEstimate(state, types.BuildTS(snapshot, 0))
		require.NoError(t, err)
		require.Equal(t, float64(5), rows, "sealed historical appendable objects must not inflate a five-row source")
	}
	rows, err := partitionRowEstimate(state, types.BuildTS(30, 0))
	require.NoError(t, err)
	require.Equal(t, float64(12), rows, "future objects become eligible only at their creation snapshot")
	rows, err = partitionRowEstimate(state, types.BuildTS(45, 0))
	require.NoError(t, err)
	require.Equal(t, float64(12)+float64(math.MaxUint32), rows, "visible sealed metadata with unknown rows still fails closed")
	rows, err = partitionRowEstimate(state, types.BuildTS(50, 0))
	require.NoError(t, err)
	require.Equal(t, float64(12), rows, "a deleted unknown object cannot inflate a later snapshot")

}

func TestTransientTableStatsByteBounds(t *testing.T) {
	for _, tc := range []struct {
		name             string
		oldRows, newRows float64
		sizes, want      map[string]uint64
	}{
		{"round_up", 3, 5, map[string]uint64{"v": 2, "empty": 0}, map[string]uint64{"v": 4, "empty": 0}},
		{"same", 5, 5, map[string]uint64{"v": 40960}, map[string]uint64{"v": 40960}},
		{"smaller", 5, 3, map[string]uint64{"v": 40960}, map[string]uint64{"v": 40960}},
		{"missing_denominator", 0, 5, map[string]uint64{"v": 1}, nil},
		{"nan_denominator", math.NaN(), 5, map[string]uint64{"v": 1}, nil},
		{"infinite_denominator", math.Inf(1), 5, map[string]uint64{"v": 1}, nil},
		{"invalid_new_rows", 5, math.Inf(1), map[string]uint64{"v": 1}, nil},
		{"individual_overflow", 5, 10, map[string]uint64{"v": math.MaxUint64, "small": 1}, nil},
		{"sum_overflow", 5, 10, map[string]uint64{"a": math.MaxUint64 / 3, "b": math.MaxUint64 / 3}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			published := &pbstats.StatsInfo{TableCnt: tc.oldRows, SizeMap: tc.sizes}
			got := transientTableStats(published, tc.newRows)
			require.Equal(t, tc.want, got.SizeMap)
			require.Equal(t, tc.sizes, published.SizeMap)
		})
	}
}

func TestWorkspaceEstimateUnderConcurrentWriter(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	bat := batch.NewWithSize(0)
	bat.SetRowCount(5)
	txn.Lock()
	locked := true
	defer func() {
		if locked {
			txn.Unlock()
		}
	}()
	result := make(chan float64, 1)
	go func() { result <- tbl.workspaceInsertRowEstimate() }()
	select {
	case rows := <-result:
		require.Equal(t, float64(^uint64(0)), rows, "unavailable workspace must not wait or expose a partial small estimate")
	case <-time.After(time.Second):
		t.Fatal("planner waited for a workspace writer")
	}
	txn.writes = []Entry{{typ: INSERT, databaseId: 7, tableId: 42, bat: bat}}
	txn.Unlock()
	locked = false
	require.Equal(t, float64(5), tbl.workspaceInsertRowEstimate(), "subsequent admission observes the released writer")
}

func BenchmarkWorkspaceInsertRowEstimate(b *testing.B) {
	for _, entries := range []int{100, 10000, 100000} {
		b.Run(fmt.Sprint(entries), func(b *testing.B) {
			txn := newTransactionWithActivePKTableForTest(b, "pk")
			tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
			bat := batch.NewWithSize(0)
			bat.SetRowCount(5)
			txn.writes = make([]Entry, entries)
			for i := range txn.writes {
				txn.writes[i] = Entry{typ: INSERT, databaseId: 7, tableId: 43, bat: bat}
			}
			txn.writes[0].tableId = 42
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if tbl.workspaceInsertRowEstimate() != 5 {
					b.Fatal("unexpected target estimate")
				}
			}
		})
	}
}
