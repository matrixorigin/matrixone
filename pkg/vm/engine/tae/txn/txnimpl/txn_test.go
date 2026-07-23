// Copyright 2021 Matrix Origin
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

package txnimpl

import (
	"context"
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ModuleName = "TAETXN"
)

type noopReplayObserver struct{}

func (noopReplayObserver) OnTimeStamp(types.TS) {}

func newPreparingEpochTestTxn(t *testing.T, id string, start, prepare types.TS) *txnbase.Txn {
	t.Helper()
	txn := txnbase.NewTxn(nil, nil, []byte(id), start, types.TS{})
	txn.Lock()
	assert.NoError(t, txn.ToPreparingLocked(prepare))
	txn.Unlock()
	return txn
}

func TestAutoIncrementAlterDetectsAcceptedDMLAfterSnapshot(t *testing.T) {
	entry := catalog.MockTableEntryWithDB(nil, 42)
	var wg sync.WaitGroup
	for _, ts := range []types.TS{types.BuildTS(5, 0), types.BuildTS(3, 0), types.BuildTS(7, 0)} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry.RecordKnownDMLPrepare(ts)
		}()
	}
	wg.Wait()
	assert.Equal(t, types.BuildTS(7, 0), entry.GetLatestKnownDMLPrepare())

	unsafeTxn := newPreparingEpochTestTxn(t, "alter-retry", types.BuildTS(6, 0), types.BuildTS(8, 0))
	unsafe := &txnTable{store: &txnStore{txn: unsafeTxn}, entry: entry, autoIncrementAlter: true}
	err := unsafe.validateAutoIncrementDMLOrder()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), err)

	safeTxn := newPreparingEpochTestTxn(t, "alter-safe", types.BuildTS(7, 0), types.BuildTS(8, 0))
	safe := &txnTable{store: &txnStore{txn: safeTxn}, entry: entry, autoIncrementAlter: true}
	assert.NoError(t, safe.validateAutoIncrementDMLOrder())
}

func TestReplayOnePCRebuildsAutoIncrementDMLWatermark(t *testing.T) {
	c := catalog.MockCatalog(nil)
	defer c.Close()
	mgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	mgr.Start(context.Background())
	defer mgr.Stop()

	setupTxn, err := mgr.StartTxn(nil)
	assert.NoError(t, err)
	dbEntry, err := c.CreateDBEntry("replay_1pc", "", "", setupTxn)
	assert.NoError(t, err)
	tableEntry, err := dbEntry.CreateTableEntry(catalog.MockSchemaAll(3, 1), setupTxn, nil)
	assert.NoError(t, err)
	assert.NoError(t, setupTxn.Commit(context.Background()))

	startTS := types.BuildTS(10, 0)
	prepareTS := types.BuildTS(11, 0)
	commitTS := types.BuildTS(12, 0)
	replayTxn := newPreparingEpochTestTxn(t, "replay-1pc", startTS, prepareTS)
	assert.False(t, replayTxn.Is2PC())
	replayTxn.GetMemo().AddTable(dbEntry.ID, tableEntry.ID)
	assert.NoError(t, replayTxn.SetCommitTS(commitTS))
	store := &replayTxnStore{Cmd: &txnbase.TxnCmd{ComposedCmd: txnbase.NewComposedCmd()}, Observer: noopReplayObserver{}, catalog: c}

	assert.NoError(t, store.prepareCommit(replayTxn))
	assert.True(t, tableEntry.ShouldRetryAutoIncrementAlter(startTS.Prev()))
	assert.NoError(t, store.applyCommit(replayTxn))
	assert.Equal(t, commitTS, tableEntry.GetLatestKnownDMLPrepare())
}

func TestReplayPreparedRollbackReleasesAutoIncrementFence(t *testing.T) {
	c := catalog.MockCatalog(nil)
	defer c.Close()
	mgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(c), catalog.MockTxnFactory(c), types.NewMockHLCClock(1))
	mgr.Start(context.Background())
	defer mgr.Stop()

	setupTxn, err := mgr.StartTxn(nil)
	assert.NoError(t, err)
	dbEntry, err := c.CreateDBEntry("replay_rollback", "", "", setupTxn)
	assert.NoError(t, err)
	tableEntry, err := dbEntry.CreateTableEntry(catalog.MockSchemaAll(3, 1), setupTxn, nil)
	assert.NoError(t, err)
	assert.NoError(t, setupTxn.Commit(context.Background()))

	startTS := types.BuildTS(10, 0)
	replayTxn := newPreparingEpochTestTxn(t, "replay-2pc", startTS, types.BuildTS(11, 0))
	assert.NoError(t, replayTxn.SetParticipants([]uint64{1, 2}))
	assert.True(t, replayTxn.Is2PC())
	replayTxn.GetMemo().AddTable(dbEntry.ID, tableEntry.ID)
	store := &replayTxnStore{Cmd: &txnbase.TxnCmd{ComposedCmd: txnbase.NewComposedCmd()}, Observer: noopReplayObserver{}, catalog: c}

	assert.NoError(t, store.prepareCommit(replayTxn))
	assert.True(t, tableEntry.ShouldRetryAutoIncrementAlter(startTS))
	assert.NoError(t, store.applyRollback(replayTxn))
	assert.False(t, tableEntry.ShouldRetryAutoIncrementAlter(startTS))
	watermark := tableEntry.GetLatestKnownDMLPrepare()
	assert.True(t, watermark.IsEmpty())
}

type waitingSchemaTxn struct {
	txnif.TxnReader
	prepareTS types.TS
	waited    chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (txn *waitingSchemaTxn) GetPrepareTS() types.TS { return txn.prepareTS }
func (txn *waitingSchemaTxn) GetCommitTS() types.TS  { return txn.prepareTS }
func (txn *waitingSchemaTxn) GetTxnState(wait bool) txnif.TxnState {
	if !wait {
		return txnif.TxnStatePreparing
	}
	txn.once.Do(func() { close(txn.waited) })
	<-txn.release
	return txnif.TxnStateCommitted
}

func TestAutoIncrEpochFenceWaitsForEarlierSchemaPrepare(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rollback bool
	}{
		{name: "commit retries"},
		{name: "rollback continues", rollback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := catalog.MockSchemaAll(3, 1)
			schema.Version = 7
			schema.Extra.AutoIncrEpoch = 7
			entry := catalog.MockTableEntryWithDB(nil, 42)
			entry.CreateWithTSLocked(types.BuildTS(1, 0), &catalog.TableMVCCNode{
				Schema:          schema,
				TombstoneSchema: catalog.GetTombstoneSchema(schema),
			})

			alterBase := txnbase.NewTxn(nil, nil, []byte("alter"), types.BuildTS(2, 0), types.TS{})
			alterTxn := &waitingSchemaTxn{
				TxnReader: alterBase,
				prepareTS: types.BuildTS(3, 0),
				waited:    make(chan struct{}),
				release:   make(chan struct{}),
			}
			_, _, err := entry.AlterTable(context.Background(), alterTxn,
				apipb.NewUpdateAutoIncrementReq(0, 42, 20, 8))
			assert.NoError(t, err)

			dmlTxn := txnbase.NewTxn(nil, nil, []byte("dml"), types.BuildTS(2, 1), types.TS{})
			dmlTxn.Lock()
			assert.NoError(t, dmlTxn.ToPreparingLocked(types.BuildTS(4, 0)))
			dmlTxn.Unlock()
			tbl := &txnTable{
				store:                  &txnStore{txn: dmlTxn},
				entry:                  entry,
				expectedAutoIncrEpochs: map[uint32]struct{}{7: {}},
			}

			result := make(chan error, 1)
			go func() { result <- tbl.validateAutoIncrEpoch() }()
			select {
			case <-alterTxn.waited:
			case <-time.After(5 * time.Second):
				t.Fatal("version fence did not wait for earlier schema prepare")
			}

			if tc.rollback {
				_, err = entry.BaseEntryImpl.PrepareRollback()
				assert.NoError(t, err)
			} else {
				assert.NoError(t, entry.BaseEntryImpl.PrepareCommit())
				assert.NoError(t, entry.BaseEntryImpl.ApplyCommit(alterTxn.GetID()))
			}
			close(alterTxn.release)

			select {
			case err = <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("version fence did not resume after schema transaction finished")
			}
			if tc.rollback {
				assert.NoError(t, err)
			} else {
				assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), err)
			}
		})
	}
}

func TestAutoIncrEpochDependencyRecordsMultipleExpectations(t *testing.T) {
	tbl := new(txnTable)
	assert.NoError(t, tbl.setExpectedAutoIncrEpoch(0))
	assert.NoError(t, tbl.setExpectedAutoIncrEpoch(7))
	assert.NoError(t, tbl.setExpectedAutoIncrEpoch(7))
	assert.NoError(t, tbl.setExpectedAutoIncrEpoch(8))
	assert.Equal(t, map[uint32]struct{}{0: {}, 7: {}, 8: {}}, tbl.expectedAutoIncrEpochs)
}

func TestAutoIncrEpochFenceUsesPrepareOrderForCommittedSchema(t *testing.T) {
	schema := catalog.MockSchemaAll(3, 1)
	schema.Version = 7
	schema.Extra.AutoIncrEpoch = 7
	entry := catalog.MockTableEntryWithDB(nil, 42)
	entry.CreateWithTSLocked(types.BuildTS(1, 0), &catalog.TableMVCCNode{
		Schema: schema, TombstoneSchema: catalog.GetTombstoneSchema(schema),
	})

	alterBase := txnbase.NewTxn(nil, nil, []byte("later-alter"), types.BuildTS(2, 0), types.TS{})
	alterTxn := &waitingSchemaTxn{
		TxnReader: alterBase,
		prepareTS: types.BuildTS(5, 0),
		waited:    make(chan struct{}),
		release:   make(chan struct{}),
	}
	_, _, err := entry.AlterTable(context.Background(), alterTxn,
		apipb.NewUpdateAutoIncrementReq(0, 42, 20, 8))
	assert.NoError(t, err)
	assert.NoError(t, entry.BaseEntryImpl.PrepareCommit())
	assert.NoError(t, entry.BaseEntryImpl.ApplyCommit(alterTxn.GetID()))

	dmlTxn := txnbase.NewTxn(nil, nil, []byte("earlier-dml"), types.BuildTS(2, 1), types.TS{})
	dmlTxn.Lock()
	assert.NoError(t, dmlTxn.ToPreparingLocked(types.BuildTS(4, 0)))
	dmlTxn.Unlock()
	tbl := &txnTable{
		store:                  &txnStore{txn: dmlTxn},
		entry:                  entry,
		expectedAutoIncrEpochs: map[uint32]struct{}{7: {}},
	}
	assert.NoError(t, tbl.validateAutoIncrEpoch())
}

func TestAutoIncrEpochFenceRejectsMultipleEpochsWithoutLocalAlter(t *testing.T) {
	schema := catalog.MockSchemaAll(3, 1)
	schema.Version = 7
	schema.Extra.AutoIncrEpoch = 7
	entry := catalog.MockTableEntryWithDB(nil, 42)
	entry.CreateWithTSLocked(types.BuildTS(1, 0), &catalog.TableMVCCNode{
		Schema: schema, TombstoneSchema: catalog.GetTombstoneSchema(schema),
	})

	dmlTxn := txnbase.NewTxn(nil, nil, []byte("dml"), types.BuildTS(2, 0), types.TS{})
	dmlTxn.Lock()
	assert.NoError(t, dmlTxn.ToPreparingLocked(types.BuildTS(3, 0)))
	dmlTxn.Unlock()
	tbl := &txnTable{
		store:                  &txnStore{txn: dmlTxn},
		entry:                  entry,
		expectedAutoIncrEpochs: map[uint32]struct{}{7: {}, 8: {}},
	}
	err := tbl.validateAutoIncrEpoch()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), err)
}

func TestTable(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(3, 2)
	schema.Extra.BlockMaxRows = 10000
	schema.Extra.ObjectMaxBlocks = 10
	{
		txn, _ := mgr.StartTxn(nil)
		db, err := txn.CreateDatabase("db", "", "")
		assert.Nil(t, err)
		rel, _ := db.CreateRelation(schema)
		bat := catalog.MockBatch(schema, int(mpool.KB)*100)
		defer bat.Close()
		bats := bat.Split(100)
		for _, data := range bats {
			err := rel.Append(context.Background(), data)
			assert.Nil(t, err)
		}
		tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
		tbl, _ := tDB.getOrSetTable(rel.ID())
		err = tbl.RangeDeleteLocalRows(1024+20, 1024+30)
		assert.Nil(t, err)
		err = tbl.RangeDeleteLocalRows(1024*2+38, 1024*2+40)
		assert.Nil(t, err)
		err = tbl.RangeDeleteLocalRows(1024*10+38, 1024*40+40)
		assert.Nil(t, err)
		assert.True(t, tbl.dataTable.tableSpace.IsDeleted(1024+20))
		assert.True(t, tbl.dataTable.tableSpace.IsDeleted(1024+30))
		assert.True(t, tbl.dataTable.tableSpace.IsDeleted(1024*10+38))
		assert.True(t, tbl.dataTable.tableSpace.IsDeleted(1024*40+40))
		assert.True(t, tbl.dataTable.tableSpace.IsDeleted(1024*30+40))
		assert.False(t, tbl.dataTable.tableSpace.IsDeleted(1024+19))
		assert.False(t, tbl.dataTable.tableSpace.IsDeleted(1024+31))
		assert.False(t, tbl.dataTable.tableSpace.IsDeleted(1024*10+37))
		assert.False(t, tbl.dataTable.tableSpace.IsDeleted(1024*40+41))
		err = txn.Commit(context.Background())
		assert.Nil(t, err)
	}
}

func TestAppend(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(3, 1)
	schema.Extra.BlockMaxRows = 10000
	schema.Extra.ObjectMaxBlocks = 10

	txn, _ := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db", "", "")
	rel, _ := db.CreateRelation(schema)
	tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
	tbl, _ := tDB.getOrSetTable(rel.ID())
	rows := uint64(MaxNodeRows) / 8 * 3
	brows := rows / 3

	bat := catalog.MockBatch(tbl.GetLocalSchema(false), int(rows))
	defer bat.Close()
	bats := bat.Split(3)

	err := tbl.BatchDedupLocal(bats[0])
	assert.Nil(t, err)
	err = tbl.Append(context.Background(), bats[0])
	assert.Nil(t, err)
	assert.Equal(t, int(brows), int(tbl.dataTable.tableSpace.Rows()))
	assert.Equal(t, int(brows), int(tbl.dataTable.tableSpace.index.Count()))

	err = tbl.BatchDedupLocal(bats[0])
	assert.NotNil(t, err)

	err = tbl.BatchDedupLocal(bats[1])
	assert.Nil(t, err)
	err = tbl.Append(context.Background(), bats[1])
	assert.Nil(t, err)
	assert.Equal(t, 2*int(brows), int(tbl.dataTable.tableSpace.Rows()))
	assert.Equal(t, 2*int(brows), int(tbl.dataTable.tableSpace.index.Count()))

	err = tbl.BatchDedupLocal(bats[2])
	assert.Nil(t, err)
	err = tbl.Append(context.Background(), bats[2])
	assert.Nil(t, err)
	assert.Equal(t, 3*int(brows), int(tbl.dataTable.tableSpace.Rows()))
	assert.Equal(t, 3*int(brows), int(tbl.dataTable.tableSpace.index.Count()))
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestIndex(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	index := NewSimpleTableIndex()
	err := index.Insert(1, 10)
	assert.Nil(t, err)
	err = index.Insert("one", 10)
	assert.Nil(t, err)
	row, err := index.Search("one")
	assert.Nil(t, err)
	assert.Equal(t, 10, int(row))
	err = index.Delete("one")
	assert.Nil(t, err)
	_, err = index.Search("one")
	assert.NotNil(t, err)

	schema := catalog.MockSchemaAll(14, 1)
	bat := catalog.MockBatch(schema, 500)
	defer bat.Close()

	idx := NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Attrs[0], bat.Vecs[0])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Attrs[0], bat.Vecs[0], 0, bat.Vecs[0].Length(), 0, false)
	assert.NotNil(t, err)

	err = idx.BatchDedup(bat.Attrs[1], bat.Vecs[1])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Attrs[1], bat.Vecs[1], 0, bat.Vecs[1].Length(), 0, false)
	assert.Nil(t, err)

	window := bat.Vecs[1].Window(20, 2)
	assert.Equal(t, 2, window.Length())
	err = idx.BatchDedup(bat.Attrs[1], window)
	assert.NotNil(t, err)

	schema = catalog.MockSchemaAll(14, 12)
	bat = catalog.MockBatch(schema, 500)
	defer bat.Close()
	idx = NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Attrs[12], bat.Vecs[12])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Attrs[12], bat.Vecs[12], 0, bat.Vecs[12].Length(), 0, false)
	assert.Nil(t, err)

	window = bat.Vecs[12].Window(20, 2)
	assert.Equal(t, 2, window.Length())
	err = idx.BatchDedup(bat.Attrs[12], window)
	assert.Error(t, err)

	// T_array
	schema = catalog.MockSchemaAll(20, 12)
	bat = catalog.MockBatch(schema, 500)
	defer bat.Close()
	idx = NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Attrs[19], bat.Vecs[19])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Attrs[19], bat.Vecs[19], 0, bat.Vecs[19].Length(), 0, false)
	assert.Nil(t, err)

	window = bat.Vecs[19].Window(20, 2)
	assert.Equal(t, 2, window.Length())
	err = idx.BatchDedup(bat.Attrs[19], window)
	assert.Error(t, err)
}

func TestDedupOpDuplicateEntry(t *testing.T) {
	colType := types.T_int64.ToType()
	tree := map[any]uint32{int64(7): 1}

	err := DedupOp(&colType, "pk", []int64{3, 7}, tree)
	assert.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
}

func TestInsertOpDuplicateEntry(t *testing.T) {
	t.Run("dedup input", func(t *testing.T) {
		colType := types.T_int64.ToType()
		tree := make(map[any]uint32)

		err := InsertOp(&colType, "pk", []int64{5, 5}, 0, 2, 0, true, tree)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
		assert.Len(t, tree, 0)
	})

	t.Run("tree conflict", func(t *testing.T) {
		colType := types.T_int64.ToType()
		tree := map[any]uint32{int64(9): 2}

		err := InsertOp(&colType, "pk", []int64{9, 10}, 0, 2, 0, false, tree)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})
}

func TestSimpleTableIndexVarlenDuplicatePaths(t *testing.T) {
	t.Run("string batch insert dedup input", func(t *testing.T) {
		idx := NewSimpleTableIndex()
		vec := makeWorkspaceVector(types.T_varchar.ToType())
		defer vec.Close()
		vec.Append([]byte("dup"), false)
		vec.Append([]byte("dup"), false)

		err := idx.BatchInsert("pk", vec, 0, vec.Length(), 0, true)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("string batch insert tree conflict", func(t *testing.T) {
		idx := NewSimpleTableIndex()
		idx.tree["dup"] = 1
		vec := makeWorkspaceVector(types.T_varchar.ToType())
		defer vec.Close()
		vec.Append([]byte("dup"), false)

		err := idx.BatchInsert("pk", vec, 0, vec.Length(), 0, false)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("string batch dedup", func(t *testing.T) {
		idx := NewSimpleTableIndex()
		idx.tree["dup"] = 1
		vec := makeWorkspaceVector(types.T_varchar.ToType())
		defer vec.Close()
		vec.Append([]byte("dup"), false)

		err := idx.BatchDedup("pk", vec)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("array32 batch insert dedup input", func(t *testing.T) {
		idx := NewSimpleTableIndex()
		vec := makeWorkspaceVector(types.T_array_float32.ToType())
		defer vec.Close()
		val := types.ArrayToBytes([]float32{1, 2})
		vec.Append(val, false)
		vec.Append(val, false)

		err := idx.BatchInsert("pk", vec, 0, vec.Length(), 0, true)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("array32 batch insert tree conflict", func(t *testing.T) {
		idx := NewSimpleTableIndex()
		key := types.ArrayToString([]float32{1, 2})
		idx.tree[key] = 1
		vec := makeWorkspaceVector(types.T_array_float32.ToType())
		defer vec.Close()
		vec.Append(types.ArrayToBytes([]float32{1, 2}), false)

		err := idx.BatchInsert("pk", vec, 0, vec.Length(), 0, false)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("array32 batch dedup", func(t *testing.T) {
		idx := NewSimpleTableIndex()
		key := types.ArrayToString([]float32{1, 2})
		idx.tree[key] = 1
		vec := makeWorkspaceVector(types.T_array_float32.ToType())
		defer vec.Close()
		vec.Append(types.ArrayToBytes([]float32{1, 2}), false)

		err := idx.BatchDedup("pk", vec)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("array64 batch insert dedup input", func(t *testing.T) {
		idx := NewSimpleTableIndex()
		vec := makeWorkspaceVector(types.T_array_float64.ToType())
		defer vec.Close()
		val := types.ArrayToBytes([]float64{1, 2})
		vec.Append(val, false)
		vec.Append(val, false)

		err := idx.BatchInsert("pk", vec, 0, vec.Length(), 0, true)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("array64 batch insert tree conflict", func(t *testing.T) {
		idx := NewSimpleTableIndex()
		key := types.ArrayToString([]float64{1, 2})
		idx.tree[key] = 1
		vec := makeWorkspaceVector(types.T_array_float64.ToType())
		defer vec.Close()
		vec.Append(types.ArrayToBytes([]float64{1, 2}), false)

		err := idx.BatchInsert("pk", vec, 0, vec.Length(), 0, false)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("array64 batch dedup", func(t *testing.T) {
		idx := NewSimpleTableIndex()
		key := types.ArrayToString([]float64{1, 2})
		idx.tree[key] = 1
		vec := makeWorkspaceVector(types.T_array_float64.ToType())
		defer vec.Close()
		vec.Append(types.ArrayToBytes([]float64{1, 2}), false)

		err := idx.BatchDedup("pk", vec)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})
}

func TestLoad(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(14, 13)
	schema.Extra.BlockMaxRows = 10000
	schema.Extra.ObjectMaxBlocks = 10

	bat := catalog.MockBatch(schema, 60000)
	defer bat.Close()
	bats := bat.Split(5)

	txn, _ := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db", "", "")
	rel, _ := db.CreateRelation(schema)
	tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
	tbl, _ := tDB.getOrSetTable(rel.ID())

	err := tbl.Append(context.Background(), bats[0])
	assert.NoError(t, err)

	v, _, err := tbl.dataTable.tableSpace.GetValue(100, 0)
	assert.NoError(t, err)
	t.Logf("Row %d, Col %d, Val %v", 100, 0, v)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestNodeCommand(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(14, 13)
	schema.Extra.BlockMaxRows = 10000
	schema.Extra.ObjectMaxBlocks = 10

	bat := catalog.MockBatch(schema, 15000)
	defer bat.Close()

	txn, _ := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db", "", "")
	rel, _ := db.CreateRelation(schema)

	tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
	tbl, _ := tDB.getOrSetTable(rel.ID())
	err := tbl.Append(context.Background(), bat)
	assert.Nil(t, err)

	err = tbl.RangeDeleteLocalRows(100, 200)
	assert.NoError(t, err)

	inode := tbl.dataTable.tableSpace.node
	cmd, err := inode.MakeCommand(uint32(0))
	assert.NoError(t, err)
	assert.NotNil(t, cmd.(*AppendCmd).Data)
	//if entry != nil {
	//	_ = entry.WaitDone()
	//	entry.Free()
	//}
	if cmd != nil {
		t.Log(cmd.String())
	}
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestTxnManager1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	mgr := txnbase.NewTxnManager(TxnStoreFactory(context.Background(), nil, nil, nil),
		TxnFactory(nil), types.NewMockHLCClock(1))
	mgr.Start(context.Background())
	txn, _ := mgr.StartTxn(nil)
	txn.MockIncWriteCnt()

	lock := sync.Mutex{}
	seqs := make([]int, 0)

	txn.SetPrepareCommitFn(func(_ txnif.AsyncTxn) error {
		time.Sleep(time.Millisecond * 100)
		lock.Lock()
		seqs = append(seqs, 2)
		lock.Unlock()
		return nil
	})

	var wg sync.WaitGroup
	short := func() {
		defer wg.Done()
		txn2, _ := mgr.StartTxn(nil)
		txn2.MockIncWriteCnt()
		txn2.SetPrepareCommitFn(func(_ txnif.AsyncTxn) error {
			lock.Lock()
			seqs = append(seqs, 4)
			lock.Unlock()
			return nil
		})
		time.Sleep(10 * time.Millisecond)
		lock.Lock()
		seqs = append(seqs, 1)
		lock.Unlock()
		txn.GetTxnState(true)
		lock.Lock()
		seqs = append(seqs, 3)
		lock.Unlock()
		err := txn2.Commit(context.Background())
		assert.Nil(t, err)
	}

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go short()
	}

	err := txn.Commit(context.Background())
	assert.Nil(t, err)
	wg.Wait()
	defer mgr.Stop()
	expected := []int{1, 2, 3, 4}
	assert.Equal(t, expected, seqs)
}

func initTestContext(ctx context.Context, t *testing.T, dir string) (*catalog.Catalog, *txnbase.TxnManager, wal.Store) {
	fs := objectio.TmpNewFileservice(ctx, path.Join(dir, "data"))
	rt := dbutils.NewRuntime(
		dbutils.WithRuntimeObjectFS(fs),
	)
	factory := tables.NewDataFactory(rt, dir)
	c := catalog.MockCatalog(factory)
	driver := wal.NewLocalHandle(dir, "store", nil)
	mgr := txnbase.NewTxnManager(TxnStoreFactory(context.Background(), c, driver, rt),
		TxnFactory(c), types.NewMockHLCClock(1))
	rt.Now = mgr.Now
	mgr.Start(context.Background())
	return c, mgr, driver
}

// 1. Txn1 create database "db" and table "tb1". Commit
// 2. Txn2 drop database
// 3. Txn3 create table "tb2"
// 4. Txn2 commit
// 5. Txn3 commit
func TestTransaction1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	txn1, _ := mgr.StartTxn(nil)
	name := "db"
	schema := catalog.MockSchema(1, 0)
	db, err := txn1.CreateDatabase(name, "", "")
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)
	err = txn1.Commit(context.Background())
	assert.Nil(t, err)
	t.Log(c.SimplePPString(common.PPL1))

	txn2, _ := mgr.StartTxn(nil)
	db2, err := txn2.DropDatabase(name)
	assert.Nil(t, err)
	t.Log(db2.String())

	txn3, _ := mgr.StartTxn(nil)
	db3, err := txn3.GetDatabase(name)
	assert.Nil(t, err)
	t.Log(db3.String())
	schema = catalog.MockSchema(1, 0)
	rel, err := db3.CreateRelation(schema)
	assert.Nil(t, err)
	t.Log(rel.String())

	err = txn2.Commit(context.Background())
	assert.Nil(t, err)
	err = txn3.Commit(context.Background())
	assert.NoError(t, err)
	// assert.Equal(t, txnif.TxnStateRollbacked, txn3.GetTxnState(true))
	t.Log(txn3.String())
	t.Log(db2.String())
	t.Log(rel.String())
	t.Log(c.SimplePPString(common.PPL1))
}

func TestTransaction2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	name := "db"
	txn1, _ := mgr.StartTxn(nil)
	db, err := txn1.CreateDatabase(name, "", "")
	assert.Nil(t, err)
	t.Log(db.String())

	schema := catalog.MockSchema(1, 0)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	t.Log(rel.String())

	err = txn1.Commit(context.Background())
	assert.Nil(t, err)
	t.Log(db.String())
	assert.Equal(t, txn1.GetCommitTS(), db.GetMeta().(*catalog.DBEntry).GetCreatedAtLocked())
	assert.True(t, db.GetMeta().(*catalog.DBEntry).IsCommittedLocked())
	assert.Equal(t, txn1.GetCommitTS(), rel.GetMeta().(*catalog.TableEntry).GetCreatedAtLocked())
	assert.True(t, rel.GetMeta().(*catalog.TableEntry).IsCommittedLocked())

	txn2, _ := mgr.StartTxn(nil)
	get, err := txn2.GetDatabase(name)
	assert.Nil(t, err)
	t.Log(get.String())

	dropped, err := txn2.DropDatabase(name)
	assert.Nil(t, err)
	t.Log(dropped.String())

	_, err = txn2.GetDatabase(name)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadDB))
	t.Log(err)

	txn3, _ := mgr.StartTxn(nil)

	db3, err := txn3.GetDatabase(name)
	assert.Nil(t, err)

	rel, err = db3.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	t.Log(rel.String())
}

func TestTransaction3(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	pool, _ := ants.NewPool(20)
	defer pool.Release()

	var wg sync.WaitGroup

	flow := func(i int) func() {
		return func() {
			defer wg.Done()
			txn, _ := mgr.StartTxn(nil)
			name := fmt.Sprintf("db-%d", i)
			db, err := txn.CreateDatabase(name, "", "")
			assert.Nil(t, err)
			schema := catalog.MockSchemaAll(13, 12)
			_, err = db.CreateRelation(schema)
			assert.Nil(t, err)
			err = txn.Commit(context.Background())
			assert.Nil(t, err)
		}
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		err := pool.Submit(flow(i))
		assert.Nil(t, err)
	}
	wg.Wait()
}

func TestObject1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1, _ := mgr.StartTxn(nil)
	name := "db"
	schema := catalog.MockSchema(1, 0)
	db, err := txn1.CreateDatabase(name, "", "")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	_, err = rel.CreateObject(false)
	assert.Nil(t, err)
	err = txn1.Commit(context.Background())
	assert.Nil(t, err)

	txn2, _ := mgr.StartTxn(nil)
	db, err = txn2.GetDatabase(name)
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	objIt := rel.MakeObjectIt(false)
	cnt := 0
	for objIt.Next() {
		iobj := objIt.GetObject()
		t.Log(iobj.String())
		cnt++
	}
	assert.Equal(t, 1, cnt)

	_, err = rel.CreateObject(false)
	assert.Nil(t, err)

	objIt = rel.MakeObjectIt(false)
	cnt = 0
	for objIt.Next() {
		iobj := objIt.GetObject()
		t.Log(iobj.String())
		cnt++
	}
	assert.Equal(t, 1, cnt)

	txn3, _ := mgr.StartTxn(nil)
	db, _ = txn3.GetDatabase(name)
	rel, _ = db.GetRelationByName(schema.Name)
	objIt = rel.MakeObjectIt(false)
	cnt = 0
	for objIt.Next() {
		iobj := objIt.GetObject()
		t.Log(iobj.String())
		cnt++
	}
	assert.Equal(t, 2, cnt)

	err = txn2.Commit(context.Background())
	assert.Nil(t, err)

	objIt = rel.MakeObjectIt(false)
	cnt = 0
	for objIt.Next() {
		iobj := objIt.GetObject()
		t.Log(iobj.String())
		cnt++
	}
	assert.Equal(t, 2, cnt)
}

func TestObject2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1, _ := mgr.StartTxn(nil)
	db, _ := txn1.CreateDatabase("db", "", "")
	schema := catalog.MockSchema(1, 0)
	rel, _ := db.CreateRelation(schema)
	objCnt := 10
	for i := 0; i < objCnt; i++ {
		_, err := rel.CreateObject(false)
		assert.Nil(t, err)
	}

	err := txn1.Commit(context.Background())
	assert.Nil(t, err)

	txn2, _ := mgr.StartTxn(nil)
	db, _ = txn2.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
	it := rel.MakeObjectIt(false)
	cnt := 0
	for it.Next() {
		cnt++
	}
	assert.Equal(t, objCnt, cnt)
	assert.Nil(t, txn2.Commit(context.Background()))
	t.Log(c.SimplePPString(common.PPL1))
}

func TestIsEmptyDroppedAppendableObject(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	ctx := context.Background()
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchema(1, 0)
	txn, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	db, err := txn.CreateDatabase("db", "", "")
	require.NoError(t, err)
	_, err = db.CreateRelation(schema)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctx))

	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()
	txn, err = mgr.StartTxn(nil)
	require.NoError(t, err)
	db, err = txn.GetDatabase("db")
	require.NoError(t, err)
	rel, err := db.GetRelationByName(schema.Name)
	require.NoError(t, err)
	require.NoError(t, rel.Append(ctx, bat))
	require.NoError(t, txn.Commit(ctx))

	txn, err = mgr.StartTxn(nil)
	require.NoError(t, err)
	db, err = txn.GetDatabase("db")
	require.NoError(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	require.NoError(t, err)
	it := rel.MakeObjectIt(false)
	require.True(t, it.Next())
	dataObjectID := *it.GetObject().GetID()
	it.Close()
	emptyObject, err := rel.CreateObject(false)
	require.NoError(t, err)
	emptyObjectID := *emptyObject.GetID()
	require.NoError(t, emptyObject.Close())
	require.NoError(t, txn.Commit(ctx))

	txn, err = mgr.StartTxn(nil)
	require.NoError(t, err)
	db, err = txn.GetDatabase("db")
	require.NoError(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	require.NoError(t, err)
	require.NoError(t, rel.SoftDeleteObject(&dataObjectID, false))
	require.NoError(t, rel.SoftDeleteObject(&emptyObjectID, false))
	require.NoError(t, txn.Commit(ctx))

	table := rel.GetMeta().(*catalog.TableEntry)
	dataObject, err := table.GetObjectByID(&dataObjectID, false)
	require.NoError(t, err)
	require.True(t, dataObject.HasDropCommitted())
	require.Zero(t, dataObject.GetObjectStats().Rows())
	rows, err := dataObject.GetObjectData().Rows()
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	require.False(t, isEmptyDroppedAppendableObject(dataObject))

	emptyObjectMeta, err := table.GetObjectByID(&emptyObjectID, false)
	require.NoError(t, err)
	require.True(t, emptyObjectMeta.HasDropCommitted())
	require.True(t, isEmptyDroppedAppendableObject(emptyObjectMeta))
}

func TestDedup1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(4, 2)
	schema.Extra.BlockMaxRows = 20
	schema.Extra.ObjectMaxBlocks = 4
	cnt := uint64(10)
	rows := uint64(schema.Extra.BlockMaxRows) / 2 * cnt
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(int(cnt))
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase("db", "", "")
		_, err := db.CreateRelation(schema)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[0])
		assert.NoError(t, err)
		err = rel.Append(context.Background(), bats[0])
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
		assert.Nil(t, txn.Rollback(context.Background()))
	}

	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[0])
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
		assert.Nil(t, txn.Rollback(context.Background()))
	}
	{
		txn, _ := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		err := rel.Append(context.Background(), bats[1])
		assert.Nil(t, err)

		txn2, _ := mgr.StartTxn(nil)
		db2, _ := txn2.GetDatabase("db")
		rel2, _ := db2.GetRelationByName(schema.Name)
		err = rel2.Append(context.Background(), bats[2])
		assert.Nil(t, err)
		err = rel2.Append(context.Background(), bats[3])
		assert.Nil(t, err)
		assert.Nil(t, txn2.Commit(context.Background()))

		txn3, _ := mgr.StartTxn(nil)
		db3, _ := txn3.GetDatabase("db")
		rel3, _ := db3.GetRelationByName(schema.Name)
		err = rel3.Append(context.Background(), bats[4])
		assert.Nil(t, err)
		err = rel3.Append(context.Background(), bats[5])
		assert.Nil(t, err)
		assert.Nil(t, txn3.Commit(context.Background()))

		err = rel.Append(context.Background(), bats[3])
		assert.NoError(t, err)
		err = txn.Commit(context.Background())
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
	}
	t.Log(c.SimplePPString(common.PPL1))
}

func TestLogTxnState(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	// Test with sync=false
	{
		txn, _ := mgr.StartTxn(nil)
		store := txn.GetStore().(*txnStore)
		logEntry, err := store.LogTxnState(false)
		assert.NoError(t, err)
		assert.NotNil(t, logEntry)
		assert.Equal(t, IOET_WALEntry_TxnRecord, logEntry.GetType())
		info := logEntry.GetInfo()
		assert.NotNil(t, info)
		entryInfo, ok := info.(*entry.Info)
		assert.True(t, ok)
		assert.Equal(t, wal.GroupC, entryInfo.Group)
	}

	// Test with sync=true
	{
		txn, _ := mgr.StartTxn(nil)
		store := txn.GetStore().(*txnStore)
		logEntry, err := store.LogTxnState(true)
		assert.NoError(t, err)
		assert.NotNil(t, logEntry)
		assert.Equal(t, IOET_WALEntry_TxnRecord, logEntry.GetType())
		info := logEntry.GetInfo()
		assert.NotNil(t, info)
		entryInfo, ok := info.(*entry.Info)
		assert.True(t, ok)
		assert.Equal(t, wal.GroupC, entryInfo.Group)
		// When sync=true, WaitDone() should be called, so the entry should be done
		// We can verify this by checking if the entry is ready
	}

	// Test with a transaction that has some operations
	{
		txn, _ := mgr.StartTxn(nil)
		_, err := txn.CreateDatabase("testdb", "", "")
		assert.NoError(t, err)
		store := txn.GetStore().(*txnStore)
		logEntry, err := store.LogTxnState(false)
		assert.NoError(t, err)
		assert.NotNil(t, logEntry)
		assert.Equal(t, IOET_WALEntry_TxnRecord, logEntry.GetType())
		// Verify the payload is set
		payload := logEntry.GetPayload()
		assert.NotNil(t, payload)
		assert.Greater(t, len(payload), 0)
	}
}

func TestCreateAppendableObjectWithOptions(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	ctx := context.Background()
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchema(1, 0)
	txn, err := mgr.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.CreateDatabase("db", "", "")
	assert.NoError(t, err)
	_, err = db.CreateRelation(schema)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctx))

	txn, err = mgr.StartTxn(nil)
	assert.NoError(t, err)
	db, err = txn.GetDatabase("db")
	assert.NoError(t, err)
	rel, err := db.GetRelationByName(schema.Name)
	assert.NoError(t, err)

	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
	obj, err := rel.CreateObjectWithOpt(false, &objectio.CreateObjOpt{Stats: stats})
	assert.NoError(t, err)
	assert.True(t, obj.GetMeta().(*catalog.ObjectEntry).IsAppendable())
	assert.Equal(t, id, *obj.GetID())

	store := txn.GetStore().(*txnStore)
	txnDB, err := store.getOrSetDB(db.GetID())
	assert.NoError(t, err)
	txnTable, err := txnDB.getOrSetTable(rel.ID())
	assert.NoError(t, err)
	assert.Zero(t, txnTable.txnEntries.Len())
	assert.NoError(t, txn.Commit(ctx))

	txn, err = mgr.StartTxn(nil)
	assert.NoError(t, err)
	db, err = txn.GetDatabase("db")
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	_, err = rel.GetObject(&id, false)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctx))
}

func TestCreateAppendableObjectWithOptionsRejectsNonAppendable(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	ctx := context.Background()
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	txn, err := mgr.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.CreateDatabase("db", "", "")
	assert.NoError(t, err)
	rel, err := db.CreateRelation(catalog.MockSchema(1, 0))
	assert.NoError(t, err)

	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	_, err = rel.CreateObjectWithOpt(false, &objectio.CreateObjOpt{Stats: stats})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only supports appendable object")
	assert.NoError(t, txn.Rollback(ctx))
}

func TestCreateAppendableObjectWithOptionsErrors(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	ctx := context.Background()
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	txn, err := mgr.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.CreateDatabase("db", "", "")
	assert.NoError(t, err)
	rel, err := db.CreateRelation(catalog.MockSchema(1, 0))
	assert.NoError(t, err)

	store := txn.GetStore().(*txnStore)
	newOpt := func() *objectio.CreateObjOpt {
		id := objectio.NewObjectid()
		return &objectio.CreateObjOpt{
			Stats: objectio.NewObjectStatsWithObjectID(&id, true, false, false),
		}
	}

	_, err = store.CreateObjectWithOpt(db.GetID()+1, rel.ID(), false, newOpt())
	assert.Error(t, err)

	txnDB, err := store.getOrSetDB(db.GetID())
	assert.NoError(t, err)
	_, err = txnDB.CreateObjectWithOpt(rel.ID()+1, newOpt(), false)
	assert.Error(t, err)

	store.isOffline = true
	_, err = store.CreateObjectWithOpt(db.GetID(), rel.ID(), false, newOpt())
	assert.Error(t, err)
	_, err = txnDB.CreateObjectWithOpt(rel.ID(), newOpt(), false)
	assert.Error(t, err)
	store.isOffline = false
	assert.NoError(t, txn.Rollback(ctx))
}

type replayAObjectCreateRecorder struct {
	created []replayAObjectCreateRecord
}

type replayAObjectCreateRecord struct {
	id          common.ID
	isTombstone bool
	ts          types.TS
}

func (*replayAObjectCreateRecorder) OnTimeStamp(types.TS) {}

func (r *replayAObjectCreateRecorder) RecordReplayAObjectCreate(
	id *common.ID,
	isTombstone bool,
	ts types.TS,
) {
	r.created = append(r.created, replayAObjectCreateRecord{*id, isTombstone, ts})
}

func TestEnsureReplayAObject(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	ctx := context.Background()
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	txn, err := mgr.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.CreateDatabase("db", "", "")
	assert.NoError(t, err)
	rel, err := db.CreateRelation(catalog.MockSchema(1, 0))
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctx))

	database, err := c.GetDatabaseByID(db.GetID())
	assert.NoError(t, err)
	table, err := database.GetTableEntryByID(rel.ID())
	assert.NoError(t, err)
	id := table.AsCommonID()
	objectID := objectio.NewObjectid()
	id.SetObjectID(&objectID)

	createTS := mgr.Now()
	recorder := new(replayAObjectCreateRecorder)
	store := &replayTxnStore{catalog: c}
	obj, created, err := store.ensureReplayAObject(id, false, createTS, recorder)
	assert.NoError(t, err)
	assert.True(t, created)
	assert.True(t, obj.IsAppendable())
	assert.Equal(t, createTS, obj.GetCreatedAt())
	assert.Len(t, recorder.created, 1)
	assert.Equal(t, *id, recorder.created[0].id)
	assert.Equal(t, createTS, recorder.created[0].ts)

	obj, created, err = store.ensureReplayAObject(id, false, mgr.Now(), recorder)
	assert.NoError(t, err)
	assert.False(t, created)
	assert.True(t, obj.IsAppendable())
	assert.Len(t, recorder.created, 1)

	tombstoneID := table.AsCommonID()
	objectID = objectio.NewObjectid()
	tombstoneID.SetObjectID(&objectID)
	obj, created, err = store.ensureReplayAObject(tombstoneID, true, mgr.Now(), nil)
	assert.NoError(t, err)
	assert.True(t, created)
	assert.True(t, obj.IsTombstone)

	missingDB := *id
	missingDB.DbID++
	_, _, err = store.ensureReplayAObject(&missingDB, false, mgr.Now(), recorder)
	assert.Error(t, err)

	missingTable := *id
	missingTable.TableID++
	_, _, err = store.ensureReplayAObject(&missingTable, false, mgr.Now(), recorder)
	assert.Error(t, err)
}

func TestReplayAppendNodeCreateTS(t *testing.T) {
	prepareTS := types.BuildTS(10, 0)
	commitTS := types.BuildTS(20, 0)
	node := updates.NewEmptyAppendNode()
	node.TxnMVCCNode.Prepare = prepareTS
	node.TxnMVCCNode.End = commitTS
	assert.Equal(t, commitTS, replayAppendNodeCreateTS(node))

	txn := txnbase.NewTxn(nil, &txnbase.NoopTxnStore{}, []byte("txn"), prepareTS, prepareTS)
	assert.NoError(t, txn.SetCommitTS(types.BuildTS(30, 0)))
	node.TxnMVCCNode.Txn = txn
	assert.Equal(t, types.BuildTS(30, 0), replayAppendNodeCreateTS(node))

	node.TxnMVCCNode.Txn = nil
	node.TxnMVCCNode.End = types.TS{}
	assert.Equal(t, prepareTS, replayAppendNodeCreateTS(node))
}
