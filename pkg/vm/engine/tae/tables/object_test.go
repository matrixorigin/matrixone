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

package tables

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	api "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type waitCountingTxn struct {
	txnif.AsyncTxn
	waits atomic.Int32
}

func (txn *waitCountingTxn) GetTxnState(wait bool) txnif.TxnState {
	if wait {
		txn.waits.Add(1)
	}
	return txn.AsyncTxn.GetTxnState(false)
}

func TestValidatePersistedCommitTSVectors(t *testing.T) {
	mp := mpool.MustNewZero()

	valid := containers.MakeVector(types.T_TS.ToType(), mp)
	valid.Append(types.BuildTS(1, 0), false)
	got, err := validatePersistedCommitTSVectors(
		"valid",
		[]containers.Vector{valid},
	)
	require.NoError(t, err)
	require.Same(t, valid, got)
	got.Close()
	require.Zero(t, mp.CurrNB())

	for _, test := range []struct {
		name    string
		vectors func() []containers.Vector
	}{
		{
			name: "empty",
			vectors: func() []containers.Vector {
				return nil
			},
		},
		{
			name: "nil vector",
			vectors: func() []containers.Vector {
				return []containers.Vector{nil}
			},
		},
		{
			name: "wrong type is released",
			vectors: func() []containers.Vector {
				vec := containers.MakeVector(types.T_int64.ToType(), mp)
				vec.Append(int64(1), false)
				return []containers.Vector{vec}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := validatePersistedCommitTSVectors(
				"bad",
				test.vectors(),
			)
			require.ErrorContains(t, err, "bad commits layout")
			require.Nil(t, got)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestUnwrittenAppendableNodeLifecycle(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		for _, tc := range []struct {
			name       string
			checkpoint bool
			dropped    bool
			size       uint32
			persisted  bool
		}{
			{name: "active"},
			{name: "dropped", dropped: true},
			{name: "checkpoint", checkpoint: true},
			{name: "legacy placeholder", checkpoint: true, size: 1000, persisted: true},
		} {
			t.Run(fmt.Sprintf("tombstone=%t/%s", tombstone, tc.name), func(t *testing.T) {
				c := catalog.MockCatalog(nil)
				t.Cleanup(func() { require.NoError(t, c.Close()) })
				db, err := c.CreateDBEntry("db", "", "", nil)
				require.NoError(t, err)
				table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
				require.NoError(t, err)
				oid := objectio.NewObjectid()
				stats := objectio.NewObjectStatsWithObjectID(&oid, true, false, false)
				require.NoError(t, objectio.SetObjectStatsSize(stats, tc.size))
				meta := catalog.MockObjectEntry(table, stats, tombstone, nil, types.BuildTS(1, 0))
				if tc.checkpoint {
					meta = catalog.MockCreatedObjectEntry2List(table, c, tombstone, types.BuildTS(1, 0))
					meta.ObjectStats = *stats
					require.True(t, meta.IsForcePNode())
				}
				if tc.dropped {
					meta.DeletedAt = types.BuildTS(2, 0)
					meta.DeleteNode = txnbase.NewTxnMVCCNodeWithTS(meta.DeletedAt)
					meta.ObjectState = catalog.ObjectState_Delete_ApplyCommit
				}
				obj := newAObject(meta, nil, tombstone)
				t.Cleanup(func() { obj.node.Load().Unref() })
				require.Equal(t, tc.persisted, obj.node.Load().IsPersisted())
				require.Equal(t, tc.dropped || tc.checkpoint, obj.IsAppendFrozen())
				before := obj.node.Load()
				require.NoError(t, obj.TryUpgrade())
				require.Same(t, before, obj.node.Load())
			})
		}
	}
}

func TestAppendableUpgradeAfterSnapshotPublication(t *testing.T) {
	c := catalog.MockCatalog(nil)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
	require.NoError(t, err)
	oid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&oid, true, false, false)
	meta := catalog.MockObjectEntry(table, stats, false, nil, types.BuildTS(1, 0))
	obj := newAObject(meta, nil, false)
	t.Cleanup(func() { obj.node.Load().Unref() })
	old := obj.PinNode()
	defer old.Unref()
	require.NoError(t, obj.TryUpgrade())
	require.Same(t, old, obj.node.Load())
	// A physical snapshot changes the representation; pinned old readers
	// retain their memory node until they release it.
	published := meta.Clone()
	require.NoError(t, objectio.SetObjectStatsExtent(&published.ObjectStats, objectio.NewExtent(0, 1, 2, 3)))
	require.NoError(t, objectio.SetObjectStatsRowCnt(&published.ObjectStats, 1))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(&published.ObjectStats, 1))
	obj.UpdateMeta(published)
	require.NoError(t, obj.TryUpgrade())
	require.True(t, obj.node.Load().IsPersisted())
	require.False(t, old.IsPersisted())
	require.Equal(t, int64(1), old.RefCount())
}

func TestCompactAbortRowsRemapsDeletes(t *testing.T) {
	bat := containers.MockBatch([]types.Type{types.T_int32.ToType()}, 6, -1, nil)
	defer bat.Close()
	// Rows 0, 1, 3, and 4 are aborted append holes. Row 2 is a committed row
	// deleted by a tombstone; row 5 remains visible.
	bat.Deletes = nulls.Build(6, 0, 1, 2, 3, 4)
	abortRows := nulls.Build(6, 0, 1, 3, 4)

	compactInvisibleAppendRows(bat, abortRows)

	require.Equal(t, 2, bat.Length())
	require.Equal(t, []uint64{0}, bat.Deletes.ToArray())
}

func TestGetActiveRow(t *testing.T) {
	ts1 := types.BuildTS(1, 0)
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	mvcc := updates.NewAppendMVCCHandle(obj)
	// blk := &dataBlock{
	// 	mvcc: mvcc,
	// }
	b := &baseObject{
		RWMutex:    mvcc.RWMutex,
		appendMVCC: mvcc,
	}
	b.meta.Store(obj)
	mnode := &memoryNode{
		object: b,
	}
	blk := &aobject{baseObject: b}

	mnode.Ref()
	n := NewNode(mnode)
	blk.node.Store(n)

	// appendnode1 [0,1)
	an1, _ := mvcc.AddAppendNodeLocked(nil, 0, 1)
	an1.Start = ts1
	an1.Prepare = ts1
	an1.End = ts1

	// appendnode1 [1,2)
	an2, _ := mvcc.AddAppendNodeLocked(nil, 1, 2)
	an2.Start = ts1
	an2.Prepare = ts1
	an2.End = ts1

	// index uint8(1)-0,1
	vec := containers.MakeVector(types.T_int8.ToType(), common.DefaultAllocator)
	vec.Append(int8(1), false)
	vec.Append(int8(1), false)
	idx := indexwrapper.NewMutIndex(types.T_int8.ToType())
	err := idx.BatchUpsert(vec.GetDownstreamVector(), 0)
	assert.NoError(t, err)
	blk.node.Load().MustMNode().pkIndex = idx

	// An append that has not reached its prepare-queue slot is not a valid PK
	// owner for point lookup or snapshot dedup.
	an2.Prepare = txnif.UncommitTS
	require.ErrorIs(t, mnode.checkConflictLocked(nil)(1), index.ErrNotFound)
}

func TestPrepareAppendSealsFullObjectAndPublishesCommitBound(t *testing.T) {
	schema := catalog.MockSchema(1, 0)
	schema.Extra.BlockMaxRows = 2
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	entry, err := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	require.NoError(t, err)
	obj := newAObject(entry, nil, false)
	appender := newAppender(obj)
	txn := &txnbase.Txn{
		TxnCtx: txnbase.NewTxnCtx(
			common.NewTxnIDAllocator().Alloc(),
			types.BuildTS(1, 0),
			types.TS{},
		),
	}

	appender.LockFreeze()
	appendNode, created, rows, err := appender.PrepareAppend(false, 2, txn)
	appender.UnlockFreeze()
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, uint32(2), rows)
	require.True(t, obj.IsAppendFrozen())
	_, finalized := obj.GetAppendMaxCommitTS()
	require.False(t, finalized)

	txn.CommitTS = types.BuildTS(5, 0)
	require.NoError(t, appendNode.ApplyCommit(txn.ID))
	maxCommit, finalized := obj.GetAppendMaxCommitTS()
	require.True(t, finalized)
	require.Equal(t, types.BuildTS(5, 0), maxCommit)
	require.False(t, obj.CoarseCheckAllRowsCommittedBefore(types.BuildTS(5, 0)))
	require.True(t, obj.CoarseCheckAllRowsCommittedBefore(types.BuildTS(6, 0)))
}

func TestWaitAppendCommittingBeforeSkipsSameTxn(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, err := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	require.NoError(t, err)

	mvcc := updates.NewAppendMVCCHandle(obj)
	base := &baseObject{RWMutex: mvcc.RWMutex, appendMVCC: mvcc}
	base.meta.Store(obj)
	mnode := newMemoryNode(base, false)
	node := NewNode(mnode)
	node.Ref()
	base.node.Store(node)
	defer node.Unref()

	startTS := types.BuildTS(1, 0)
	prepareTS := types.BuildTS(2, 0)
	txn := &txnbase.Txn{
		TxnCtx: txnbase.NewTxnCtx(
			common.NewTxnIDAllocator().Alloc(),
			startTS,
			types.TS{},
		),
	}
	txn.Lock()
	require.NoError(t, txn.ToPreparingLocked(prepareTS))
	txn.Unlock()
	appendNode, _ := mvcc.AddAppendNodeLocked(txn, 0, 1)
	require.NoError(t, appendNode.PrepareCommit())
	keys := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer keys.Close()
	keys.Append(int32(7), false)
	require.NoError(t, mnode.pkIndex.BatchUpsert(keys.GetDownstreamVector(), 0))
	keysZM := index.NewZM(types.T_int32, 0)
	require.NoError(t, index.BatchUpdateZM(keysZM, keys.GetDownstreamVector()))

	// The transaction is still preparing. Without the same-txn guard this call
	// waits on its own DoneCond forever.
	base.WaitAppendCommittingBefore(types.MaxTs(), txn, keys, keysZM)
}

func TestWaitAppendCommittingBeforeOnlyWaitsSelectedKeys(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, err := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	require.NoError(t, err)

	mvcc := updates.NewAppendMVCCHandle(obj)
	base := &baseObject{RWMutex: mvcc.RWMutex, appendMVCC: mvcc}
	base.meta.Store(obj)
	mnode := newMemoryNode(base, false)
	node := NewNode(mnode)
	node.Ref()
	base.node.Store(node)
	defer node.Unref()

	newPreparingTxn := func(start, prepare types.TS) *waitCountingTxn {
		txn := &txnbase.Txn{
			TxnCtx: txnbase.NewTxnCtx(
				common.NewTxnIDAllocator().Alloc(), start, types.TS{},
			),
		}
		txn.Lock()
		require.NoError(t, txn.ToPreparingLocked(prepare))
		txn.Unlock()
		return &waitCountingTxn{AsyncTxn: txn}
	}
	txn1 := newPreparingTxn(types.BuildTS(1, 0), types.BuildTS(3, 0))
	txn2 := newPreparingTxn(types.BuildTS(2, 0), types.BuildTS(4, 0))
	an1, _ := mvcc.AddAppendNodeLocked(txn1, 0, 1)
	an2, _ := mvcc.AddAppendNodeLocked(txn2, 1, 2)
	require.NoError(t, an1.PrepareCommit())
	require.NoError(t, an2.PrepareCommit())

	indexed := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer indexed.Close()
	indexed.Append(int32(10), false)
	indexed.Append(int32(20), false)
	require.NoError(t, mnode.pkIndex.BatchUpsert(indexed.GetDownstreamVector(), 0))

	selected := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer selected.Close()
	selected.Append(int32(10), false)
	selected.Append(nil, true)
	selectedZM := index.NewZM(types.T_int32, 0)
	require.NoError(t, index.BatchUpdateZM(selectedZM, selected.GetDownstreamVector()))

	require.True(t, base.WaitAppendCommittingBefore(
		types.MaxTs(), nil, selected, selectedZM,
	))
	require.Equal(t, int32(1), txn1.waits.Load())
	require.Equal(t, int32(0), txn2.waits.Load())
}

func TestApplyAppendLockedPadsMissingColumnsForUpgradedSchema(t *testing.T) {

	oldSchema := catalog.MockSchema(2, 0)
	newSchema := oldSchema.Clone()
	require.NoError(t, newSchema.ApplyAlterTable(
		api.NewAddColumnReq(0, 0, "added_flag", types.NewProtoType(types.T_int8), 2),
	))

	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(oldSchema, nil, nil)
	require.NoError(t, err)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, err := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	require.NoError(t, err)

	mvcc := updates.NewAppendMVCCHandle(obj)
	b := &baseObject{
		RWMutex:    mvcc.RWMutex,
		appendMVCC: mvcc,
	}
	b.meta.Store(obj)

	mnode := &memoryNode{
		object:      b,
		writeSchema: newSchema,
	}

	bat := containers.BuildBatch(
		oldSchema.AllNames(),
		oldSchema.AllTypes(),
		containers.Options{Allocator: common.DefaultAllocator},
	)
	defer bat.Close()
	for _, vec := range bat.Vecs {
		vec.Append(nil, true)
	}

	from, err := mnode.ApplyAppendLocked(bat)
	require.NoError(t, err)
	require.Equal(t, 0, from)

	for _, vec := range mnode.data.Vecs {
		require.Equal(t, bat.Length(), vec.Length())
	}
	addedVec := mnode.data.GetVectorByName("added_flag")
	require.Equal(t, bat.Length(), addedVec.Length())
	require.True(t, addedVec.IsNull(0))

	pool := containers.NewVectorPool("upgrade-compat", 4, containers.WithMPool(common.DefaultAllocator))
	require.NotPanics(t, func() {
		win := mnode.data.CloneWindowWithPool(0, bat.Length(), pool)
		win.Close()
	})
}

func TestMemoryNodeRollbackHoleVisibilityAndWriteLayout(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	originalVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion11)
	defer func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, originalVersion)
		}
	}()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)

	mvcc := updates.NewAppendMVCCHandle(obj)
	base := &baseObject{
		RWMutex:    mvcc.RWMutex,
		appendMVCC: mvcc,
		rt:         dbutils.NewRuntime(),
	}
	base.meta.Store(obj)
	mnode := &memoryNode{object: base, writeSchema: schema}

	input := containers.BuildBatch(
		schema.AllNames(),
		schema.AllTypes(),
		containers.Options{Allocator: common.DefaultAllocator},
	)
	defer input.Close()
	for _, vec := range input.Vecs {
		for range 3 {
			vec.Append(nil, true)
		}
	}
	_, err := mnode.ApplyAppendLocked(input)
	require.NoError(t, err)
	defer mnode.data.Close()

	for row := uint32(0); row < 3; row++ {
		appendNode, _ := mvcc.AddAppendNodeLocked(nil, row, row+1)
		ts := types.BuildTS(int64(row+1), 0)
		appendNode.Start, appendNode.Prepare, appendNode.End = ts, ts, ts
		if row == 1 {
			appendNode.Aborted = true
		}
	}

	flushBatches := make(map[uint32]*containers.BatchWithVersion)
	err = mnode.getDataWindowOnWriteSchema(
		context.Background(),
		flushBatches,
		types.TS{},
		types.BuildTS(3, 0),
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	flushBatch := flushBatches[schema.Version]
	require.NotNil(t, flushBatch)
	defer flushBatch.Close()
	require.Contains(t, flushBatch.Seqnums, uint16(objectio.SEQNUM_ABORT))
	aborts := vector.MustFixedColWithTypeCheck[bool](
		flushBatch.GetVectorByName(objectio.TombstoneAttr_Abort_Attr).GetDownstreamVector(),
	)
	require.Equal(t, []bool{false, true, false}, aborts)

	// During a rolling upgrade, the TN keeps the legacy layout and marks abort
	// holes uncommitted so old readers hide them without shifting RowID offsets.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion10)
	legacyBatches := make(map[uint32]*containers.BatchWithVersion)
	err = mnode.getDataWindowOnWriteSchema(
		context.Background(),
		legacyBatches,
		types.TS{},
		types.BuildTS(3, 0),
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	legacyBatch := legacyBatches[schema.Version]
	require.NotNil(t, legacyBatch)
	defer legacyBatch.Close()
	require.NotContains(t, legacyBatch.Seqnums, uint16(objectio.SEQNUM_ABORT))
	require.NotContains(t, legacyBatch.Nameidx, objectio.TombstoneAttr_Abort_Attr)
	require.Contains(t, legacyBatch.Seqnums, uint16(objectio.SEQNUM_COMMITTS))
	require.Equal(t, 3, legacyBatch.Length())
	legacyCommitTS := vector.MustFixedColWithTypeCheck[types.TS](
		legacyBatch.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).GetDownstreamVector(),
	)
	require.Equal(t, types.BuildTS(1, 0), legacyCommitTS[0])
	require.Equal(t, txnif.UncommitTS, legacyCommitTS[1])
	require.Equal(t, types.BuildTS(3, 0), legacyCommitTS[2])

	var output *containers.Batch
	err = mnode.Scan(
		context.Background(),
		&output,
		updates.MockTxnWithStartTS(types.BuildTS(3, 0)),
		schema,
		0,
		[]int{0},
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	require.NotNil(t, output)
	defer output.Close()
	require.Equal(t, 3, output.Length())
	require.True(t, output.IsDeleted(1))
	require.False(t, output.IsDeleted(0))
	require.False(t, output.IsDeleted(2))
}

func TestAppendableDuplicatedRowsTimestampWindow(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		name := "data"
		if tombstone {
			name = "tombstone"
		}
		t.Run(name, func(t *testing.T) {
			schema := catalog.MockSchema(1, 0)
			table := catalog.MockStaloneTableEntry(1, schema)
			oid := objectio.NewObjectid()
			stats := objectio.NewObjectStatsWithObjectID(&oid, true, false, false)
			meta := catalog.MockObjectEntry(table, stats, tombstone, nil, types.BuildTS(1, 0))
			mvcc := updates.NewAppendMVCCHandle(meta)
			base := &baseObject{RWMutex: mvcc.RWMutex, appendMVCC: mvcc}
			base.meta.Store(meta)
			node := &memoryNode{object: base, pkIndex: indexwrapper.NewMutIndex(types.T_int8.ToType())}
			node.OnZeroCB = node.close
			node.Ref()
			defer node.Unref()
			obj := &aobject{baseObject: base}
			obj.node.Store(NewNode(node))
			for _, appendRange := range []struct {
				start, end uint32
				ts         int64
			}{{0, 1, 10}, {1, 3, 20}, {3, 4, 30}} {
				txn := updates.MockTxnWithStartTS(types.BuildTS(appendRange.ts-1, 0))
				txn.PrepareTS = types.BuildTS(appendRange.ts, 0)
				txn.CommitTS = txn.PrepareTS
				an, _ := mvcc.AddAppendNodeLocked(txn, appendRange.start, appendRange.end)
				require.NoError(t, an.PrepareCommit())
				require.NoError(t, an.ApplyCommit(txn.ID))
			}
			keys := containers.MakeVector(types.T_int8.ToType(), common.DefaultAllocator)
			defer keys.Close()
			for i := int8(0); i < 4; i++ {
				keys.Append(i, false)
			}
			require.NoError(t, node.pkIndex.BatchUpsert(keys.GetDownstreamVector(), 0))
			txn := txnbase.MockTxnReaderWithStartTS(types.BuildTS(100, 0))
			for _, tc := range []struct {
				name     string
				from, to types.TS
				want     []bool
			}{
				{"multirow equality", types.BuildTS(20, 0), types.BuildTS(20, 0), []bool{false, true, true, false}},
				{"between appends", types.BuildTS(11, 0), types.BuildTS(19, 0), []bool{false, false, false, false}},
				{"after lower equality", types.BuildTS(20, 1), types.BuildTS(30, 0), []bool{false, false, false, true}},
				{"empty lower", types.TS{}, types.BuildTS(10, 0), []bool{true, false, false, false}},
				{"all appends", types.TS{}, types.MaxTs(), []bool{true, true, true, true}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					rows := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
					defer rows.Close()
					for i := 0; i < 4; i++ {
						rows.Append(nil, true)
					}
					require.NoError(t, obj.GetDuplicatedRows(context.Background(), txn, keys, nil, tc.from, tc.to, rows, common.DefaultAllocator))
					for i, visible := range tc.want {
						require.Equal(t, !visible, rows.IsNull(i), "row %d", i)
						if visible {
							rowID := rows.Get(i).(types.Rowid)
							require.Equal(t, uint32(i), rowID.GetRowOffset())
						}
					}
				})
			}
		})
	}
}

func TestApplyAppendAtLockedOutOfOrder(t *testing.T) {
	schema := catalog.MockSchema(2, 0)
	mnode := &memoryNode{writeSchema: schema}

	makeBatch := func(value int32) *containers.Batch {
		bat := containers.BuildBatch(
			schema.AllNames(), schema.AllTypes(),
			containers.Options{Allocator: common.DefaultAllocator},
		)
		for i, vec := range bat.Vecs {
			if i == 0 {
				vec.Append(value, false)
			} else {
				vec.Append(nil, true)
			}
		}
		return bat
	}

	later := makeBatch(3)
	defer later.Close()
	require.NoError(t, mnode.ApplyAppendAtLocked(later, 2))
	defer mnode.data.Close()
	// An earlier owner may materialize after the later payload. Reserving a
	// shorter prefix must not clear that payload, including varlen/null data.
	mnode.ReserveRowsLocked(1)
	require.Equal(t, int32(3), mnode.data.Vecs[0].Get(2))
	mnode.ReserveRowsLocked(4)
	for _, vec := range mnode.data.Vecs {
		require.Equal(t, 4, vec.Length())
		require.True(t, vec.IsNull(3), "a failed trailing owner retains a physical hole")
	}
	first := makeBatch(1)
	defer first.Close()
	require.NoError(t, mnode.ApplyAppendAtLocked(first, 0))
	second := makeBatch(2)
	defer second.Close()
	require.NoError(t, mnode.ApplyAppendAtLocked(second, 1))

	vec := mnode.data.Vecs[0]
	require.Equal(t, 4, vec.Length())
	require.Equal(t, int32(1), vec.Get(0))
	require.Equal(t, int32(2), vec.Get(1))
	require.Equal(t, int32(3), vec.Get(2))
}

func TestFrozenAppenderRejectionDoesNotWaitForSealing(t *testing.T) {
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
	entry, err := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	require.NoError(t, err)
	obj := newAObject(entry, nil, false)
	defer obj.node.Load().Unref()
	h := newHandle(newTable(table, nil), obj, false)
	obj.FreezeAppend() // Legacy flag-only freezing leaves the bound unknown.
	obj.appendMVCC.LockForAppend()
	locked := true
	release := func() {
		if locked {
			locked = false
			obj.appendMVCC.UnlockForAppend()
		}
	}
	done := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _, err := h.GetAppender(); done <- err }()
	defer func() { release(); wg.Wait() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case err := <-done:
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrAppendableObjectNotFound), "%v", err)
	case <-ctx.Done():
		t.Fatal("candidate rejection waited for the object's writer")
	}
	_, finalized := obj.GetAppendMaxCommitTS()
	require.False(t, finalized, "unknown history remains conservative before lifecycle sealing")
	release()
	obj.SealAppend()
	_, finalized = obj.GetAppendMaxCommitTS()
	require.True(t, finalized)
}
