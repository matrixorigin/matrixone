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

package tables

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func TestObjectLifetimeOverlapsRange(t *testing.T) {
	start := types.BuildTS(20, 0)
	end := types.BuildTS(30, 0)

	for _, test := range []struct {
		name     string
		createAt types.TS
		deleteAt types.TS
		overlaps bool
	}{
		{
			name:     "live object",
			createAt: types.BuildTS(10, 0),
			overlaps: true,
		},
		{
			name:     "deleted before range",
			createAt: types.BuildTS(10, 0),
			deleteAt: types.BuildTS(19, 0),
			overlaps: false,
		},
		{
			name:     "deleted at range start",
			createAt: types.BuildTS(10, 0),
			deleteAt: start,
			overlaps: true,
		},
		{
			name:     "created after range",
			createAt: types.BuildTS(31, 0),
			overlaps: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.overlaps, objectLifetimeOverlapsRange(
				test.createAt, test.deleteAt, start, end,
			))
		})
	}
}

func TestValidatePersistedObjectBlockCount(t *testing.T) {
	require.Error(t, validatePersistedObjectBlockCount(0))
	require.NoError(t, validatePersistedObjectBlockCount(1))
	require.NoError(t, validatePersistedObjectBlockCount(maxPersistedObjectBlockCount))
	require.Error(t, validatePersistedObjectBlockCount(maxPersistedObjectBlockCount+1))
}

func TestCheckedDeleteOffsetRejectsOverflow(t *testing.T) {
	offset, err := checkedDeleteOffset(7, 100, 108)
	require.NoError(t, err)
	require.Equal(t, uint64(107), offset)

	_, err = checkedDeleteOffset(1, math.MaxUint64, math.MaxUint64)
	require.ErrorContains(t, err, "overflows")
	_, err = checkedDeleteOffset(7, 100, 107)
	require.ErrorContains(t, err, "outside output row range")
}

func TestGetPersistedBlockBloomFilterRejectsMalformedMetadata(t *testing.T) {
	require.NotPanics(t, func() {
		_, err := getPersistedBlockBloomFilter(objectio.BloomFilter{1}, 1, 0)
		require.ErrorContains(t, err, "malformed")
	})
}

func TestPersistedTombstoneFillRejectsCanceledContextBeforeIO(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	node := &persistedNode{object: &baseObject{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	blockID := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	deletes := &nulls.Nulls{}
	deletes.Add(7)

	err := node.FillBlockTombstones(
		ctx,
		txnbase.MockTxnReaderWithStartTS(types.BuildTS(10, 0)),
		blockID,
		&deletes,
		0,
		1,
		mp,
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, deletes.Count())
	require.True(t, deletes.Contains(7))
}

func TestPersistedScanRejectsMalformedProjectionBeforeIO(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	node := &persistedNode{object: &baseObject{}}
	schema := catalog.MockSchema(1, 0)
	var output *containers.Batch

	err := node.Scan(
		context.Background(), &output, nil, schema, 0,
		[]int{len(schema.ColDefs)}, mp,
	)
	require.ErrorContains(t, err, "outside the read schema")
	require.Nil(t, output)

	err = node.Scan(
		context.Background(), &output, nil, schema, 0,
		[]int{objectio.SEQNUM_COMMITTS, objectio.SEQNUM_COMMITTS}, mp,
	)
	require.ErrorContains(t, err, "duplicate column")
	require.Nil(t, output)
}

func TestPersistedNodeReadsLegacyBackupTombstone(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	smallPool := containers.NewVectorPool("legacy-tombstone-small", 4, containers.WithMPool(mp))
	transientPool := containers.NewVectorPool("legacy-tombstone-transient", 4, containers.WithMPool(mp))
	defer smallPool.Destory()
	defer transientPool.Destory()
	rt := dbutils.NewRuntime(
		dbutils.WithRuntimeObjectFS(fs),
		dbutils.WithRuntimeSmallPool(smallPool),
		dbutils.WithRuntimeTransientPool(transientPool),
	)

	targetObject := objectio.NewObjectid()
	input := batch.NewWithSize(3)
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	input.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	for row, commitTS := range []types.TS{types.BuildTS(10, 0), types.BuildTS(20, 0)} {
		require.NoError(t, vector.AppendFixed(
			input.Vecs[0],
			types.NewRowIDWithObjectIDBlkNumAndRowID(targetObject, 0, uint32(row+1)),
			false,
			mp,
		))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(row+1), false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], commitTS, false, mp))
	}
	input.SetRowCount(2)
	defer input.Clean(mp)

	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	writer, err := ioutil.NewBlockWriter(fs, name.String())
	require.NoError(t, err)
	writer.SetTombstone()
	writer.SetPrimaryKeyWithType(
		uint16(objectio.TombstonePrimaryKeyIdx),
		index.HBF,
		index.ObjectPrefixFn,
		index.BlockPrefixFn,
	)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	blocks, extent, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	require.Equal(t, uint16(3), blocks[0].GetMetaColumnCount())
	require.Equal(t, uint16(2), blocks[0].GetMaxSeqnum())

	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, name))
	require.NoError(t, objectio.SetObjectStatsExtent(stats, extent))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, uint32(input.RowCount())))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, uint32(len(blocks))))
	require.NoError(t, objectio.SetObjectStatsSize(stats, extent.End()))

	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(1, 0), nil, nil)
	require.NoError(t, err)
	entry, err := table.CreateCommittedObject(
		// A rewritten/backup tombstone object can be created long after the
		// per-row deletes it carries. FillBlockTombstones must use those row
		// timestamps instead of pruning by this object timestamp.
		types.BuildTS(100, 0),
		&objectio.CreateObjOpt{Stats: stats, IsTombstone: true},
		NewDataFactory(rt, "").MakeObjectFactory(),
	)
	require.NoError(t, err)

	t.Run("scan preserves per-row commit timestamps", func(t *testing.T) {
		var result *containers.Batch
		err := entry.GetObjectData().Scan(
			ctx,
			&result,
			nil,
			table.GetLastestSchema(true),
			0,
			objectio.TombstoneColumns_TN_Created,
			mp,
		)
		require.NoError(t, err)
		if result != nil {
			defer result.Close()
		}
		require.NotNil(t, result)
		require.Equal(t, 2, result.Length())
		require.Equal(t, types.BuildTS(10, 0), result.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Get(0))
		require.Equal(t, types.BuildTS(20, 0), result.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Get(1))
	})

	t.Run("zero-copy scan transfers the compatibility reload lease", func(t *testing.T) {
		var result *containers.Batch
		err := entry.GetObjectData().Scan(
			WithScanNoCopy(ctx),
			&result,
			nil,
			table.GetLastestSchema(true),
			0,
			objectio.TombstoneColumns_TN_Created,
			mp,
		)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotNil(t, result.DataRelease)
		require.Equal(t, types.BuildTS(10, 0), result.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Get(0))
		require.Equal(t, types.BuildTS(20, 0), result.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Get(1))
		result.Close()
		require.Nil(t, result.DataRelease)
	})

	t.Run("range collection uses physical commit timestamp", func(t *testing.T) {
		var result *containers.Batch
		err := entry.GetObjectData().CollectObjectTombstoneInRange(
			ctx,
			types.BuildTS(5, 0),
			types.BuildTS(15, 0),
			&targetObject,
			&result,
			mp,
			transientPool,
		)
		require.NoError(t, err)
		if result != nil {
			defer result.Close()
		}
		require.NotNil(t, result)
		require.Equal(t, 1, result.Length())
		require.Equal(t, int32(1), result.GetVectorByName(objectio.TombstoneAttr_PK_Attr).Get(0))
		require.Equal(t, types.BuildTS(10, 0), result.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Get(0))
	})

	t.Run("range collection does not prune by TN object creation", func(t *testing.T) {
		var result *containers.Batch
		err := entry.GetObjectData().CollectObjectTombstoneInRange(
			ctx,
			types.BuildTS(15, 0),
			types.BuildTS(25, 0),
			&targetObject,
			&result,
			mp,
			transientPool,
		)
		require.NoError(t, err)
		if result != nil {
			defer result.Close()
		}
		require.NotNil(t, result)
		require.Equal(t, 1, result.Length())
		require.Equal(t, int32(2), result.GetVectorByName(objectio.TombstoneAttr_PK_Attr).Get(0))
		require.Equal(t, types.BuildTS(20, 0), result.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Get(0))
	})

	t.Run("block filtering uses per-row commit timestamp", func(t *testing.T) {
		blockID := types.NewBlockidWithObjectID(&targetObject, 0)
		for _, test := range []struct {
			name        string
			startTS     types.TS
			wantOffsets []uint64
		}{
			{name: "before object creation", startTS: types.BuildTS(5, 0)},
			{name: "between row commits", startTS: types.BuildTS(15, 0), wantOffsets: []uint64{101}},
			{name: "after row commits", startTS: types.BuildTS(25, 0), wantOffsets: []uint64{101, 102}},
		} {
			t.Run(test.name, func(t *testing.T) {
				var deletes *nulls.Nulls
				err := entry.GetObjectData().FillBlockTombstones(
					ctx,
					txnbase.MockTxnReaderWithStartTS(test.startTS),
					&blockID,
					&deletes,
					100,
					103,
					mp,
				)
				require.NoError(t, err)
				require.Equal(t, len(test.wantOffsets), deletes.Count())
				for _, offset := range test.wantOffsets {
					require.True(t, deletes.Contains(offset))
				}
			})
		}
	})

	t.Run("block filtering rejects out-of-range rowid atomically", func(t *testing.T) {
		blockID := types.NewBlockidWithObjectID(&targetObject, 0)
		deletes := &nulls.Nulls{}
		deletes.Add(7)
		err := entry.GetObjectData().FillBlockTombstones(
			ctx,
			txnbase.MockTxnReaderWithStartTS(types.BuildTS(25, 0)),
			&blockID,
			&deletes,
			100,
			102,
			mp,
		)
		require.ErrorContains(t, err, "outside output row range")
		require.Equal(t, 1, deletes.Count())
		require.True(t, deletes.Contains(7))
	})
}

func TestPersistedNodeFiltersCanonicalTombstoneByCommitTS(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	smallPool := containers.NewVectorPool("canonical-tombstone-small", 4, containers.WithMPool(mp))
	transientPool := containers.NewVectorPool("canonical-tombstone-transient", 4, containers.WithMPool(mp))
	defer smallPool.Destory()
	defer transientPool.Destory()
	rt := dbutils.NewRuntime(
		dbutils.WithRuntimeObjectFS(fs),
		dbutils.WithRuntimeSmallPool(smallPool),
		dbutils.WithRuntimeTransientPool(transientPool),
	)

	targetObject := objectio.NewObjectid()
	const rowCount = 2
	seqnums := []uint16{0, 1, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT}
	input := batch.NewWithSize(len(seqnums))
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	var err error
	input.Vecs[2], err = vector.NewConstFixed(
		types.T_TS.ToType(), types.BuildTS(10, 0), rowCount, mp,
	)
	require.NoError(t, err)
	input.Vecs[3], err = vector.NewConstFixed(
		types.T_bool.ToType(), false, rowCount, mp,
	)
	require.NoError(t, err)
	for row := 0; row < rowCount; row++ {
		require.NoError(t, vector.AppendFixed(
			input.Vecs[0],
			types.NewRowIDWithObjectIDBlkNumAndRowID(targetObject, 0, uint32(row+1)),
			false,
			mp,
		))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(row+1), false, mp))
	}
	input.SetRowCount(rowCount)
	defer input.Clean(mp)

	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	writer, err := ioutil.NewBlockWriterNew(
		fs,
		name,
		0,
		seqnums,
		true,
	)
	require.NoError(t, err)
	writer.SetPrimaryKeyWithType(
		uint16(objectio.TombstonePrimaryKeyIdx),
		index.HBF,
		index.ObjectPrefixFn,
		index.BlockPrefixFn,
	)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	require.Equal(t, uint16(4), blocks[0].GetMetaColumnCount())

	stats := writer.GetObjectStats()
	require.False(t, stats.GetCNCreated())
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(1, 0), nil, nil)
	require.NoError(t, err)
	entry, err := table.CreateCommittedObject(
		types.BuildTS(10, 0),
		&objectio.CreateObjOpt{Stats: &stats, IsTombstone: true},
		NewDataFactory(rt, "").MakeObjectFactory(),
	)
	require.NoError(t, err)

	t.Run("range collection broadcasts constant MVCC columns", func(t *testing.T) {
		var result *containers.Batch
		err := entry.GetObjectData().CollectObjectTombstoneInRange(
			ctx,
			types.BuildTS(5, 0),
			types.BuildTS(15, 0),
			&targetObject,
			&result,
			mp,
			transientPool,
		)
		require.NoError(t, err)
		if result != nil {
			defer result.Close()
		}
		require.NotNil(t, result)
		require.Equal(t, rowCount, result.Length())
		for row := 0; row < rowCount; row++ {
			require.Equal(
				t, types.BuildTS(10, 0),
				result.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Get(row),
			)
		}
	})

	blockID := types.NewBlockidWithObjectID(&targetObject, 0)
	for _, test := range []struct {
		name        string
		startTS     types.TS
		wantOffsets []uint64
	}{
		{name: "before object creation", startTS: types.BuildTS(5, 0)},
		{name: "after constant commit", startTS: types.BuildTS(15, 0), wantOffsets: []uint64{101, 102}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var deletes *nulls.Nulls
			fillErr := entry.GetObjectData().FillBlockTombstones(
				ctx,
				txnbase.MockTxnReaderWithStartTS(test.startTS),
				&blockID,
				&deletes,
				100,
				103,
				mp,
			)
			require.NoError(t, fillErr)
			require.Equal(t, len(test.wantOffsets), deletes.Count())
			for _, offset := range test.wantOffsets {
				require.True(t, deletes.Contains(offset))
			}
		})
	}
}

func TestLoadPersistedColumnDataClosesPhysicalAddressOnMetadataError(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	smallPool := containers.NewVectorPool("metadata-error-small", 4, containers.WithMPool(mp))
	transientPool := containers.NewVectorPool("metadata-error-transient", 4, containers.WithMPool(mp))
	defer smallPool.Destory()
	defer transientPool.Destory()
	rt := dbutils.NewRuntime(
		dbutils.WithRuntimeObjectFS(fs),
		dbutils.WithRuntimeSmallPool(smallPool),
		dbutils.WithRuntimeTransientPool(transientPool),
	)

	schema := catalog.MockSchema(1, 0)
	physicalAddress := schema.GetColIdx(catalog.PhyAddrColumnName)
	usedBefore, _ := transientPool.Used(false)
	require.Zero(t, usedBefore)

	_, _, _, err := LoadPersistedColumnData(
		ctx,
		schema,
		rt,
		&common.ID{},
		[]int{physicalAddress, objectio.SEQNUM_COMMITTS},
		objectio.NewRandomLocation(0, 3),
		mp,
		nil,
		true,
		false,
	)
	require.Error(t, err)
	usedAfter, _ := transientPool.Used(false)
	require.Zero(t, usedAfter)
}

func TestPersistedNodeReadsCNCreatedTombstone(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	smallPool := containers.NewVectorPool("cn-tombstone-small", 4, containers.WithMPool(mp))
	transientPool := containers.NewVectorPool("cn-tombstone-transient", 4, containers.WithMPool(mp))
	defer smallPool.Destory()
	defer transientPool.Destory()
	rt := dbutils.NewRuntime(
		dbutils.WithRuntimeObjectFS(fs),
		dbutils.WithRuntimeSmallPool(smallPool),
		dbutils.WithRuntimeTransientPool(transientPool),
	)

	targetObject := objectio.NewObjectid()
	input := batch.NewWithSize(len(objectio.TombstoneSeqnums_CN_Created))
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for row := range 2 {
		require.NoError(t, vector.AppendFixed(
			input.Vecs[0],
			types.NewRowIDWithObjectIDBlkNumAndRowID(targetObject, 0, uint32(row+1)),
			false,
			mp,
		))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(row+1), false, mp))
	}
	input.SetRowCount(2)
	defer input.Clean(mp)

	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	writer, err := ioutil.NewBlockWriterNew(
		fs,
		name,
		0,
		objectio.TombstoneSeqnums_CN_Created,
		true,
	)
	require.NoError(t, err)
	writer.SetTombstone()
	writer.SetPrimaryKeyWithType(
		uint16(objectio.TombstonePrimaryKeyIdx),
		index.HBF,
		index.ObjectPrefixFn,
		index.BlockPrefixFn,
	)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	require.Equal(t, uint16(2), blocks[0].GetMetaColumnCount())

	stats := writer.GetObjectStats(objectio.WithCNCreated())
	require.True(t, stats.GetCNCreated())
	createdAt := types.BuildTS(10, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(1, 0), nil, nil)
	require.NoError(t, err)
	entry, err := table.CreateCommittedObject(
		createdAt,
		&objectio.CreateObjOpt{Stats: &stats, IsTombstone: true},
		NewDataFactory(rt, "").MakeObjectFactory(),
	)
	require.NoError(t, err)

	t.Run("scan derives commit timestamp from object creation", func(t *testing.T) {
		var result *containers.Batch
		err := entry.GetObjectData().Scan(
			ctx,
			&result,
			nil,
			table.GetLastestSchema(true),
			0,
			objectio.TombstoneColumns_TN_Created,
			mp,
		)
		require.NoError(t, err)
		if result != nil {
			defer result.Close()
		}
		require.NotNil(t, result)
		require.Equal(t, 2, result.Length())
		for row := range result.Length() {
			require.Equal(t, createdAt, result.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Get(row))
		}
	})

	t.Run("range collection uses object creation timestamp", func(t *testing.T) {
		var result *containers.Batch
		err := entry.GetObjectData().CollectObjectTombstoneInRange(
			ctx,
			types.BuildTS(5, 0),
			types.BuildTS(15, 0),
			&targetObject,
			&result,
			mp,
			transientPool,
		)
		require.NoError(t, err)
		if result != nil {
			defer result.Close()
		}
		require.NotNil(t, result)
		require.Equal(t, 2, result.Length())
		for row := range result.Length() {
			require.Equal(t, createdAt, result.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Get(row))
		}
	})

	t.Run("block filtering keeps object creation timestamp semantics", func(t *testing.T) {
		blockID := types.NewBlockidWithObjectID(&targetObject, 0)
		for _, test := range []struct {
			name        string
			startTS     types.TS
			wantOffsets []uint64
		}{
			{name: "before object creation", startTS: types.BuildTS(5, 0)},
			{name: "after object creation", startTS: types.BuildTS(15, 0), wantOffsets: []uint64{101, 102}},
		} {
			t.Run(test.name, func(t *testing.T) {
				var deletes *nulls.Nulls
				err := entry.GetObjectData().FillBlockTombstones(
					ctx,
					txnbase.MockTxnReaderWithStartTS(test.startTS),
					&blockID,
					&deletes,
					100,
					103,
					mp,
				)
				require.NoError(t, err)
				require.Equal(t, len(test.wantOffsets), deletes.Count())
				for _, offset := range test.wantOffsets {
					require.True(t, deletes.Contains(offset))
				}
			})
		}
	})
}

func TestAppendPersistedScanVectorsMaterializesConstAndBorrowedDestinations(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	dst := containers.NewBatch()
	constIDs, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 2, mp)
	require.NoError(t, err)
	dst.AddVector("id", containers.ToTNVector(constIDs, mp))

	borrowedOwner := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(borrowedOwner, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(borrowedOwner, []byte("b"), false, mp))
	borrowed, err := borrowedOwner.Window(0, 2)
	require.NoError(t, err)
	require.True(t, borrowed.NeedDup())
	dst.AddVector("value", containers.ToTNVector(borrowed, mp))

	src := containers.NewBatch()
	srcID := containers.MakeVector(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(srcID.GetDownstreamVector(), int64(8), false, mp))
	src.AddVector("id", srcID)
	srcValue := containers.MakeVector(types.T_varchar.ToType(), mp)
	require.NoError(t, vector.AppendBytes(srcValue.GetDownstreamVector(), []byte("c"), false, mp))
	src.AddVector("value", srcValue)

	offset, err := appendTNBatchVectorsAtomic(dst, src.Attrs, src.Vecs, mp)
	require.NoError(t, err)
	require.Equal(t, 2, offset)
	require.Equal(t, 3, dst.Length())
	require.False(t, dst.GetVectorByName("id").IsConst())
	require.False(t, dst.GetVectorByName("value").GetDownstreamVector().NeedDup())
	require.Equal(t, int64(7), dst.GetVectorByName("id").Get(0))
	require.Equal(t, int64(7), dst.GetVectorByName("id").Get(1))
	require.Equal(t, int64(8), dst.GetVectorByName("id").Get(2))
	require.Equal(t, []byte("c"), dst.GetVectorByName("value").Get(2))

	dst.Close()
	src.Close()
	borrowedOwner.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestAppendPersistedScanVectorsRollsBackAllocationFailure(t *testing.T) {
	dstMP, err := mpool.NewMPool("persisted-scan-rollback", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	srcMP := mpool.MustNewZero()

	dst := containers.NewBatch()
	dstID := containers.MakeVector(types.T_int64.ToType(), dstMP)
	require.NoError(t, vector.AppendFixed(dstID.GetDownstreamVector(), int64(1), false, dstMP))
	dst.AddVector("id", dstID)
	dstValue := containers.MakeVector(types.T_varchar.ToType(), dstMP)
	require.NoError(t, vector.AppendBytes(dstValue.GetDownstreamVector(), []byte("kept"), false, dstMP))
	dst.AddVector("value", dstValue)

	src := containers.NewBatch()
	srcID := containers.MakeVector(types.T_int64.ToType(), srcMP)
	require.NoError(t, vector.AppendFixed(srcID.GetDownstreamVector(), int64(2), false, srcMP))
	src.AddVector("id", srcID)
	srcValue := containers.MakeVector(types.T_varchar.ToType(), srcMP)
	require.NoError(t, vector.AppendBytes(srcValue.GetDownstreamVector(), make([]byte, 2<<20), false, srcMP))
	src.AddVector("value", srcValue)

	_, err = appendTNBatchVectorsAtomic(dst, src.Attrs, src.Vecs, dstMP)
	require.Error(t, err)
	require.Equal(t, 1, dst.Length())
	require.Equal(t, int64(1), dst.GetVectorByName("id").Get(0))
	require.Equal(t, []byte("kept"), dst.GetVectorByName("value").Get(0))

	dst.Close()
	src.Close()
	require.Zero(t, dstMP.CurrNB())
	require.Zero(t, srcMP.CurrNB())
	mpool.DeleteMPool(dstMP)
	mpool.DeleteMPool(srcMP)
}

func TestTombstoneResultAppenderRollsBackAllocationFailure(t *testing.T) {
	dstMP, err := mpool.NewMPool("tombstone-row-rollback", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	srcMP := mpool.MustNewZero()

	dst := catalog.NewTombstoneBatchByPKType(types.T_varchar.ToType(), dstMP)
	var blockID types.Blockid
	require.NoError(t, vector.AppendFixed(
		dst.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).GetDownstreamVector(),
		types.NewRowid(&blockID, 0), false, dstMP,
	))
	require.NoError(t, vector.AppendBytes(
		dst.GetVectorByName(objectio.TombstoneAttr_PK_Attr).GetDownstreamVector(),
		[]byte("kept"), false, dstMP,
	))
	require.NoError(t, vector.AppendFixed(
		dst.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).GetDownstreamVector(),
		types.BuildTS(1, 0), false, dstMP,
	))

	sourcePK := containers.MakeVector(types.T_varchar.ToType(), srcMP)
	require.NoError(t, vector.AppendBytes(
		sourcePK.GetDownstreamVector(), make([]byte, 2<<20), false, srcMP,
	))
	appender, err := newTombstoneResultAppender(dst, sourcePK.GetType(), dstMP)
	require.NoError(t, err)
	err = appender.Append(types.NewRowid(&blockID, 1), sourcePK, 0, types.BuildTS(2, 0))
	require.Error(t, err)
	require.Equal(t, 1, dst.Length())
	require.Equal(t, []byte("kept"), dst.GetVectorByName(objectio.TombstoneAttr_PK_Attr).Get(0))
	require.Equal(t, types.BuildTS(1, 0), dst.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Get(0))

	dst.Close()
	sourcePK.Close()
	require.Zero(t, dstMP.CurrNB())
	require.Zero(t, srcMP.CurrNB())
	mpool.DeleteMPool(dstMP)
	mpool.DeleteMPool(srcMP)
}
