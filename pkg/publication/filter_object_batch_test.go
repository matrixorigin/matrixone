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

package publication

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertObjectToBatchSupportsChunkedColumnExtents(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil,
	)
	require.NoError(t, err)

	writer := ioutil.ConstructWriter(
		0,
		[]uint16{0, objectio.SEQNUM_ROWID, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT},
		-1, false, false, fs,
	)
	writer.SetChunkedColumnPolicy(func() bool { return true })
	writer.SetAppendable()
	source := batch.NewWithSize(4)
	source.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	source.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	source.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	source.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	payload := []byte(strings.Repeat("p", 3<<20))
	var blk types.Blockid
	for row := 0; row < 3; row++ {
		require.NoError(t, vector.AppendBytes(source.Vecs[0], payload, false, mp))
		require.NoError(t, vector.AppendFixed(
			source.Vecs[1], types.NewRowid(&blk, uint32(row)), false, mp,
		))
		require.NoError(t, vector.AppendFixed(
			source.Vecs[2], types.BuildTS(100, uint32(row)), false, mp,
		))
		require.NoError(t, vector.AppendFixed(source.Vecs[3], row == 1, false, mp))
	}
	source.SetRowCount(3)
	_, err = writer.WriteBatch(source)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	source.Clean(mp)

	stats := writer.Stats()
	require.NoError(t, objectio.SetObjectStatsBlkCnt(&stats, uint32(len(blocks))))
	metaLoc := stats.ObjectLocation()
	meta, err := objectio.FastLoadObjectMeta(ctx, &metaLoc, false, fs)
	require.NoError(t, err)
	require.Equal(t, uint8(compress.Lz4Chunked), meta.MustGetMeta(objectio.SchemaData).
		GetBlockMeta(0).ColumnMeta(0).Location().Alg())

	var reader io.ReadCloser
	err = fs.Read(ctx, &fileservice.IOVector{
		FilePath: stats.ObjectName().String(),
		Entries: []fileservice.IOEntry{{
			Offset: 0, Size: -1, ReadCloserForRead: &reader,
		}},
		Policy: fileservice.SkipAllCache,
	})
	require.NoError(t, err)
	objectContent, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	converted, err := convertObjectToBatch(
		ctx, objectContent, &stats, types.BuildTS(200, 0), fs, mp,
	)
	require.NoError(t, err)
	defer converted.Close()
	require.Equal(t, 3, converted.Length())
	require.Len(t, converted.GetVectorByName("tmp_0").ShallowGet(0).([]byte), len(payload))

	filtered, offsets, err := filterBatchBySnapshotTS(
		ctx, converted, types.BuildTS(200, 0), mp,
	)
	require.NoError(t, err)
	require.Same(t, converted, filtered)
	require.Equal(t, []uint32{0, 2}, offsets)
	require.Equal(t, 2, filtered.Length())
}

func TestDecompressObjectColumnExtentNoneClonesData(t *testing.T) {
	input := []byte("uncompressed-column")
	ext := objectio.NewExtent(compress.None, 0, uint32(len(input)), uint32(len(input)))
	got, err := decompressObjectColumnExtent(context.Background(), input, ext, nil)
	require.NoError(t, err)
	require.Equal(t, input, got)
	got[0] = 'U'
	require.Equal(t, byte('u'), input[0])
}

func TestDecompressObjectColumnExtentRejectsMalformedCompressedData(t *testing.T) {
	ext := objectio.NewExtent(compress.Lz4, 0, 3, 8)
	_, err := decompressObjectColumnExtent(context.Background(), []byte{1, 2, 3}, ext, nil)
	require.Error(t, err)
}

// ============================================================
// Tests for rewriteTombstoneRowidsBatch (CN batch version)
// ============================================================

func TestRewriteTombstoneRowidsBatch_NilBatch(t *testing.T) {
	err := rewriteTombstoneRowidsBatch(context.Background(), nil, nil, nil)
	assert.NoError(t, err)
}

func TestRewriteTombstoneRowidsBatch_EmptyBatch(t *testing.T) {
	bat := &batch.Batch{}
	bat.SetRowCount(0)
	err := rewriteTombstoneRowidsBatch(context.Background(), bat, nil, nil)
	assert.NoError(t, err)
}

func TestRewriteTombstoneRowidsBatch_NilAObjectMap(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	rid := types.BuildTestRowid(1, 2)
	require.NoError(t, vector.AppendFixed(rowidVec, rid, false, mp))

	bat := &batch.Batch{Vecs: []*vector.Vector{rowidVec}}
	bat.SetRowCount(1)

	err = rewriteTombstoneRowidsBatch(context.Background(), bat, nil, mp)
	assert.NoError(t, err)
	rowidVec.Free(mp)
}

func TestRewriteTombstoneRowidsBatch_NilVec(t *testing.T) {
	bat := &batch.Batch{Vecs: []*vector.Vector{nil}}
	bat.SetRowCount(1)
	amap := NewAObjectMap()
	err := rewriteTombstoneRowidsBatch(context.Background(), bat, amap, nil)
	assert.NoError(t, err)
}

func TestRewriteTombstoneRowidsBatch_WrongType(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	intVec := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(intVec, int32(1), false, mp))

	bat := &batch.Batch{Vecs: []*vector.Vector{intVec}}
	bat.SetRowCount(1)
	amap := NewAObjectMap()

	err = rewriteTombstoneRowidsBatch(context.Background(), bat, amap, mp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "first column of tombstone should be rowid")
	intVec.Free(mp)
}

func TestRewriteTombstoneRowidsBatch_WithMapping(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	// Create upstream object ID
	upstreamObjID := types.NewObjectid()

	// Create a rowid referencing the upstream object
	rid := types.NewRowIDWithObjectIDBlkNumAndRowID(upstreamObjID, 0, 42)

	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(rowidVec, rid, false, mp))

	bat := &batch.Batch{Vecs: []*vector.Vector{rowidVec}}
	bat.SetRowCount(1)

	// Create downstream stats with a different object ID
	downstreamObjID := types.NewObjectid()
	var downstreamStats objectio.ObjectStats
	objectio.SetObjectStatsObjectName(&downstreamStats, objectio.BuildObjectNameWithObjectID(&downstreamObjID))

	// Build AObjectMap
	amap := NewAObjectMap()
	amap.Set(upstreamObjID.String(), &AObjectMapping{
		DownstreamStats: downstreamStats,
		RowOffsetMap:    map[uint32]uint32{42: 99},
	})

	err = rewriteTombstoneRowidsBatch(context.Background(), bat, amap, mp)
	assert.NoError(t, err)

	// Verify the rowid was rewritten
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](rowidVec)
	rewrittenObjID := rowids[0].BorrowObjectID()
	assert.Equal(t, downstreamObjID.Segment(), rewrittenObjID.Segment())
	assert.Equal(t, uint32(99), rowids[0].GetRowOffset())

	rowidVec.Free(mp)
}

// ============================================================
// Tests for filterBatchBySnapshotTS (containers.Batch version)
// ============================================================

func TestFilterBatchBySnapshotTS_NilBatch(t *testing.T) {
	result, offsets, err := filterBatchBySnapshotTS(context.Background(), nil, types.TS{}, nil)
	assert.NoError(t, err)
	assert.Nil(t, result)
	assert.Nil(t, offsets)
}

func TestFilterBatchBySnapshotTS_NoDeletes(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	// Create batch with commit TS column where all TS <= snapshotTS
	bat := containers.NewBatch()
	tsVec := containers.MakeVector(types.T_TS.ToType(), mp)
	ts1 := types.BuildTS(100, 0)
	ts2 := types.BuildTS(200, 0)
	tsVec.Append(ts1, false)
	tsVec.Append(ts2, false)
	bat.AddVector(objectio.TombstoneAttr_CommitTs_Attr, tsVec)

	snapshotTS := types.BuildTS(300, 0)
	result, offsets, err := filterBatchBySnapshotTS(context.Background(), bat, snapshotTS, mp)
	assert.NoError(t, err)
	assert.Equal(t, 2, result.Length())
	assert.Equal(t, []uint32{0, 1}, offsets)
	bat.Close()
}

func TestFilterBatchBySnapshotTS_WithDeletes(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	bat := containers.NewBatch()
	tsVec := containers.MakeVector(types.T_TS.ToType(), mp)
	ts1 := types.BuildTS(100, 0)
	ts2 := types.BuildTS(500, 0) // > snapshotTS, should be filtered
	ts3 := types.BuildTS(200, 0)
	tsVec.Append(ts1, false)
	tsVec.Append(ts2, false)
	tsVec.Append(ts3, false)
	bat.AddVector(objectio.TombstoneAttr_CommitTs_Attr, tsVec)
	abortVec := containers.MakeVector(types.T_bool.ToType(), mp)
	abortVec.Append(false, false)
	abortVec.Append(false, false)
	abortVec.Append(true, false) // visible commit TS, but rolled back
	bat.AddVector(objectio.TombstoneAttr_Abort_Attr, abortVec)

	snapshotTS := types.BuildTS(300, 0)
	result, offsets, err := filterBatchBySnapshotTS(context.Background(), bat, snapshotTS, mp)
	assert.NoError(t, err)
	assert.Equal(t, 1, result.Length())
	assert.Equal(t, []uint32{0}, offsets)
	bat.Close()
}

func TestFilterBatchBySnapshotTS_AllDeleted(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	bat := containers.NewBatch()
	tsVec := containers.MakeVector(types.T_TS.ToType(), mp)
	ts1 := types.BuildTS(500, 0)
	ts2 := types.BuildTS(600, 0)
	tsVec.Append(ts1, false)
	tsVec.Append(ts2, false)
	bat.AddVector(objectio.TombstoneAttr_CommitTs_Attr, tsVec)

	snapshotTS := types.BuildTS(100, 0)
	result, offsets, err := filterBatchBySnapshotTS(context.Background(), bat, snapshotTS, mp)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Length())
	assert.Empty(t, offsets)
	bat.Close()
}

func TestPublicationOffsetMapPreservesInteriorAbortProvenance(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	bat := containers.NewBatch()
	pkVec := containers.MakeVector(types.T_int32.ToType(), mp)
	commitTSVec := containers.MakeVector(types.T_TS.ToType(), mp)
	abortVec := containers.MakeVector(types.T_bool.ToType(), mp)
	for row, key := range []int32{20, 99, 10} {
		pkVec.Append(key, false)
		commitTSVec.Append(types.BuildTS(1, 0), false)
		abortVec.Append(row == 1, false)
	}
	bat.AddVector("pk", pkVec)
	bat.AddVector(objectio.TombstoneAttr_CommitTs_Attr, commitTSVec)
	bat.AddVector(objectio.TombstoneAttr_Abort_Attr, abortVec)

	filtered, originalOffsets, err := filterBatchBySnapshotTS(
		context.Background(), bat, types.BuildTS(2, 0), mp,
	)
	require.NoError(t, err)
	require.Equal(t, []uint32{0, 2}, originalOffsets)

	var originalStats objectio.ObjectStats
	downstreamStats, rowOffsetMap, err := createObjectFromBatch(
		context.Background(),
		filtered,
		originalOffsets,
		&originalStats,
		types.BuildTS(2, 0),
		false,
		testutil.NewSharedFS(),
		mp,
		0,
		false,
	)
	require.NoError(t, err)
	require.Equal(t, map[uint32]uint32{0: 1, 2: 0}, rowOffsetMap)

	upstreamObjID := types.NewObjectid()
	deleteRowID := types.NewRowIDWithObjectIDBlkNumAndRowID(upstreamObjID, 0, 2)
	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(rowIDVec, deleteRowID, false, mp))
	tombstoneBatch := &batch.Batch{Vecs: []*vector.Vector{rowIDVec}}
	tombstoneBatch.SetRowCount(1)
	defer tombstoneBatch.Clean(mp)

	aobjectMap := NewAObjectMap()
	aobjectMap.Set(upstreamObjID.String(), &AObjectMapping{
		DownstreamStats: downstreamStats,
		RowOffsetMap:    rowOffsetMap,
	})
	require.NoError(t, rewriteTombstoneRowidsBatch(
		context.Background(), tombstoneBatch, aobjectMap, mp,
	))
	rewritten := vector.GetFixedAtNoTypeCheck[types.Rowid](rowIDVec, 0)
	require.Equal(t, uint32(0), rewritten.GetRowOffset())
	require.Equal(
		t,
		downstreamStats.ObjectName().ObjectId().String(),
		rewritten.BorrowObjectID().String(),
	)
}

// ============================================================
// Tests for rewriteTombstoneRowids (containers.Batch version)
// ============================================================

func TestRewriteTombstoneRowids_NilBatch(t *testing.T) {
	err := rewriteTombstoneRowids(context.Background(), nil, nil, nil)
	assert.NoError(t, err)
}

func TestRewriteTombstoneRowids_EmptyBatch(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	// An empty containers.Batch with a zero-length vector
	bat := containers.NewBatch()
	rowidVec := containers.MakeVector(types.T_Rowid.ToType(), mp)
	bat.AddVector("rowid", rowidVec)

	err = rewriteTombstoneRowids(context.Background(), bat, nil, mp)
	assert.NoError(t, err)
	bat.Close()
}

func TestRewriteTombstoneRowids_NilAObjectMap(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	bat := containers.NewBatch()
	rowidVec := containers.MakeVector(types.T_Rowid.ToType(), mp)
	rid := types.BuildTestRowid(1, 2)
	rowidVec.Append(rid, false)
	bat.AddVector("rowid", rowidVec)

	err = rewriteTombstoneRowids(context.Background(), bat, nil, mp)
	assert.NoError(t, err)
	bat.Close()
}

func TestRewriteTombstoneRowids_EmptyVec(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	bat := containers.NewBatch()
	rowidVec := containers.MakeVector(types.T_Rowid.ToType(), mp)
	bat.AddVector("rowid", rowidVec)
	amap := NewAObjectMap()

	err = rewriteTombstoneRowids(context.Background(), bat, amap, mp)
	assert.NoError(t, err)
	bat.Close()
}

func TestRewriteTombstoneRowids_WrongType(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	bat := containers.NewBatch()
	intVec := containers.MakeVector(types.T_int32.ToType(), mp)
	intVec.Append(int32(1), false)
	bat.AddVector("col", intVec)
	amap := NewAObjectMap()

	err = rewriteTombstoneRowids(context.Background(), bat, amap, mp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "first column of tombstone should be rowid")
	bat.Close()
}

func TestRewriteTombstoneRowids_WithMapping(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	upstreamObjID := types.NewObjectid()
	rid := types.NewRowIDWithObjectIDBlkNumAndRowID(upstreamObjID, 0, 10)

	bat := containers.NewBatch()
	rowidVec := containers.MakeVector(types.T_Rowid.ToType(), mp)
	rowidVec.Append(rid, false)
	bat.AddVector("rowid", rowidVec)

	downstreamObjID := types.NewObjectid()
	var downstreamStats objectio.ObjectStats
	objectio.SetObjectStatsObjectName(&downstreamStats, objectio.BuildObjectNameWithObjectID(&downstreamObjID))

	amap := NewAObjectMap()
	amap.Set(upstreamObjID.String(), &AObjectMapping{
		DownstreamStats: downstreamStats,
		RowOffsetMap:    map[uint32]uint32{10: 20},
	})

	err = rewriteTombstoneRowids(context.Background(), bat, amap, mp)
	assert.NoError(t, err)

	// Verify rewrite
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](rowidVec.GetDownstreamVector())
	rewrittenObjID := rowids[0].BorrowObjectID()
	assert.Equal(t, downstreamObjID.Segment(), rewrittenObjID.Segment())
	assert.Equal(t, uint32(20), rowids[0].GetRowOffset())
	bat.Close()
}

func TestRewriteTombstoneRowids_NoMatchingMapping(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	upstreamObjID := types.NewObjectid()
	rid := types.NewRowIDWithObjectIDBlkNumAndRowID(upstreamObjID, 0, 5)

	bat := containers.NewBatch()
	rowidVec := containers.MakeVector(types.T_Rowid.ToType(), mp)
	rowidVec.Append(rid, false)
	bat.AddVector("rowid", rowidVec)

	// AObjectMap with a different object ID
	amap := NewAObjectMap()
	otherObjID := types.NewObjectid()
	var stats objectio.ObjectStats
	objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectNameWithObjectID(&otherObjID))
	amap.Set(otherObjID.String(), &AObjectMapping{DownstreamStats: stats})

	err = rewriteTombstoneRowids(context.Background(), bat, amap, mp)
	assert.NoError(t, err)

	// Rowid should be unchanged
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](rowidVec.GetDownstreamVector())
	assert.Equal(t, upstreamObjID.String(), rowids[0].BorrowObjectID().String())
	bat.Close()
}
