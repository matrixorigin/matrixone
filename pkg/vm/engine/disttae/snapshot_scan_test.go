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
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestBuildSnapshotBlockFilter(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	tableDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 7},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 9},
		},
		Name2ColIndex: map[string]int32{
			"a": 0,
			"b": 1,
		},
	}

	t.Run("nil expr", func(t *testing.T) {
		filter, seqnum, typ, ok, err := buildSnapshotBlockFilter(tableDef, nil, mp)
		require.NoError(t, err)
		require.False(t, ok)
		require.False(t, filter.Valid)
		require.Zero(t, seqnum)
		require.Equal(t, types.Type{}, typ)
	})

	t.Run("non pk expr", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		defer vec.Free(mp)
		for _, v := range []int64{1, 3, 5} {
			require.NoError(t, vector.AppendFixed(vec, v, false, mp))
		}
		expr := readutil.ConstructInExpr(context.Background(), "b", vec)

		filter, seqnum, typ, ok, err := buildSnapshotBlockFilter(tableDef, expr, mp)
		require.NoError(t, err)
		require.False(t, ok)
		require.False(t, filter.Valid)
		require.Zero(t, seqnum)
		require.Equal(t, types.Type{}, typ)
	})

	t.Run("pk in expr", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		defer vec.Free(mp)
		for _, v := range []int64{1, 3, 5} {
			require.NoError(t, vector.AppendFixed(vec, v, false, mp))
		}
		vec.InplaceSort()
		expr := readutil.ConstructInExpr(context.Background(), "a", vec)

		filter, seqnum, typ, ok, err := buildSnapshotBlockFilter(tableDef, expr, mp)
		if filter.Cleanup != nil {
			defer filter.Cleanup()
		}

		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, filter.Valid)
		require.NotNil(t, filter.DecideSearchFunc(true))
		require.NotNil(t, filter.DecideSearchFunc(false))
		require.Equal(t, uint16(7), seqnum)
		require.Equal(t, types.T_int64, typ.Oid)
	})
}

func TestNormalizeSnapshotScanParallelism(t *testing.T) {
	t.Run("nil rel data", func(t *testing.T) {
		require.Equal(t, 1, normalizeSnapshotScanParallelism(nil, 0))
	})

	t.Run("serial on small scan", func(t *testing.T) {
		relData := newTestSnapshotRelData(snapshotScanMinBlocksPerReader*2 - 1)
		require.Equal(t, 1, normalizeSnapshotScanParallelism(relData, 0))
		require.Equal(t, 1, normalizeSnapshotScanParallelism(relData, 1))
	})

	t.Run("auto parallelism on large scan", func(t *testing.T) {
		relData := newTestSnapshotRelData(snapshotScanMinBlocksPerReader * 4)
		actual := normalizeSnapshotScanParallelism(relData, 0)
		require.GreaterOrEqual(t, actual, 2)
		require.LessOrEqual(t, actual, snapshotScanMaxParallelism)
	})

	t.Run("respect requested cap", func(t *testing.T) {
		relData := newTestSnapshotRelData(snapshotScanMinBlocksPerReader * 4)
		require.Equal(t, 2, normalizeSnapshotScanParallelism(relData, 2))
	})
}

func TestSplitSnapshotScanShards(t *testing.T) {
	t.Run("nil rel data", func(t *testing.T) {
		shards := splitSnapshotScanShards(nil, 4)
		require.Len(t, shards, 1)
		require.Nil(t, shards[0])
	})

	relData := newTestSnapshotRelData(snapshotScanMinBlocksPerReader*4 + 3)
	shards := splitSnapshotScanShards(relData, 4)
	require.Len(t, shards, 4)

	totalBlocks := 0
	for _, shard := range shards {
		require.NotNil(t, shard)
		require.Greater(t, shard.DataCnt(), 0)
		totalBlocks += shard.DataCnt()
	}
	require.Equal(t, relData.DataCnt(), totalBlocks)
}

func TestAttrsToSeqnums(t *testing.T) {
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id", Seqnum: 3},
		},
		Name2ColIndex: map[string]int32{
			"id": 0,
		},
	}

	seqnums := attrsToSeqnums([]string{"id", objectio.DefaultCommitTS_Attr, objectio.PhysicalAddr_Attr}, tableDef)
	require.Equal(t, []uint16{3, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ROWID}, seqnums)
}

func TestReadSnapshotWithSource_SkipsEmptyBatchAndEmitsRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	source := &stubSnapshotDataSource{
		steps: []snapshotDataSourceStep{
			{state: engine.InMem, values: nil},
			{state: engine.InMem, values: []int64{7, 9}},
			{state: engine.End},
		},
	}

	var got []int64
	err := readSnapshotWithSource(
		context.Background(),
		nil,
		source,
		types.BuildTS(20, 0),
		snapshotScanReaderConfig{
			attrs:    []string{"id"},
			seqnums:  []uint16{1},
			colTypes: []types.Type{types.T_int64.ToType()},
		},
		mp,
		func(bat *batch.Batch) error {
			got = append(got, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])...)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []int64{7, 9}, got)
	require.True(t, source.closed)
}

func TestReadSnapshotWithSource_PropagatesOnBatchError(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	source := &stubSnapshotDataSource{
		steps: []snapshotDataSourceStep{
			{state: engine.InMem, values: []int64{1}},
		},
	}
	wantErr := moerr.NewInternalErrorNoCtx("emit failed")

	err := readSnapshotWithSource(
		context.Background(),
		nil,
		source,
		types.BuildTS(20, 0),
		snapshotScanReaderConfig{
			attrs:    []string{"id"},
			seqnums:  []uint16{1},
			colTypes: []types.Type{types.T_int64.ToType()},
		},
		mp,
		func(*batch.Batch) error {
			return wantErr
		},
	)
	require.ErrorIs(t, err, wantErr)
	require.True(t, source.closed)
}

func TestBuildSnapshotScanReaderConfig(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	tableDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 5},
			{Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 6},
		},
		Name2ColIndex: map[string]int32{
			"id":      0,
			"payload": 1,
		},
	}

	filterVec := vector.NewVec(types.T_int64.ToType())
	defer filterVec.Free(mp)
	require.NoError(t, vector.AppendFixed(filterVec, int64(3), false, mp))
	filterExpr := readutil.ConstructInExpr(context.Background(), "id", filterVec)

	cfg, err := buildSnapshotScanReaderConfig(
		tableDef,
		[]string{"ID", "payload", objectio.DefaultCommitTS_Attr},
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType(), types.T_TS.ToType()},
		filterExpr,
		mp,
	)
	if cfg.blockFilter.Cleanup != nil {
		defer cfg.blockFilter.Cleanup()
	}

	require.NoError(t, err)
	require.Equal(t, []uint16{5, 6, objectio.SEQNUM_COMMITTS}, cfg.seqnums)
	require.True(t, cfg.hasBlockFilter)
	require.Equal(t, uint16(5), cfg.filterSeqnum)
	require.Equal(t, types.T_int64, cfg.filterType.Oid)

	cfg, err = buildSnapshotScanReaderConfig(
		tableDef,
		[]string{"id", "payload"},
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
		nil,
		mp,
	)
	require.NoError(t, err)
	require.Equal(t, []uint16{5, 6}, cfg.seqnums)
	require.False(t, cfg.hasBlockFilter)
}

func TestUnwrapTxnTable(t *testing.T) {
	tbl := &txnTable{}

	got, err := unwrapTxnTable(tbl)
	require.NoError(t, err)
	require.Same(t, tbl, got)

	got, err = unwrapTxnTable(&txnTableDelegate{origin: tbl})
	require.NoError(t, err)
	require.Same(t, tbl, got)

	_, err = unwrapTxnTable(&mockRelation{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot scan requires disttae relation")
}

func TestGetSnapshotScanSubmitPool(t *testing.T) {
	t.Cleanup(releaseSnapshotScanSubmitPoolForTest)

	pool1, err := getSnapshotScanSubmitPool()
	require.NoError(t, err)
	require.NotNil(t, pool1)

	pool2, err := getSnapshotScanSubmitPool()
	require.NoError(t, err)
	require.Same(t, pool1, pool2)
}

func TestScanSnapshotShardsParallel_SkipsEmptyShards(t *testing.T) {
	t.Cleanup(releaseSnapshotScanSubmitPoolForTest)

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	err := scanSnapshotShardsParallel(
		context.Background(),
		nil,
		types.BuildTS(20, 0),
		[]engine.RelData{nil, newTestSnapshotRelData(0)},
		snapshotScanReaderConfig{
			attrs:    []string{"id"},
			seqnums:  []uint16{1},
			colTypes: []types.Type{types.T_int64.ToType()},
		},
		mp,
		func(*batch.Batch) error {
			t.Fatal("empty shards should not emit batches")
			return nil
		},
	)
	require.NoError(t, err)
}

func TestScanSnapshotShardsParallel_ContextCanceled(t *testing.T) {
	t.Cleanup(releaseSnapshotScanSubmitPoolForTest)

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := scanSnapshotShardsParallel(
		ctx,
		nil,
		types.BuildTS(20, 0),
		[]engine.RelData{newTestSnapshotRelData(1)},
		snapshotScanReaderConfig{
			attrs:    []string{"id"},
			seqnums:  []uint16{1},
			colTypes: []types.Type{types.T_int64.ToType()},
		},
		mp,
		func(*batch.Batch) error { return nil },
	)
	require.ErrorIs(t, err, context.Canceled)
}

func TestScanSnapshotShardWithCommittedInMem_EmptyRange(t *testing.T) {
	tbl, fs := newSnapshotScanTxnTable(t)
	mp := tbl.proc.Load().Mp()

	pState := tbl.eng.(*Engine).GetOrCreateLatestPart(context.Background(), uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId).Snapshot()
	err := scanSnapshotShardWithCommittedInMem(
		context.Background(),
		fs,
		pState,
		types.TimestampToTS(tbl.db.op.SnapshotTS()),
		types.BuildTS(30, 0),
		nil,
		readutil.NewEmptyTombstoneData(),
		snapshotScanReaderConfig{
			attrs:    []string{"id"},
			seqnums:  []uint16{1},
			colTypes: []types.Type{types.T_int64.ToType()},
		},
		mp,
		func(*batch.Batch) error {
			t.Fatal("empty local scan should not emit batches")
			return nil
		},
	)
	require.NoError(t, err)
}

func TestScanSnapshotCommittedInMem_EmptyState(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	err := scanSnapshotCommittedInMem(
		context.Background(),
		nil,
		nil,
		types.BuildTS(30, 0),
		types.BuildTS(20, 0),
		nil,
		snapshotScanReaderConfig{
			attrs:    []string{"id"},
			seqnums:  []uint16{1},
			colTypes: []types.Type{types.T_int64.ToType()},
		},
		mp,
		func(*batch.Batch) error {
			t.Fatal("empty committed in-memory scan should not emit batches")
			return nil
		},
	)
	require.NoError(t, err)
}

func TestMaterializedSnapshotDataSource_NoPartitionState(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	source := newMaterializedSnapshotDataSource(
		context.Background(),
		nil,
		nil,
		types.BuildTS(30, 0),
		types.BuildTS(20, 0),
		nil,
		nil,
	)
	ds := source.(*materializedSnapshotDataSource)
	require.Contains(t, ds.String(), "MaterializedSnapshotDataSource")

	out := batch.NewWithSize(1)
	out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer out.Clean(mp)

	_, state, err := source.Next(
		context.Background(),
		[]string{"id"},
		[]types.Type{types.T_int64.ToType()},
		[]uint16{1},
		0,
		nil,
		mp,
		out,
	)
	require.NoError(t, err)
	require.Equal(t, engine.End, state)

	rows, err := source.ApplyTombstones(
		context.Background(),
		nil,
		[]int64{2, 1},
		engine.Policy_CheckAll,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 1}, rows)

	bm, err := source.GetTombstones(context.Background(), nil)
	require.NoError(t, err)
	bm.Release()

	orderBy := []*plan.OrderBySpec{{}}
	source.SetOrderBy(orderBy)
	require.Equal(t, orderBy, source.GetOrderBy())
	source.SetFilterZM(objectio.ZoneMap{})
	source.Close()
	require.Nil(t, ds.inMemBatches)
}

func TestMaterializedSnapshotDataSource_BuildsCommittedRows(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	state := logtailreplay.NewPartitionState("svc", false, 42, false)
	packer := types.NewPacker()
	defer packer.Close()

	var blk types.Blockid
	copy(blk[:], []byte("materialized-row"))
	rowID := types.NewRowid(&blk, 0)
	commitTS := types.BuildTS(10, 0)

	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())
	idVec := vector.NewVec(types.T_int64.ToType())
	defer rowIDVec.Free(mp)
	defer tsVec.Free(mp)
	defer idVec.Free(mp)
	require.NoError(t, vector.AppendFixed(rowIDVec, rowID, false, mp))
	require.NoError(t, vector.AppendFixed(tsVec, commitTS, false, mp))
	require.NoError(t, vector.AppendFixed(idVec, int64(42), false, mp))
	state.HandleRowsInsert(ctx, &api.Batch{
		Attrs: []string{"rowid", "time", "id"},
		Vecs: []api.Vector{
			mustProtoVectorForSnapshotTest(t, rowIDVec),
			mustProtoVectorForSnapshotTest(t, tsVec),
			mustProtoVectorForSnapshotTest(t, idVec),
		},
	}, 0, packer, mp)

	source := newMaterializedSnapshotDataSource(
		ctx,
		nil,
		state,
		types.BuildTS(30, 0),
		types.BuildTS(20, 0),
		nil,
		nil,
	)
	defer source.Close()

	out := batch.NewWithSize(3)
	out.SetAttributes([]string{"rowid", "id", objectio.DefaultCommitTS_Attr})
	out.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	out.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	out.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	defer out.Clean(mp)

	attrs := []string{"rowid", "id", objectio.DefaultCommitTS_Attr}
	colTypes := []types.Type{types.T_Rowid.ToType(), types.T_int64.ToType(), types.T_TS.ToType()}
	seqnums := []uint16{objectio.SEQNUM_ROWID, 0, objectio.SEQNUM_COMMITTS}

	_, stateKind, err := source.Next(ctx, attrs, colTypes, seqnums, 0, nil, mp, out)
	require.NoError(t, err)
	require.Equal(t, engine.InMem, stateKind)
	require.Equal(t, 1, out.RowCount())
	require.Equal(t, rowID, vector.MustFixedColWithTypeCheck[types.Rowid](out.Vecs[0])[0])
	require.Equal(t, int64(42), vector.MustFixedColWithTypeCheck[int64](out.Vecs[1])[0])
	require.Equal(t, commitTS, vector.MustFixedColWithTypeCheck[types.TS](out.Vecs[2])[0])

	_, stateKind, err = source.Next(ctx, attrs, colTypes, seqnums, 0, nil, mp, out)
	require.NoError(t, err)
	require.Equal(t, engine.End, stateKind)
}

func TestMaterializedSnapshotDataSource_DelegatesToRemote(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	remote := &stubSnapshotDataSource{
		steps: []snapshotDataSourceStep{
			{state: engine.Persisted, values: []int64{3, 4}},
		},
	}
	ds := &materializedSnapshotDataSource{
		currentTS:        types.BuildTS(30, 0),
		snapshotTS:       types.BuildTS(20, 0),
		remote:           remote,
		deletedRowsCache: make(map[objectio.Blockid]*objectio.Bitmap),
	}

	out := batch.NewWithSize(1)
	out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer out.Clean(mp)

	_, state, err := ds.Next(
		context.Background(),
		[]string{"id"},
		[]types.Type{types.T_int64.ToType()},
		[]uint16{1},
		0,
		nil,
		mp,
		out,
	)
	require.NoError(t, err)
	require.Equal(t, engine.Persisted, state)

	rows, err := ds.ApplyTombstones(context.Background(), nil, []int64{1}, engine.Policy_CheckCommittedOnly)
	require.NoError(t, err)
	require.Nil(t, rows)

	_, err = ds.GetTombstones(context.Background(), nil)
	require.NoError(t, err)

	orderBy := []*plan.OrderBySpec{{}}
	ds.SetOrderBy(orderBy)
	require.Equal(t, orderBy, ds.GetOrderBy())
	ds.SetFilterZM(objectio.ZoneMap{})
	ds.Close()
	require.True(t, remote.closed)
}

func TestAppendCommittedInMemEntry(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	var blk types.Blockid
	copy(blk[:], []byte("snapshot-materialized"))
	rowID := types.NewRowid(&blk, 7)
	commitTS := types.BuildTS(42, 0)

	src := batch.NewWithSize(3)
	src.SetAttributes([]string{"rowid", "commit_ts", "id"})
	src.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	src.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	src.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(src.Vecs[0], rowID, false, mp))
	require.NoError(t, vector.AppendFixed(src.Vecs[1], commitTS, false, mp))
	require.NoError(t, vector.AppendFixed(src.Vecs[2], int64(99), false, mp))
	src.SetRowCount(1)
	defer src.Clean(mp)

	out := newMaterializedInMemBatch(
		[]string{"rowid", "commit_ts", "id", "missing"},
		[]types.Type{
			types.T_Rowid.ToType(),
			types.T_TS.ToType(),
			types.T_int64.ToType(),
			types.T_varchar.ToType(),
		},
	)
	defer out.Clean(mp)

	err := appendCommittedInMemEntry(
		out,
		&logtailreplay.RowEntry{
			RowID:  rowID,
			Time:   commitTS,
			Batch:  src,
			Offset: 0,
		},
		[]uint16{
			objectio.SEQNUM_ROWID,
			objectio.SEQNUM_COMMITTS,
			0,
			1,
		},
		mp,
	)
	require.NoError(t, err)
	require.Equal(t, 1, out.RowCount())
	require.Equal(t, rowID, vector.MustFixedColWithTypeCheck[types.Rowid](out.Vecs[0])[0])
	require.Equal(t, commitTS, vector.MustFixedColWithTypeCheck[types.TS](out.Vecs[1])[0])
	require.Equal(t, int64(99), vector.MustFixedColWithTypeCheck[int64](out.Vecs[2])[0])
	require.True(t, out.Vecs[3].IsNull(0))
}

func TestMaterializedSnapshotDataSource_TombstoneCache(t *testing.T) {
	var blk types.Blockid
	copy(blk[:], []byte("materialized-tomb"))
	tombstones := &stubMaterializedTombstoner{
		deletedOffsets: map[uint64]struct{}{
			2: {},
		},
	}
	ds := &materializedSnapshotDataSource{
		fs:               nil,
		snapshotTS:       types.BuildTS(20, 0),
		tombstone:        tombstones,
		deletedRowsCache: make(map[objectio.Blockid]*objectio.Bitmap),
	}
	defer ds.Close()

	rowID := types.NewRowid(&blk, 2)
	deleted, err := ds.isDeletedByCommittedTombstones(context.Background(), rowID)
	require.NoError(t, err)
	require.True(t, deleted)
	require.Equal(t, 1, tombstones.persistedCalls)

	deleted, err = ds.isDeletedByCommittedTombstones(context.Background(), rowID)
	require.NoError(t, err)
	require.True(t, deleted)
	require.Equal(t, 1, tombstones.persistedCalls)

	left, err := ds.ApplyTombstones(
		context.Background(),
		&blk,
		[]int64{3, 2, 1},
		engine.Policy_CheckAll,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3}, left)

	bm, err := ds.GetTombstones(context.Background(), &blk)
	require.NoError(t, err)
	require.True(t, bm.Contains(2))
	bm.Release()
}

func TestScanSnapshotWithCurrentRanges_EmptyRange(t *testing.T) {
	tbl, _ := newSnapshotScanTxnTable(t)
	mp := tbl.proc.Load().Mp()

	err := ScanSnapshotWithCurrentRanges(
		context.Background(),
		"unit-test",
		tbl,
		readutil.NewBlockListRelationData(0),
		types.BuildTS(40, 0),
		[]string{"id"},
		[]types.Type{types.T_int64.ToType()},
		nil,
		0,
		mp,
		func(*batch.Batch) error {
			t.Fatal("empty snapshot scan should not emit batches")
			return nil
		},
	)
	require.NoError(t, err)
}

func TestScanSnapshotWithCurrentRanges_EarlyValidation(t *testing.T) {
	t.Run("attrs type mismatch", func(t *testing.T) {
		tbl, _ := newSnapshotScanTxnTable(t)
		err := ScanSnapshotWithCurrentRanges(
			context.Background(),
			"unit-test",
			tbl,
			nil,
			types.BuildTS(40, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
			nil,
			0,
			tbl.proc.Load().Mp(),
			func(*batch.Batch) error { return nil },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "attrs/colTypes length mismatch")
	})

	t.Run("nil mpool", func(t *testing.T) {
		tbl, _ := newSnapshotScanTxnTable(t)
		err := ScanSnapshotWithCurrentRanges(
			context.Background(),
			"unit-test",
			tbl,
			nil,
			types.BuildTS(40, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType()},
			nil,
			0,
			nil,
			func(*batch.Batch) error { return nil },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-nil mpool")
	})

	t.Run("non disttae relation", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)

		err := ScanSnapshotWithCurrentRanges(
			context.Background(),
			"unit-test",
			&mockRelation{},
			nil,
			types.BuildTS(40, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType()},
			nil,
			0,
			mp,
			func(*batch.Batch) error { return nil },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "snapshot scan requires disttae relation")
	})

	t.Run("non disttae engine", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)

		err := ScanSnapshotWithCurrentRanges(
			context.Background(),
			"unit-test",
			&txnTable{eng: &engine.EntireEngine{}},
			nil,
			types.BuildTS(40, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType()},
			nil,
			0,
			mp,
			func(*batch.Batch) error { return nil },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires disttae engine")
	})
}

func TestReadSnapshotWithSource_ReaderError(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	wantErr := moerr.NewInternalErrorNoCtx("reader failed")
	source := &stubSnapshotDataSource{
		steps: []snapshotDataSourceStep{
			{state: engine.InMem, err: wantErr},
		},
	}

	err := readSnapshotWithSource(
		context.Background(),
		nil,
		source,
		types.BuildTS(20, 0),
		snapshotScanReaderConfig{
			attrs:    []string{"id"},
			seqnums:  []uint16{1},
			colTypes: []types.Type{types.T_int64.ToType()},
		},
		mp,
		func(*batch.Batch) error { return nil },
	)
	require.ErrorIs(t, err, wantErr)
	require.True(t, source.closed)
}

func TestBuildSnapshotBlockFilter_MissingPrimaryKeyColumn(t *testing.T) {
	mp := mpool.MustNew(t.Name())

	tableDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "missing_pk",
		},
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 1},
		},
		Name2ColIndex: map[string]int32{
			"id": 0,
		},
	}

	filterVec := vector.NewVec(types.T_int64.ToType())
	defer filterVec.Free(mp)
	require.NoError(t, vector.AppendFixed(filterVec, int64(3), false, mp))
	expr := readutil.ConstructInExpr(context.Background(), "missing_pk", filterVec)

	_, _, _, _, err := buildSnapshotBlockFilter(tableDef, expr, mp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot resolve primary key column")
}

type snapshotDataSourceStep struct {
	state  engine.DataState
	values []int64
	err    error
}

type stubSnapshotDataSource struct {
	steps   []snapshotDataSourceStep
	idx     int
	closed  bool
	orderBy []*plan.OrderBySpec
}

func (s *stubSnapshotDataSource) Next(
	_ context.Context,
	_ []string,
	_ []types.Type,
	_ []uint16,
	_ int32,
	_ any,
	mp *mpool.MPool,
	bat *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {
	if s.idx >= len(s.steps) {
		return nil, engine.End, nil
	}
	step := s.steps[s.idx]
	s.idx++
	if step.err != nil {
		return nil, step.state, step.err
	}
	if step.state != engine.InMem {
		return nil, step.state, nil
	}
	for _, value := range step.values {
		if err := vector.AppendFixed(bat.Vecs[0], value, false, mp); err != nil {
			return nil, engine.End, err
		}
	}
	bat.SetRowCount(len(step.values))
	return nil, engine.InMem, nil
}

func (s *stubSnapshotDataSource) ApplyTombstones(
	context.Context,
	*objectio.Blockid,
	[]int64,
	engine.TombstoneApplyPolicy,
) ([]int64, error) {
	return nil, nil
}

func (s *stubSnapshotDataSource) GetTombstones(context.Context, *objectio.Blockid) (objectio.Bitmap, error) {
	return objectio.Bitmap{}, nil
}

func (s *stubSnapshotDataSource) SetOrderBy(orderBy []*plan.OrderBySpec) {
	s.orderBy = orderBy
}

func (s *stubSnapshotDataSource) GetOrderBy() []*plan.OrderBySpec {
	return s.orderBy
}

func (s *stubSnapshotDataSource) SetFilterZM(objectio.ZoneMap) {}

func (s *stubSnapshotDataSource) Close() {
	s.closed = true
}

func (s *stubSnapshotDataSource) String() string {
	return "stubSnapshotDataSource"
}

func newTestSnapshotRelData(blockCnt int) *readutil.BlockListRelData {
	relData := readutil.NewBlockListRelationData(0)
	for i := 0; i < blockCnt; i++ {
		blk := objectio.BlockInfo{}
		relData.AppendBlockInfo(&blk)
	}
	return relData
}

func newSnapshotScanTxnTable(t *testing.T) (*txnTable, fileservice.FileService) {
	t.Helper()

	tbl := newTxnTableForTest()
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	eng := tbl.eng.(*Engine)
	eng.fs = fs
	eng.service = t.Name()
	eng.partitions = make(map[[2]uint64]*logtailreplay.Partition)
	eng.catalog.Store(cache.NewCatalog())

	txn := tbl.db.op.GetWorkspace().(*Transaction)
	txn.engine = eng
	txn.proc = proc
	txn.tableCache = &sync.Map{}
	txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
	txn.cn_flushed_s3_tombstone_object_stats_list = &sync.Map{}

	tbl.proc.Store(proc)
	tbl.accountId = 0
	tbl.tableId = 100
	tbl.tableName = "snapshot_scan_test"
	tbl.fake = true
	tbl.relKind = "V"
	tbl.tableDef = &plan.TableDef{
		Name: "snapshot_scan_test",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 1},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
		Name2ColIndex: map[string]int32{
			"id": 0,
		},
	}
	tbl.db.databaseId = 10
	tbl.db.databaseName = "db_snapshot_scan_test"

	return tbl, fs
}

// releaseSnapshotScanSubmitPoolForTest releases the global snapshot scan pool
// and resets the sync.Once so that subsequent tests or leaktest checks do not
// observe lingering goroutines from the pool.
func releaseSnapshotScanSubmitPoolForTest() {
	if snapshotScanSubmitPool != nil {
		snapshotScanSubmitPool.Release()
		snapshotScanSubmitPool = nil
	}
	snapshotScanSubmitPoolOnce = sync.Once{}
	snapshotScanSubmitPoolErr = nil
}

func mustProtoVectorForSnapshotTest(t *testing.T, vec *vector.Vector) api.Vector {
	t.Helper()

	protoVec, err := vector.VectorToProtoVector(vec)
	require.NoError(t, err)
	return protoVec
}

type stubMaterializedTombstoner struct {
	deletedOffsets map[uint64]struct{}
	persistedCalls int
}

func (s *stubMaterializedTombstoner) Type() engine.TombstoneType {
	return engine.TombstoneData
}

func (s *stubMaterializedTombstoner) HasAnyInMemoryTombstone() bool {
	return len(s.deletedOffsets) > 0
}

func (s *stubMaterializedTombstoner) HasAnyTombstoneFile() bool {
	return len(s.deletedOffsets) > 0
}

func (s *stubMaterializedTombstoner) String() string {
	return "stubMaterializedTombstoner"
}

func (s *stubMaterializedTombstoner) StringWithPrefix(prefix string) string {
	return prefix + s.String()
}

func (s *stubMaterializedTombstoner) HasBlockTombstone(
	context.Context,
	*objectio.Blockid,
	fileservice.FileService,
) (bool, error) {
	return len(s.deletedOffsets) > 0, nil
}

func (s *stubMaterializedTombstoner) MarshalBinaryWithBuffer(*bytes.Buffer) error {
	return nil
}

func (s *stubMaterializedTombstoner) UnmarshalBinary([]byte) error {
	return nil
}

func (s *stubMaterializedTombstoner) PrefetchTombstones(
	string,
	fileservice.FileService,
	[]objectio.Blockid,
) {
}

func (s *stubMaterializedTombstoner) ApplyInMemTombstones(
	_ *types.Blockid,
	rowsOffset []int64,
	deleted *objectio.Bitmap,
) (left []int64) {
	if deleted != nil {
		for row := range s.deletedOffsets {
			deleted.Add(row)
		}
	}
	if rowsOffset == nil {
		return nil
	}
	left = make([]int64, 0, len(rowsOffset))
	for _, row := range rowsOffset {
		if _, ok := s.deletedOffsets[uint64(row)]; !ok {
			left = append(left, row)
		}
	}
	return left
}

func (s *stubMaterializedTombstoner) ApplyPersistedTombstones(
	_ context.Context,
	_ fileservice.FileService,
	_ *types.TS,
	_ *types.Blockid,
	rowsOffset []int64,
	deleted *objectio.Bitmap,
) ([]int64, error) {
	s.persistedCalls++
	if deleted != nil {
		for row := range s.deletedOffsets {
			deleted.Add(row)
		}
	}
	if rowsOffset == nil {
		return nil, nil
	}
	return rowsOffset, nil
}

func (s *stubMaterializedTombstoner) Merge(engine.Tombstoner) error {
	return nil
}

func (s *stubMaterializedTombstoner) SortInMemory() {}
