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

package ioutil

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

type releaseTrackingFS struct {
	fileservice.FileService
	tracked     atomic.Int32
	outstanding atomic.Int32
}

type failLegacyReloadFS struct {
	*releaseTrackingFS
	failed                   atomic.Bool
	firstLeaseReleasedOnFail atomic.Bool
}

func (f *failLegacyReloadFS) Read(
	ctx context.Context,
	ioVector *fileservice.IOVector,
) error {
	// The first legacy read loads rowid while commitTS is synthesized. The
	// compatibility reload then has two physical entries: rowid + commitTS.
	if len(ioVector.Entries) == 2 && f.failed.CompareAndSwap(false, true) {
		f.firstLeaseReleasedOnFail.Store(f.outstanding.Load() == 0)
		return fmt.Errorf("injected legacy commit-ts reload failure")
	}
	return f.releaseTrackingFS.Read(ctx, ioVector)
}

func (f *releaseTrackingFS) Read(
	ctx context.Context,
	ioVector *fileservice.IOVector,
) error {
	if err := f.FileService.Read(ctx, ioVector); err != nil {
		return err
	}
	for i := range ioVector.Entries {
		data := ioVector.Entries[i].CachedData
		if data == nil {
			continue
		}
		f.tracked.Add(1)
		f.outstanding.Add(1)
		ioVector.Entries[i].CachedData = &releaseTrackingData{
			Data:        data,
			outstanding: &f.outstanding,
		}
	}
	return nil
}

type releaseTrackingData struct {
	fscache.Data
	outstanding *atomic.Int32
}

func TestValidateTombstoneCommitTSColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	valid := vector.NewVec(types.T_TS.ToType())
	for i := int64(1); i <= 3; i++ {
		require.NoError(t, vector.AppendFixed(valid, types.BuildTS(i, 0), false, mp))
	}
	column, err := ValidateTombstoneCommitTSColumn(3, valid)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(1, 0), column.At(0))
	require.Equal(t, types.BuildTS(3, 0), column.At(2))
	valid.Free(mp)

	missing := vector.NewConstNull(types.T_TS.ToType(), 3, mp)
	_, err = ValidateTombstoneCommitTSColumn(3, missing)
	require.ErrorContains(t, err, "unavailable")
	missing.Free(mp)

	partial := vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(partial, types.BuildTS(1, 0), false, mp))
	_, err = ValidateTombstoneCommitTSColumn(3, partial)
	require.ErrorContains(t, err, "1 rows, expected 3")
	partial.Free(mp)

	shortBacking := vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(shortBacking, types.BuildTS(1, 0), false, mp))
	shortBacking.SetLength(3)
	_, err = ValidateTombstoneCommitTSColumn(3, shortBacking)
	require.ErrorContains(t, err, "backing bytes")
	shortBacking.Free(mp)

	nullCommitTS := vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(nullCommitTS, types.BuildTS(1, 0), true, mp))
	_, err = ValidateTombstoneCommitTSColumn(1, nullCommitTS)
	require.ErrorContains(t, err, "contains null rows")
	nullCommitTS.Free(mp)

	wrongType := vector.NewVec(types.T_int64.ToType())
	_, err = ValidateTombstoneCommitTSColumn(0, wrongType)
	require.ErrorContains(t, err, "expected TS")
	wrongType.Free(mp)

	constant, err := vector.NewConstFixed(types.T_TS.ToType(), types.BuildTS(1, 0), 3, mp)
	require.NoError(t, err)
	column, err = ValidateTombstoneCommitTSColumn(3, constant)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(1, 0), column.At(0))
	require.Equal(t, types.BuildTS(1, 0), column.At(2))
	constant.Free(mp)
}

func TestLegacyBackupTombstoneUsesTrailingCommitTS(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	blockID := *objectio.NewBlockid(objectio.NewSegmentid(), 1, 0)
	input := batch.NewWithSize(3)
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	input.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	defer input.Clean(mp)

	for _, row := range []struct {
		offset   uint32
		pk       types.TS
		commitTS types.TS
	}{
		{offset: 1, pk: types.BuildTS(100, 0), commitTS: types.BuildTS(5, 0)},
		{offset: 2, pk: types.BuildTS(100, 0), commitTS: types.BuildTS(20, 0)},
	} {
		require.NoError(t, vector.AppendFixed(
			input.Vecs[0], types.NewRowid(&blockID, row.offset), false, mp,
		))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], row.pk, false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], row.commitTS, false, mp))
	}
	input.SetRowCount(2)

	// The old Backup path used the generic writer. It persisted commitTS as
	// dense user seqnum 2 instead of hidden SEQNUM_COMMITTS.
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	writer, err := NewBlockWriter(fs, name.String())
	require.NoError(t, err)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	require.False(t, blocks[0].BlockHeader().Appendable())
	require.Equal(t, uint16(3), blocks[0].GetMetaColumnCount())
	require.Equal(t, uint16(2), blocks[0].GetMaxSeqnum())
	require.Equal(t, uint8(types.T_TS), blocks[0].ColumnMeta(2).DataType())

	location := objectio.BuildLocation(
		name,
		blocks[0].GetExtent(),
		uint32(input.RowCount()),
		blocks[0].GetID(),
	)
	snapshot := types.BuildTS(10, 0)
	trackingFS := &releaseTrackingFS{FileService: fs}

	t.Run("timestamp primary key remains distinct", func(t *testing.T) {
		cacheVectors := containers.NewVectors(3)
		pkType := types.T_TS.ToType()
		_, release, err := ReadDeletes(ctx, location, trackingFS, false, cacheVectors, &pkType)
		require.NoError(t, err)
		primaryKeys := vector.MustFixedColWithTypeCheck[types.TS](&cacheVectors[1])
		commitTSs := vector.MustFixedColWithTypeCheck[types.TS](&cacheVectors[2])
		require.Equal(t, []types.TS{types.BuildTS(100, 0), types.BuildTS(100, 0)}, primaryKeys)
		require.Equal(t, []types.TS{types.BuildTS(5, 0), types.BuildTS(20, 0)}, commitTSs)
		release()
		require.Zero(t, trackingFS.outstanding.Load())
	})

	t.Run("point lookup", func(t *testing.T) {
		row := types.NewRowid(&blockID, 1)
		deleted, err := IsRowDeletedByLocation(ctx, &snapshot, &row, location, trackingFS, false)
		require.NoError(t, err)
		require.True(t, deleted)
		require.Zero(t, trackingFS.outstanding.Load())
	})

	t.Run("block mask", func(t *testing.T) {
		mask, err := FillBlockDeleteMask(ctx, &snapshot, &blockID, location, trackingFS, false)
		require.NoError(t, err)
		defer mask.Release()
		require.True(t, mask.Contains(1))
		require.False(t, mask.Contains(2))
		require.Zero(t, trackingFS.outstanding.Load())
	})

	t.Run("reload failure releases first lease", func(t *testing.T) {
		failingFS := &failLegacyReloadFS{
			releaseTrackingFS: &releaseTrackingFS{FileService: fs},
		}
		cacheVectors := containers.NewVectors(2)
		_, release, err := ReadDeletes(ctx, location, failingFS, false, cacheVectors, nil)
		require.ErrorContains(t, err, "injected legacy commit-ts reload failure")
		require.Nil(t, release)
		require.True(t, failingFS.failed.Load())
		require.True(t, failingFS.firstLeaseReleasedOnFail.Load())
		require.Zero(t, failingFS.outstanding.Load())
	})
	require.Positive(t, trackingFS.tracked.Load())
}

func TestReadDeletesBroadcastsPersistedConstantCommitTS(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	blockID := *objectio.NewBlockid(objectio.NewSegmentid(), 1, 0)
	input := batch.NewWithSize(3)
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for offset := uint32(1); offset <= 2; offset++ {
		require.NoError(t, vector.AppendFixed(
			input.Vecs[0], types.NewRowid(&blockID, offset), false, mp,
		))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(offset), false, mp))
	}
	commitTS := types.BuildTS(5, 0)
	var err error
	input.Vecs[2], err = vector.NewConstFixed(types.T_TS.ToType(), commitTS, 2, mp)
	require.NoError(t, err)
	input.SetRowCount(2)
	defer input.Clean(mp)

	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	writer, err := objectio.NewObjectWriter(
		name, fs, 0, objectio.TombstoneSeqnums_DN_Created, nil,
	)
	require.NoError(t, err)
	_, err = writer.Write(input)
	require.NoError(t, err)
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)

	location := objectio.BuildLocation(
		name, blocks[0].GetExtent(), uint32(input.RowCount()), blocks[0].GetID(),
	)
	cacheVectors := containers.NewVectors(2)
	_, release, err := ReadDeletes(ctx, location, fs, false, cacheVectors, nil)
	require.NoError(t, err)
	require.True(t, cacheVectors[1].IsConst())
	validated, err := ValidateTombstoneCommitTSColumn(2, &cacheVectors[1])
	require.NoError(t, err)
	require.Equal(t, commitTS, validated.At(0))
	require.Equal(t, commitTS, validated.At(1))
	release()

	row := types.NewRowid(&blockID, 2)
	deleted, err := IsRowDeletedByLocation(ctx, &commitTS, &row, location, fs, false)
	require.NoError(t, err)
	require.True(t, deleted)
	mask, err := FillBlockDeleteMask(ctx, &commitTS, &blockID, location, fs, false)
	require.NoError(t, err)
	defer mask.Release()
	require.True(t, mask.Contains(1))
	require.True(t, mask.Contains(2))
}

func TestTimestampPrimaryKeyWithoutCommitTSIsRejected(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	blockID := *objectio.NewBlockid(objectio.NewSegmentid(), 1, 0)
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	defer input.Clean(mp)
	require.NoError(t, vector.AppendFixed(
		input.Vecs[0], types.NewRowid(&blockID, 1), false, mp,
	))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], types.BuildTS(5, 0), false, mp))
	input.SetRowCount(1)

	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	writer, err := NewBlockWriter(fs, name.String())
	require.NoError(t, err)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	require.Equal(t, uint16(2), blocks[0].GetMetaColumnCount())
	require.Equal(t, uint16(1), blocks[0].GetMaxSeqnum())

	location := objectio.BuildLocation(
		name,
		blocks[0].GetExtent(),
		uint32(input.RowCount()),
		blocks[0].GetID(),
	)
	snapshot := types.BuildTS(10, 0)
	row := types.NewRowid(&blockID, 1)
	trackingFS := &releaseTrackingFS{FileService: fs}

	t.Run("non-CN point lookup", func(t *testing.T) {
		_, err := IsRowDeletedByLocation(ctx, &snapshot, &row, location, trackingFS, false)
		require.ErrorContains(t, err, "commit-ts column is unavailable")
		require.Zero(t, trackingFS.outstanding.Load())
	})

	t.Run("non-CN block mask", func(t *testing.T) {
		_, err := FillBlockDeleteMask(ctx, &snapshot, &blockID, location, trackingFS, false)
		require.ErrorContains(t, err, "commit-ts column is unavailable")
		require.Zero(t, trackingFS.outstanding.Load())
	})

	t.Run("CN-created path", func(t *testing.T) {
		deleted, err := IsRowDeletedByLocation(ctx, &snapshot, &row, location, fs, true)
		require.NoError(t, err)
		require.True(t, deleted)
	})
}

func TestEvalDeleteMaskRejectsMissingCommitTS(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	var blockID types.Blockid
	rowIDs := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(rowIDs, types.NewRowid(&blockID, 1), false, mp))
	defer rowIDs.Free(mp)
	missingCommitTS := vector.NewConstNull(types.T_TS.ToType(), 1, mp)
	defer missingCommitTS.Free(mp)

	rows, err := EvalDeleteMaskFromDNCreatedTombstones(
		rowIDs, missingCommitTS, objectio.BlockObject{}, &types.TS{}, &blockID,
	)
	require.ErrorContains(t, err, "commit-ts column is unavailable")
	require.False(t, rows.IsValid())
}

func (d *releaseTrackingData) Slice(length int) fscache.Data {
	d.Data = d.Data.Slice(length)
	return d
}

func (d *releaseTrackingData) Retain() {
	d.Data.Retain()
	d.outstanding.Add(1)
}

func (d *releaseTrackingData) Release() {
	d.Data.Release()
	d.outstanding.Add(-1)
}

func TestLoadColumnsDataIntoSelectedRowsUsesHotSealedCache(t *testing.T) {
	const rowCount = 8192
	ctx := context.Background()
	cacheCapacity := toml.ByteSize(64 << 20)
	fs, err := fileservice.NewLocalFS2(
		ctx,
		defines.SharedFileServiceName,
		t.TempDir(),
		fileservice.CacheConfig{MemoryCapacity: &cacheCapacity},
		nil,
	)
	require.NoError(t, err)
	fs.SetAsyncUpdate(false)
	t.Cleanup(func() {
		fs.Close(ctx)
	})

	typs := []types.Type{
		types.T_int32.ToType(),
		types.T_int32.ToType(),
		types.New(types.T_char, 120, 0),
		types.New(types.T_char, 60, 0),
	}
	writeMP := mpool.MustNewZero()
	input := batch.NewWithSize(len(typs))
	for i := range typs {
		input.Vecs[i] = vector.NewVec(typs[i])
	}
	for i := 0; i < rowCount; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i+1), false, writeMP))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int32((i*7919)%rowCount), false, writeMP))
		require.NoError(t, vector.AppendBytes(
			input.Vecs[2],
			[]byte(fmt.Sprintf("%08d", i)+strings.Repeat("c", 111)),
			false,
			writeMP,
		))
		require.NoError(t, vector.AppendBytes(
			input.Vecs[3],
			[]byte(fmt.Sprintf("%08d", i)+strings.Repeat("p", 51)),
			false,
			writeMP,
		))
	}
	input.SetRowCount(rowCount)

	writer := ConstructWriter(0, []uint16{0, 1, 2, 3}, -1, false, false, fs)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	location := stats.ObjectLocation()
	input.Clean(writeMP)
	require.Zero(t, writeMP.CurrNB())
	mpool.DeleteMPool(writeMP)

	newDestinations := func() []*vector.Vector {
		ret := make([]*vector.Vector, len(typs))
		for i := range typs {
			ret[i] = vector.NewOffHeapVecWithType(typs[i])
		}
		return ret
	}
	freeDestinations := func(vectors []*vector.Vector, mp *mpool.MPool) {
		for _, vec := range vectors {
			vec.Free(mp)
		}
	}

	// Populate the memory cache deterministically before measuring the bounded
	// selected-row materialization path.
	warmMP := mpool.MustNewZero()
	warm := newDestinations()
	deleteMask, _, err := LoadColumnsDataInto(
		ctx,
		[]uint16{0, 1, 2, 3},
		typs,
		fs,
		location,
		warm,
		[]int64{0},
		nil,
		warmMP,
		fileservice.Policy(0),
	)
	require.NoError(t, err)
	deleteMask.Release()
	freeDestinations(warm, warmMP)
	require.Zero(t, warmMP.CurrNB())
	mpool.DeleteMPool(warmMP)

	queryMP, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(queryMP)
	sels := []int64{0, 7, 511, 1024, 2047, 4096, 6143, 7001, 8000, 8191}
	selected := newDestinations()
	deleteMask, fromCache, err := LoadColumnsDataInto(
		ctx,
		[]uint16{0, 1, 2, 3},
		typs,
		fs,
		location,
		selected,
		sels,
		nil,
		queryMP,
		fileservice.Policy(0),
	)
	require.NoError(t, err)
	deleteMask.Release()
	require.True(t, fromCache)
	require.Equal(t, len(sels), selected[0].Length())
	ids := vector.MustFixedColWithTypeCheck[int32](selected[0])
	keys := vector.MustFixedColWithTypeCheck[int32](selected[1])
	for i, sel := range sels {
		require.Equal(t, int32(sel+1), ids[i])
		require.Equal(t, int32((sel*7919)%rowCount), keys[i])
		require.Equal(t, fmt.Sprintf("%08d", sel)+strings.Repeat("c", 111), selected[2].GetStringAt(i))
		require.Equal(t, fmt.Sprintf("%08d", sel)+strings.Repeat("p", 51), selected[3].GetStringAt(i))
	}

	target := []byte(fmt.Sprintf("%08d", rowCount-1) + strings.Repeat("c", 111))
	search := objectio.NewReadFilterSearch(typs[2].Oid, [][]byte{target})
	beforeSearch := queryMP.CurrNB()
	found, fromCache, err := LoadColumnDataBySearch(
		ctx,
		2,
		typs[2],
		fs,
		location,
		search,
		false,
		nil,
		queryMP,
		fileservice.Policy(0),
	)
	require.NoError(t, err)
	require.True(t, fromCache)
	require.Equal(t, []int64{rowCount - 1}, found)
	require.Equal(t, beforeSearch, queryMP.CurrNB(), "scoped search must not duplicate the full varchar column")

	selected[2].GetBytesAt(0)[0] = 'X'
	again := newDestinations()
	deleteMask, fromCache, err = LoadColumnsDataInto(
		ctx,
		[]uint16{0, 1, 2, 3},
		typs,
		fs,
		location,
		again,
		sels[:1],
		nil,
		queryMP,
		fileservice.Policy(0),
	)
	require.NoError(t, err)
	deleteMask.Release()
	require.True(t, fromCache)
	require.Equal(t, fmt.Sprintf("%08d", sels[0])+strings.Repeat("c", 111), again[2].GetStringAt(0))

	trackingFS := &releaseTrackingFS{FileService: fs}
	partial := []*vector.Vector{
		vector.NewOffHeapVecWithType(typs[2]),
		vector.NewOffHeapVecWithType(types.T_int64.ToType()),
	}
	_, _, err = LoadColumnsDataInto(
		ctx,
		[]uint16{2, 3},
		typs[2:],
		trackingFS,
		location,
		partial,
		sels[:1],
		nil,
		queryMP,
		fileservice.Policy(0),
	)
	require.Error(t, err)
	require.Positive(t, trackingFS.tracked.Load())
	require.Zero(t, trackingFS.outstanding.Load(), "all cache leases must be released after a later column fails")
	require.Equal(t, 1, partial[0].Length())

	freeDestinations(selected, queryMP)
	freeDestinations(again, queryMP)
	freeDestinations(partial, queryMP)
	require.Zero(t, queryMP.CurrNB())
}

func TestLoadColumns2NeedCopyReleasesSourceCachedData(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(
		bat.Vecs[0],
		types.BuildTS(42, 0),
		false,
		mp,
	))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	writer := ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	trackingFS := &releaseTrackingFS{FileService: fs}
	pool := containers.NewVectorPool(
		t.Name(),
		1,
		containers.WithMPool(mp),
	)
	defer pool.Destory()
	stats := writer.GetObjectStats()

	vectors, release, err := LoadColumns2(
		ctx,
		[]uint16{0},
		[]types.Type{types.T_TS.ToType()},
		trackingFS,
		stats.ObjectLocation(),
		fileservice.Policy(0),
		true,
		pool,
	)
	require.NoError(t, err)
	require.Nil(t, release)
	require.Positive(t, trackingFS.tracked.Load())
	require.Zero(t, trackingFS.outstanding.Load())
	require.Len(t, vectors, 1)
	require.Equal(t, types.BuildTS(42, 0), vectors[0].Get(0))
	vectors[0].Close()
}
