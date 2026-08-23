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

func TestAppendableVisibilityFiltersAbortFromMaterializeAndSearch(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	input := batch.NewWithSize(3)
	input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	input.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	input.Vecs[2] = vector.NewVec(types.T_bool.ToType())
	defer input.Clean(mp)
	for _, row := range []struct {
		key     string
		commit  types.TS
		aborted bool
	}{
		{key: "live", commit: types.BuildTS(5, 0)},
		{key: "aborted", commit: types.BuildTS(6, 0), aborted: true},
		{key: "future", commit: types.BuildTS(20, 0)},
	} {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(row.key), false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], row.commit, false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], row.aborted, false, mp))
	}
	input.SetRowCount(3)

	writer := ConstructWriter(
		0,
		[]uint16{0, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT},
		-1,
		false,
		false,
		fs,
	)
	writer.SetAppendable()
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats(objectio.WithAppendable())
	location := stats.ObjectLocation()
	snapshot := types.BuildTS(10, 0)

	destination := vector.NewVec(types.T_varchar.ToType())
	defer destination.Free(mp)
	deleteMask, _, err := LoadColumnsDataInto(
		ctx,
		[]uint16{0},
		[]types.Type{types.T_varchar.ToType()},
		fs,
		location,
		[]*vector.Vector{destination},
		nil,
		&snapshot,
		mp,
		fileservice.Policy(0),
	)
	require.NoError(t, err)
	defer deleteMask.Release()
	require.True(t, deleteMask.Contains(1), "aborted row must be hidden from full scans")
	require.True(t, deleteMask.Contains(2), "future row must be hidden from full scans")
	require.False(t, deleteMask.Contains(0))

	search := objectio.NewReadFilterSearch(
		types.T_varchar,
		[][]byte{[]byte("live"), []byte("aborted"), []byte("future")},
	)
	sels, _, err := LoadColumnDataBySearch(
		ctx,
		0,
		types.T_varchar.ToType(),
		fs,
		location,
		search,
		false,
		&snapshot,
		mp,
		fileservice.Policy(0),
	)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, sels, "cached search must return only live visible rows")
}

func TestReadDeletesBroadcastsConstantAbort(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	blockID := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	input := batch.NewWithSize(3)
	input.Vecs[0] = vector.NewVec(objectio.RowidType)
	input.Vecs[1] = vector.NewVec(objectio.TSType)
	input.Vecs[2], _ = vector.NewConstFixed(types.T_bool.ToType(), false, 3, mp)
	defer input.Clean(mp)
	for row := 0; row < 3; row++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], types.NewRowid(blockID, uint32(row)), false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], types.BuildTS(5, 0), false, mp))
	}
	input.SetRowCount(3)
	writer := ConstructWriter(0, []uint16{objectio.SEQNUM_ROWID, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT}, -1, false, false, fs)
	writer.SetAppendable()
	require.NoError(t, func() error { _, err := writer.WriteBatch(input); return err }())
	_, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats(objectio.WithAppendable())
	location := stats.ObjectLocation()

	cache := containers.NewVectors(3)
	_, release, err := ReadDeletes(ctx, location, fs, false, cache, nil)
	require.NoError(t, err)
	defer release()
	aborts, err := ValidateTombstoneAbortColumn(3, &cache[2])
	require.NoError(t, err)
	require.True(t, aborts.IsPresent())
	require.False(t, aborts.At(1))
}

func TestFillBlockDeleteMaskSupportsLegacyTombstoneWithoutAbortColumn(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	blockID := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(objectio.RowidType)
	input.Vecs[1] = vector.NewVec(objectio.TSType)
	defer input.Clean(mp)
	require.NoError(t, vector.AppendFixed(
		input.Vecs[0], types.NewRowid(blockID, 7), false, mp,
	))
	require.NoError(t, vector.AppendFixed(
		input.Vecs[1], types.BuildTS(5, 0), false, mp,
	))
	input.SetRowCount(1)

	writer := ConstructWriter(
		0,
		[]uint16{objectio.SEQNUM_ROWID, objectio.SEQNUM_COMMITTS},
		-1,
		false,
		false,
		fs,
	)
	writer.SetAppendable()
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats(objectio.WithAppendable())
	location := stats.ObjectLocation()
	snapshot := types.BuildTS(10, 0)

	mask, err := FillBlockDeleteMask(
		ctx, &snapshot, blockID, location, fs, false,
	)
	require.NoError(t, err)
	defer mask.Release()
	require.True(t, mask.Contains(7))
}

func TestValidateTombstoneAbortColumnRejectsMalformed(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	wrongType := vector.NewVec(types.T_int8.ToType())
	defer wrongType.Free(mp)
	require.NoError(t, func() error { return vector.AppendFixed(wrongType, int8(1), false, mp) }())
	_, err := ValidateTombstoneAbortColumn(1, wrongType)
	require.Error(t, err)

	short := vector.NewVec(types.T_bool.ToType())
	defer short.Free(mp)
	require.NoError(t, func() error { return vector.AppendFixed(short, true, false, mp) }())
	_, err = ValidateTombstoneAbortColumn(2, short)
	require.Error(t, err)

	null := vector.NewVec(types.T_bool.ToType())
	defer null.Free(mp)
	require.NoError(t, func() error { return vector.AppendFixed(null, false, true, mp) }())
	_, err = ValidateTombstoneAbortColumn(1, null)
	require.Error(t, err)
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
