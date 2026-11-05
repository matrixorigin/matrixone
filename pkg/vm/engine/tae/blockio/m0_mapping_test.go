// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockio

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
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
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

type m0TrackingFS struct {
	fileservice.FileService
	tracked         atomic.Int32
	outstanding     atomic.Int32
	corruptNext     bool
	corrupted       atomic.Bool
	cancelAfterRead bool
}

type m0TrackingData struct {
	fscache.Data
	outstanding *atomic.Int32
	corrupt     bool
}

func (d *m0TrackingData) Bytes() []byte {
	if d.corrupt {
		return nil
	}
	return d.Data.Bytes()
}

func (d *m0TrackingData) Slice(length int) fscache.Data {
	d.Data = d.Data.Slice(length)
	return d
}

func (d *m0TrackingData) Retain()  { d.Data.Retain(); d.outstanding.Add(1) }
func (d *m0TrackingData) Release() { d.Data.Release(); d.outstanding.Add(-1) }

func (f *m0TrackingFS) Read(ctx context.Context, ioVector *fileservice.IOVector) error {
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
		corrupt := f.corruptNext && len(ioVector.Entries) == 2 && i == 1
		if corrupt {
			f.corrupted.Store(true)
		}
		ioVector.Entries[i].CachedData = &m0TrackingData{
			Data: data, outstanding: &f.outstanding,
			corrupt: corrupt,
		}
	}
	if f.cancelAfterRead && len(ioVector.Entries) == 2 && f.tracked.Load() > 0 {
		return context.Canceled
	}
	return nil
}

// This is a mapping-consumer fixture, not evidence for historical provenance or
// the ordinary/M0 classifier. Real-classifier/SQL regressions remain separate.
func m0MappingFixture(t *testing.T) (context.Context, fileservice.FileService, objectio.Location, *mpool.MPool, *m0read.ObjectReadLayout) {
	t.Helper()
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	t.Cleanup(func() { input.Clean(mp) })
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int32{11, 22, 33}, nil, mp))
	input.SetRowCount(3)
	writer := ioutil.ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	location := info.MetaLocation()
	layout := &m0read.ObjectReadLayout{Evolution: true, ObjectName: location.Name().String(), WrittenSize: 1, SchemaSize: 2}
	return ctx, fs, location, mp, layout
}

// The same immutable object is read through the physical ordinary API and
// through the explicit strict Engine API. This is a geometry/contract control,
// not a claim of qualification for any historical upgrade source.
func TestM3OrdinaryAndStrictReadSameObjectBytes(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	typ := types.T_int32.ToType()
	cache := containers.NewVectors(2)
	defer cache.Free(mp)
	release, _, err := ioutil.LoadColumns(ctx, []uint16{0, 1}, []types.Type{typ, typ}, fs, loc, cache, mp, fileservice.SkipAllCache)
	require.NoError(t, err)
	require.NotNil(t, release)
	defer release()
	require.Equal(t, []int32{11, 22, 33}, vector.MustFixedColWithTypeCheck[int32](&cache[0]))
	require.Equal(t, 3, cache[1].Length())
	require.Equal(t, typ, *cache[1].GetType())
	require.True(t, cache[1].IsConstNull())
	strict := containers.NewVectors(2)
	defer strict.Free(mp)
	_, _, err = loadColumns(ctx, []uint16{0, 1}, []types.Type{typ, typ}, fs, loc, strict, mp, fileservice.SkipAllCache, &m0read.ObjectReadLayout{ObjectName: layout.ObjectName})
	require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
	// An empty upstream selection must not bypass strict projection checks.
	var info objectio.BlockInfo
	info.SetMetaLocation(loc)
	info.ConstructBlockID(loc.Name(), loc.ID())
	out := batch.NewWithSize(2)
	out.Vecs[0] = vector.NewVec(typ)
	out.Vecs[1] = vector.NewVec(typ)
	defer out.Clean(mp)
	err = BlockDataReadInner(ctx, &info, &blockReadTestDataSource{}, []uint16{0, 1}, []types.Type{typ, typ}, -1, types.TS{}, []int64{}, nil, fileservice.SkipAllCache, out, strict, mp, fs, &m0read.ObjectReadLayout{ObjectName: layout.ObjectName})
	require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
}

func TestM0EngineColumnMapping(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	ty := types.T_int32.ToType()
	for _, tc := range []struct {
		name string
		cols []uint16
	}{
		{"reordered mixed projection", []uint16{1, 0, 1}},
		{"all null projection", []uint16{1}},
		{"physical only", []uint16{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typs := make([]types.Type, len(tc.cols))
			for i := range typs {
				typs[i] = ty
			}
			cache := containers.NewVectors(len(tc.cols))
			release, _, err := loadColumns(ctx, tc.cols, typs, fs, loc, cache, mp, fileservice.SkipAllCache, layout)
			require.NoError(t, err)
			defer release()
			for i, seq := range tc.cols {
				require.Equal(t, 3, cache[i].Length())
				require.Equal(t, ty, *cache[i].GetType())
				if seq == 1 {
					require.True(t, cache[i].IsConstNull())
				} else {
					require.Equal(t, []int32{11, 22, 33}, vector.MustFixedColWithTypeCheck[int32](&cache[i]))
				}
			}
		})
	}

	t.Run("explicit physical-only Engine read rejects missing column", func(t *testing.T) {
		cache := containers.NewVectors(1)
		_, _, err := loadColumns(context.Background(), []uint16{1}, []types.Type{ty}, fs, loc, cache, mp, fileservice.SkipAllCache, &m0read.ObjectReadLayout{ObjectName: layout.ObjectName})
		require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
	})
	t.Run("wrong object", func(t *testing.T) {
		wrong := &m0read.ObjectReadLayout{Evolution: true, ObjectName: "other", WrittenSize: 1, SchemaSize: 2}
		_, err := mapM0Columns(ctx, []uint16{1}, []types.Type{ty}, fs, loc, wrong)
		require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
	})
	t.Run("wrong checked block", func(t *testing.T) {
		wrong := *layout
		wrong.BlockScoped = true
		wrong.BlockID = loc.ID() + 1
		_, err := mapM0Columns(ctx, []uint16{1}, []types.Type{ty}, fs, loc, &wrong)
		require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
	})
	t.Run("schema bound", func(t *testing.T) {
		_, err := mapM0Columns(ctx, []uint16{2}, []types.Type{ty}, fs, loc, layout)
		require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
	})
	t.Run("physical type mismatch", func(t *testing.T) {
		_, err := mapM0Columns(ctx, []uint16{0, 1}, []types.Type{types.T_int64.ToType(), ty}, fs, loc, layout)
		require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
	})
	t.Run("prefix width mismatch", func(t *testing.T) {
		wrong := &m0read.ObjectReadLayout{Evolution: true, ObjectName: loc.Name().String(), WrittenSize: 2, SchemaSize: 3}
		_, err := mapM0Columns(ctx, []uint16{2}, []types.Type{ty}, fs, loc, wrong)
		require.ErrorIs(t, err, m0read.ErrMissingColumnWithoutProof)
	})
	t.Run("canceled", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := mapM0Columns(canceled, []uint16{1}, []types.Type{ty}, fs, loc, layout)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestCopyBlockDataRejectsInvalidDeleteOffsets(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	typ := types.T_int32.ToType()
	for _, tc := range []struct {
		name    string
		deletes []int64
		wantErr bool
	}{
		{"valid", []int64{1}, false},
		{"duplicate", []int64{1, 1}, true},
		{"outside block", []int64{3}, true},
		{"negative", []int64{-1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := batch.NewWithSize(1)
			defer out.Clean(mp)
			err := CopyBlockData(ctx, loc, tc.deletes, []uint16{0}, []types.Type{typ}, out, fs, mp, layout)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []int32{11, 33}, vector.MustFixedColWithTypeCheck[int32](out.Vecs[0]))
		})
	}
}

func TestM0EngineMappingSelectedRows(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	ty := types.T_int32.ToType()
	t.Run("destination scale mismatch", func(t *testing.T) {
		requested := types.T_decimal64.ToType()
		requested.Width, requested.Scale = 10, 2
		destinationType := requested
		destinationType.Scale = 3
		destination := vector.NewVec(destinationType)
		defer destination.Free(mp)
		mask, _, err := loadColumnsDataInto(ctx, []uint16{1}, []types.Type{requested}, fs, loc,
			[]*vector.Vector{destination}, nil, nil, mp, fileservice.SkipAllCache, layout)
		defer mask.Release()
		require.Error(t, err, "an M0 NULL projection must retain the requested decimal scale")
		require.Zero(t, destination.Length(), "type validation must precede materialization")
	})
	for _, cols := range [][]uint16{{1, 0}, {1}} {
		for _, sels := range [][]int64{nil, {}, {2, 0}, {-1}, {3}} {
			func() {
				dst := make([]*vector.Vector, len(cols))
				typs := make([]types.Type, len(cols))
				for i := range cols {
					dst[i] = vector.NewVec(ty)
					defer dst[i].Free(mp)
					typs[i] = ty
				}
				mask, _, err := loadColumnsDataInto(ctx, cols, typs, fs, loc, dst, sels, nil, mp, fileservice.SkipAllCache, layout)
				defer mask.Release()
				if len(sels) == 1 && (sels[0] == -1 || sels[0] == 3) {
					require.Error(t, err)
					for _, v := range dst {
						require.Zero(t, v.Length())
					}
					return
				}
				require.NoError(t, err)
				rows := 3
				if sels != nil {
					rows = len(sels)
				}
				for i, col := range cols {
					require.Equal(t, rows, dst[i].Length())
					for row := 0; row < rows; row++ {
						if col == 1 {
							require.True(t, dst[i].IsNull(uint64(row)))
						} else {
							physicalRow := row
							if sels != nil {
								physicalRow = int(sels[row])
							}
							require.Equal(t, int32((physicalRow+1)*11), vector.GetFixedAtNoTypeCheck[int32](dst[i], row))
						}
					}
				}
			}()
		}
	}
}

// A destination owns its successfully materialized prefix after an error.
// Exhaust a separate pool only after preparing the first destination so the
// NULL projection fails after the physical projection was produced.
func TestM0EngineMappingPartialFailureCleanup(t *testing.T) {
	ctx, fs, loc, _, layout := m0MappingFixture(t)
	tracking := &m0TrackingFS{FileService: fs}
	mp, err := mpool.NewMPool("m0-partial-failure", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)
	ty := types.T_int32.ToType()
	first, second := vector.NewVec(ty), vector.NewOffHeapVecWithType(ty)
	var reserved [][]byte
	defer func() {
		first.Free(mp)
		second.Free(mp)
		for _, buf := range reserved {
			mp.Free(buf)
		}
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, first.PreExtend(3, mp))
	require.NoError(t, first.PreExtendNulls(3, mp))
	for _, size := range []int{4096, 128, 1} {
		for n := 0; n < 1025; n++ {
			buf, allocErr := mp.Alloc(size, true)
			if allocErr != nil {
				break
			}
			reserved = append(reserved, buf)
		}
	}
	mask, _, err := loadColumnsDataInto(ctx, []uint16{0, 1}, []types.Type{ty, ty}, tracking, loc,
		[]*vector.Vector{first, second}, nil, nil, mp, fileservice.SkipAllCache, layout)
	mask.Release()
	require.Error(t, err)
	require.Equal(t, 3, first.Length())
	require.Equal(t, []int32{11, 22, 33}, vector.MustFixedColWithTypeCheck[int32](first))
	require.Positive(t, tracking.tracked.Load())
	require.Zero(t, tracking.outstanding.Load(), "the physical I/O lease must be released before NULL materialization fails")
}

func TestM0EngineMappingCacheLeaseFailure(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	tracking := &m0TrackingFS{FileService: fs, corruptNext: true}
	ty := types.T_int32.ToType()
	cache := containers.NewVectors(3)
	defer cache.Free(mp)
	release, _, err := loadColumns(ctx, []uint16{0, 0, 1},
		[]types.Type{ty, ty, ty}, tracking, loc, cache, mp, fileservice.SkipAllCache, layout)
	require.Error(t, err)
	require.Nil(t, release)
	require.True(t, tracking.corrupted.Load())
	require.Positive(t, tracking.tracked.Load())
	require.Zero(t, tracking.outstanding.Load(), "failed cache-vector decoding must release the I/O lease")
}

func TestM0EngineMappingCancellationAndCacheLease(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	ty := types.T_int32.ToType()
	t.Run("cache-success", func(t *testing.T) {
		tracking := &m0TrackingFS{FileService: fs}
		cache := containers.NewVectors(2)
		release, _, err := loadColumns(ctx, []uint16{0, 1}, []types.Type{ty, ty}, tracking, loc, cache, mp, fileservice.SkipAllCache, layout)
		if err != nil {
			cache.Free(mp)
			require.NoError(t, err)
		}
		require.NotNil(t, release)
		defer func() {
			release()
			require.Zero(t, tracking.outstanding.Load())
		}()
		require.Positive(t, tracking.tracked.Load())
		require.Positive(t, tracking.outstanding.Load())
		require.Equal(t, []int32{11, 22, 33}, vector.MustFixedColWithTypeCheck[int32](&cache[0]))
		require.True(t, cache[1].IsConstNull())
	})
	t.Run("canceled-before-read", func(t *testing.T) {
		tracking := &m0TrackingFS{FileService: fs}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		cache := containers.NewVectors(2)
		defer cache.Free(mp)
		release, _, err := loadColumns(canceled, []uint16{0, 1}, []types.Type{ty, ty}, tracking, loc, cache, mp, fileservice.SkipAllCache, layout)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, release)
		dest := []*vector.Vector{vector.NewVec(ty), vector.NewVec(ty)}
		defer dest[0].Free(mp)
		defer dest[1].Free(mp)
		mask, _, err := loadColumnsDataInto(canceled, []uint16{0, 1}, []types.Type{ty, ty}, tracking, loc, dest, []int64{2, 0}, nil, mp, fileservice.SkipAllCache, layout)
		defer mask.Release()
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, tracking.outstanding.Load())
		require.Zero(t, dest[0].Length())
		require.Zero(t, dest[1].Length())
	})
}

func TestM0EngineMappingTNLease(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	pool := containers.NewVectorPool("m0-mapping", 0, containers.WithMPool(mp))
	defer pool.Destory()
	ty := types.T_int32.ToType()
	for _, copyData := range []bool{false, true} {
		for _, cols := range [][]uint16{{1, 0}, {1}} {
			func() {
				typs := make([]types.Type, len(cols))
				for i := range typs {
					typs[i] = ty
				}
				vecs, release, err := LoadColumns2(ctx, cols, typs, fs, loc, fileservice.SkipAllCache, copyData, pool, layout)
				require.NoError(t, err)
				defer func() {
					for _, v := range vecs {
						v.Close()
					}
					if release != nil {
						release()
					}
				}()
				for i, col := range cols {
					require.Equal(t, 3, vecs[i].Length())
					if col == 1 {
						require.True(t, vecs[i].IsConstNull())
					} else {
						require.Equal(t, []int32{11, 22, 33}, vector.MustFixedColWithTypeCheck[int32](vecs[i].GetDownstreamVector()))
					}
				}
			}()
		}
	}
}

func TestM0EngineMappingTNLeaseFailure(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	pool := containers.NewVectorPool("m0-mapping-failure", 0, containers.WithMPool(mp))
	defer pool.Destory()
	ty := types.T_int32.ToType()
	for _, needCopy := range []bool{false, true} {
		t.Run(fmt.Sprintf("copy=%t", needCopy), func(t *testing.T) {
			tracking := &m0TrackingFS{FileService: fs, corruptNext: true}
			vectors, release, err := LoadColumns2(ctx, []uint16{0, 0, 1},
				[]types.Type{ty, ty, ty}, tracking, loc, fileservice.SkipAllCache, needCopy, pool, layout)
			require.Error(t, err)
			require.True(t, tracking.corrupted.Load(), "the second physical cache entry must be corrupted")
			require.Positive(t, tracking.tracked.Load())
			require.Empty(t, vectors)
			require.Nil(t, release)
			require.Zero(t, tracking.outstanding.Load(), "loader must release all source leases on decode failure")
		})
		t.Run(fmt.Sprintf("success-copy=%t", needCopy), func(t *testing.T) {
			tracking := &m0TrackingFS{FileService: fs}
			vectors, release, err := LoadColumns2(ctx, []uint16{0, 1},
				[]types.Type{ty, ty}, tracking, loc, fileservice.SkipAllCache, needCopy, pool, layout)
			defer func() {
				for _, v := range vectors {
					if v != nil {
						v.Close()
					}
				}
				if release != nil {
					release()
				}
				require.Zero(t, tracking.outstanding.Load())
			}()
			require.NoError(t, err)
			require.Positive(t, tracking.tracked.Load())
			if needCopy {
				require.Zero(t, tracking.outstanding.Load())
			} else {
				require.NotNil(t, release)
				require.Positive(t, tracking.outstanding.Load())
			}
		})
	}
	t.Run("canceled", func(t *testing.T) {
		tracking := &m0TrackingFS{FileService: fs}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		vectors, release, err := LoadColumns2(canceled, []uint16{0, 1},
			[]types.Type{ty, ty}, tracking, loc, fileservice.SkipAllCache, false, pool, layout)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, vectors)
		require.Nil(t, release)
		require.Zero(t, tracking.outstanding.Load())
	})
}

// Simulate a file-service read that acquired physical leases and then failed
// with cancellation. Each public A1 entry must release the acquired data.
func TestM0EngineMappingCancelAfterRead(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	ty := types.T_int32.ToType()
	cols, typs := []uint16{0, 0, 1}, []types.Type{ty, ty, ty}
	t.Run("cache", func(t *testing.T) {
		tracking := &m0TrackingFS{FileService: fs, cancelAfterRead: true}
		cache := containers.NewVectors(3)
		defer cache.Free(mp)
		release, _, err := loadColumns(ctx, cols, typs, tracking, loc, cache, mp, fileservice.SkipAllCache, layout)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, release)
		require.Positive(t, tracking.tracked.Load())
		require.Zero(t, tracking.outstanding.Load())
	})
	t.Run("selected", func(t *testing.T) {
		tracking := &m0TrackingFS{FileService: fs, cancelAfterRead: true}
		dest := []*vector.Vector{vector.NewVec(ty), vector.NewVec(ty), vector.NewVec(ty)}
		defer func() {
			for _, v := range dest {
				v.Free(mp)
			}
		}()
		mask, _, err := loadColumnsDataInto(ctx, cols, typs, tracking, loc, dest, []int64{2, 0}, nil, mp, fileservice.SkipAllCache, layout)
		defer mask.Release()
		require.ErrorIs(t, err, context.Canceled)
		require.Positive(t, tracking.tracked.Load())
		require.Zero(t, tracking.outstanding.Load())
	})
	for _, copyData := range []bool{false, true} {
		t.Run(fmt.Sprintf("tn-copy=%t", copyData), func(t *testing.T) {
			tracking := &m0TrackingFS{FileService: fs, cancelAfterRead: true}
			pool := containers.NewVectorPool(t.Name(), 0, containers.WithMPool(mp))
			defer pool.Destory()
			vectors, release, err := LoadColumns2(ctx, cols, typs, tracking, loc, fileservice.SkipAllCache, copyData, pool, layout)
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, vectors)
			require.Nil(t, release)
			require.Positive(t, tracking.tracked.Load())
			require.Zero(t, tracking.outstanding.Load())
		})
	}
}

func TestM0EngineMappingBlockReadTombstones(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	var info objectio.BlockInfo
	info.SetMetaLocation(loc)
	info.ConstructBlockID(loc.Name(), loc.ID())
	ty := types.T_int32.ToType()
	for _, cols := range [][]uint16{{1, 0}, {1}} {
		func() {
			typs := make([]types.Type, len(cols))
			output := batch.NewWithSize(len(cols))
			defer output.Clean(mp)
			for i := range cols {
				typs[i] = ty
				output.Vecs[i] = vector.NewVec(ty)
			}
			cache := containers.NewVectors(len(cols))
			defer cache.Free(mp)
			require.NoError(t, BlockDataReadInner(
				ctx, &info, &blockReadTestDataSource{deleted: []uint64{1}}, cols, typs, -1,
				types.TS{}, nil, nil, fileservice.SkipAllCache, output, cache, mp, fs, layout,
			))
			for i, col := range cols {
				require.Equal(t, 2, output.Vecs[i].Length())
				if col == 1 {
					require.True(t, output.Vecs[i].IsNull(0))
					require.True(t, output.Vecs[i].IsNull(1))
				} else {
					require.Equal(t, []int32{11, 33}, vector.MustFixedColWithTypeCheck[int32](output.Vecs[i]))
				}
			}
		}()
	}
}

func TestM0EngineTopNMissingVector(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	typ := types.T_array_float32.ToType()
	top := &objectio.IndexReaderTopOp{ColPos: 1, Limit: 2, Typ: types.T_array_float32, NumVec: types.ArrayToBytes([]float32{0}), MetricType: metric.Metric_L2Distance, DistHeap: make(objectio.Float64Heap, 0, 2)}
	rows, distances, _, err := loadColumnTopN(ctx, 1, typ, fs, loc, []int64{0, 2}, top, mp, fileservice.SkipAllCache, layout)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Empty(t, distances)
	var info objectio.BlockInfo
	info.SetMetaLocation(loc)
	info.ConstructBlockID(loc.Name(), loc.ID())
	out := batch.NewWithSize(2)
	defer out.Clean(mp)
	out.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	out.Vecs[1] = vector.NewVec(typ)
	cache := containers.NewVectors(2)
	defer cache.Free(mp)
	require.NoError(t, BlockDataReadInner(ctx, &info, &blockReadTestDataSource{deleted: []uint64{1}}, []uint16{0, 1}, []types.Type{types.T_int32.ToType(), typ}, -1, types.TS{}, nil, top, fileservice.SkipAllCache, out, cache, mp, fs, layout))
	require.Zero(t, out.RowCount())
	for _, v := range out.Vecs {
		require.Zero(t, v.Length(), "no winners must not load all payload rows")
	}
}

func TestM0EngineFilteredTopNMissingEarlyColumn(t *testing.T) {
	ctx, fs, _, mp, _ := m0MappingFixture(t)
	typ := types.T_array_float32.ToType()
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(typ)
	defer input.Clean(mp)
	for _, v := range []float32{3, 2, 1} {
		require.NoError(t, vector.AppendArray(input.Vecs[0], []float32{v}, false, mp))
	}
	input.SetRowCount(3)
	writer := ioutil.ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	loc := info.MetaLocation()
	layout := &m0read.ObjectReadLayout{Evolution: true, ObjectName: loc.Name().String(), WrittenSize: 1, SchemaSize: 2}
	out := batch.NewWithSize(2)
	defer out.Clean(mp)
	out.Vecs[0] = vector.NewVec(typ)
	out.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	cache := containers.NewVectors(2)
	defer cache.Free(mp)
	top := &objectio.IndexReaderTopOp{ColPos: 0, Limit: 1, Typ: types.T_array_float32, NumVec: types.ArrayToBytes([]float32{0}), MetricType: metric.Metric_L2Distance}
	calls := 0
	err = blockDataReadWithFilter(ctx, &info, &blockReadTestDataSource{}, []uint16{0, 1}, []types.Type{typ, types.T_int32.ToType()}, -1, types.TS{}, []int64{0, 1, 2}, fileservice.SkipAllCache, out, cache, mp, fs, []int{1}, &engine.ReaderFilterRequest{Apply: func(b *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
		calls++
		require.Equal(t, []int{1}, loaded)
		require.Equal(t, 3, b.RowCount())
		for i := uint64(0); i < 3; i++ {
			require.True(t, b.Vecs[1].IsNull(i))
		}
		selected := []int64{1, 2}
		require.NoError(t, b.Vecs[1].Shrink(selected, false, mp))
		b.SetRowCount(2)
		return engine.ReaderFilterResult{Sels: selected}, nil
	}}, top, nil, layout)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, 1, out.RowCount())
	require.True(t, out.Vecs[1].IsNull(0))
	require.Equal(t, []float64{1}, vector.MustFixedColWithTypeCheck[float64](out.Vecs[2]))
}

// Exact membership may use a physical root key while projecting a later
// nullable vector. The fused physical TopK loader cannot materialize that
// projection; it must retain membership and tombstones on the generic path.
func TestM0EngineExactMembershipTopNMissingVector(t *testing.T) {
	ctx, fs, loc, mp, layout := m0MappingFixture(t)
	var info objectio.BlockInfo
	info.SetMetaLocation(loc)
	info.ConstructBlockID(loc.Name(), loc.ID())
	typ := types.T_array_float32.ToType()
	out := batch.NewWithSize(2)
	defer out.Clean(mp)
	out.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	out.Vecs[1] = vector.NewVec(typ)
	cache := containers.NewVectors(2)
	defer cache.Free(mp)
	calls := 0
	filter := objectio.BlockReadFilter{Valid: true, ExactMembership: true, UnSortedSearchFunc: func(v containers.Vectors) []int64 {
		calls++
		require.Equal(t, []int32{11, 22, 33}, vector.MustFixedColNoTypeCheck[int32](&v[0]))
		return []int64{0, 2}
	}}
	top := &objectio.IndexReaderTopOp{ColPos: 1, Limit: 2, Typ: types.T_array_float32, NumVec: types.ArrayToBytes([]float32{0}), MetricType: metric.Metric_L2Distance}
	require.NoError(t, BlockDataRead(ctx, &info, &blockReadTestDataSource{deleted: []uint64{2}}, []uint16{0, 1}, []types.Type{types.T_int32.ToType(), typ}, -1, timestamp.Timestamp{}, []uint16{0}, []types.Type{types.T_int32.ToType()}, filter, top, fileservice.SkipAllCache, "m0", out, cache, mp, fs, layout))
	require.Equal(t, 1, calls)
	require.Zero(t, out.RowCount())
	require.Len(t, out.Vecs, 3)
}
